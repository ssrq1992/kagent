# kagent 编译、构建与生产环境启动手册

适用对象：kagent 应用管理员、构建和运维人员。适用代码：本目录 `kagent/` 与同级配套 `ax/`。更新时间：2026-10-01。

本手册从 Linux 源码准备开始，完成镜像、数据库、证书、Helm、SSO、首个 Agent 与 Sandbox 验证。实际 Kubernetes/Substrate/AX 平台搭建见 [AX 手册](AX编译构建与生产环境启动手册.md)，两份按顺序使用。这里只写可核对的操作，不声称目标生产集群已经验收。

## 0. 必须先了解的运行边界

1. **仅新安装**。新库完整执行 Goose 000001–000004 及启用的 vector track；发现需要接管的旧运行数据会拒绝，不能删除业务数据绕过。
2. kagent 仅连接 AX，不安装 Substrate、不直接创建 WorkerPool。TaskGroup、运行资产、MITM 与 credential provider 由 AX 平台管理员准备。
3. 生产默认不能沿用 Chart 的 `auth.mode: insecure`、bundled PostgreSQL 或直接向公网暴露 UI/controller。本文示例用外部 PostgreSQL、TLS 和 trusted-proxy。
4. **trusted-proxy 不是 JWT 验证器**：当前 controller 解析上游已验证的 JWT，默认业务 Authorizer 为 Noop。适用于受信任组织边界下的安装，不等于已经具备敌对多租户隔离。必须部署真正验签的身份代理和网络/L7 隔离；租户级授权需要平台另行交付，不能靠修改本手册参数获得。
5. 原生 kagent CLI 当前有 TLS/用户分区参数，但没有完整 OIDC 登录或 bearer-token 通用注入选项。`--user-id` 不是认证凭据，`invoke --token` 是模型 token，不是平台登录 token。生产业务验收先走已验证 OIDC 的 UI/API；不能编造 `--auth-token` 参数，也不能为了让 CLI 成功关闭生产鉴权。见第 10 节。
6. 本次只核对文档与本地渲染，不执行部署。真实 golden、MITM、快照、放置与灾难恢复仍是发布前置条件。

## 1. 安装前交接单

平台管理员应提供：

| 项目 | 本手册默认值/要求 |
|---|---|
| Kubernetes context | 显式 `agent-prod`，真实 Linux 节点/CNI/CSI/证书 API |
| 业务 namespace | `kagent`；必须存在且已纳入 AX clients 授权 |
| AX endpoint | `dns:///ax-server.ax-system.svc:8443` |
| AX server SAN | `ax-server.ax-system.svc` |
| AX client identity | `spiffe://agent.example/kagent/controller`，允许 atespace `kagent` |
| TaskGroup | `kagent-default`，UID 和容量已确认，gVisor 已验证 |
| callbackURL | `https://kagent-controller.kagent:8083` |
| Credential provider | 已验证 `k8s.io/default/...` URI、Secret 授权、mTLS 和撤销 |
| 存储 | 独立业务 PostgreSQL database/role、TLS CA、pgvector、备份/PITR |
| PKI | AX server CA/client cert/key，controller server cert/key/CA，公共入口证书 |
| 公共身份入口 | `https://agents.example.com` 与组织 OIDC issuer/client |
| 发布制品 | 配套 AX/kagent 源码版本、controller/UI/各 Harness/Guest image digest |

四类编译器当前注入 `https://kagent-controller.kagent:8083`，因此 AX callbackURL 必须使用这个短主机名；UI 代理验证 `.svc` 名称，证书应同时含两者 SAN。只保证两个 DNS 解析到同一地址不能让凭据 hostname 匹配。修改 Helm release name 会改变服务名，所以本文固定 release/fullname 为 `kagent`。

## 2. Linux 源码与工具

### 2.1 目录布局和版本

```text
/srv/agent-platform/
  kagentOnAx/
    ax/                 # 配套改造版本，不是未修改的上游 AX
    kagent/
      go/go.mod         # replace github.com/google/ax => ../../ax
  bin/
  release/
  ops/                  # 环境配置；私钥、DSN、模型 key 不入 Git
```

```bash
export WORK_ROOT=/srv/agent-platform
export OPS_DIR="$WORK_ROOT/ops"
export RELEASE_DIR="$WORK_ROOT/release"
export KUBE_CONTEXT=agent-prod
export RELEASE_VERSION=0.0.0-ax.release.001
export IMAGE_REGISTRY=registry.example.com
export PATH="/opt/go-1.27.1/bin:$WORK_ROOT/bin:$PATH"
mkdir -p "$OPS_DIR/pki" "$RELEASE_DIR" "$WORK_ROOT/bin"
umask 077
kubectl config use-context "$KUBE_CONTEXT"
test "$(kubectl config current-context)" = "$KUBE_CONTEXT"
test -f "$WORK_ROOT/kagentOnAx/ax/pkg/apis/v1alpha1/execution.proto"
```

若从 Git 获取，分别检出包含本次实现的完整 commit，不能以原始基线 SHA 代替尚未发布的工作区 patch。AX 与 kagent 的源代码必须作为同一发布单元记录。

### 2.2 工具版本

| 工具 | 要求 |
|---|---|
| Go | 1.27.1，参照 AX 手册安装独立 `/opt/go-1.27.1` |
| Node.js | 至少 24.13.0；本地验证使用 24.15.0 |
| Yarn | `packageManager` 固定 4.9.0；锁文件不可静默更新 |
| Python | 仓库 `.python-version` 为 3.13；使用 uv.lock |
| Docker | Engine + buildx，Linux 目标平台与 worker 架构一致 |
| Helm/kubectl | 与目标平台配套；固定工具和 chart dependency 制品 |
| 其他 | Git、GNU make、gettext/envsubst、jq、OpenSSL、pg 客户端、Python PyYAML |

```bash
sudo apt-get update
sudo apt-get install -y build-essential git curl ca-certificates jq openssl \
  gettext-base python3-venv postgresql-client
go version
node --version
corepack enable
corepack prepare yarn@4.9.0 --activate
yarn --version
uv --version
docker buildx version
```

Node、uv、Docker 等二进制按组织发行源和 SHA256 安装，版本写入发布清单。若 Node 发行物未带 corepack，先安装组织固定版本的 corepack；不把 `npm install -g ...@latest` 作为可重复发布步骤。

## 3. 编译二进制和前端

### 3.1 Go

```bash
cd "$WORK_ROOT/kagentOnAx/kagent/go"
go mod download
go mod verify
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$WORK_ROOT/bin/kagent-controller" ./core/cmd/controller
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$WORK_ROOT/bin/kagent" ./core/cli/cmd/kagent
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$WORK_ROOT/bin/kagent-adk" ./adk/cmd
go build ./harness/claude/cmd/... ./harness/codex/cmd/...
go list -deps ./... > "$RELEASE_DIR/kagent-go-deps.txt"
if grep -E 'github.com/agent-substrate/(substrate|env)' "$RELEASE_DIR/kagent-go-deps.txt"; then
  printf 'ERROR: kagent contains a direct backend dependency\n' >&2
  exit 1
fi
sha256sum "$WORK_ROOT/bin/kagent" "$WORK_ROOT/bin/kagent-controller" > "$RELEASE_DIR/kagent-binaries.sha256"
```

这一步是编译验证。controller 启动仍需要 Kubernetes、数据库、AX mTLS 和 HTTPS 配置；不要把 Linux 命令行直接执行成功当作完整平台运行。

### 3.2 UI

```bash
cd "$WORK_ROOT/kagentOnAx/kagent/ui"
yarn install --immutable
yarn typecheck
yarn test
yarn build
```

产物是 Vite 静态文件，生产使用仓库 UI 镜像中的 nginx；不是 Next.js server，不用 `next start`。`yarn dev --host 127.0.0.1` 仅用于前端开发，其 mock 模式不证明 AX 后端可用。

### 3.3 Python SDK/运行环境

```bash
cd "$WORK_ROOT/kagentOnAx/kagent/python"
uv sync --locked
uv run python -c 'import kagent.core; import kagent.api.v1alpha1.task_store_pb2'
```

所有客户端与运行镜像使用同一源码/lock。ResolveSession 和 AX Guest 六接口需要配套版本；不要混用之前安装的 Python wheel 或旧 Guest wire。

### 3.4 需要重新生成协议时

修改 Proto 才运行 `scripts/generate-runtime-contracts.sh`，其版本检查要求 protoc 31.1、protoc-gen-go 1.36.11、go-grpc 1.6.2、ES 2.13.0、Python grpc-tools 1.76.0。修改 CRD 源类型使用 controller-gen v0.19.0，更新 CRD、DeepCopy 和 Helm CRD。普通安装直接使用已提交生成物，不在生产机临时安装 latest 插件。

## 4. 构建并发布 Linux 镜像

### 4.1 正确的上下文

Go 镜像要访问 `../ax`。以下命令从 `kagent/` 执行，Go 相关 Dockerfile 的 context 是 `..`；UI/Python 保持各自子目录。

```bash
cd "$WORK_ROOT/kagentOnAx/kagent"
docker buildx build --platform linux/amd64 --push \
  -f go/Dockerfile --build-arg BUILD_PACKAGE=core/cmd/controller/main.go \
  -t "$IMAGE_REGISTRY/agent-platform/kagent-controller:$RELEASE_VERSION" \
  --metadata-file "$RELEASE_DIR/controller-build.json" ..
docker buildx build --platform linux/amd64 --push \
  -f go/Dockerfile --build-arg BUILD_PACKAGE=adk/cmd/main.go \
  -t "$IMAGE_REGISTRY/agent-platform/golang-adk:$RELEASE_VERSION" \
  --metadata-file "$RELEASE_DIR/adk-build.json" ..
docker buildx build --platform linux/amd64 --push \
  -f go/harness/claude/Dockerfile \
  -t "$IMAGE_REGISTRY/agent-platform/claude:$RELEASE_VERSION" \
  --metadata-file "$RELEASE_DIR/claude-build.json" ..
docker buildx build --platform linux/amd64 --push \
  -f go/harness/codex/Dockerfile \
  -t "$IMAGE_REGISTRY/agent-platform/codex:$RELEASE_VERSION" \
  --metadata-file "$RELEASE_DIR/codex-build.json" ..
docker buildx build --platform linux/amd64 --push -f ui/Dockerfile \
  -t "$IMAGE_REGISTRY/agent-platform/kagent-ui:$RELEASE_VERSION" \
  --metadata-file "$RELEASE_DIR/ui-build.json" ./ui
docker buildx build --platform linux/amd64 --push -f python/Dockerfile \
  -t "$IMAGE_REGISTRY/agent-platform/python-adk:$RELEASE_VERSION" \
  --metadata-file "$RELEASE_DIR/python-build.json" ./python
```

Guest 镜像由 AX 手册构建并填入 AX `guestImage`。`make build-sandbox-guest` 也从同级 AX 构建包装镜像，不再是 kagent 自带 Guest 协议。SandboxTemplate 的 workload image 是业务工具环境镜像，不是 Guest 镜像。

### 4.2 固定 digest 与构建制品

```bash
export CONTROLLER_IMAGE="$IMAGE_REGISTRY/agent-platform/kagent-controller@$(jq -r '."containerimage.digest"' "$RELEASE_DIR/controller-build.json")"
export UI_IMAGE="$IMAGE_REGISTRY/agent-platform/kagent-ui@$(jq -r '."containerimage.digest"' "$RELEASE_DIR/ui-build.json")"
export ADK_IMAGE="$IMAGE_REGISTRY/agent-platform/golang-adk@$(jq -r '."containerimage.digest"' "$RELEASE_DIR/adk-build.json")"
export CLAUDE_IMAGE="$IMAGE_REGISTRY/agent-platform/claude@$(jq -r '."containerimage.digest"' "$RELEASE_DIR/claude-build.json")"
export CODEX_IMAGE="$IMAGE_REGISTRY/agent-platform/codex@$(jq -r '."containerimage.digest"' "$RELEASE_DIR/codex-build.json")"
for image in "$CONTROLLER_IMAGE" "$UI_IMAGE" "$ADK_IMAGE" "$CLAUDE_IMAGE" "$CODEX_IMAGE"; do
  [[ "$image" == *@sha256:* ]] || exit 1
done
printf '%s\n' "$CONTROLLER_IMAGE" "$UI_IMAGE" "$ADK_IMAGE" "$CLAUDE_IMAGE" "$CODEX_IMAGE" \
  > "$RELEASE_DIR/kagent-images.txt"
```

记录基础镜像、构建工具和 lock 的版本；部分源码 Dockerfile 基础镜像浮动，生产流水线需额外锁定它们。Chart controller/UI image helper 当前按 registry/repository/tag 拼接，没有顶层 digest 参数，因此第 8 节使用 Helm post-renderer 固定部署 digest。不要把 `@sha256:...` 填进 tag 后得到错误的 `:tag` 拼接。

## 5. 准备 PostgreSQL 并执行迁移

### 5.1 建库

由 DBA 在真实 PostgreSQL 服务执行，角色/密码通过密码管理系统设置，示意 SQL：

```sql
CREATE ROLE kagent_app LOGIN;
CREATE DATABASE kagent OWNER kagent_app;
-- 连接到 kagent database 后，由有权限的 DBA 安装已提供的扩展：
CREATE EXTENSION IF NOT EXISTS vector;
```

示例采用已测试 PostgreSQL 18 + pgvector。DBA 配置 TLS、账号密码、网络、PITR、容量和连接上限。不要给应用 superuser。迁移账号需要 schema DDL 权限；完成后可以单独收回 DDL/使用独立 migration role，但需验证后续迁移授权。外部数据库必须全新且没有旧运行绑定。

### 5.2 两份 DSN 的证书路径

准备两个权限 `0600` 的文件：

| 文件 | 用途 |
|---|---|
| `$OPS_DIR/kagent-postgres-admin.dsn` | 运维机迁移使用；`sslrootcert` 是本机绝对路径，如 `/srv/agent-platform/ops/pki/postgres-ca.pem` |
| `$OPS_DIR/kagent-postgres-runtime.dsn` | Pod 使用；`sslrootcert=/run/database/ca.crt` |

格式示例：`postgresql://kagent_app:URL_ENCODED_PASSWORD@db.example.com:5432/kagent?sslmode=verify-full&sslrootcert=/run/database/ca.crt`。密码必须 URL encode；`verify-full` 的 hostname 要与数据库证书匹配。DSN 不写入 values、终端历史或 Git。

```bash
export KAGENT_POSTGRES_DATABASE_URL="$(cat "$OPS_DIR/kagent-postgres-admin.dsn")"
export KAGENT_DATABASE_VECTOR_ENABLED=true
kagent db migrate status
kagent db migrate up
kagent db migrate status > "$RELEASE_DIR/kagent-migration-status.txt"
unset KAGENT_POSTGRES_DATABASE_URL
```

确认 core 迁移为 000004 且所有启用 track 已应用。不要改旧 migration，也不要对生产运行 `down` 验证；Up/Down 仅用于隔离测试库。随后 Chart 设 `skipMigrations: true`，controller 启动只验证版本，避免把首次 DDL 混入应用滚动发布。

### 5.3 创建数据库 Secret

```bash
kubectl create namespace kagent --dry-run=client -o yaml | kubectl apply -f -
kubectl -n kagent create secret generic kagent-database \
  --from-file=url="$OPS_DIR/kagent-postgres-runtime.dsn" \
  --from-file=ca.crt="$OPS_DIR/pki/postgres-ca.pem" \
  --dry-run=client -o yaml | kubectl apply -f -
```

## 6. 证书、模型凭据和 SSO

### 6.1 AX client 与 controller HTTPS

AX 管理员交付第 1 节所列 client cert/key 和 server CA。在本机生成 controller CSR，交组织 CA 签发：

```bash
openssl req -new -newkey rsa:3072 -nodes \
  -keyout "$OPS_DIR/pki/kagent-controller.key" -out "$OPS_DIR/pki/kagent-controller.csr" \
  -subj '/CN=kagent-controller.kagent.svc' \
  -addext 'subjectAltName=DNS:kagent-controller.kagent,DNS:kagent-controller.kagent.svc,DNS:kagent-controller.kagent.svc.cluster.local' \
  -addext 'extendedKeyUsage=serverAuth'
```

获得 `kagent-controller.crt`、`kagent-controller-ca.pem` 后：

```bash
openssl verify -purpose sslserver -CAfile "$OPS_DIR/pki/kagent-controller-ca.pem" "$OPS_DIR/pki/kagent-controller.crt"
kubectl -n kagent create secret tls kagent-controller-tls \
  --cert="$OPS_DIR/pki/kagent-controller.crt" --key="$OPS_DIR/pki/kagent-controller.key" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n kagent create secret generic kagent-controller-ca \
  --from-file=ca.crt="$OPS_DIR/pki/kagent-controller-ca.pem" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n kagent create secret generic ax-server-ca \
  --from-file=ca.crt="$OPS_DIR/pki/ax-server-ca.pem" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n kagent create secret tls kagent-ax-client \
  --cert="$OPS_DIR/pki/kagent-ax-client.crt" --key="$OPS_DIR/pki/kagent-ax-client.key" --dry-run=client -o yaml | kubectl apply -f -
```

证书链按客户端要求包含中间 CA。controller CA 同时交给 MITM 平台配置它的上游信任；UI 挂载同一 CA 验证 controller。AX mTLS CA、controller CA、MITM CA 作用不同，不能任意互换。

### 6.2 模型 API Key

示例使用 OpenAI-compatible ModelConfig，实际模型名称填组织可用模型；不是推荐采购某个模型。把 key 从密码管理系统写到 `$OPS_DIR/openai-key`，不要通过 `--from-literal` 暴露在历史中：

```bash
kubectl -n kagent create secret generic kagent-openai \
  --from-file=OPENAI_API_KEY="$OPS_DIR/openai-key" --dry-run=client -o yaml | kubectl apply -f -
```

凭据由 AX 平台出站策略/provider 注入，provider 必须授权目标 namespace/运行实例；Secret 存在不等于注入成功。自建模型网关需配置对应 `providers.openAI.config.baseUrl`（先核对当前 CRD 属性），并把实际 CA/域名纳入平台验收，不能假定任意云签名认证都可由静态 header 注入实现。

### 6.3 OIDC 与公共证书

在 IdP 建立专用 client，登记精确 callback：`https://agents.example.com/oauth2/callback`，限制允许的组织/用户组。准备三份文件 `oidc-client-id`、`oidc-client-secret`、`oidc-cookie-secret`。cookie secret 为随机 32 字节的 base64 编码值：

```bash
openssl rand -base64 32 > "$OPS_DIR/oidc-cookie-secret"
kubectl -n kagent create secret generic kagent-oidc \
  --from-file=client-id="$OPS_DIR/oidc-client-id" \
  --from-file=client-secret="$OPS_DIR/oidc-client-secret" \
  --from-file=cookie-secret="$OPS_DIR/oidc-cookie-secret" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n kagent create secret tls kagent-public-tls \
  --cert="$OPS_DIR/pki/public.crt" --key="$OPS_DIR/pki/public.key" \
  --dry-run=client -o yaml | kubectl apply -f -
```

公共证书 SAN 是 `agents.example.com`；它与内部 controller 证书是两张证书。私有 IdP CA 还需挂载到 oauth2-proxy；不要打开 provider TLS skip verify。

## 7. 生产 values 与访问控制

### 7.1 渲染提供的 values 模板

仓库提供 [kagent.production.values.yaml.in](运维示例/kagent.production.values.yaml.in)。默认关闭可选 kmcp controller/工具服务以便先完成核心安装；**kmcp CRD 仍必须安装**，因为 kagent 总是注册 MCPServer discovery controller，缺少 CRD 会阻止 informer 正常启动。需要这些功能时单独启用并验收它们的权限、镜像和 CRD，不会删掉源码业务能力。

```bash
export PUBLIC_HOST=agents.example.com
export OIDC_ISSUER_URL=https://id.example.com/realms/agents
export OIDC_EMAIL_DOMAIN=example.com
export MODEL_NAME=YOUR_APPROVED_MODEL
envsubst '${IMAGE_REGISTRY} ${RELEASE_VERSION} ${PUBLIC_HOST} ${OIDC_ISSUER_URL} ${OIDC_EMAIL_DOMAIN} ${MODEL_NAME}' \
  < "$WORK_ROOT/kagentOnAx/运维示例/kagent.production.values.yaml.in" \
  > "$OPS_DIR/kagent.values.yaml"
```

建立 `registry-pull` Secret，使用组织镜像拉取凭据，避免把推送权限放入节点：

```bash
kubectl -n kagent create secret generic registry-pull \
  --type=kubernetes.io/dockerconfigjson \
  --from-file=.dockerconfigjson="$OPS_DIR/registry-pull.json" \
  --dry-run=client -o yaml | kubectl apply -f -
```

`registry-pull.json` 是只含拉取权限的 Docker config JSON，由组织密码系统提供。模板使用 `global.watchNamespaces: [kagent]` 将 watch 与 RBAC 范围对齐。以后增加 namespace 时，同时调整 kagent Role/watch、AX clients allowlist、对应 TaskGroup 和 provider 授权，不能只创建 namespace。

### 7.2 必须先做的访问隔离

公共链路：浏览器 → HTTPS Ingress → oauth2-proxy → UI nginx → controller HTTPS。不得新增一个绕过 oauth2-proxy 的公共 UI/controller Ingress。

内部 callback 链路需要单独的 L7 约束：MITM/运行实例允许调用经过运行凭据认证的 TaskStore/其他已核实 runtime 方法，**不允许凭伪造用户 JWT 调用用户业务 API**。仅按 Pod 源开放 controller:8083 会同时开放所有路径，不能满足 trusted-proxy 的信任前提。平台应在现有服务网格/反向代理实现按工作负载身份和完整 gRPC method 的规则，测试失败后拒绝放量。不要将未经核对的通配 `/kagent.*` 用作 runtime 白名单。

当前 `policy.go` 将 TaskStore 的 ResolveSession、CreateTask、GetTask、UpdateTask、SettleTask、ListTasks 六方法标为 AccessRuntime；Memory 等业务方法不是运行凭据通用放行入口，需独立的可信用户身份。按当前 `go/core/internal/grpcserver` 的 method access 分类和 `go/core/internal/httpserver/auth` 契约核对实际规则：

| 来源 | controller 允许能力 |
|---|---|
| 经 IdP 验签且清理伪造头的业务代理 | 用户 API、A2A、MCP，保留已验证 Authorization |
| AX 平台 egress/runtime | 仅明确的 runtime RPC；仍需 AX 凭据/Task UID 验证 |
| 任意普通 Pod/不可信 runtime 用户请求 | 拒绝用户 API 直连 |
| kubelet/监控 | 指定健康/监控端口和路径 |

网络策略另限制 AX 8443 仅 controller/运维访问，数据库仅 controller/migration job 访问。上述 L7 边界由站点设施提供，当前 Chart 不自动安装服务网格或完整授权策略。没有可用实现时不能把这套部署宣称为安全的生产多租户安装。

## 8. 打包 Chart、固定镜像并安装

### 8.1 只打包，不运行 kind 安装目标

```bash
cd "$WORK_ROOT/kagentOnAx/kagent"
make helm-version VERSION="$RELEASE_VERSION"
```

该目标生成 Chart.yaml、解析依赖并打包到仓库 `dist`。构建流水线应保存实际 Chart.lock、依赖 tgz 和哈希；首次 `dependency update` 解析到的依赖是发行输入，以后从审核制品使用 `helm dependency build`/缓存，避免再次解析改变依赖。

**生产不调用** `make helm-install`、`make helm-install-provider` 或 `scripts/setup-cluster/setup-cluster.sh`：现有 Makefile 的安装目标明确指向 `kind-$(KIND_CLUSTER_NAME)`，setup 脚本检查 context 后仍调用该目标。本文用带 `--kube-context` 的直接 Helm 命令避免装错集群。

### 8.2 Helm post-renderer 固定 controller/UI digest

先按 AX 手册建立 `$WORK_ROOT/tools-venv` 并安装 PyYAML。生成发布专用 renderer，输入输出均为 YAML，不记录 Secret：

```bash
cat > "$OPS_DIR/pin-images.py" <<'PY'
import os, sys, yaml
images = {'kagent-controller': os.environ['CONTROLLER_IMAGE'], 'kagent-ui': os.environ['UI_IMAGE']}
for value in images.values():
    if '@sha256:' not in value:
        raise SystemExit('Expected digest-pinned image')
docs = list(yaml.safe_load_all(sys.stdin))
seen = set()
for d in docs:
    if not isinstance(d, dict) or d.get('kind') != 'Deployment':
        continue
    name = d.get('metadata', {}).get('name')
    if name in images:
        d['spec']['template']['spec']['containers'][0]['image'] = images[name]
        seen.add(name)
if seen != set(images):
    raise SystemExit('Expected controller and UI deployments were not rendered')
yaml.safe_dump_all(docs, sys.stdout, sort_keys=False)
PY
cat > "$OPS_DIR/pin-images" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
exec "$WORK_ROOT/tools-venv/bin/python" "$OPS_DIR/pin-images.py"
SH
chmod 0700 "$OPS_DIR/pin-images"
helm template kagent helm/kagent -n kagent -f "$OPS_DIR/kagent.values.yaml" \
  --post-renderer "$OPS_DIR/pin-images" > "$RELEASE_DIR/kagent.rendered.yaml"
helm template kagent-crds helm/kagent-crds -n kagent --set kmcp.enabled=true \
  > "$RELEASE_DIR/kagent-crds.rendered.yaml"
```

oauth2-proxy/其他子 chart 镜像也应使用其 chart 支持的 digest 配置或组织固定制品，经发布清单核对；上述 renderer 只替换当前自己构建的 controller/UI，不能宣称把所有依赖都锁定了。

确认渲染结果：数据库 bundled 已关闭、没有密码明文、HTTPS volumes/探针齐全、AX endpoint 正确、没有 `workerpools.ate.dev` 权限/CRD、没有旧 Substrate chart。随后：

```bash
helm upgrade --install kagent-crds ./helm/kagent-crds \
  --kube-context "$KUBE_CONTEXT" --namespace kagent --set kmcp.enabled=true --wait --timeout 10m
kubectl wait --for=condition=Established --timeout=120s \
  crd/agents.api.kagent.dev crd/harnesses.api.kagent.dev crd/sandboxtemplates.api.kagent.dev crd/mcpservers.kagent.dev
helm upgrade --install kagent ./helm/kagent \
  --kube-context "$KUBE_CONTEXT" --namespace kagent \
  -f "$OPS_DIR/kagent.values.yaml" --post-renderer "$OPS_DIR/pin-images" \
  --wait --timeout 10m
kubectl -n kagent rollout status deployment/kagent-controller --timeout=10m
kubectl -n kagent rollout status deployment/kagent-ui --timeout=10m
kubectl -n kagent get pods,svc
```

原生资源配对失败时保留日志。不要用自动回滚假设数据库也会回滚；新装失败不应直接删除数据库/PVC。以后每次 Helm 更新都必须带相同 renderer 和对应 release digest 环境，否则可能退回 tag 镜像。

### 8.3 公共入口

在组织已有的 Ingress Controller 上创建如下 Ingress，把 `ingressClassName` 换成真实类；控制器必须支持本产品 HTTP/2、gRPC-Web、流式长连接及超时策略。不要假设安装 kagent 会安装 Ingress Controller。

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: kagent
  namespace: kagent
spec:
  ingressClassName: YOUR_INGRESS_CLASS
  tls:
    - hosts: [agents.example.com]
      secretName: kagent-public-tls
  rules:
    - host: agents.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: kagent-oauth2-proxy
                port:
                  number: 4180
```

保存为 `$OPS_DIR/kagent-ingress.yaml`，替换所有占位值，核对渲染出来的 oauth2 Service 名称后 apply。配置 DNS 指向 Ingress 地址。连接超时、buffering、最大请求大小应通过实际 streaming/cancel 和文件传输验证；不能只用首页 200 判断成功。

## 9. 启动检查与第一个 Agent

### 9.1 分层检查

```bash
kubectl -n kagent logs deployment/kagent-controller --tail=150
kubectl -n kagent logs deployment/kagent-ui --tail=80
kubectl -n kagent get deploy kagent-controller -o jsonpath='{.spec.template.spec.containers[0].image}'
kubectl auth can-i create workerpools.ate.dev \
  --as=system:serviceaccount:kagent:kagent-controller -n kagent
kubectl get crd mcpservers.kagent.dev
kubectl -n kagent get modelconfigs.api.kagent.dev
```

最后一个权限检查应为 **no**。controller 日志应无 AX TLS/SAN/PermissionDenied、DB migration pending、watch Forbidden。UI 应显示登录入口且无 Substrate 页签。使用未登录请求检查受保护业务 API 返回认证失败/跳转，不能以登录页可访问代替 API 身份边界检查。

如需在运维机检查内部 controller 健康，可端口转发并显式验证 SAN：

```bash
kubectl -n kagent port-forward svc/kagent-controller 18083:8083
# 另一个终端；不使用 curl -k。
curl --fail --cacert "$OPS_DIR/pki/kagent-controller-ca.pem" \
  --resolve kagent-controller.kagent.svc:18083:127.0.0.1 \
  https://kagent-controller.kagent.svc:18083/health
```

### 9.2 创建 Harness、AgentTemplate 和 Agent

先确认 AX `kagent-default` 已就绪。下面只替换镜像变量，避免 envsubst 意外展开模板中的业务文本：

```bash
cat > "$OPS_DIR/agent.yaml.in" <<'YAML'
apiVersion: api.kagent.dev/v1alpha3
kind: Harness
metadata:
  name: ax-adk
  namespace: kagent
spec:
  kagent: {}
  workload:
    image: ${ADK_IMAGE}
  ax:
    taskGroupRef:
      name: kagent-default
---
apiVersion: api.kagent.dev/v1alpha3
kind: AgentTemplate
metadata:
  name: ax-smoke
  namespace: kagent
spec:
  description: AX production-path smoke test
  modelConfig:
    name: default-model-config
  systemPrompt: Reply briefly and do not call external tools.
---
apiVersion: api.kagent.dev/v1alpha3
kind: Agent
metadata:
  name: ax-smoke
  namespace: kagent
spec:
  templateRef:
    name: ax-smoke
  harnessRef:
    name: ax-adk
YAML
envsubst '${ADK_IMAGE}' < "$OPS_DIR/agent.yaml.in" > "$OPS_DIR/agent.yaml"
kubectl apply --dry-run=server -f "$OPS_DIR/agent.yaml"
kubectl apply -f "$OPS_DIR/agent.yaml"
kubectl -n kagent get harness ax-adk -o yaml
kubectl -n kagent get agent ax-smoke -o yaml
```

确认 Chart 实际创建的 ModelConfig 名称是 `default-model-config`，否则修改引用。条件应对应当前 observedGeneration；首次准备需要 golden 生成，耗时取决于真实后端，不用不断 delete/recreate 缩短等待。准备失败检查 AX PreparedRuntime、worker、镜像、模型 Secret 和出站策略。

登录 UI 后选择 `ax-smoke`，创建 Session，发送一条文本并确认返回；继续发第二条，检查历史；等待静默后继续；创建 Checkpoint，再 Fork 并继续；删除测试 Session。保存业务 Session ID、AX Task UID、Checkpoint ref 和 trace，确认没有沿用源 Fork 的运行身份。

### 9.3 其他三类 Harness

复制 Harness 清单，一次只设置一种 adapter：

| 类型 | spec adapter | 镜像/准备 |
|---|---|---|
| Go ADK | `kagent: {}` | 本次 `$ADK_IMAGE` |
| Claude | `claude: {}` | 本次 `$CLAUDE_IMAGE`，匹配的 Anthropic ModelConfig/凭据 |
| Codex | `codex: {}` | 本次 `$CODEX_IMAGE`，支持的模型配置/凭据 |
| BYO | `byo: {}` | 自有 A2A 镜像 digest；按其实际入口和环境配置 command/env |

不能在同一 Harness 同时设置两个 adapter。BYO 必须实现配套 A2A/TaskStore 生命周期契约；一个任意 Web 服务镜像不等价于兼容 Harness。完整四类验收使用第 11 节入口。

### 9.4 Sandbox

准备含需要工具的不可变 Linux workload 镜像（不是 AX Guest），将 digest 填为 `SANDBOX_WORKLOAD_IMAGE`：

```bash
export SANDBOX_WORKLOAD_IMAGE=registry.example.com/agent-platform/tools@sha256:REPLACE_64_HEX
cat > "$OPS_DIR/sandbox.yaml.in" <<'YAML'
apiVersion: api.kagent.dev/v1alpha3
kind: SandboxTemplate
metadata:
  name: tools
  namespace: kagent
spec:
  workload:
    image: ${SANDBOX_WORKLOAD_IMAGE}
  ax:
    taskGroupRef:
      name: kagent-default
YAML
envsubst '${SANDBOX_WORKLOAD_IMAGE}' < "$OPS_DIR/sandbox.yaml.in" > "$OPS_DIR/sandbox.yaml"
kubectl apply --dry-run=server -f "$OPS_DIR/sandbox.yaml"
kubectl apply -f "$OPS_DIR/sandbox.yaml"
kubectl -n kagent get sandboxtemplate tools -o yaml
```

通过已认证的业务客户端创建 Sandbox，验证进程启动、输出/退出码、等待、终止、上传和下载，检查文件限制、TTL、删除引用保护。不要通过直接访问后端 Guest 地址来替代 AX TaskExecutionService 验收。

## 10. CLI、SDK、MCP 与本地调试

### 10.1 CLI 已有能力与认证限制

`kagent --help`、`kagent session create --help`、`kagent sandbox --help` 给出当前真实参数。它支持 `--api-url`、`--gateway-url`、`--ca-file`、`--server-name`、`--namespace`、`--user-id`。但当前 CLI 没有完整 OIDC token 传递入口，所以不能把下面独立测试实例的命令照搬到生产 trusted-proxy 入口并期望通过。

**仅在独立隔离测试安装、明确使用测试身份机制时**可验证 CLI 流程（生产不得为此切回 insecure）：

```bash
kagent --api-url https://test-kagent.example.com \
  --gateway-url https://test-kagent.example.com --ca-file /path/to/test-ca.pem \
  --namespace kagent session create --agent ax-smoke --request-id smoke-session-001
# 取返回的真实业务 Session ID，不是 AX Task UID。
kagent --api-url https://test-kagent.example.com \
  --gateway-url https://test-kagent.example.com --ca-file /path/to/test-ca.pem \
  invoke --session SESSION_UUID --task 'Reply with OK' --stream
kagent --api-url https://test-kagent.example.com --ca-file /path/to/test-ca.pem \
  sandbox create tools --request-id sandbox-smoke-001 --ttl 10m
```

对生产，使用支持已验证 IdP token 的业务 API 客户端，或由平台交付并验收的认证入口。原生 gRPC 与浏览器 gRPC-Web 是不同传输；oauth2-proxy/UI 链路通过浏览器验收，不自动意味着原生 CLI gRPC 也能穿透它。**CLI OIDC 能力属于当前生产接入缺口**，不能用手册伪装成已经支持。

### 10.2 Go/Python SDK 与 MCP

Go SDK 在原 module 下编译，依赖同级 AX module；外部消费者需要发布配套 AX 模块或保留明确 replacement，不能把工作区依赖当成公开上游版本。调用者可使用标准 gRPC metadata/transport 传递已认证身份，平台入口仍负责验签。Python runtime 使用同源码的 `kagent-core` 和 `kagent-proto`，TaskStore 不接受旧 Actor 身份头。

MCP 业务入口必须经过组织认证代理，保留 streaming/cancel。不要把模型 token、AX admin client key 或数据库口令配置给浏览器/MCP 普通用户。AX mTLS 只存在 controller 与 AX 之间，浏览器不直接查询 AX。

### 10.3 Linux 本地调试连接真实测试平台

运行 controller 前需要匹配 Kubernetes kubeconfig/watch 权限、真实测试 DB、AX mTLS 和业务 HTTPS 文件。环境变量至少包括：

```text
KAGENT_NAMESPACE=kagent
KAGENT_WATCH_NAMESPACES=kagent
KAGENT_POSTGRES_DATABASE_URL_FILE=/absolute/path/database-url
KAGENT_DATABASE_VECTOR_ENABLED=true
KAGENT_SKIP_MIGRATIONS=true
KAGENT_AX_ENDPOINT=<可达的 AX managed TLS 地址>
KAGENT_AX_SERVER_NAME=ax-server.ax-system.svc
KAGENT_AX_CA_FILE=/absolute/path/ax-ca.pem
KAGENT_AX_CLIENT_CERT_FILE=/absolute/path/client.crt
KAGENT_AX_CLIENT_KEY_FILE=/absolute/path/client.key
KAGENT_API_TLS_CERT_FILE=/absolute/path/controller.crt
KAGENT_API_TLS_KEY_FILE=/absolute/path/controller.key
KAGENT_GATEWAY_URL=https://<真实可从测试运行环境访问的 controller 地址>:8083
KAGENT_AUTH_MODE=trusted-proxy
```

这些必须与 AX callbackURL、证书 SAN 和平台入口一致。`kubectl port-forward` 只解决本机到集群方向；Agent 不能访问运维机的 localhost。若没有双向 DNS/路由/证书配置，使用 Kubernetes 测试 namespace 部署 controller，而不是编造 localhost callback。不要让本地 controller 与生产 controller 同时操作同一数据库和命名空间。

## 11. 测试与验收

### 11.1 本地代码回归

先启动独立测试 PostgreSQL/Redis，安装 envtest 与配套 Python 环境；DSN 账号需能创建随机测试 database。不得指向生产数据库。

```bash
export KAGENT_TEST_POSTGRES_DSN='postgres://TEST_USER@127.0.0.1:TEST_PORT/postgres?sslmode=disable'
export AX_TEST_REDIS_ADDR=127.0.0.1:TEST_REDIS_PORT
export KUBEBUILDER_ASSETS=/path/to/envtest/bin
export KAGENT_TEST_PYTHON=/path/to/paired/python
cd "$WORK_ROOT/kagentOnAx/kagent/go"
env -u ANTHROPIC_AUTH_TOKEN -u ANTHROPIC_API_KEY \
  go test -p 2 ./adk/... ./harness/... ./api/... ./core/internal/... ./core/pkg/... ./core/cli/... \
  -count=1 -timeout=240s
cd ../ui
yarn typecheck
yarn test
yarn build
```

不使用 `-short` 把真实存储测试跳过后称通过；环境不足明确记为未执行。记录中此前通过的 539 项 UI、14 项 Python、365 项 Helm 和 6 项浏览器测试不是当前新集群的验收证据。

### 11.2 真实集群 release gate

`scripts/verify-ax-cluster.sh` 会创建/删除测试业务资源，部分 E2E 会重启 controller。**只能在与生产同配置的独立预生产/可丢弃集群运行，不能直接对生产 namespace 跑整个套件。** 它不负责安装 Substrate/AX，也不补齐前文外部身份和 provider 设施。

按脚本要求设置：独立 `KAGENT_E2E_KUBE_CONTEXT`、HTTPS API URL、CA、AX mTLS 文件、四类测试 runtime image digest，以及测试模型/观测配置。现有原生客户端身份限制意味着不能假设直接在生产 OIDC 入口执行套件；需要隔离测试身份入口，并另行验证生产认证边界。

```bash
cd "$WORK_ROOT/kagentOnAx/kagent"
# 所有 KAGENT_E2E_* / KAGENT_AX_* 参数按脚本顶部要求配置。
scripts/verify-ax-cluster.sh
```

分开保存以下结果：

| 验收层 | 必须验证 |
|---|---|
| 安装 | image digest、DB migration、Secret mounts、RBAC、AX Group UID |
| 业务 | 四类 Harness、输入继续、终态静默、Session 历史、Checkpoint/Fork、分享、ScheduledRun |
| Sandbox | 六接口、退出码、流取消、路径/大小限制、TTL、删除 |
| 身份 | OIDC 正反例、runtime 凭据撤销、伪造头拒绝、不能绕过代理、不同业务分区 |
| 原生平台 | golden、DATA/FULL restore、实际 worker 放置、gVisor/MicroVM |
| 故障 | 响应丢失/迟到操作、claim 失效、Redis/PG/对象存储故障、联合备份恢复 |

## 12. 日常运维、停机和问题定位

### 12.1 健康与日志

```bash
kubectl -n kagent get pods,events --sort-by=.metadata.creationTimestamp
kubectl -n kagent logs deployment/kagent-controller --since=10m
kubectl -n ax-system logs deployment/ax-server --since=10m
kubectl -n kagent get harnesses,agents,sandboxtemplates
helm --kube-context "$KUBE_CONTEXT" status kagent -n kagent
```

采集业务操作 ID、Session ID、AX Task UID 和时间；日志输出前清理 token/DSN/模型 key。监控 DB 池、AX 连接、准备失败、Session 不可静默、TTL 积压和证书有效期。`/health` 成功不证明模型请求或快照恢复成功。

### 12.2 停机、证书轮换与升级

先关闭新业务入口，等待或明确取消正在运行的任务，并通过正常业务生命周期形成可恢复边界；检查未决操作后再停止 controller/AX。不要直接杀 worker 当作静默。证书/CA 轮换保持双根过渡、重新建立 TLS 连接并做负向测试；部分 server 配置在启动时加载，需要重启。

升级前备份 kagent DB、AX ledger/epoch、Substrate DB、对象存储和 PKI；同一发布单元升级 controller/UI/CLI/SDK/Harness/AX。回滚前确认迁移和运行协议能回退；Helm rollback 不会自动恢复数据库或快照。新安装范围不提供旧实例接管脚本。

### 12.3 常见故障

| 现象 | 检查项 |
|---|---|
| controller CrashLoop | DB DSN/CA/迁移、业务 TLS 配对文件、AX mTLS、namespace 权限 |
| Helm 渲染报 legacy runtime values | 先用 AX `convert-kagent`/`convert-workerpools` 转换旧配置，不能只删未知字段 |
| UI 能打开但 API 401/403 | OIDC token 验证与转发、private/public 路径分流、userIdClaim、controller trusted-proxy |
| AX TaskGroup 下拉为空 | kagent 授权接口、namespace、AX allowlist、组是否存在；不让浏览器直连 AX |
| Harness 长期不 Ready | observedGeneration、PreparedRuntime、真实 golden、组容量/UID、镜像和出站凭据 |
| TaskStore Unauthenticated | AX credential provider、callback CA、MITM 注入、Task UID 与 Session binding |
| native CLI 在生产失败 | 当前没有 OIDC token 通用选项、gRPC 与 gRPC-Web 入口差异；不是使用 `--user-id` 可解决 |
| Fork/删除 FailedPrecondition | 原边界/引用/未决操作；保留原 request-id，按 AX 恢复流程核查 |
| worker 空闲仍不调度 | AX/Substrate 容量状态、节点版本 label、SandboxConfig、运行资源规格 |

## 13. 配置转换与文档交接

旧安装配置可通过 AX `convert-kagent --format resources|values` 和 `convert-workerpools` 离线转换，详细参数见 [AX运行时安装与验收.md](AX运行时安装与验收.md)。不转换活实例，不迁移历史运行状态。非默认 WorkerPool 配置输出给 AX 平台，不放回 kagent values。

交付包应包含：两仓库源版本/patch、生成文件哈希、Linux 二进制、所有 image digest、Chart/lock/dependency tgz、已脱敏环境 values/渲染清单、迁移状态、PKI/Secret 管理索引、TaskGroup refs、真实测试报告和恢复演练记录。Secret 内容通过独立密码管理系统交付。

相关源码依据：`go/go.mod`、`go/core/pkg/app`、`go/core/internal/axruntime`、`go/core/cli/internal/connection`、`helm/kagent/values.yaml`、`helm/kagent/templates`、`docs/architecture/oidc-proxy-authentication.md`。这些是当前配套分支的实际约束，不能用旧上游 README 的 Substrate 安装命令替代。
