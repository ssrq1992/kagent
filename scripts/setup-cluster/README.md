# Install the paired AX version

Use sibling `kagent/` and `ax/` checkouts from the same migration release. Build Go
images with the **parent directory** as context (`make build-controller`, etc.).
The AX Guest executable is built by the paired AX module.

Provision the backend at AX's locked revision, persistent Redis, AX managed
configuration, TLS secrets and TaskGroups using the installation procedure in
`kagentOnAx/软件实现.md`. kagent does not install or manage the backend.

On a disposable test cluster, supply a Helm values file containing the paired
controller/runtime image digests, `controller.ax`, and `controller.tls`. Create
`kagent-ax-client`, `ax-server-ca`, `kagent-controller-tls`, and
`kagent-controller-ca` in the kagent namespace (or install with Helm directly for
custom secret names). Then run:

```sh
KAGENT_INSTALL_CONTEXT=your-test-context \
KAGENT_AX_VALUES_FILE=/absolute/path/to/ax-values.yaml \
  scripts/setup-cluster/setup-cluster.sh
```

This is an explicit installation command; it has **not** been executed as part of
local implementation validation. It no longer bootstraps the old backend fork.
For release acceptance, use `scripts/verify-ax-cluster.sh` on a dedicated cluster.
It creates test resources and some tests restart the test controller; do not run
against a production installation. The runner must have all four digest-pinned
Harness images, SandboxTemplate capacity, AX mTLS credentials, HTTPS trust, a
reachable mock-model/OTLP endpoint and matching kagent/AX server builds.
