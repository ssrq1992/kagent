#!/usr/bin/env bash
# Explicit release acceptance against a disposable, paired installation.
# This changes test business resources and restarts the test controller.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${KAGENT_E2E_KUBE_CONTEXT:?Dedicated test cluster context is required}"
: "${KAGENT_E2E_API_URL:?Controller HTTPS endpoint is required}"
: "${SSL_CERT_FILE:?Controller CA PEM file is required}"
: "${KAGENT_AX_ENDPOINT:?AX managed endpoint is required}"
: "${KAGENT_AX_CA_FILE:?AX CA PEM file is required}"
: "${KAGENT_AX_CLIENT_CERT_FILE:?AX client certificate is required}"
: "${KAGENT_AX_CLIENT_KEY_FILE:?AX client key is required}"
: "${KAGENT_E2E_RUNTIME_IMAGE:?Digest-pinned paired Go runtime image is required}"
: "${KAGENT_E2E_BYO_IMAGE:?Digest-pinned BYO image is required}"
: "${KAGENT_E2E_CLAUDE_IMAGE:?Digest-pinned Claude runtime image is required}"
: "${KAGENT_E2E_CODEX_IMAGE:?Digest-pinned Codex runtime image is required}"
[[ "$KAGENT_E2E_API_URL" == https://* ]] || { echo 'Controller must use HTTPS' >&2; exit 1; }
for image in "$KAGENT_E2E_RUNTIME_IMAGE" "$KAGENT_E2E_BYO_IMAGE" "$KAGENT_E2E_CLAUDE_IMAGE" "$KAGENT_E2E_CODEX_IMAGE"; do
 [[ "$image" == *@sha256:* ]] || { echo 'Runtime images must be pinned by digest' >&2; exit 1; }
done
[[ "$(kubectl config current-context)" == "$KAGENT_E2E_KUBE_CONTEXT" ]] || { echo 'Active Kubernetes context differs; select the dedicated test context first' >&2; exit 1; }
export KAGENT_API_URL="$KAGENT_E2E_API_URL" KAGENT_GATEWAY_URL="$KAGENT_E2E_API_URL"
mkdir -p artifacts/ax-cluster-acceptance
make -C go core/bin/kagent-local
export KAGENT_E2E_CLI="$PWD/go/core/bin/kagent-local"
for manifest in lifecycle tracing; do
 envsubst < "go/core/test/e2e/manifests/$manifest.yaml.tmpl" | kubectl apply -f -
done
(cd go && go test ./core/test/e2e -v -count=1 -parallel "${E2E_PARALLEL:-4}" -timeout=30m) 2>&1 | tee artifacts/ax-cluster-acceptance/tests.log
