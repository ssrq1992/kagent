#!/usr/bin/env bash
# Install kagent on an operator-provisioned AX test platform. This script does
# not install a different backend version or create backend resources itself.
set -euo pipefail
cd "$(dirname "$0")/../.."
: "${KAGENT_INSTALL_CONTEXT:?Explicit disposable Kubernetes context is required}"
: "${KAGENT_AX_VALUES_FILE:?AX/TLS/controller image values file is required}"
[[ "$(kubectl config current-context)" == "$KAGENT_INSTALL_CONTEXT" ]] || { echo 'Select the dedicated installation context first' >&2; exit 1; }
test -f "$KAGENT_AX_VALUES_FILE"
test -f ../ax/pkg/apis/v1alpha1/execution.proto
kubectl get secret kagent-ax-client ax-server-ca kagent-controller-tls kagent-controller-ca -n "${NAMESPACE:-kagent}" >/dev/null
# Helm validates legacy keys and all mounted TLS configuration. Capacity and
# PreparedRuntime ownership remain exclusively in AX.
make helm-install-provider KAGENT_HELM_EXTRA_ARGS="-f $KAGENT_AX_VALUES_FILE ${KAGENT_HELM_EXTRA_ARGS:-}"
kubectl -n "${NAMESPACE:-kagent}" rollout status deployment/kagent-controller --timeout=5m
kubectl -n "${NAMESPACE:-kagent}" rollout status deployment/kagent-ui --timeout=5m
