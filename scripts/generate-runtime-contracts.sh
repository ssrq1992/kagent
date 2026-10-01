#!/usr/bin/env bash
# Generate changed runtime contracts from source using pinned local tools.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${PROTO_TOOLS:?Directory with python/, kagent-proto/ and npm/ pinned generators}"
[[ "$("$PROTO_TOOLS/kagent-proto/protoc-gen-go" --version)" == "protoc-gen-go v1.36.11" ]]
[[ "$("$PROTO_TOOLS/kagent-proto/protoc-gen-go-grpc" --version)" == "protoc-gen-go-grpc 1.6.2" ]]
export PYTHONPATH="$PROTO_TOOLS/python${PYTHONPATH:+:$PYTHONPATH}"
[[ "$(python3 -m grpc_tools.protoc --version)" == "libprotoc 31.1" ]]
[[ "$(node -p "require('$PROTO_TOOLS/npm/node_modules/@bufbuild/protoc-gen-es/package.json').version")" == "2.13.0" ]]
descriptors=$(mktemp)
trap 'rm -f "$descriptors"' EXIT
(cd go && go run ./scripts/protodescriptors "$descriptors")
python3 -m grpc_tools.protoc -Iproto --descriptor_set_in="$descriptors" \
 --plugin="protoc-gen-go=$PROTO_TOOLS/kagent-proto/protoc-gen-go" \
 --plugin="protoc-gen-go-grpc=$PROTO_TOOLS/kagent-proto/protoc-gen-go-grpc" \
 --go_out=paths=source_relative:go/api/gen \
 --go_opt=Ma2a.proto=github.com/a2aproject/a2a-go/v2/a2apb/v1 \
 --go-grpc_out=paths=source_relative:go/api/gen \
 --go-grpc_opt=Ma2a.proto=github.com/a2aproject/a2a-go/v2/a2apb/v1 \
 proto/kagent/api/v1alpha1/system.proto proto/kagent/api/v1alpha1/task_store.proto proto/kagent/api/v1alpha1/sandboxes.proto
python3 -m grpc_tools.protoc -Iproto --descriptor_set_in="$descriptors" \
 --python_out=python/packages/kagent-proto/src \
 --pyi_out=python/packages/kagent-proto/src \
 --grpc_python_out=python/packages/kagent-proto/src \
 proto/kagent/api/v1alpha1/task_store.proto proto/kagent/api/v1alpha1/memory.proto
python3 -m grpc_tools.protoc -Iproto --descriptor_set_in="$descriptors" \
 --plugin="protoc-gen-es=$PROTO_TOOLS/npm/node_modules/.bin/protoc-gen-es" \
 --es_out=target=ts:ui/src/generated \
 proto/kagent/api/v1alpha1/system.proto proto/kagent/api/v1alpha1/sandboxes.proto
