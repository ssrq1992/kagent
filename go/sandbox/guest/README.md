# AX sandbox Guest image

The executable and pinned private Guest protocol are owned by the sibling AX
module (`ax/cmd/ax-sandbox-guest`). kagent's CLI, SDK and MCP use AX's public
`TaskExecutionService`; kagent does not import the private Guest library.

Build from the paired layout with `make build-sandbox-guest`. The Docker build
context is the parent directory containing `kagent/` and `ax/`. Configure the
resulting immutable image digest as AX's operator `guestImage`, not a kagent Helm
value. AX mounts `/usr/local/bin/ax-sandbox-guest` from that image into the chosen
Sandbox workload and invokes it on port 80 with workspace `/data/workspace` and
logs `/data/guest-logs`.

AX exposes six process/file operations and validates the caller's mTLS identity,
Task UID and runtime kind. The underlying Guest listener stays private; network
access to it is a platform responsibility. No old Guest wire compatibility is
provided to kagent clients: deploy the paired controller, CLI and SDK together.

Run `go test ./core/internal/service/sandbox` from `kagent/go` with a real test
PostgreSQL instance (`KAGENT_TEST_POSTGRES_DSN`) to exercise kagent API → AX mTLS
→ Guest, including file transfer, limits, process exit and stream cancellation.
`AX_TEST_RUNTIME_BINARY` may point to the locally built AX test fixture to avoid
rebuilding it per test. Native golden snapshots, VM restores, placement and TTL
survival across actual worker failure remain cluster acceptance tests.

Guest process identities are in memory. A process start is not idempotent; do not
retry an ambiguous StartProcess response. Interrupted writes can leave a partial
file. Workspace persistence does not imply process persistence or exactly-once
execution. AX owns the backend mapping and documents these limits in its execution
API; kagent preserves them in its CLI/SDK/MCP behavior.
