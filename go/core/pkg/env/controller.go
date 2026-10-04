package env

const (
	AuthModeInsecure     = "insecure"
	AuthModeTrustedProxy = "trusted-proxy"
)

var (
	AuthMode = RegisterStringVar(
		"KAGENT_AUTH_MODE", AuthModeInsecure,
		"Controller authentication mode: insecure or trusted-proxy. trusted-proxy requires an upstream credential-validating proxy and network isolation preventing bypass.", ComponentController,
	)
	AuthUserIDClaim = RegisterStringVar(
		"KAGENT_AUTH_USER_ID_CLAIM", "",
		"JWT claim used for the caller identity in trusted-proxy mode. Empty uses sub; a missing or empty custom claim falls back to sub.", ComponentController,
	)
	HTTPBindAddress = RegisterStringVar(
		"KAGENT_HTTP_BIND_ADDRESS", ":8083",
		"Listen address for the controller HTTP, gRPC, A2A, and MCP server.", ComponentController,
	)
	GRPCReflection = RegisterBoolVar(
		"KAGENT_GRPC_REFLECTION", false,
		"Enable gRPC server reflection on the controller.", ComponentController,
	)
	WatchNamespaces = RegisterStringVar(
		"KAGENT_WATCH_NAMESPACES", "",
		"Comma-separated namespaces to watch. Empty watches all namespaces.", ComponentController,
	)
)

// PATCH(local-env): these definitions are referenced by app.go (AX mTLS dial and
// the HTTPS callback server) but were missing from the pushed commit — the Helm
// chart (controller-configmap KAGENT_AX_* / KAGENT_API_TLS_* keys) and the ops
// manual both document them, so the definitions below restore the intended
// registration. Likely an uncommitted local change lost upstream.
var (
	APITLSCertFile = RegisterStringVar(
		"KAGENT_API_TLS_CERT_FILE", "",
		"TLS certificate file for the controller HTTPS server (AX TaskStore callbacks and browser/API proxy traffic).", ComponentController,
	)
	APITLSKeyFile = RegisterStringVar(
		"KAGENT_API_TLS_KEY_FILE", "",
		"TLS key file for the controller HTTPS server.", ComponentController,
	)
	AXEndpoint = RegisterStringVar(
		"KAGENT_AX_ENDPOINT", "",
		"AX managed-runtime gRPC endpoint, e.g. dns:///ax-server.ax-system.svc:8443.", ComponentController,
	)
	AXServerName = RegisterStringVar(
		"KAGENT_AX_SERVER_NAME", "",
		"Expected TLS server name (SAN) of the AX endpoint.", ComponentController,
	)
	AXCAFile = RegisterStringVar(
		"KAGENT_AX_CA_FILE", "",
		"CA bundle file that verifies the AX server certificate.", ComponentController,
	)
	AXClientCertFile = RegisterStringVar(
		"KAGENT_AX_CLIENT_CERT_FILE", "",
		"Client certificate file for the AX mTLS connection.", ComponentController,
	)
	AXClientKeyFile = RegisterStringVar(
		"KAGENT_AX_CLIENT_KEY_FILE", "",
		"Client key file for the AX mTLS connection.", ComponentController,
	)
)

// Shared settings read by logging and Kubernetes libraries.
var (
	LogLevel   = RegisterStringVar("KAGENT_LOG_LEVEL", "info", "Logging level for the controller, CLI, and Go/Python runtimes, including the Python ADK HTTP server: debug, info, warn, or error. Python also accepts standard Python logging levels.", ComponentController, ComponentCLI, ComponentAgentRuntime)
	Kubeconfig = RegisterStringVar("KUBECONFIG", "", "Kubernetes client configuration file list for the controller, CLI Kubernetes operations, and tests. When unset, client-go uses its normal in-cluster or user kubeconfig discovery.", ComponentController, ComponentCLI, ComponentTesting)
)
