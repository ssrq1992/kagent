// Package axruntime implements kagent's typed AX control/data-plane boundary.
// No backend resource names, addresses, or protocol messages belong here.
package axruntime

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

type Config struct {
	Endpoint       string
	CAFile         string
	ClientCertFile string
	ClientKeyFile  string
	ServerName     string
	DialTimeout    time.Duration
}
type Client struct {
	ax.AXClient
	Execution  ax.TaskExecutionServiceClient
	connection *grpc.ClientConn
}

func TLSConfig(cfg Config) (*tls.Config, error) {
	if cfg.ClientCertFile == "" || cfg.ClientKeyFile == "" {
		return nil, fmt.Errorf("AX client certificate and key are required")
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.ServerName}
	if cfg.CAFile != "" {
		raw, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read AX CA: %w", err)
		}
		tlsConfig.RootCAs = x509.NewCertPool()
		if !tlsConfig.RootCAs.AppendCertsFromPEM(raw) {
			return nil, fmt.Errorf("AX CA contains no certificates")
		}
	}
	// Load once to reject a broken installation before dialing, then reload on
	// each handshake so certificate rotation does not require a process restart.
	if _, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile); err != nil {
		return nil, fmt.Errorf("load AX client identity: %w", err)
	}
	tlsConfig.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
		cert, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
		return &cert, err
	}
	return tlsConfig, nil
}
func Dial(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("AX endpoint is required")
	}
	tlsConfig, err := TLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(cfg.Endpoint, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	if err != nil {
		return nil, err
	}
	timeout := cfg.DialTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			break
		}
		if state == connectivity.Shutdown || !conn.WaitForStateChange(dialCtx, state) {
			_ = conn.Close()
			return nil, fmt.Errorf("connect to AX: %w", dialCtx.Err())
		}
	}
	return &Client{AXClient: ax.NewAXClient(conn), Execution: ax.NewTaskExecutionServiceClient(conn), connection: conn}, nil
}
func (c *Client) Close() error { return c.connection.Close() }

// Connection also carries A2A calls through the shared AX TaskGateway.
func (c *Client) Connection() grpc.ClientConnInterface { return c.connection }
func TaskContext(ctx context.Context, ref *ax.ResourceRef) (context.Context, error) {
	if err := ax.ValidateRef(ref, true); err != nil {
		return nil, err
	}
	incoming, _ := metadata.FromOutgoingContext(ctx)
	md := metadata.MD{}
	for _, key := range []string{"traceparent", "tracestate", "a2a-extensions", "a2a-version", "x-kagent-dispatch-id"} {
		if values := incoming.Get(key); len(values) > 0 {
			md.Set(key, values...)
		}
	}
	md.Set(ax.TargetTaskHeader, ref.Atespace+"/"+ref.Name)
	md.Set(ax.TargetTaskUIDHeader, ref.Uid)
	return metadata.NewOutgoingContext(ctx, md), nil
}

// Binding is the complete persisted runtime identity. It contains only AX refs.
// Operations use the owning business claim ID, stable across transport retries.
type Binding struct {
	Task        *ax.ResourceRef
	Group       *ax.ResourceRef
	Runtime     *ax.ResourceRef
	RestoreFrom *ax.ResourceRef
}

func (c *Client) Create(ctx context.Context, b Binding, requestID string) (*ax.Task, error) {
	if b.Task == nil {
		return nil, fmt.Errorf("task name is required")
	}
	return c.CreateTask(ctx, &ax.CreateTaskRequest{RequestId: requestID, Task: &ax.Task{Metadata: &ax.ObjectMeta{Name: b.Task.Name, Atespace: b.Task.Atespace}, Spec: &ax.TaskSpec{GroupRef: b.Group, PreparedRuntimeRef: b.Runtime, RestoreFrom: b.RestoreFrom}}})
}
func (c *Client) Resume(ctx context.Context, ref *ax.ResourceRef, operationID string) (*ax.Task, error) {
	return c.ResumeTask(ctx, &ax.ResumeTaskRequest{Atespace: ref.GetAtespace(), Name: ref.GetName(), ExpectedUid: ref.GetUid(), OperationId: operationID})
}
func (c *Client) Suspend(ctx context.Context, ref *ax.ResourceRef, operationID string) (*ax.Task, error) {
	return c.SuspendTask(ctx, &ax.SuspendTaskRequest{Atespace: ref.GetAtespace(), Name: ref.GetName(), ExpectedUid: ref.GetUid(), OperationId: operationID})
}
func (c *Client) Delete(ctx context.Context, ref *ax.ResourceRef, operationID string) error {
	_, err := c.DeleteTask(ctx, &ax.DeleteTaskRequest{Atespace: ref.GetAtespace(), Name: ref.GetName(), ExpectedUid: ref.GetUid(), OperationId: operationID})
	return err
}
