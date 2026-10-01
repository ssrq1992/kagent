package e2e_test

import (
	"context"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/kagent-dev/kagent/go/core/internal/axruntime"
	env "github.com/kagent-dev/kagent/go/core/pkg/env"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
	"time"
)

// Cluster acceptance uses AX's managed mTLS API, never backend inventory.
func newAXRuntimeClient(t *testing.T) *axruntime.Client {
	t.Helper()
	client, err := axruntime.Dial(t.Context(), axruntime.Config{Endpoint: env.AXEndpoint.Get(), CAFile: env.AXCAFile.Get(), ClientCertFile: env.AXClientCertFile.Get(), ClientKeyFile: env.AXClientKeyFile.Get(), ServerName: env.AXServerName.Get(), DialTimeout: 10 * time.Second})
	require.NoError(t, err, "cluster acceptance requires AX mTLS configuration")
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client
}
func findAXTask(ctx context.Context, client ax.AXClient, namespace, name string) (*ax.Task, error) {
	task, err := client.GetTask(ctx, &ax.GetTaskRequest{Atespace: namespace, Name: name, RefreshRuntime: true})
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	return task, err
}
