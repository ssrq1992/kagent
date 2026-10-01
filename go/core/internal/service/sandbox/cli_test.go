package sandbox_test

import (
	"bytes"
	"context"
	"github.com/kagent-dev/kagent/go/core/cli"
	"github.com/kagent-dev/kagent/go/core/internal/grpcserver"
	"github.com/kagent-dev/kagent/go/core/internal/service/system"
	"github.com/stretchr/testify/require"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSandboxCLIWithAXGuest(t *testing.T) {
	service, ctx, _ := guestFixture(t)
	instance, err := service.Create(ctx, createRequest())
	require.NoError(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server, err := grpcserver.New(grpcserver.Config{Listener: listener, Authenticator: testAuth{}, SystemService: system.NewService(nil, nil, nil, nil), SandboxService: service})
	require.NoError(t, err)
	serverCtx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Start(serverCtx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("server did not stop")
		}
	})
	run := func(args ...string) (string, string, error) {
		cmd := cli.Root()
		var stdout, stderr bytes.Buffer
		cmd.SetOut(&stdout)
		cmd.SetErr(&stderr)
		cmd.SetArgs(append([]string{"--api-url", "http://" + listener.Addr().String(), "--user-id", "alice", "sandbox"}, args...))
		err := cmd.ExecuteContext(t.Context())
		return stdout.String(), stderr.String(), err
	}
	dir := t.TempDir()
	input, output := filepath.Join(dir, "input.txt"), filepath.Join(dir, "output.txt")
	require.NoError(t, os.WriteFile(input, []byte("hello"), 0600))
	_, _, err = run("upload", instance.Id, input, "input.txt")
	require.NoError(t, err)
	out, stderr, err := run("exec", instance.Id, "--cwd", "", "--", "sh", "-c", "tr a-z A-Z < input.txt > output.txt; printf done; printf warning >&2; exit 7")
	var exitError interface{ ExitCode() int }
	require.ErrorAs(t, err, &exitError)
	require.Equal(t, 7, exitError.ExitCode())
	require.Equal(t, "done", out)
	require.Contains(t, stderr, "warning")
	_, _, err = run("download", instance.Id, "output.txt", output)
	require.NoError(t, err)
	data, err := os.ReadFile(output)
	require.NoError(t, err)
	require.Equal(t, "HELLO", string(data))
}
