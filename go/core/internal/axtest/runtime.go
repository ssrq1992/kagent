package axtest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"testing"
	"time"

	"github.com/kagent-dev/kagent/go/core/internal/axruntime"
	"github.com/stretchr/testify/require"
)

// Start launches the real AX transport in its own module so tests never import
// the private backend protocol. Compute placement remains a controlled fixture.
func Start(t *testing.T) *axruntime.Client {
	t.Helper()
	binary := os.Getenv("AX_TEST_RUNTIME_BINARY")
	if binary == "" {
		binary = filepath.Join(t.TempDir(), "ax-managed-test-runtime")
		_, source, _, _ := goruntime.Caller(0)
		build := exec.Command(filepath.Join(goruntime.GOROOT(), "bin", "go"), "build", "-o", binary, "./internal/testfixture/cmd/managed-runtime")
		build.Dir = filepath.Clean(filepath.Join(filepath.Dir(source), "../../../../../ax"))
		output, err := build.CombinedOutput()
		require.NoError(t, err, fmt.Sprintf("build AX fixture: %s", output))
	}
	command := exec.Command(binary, "--root", t.TempDir())
	command.Stderr = os.Stderr
	pipe, err := command.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, command.Start())
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	t.Cleanup(func() {
		_ = command.Process.Signal(os.Interrupt)
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			_ = command.Process.Kill()
			<-done
			t.Error("AX fixture did not stop")
		}
	})
	config := make(chan axruntime.Config, 1)
	errors := make(chan error, 1)
	go func() {
		var cfg axruntime.Config
		err := json.NewDecoder(pipe).Decode(&cfg)
		if err != nil {
			errors <- err
			return
		}
		config <- cfg
	}()
	var cfg axruntime.Config
	select {
	case cfg = <-config:
	case err := <-errors:
		t.Fatal(err)
	case <-time.After(30 * time.Second):
		t.Fatal("AX fixture did not become ready")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	client, err := axruntime.Dial(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client
}
