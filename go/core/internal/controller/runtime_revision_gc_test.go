package controller

import (
	"context"
	"errors"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/stretchr/testify/require"
)

func TestRuntimeRevisionGCStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		store := &fakeGCStore{revisions: []database.RuntimeArtifact{
			{Revision: "failed", PreparedRuntimeName: "failed", PreparedRuntimeAtespace: "team", PreparedRuntimeUID: "failed-uid"},
			{Revision: "healthy", PreparedRuntimeName: "healthy", PreparedRuntimeAtespace: "team", PreparedRuntimeUID: "healthy-uid"},
		}, listErr: errors.New("database unavailable")}
		templates := &fakeGCTemplates{deleteErr: errors.New("Substrate unavailable")}
		collector := NewRuntimeRevisionGC(store, templates)
		require.True(t, collector.NeedLeaderElection())
		done := make(chan error, 1)
		go func() { done <- collector.Start(ctx) }()
		synctest.Wait()
		store.mu.Lock()
		require.Equal(t, 1, store.lists, "startup must sweep immediately")

		store.listErr = nil
		store.mu.Unlock()
		time.Sleep(runtimeRevisionGCInterval)
		synctest.Wait()
		store.mu.Lock()
		require.Equal(t, []string{"healthy"}, store.deleted, "a failed candidate must not block later candidates")
		require.Len(t, store.revisions, 1)
		store.mu.Unlock()
		templates.mu.Lock()
		templates.deleteErr = nil
		templates.mu.Unlock()
		time.Sleep(runtimeRevisionGCInterval)
		synctest.Wait()
		store.mu.Lock()
		require.Equal(t, []string{"healthy", "failed"}, store.deleted, "periodic sweeps must retry without template events")
		store.mu.Unlock()
		cancel()
		require.NoError(t, <-done)
	})
}

func TestRuntimeRevisionGCDeadlineAndCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		store := &fakeGCStore{revisions: []database.RuntimeArtifact{
			{Revision: "failed", PreparedRuntimeName: "failed", PreparedRuntimeAtespace: "team", PreparedRuntimeUID: "failed-uid"},
			{Revision: "healthy", PreparedRuntimeName: "healthy", PreparedRuntimeAtespace: "team", PreparedRuntimeUID: "healthy-uid"},
		}}
		templates := &fakeGCTemplates{block: true}
		collector := NewRuntimeRevisionGC(store, templates)
		done := make(chan error, 1)
		go func() { done <- collector.Start(ctx) }()
		synctest.Wait()
		time.Sleep(time.Minute)
		synctest.Wait()
		store.mu.Lock()
		require.Equal(t, []string{"healthy"}, store.deleted, "deadline must release the sweep to process healthy candidates")
		store.mu.Unlock()
		cancel() // The next sweep is blocked in the backend again.
		require.NoError(t, <-done, "shutdown must cancel in-flight cleanup")
		require.Len(t, store.revisions, 1, "failed deletion must retain its durable revision")
	})
}

type fakeGCStore struct {
	mu        sync.Mutex
	revisions []database.RuntimeArtifact
	listErr   error
	lists     int
	deleted   []string
}

func (s *fakeGCStore) ListUnreferencedRuntimeRevisions(context.Context) ([]database.RuntimeArtifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lists++
	return s.revisions, s.listErr
}

func (s *fakeGCStore) BeginRuntimeRevisionDeletion(_ context.Context, id string) (*database.RuntimeArtifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, revision := range s.revisions {
		if revision.Revision == id {
			return &revision, nil
		}
	}
	return nil, nil
}

func (s *fakeGCStore) DeleteRuntimeRevision(_ context.Context, id, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted = append(s.deleted, id)
	for i, revision := range s.revisions {
		if revision.Revision == id {
			s.revisions = append(s.revisions[:i:i], s.revisions[i+1:]...)
			break
		}
	}
	return nil
}

type fakeGCTemplates struct {
	mu sync.Mutex
	fakePreparedRuntimes
	deleteErr error
	block     bool
}

func (f *fakeGCTemplates) ReleasePreparedRuntime(ctx context.Context, req *ax.ReleasePreparedRuntimeRequest, _ ...grpc.CallOption) (*ax.ReleasePreparedRuntimeResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if req.Ref.Name == "failed" {
		if f.block {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if f.deleteErr != nil {
			return nil, f.deleteErr
		}
	}
	return &ax.ReleasePreparedRuntimeResponse{}, nil
}

type sandboxGCClient struct {
	ax.AXClient
	request *ax.ReleasePreparedRuntimeRequest
	failure error
}

func (c *sandboxGCClient) ReleasePreparedRuntime(_ context.Context, req *ax.ReleasePreparedRuntimeRequest, _ ...grpc.CallOption) (*ax.ReleasePreparedRuntimeResponse, error) {
	c.request = req
	return &ax.ReleasePreparedRuntimeResponse{}, c.failure
}
func TestSandboxRuntimeGCReleasesAXBeforeDatabase(t *testing.T) {
	artifact := database.RuntimeArtifact{Revision: "revision", Kind: "sandbox", PreparedRuntimeAtespace: "team", PreparedRuntimeName: "runtime", PreparedRuntimeUID: "runtime-uid"}
	store := &fakeGCStore{revisions: []database.RuntimeArtifact{artifact}}
	client := &sandboxGCClient{failure: errors.New("AX release response unknown")}
	gc := NewRuntimeRevisionGC(store, client)
	require.Error(t, gc.collect(t.Context(), "revision"))
	require.Empty(t, store.deleted)
	require.Equal(t, "runtime-uid", client.request.Ref.Uid)
	require.Equal(t, "gc-revision", client.request.OperationId)
	client.failure = nil
	require.NoError(t, gc.collect(t.Context(), "revision"))
	require.Equal(t, []string{"revision"}, store.deleted)
}
