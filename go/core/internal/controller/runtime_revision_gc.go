package controller

import (
	"context"
	"fmt"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"time"

	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/pkg/logging"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const runtimeRevisionGCInterval = time.Minute

type runtimeRevisionGCStore interface {
	ListUnreferencedRuntimeRevisions(context.Context) ([]database.RuntimeArtifact, error)
	BeginRuntimeRevisionDeletion(context.Context, string) (*database.RuntimeArtifact, error)
	DeleteRuntimeRevision(context.Context, string, string) error
}

// RuntimeRevisionGC retries durable runtime deletions independently of preparation.
type RuntimeRevisionGC struct {
	store runtimeRevisionGCStore
	ax    ax.AXClient
}

var (
	_ manager.Runnable               = (*RuntimeRevisionGC)(nil)
	_ manager.LeaderElectionRunnable = (*RuntimeRevisionGC)(nil)
)

func NewRuntimeRevisionGC(store runtimeRevisionGCStore, client ax.AXClient) *RuntimeRevisionGC {
	return &RuntimeRevisionGC{store: store, ax: client}
}

func (r *RuntimeRevisionGC) NeedLeaderElection() bool { return true }

func (r *RuntimeRevisionGC) Start(ctx context.Context) error {
	ticker := time.NewTicker(runtimeRevisionGCInterval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		r.sweep(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
	return nil
}

func (r *RuntimeRevisionGC) sweep(ctx context.Context) {
	listCtx, cancel := context.WithTimeout(ctx, time.Minute)
	revisions, err := r.store.ListUnreferencedRuntimeRevisions(listCtx)
	cancel()
	if err != nil {
		logging.FromContext(ctx).ErrorContext(ctx, "failed to list unreferenced runtime revisions", "error", err)
		return
	}
	for _, candidate := range revisions {
		if ctx.Err() != nil {
			return
		}
		if err := r.collect(ctx, candidate.Revision); err != nil {
			logging.FromContext(ctx).ErrorContext(ctx, "failed to collect runtime revision", "revision", candidate.Revision, "error", err)
		}
	}
}

// collect retains the database row until compute deletion succeeds. Each candidate
// has its own deadline so a stuck backend or database lock cannot stall the sweep.
func (r *RuntimeRevisionGC) collect(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	revision, err := r.store.BeginRuntimeRevisionDeletion(ctx, id)
	if err != nil {
		return fmt.Errorf("begin deletion of runtime revision %s: %w", id, err)
	}
	if revision == nil {
		return nil
	}
	ref := &ax.ResourceRef{Atespace: revision.PreparedRuntimeAtespace, Name: revision.PreparedRuntimeName, Uid: revision.PreparedRuntimeUID}
	if err := ax.ValidateRef(ref, true); err != nil {
		return err
	}
	_, err = r.ax.ReleasePreparedRuntime(ctx, &ax.ReleasePreparedRuntimeRequest{Ref: ref, OperationId: "gc-" + revision.Revision})
	if err != nil && status.Code(err) != codes.NotFound {
		return fmt.Errorf("release AX runtime: %w", err)
	}
	return r.store.DeleteRuntimeRevision(ctx, revision.Revision, revision.PreparedRuntimeUID)
}
