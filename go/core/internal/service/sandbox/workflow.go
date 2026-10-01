package sandbox

import (
	"context"
	"errors"
	"time"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/uuid"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"github.com/kagent-dev/kagent/go/core/internal/axruntime"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/pkg/logging"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// run executes one attempt inline. Errors retain durable intent; only another
// lifecycle request retries it. Expiration independently requests deletion.
func (s *Service) run(ctx context.Context, id string, kind apiv1alpha1.RuntimeOperation) (_ *apiv1alpha1.Sandbox, err error) {
	ctx, cancel := context.WithTimeout(ctx, database.RuntimeOperationTimeout)
	defer cancel()
	op, err := s.config.Store.BeginSandboxOperation(ctx, id, kind)
	if err != nil {
		return nil, sandboxError(err)
	}
	if op.Instance.Operation != kind {
		return op.Instance, nil
	}
	defer func() {
		if err != nil {
			finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			err = errors.Join(err, s.config.Store.RecordSandboxOperationFailure(finishCtx, id, op.ID))
		}
	}()
	revision, err := s.config.Store.GetSandboxRevision(ctx, op.Instance.PreparedRevision)
	if err != nil {
		return nil, sandboxError(err)
	}
	binding := axruntime.Binding{Task: &ax.ResourceRef{Atespace: revision.PreparedRuntimeAtespace, Name: "sandbox-" + id}, Runtime: &ax.ResourceRef{Atespace: revision.PreparedRuntimeAtespace, Name: revision.PreparedRuntimeName, Uid: revision.PreparedRuntimeUID}}
	saved, bindingErr := s.config.Store.GetAXBinding(ctx, id)
	if bindingErr == nil {
		binding.Task = saved.Task
		binding.Runtime = saved.PreparedRuntime
		binding.Group = saved.Group
	} else if !errors.Is(bindingErr, database.ErrNotFound) {
		return nil, sandboxError(bindingErr)
	}
	operation := map[apiv1alpha1.RuntimeOperation]axruntime.Operation{
		apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE:  axruntime.Create,
		apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_RESUME:  axruntime.Resume,
		apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_SUSPEND: axruntime.Suspend,
		apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_DELETE:  axruntime.Delete,
	}[kind]
	if operation == axruntime.Delete && binding.Task.Uid == "" {
		creation, e := s.config.Store.GetAXCreation(ctx, id)
		if e == nil {
			task, e := s.config.Runtime.GetTask(ctx, &ax.GetTaskRequest{Atespace: creation.Atespace, Name: creation.TaskName, RefreshRuntime: true})
			if e != nil {
				return nil, sandboxError(e)
			}
			if task.GetMetadata().GetUid() == "" || task.GetStatus().GetRuntimeStatus().GetLastOperationId() != creation.OperationID.String() || !proto.Equal(task.GetSpec().GetPreparedRuntimeRef(), binding.Runtime) {
				return nil, sandboxError(status.Error(codes.FailedPrecondition, "unresolved sandbox creation requires AX recovery"))
			}
			binding.Task = ax.Ref(task.Metadata)
			binding.Group = task.Spec.GroupRef
		} else if !errors.Is(e, database.ErrNotFound) {
			return nil, sandboxError(e)
		}
	}
	transition, err := axruntime.Prepare(ctx, s.config.Runtime, axruntime.Intent{Binding: binding, Operation: operation, RequestID: op.ID.String(), Start: operation == axruntime.Create}, op.ExecutorID != uuid.Nil)
	if err != nil {
		return nil, sandboxError(err)
	}
	executorID := uuid.New()
	claimed, err := s.config.Store.ClaimSandboxOperation(ctx, id, op.ID, executorID)
	if err != nil {
		return nil, sandboxError(err)
	}
	if !claimed {
		return nil, sandboxError(database.ErrConflict)
	}
	defer func() {
		if err == nil {
			return // Completion already cleared the claim.
		}
		finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		err = errors.Join(err, s.config.Store.ReleaseRuntimeOperation(finishCtx, id, op.ID, executorID))
	}()
	if operation == axruntime.Create {
		if e := s.config.Store.ReserveAXCreation(ctx, id, op.ID, executorID, binding.Task, binding.Runtime); e != nil {
			return nil, sandboxError(e)
		}
	}
	if _, err := axruntime.ApplyWithBinding(ctx, s.config.Runtime, transition, func(ctx context.Context, task *ax.Task) error {
		return s.config.Store.RecordAXBinding(ctx, id, op.ID, executorID, database.AXBinding{Task: ax.Ref(task.Metadata), Group: task.Spec.GroupRef, PreparedRuntime: task.Spec.PreparedRuntimeRef})
	}); err != nil {
		return nil, sandboxError(err)
	}
	finishCtx, cancelFinish := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancelFinish()
	result, err := s.config.Store.FinishSandboxOperation(finishCtx, id, op.ID, executorID, "")
	return result, sandboxError(err)
}

var (
	_ manager.Runnable               = (*Service)(nil)
	_ manager.LeaderElectionRunnable = (*Service)(nil)
)

func (s *Service) NeedLeaderElection() bool { return false }

// Start only deletes expired sandboxes. Ordinary pending lifecycle operations
// are client-driven. Database claims coordinate expiration with inline callers.
func (s *Service) Start(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var afterID string
	for {
		ids, err := s.config.Store.ListExpiredSandboxes(ctx, afterID, 100)
		if err != nil {
			logging.FromContext(ctx).ErrorContext(ctx, "list expired sandboxes", "error", err)
		} else {
			var group errgroup.Group
			group.SetLimit(4)
			for _, id := range ids {
				group.Go(func() error {
					if _, err := s.run(ctx, id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_DELETE); err != nil && !errors.Is(err, database.ErrConflict) && ctx.Err() == nil {
						logging.FromContext(ctx).ErrorContext(ctx, "expire sandbox", "sandbox_id", id, "error", err)
					}
					return nil
				})
			}
			if err := group.Wait(); err != nil {
				return err
			}
			afterID = ""
			if len(ids) == 100 {
				afterID = ids[len(ids)-1]
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
