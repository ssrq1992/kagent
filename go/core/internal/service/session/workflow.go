package session

import (
	"context"
	"errors"
	"fmt"
	"time"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/uuid"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"github.com/kagent-dev/kagent/go/core/internal/axruntime"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"google.golang.org/protobuf/proto"
)

type workflowStore interface {
	GetAXBinding(context.Context, string) (database.AXBinding, error)
	RecordAXBinding(context.Context, string, uuid.UUID, uuid.UUID, database.AXBinding) error
	GetAXCreation(context.Context, string) (database.AXCreation, error)
	ReserveAXCreation(context.Context, string, uuid.UUID, uuid.UUID, *ax.ResourceRef, *ax.ResourceRef) error
	ClaimSessionQuiescence(context.Context) (*database.SessionQuiescence, error)
	FinishSessionQuiescence(context.Context, *database.SessionQuiescence, *database.SessionTaskSnapshot) error
	GetSessionForRuntime(context.Context, string, string) (*apiv1alpha1.Session, error)
	GetSessionCheckpointSnapshot(context.Context, string, string) (*database.SessionTaskSnapshot, string, error)
	GetRuntimeRevision(context.Context, string) (*database.RuntimeRevision, error)
	BeginSessionOperation(context.Context, string, apiv1alpha1.RuntimeOperation) (*database.SessionOperation, error)
	ClaimSessionOperation(context.Context, string, uuid.UUID, uuid.UUID) (bool, error)
	ReleaseRuntimeOperation(context.Context, string, uuid.UUID, uuid.UUID) error
	FinishSessionOperation(context.Context, string, uuid.UUID, uuid.UUID, string, string, string) (*apiv1alpha1.Session, error)
	GetSessionOperation(context.Context, string, uuid.UUID) (*database.SessionOperation, error)
}

// TaskWorkflow preserves business claims while AX owns runtime lifecycle.
type TaskWorkflow struct {
	store   workflowStore
	runtime ax.AXClient
}

func NewTaskWorkflow(store workflowStore, runtime ax.AXClient) *TaskWorkflow {
	return &TaskWorkflow{store: store, runtime: runtime}
}

func (w *TaskWorkflow) boundTask(ctx context.Context, session *apiv1alpha1.Session) (*ax.Task, database.AXBinding, error) {
	binding, err := w.store.GetAXBinding(ctx, session.Id)
	if err != nil {
		return nil, binding, err
	}
	task, err := w.runtime.GetTask(ctx, &ax.GetTaskRequest{Atespace: binding.Task.Atespace, Name: binding.Task.Name, RefreshRuntime: true})
	if err != nil {
		return nil, binding, err
	}
	if !proto.Equal(ax.Ref(task.GetMetadata()), binding.Task) || !proto.Equal(task.GetSpec().GetPreparedRuntimeRef(), binding.PreparedRuntime) || !proto.Equal(task.GetSpec().GetGroupRef(), binding.Group) {
		return nil, binding, fmt.Errorf("AX runtime identity changed")
	}
	if _, err = w.store.GetSessionForRuntime(ctx, session.Id, binding.Task.Uid); err != nil {
		return nil, binding, err
	}
	return task, binding, nil
}
func (w *TaskWorkflow) boundary(ctx context.Context, session *apiv1alpha1.Session, operation axruntime.Operation) (*ax.Task, error) {
	task, binding, err := w.boundTask(ctx, session)
	if err != nil {
		return nil, err
	}
	phase := task.GetStatus().GetRuntimeStatus().GetPhase()
	if operation == axruntime.Pause && phase == "Paused" || operation == axruntime.Suspend && phase == "Suspended" {
		return task, nil
	}
	// Execution version scopes idle work to the observed incarnation. The durable
	// business quiescence claim prevents admission while its outcome is uncertain.
	id := fmt.Sprintf("idle-%s-%d-%s", binding.Task.Uid, task.GetStatus().GetRuntimeStatus().GetExecutionVersion(), operation)
	transition, err := axruntime.Prepare(ctx, w.runtime, axruntime.Intent{
		Binding:   axruntime.Binding{Task: binding.Task, Runtime: binding.PreparedRuntime, Group: binding.Group},
		Operation: operation, RequestID: id,
	}, true)
	if err != nil {
		return nil, err
	}
	// Acknowledgement can precede the physical boundary. Reuse lifecycle polling,
	// preserving the same operation identity and validating every observed UID.
	return axruntime.Apply(ctx, w.runtime, transition)

}
func (w *TaskWorkflow) Pause(ctx context.Context, session *apiv1alpha1.Session) error {
	_, err := w.boundary(ctx, session, axruntime.Pause)
	return err
}
func (w *TaskWorkflow) Quiesce(ctx context.Context, session *apiv1alpha1.Session) (*database.SessionTaskSnapshot, error) {
	task, err := w.boundary(ctx, session, axruntime.Suspend)
	if err != nil {
		return nil, err
	}
	ref := axruntime.ReferenceFromTask(task)
	if ref.BoundaryRef == "" || !task.GetStatus().GetRuntimeStatus().GetRestorable() {
		return nil, fmt.Errorf("AX returned no restorable boundary")
	}
	encoded, err := ref.Encode()
	if err != nil {
		return nil, err
	}
	return &database.SessionTaskSnapshot{Atespace: ref.Task.Atespace, Reference: encoded, ContentScope: "DATA"}, nil
}

// Create provisions the persisted session once, using its pinned checkpoint for
// forks. Retries return current state; an uncertain prior creation blocks execution.
func (w *TaskWorkflow) Create(ctx context.Context, session *apiv1alpha1.Session) (*apiv1alpha1.Session, error) {
	return w.run(ctx, session.GetId(), apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
}

// Suspend returns after the AX Task and session are suspended. A retry observes
// the same operation rather than issuing a second mutation.
func (w *TaskWorkflow) Suspend(ctx context.Context, session *apiv1alpha1.Session) (*apiv1alpha1.Session, error) {
	return w.run(ctx, session.GetId(), apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_SUSPEND)
}

// Resume returns after the AX Task is running and the session is ready. Missing
// Tasks are errors; Resume never creates replacement compute.
func (w *TaskWorkflow) Resume(ctx context.Context, session *apiv1alpha1.Session) (*apiv1alpha1.Session, error) {
	return w.run(ctx, session.GetId(), apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_RESUME)
}

// Delete closes admission before stopping and deleting compute. It can supersede
// unissued creation, but never deletes a session while a prior call is uncertain.
func (w *TaskWorkflow) Delete(ctx context.Context, session *apiv1alpha1.Session) (*apiv1alpha1.Session, error) {
	return w.run(ctx, session.GetId(), apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
}

func (w *TaskWorkflow) run(ctx context.Context, sessionID string, requestedKind apiv1alpha1.RuntimeOperation) (*apiv1alpha1.Session, error) {
	ctx, cancelAttempt := context.WithTimeout(ctx, database.RuntimeOperationTimeout)
	defer cancelAttempt()
	operation, err := w.store.BeginSessionOperation(ctx, sessionID, requestedKind)
	if err != nil {
		return nil, err
	}
	return w.execute(ctx, operation)
}

// execute keeps lifecycle preparation separate from the durable issue boundary.
// Multiple callers may prepare using read-only calls; exactly one can authorize
// runtime mutations for a bounded attempt. Errors retain the operation and its
// resource pins so a client retry can continue it. Session admission still
// serializes lifecycle against runtime writes, idle work, and checkpoints.
func (w *TaskWorkflow) execute(ctx context.Context, operation *database.SessionOperation) (_ *apiv1alpha1.Session, err error) {
	sessionID := operation.Instance.Id
	if operation.Instance.Operation == apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_NONE {
		return operationOutcome(operation)
	}
	kind := operation.Instance.Operation
	session := operation.Instance
	revision, err := w.store.GetRuntimeRevision(ctx, session.GetPreparedRevision())
	if err != nil {
		return w.failPreparation(ctx, operation, fmt.Errorf("load prepared revision: %w", err))
	}
	binding := axruntime.Binding{Task: &ax.ResourceRef{Atespace: revision.PreparedRuntimeAtespace, Name: axruntime.TaskName(session.Id)}, Runtime: &ax.ResourceRef{Atespace: revision.PreparedRuntimeAtespace, Name: revision.PreparedRuntimeName, Uid: revision.PreparedRuntimeUID}}
	saved, bindingErr := w.store.GetAXBinding(ctx, sessionID)
	if bindingErr == nil {
		binding.Task, binding.Runtime, binding.Group = saved.Task, saved.PreparedRuntime, saved.Group
	} else if !errors.Is(bindingErr, database.ErrNotFound) {
		return w.failPreparation(ctx, operation, bindingErr)
	}
	if kind == apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE && operation.SourceCheckpointID != nil {
		snapshot, _, err := w.store.GetSessionCheckpointSnapshot(ctx, operation.SourceCheckpointID.String(), session.Creator)
		if err != nil {
			return w.failPreparation(ctx, operation, err)
		}
		ref, err := axruntime.DecodeReference(snapshot.Reference)
		if err != nil || ref.Checkpoint == nil || snapshot.ContentScope != "DATA" {
			return w.failPreparation(ctx, operation, fmt.Errorf("fork requires an AX checkpoint reference"))
		}
		binding.RestoreFrom = ref.Checkpoint
	}
	action := map[apiv1alpha1.RuntimeOperation]axruntime.Operation{
		apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE:  axruntime.Create,
		apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_RESUME:  axruntime.Resume,
		apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_SUSPEND: axruntime.Suspend,
		apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_DELETE:  axruntime.Delete,
	}[kind]
	if action == axruntime.Delete && binding.Task.Uid == "" {
		creation, e := w.store.GetAXCreation(ctx, sessionID)
		if e == nil {
			task, e := w.runtime.GetTask(ctx, &ax.GetTaskRequest{Atespace: creation.Atespace, Name: creation.TaskName, RefreshRuntime: true})
			if e != nil {
				return nil, e
			}
			if task.GetMetadata().GetUid() == "" || task.GetStatus().GetRuntimeStatus().GetLastOperationId() != creation.OperationID.String() || !proto.Equal(task.GetSpec().GetPreparedRuntimeRef(), binding.Runtime) {
				return nil, fmt.Errorf("unresolved AX creation requires recovery")
			}
			binding.Task, binding.Group = ax.Ref(task.Metadata), task.Spec.GroupRef
		} else if !errors.Is(e, database.ErrNotFound) {
			return nil, e
		}
	}
	transition, err := axruntime.Prepare(ctx, w.runtime, axruntime.Intent{Binding: binding, Operation: action, RequestID: operation.ID.String()}, operation.ExecutorID != uuid.Nil)
	if err != nil {
		return w.failPreparation(ctx, operation, err)
	}

	executorID := uuid.New()
	claimed, err := w.store.ClaimSessionOperation(ctx, sessionID, operation.ID, executorID)
	if err != nil {
		return nil, err
	}
	if !claimed {
		// A superseded generation returns a conflict without runtime work.
		current, err := w.store.GetSessionOperation(ctx, sessionID, operation.ID)
		if err != nil {
			return nil, err
		}
		return operationOutcome(current)
	}
	defer func() {
		if err == nil {
			return // Completion already cleared the claim.
		}
		finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		err = errors.Join(err, w.store.ReleaseRuntimeOperation(finishCtx, sessionID, operation.ID, executorID))
	}()
	if action == axruntime.Create {
		if err := w.store.ReserveAXCreation(ctx, sessionID, operation.ID, executorID, binding.Task, binding.Runtime); err != nil {
			return nil, err
		}
	}
	if _, err := axruntime.ApplyWithBinding(ctx, w.runtime, transition, func(ctx context.Context, task *ax.Task) error {
		return w.store.RecordAXBinding(ctx, sessionID, operation.ID, executorID, database.AXBinding{Task: ax.Ref(task.Metadata), Group: task.Spec.GroupRef, PreparedRuntime: task.Spec.PreparedRuntimeRef})
	}); err != nil {
		return nil, fmt.Errorf("lifecycle operation %s remains pending: %w", operation.ID, err)
	}
	var authority string
	if action == axruntime.Create {
		authority = transition.TaskUID()
	}

	// A disconnected client must not discard an already known runtime outcome.
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return w.store.FinishSessionOperation(finishCtx, sessionID, operation.ID, executorID, authority, transition.TaskUID(), "")
}

// failPreparation releases only unissued work. If another caller won, observe
// the same generation instead. Completion returns current state; supersession
// returns a conflict. A local preparation error cannot clear a newer operation.
func (w *TaskWorkflow) failPreparation(ctx context.Context, admitted *database.SessionOperation, cause error) (*apiv1alpha1.Session, error) {
	if admitted.ExecutorID != uuid.Nil {
		return nil, cause // An earlier attempt may have issued work; retain its intent.
	}
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, err := w.store.FinishSessionOperation(finishCtx, admitted.Instance.Id, admitted.ID, uuid.Nil, "", "", "lifecycle preparation failed")
	if errors.Is(err, database.ErrConflict) {
		operation, readErr := w.store.GetSessionOperation(finishCtx, admitted.Instance.Id, admitted.ID)
		if readErr != nil {
			return nil, errors.Join(cause, err, readErr)
		}
		return operationOutcome(operation)
	}
	return nil, errors.Join(cause, err)
}

func operationOutcome(operation *database.SessionOperation) (*apiv1alpha1.Session, error) {
	if operation.Instance.Operation == apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_NONE {
		return operation.Instance, nil
	}
	return nil, fmt.Errorf("lifecycle operation %s is pending; runtime effects may be unresolved: %w", operation.ID, database.ErrConflict)
}
