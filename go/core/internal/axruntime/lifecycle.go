package axruntime

import (
	"context"
	"fmt"
	"time"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Operation string

const (
	Create  Operation = "Create"
	Resume  Operation = "Resume"
	Pause   Operation = "Pause"
	Suspend Operation = "Suspend"
	Delete  Operation = "Delete"
)

type Intent struct {
	Binding   Binding
	Operation Operation
	RequestID string
	Start     bool
}
type Transition struct {
	intent   Intent
	observed *ax.Task
}

func TaskName(id string) string { return "session-" + id }

// Prepare only reads. The caller must win its PostgreSQL business claim before
// Apply. Neither a missing runtime nor a mismatching UID permits replacement.
func Prepare(ctx context.Context, client ax.AXClient, intent Intent, previouslyIssued bool) (*Transition, error) {
	if intent.RequestID == "" {
		return nil, fmt.Errorf("durable operation ID is required")
	}
	b := &intent.Binding
	if err := ax.ValidateRef(b.Task, intent.Operation != Create && intent.Operation != Delete); err != nil {
		return nil, err
	}
	switch intent.Operation {
	case Create, Resume, Pause, Suspend, Delete:
	default:
		return nil, fmt.Errorf("invalid AX operation %q", intent.Operation)
	}
	if intent.Operation == Create {
		if err := ax.ValidateRef(b.Runtime, true); err != nil {
			return nil, err
		}
		runtime, err := client.GetPreparedRuntime(ctx, &ax.GetPreparedRuntimeRequest{Ref: b.Runtime})
		if err != nil {
			return nil, err
		}
		if runtime.Phase != "Ready" || !proto.Equal(ax.Ref(runtime.Metadata), b.Runtime) {
			return nil, status.Error(codes.FailedPrecondition, "AX runtime identity changed or is not ready")
		}
		if b.Group == nil {
			b.Group = proto.Clone(runtime.Spec.GroupRef).(*ax.ResourceRef)
		}
		if !proto.Equal(b.Group, runtime.Spec.GroupRef) || b.Task.Atespace != b.Runtime.Atespace {
			return nil, status.Error(codes.FailedPrecondition, "AX runtime group or namespace differs")
		}
		if b.RestoreFrom != nil {
			snapshot, err := client.GetTaskCheckpoint(ctx, &ax.GetTaskCheckpointRequest{Ref: b.RestoreFrom})
			if err != nil {
				return nil, err
			}
			if snapshot.Phase != "Ready" || !proto.Equal(snapshot.RuntimeRef, b.Runtime) || !proto.Equal(snapshot.GroupRef, b.Group) {
				return nil, status.Error(codes.FailedPrecondition, "checkpoint lineage differs")
			}
		}
	}
	observed, err := client.GetTask(ctx, &ax.GetTaskRequest{Atespace: b.Task.Atespace, Name: b.Task.Name, RefreshRuntime: true})
	if status.Code(err) == codes.NotFound && (intent.Operation == Create || intent.Operation == Delete) {
		observed = nil
		err = nil
	}
	if err != nil {
		return nil, err
	}
	if observed != nil {
		if intent.Operation == Create && !previouslyIssued {
			return nil, status.Error(codes.AlreadyExists, "AX task existed before this operation was issued")
		}
		if intent.Operation != Create && b.Task.Uid == "" {
			return nil, status.Error(codes.FailedPrecondition, "existing AX task requires its persisted UID")
		}
		if err = verifyTask(observed, *b); err != nil {
			return nil, err
		}
	}
	return &Transition{intent: intent, observed: observed}, nil
}
func verifyTask(task *ax.Task, b Binding) error {
	m := task.GetMetadata()
	if m.GetUid() == "" || m.GetAtespace() != b.Task.Atespace || m.GetName() != b.Task.Name || (b.Task.Uid != "" && b.Task.Uid != m.Uid) {
		return status.Error(codes.FailedPrecondition, "AX task identity changed")
	}
	if b.Runtime != nil && !proto.Equal(task.GetSpec().GetPreparedRuntimeRef(), b.Runtime) {
		return status.Error(codes.FailedPrecondition, "AX prepared runtime changed")
	}
	if b.Group != nil && !proto.Equal(task.GetSpec().GetGroupRef(), b.Group) {
		return status.Error(codes.FailedPrecondition, "AX task group changed")
	}
	return nil
}
func (t *Transition) TaskUID() string { return t.observed.GetMetadata().GetUid() }
func (t *Transition) Task() *ax.Task  { return t.observed }
func Apply(ctx context.Context, client ax.AXClient, t *Transition) (*ax.Task, error) {
	return ApplyWithBinding(ctx, client, t, nil)
}

// ApplyWithBinding publishes a known creation result before starting compute.
// Failure to persist the UID prevents Resume; retries use the original AX receipt.
func ApplyWithBinding(ctx context.Context, client ax.AXClient, t *Transition, persist func(context.Context, *ax.Task) error) (*ax.Task, error) {
	if t == nil {
		return nil, fmt.Errorf("prepared transition is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	i := t.intent
	b := i.Binding
	var task *ax.Task
	var err error
	switch i.Operation {
	case Create:
		task, err = client.CreateTask(ctx, &ax.CreateTaskRequest{RequestId: i.RequestID, Task: &ax.Task{Metadata: &ax.ObjectMeta{Atespace: b.Task.Atespace, Name: b.Task.Name}, Spec: &ax.TaskSpec{GroupRef: b.Group, PreparedRuntimeRef: b.Runtime, RestoreFrom: b.RestoreFrom}}})
		if err == nil {
			if err = verifyTask(task, b); err != nil {
				return nil, err
			}
			t.observed = task
			b.Task = ax.Ref(task.Metadata)
		}
		if err == nil && persist != nil {
			persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			err = persist(persistCtx, task)
			cancel()
		}
		if err == nil && i.Start {
			task, err = client.ResumeTask(ctx, &ax.ResumeTaskRequest{Atespace: b.Task.Atespace, Name: b.Task.Name, ExpectedUid: b.Task.Uid, OperationId: i.RequestID + ":start"})
		}
	case Resume:
		task, err = client.ResumeTask(ctx, &ax.ResumeTaskRequest{Atespace: b.Task.Atespace, Name: b.Task.Name, ExpectedUid: b.Task.Uid, OperationId: i.RequestID})
	case Pause:
		task, err = client.PauseTask(ctx, &ax.PauseTaskRequest{Ref: b.Task, OperationId: i.RequestID})
	case Suspend:
		task, err = client.SuspendTask(ctx, &ax.SuspendTaskRequest{Atespace: b.Task.Atespace, Name: b.Task.Name, ExpectedUid: b.Task.Uid, OperationId: i.RequestID})
	case Delete:
		if t.observed == nil && b.Task.Uid == "" {
			return nil, nil
		} // No compute was ever published.
		_, err = client.DeleteTask(ctx, &ax.DeleteTaskRequest{Atespace: b.Task.Atespace, Name: b.Task.Name, ExpectedUid: b.Task.Uid, OperationId: i.RequestID})
		if status.Code(err) == codes.NotFound && t.observed == nil {
			err = nil
		}
		return t.observed, err
	}
	if err != nil {
		return t.observed, err
	}
	expected := map[Operation]string{Create: "Suspended", Resume: "Running", Pause: "Paused", Suspend: "Suspended"}[i.Operation]
	if i.Operation == Create && i.Start {
		expected = "Running"
	}
	for {
		if err = verifyTask(task, b); err != nil {
			return nil, err
		}
		t.observed = task
		phase := task.GetStatus().GetRuntimeStatus().GetPhase()
		if phase == expected {
			return task, nil
		}
		if phase != "Transitioning" {
			return nil, status.Errorf(codes.FailedPrecondition, "AX task is %s, expected %s", phase, expected)
		}
		select {
		case <-ctx.Done():
			return task, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
		task, err = client.GetTask(ctx, &ax.GetTaskRequest{Atespace: b.Task.Atespace, Name: b.Task.Name, RefreshRuntime: true})
		if err != nil {
			return nil, err
		}
	}
}
