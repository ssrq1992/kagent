package axruntime

import (
	"context"
	"strings"
	"testing"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type lifecycleFake struct {
	ax.AXClient
	task    *ax.Task
	runtime *ax.PreparedRuntime
	creates int
	resumes int
	ids     []string
	failure error
}

func (f *lifecycleFake) GetPreparedRuntime(context.Context, *ax.GetPreparedRuntimeRequest, ...grpc.CallOption) (*ax.PreparedRuntime, error) {
	return f.runtime, nil
}
func (f *lifecycleFake) GetTask(context.Context, *ax.GetTaskRequest, ...grpc.CallOption) (*ax.Task, error) {
	if f.task == nil {
		return nil, status.Error(codes.NotFound, "missing")
	}
	return proto.Clone(f.task).(*ax.Task), nil
}
func (f *lifecycleFake) CreateTask(_ context.Context, req *ax.CreateTaskRequest, _ ...grpc.CallOption) (*ax.Task, error) {
	f.creates++
	f.ids = append(f.ids, req.RequestId)
	if f.failure != nil {
		return nil, f.failure
	}
	f.task = proto.Clone(req.Task).(*ax.Task)
	f.task.Metadata.Uid = "task-uid"
	f.task.Status = &ax.TaskStatus{RuntimeStatus: &ax.TaskRuntimeStatus{Phase: "Suspended"}}
	return proto.Clone(f.task).(*ax.Task), nil
}
func (f *lifecycleFake) ResumeTask(_ context.Context, req *ax.ResumeTaskRequest, _ ...grpc.CallOption) (*ax.Task, error) {
	f.resumes++
	f.ids = append(f.ids, req.OperationId)
	if req.ExpectedUid != f.task.Metadata.Uid {
		return nil, status.Error(codes.FailedPrecondition, "wrong UID")
	}
	f.task.Status.RuntimeStatus.Phase = "Running"
	return proto.Clone(f.task).(*ax.Task), nil
}
func fixture() (*lifecycleFake, Intent) {
	r := &ax.PreparedRuntime{Metadata: &ax.ObjectMeta{Atespace: "test", Name: "runtime", Uid: "runtime-uid"}, Spec: &ax.PreparedRuntimeSpec{GroupRef: &ax.ResourceRef{Atespace: "test", Name: "group", Uid: "group-uid"}}, Phase: "Ready"}
	return &lifecycleFake{runtime: r}, Intent{Binding: Binding{Task: &ax.ResourceRef{Atespace: "test", Name: "task"}, Runtime: ax.Ref(r.Metadata)}, Operation: Create, RequestID: "business-claim", Start: true}
}
func TestLifecycleClaimsAndNoAdoption(t *testing.T) {
	f, intent := fixture()
	prepared, err := Prepare(t.Context(), f, intent, false)
	if err != nil {
		t.Fatal(err)
	}
	if f.creates != 0 || f.resumes != 0 {
		t.Fatal("preparation issued compute work")
	}
	task, err := Apply(t.Context(), f, prepared)
	if err != nil {
		t.Fatal(err)
	}
	if task.Metadata.Uid != "task-uid" || f.creates != 1 || f.resumes != 1 || strings.Join(f.ids, ",") != "business-claim,business-claim:start" {
		t.Fatal("lost durable operation or task identity")
	}
	if _, err = Prepare(t.Context(), f, intent, false); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("adopted existing task: %v", err)
	}
	intent.Operation = Resume
	intent.Binding.Task = ax.Ref(task.Metadata)
	intent.Binding.Task.Uid = "replacement"
	if _, err = Prepare(t.Context(), f, intent, true); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("replacement accepted: %v", err)
	}
}
func TestLifecycleUnknownResultDoesNotRetry(t *testing.T) {
	f, intent := fixture()
	f.failure = status.Error(codes.DeadlineExceeded, "result unknown")
	prepared, err := Prepare(t.Context(), f, intent, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Apply(t.Context(), f, prepared); status.Code(err) != codes.DeadlineExceeded {
		t.Fatal(err)
	}
	if f.creates != 1 || f.resumes != 0 {
		t.Fatal("unknown create was retried or followed by resume")
	}
}
func TestReferenceRejectsUnboundOrCrossNamespaceIdentity(t *testing.T) {
	f, intent := fixture()
	prepared, err := Prepare(t.Context(), f, intent, false)
	if err != nil {
		t.Fatal(err)
	}
	task, err := Apply(t.Context(), f, prepared)
	if err != nil {
		t.Fatal(err)
	}
	ref := ReferenceFromTask(task)
	encoded, err := ref.Encode()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeReference(encoded)
	if err != nil || !proto.Equal(decoded.Task, ref.Task) {
		t.Fatalf("decode: %v", err)
	}
	if _, err = DecodeReference(encoded + " {}"); err == nil {
		t.Fatal("accepted trailing identity")
	}
	ref.Group.Atespace = "another"
	if _, err = ref.Encode(); err == nil {
		t.Fatal("accepted cross-namespace group")
	}
	if _, err = DecodeReference(`{"version":1,"actor":"backend"}`); err == nil {
		t.Fatal("accepted backend identity")
	}
}

func TestLifecycleCreationBindingMustPersistBeforeResume(t *testing.T) {
	f, intent := fixture()
	transition, err := Prepare(t.Context(), f, intent, false)
	if err != nil {
		t.Fatal(err)
	}
	task, err := ApplyWithBinding(t.Context(), f, transition, func(ctx context.Context, task *ax.Task) error {
		if task.Metadata.Uid == "" || f.resumes != 0 {
			t.Fatal("resume preceded binding")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("binding persistence must be bounded")
		}
		return status.Error(codes.Unavailable, "database unavailable")
	})
	if status.Code(err) != codes.Unavailable || f.resumes != 0 || task.GetMetadata().GetUid() == "" {
		t.Fatalf("lost known create result or resumed without binding: %v %v", task, err)
	}
}
