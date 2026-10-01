package session

import (
	"context"
	"crypto/sha256"
	"sync"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2apb/v1"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"github.com/kagent-dev/kagent/go/core/internal/axruntime"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/dbtest"
	"github.com/kagent-dev/kagent/go/core/internal/service/serviceerrors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestActorWorkflowLifecycle(t *testing.T) {
	store, session := lifecycleFixture(t)
	actors := &lifecycleTestActors{actors: map[string]*ax.Task{}}
	workflow := NewTaskWorkflow(store, actors)

	created, err := workflow.Create(context.Background(), session)
	if err != nil {
		t.Fatal(err)
	}
	if created.GetState() != apiv1alpha1.RuntimeState_RUNTIME_STATE_READY || created.GetA2AAuthority() == "" {
		t.Fatalf("created session = %+v", created)
	}
	if len(actors.actors) != 1 {
		t.Fatalf("actors = %v", actors.actors)
	}
	if actor := actors.actors[actorKey("team-a", axruntime.TaskName(session.GetId()))]; actor.GetStatus().GetRuntimeStatus().GetPhase() != "Suspended" {
		t.Fatalf("created Actor status = %s", actor.GetStatus().GetRuntimeStatus().GetPhase())
	}
	actors.actors[actorKey("team-a", axruntime.TaskName(session.GetId()))].Status.RuntimeStatus.Phase = "Running"
	if err := workflow.Pause(context.Background(), created); err != nil {
		t.Fatal(err)
	}
	if actor := actors.actors[actorKey("team-a", axruntime.TaskName(session.GetId()))]; actor.GetStatus().GetRuntimeStatus().GetPhase() != "Paused" {
		t.Fatalf("paused Actor status = %s", actor.GetStatus().GetRuntimeStatus().GetPhase())
	}
	boundary, err := workflow.Quiesce(context.Background(), created)
	if err != nil {
		t.Fatal(err)
	}
	if created.GetState() != apiv1alpha1.RuntimeState_RUNTIME_STATE_READY || boundary.Reference == "" {
		t.Fatalf("quiesced session = %+v, boundary = %+v", created, boundary)
	}

	suspended, err := workflow.Suspend(context.Background(), created)
	if err != nil {
		t.Fatal(err)
	}
	if suspended.GetState() != apiv1alpha1.RuntimeState_RUNTIME_STATE_SUSPENDED || suspended.GetOperation() != apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_NONE {
		t.Fatalf("suspended session = %+v", suspended)
	}
	if actor := actors.actors[actorKey("team-a", axruntime.TaskName(session.GetId()))]; actor.GetStatus().GetRuntimeStatus().GetPhase() != "Suspended" {
		t.Fatalf("suspended Actor status = %s", actor.GetStatus().GetRuntimeStatus().GetPhase())
	}

	resumed, err := workflow.Resume(context.Background(), suspended)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.GetState() != apiv1alpha1.RuntimeState_RUNTIME_STATE_READY || resumed.GetOperation() != apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_NONE {
		t.Fatalf("resumed session = %+v", resumed)
	}

	deleted, err := workflow.Delete(context.Background(), resumed)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.GetSessionByID(t.Context(), session.Id)
	require.ErrorIs(t, err, database.ErrNotFound)
	if deleted.GetState() != apiv1alpha1.RuntimeState_RUNTIME_STATE_DELETED || len(actors.actors) != 0 {
		t.Fatalf("deleted session = %+v, actors = %v", deleted, actors.actors)
	}
}

func TestActorWorkflowRejectsReplacedRuntime(t *testing.T) {
	for _, operation := range []string{"pause", "quiesce", "suspend", "delete"} {
		t.Run(operation, func(t *testing.T) {
			store, session := lifecycleFixture(t)
			actors := &lifecycleTestActors{actors: map[string]*ax.Task{}}
			workflow := NewTaskWorkflow(store, actors)
			session, err := workflow.Create(t.Context(), session)
			require.NoError(t, err)
			actor := actors.actors[actorKey("team-a", axruntime.TaskName(session.Id))]
			actor.Metadata.Uid = "replacement-uid"
			actor.Status.RuntimeStatus.Phase = "Running"
			switch operation {
			case "pause":
				err = workflow.Pause(t.Context(), session)
			case "quiesce":
				_, err = workflow.Quiesce(t.Context(), session)
			case "suspend":
				_, err = workflow.Suspend(t.Context(), session)
			case "delete":
				_, err = workflow.Delete(t.Context(), session)
			}
			require.Error(t, err)
			require.Equal(t, "Running", actor.Status.RuntimeStatus.Phase)
			require.Len(t, actors.actors, 1)
		})
	}
}

func TestActorWorkflowForkCreatesSuspendedActorFromCheckpoint(t *testing.T) {
	store, session := lifecycleFixture(t)
	actors := &lifecycleTestActors{actors: map[string]*ax.Task{}}
	session, checkpointID := lifecycleForkFixture(t, store, actors, session)
	fork, err := NewTaskWorkflow(store, actors).Create(t.Context(), session)
	if err != nil {
		t.Fatal(err)
	}
	actor := actors.actors[actorKey("team-a", axruntime.TaskName(session.GetId()))]
	if fork.GetState() != apiv1alpha1.RuntimeState_RUNTIME_STATE_READY ||
		actor.GetStatus().GetRuntimeStatus().GetPhase() != "Suspended" ||
		actor.GetSpec().GetRestoreFrom().GetName() != "checkpoint-"+checkpointID {
		t.Fatalf("fork = %+v, actor = %+v", fork, actor)
	}
	actor.Status.RuntimeStatus.BoundaryRef = "s3://snapshots/later-turn"
	replayed, err := NewTaskWorkflow(store, actors).Create(t.Context(), session)
	require.NoError(t, err)
	require.True(t, proto.Equal(fork, replayed), "a retry returns the current session without revalidating later Actor state")
}

// lifecycleFixture uses the same persistence boundary as production; only Actor
// calls are faked, so concurrency assertions exercise PostgreSQL admission.
func lifecycleFixture(t *testing.T) (*lifecycleTestStore, *apiv1alpha1.Session) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	t.Cleanup(cancel)
	conn, cleanup, err := dbtest.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NoError(t, dbtest.Migrate(conn, false))
	pool, err := pgxpool.New(t.Context(), conn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	client := database.NewClient(pool)
	revision := &database.RuntimeRevision{
		Revision: "revision-1", Namespace: "team-a", AgentName: "assistant", AgentUID: "template-uid",
		SourceSnapshot: []byte("{}"),
		AgentCard:      &a2apb.AgentCard{Name: "assistant"}, EgressDestinations: []string{},
		PreparedRuntimeAtespace: "team-a", PreparedRuntimeName: "assistant-kagent-revision", PreparedRuntimeUID: "actor-template-uid",
	}
	require.NoError(t, client.UpsertAgentDefinition(t.Context(), database.AgentDefinition{Namespace: "team-a", AgentName: "assistant", AgentUID: "template-uid", DesiredRevision: revision.Revision}))
	require.NoError(t, client.RecordRuntimeRevision(t.Context(), *revision, true))
	session, _, err := client.CreateSession(t.Context(), &apiv1alpha1.Session{Id: uuid.NewString(), Creator: "alice", Agent: &apiv1alpha1.ResourceReference{Namespace: "team-a", Name: "assistant"}}, uuid.NewString())
	require.NoError(t, err)
	return &lifecycleTestStore{Client: client, revision: revision}, session
}

type lifecycleTestStore struct {
	*database.Client
	revision *database.RuntimeRevision
}

func (s *lifecycleTestStore) GetRuntimeRevision(context.Context, string) (*database.RuntimeRevision, error) {
	return s.revision, nil
}

type lifecycleTestActors struct {
	ax.AXClient
	mu          sync.Mutex
	actors      map[string]*ax.Task
	checkpoints map[string]*ax.TaskCheckpoint
	prepareErr  error
}

func actorKey(space, name string) string { return space + "/" + name }
func (a *lifecycleTestActors) GetPreparedRuntime(_ context.Context, req *ax.GetPreparedRuntimeRequest, _ ...grpc.CallOption) (*ax.PreparedRuntime, error) {
	if a.prepareErr != nil {
		return nil, a.prepareErr
	}
	return &ax.PreparedRuntime{Metadata: &ax.ObjectMeta{Atespace: req.Ref.Atespace, Name: req.Ref.Name, Uid: req.Ref.Uid}, Spec: &ax.PreparedRuntimeSpec{GroupRef: &ax.ResourceRef{Atespace: req.Ref.Atespace, Name: "default", Uid: "group-uid"}}, Phase: "Ready"}, nil
}
func (a *lifecycleTestActors) GetTask(_ context.Context, req *ax.GetTaskRequest, _ ...grpc.CallOption) (*ax.Task, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	task := a.actors[actorKey(req.Atespace, req.Name)]
	if task == nil {
		return nil, status.Error(codes.NotFound, "missing")
	}
	return proto.CloneOf(task), nil
}
func (a *lifecycleTestActors) CreateTask(_ context.Context, req *ax.CreateTaskRequest, _ ...grpc.CallOption) (*ax.Task, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := actorKey(req.Task.Metadata.Atespace, req.Task.Metadata.Name)
	if task := a.actors[key]; task != nil {
		if task.Status.RuntimeStatus.LastOperationId != req.RequestId {
			return nil, status.Error(codes.AlreadyExists, "different request")
		}
		return proto.CloneOf(task), nil
	}
	task := proto.CloneOf(req.Task)
	task.Metadata.Uid = uuid.NewString()
	task.Status = &ax.TaskStatus{RuntimeStatus: &ax.TaskRuntimeStatus{Phase: "Suspended", LastOperationId: req.RequestId, BoundaryRef: "boundary-1", Restorable: true}}
	a.actors[key] = task
	return proto.CloneOf(task), nil
}
func (a *lifecycleTestActors) transition(space, name, uid, id, phase string) (*ax.Task, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	task := a.actors[actorKey(space, name)]
	if task == nil {
		return nil, status.Error(codes.NotFound, "missing")
	}
	if task.Metadata.Uid != uid {
		return nil, status.Error(codes.FailedPrecondition, "UID changed")
	}
	task.Status.RuntimeStatus.Phase = phase
	task.Status.RuntimeStatus.LastOperationId = id
	if phase == "Suspended" {
		task.Status.RuntimeStatus.BoundaryRef = "boundary-1"
		task.Status.RuntimeStatus.Restorable = true
	}
	return proto.CloneOf(task), nil
}
func (a *lifecycleTestActors) ResumeTask(_ context.Context, r *ax.ResumeTaskRequest, _ ...grpc.CallOption) (*ax.Task, error) {
	return a.transition(r.Atespace, r.Name, r.ExpectedUid, r.OperationId, "Running")
}
func (a *lifecycleTestActors) SuspendTask(_ context.Context, r *ax.SuspendTaskRequest, _ ...grpc.CallOption) (*ax.Task, error) {
	return a.transition(r.Atespace, r.Name, r.ExpectedUid, r.OperationId, "Suspended")
}
func (a *lifecycleTestActors) PauseTask(_ context.Context, r *ax.PauseTaskRequest, _ ...grpc.CallOption) (*ax.Task, error) {
	return a.transition(r.Ref.Atespace, r.Ref.Name, r.Ref.Uid, r.OperationId, "Paused")
}
func (a *lifecycleTestActors) DeleteTask(_ context.Context, r *ax.DeleteTaskRequest, _ ...grpc.CallOption) (*ax.DeleteTaskResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := actorKey(r.Atespace, r.Name)
	if task := a.actors[key]; task != nil && task.Metadata.Uid != r.ExpectedUid {
		return nil, status.Error(codes.FailedPrecondition, "UID changed")
	}
	delete(a.actors, key)
	return &ax.DeleteTaskResponse{}, nil
}
func (a *lifecycleTestActors) GetTaskCheckpoint(_ context.Context, r *ax.GetTaskCheckpointRequest, _ ...grpc.CallOption) (*ax.TaskCheckpoint, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	checkpoint := a.checkpoints[r.Ref.Name]
	if checkpoint == nil || checkpoint.Metadata.Uid != r.Ref.Uid {
		return nil, status.Error(codes.NotFound, "missing")
	}
	return proto.CloneOf(checkpoint), nil
}
func TestQuiesceRejectsWrongTaskIdentity(t *testing.T) {
	store, session := lifecycleFixture(t)
	runtime := &lifecycleTestActors{actors: map[string]*ax.Task{}}
	session, err := NewTaskWorkflow(store, runtime).Create(t.Context(), session)
	require.NoError(t, err)
	runtime.actors[actorKey("team-a", axruntime.TaskName(session.Id))].Metadata.Name = "different-task"
	_, err = NewTaskWorkflow(store, runtime).Quiesce(t.Context(), session)
	require.Error(t, err)
}

// lifecycleForkFixture retains a real checkpoint and its independent fork history.
func lifecycleForkFixture(t *testing.T, store *lifecycleTestStore, actors *lifecycleTestActors, source *apiv1alpha1.Session) (*apiv1alpha1.Session, string) {
	t.Helper()
	source, err := NewTaskWorkflow(store, actors).Create(t.Context(), source)
	require.NoError(t, err)
	message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("hello"))
	message.ContextID = source.ContextId
	task := a2a.NewSubmittedTask(message, message)
	createHash := sha256.Sum256([]byte("fixture-create"))
	initialVersion, err := store.CreateRuntimeTask(t.Context(), source.Id, createHash[:], task, "")
	require.NoError(t, err)
	task.Status.State = a2a.TaskStateCompleted
	hash := sha256.Sum256([]byte("fixture-complete"))
	version, err := store.UpdateSessionTask(t.Context(), source.Id, initialVersion, hash[:], task, task, "")
	require.NoError(t, err)
	require.NoError(t, store.SettleSessionTask(t.Context(), source.Id, string(task.ID), version))
	boundary, err := store.ClaimSessionQuiescence(t.Context())
	require.NoError(t, err)
	require.NoError(t, store.FinishSessionQuiescence(t.Context(), boundary,
		&database.SessionTaskSnapshot{Atespace: "team-a", Reference: "s3://snapshots/source", ContentScope: "DATA"}))
	checkpoint, _, err := store.ReserveSessionCheckpoint(t.Context(), &apiv1alpha1.Checkpoint{Id: uuid.NewString(), SessionId: source.Id, HeadTaskId: string(task.ID)}, source.Creator, uuid.NewString())
	require.NoError(t, err)
	sourceTask := actors.actors[actorKey("team-a", axruntime.TaskName(source.Id))]
	ref := axruntime.ReferenceFromTask(sourceTask)
	ref.Checkpoint = &ax.ResourceRef{Atespace: "team-a", Name: "checkpoint-" + checkpoint.Id, Uid: "checkpoint-uid"}
	encoded, err := ref.Encode()
	require.NoError(t, err)
	if actors.checkpoints == nil {
		actors.checkpoints = map[string]*ax.TaskCheckpoint{}
	}
	actors.checkpoints[ref.Checkpoint.Name] = &ax.TaskCheckpoint{Metadata: &ax.ObjectMeta{Atespace: "team-a", Name: ref.Checkpoint.Name, Uid: ref.Checkpoint.Uid}, SourceTask: ref.Task, RuntimeRef: ref.Runtime, GroupRef: ref.Group, BoundaryRef: ref.BoundaryRef, Phase: "Ready"}
	_, err = store.FinalizeSessionCheckpoint(t.Context(), checkpoint.Id, "checkpoint-uid", encoded, "")
	require.NoError(t, err)
	requestID := uuid.NewString()
	fork, _, err := store.ForkSession(t.Context(), checkpoint.Id, source.Creator, requestID, uuid.NewString())
	require.NoError(t, err)
	// An ordinary Create must not reuse a fork request ID, even for the same pair.
	_, _, err = store.CreateSession(t.Context(), &apiv1alpha1.Session{Id: uuid.NewString(), Creator: source.Creator, Agent: source.Agent, Name: fork.Name}, requestID)
	require.ErrorIs(t, err, database.ErrIdempotencyConflict)
	return fork, checkpoint.Id
}

func TestTaskCreationRetainsPreparationFailure(t *testing.T) {
	store, session := lifecycleFixture(t)
	runtime := &lifecycleTestActors{actors: map[string]*ax.Task{}, prepareErr: status.Error(codes.Unavailable, "AX not ready")}
	_, err := NewTaskWorkflow(store, runtime).Create(t.Context(), session)
	require.Error(t, err)
	require.Empty(t, runtime.actors)
	runtime.prepareErr = nil
	_, err = NewTaskWorkflow(store, runtime).Create(t.Context(), session)
	require.NoError(t, err)
	require.Len(t, runtime.actors, 1)
}

func TestServiceLifecycleRetriesUseCurrentStateAndRespectDeletion(t *testing.T) {
	store, fixture := lifecycleFixture(t)
	actors := &retryTestActors{lifecycleTestActors: &lifecycleTestActors{actors: map[string]*ax.Task{}}}
	service := NewService(store, serviceTestAuthorizer{}, NewTaskWorkflow(store, actors))
	ctx := serviceTestContext("alice")
	session, err := service.Create(ctx, fixture.Agent, "retry-request", "conversation")
	require.NoError(t, err)
	suspended, err := service.Suspend(ctx, session.Id)
	require.NoError(t, err)
	mutations := actors.mutations.Load()
	retried, err := service.Create(ctx, fixture.Agent, "retry-request", "ignored retry name")
	require.NoError(t, err)
	require.Equal(t, suspended.Id, retried.Id)
	require.Equal(t, suspended.State, retried.State)
	require.Equal(t, mutations, actors.mutations.Load(), "creation retry cannot reissue runtime work")
	deleted, err := service.Delete(ctx, session.Id)
	require.NoError(t, err)
	require.Equal(t, apiv1alpha1.RuntimeState_RUNTIME_STATE_DELETED, deleted.State)
	require.Empty(t, deleted.A2AAuthority)
	mutations = actors.mutations.Load()
	_, err = service.Create(ctx, fixture.Agent, "retry-request", "")
	require.True(t, serviceerrors.IsCode(err, serviceerrors.CodeFailedPrecondition))
	_, err = service.Get(ctx, session.Id)
	require.True(t, serviceerrors.IsCode(err, serviceerrors.CodeNotFound))
	_, err = service.Delete(ctx, session.Id)
	require.True(t, serviceerrors.IsCode(err, serviceerrors.CodeNotFound))
	_, err = service.Resume(ctx, session.Id)
	require.True(t, serviceerrors.IsCode(err, serviceerrors.CodeNotFound))
	require.Equal(t, mutations, actors.mutations.Load(), "a tombstoned request must not create or touch compute")
}

type asynchronousBoundary struct {
	*lifecycleTestActors
	pending   bool
	refreshed int
}

func (a *asynchronousBoundary) SuspendTask(ctx context.Context, req *ax.SuspendTaskRequest, opts ...grpc.CallOption) (*ax.Task, error) {
	task, err := a.lifecycleTestActors.SuspendTask(ctx, req, opts...)
	if err == nil {
		a.pending = true
		task = proto.CloneOf(task)
		task.Status.RuntimeStatus.Phase = "Transitioning"
	}
	return task, err
}
func (a *asynchronousBoundary) GetTask(ctx context.Context, req *ax.GetTaskRequest, opts ...grpc.CallOption) (*ax.Task, error) {
	if a.pending {
		a.refreshed++
		requireRefresh := req.RefreshRuntime
		if !requireRefresh {
			return nil, status.Error(codes.InvalidArgument, "refresh required")
		}
	}
	return a.lifecycleTestActors.GetTask(ctx, req, opts...)
}
func TestQuiescenceWaitsForAcknowledgedAXBoundary(t *testing.T) {
	store, session := lifecycleFixture(t)
	runtime := &asynchronousBoundary{lifecycleTestActors: &lifecycleTestActors{actors: map[string]*ax.Task{}}}
	workflow := NewTaskWorkflow(store, runtime)
	created, err := workflow.Create(t.Context(), session)
	require.NoError(t, err)
	runtime.actors[actorKey("team-a", axruntime.TaskName(session.Id))].Status.RuntimeStatus.Phase = "Running"
	boundary, err := workflow.Quiesce(t.Context(), created)
	require.NoError(t, err)
	require.NotEmpty(t, boundary.Reference)
	require.Positive(t, runtime.refreshed)
}
