package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"github.com/kagent-dev/kagent/go/core/internal/axruntime"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/service/serviceerrors"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type testSession struct{ userID string }

func (s testSession) Principal() auth.Principal { return auth.Principal{User: auth.User{ID: s.userID}} }

type testAuthorizer struct{}

func (testAuthorizer) Check(context.Context, auth.Principal, auth.Verb, auth.Resource) error {
	return nil
}

type testStore struct {
	prepared    *apiv1alpha1.Checkpoint
	snapshot    *database.SessionTaskSnapshot
	tagUID      string
	snapshotErr error
	forked      *apiv1alpha1.Session
	failed      string
	deleted     bool
	finalizeErr error
	reserveErr  error
}

func (s *testStore) ReserveSessionCheckpoint(_ context.Context, checkpoint *apiv1alpha1.Checkpoint, _, _ string) (*apiv1alpha1.Checkpoint, *database.SessionTaskSnapshot, error) {
	if s.reserveErr != nil {
		return nil, nil, s.reserveErr
	}
	if s.prepared != nil {
		return s.prepared, s.snapshot, nil
	}
	checkpoint.HeadTaskId = "task-1"
	checkpoint.HistorySequence = 7
	ref := checkpointSource(checkpoint.SessionId)
	encoded, err := ref.Encode()
	if err != nil {
		return nil, nil, err
	}
	s.snapshot = &database.SessionTaskSnapshot{Atespace: "team-a", Reference: encoded, ContentScope: "DATA"}
	checkpoint.State = apiv1alpha1.CheckpointState_CHECKPOINT_STATE_CREATING
	checkpoint.CreatedAt = timestamppb.Now()
	s.prepared = checkpoint
	return checkpoint, s.snapshot, nil
}

func (s *testStore) FinalizeSessionCheckpoint(_ context.Context, _ string, tagUID, snapshotURI, failure string) (*apiv1alpha1.Checkpoint, error) {
	if s.finalizeErr != nil {
		return nil, s.finalizeErr
	}
	if failure != "" {
		s.prepared.State, s.failed = apiv1alpha1.CheckpointState_CHECKPOINT_STATE_FAILED, failure
	} else {
		s.prepared.State, s.tagUID = apiv1alpha1.CheckpointState_CHECKPOINT_STATE_READY, tagUID
		s.snapshot.Reference = snapshotURI
	}
	return s.prepared, nil
}

func (s *testStore) GetSessionCheckpoint(context.Context, string, string) (*apiv1alpha1.Checkpoint, error) {
	if s.prepared == nil {
		return nil, database.ErrNotFound
	}
	return s.prepared, nil
}

func (s *testStore) GetSessionCheckpointSnapshot(context.Context, string, string) (*database.SessionTaskSnapshot, string, error) {
	if s.snapshotErr != nil {
		return nil, "", s.snapshotErr
	}
	if s.prepared == nil {
		return nil, "", database.ErrNotFound
	}
	return s.snapshot, s.tagUID, nil
}

func (*testStore) ListSessionCheckpoints(context.Context, string, string, string, int) ([]*apiv1alpha1.Checkpoint, error) {
	return nil, nil
}

func (s *testStore) BeginDeleteSessionCheckpoint(context.Context, string, string) (*database.SessionTaskSnapshot, string, error) {
	if s.prepared == nil {
		return nil, "", database.ErrNotFound
	}
	s.prepared.State = apiv1alpha1.CheckpointState_CHECKPOINT_STATE_DELETING
	return s.snapshot, s.tagUID, nil
}

func (s *testStore) DeleteSessionCheckpoint(context.Context, string, string) error {
	s.deleted = true
	return nil
}

func (s *testStore) UpdateCheckpointName(_ context.Context, _, _, name string) (*apiv1alpha1.Checkpoint, error) {
	if s.prepared == nil {
		return nil, database.ErrNotFound
	}
	s.prepared.Name = name
	return s.prepared, nil
}

func (s *testStore) ForkSession(_ context.Context, _ string, userID, _ string, sessionID string) (*apiv1alpha1.Session, bool, error) {
	if s.forked == nil {
		s.forked = &apiv1alpha1.Session{
			Id: sessionID, Creator: userID,
			State: apiv1alpha1.RuntimeState_RUNTIME_STATE_CREATING,
		}
		return s.forked, true, nil
	}
	return s.forked, false, nil
}

type testWorkflow struct {
	session *apiv1alpha1.Session
}

func (w *testWorkflow) Create(_ context.Context, session *apiv1alpha1.Session) (*apiv1alpha1.Session, error) {
	w.session = session
	session.State = apiv1alpha1.RuntimeState_RUNTIME_STATE_READY
	return session, nil
}

type testTags struct {
	ax.AXClient
	createCalls, deleteCalls     int
	created                      *ax.TaskCheckpoint
	createErr, deleteErr, getErr error
	mutate                       func(*ax.TaskCheckpoint)
}

func checkpointSource(id string) axruntime.Reference {
	return axruntime.Reference{Version: 1, Task: &ax.ResourceRef{Atespace: "team-a", Name: axruntime.TaskName(id), Uid: "task-uid"}, Runtime: &ax.ResourceRef{Atespace: "team-a", Name: "runtime", Uid: "runtime-uid"}, Group: &ax.ResourceRef{Atespace: "team-a", Name: "default", Uid: "group-uid"}, BoundaryRef: "boundary-1"}
}
func (t *testTags) CreateTaskCheckpoint(_ context.Context, r *ax.CreateTaskCheckpointRequest, _ ...grpc.CallOption) (*ax.TaskCheckpoint, error) {
	if t.created == nil {
		t.createCalls++
		ref := checkpointSource("")
		ref.Task = r.TaskRef
		t.created = &ax.TaskCheckpoint{Metadata: &ax.ObjectMeta{Atespace: r.TaskRef.Atespace, Name: r.Name, Uid: "checkpoint-uid"}, SourceTask: proto.CloneOf(r.TaskRef), RuntimeRef: ref.Runtime, GroupRef: ref.Group, BoundaryRef: r.ExpectedBoundaryRef, Phase: "Ready"}
		if t.mutate != nil {
			t.mutate(t.created)
		}
	}
	if t.createErr != nil {
		return nil, t.createErr
	}
	return proto.CloneOf(t.created), nil
}
func (t *testTags) GetTaskCheckpoint(context.Context, *ax.GetTaskCheckpointRequest, ...grpc.CallOption) (*ax.TaskCheckpoint, error) {
	if t.getErr != nil {
		return nil, t.getErr
	}
	if t.created == nil {
		return nil, status.Error(codes.NotFound, "missing")
	}
	return proto.CloneOf(t.created), nil
}
func (t *testTags) DeleteTaskCheckpoint(_ context.Context, r *ax.DeleteTaskCheckpointRequest, _ ...grpc.CallOption) (*ax.DeleteTaskCheckpointResponse, error) {
	t.deleteCalls++
	if t.deleteErr != nil {
		return nil, t.deleteErr
	}
	if t.created != nil && !proto.Equal(ax.Ref(t.created.Metadata), r.Ref) {
		return nil, status.Error(codes.FailedPrecondition, "UID changed")
	}
	t.created = nil
	return &ax.DeleteTaskCheckpointResponse{}, nil
}

func TestCreatePreservesStoreConflictReason(t *testing.T) {
	for _, cause := range []error{database.ErrConflict, database.ErrFailedPrecondition} {
		t.Run(cause.Error(), func(t *testing.T) {
			storeErr := fmt.Errorf("Session cannot checkpoint in its current state: %w", cause)
			service := NewService(&testStore{reserveErr: storeErr}, testAuthorizer{}, nil, nil)
			ctx := auth.AuthSessionTo(t.Context(), testSession{userID: "alice"})
			_, err := service.Create(ctx, "018f47a2-4efb-7c21-a848-123456789abc", "request-1", "task-1")
			require.Equal(t, serviceerrors.CodeFailedPrecondition, serviceerrors.CodeOf(err))
			require.Equal(t, storeErr.Error(), serviceerrors.MessageOf(err))
			require.ErrorIs(t, err, cause)
		})
	}
}

func TestCreateDistinguishesPendingSnapshotFromAdvancedConversation(t *testing.T) {
	for _, test := range []struct {
		err    error
		reason string
	}{
		{database.ErrSnapshotPending, "KAGENT_CHECKPOINT_SNAPSHOT_PENDING"},
		{database.ErrCheckpointAdvanced, "KAGENT_CHECKPOINT_CONVERSATION_ADVANCED"},
	} {
		service := NewService(&testStore{reserveErr: test.err}, testAuthorizer{}, &testTags{}, &testWorkflow{})
		ctx := auth.AuthSessionTo(t.Context(), testSession{userID: "alice"})
		_, err := service.Create(ctx, "018f47a2-4efb-7c21-a848-123456789abc", "request", "task-a")
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		details := status.Convert(err).Details()
		require.Len(t, details, 1)
		info := details[0].(*errdetails.ErrorInfo)
		require.Equal(t, test.reason, info.Reason)
		require.Equal(t, "kagent.dev", info.Domain)
	}
}

func TestRenameValidatesBeforeReachingTheStore(t *testing.T) {
	store := &testStore{}
	service := NewService(store, testAuthorizer{}, nil, nil)
	ctx := auth.AuthSessionTo(t.Context(), testSession{userID: "alice"})
	const checkpointID = "018f47a2-4efb-7c21-a848-123456789abc"

	_, err := service.Rename(ctx, "not-a-uuid", "Before the detour")
	require.Equal(t, serviceerrors.CodeInvalidArgument, serviceerrors.CodeOf(err))

	_, err = service.Rename(ctx, checkpointID, "Before the detour")
	require.Equal(t, serviceerrors.CodeNotFound, serviceerrors.CodeOf(err))

	store.prepared = &apiv1alpha1.Checkpoint{Id: checkpointID}
	renamed, err := service.Rename(ctx, checkpointID, "Before the detour")
	require.NoError(t, err)
	require.Equal(t, "Before the detour", renamed.GetName())
}

const sourceSessionID = "018f47a2-4efb-7c21-a848-123456789abc"

func checkpointContext(t *testing.T) context.Context {
	return auth.AuthSessionTo(t.Context(), testSession{userID: "alice"})
}
func TestCreateRetainsRecordedAXBoundary(t *testing.T) {
	store := &testStore{snapshotErr: errors.New("must use reserved boundary")}
	runtime := &testTags{}
	checkpoint, err := NewService(store, testAuthorizer{}, runtime, nil).Create(checkpointContext(t), sourceSessionID, "request", "task-1")
	require.NoError(t, err)
	require.EqualValues(t, 7, checkpoint.HistorySequence)
	require.Equal(t, "task-1", checkpoint.HeadTaskId)
	ref, err := axruntime.DecodeReference(store.snapshot.Reference)
	require.NoError(t, err)
	require.True(t, proto.Equal(ref.Task, checkpointSource(sourceSessionID).Task))
	require.Equal(t, "boundary-1", ref.BoundaryRef)
	require.Equal(t, "checkpoint-uid", ref.Checkpoint.Uid)
	require.Equal(t, apiv1alpha1.CheckpointState_CHECKPOINT_STATE_READY, checkpoint.State)
}
func TestCheckpointUnknownResponseAndPublishFailureRetainIntent(t *testing.T) {
	store := &testStore{finalizeErr: errors.New("database unavailable")}
	runtime := &testTags{createErr: status.Error(codes.Unavailable, "response lost")}
	for attempt := 0; attempt < 3; attempt++ {
		if attempt == 1 {
			runtime.createErr = nil
		}
		if attempt == 2 {
			store.finalizeErr = nil
		}
		checkpoint, err := NewService(store, testAuthorizer{}, runtime, nil).Create(checkpointContext(t), sourceSessionID, "request", "task-1")
		if attempt < 2 {
			require.Error(t, err)
			require.Equal(t, apiv1alpha1.CheckpointState_CHECKPOINT_STATE_CREATING, store.prepared.State)
		} else {
			require.NoError(t, err)
			require.Equal(t, apiv1alpha1.CheckpointState_CHECKPOINT_STATE_READY, checkpoint.State)
		}
		require.Zero(t, runtime.deleteCalls)
		require.Equal(t, 1, runtime.createCalls)
	}
}
func TestDeleteClosesAdmissionAndRetainsOnAXFailure(t *testing.T) {
	store := &testStore{}
	runtime := &testTags{}
	service := NewService(store, testAuthorizer{}, runtime, nil)
	ctx := checkpointContext(t)
	checkpoint, err := service.Create(ctx, sourceSessionID, "request", "task-1")
	require.NoError(t, err)
	runtime.deleteErr = errors.New("AX unavailable")
	require.Error(t, service.Delete(ctx, checkpoint.Id))
	require.Equal(t, apiv1alpha1.CheckpointState_CHECKPOINT_STATE_DELETING, checkpoint.State)
	require.False(t, store.deleted)
	runtime.deleteErr = nil
	require.NoError(t, service.Delete(ctx, checkpoint.Id))
	require.True(t, store.deleted)
}
func TestForkUsesRetainedCheckpointWithoutSourceTask(t *testing.T) {
	store := &testStore{}
	runtime := &testTags{}
	workflow := &testWorkflow{}
	service := NewService(store, testAuthorizer{}, runtime, workflow)
	ctx := checkpointContext(t)
	checkpoint, err := service.Create(ctx, sourceSessionID, "request", "task-1")
	require.NoError(t, err)
	session, err := service.Fork(ctx, checkpoint.Id, "fork-request")
	require.NoError(t, err)
	require.Same(t, session, workflow.session)
	require.Equal(t, "alice", session.Creator)
}
func TestCheckpointRejectsChangedIdentityOrBoundary(t *testing.T) {
	mutations := map[string]func(*ax.TaskCheckpoint){
		"task UID":    func(c *ax.TaskCheckpoint) { c.SourceTask.Uid = "other" },
		"runtime UID": func(c *ax.TaskCheckpoint) { c.RuntimeRef.Uid = "other" },
		"group UID":   func(c *ax.TaskCheckpoint) { c.GroupRef.Uid = "other" },
		"boundary":    func(c *ax.TaskCheckpoint) { c.BoundaryRef = "other" },
		"incomplete":  func(c *ax.TaskCheckpoint) { c.Phase = "Preparing" },
		"namespace":   func(c *ax.TaskCheckpoint) { c.Metadata.Atespace = "other" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			store := &testStore{}
			runtime := &testTags{mutate: mutate}
			service := NewService(store, testAuthorizer{}, runtime, &testWorkflow{})
			ctx := checkpointContext(t)
			_, err := service.Create(ctx, sourceSessionID, "request", "task-1")
			require.Error(t, err)
			require.Zero(t, runtime.deleteCalls)
			require.Equal(t, apiv1alpha1.CheckpointState_CHECKPOINT_STATE_CREATING, store.prepared.State)
		})
	}
	for _, replace := range []func(*ax.TaskCheckpoint){func(c *ax.TaskCheckpoint) { c.Metadata.Uid = "replacement" }, mutations["boundary"]} {
		store := &testStore{}
		runtime := &testTags{}
		service := NewService(store, testAuthorizer{}, runtime, &testWorkflow{})
		ctx := checkpointContext(t)
		checkpoint, err := service.Create(ctx, sourceSessionID, "request", "task-1")
		require.NoError(t, err)
		replace(runtime.created)
		_, err = service.Fork(ctx, checkpoint.Id, "fork")
		require.Error(t, err)
		require.Nil(t, store.forked)
	}
}
func TestConcurrentCreateRetainsOneCheckpoint(t *testing.T) {
	store := &testStore{}
	backend := &testTags{mutate: func(*ax.TaskCheckpoint) { runtime.Gosched() }}
	service := NewService(store, testAuthorizer{}, backend, nil)
	ctx := checkpointContext(t)
	start := make(chan struct{})
	results := make(chan error, 32)
	for range cap(results) {
		go func() { <-start; _, err := service.Create(ctx, sourceSessionID, "request", "task-1"); results <- err }()
	}
	close(start)
	for range cap(results) {
		require.NoError(t, <-results)
	}
	require.Equal(t, 1, backend.createCalls)
	require.Zero(t, backend.deleteCalls)
}
