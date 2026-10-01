package grpcserver

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"net/http/httptest"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/uuid"
	api "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"github.com/kagent-dev/kagent/go/core/internal/axruntime"
	"github.com/kagent-dev/kagent/go/core/internal/axtest"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/dbtest"
	authimpl "github.com/kagent-dev/kagent/go/core/internal/httpserver/auth"
	"github.com/kagent-dev/kagent/go/core/internal/service/checkpoint"
	sessionsvc "github.com/kagent-dev/kagent/go/core/internal/service/session"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// Real kagent HTTPS -> AX mTLS with real PostgreSQL. The AX fixture controls
// compute observations: this verifies protocol, identity and history, not VM snapshots.
func TestCheckpointForkThroughAX(t *testing.T) {
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("x-user-id", "alice"))
	dsn := dbtest.StartT(t.Context(), t)
	dbtest.MigrateT(t, dsn, false)
	pool, err := database.Connect(ctx, &database.PostgresConfig{URL: dsn})
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	store := database.NewClient(pool)
	runtime := axtest.Start(t)
	group, err := runtime.CreateTaskGroup(ctx, &ax.CreateTaskGroupRequest{RequestId: "group-create", Group: &ax.TaskGroup{Metadata: &ax.ObjectMeta{Atespace: "team-a", Name: "agents"}, Spec: &ax.TaskGroupSpec{Replicas: proto.Int32(2), SandboxClass: "gvisor", SnapshotLocation: "s3://test/snapshots"}}})
	require.NoError(t, err)
	groupRef := &ax.ResourceRef{Atespace: "team-a", Name: "agents", Uid: group.Metadata.Uid}
	prepared, err := runtime.PrepareRuntime(ctx, &ax.PrepareRuntimeRequest{RequestId: "prepare", Metadata: &ax.ObjectMeta{Atespace: "team-a", Name: "revision"}, Spec: &ax.PreparedRuntimeSpec{Kind: "Service", GroupRef: groupRef, Image: "example/agent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}})
	require.NoError(t, err)
	require.NoError(t, store.UpsertAgentDefinition(ctx, database.AgentDefinition{Namespace: "team-a", AgentName: "assistant", AgentUID: "agent-uid", DesiredRevision: "revision-1"}))
	require.NoError(t, store.RecordRuntimeRevision(ctx, database.RuntimeRevision{Revision: "revision-1", Namespace: "team-a", AgentName: "assistant", AgentUID: "agent-uid", SourceSnapshot: []byte("{}"), AgentCard: &a2apb.AgentCard{Name: "assistant"}, PreparedRuntimeAtespace: "team-a", PreparedRuntimeName: prepared.Metadata.Name, PreparedRuntimeUID: prepared.Metadata.Uid}, true))
	workflow := sessionsvc.NewTaskWorkflow(store, runtime)
	sessions := sessionsvc.NewService(store, auth.NoopAuthorizer{}, workflow)
	checkpoints := checkpoint.NewService(store, auth.NoopAuthorizer{}, runtime, workflow)
	server, err := New(Config{SystemService: testSystemService(), Authenticator: &authimpl.InsecureAuthenticator{}, SessionService: sessions, CheckpointService: checkpoints})
	require.NoError(t, err)
	apiServer := httptest.NewUnstartedServer(server.HandlerOr(nil))
	apiServer.EnableHTTP2 = true
	apiServer.StartTLS()
	t.Cleanup(apiServer.Close)
	roots := x509.NewCertPool()
	roots.AddCert(apiServer.Certificate())
	conn, err := grpc.NewClient(apiServer.Listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	sessionRPC, checkpointRPC := api.NewSessionServiceClient(conn), api.NewCheckpointServiceClient(conn)
	created, err := sessionRPC.CreateSession(ctx, &api.CreateSessionRequest{Agent: &api.ResourceReference{Namespace: "team-a", Name: "assistant"}, RequestId: uuid.NewString()})
	require.NoError(t, err)
	source := created.Session
	_, err = sessionRPC.ResumeSession(ctx, &api.ResumeSessionRequest{SessionId: source.Id})
	require.NoError(t, err)
	// Session READY is admission state; A2A gateway normally performs lazy resume.
	bound, err := runtime.GetTask(ctx, &ax.GetTaskRequest{Atespace: "team-a", Name: axruntime.TaskName(source.Id)})
	require.NoError(t, err)
	_, err = runtime.ResumeTask(ctx, &ax.ResumeTaskRequest{Atespace: "team-a", Name: bound.Metadata.Name, ExpectedUid: bound.Metadata.Uid, OperationId: "gateway-test-resume"})
	require.NoError(t, err)
	message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("checkpoint this history"))
	message.ContextID = source.ContextId
	task := a2a.NewSubmittedTask(message, message)
	hash := sha256.Sum256([]byte("create"))
	version, err := store.CreateRuntimeTask(ctx, source.Id, hash[:], task, "")
	require.NoError(t, err)
	task.Status.State = a2a.TaskStateCompleted
	hash = sha256.Sum256([]byte("complete"))
	version, err = store.UpdateSessionTask(ctx, source.Id, version, hash[:], task, task, "")
	require.NoError(t, err)
	require.NoError(t, store.SettleSessionTask(ctx, source.Id, string(task.ID), version))
	work, err := store.ClaimSessionQuiescence(ctx)
	require.NoError(t, err)
	boundary, err := workflow.Quiesce(ctx, work.Session)
	require.NoError(t, err)
	require.NoError(t, store.FinishSessionQuiescence(ctx, work, boundary))
	request := &api.CreateCheckpointRequest{SessionId: source.Id, RequestId: uuid.NewString(), ExpectedHeadTaskId: string(task.ID)}
	saved, err := checkpointRPC.CreateCheckpoint(ctx, request)
	require.NoError(t, err)
	require.Equal(t, api.CheckpointState_CHECKPOINT_STATE_READY, saved.Checkpoint.State)
	replay, err := checkpointRPC.CreateCheckpoint(ctx, request)
	require.NoError(t, err)
	require.True(t, proto.Equal(saved, replay))
	forked, err := checkpointRPC.ForkSession(ctx, &api.ForkSessionRequest{CheckpointId: saved.Checkpoint.Id, RequestId: uuid.NewString()})
	require.NoError(t, err)
	require.NotEqual(t, source.Id, forked.Session.Id)
	sourceTask, err := runtime.GetTask(ctx, &ax.GetTaskRequest{Atespace: "team-a", Name: axruntime.TaskName(source.Id)})
	require.NoError(t, err)
	forkTask, err := runtime.GetTask(ctx, &ax.GetTaskRequest{Atespace: "team-a", Name: axruntime.TaskName(forked.Session.Id)})
	require.NoError(t, err)
	require.NotEqual(t, sourceTask.Metadata.Uid, forkTask.Metadata.Uid)
	require.Equal(t, "checkpoint-"+saved.Checkpoint.Id, forkTask.Spec.RestoreFrom.Name)
	require.True(t, proto.Equal(groupRef, forkTask.Spec.GroupRef))
	require.Equal(t, string(task.ID), saved.Checkpoint.HeadTaskId)
	inheritedTasks, total, err := store.ListSessionTasks(ctx, forked.Session.Id, "", a2a.TaskStateUnspecified, nil, 10, nil)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Len(t, inheritedTasks, 1)
	inherited := inheritedTasks[0]
	require.NotEqual(t, task.ID, inherited.ID)
	require.Equal(t, forked.Session.Id, inherited.ContextID)
	require.Equal(t, task.Status.State, inherited.Status.State)
	require.Equal(t, task.History[0].Parts[0].Text(), inherited.History[0].Parts[0].Text())

	_, err = sessionRPC.ResumeSession(ctx, &api.ResumeSessionRequest{SessionId: forked.Session.Id})
	require.NoError(t, err)
}
