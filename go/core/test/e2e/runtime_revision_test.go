package e2e_test

import (
	"context"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"strings"
	"testing"
	"time"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	"github.com/google/uuid"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"github.com/kagent-dev/kagent/go/api/v1alpha3"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// TestRuntimeRevisionLifecycle exercises actual AX runtimes through
// invalid edits, template retirement, checkpoint retention, and later preparation.
func TestRuntimeRevisionLifecycle(t *testing.T) {
	t.Parallel()
	forEachHarness(t, func(t *testing.T, harness testHarness) {
		// Each harness owns its revisions; overlap their periodic GC waits.
		t.Parallel()
		target := interactionTarget(t)
		modelURL := startInteractionMock(t)
		templateName := createInteractionTemplate(t, harness, modelURL)
		kube := interactionKubeClient(t)
		conn := newControllerConn(t, target)
		ctx, cancel := context.WithTimeout(metadata.AppendToOutgoingContext(t.Context(), "x-user-id", "e2e"), 6*time.Minute)
		t.Cleanup(cancel)
		sessions := apiv1alpha1.NewSessionServiceClient(conn)
		checkpoints := apiv1alpha1.NewCheckpointServiceClient(conn)
		runtime := newAXRuntimeClient(t)
		request := func(name string) *apiv1alpha1.CreateSessionRequest {
			return &apiv1alpha1.CreateSessionRequest{
				Agent:     &apiv1alpha1.ResourceReference{Namespace: "kagent", Name: name},
				RequestId: uuid.NewString(),
			}
		}
		deleteSession := func(id string) {
			t.Helper()
			cleanupCtx, cleanupCancel := context.WithTimeout(metadata.AppendToOutgoingContext(context.Background(), "x-user-id", "e2e"), time.Minute)
			defer cleanupCancel()
			require.NoError(t, deleteIdleSession(cleanupCtx, sessions, id))
		}
		send := func(id string) string {
			t.Helper()
			session, err := sessions.GetSession(ctx, &apiv1alpha1.GetSessionRequest{SessionId: id})
			require.NoError(t, err)
			ref := session.GetSession().GetAgent()
			fixture := &interactionFixture{
				ctx: ctx, client: a2apb.NewA2AServiceClient(conn), sessionID: id, contextID: id, tenant: ref.GetNamespace() + "/" + ref.GetName(),
			}
			_, _, task := fixture.send(t, "What is 2+2?")
			require.Equal(t, a2atype.TaskStateCompleted, task.Status.State)
			require.True(t, strings.Contains(taskText(task), "The answer is 4."))
			return string(task.ID)
		}
		// No FailedPrecondition retry: Ready must already be backed by a persisted
		// successful revision when the first create request arrives.
		created, err := sessions.CreateSession(ctx, request(templateName))
		require.NoError(t, err)
		source := created.GetSession()
		t.Cleanup(func() { deleteSession(source.GetId()) })
		sourceTaskID := send(source.GetId())

		// Observe immutable AX identities before releasing business references.
		task, err := findAXTask(ctx, runtime, source.GetAgent().GetNamespace(), "session-"+source.GetId())
		require.NoError(t, err)
		require.NotNil(t, task)
		runtimeRef := task.GetSpec().GetPreparedRuntimeRef()
		require.NotEmpty(t, runtimeRef.GetUid())
		prepared, err := runtime.GetPreparedRuntime(ctx, &ax.GetPreparedRuntimeRequest{Ref: runtimeRef})
		require.NoError(t, err)
		require.Equal(t, "Ready", prepared.GetPhase())

		template := &v1alpha3.AgentTemplate{}
		require.NoError(t, kube.Get(ctx, ctrlclient.ObjectKey{Namespace: "kagent", Name: templateName}, template))
		originalModelName := template.Spec.ModelConfig.Name
		template.Spec.ModelConfig.Name = "missing-" + uuid.NewString()
		require.NoError(t, kube.Update(ctx, template))
		require.NoError(t, wait.PollUntilContextTimeout(ctx, time.Second, time.Minute, true, func(ctx context.Context) (bool, error) {
			current := &v1alpha3.Agent{}
			if err := kube.Get(ctx, ctrlclient.ObjectKeyFromObject(template), current); err != nil {
				return false, err
			}
			for _, condition := range current.Status.Conditions {
				if condition.Type == v1alpha3.AgentConditionReady {
					return condition.Status == metav1.ConditionFalse, nil
				}
			}
			return false, nil
		}))
		fallback, err := sessions.CreateSession(ctx, request(templateName))
		require.NoError(t, err, "an invalid edit must preserve the current UID's last-good runtime")
		t.Cleanup(func() { deleteSession(fallback.GetSession().GetId()) })
		require.Equal(t, source.GetPreparedRevision(), fallback.GetSession().GetPreparedRevision())
		send(fallback.GetSession().GetId())
		deleteSession(fallback.GetSession().GetId())

		checkpointResponse := createCheckpoint(t, ctx, checkpoints, &apiv1alpha1.CreateCheckpointRequest{SessionId: source.GetId(), RequestId: uuid.NewString(), ExpectedHeadTaskId: sourceTaskID})
		checkpointID := checkpointResponse.GetCheckpoint().GetId()
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(metadata.AppendToOutgoingContext(context.Background(), "x-user-id", "e2e"), time.Minute)
			defer cleanupCancel()
			_, err := checkpoints.DeleteCheckpoint(cleanupCtx, &apiv1alpha1.DeleteCheckpointRequest{CheckpointId: checkpointID})
			if status.Code(err) != codes.NotFound {
				require.NoError(t, err)
			}
		})
		deleteSession(source.GetId())
		require.NoError(t, kube.Delete(ctx, template))
		agent := &v1alpha3.Agent{ObjectMeta: metav1.ObjectMeta{Namespace: template.Namespace, Name: template.Name}}
		require.NoError(t, kube.Get(ctx, ctrlclient.ObjectKeyFromObject(agent), agent))
		require.NoError(t, kube.Delete(ctx, agent))
		// Wait for the controller to retire the pair, using the same public create
		// path as a client. Dispose of any session created before retirement wins.
		require.NoError(t, wait.PollUntilContextTimeout(ctx, time.Second, time.Minute, true, func(ctx context.Context) (bool, error) {
			response, err := sessions.CreateSession(ctx, request(templateName))
			if status.Code(err) == codes.FailedPrecondition {
				return true, nil
			}
			if err != nil {
				return false, err
			}
			deleteSession(response.GetSession().GetId())
			return false, nil
		}))
		forked, err := checkpoints.ForkSession(ctx, &apiv1alpha1.ForkSessionRequest{CheckpointId: checkpointID, RequestId: uuid.NewString()})
		require.NoError(t, err, "a checkpoint must retain runnable inputs after its source and template are deleted")
		forkID := forked.GetSession().GetId()
		t.Cleanup(func() { deleteSession(forkID) })
		send(forkID)
		deleteSession(forkID)
		_, err = checkpoints.DeleteCheckpoint(ctx, &apiv1alpha1.DeleteCheckpointRequest{CheckpointId: checkpointID})
		require.NoError(t, err)

		// GC must remove the template after the final
		// checkpoint disappears, without another template event to drive cleanup.
		require.NoError(t, wait.PollUntilContextTimeout(ctx, time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
			_, err := runtime.GetPreparedRuntime(ctx, &ax.GetPreparedRuntimeRequest{Ref: runtimeRef})
			if status.Code(err) == codes.NotFound {
				return true, nil
			}
			return false, err
		}), "final checkpoint deletion must eventually collect its runtime without template changes")

		// Recreating the name must prepare a new identity after collection.
		replacement := template.DeepCopy()
		replacement.ObjectMeta = metav1.ObjectMeta{Namespace: template.Namespace, Name: template.Name, Labels: template.Labels}
		replacement.Spec.ModelConfig.Name = originalModelName
		createAndWaitInteractionTemplate(t, harness, kube, replacement)
		require.NotEqual(t, template.UID, replacement.UID)
		next := replacement.Name
		nextSession, err := sessions.CreateSession(ctx, request(next))
		require.NoError(t, err)
		t.Cleanup(func() { deleteSession(nextSession.GetSession().GetId()) })
		send(nextSession.GetSession().GetId())
	})
}
