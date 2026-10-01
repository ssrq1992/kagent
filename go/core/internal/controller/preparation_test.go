package controller

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/uuid"
	kagentfake "github.com/kagent-dev/kagent/go/api/clientset/versioned/fake"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	kagentv1alpha3 "github.com/kagent-dev/kagent/go/api/v1alpha3"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/dbtest"
	v2translator "github.com/kagent-dev/kagent/go/core/internal/translator"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"istio.io/istio/pkg/kube/krt"
	"istio.io/istio/pkg/kube/krt/krttest"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestUnresolvedPoolReleasesAbandonedRevision(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping database test in short mode")
	}
	for _, failed := range []bool{false, true} {
		name := "preparing revision"
		if failed {
			name = "failed revision"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			dsn := dbtest.StartT(ctx, t)
			dbtest.MigrateT(t, dsn, false)
			pool, err := database.Connect(ctx, &database.PostgresConfig{URL: dsn})
			require.NoError(t, err)
			t.Cleanup(pool.Close)
			store := database.NewClient(pool)
			collections, harnesses := newPreparationTestCollections(t, "gvisor")
			initial := collections.Reconciliations.List()[0]
			templates := &fakePreparedRuntimes{template: proto.CloneOf(initial.Target.Runtime)}
			templates.template.Metadata.Uid = "gvisor-uid"
			templates.template.Phase = "Ready"
			reconciler := &Reconciler{collections: collections, templates: templates, store: store}
			require.NoError(t, reconciler.reconcileAgent(ctx, initial.ResourceName()))

			updatedHarness := harnesses.List()[0].DeepCopy()
			updatedHarness.Spec.AX.TaskGroupRef.Name = "microvm"
			harnesses.UpdateObject(updatedHarness)
			waitFor(t, func() bool {
				return collections.Reconciliations.GetKey(initial.ResourceName()).Target.RevisionID != initial.Target.RevisionID
			})
			preparing := collections.Reconciliations.GetKey(initial.ResourceName())
			templates.template = nil
			require.NoError(t, reconciler.reconcileAgent(ctx, initial.ResourceName()))
			if failed {
				templates.template = proto.CloneOf(templates.template)
				templates.template.Phase, templates.template.Message = "Failed", "snapshot failed"
				require.NoError(t, reconciler.reconcileAgent(ctx, initial.ResourceName()))
				waitFor(t, func() bool {
					return collections.Reconciliations.GetKey(initial.ResourceName()).PreparationFailure != nil
				})
			}
			unreferenced, err := store.ListUnreferencedRuntimeRevisions(ctx)
			require.NoError(t, err)
			require.Empty(t, unreferenced)

			updatedHarness = updatedHarness.DeepCopy()
			updatedHarness.Spec.AX.TaskGroupRef.Name = "missing"
			harnesses.UpdateObject(updatedHarness)
			waitFor(t, func() bool {
				state := collections.Reconciliations.GetKey(initial.ResourceName())
				return state.CompilationFailure != nil && state.CompilationFailure.Reason == "TaskGroupNotFound"
			})
			unresolved := collections.Reconciliations.GetKey(initial.ResourceName())
			require.Nil(t, unresolved.Target)
			for range 2 {
				require.Equal(t, codes.NotFound, status.Code(errors.Unwrap(reconciler.reconcileAgent(ctx, initial.ResourceName()))))
			}
			require.Empty(t, collections.AgentRuntimeObservations.List())
			require.Empty(t, unresolved.desiredRevision())
			require.Equal(t, preparing.Target.Runtime.GetMetadata().GetName(), templates.template.GetMetadata().GetName(), "unresolved inputs must not create compute")

			// A delayed success for the abandoned preparation must not replace A.
			abandoned, err := store.GetRuntimeRevision(ctx, preparing.Target.RevisionID.String())
			require.NoError(t, err)
			require.NoError(t, store.RecordRuntimeRevision(ctx, *abandoned, true))
			unreferenced, err = store.ListUnreferencedRuntimeRevisions(ctx)
			require.NoError(t, err)
			require.Len(t, unreferenced, 1)
			require.Equal(t, preparing.Target.RevisionID.String(), unreferenced[0].Revision)
			retained, err := store.BeginRuntimeRevisionDeletion(ctx, initial.Target.RevisionID.String())
			require.NoError(t, err)
			require.Nil(t, retained, "last-successful A must remain protected without any sessions")

			session, _, err := store.CreateSession(ctx, &apiv1alpha1.Session{
				Id: uuid.NewString(), Creator: "alice",
				Agent: &apiv1alpha1.ResourceReference{Namespace: "team-a", Name: "assistant"},
			}, "last-good-session")
			require.NoError(t, err)
			require.Equal(t, initial.Target.RevisionID.String(), session.GetPreparedRevision())

			require.NoError(t, NewRuntimeRevisionGC(store, templates).collect(ctx, abandoned.Revision))
			require.Nil(t, templates.template)
			_, err = store.GetRuntimeRevision(ctx, abandoned.Revision)
			require.ErrorIs(t, err, database.ErrNotFound)
			_, err = store.GetRuntimeRevision(ctx, initial.Target.RevisionID.String())
			require.NoError(t, err)

			updatedHarness = updatedHarness.DeepCopy()
			updatedHarness.Spec.AX.TaskGroupRef.Name = "microvm"
			harnesses.UpdateObject(updatedHarness)
			waitFor(t, func() bool {
				state := collections.Reconciliations.GetKey(initial.ResourceName())
				return state.Target != nil && state.PreparationFailure == nil && state.Target.RevisionID == preparing.Target.RevisionID
			})
			require.NoError(t, reconciler.reconcileAgent(ctx, initial.ResourceName()), "restoring valid inputs must prepare the collected revision again")
			_, err = store.GetRuntimeRevision(ctx, abandoned.Revision)
			require.NoError(t, err)
		})
	}
}

func TestPreparationErrorsPublishStatusAndRecover(t *testing.T) {
	for _, test := range []struct {
		name      string
		templates fakePreparedRuntimes
		store     fakeRuntimeRevisionStore
		code      codes.Code
	}{
		{name: "missing sandbox config", templates: fakePreparedRuntimes{createErr: status.Error(codes.FailedPrecondition, `SandboxConfig "microvm" not found`)}, code: codes.FailedPrecondition},
		{name: "wrong sandbox class", templates: fakePreparedRuntimes{createErr: status.Error(codes.FailedPrecondition, `SandboxConfig "microvm" has class "gvisor" but sandbox_config.sandbox_class is "microvm"`)}, code: codes.FailedPrecondition},
		{name: "sensitive creation error", templates: fakePreparedRuntimes{createErr: status.Error(codes.FailedPrecondition, "private-backend-detail")}, code: codes.FailedPrecondition},
		{name: "get failed", templates: fakePreparedRuntimes{getErr: status.Error(codes.Unavailable, "private-endpoint")}, code: codes.Unavailable},
		{name: "get invalid argument", templates: fakePreparedRuntimes{getErr: status.Error(codes.InvalidArgument, "private-endpoint")}, code: codes.InvalidArgument},
		{name: "authorization failed", templates: fakePreparedRuntimes{getErr: status.Error(codes.PermissionDenied, "private-credential")}, code: codes.PermissionDenied},
		{name: "pair store failed", store: fakeRuntimeRevisionStore{pairErr: errors.New("private-database-connection")}, code: codes.Unknown},
		{name: "revision store failed", store: fakeRuntimeRevisionStore{revisionErr: errors.New("private-database-connection")}, code: codes.Unknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			collections, _ := newPreparationTestCollections(t, "microvm")
			initial := collections.Reconciliations.List()[0]
			statusClient := kagentfake.NewSimpleClientset(initial.Agent.DeepCopy()).ApiV1alpha3()
			reconciler := &Reconciler{collections: collections, templates: &test.templates, store: &test.store, status: statusClient}
			err := reconciler.reconcileAgent(t.Context(), initial.ResourceName())
			require.Error(t, err)
			require.Equal(t, test.code, status.Code(err))
			waitFor(t, func() bool {
				state := collections.Reconciliations.GetKey(initial.ResourceName())
				return state.PreparationFailure != nil && state.PreparationFailure.Retryable
			})
			waitFor(t, func() bool {
				updates := collections.AgentStatuses.List()
				if len(updates) != 1 {
					return false
				}
				ready := apimeta.FindStatusCondition(updates[0].Status.Conditions, kagentv1alpha3.AgentConditionReady)
				return ready != nil && ready.Reason == "RuntimePreparationFailed"
			})
			require.NoError(t, reconciler.reconcileAgentStatus(t.Context(), "team-a/assistant"))
			published, err := statusClient.Agents("team-a").Get(t.Context(), "assistant", metav1.GetOptions{})
			require.NoError(t, err)
			ready := apimeta.FindStatusCondition(published.Status.Conditions, kagentv1alpha3.AgentConditionReady)
			require.Equal(t, metav1.ConditionFalse, ready.Status)
			require.Equal(t, "RuntimePreparationFailed", ready.Reason)
			require.Contains(t, ready.Message, test.code.String())
			require.NotContains(t, ready.Message, "private-")
			if test.code == codes.FailedPrecondition {
				require.Contains(t, ready.Message, "AX runtime preparation failed")
				require.NotContains(t, ready.Message, "SandboxConfig")
			}
			observation := collections.AgentRuntimeObservations.GetKey(initial.ResourceName())
			require.Nil(t, observation.Template)
			require.Equal(t, initial.Target.RevisionID, observation.RevisionID)
			require.Error(t, reconciler.reconcileAgent(t.Context(), initial.ResourceName()), "publishing a failure must not prevent the next retry")

			test.templates.getErr, test.templates.createErr = nil, nil
			test.store.pairErr, test.store.revisionErr = nil, nil
			require.NoError(t, reconciler.reconcileAgent(t.Context(), initial.ResourceName()))
			waitFor(t, func() bool {
				state := collections.Reconciliations.GetKey(initial.ResourceName())
				return state.PreparationFailure == nil && state.ObservedRuntime != nil
			})
			require.Nil(t, collections.AgentRuntimeObservations.GetKey(initial.ResourceName()).Failure)
			test.templates.template = proto.CloneOf(test.templates.template)
			test.templates.template.Phase = "Ready"
			require.NoError(t, reconciler.reconcileAgent(t.Context(), initial.ResourceName()))
			waitFor(t, func() bool {
				harnessStatus := collections.AgentStatuses.List()[0].Status
				return apimeta.IsStatusConditionTrue(harnessStatus.Conditions, kagentv1alpha3.AgentConditionReady) &&
					harnessStatus.LatestSuccessfulRevision == initial.Target.RevisionID.String()
			})
		})
	}
}

func TestRuntimePreparationFailureSanitizesErrors(t *testing.T) {
	for _, code := range []codes.Code{codes.FailedPrecondition, codes.Unavailable, codes.PermissionDenied} {
		failure := runtimePreparationFailure(status.Error(code, "private-backend-detail"))
		require.Equal(t, kagentv1alpha3.AgentConditionReady, failure.Condition)
		require.True(t, failure.Retryable)
		require.Contains(t, failure.Message, "AX runtime preparation failed")
		require.NotContains(t, failure.Message, "private-backend-detail")
	}
}

func TestPreparationRevisionIsolation(t *testing.T) {
	collections, harnesses := newPreparationTestCollections(t, "microvm")
	initial := collections.Reconciliations.List()[0]
	templates := &fakePreparedRuntimes{createErr: status.Error(codes.FailedPrecondition, `SandboxConfig "microvm" not found`)}
	reconciler := &Reconciler{collections: collections, templates: templates, store: &fakeRuntimeRevisionStore{}}
	require.Error(t, reconciler.reconcileAgent(t.Context(), initial.ResourceName()))
	waitFor(t, func() bool {
		return collections.Reconciliations.GetKey(initial.ResourceName()).PreparationFailure != nil
	})

	updatedHarness := harnesses.List()[0].DeepCopy()
	updatedHarness.Spec.Workload.Args = []string{"changed"}
	harnesses.UpdateObject(updatedHarness)
	waitFor(t, func() bool {
		state := collections.Reconciliations.GetKey(initial.ResourceName())
		return state.Target.RevisionID != initial.Target.RevisionID && state.PreparationFailure == nil
	})
	templates.createErr = nil
	require.NoError(t, reconciler.reconcileAgent(t.Context(), initial.ResourceName()))
	observation := collections.AgentRuntimeObservations.GetKey(initial.ResourceName())
	require.NotEqual(t, initial.Target.RevisionID, observation.RevisionID)
	require.Nil(t, observation.Failure, "a new revision must not inherit an older preparation failure")
	harnesses.DeleteObject("team-a/byo")
	waitFor(t, func() bool {
		return collections.Reconciliations.GetKey(initial.ResourceName()).CompilationFailure != nil
	})
	require.NoError(t, reconciler.reconcileAgent(t.Context(), initial.ResourceName()))
	require.Empty(t, collections.AgentRuntimeObservations.List())
}

func TestPreparationQueueAndPollerBackoff(t *testing.T) {
	for _, test := range []struct {
		name      string
		templates fakePreparedRuntimes
		store     fakeRuntimeRevisionStore
	}{
		{name: "missing sandbox config", templates: fakePreparedRuntimes{createErr: status.Error(codes.FailedPrecondition, "missing config")}},
		{name: "revision being collected", store: fakeRuntimeRevisionStore{pairErr: database.ErrObjectDeleting}},
		{name: "persistence unavailable", store: fakeRuntimeRevisionStore{revisionErr: errors.New("database unavailable")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				collections, _ := newPreparationTestCollections(t, "microvm")
				initial := collections.Reconciliations.List()[0]
				statusClient := kagentfake.NewSimpleClientset(initial.Agent.DeepCopy()).ApiV1alpha3()
				backend := &preparationTestBackend{templates: &test.templates, store: &test.store}
				r := newReconciler(collections, backend, backend, statusClient)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				go r.Run(ctx.Done())
				synctest.Wait()
				backend.inspect(func() {
					require.Equal(t, 1, test.store.pairCalls, "publishing a failure must not immediately requeue it")
				})
				time.Sleep(10 * time.Second)
				synctest.Wait()
				backend.inspect(func() {
					require.Equal(t, 4, test.store.pairCalls, "only exponential backoff may retry failures")
				})
				time.Sleep(5 * time.Minute)
				synctest.Wait()
				var before int
				backend.inspect(func() {
					require.Greater(t, test.store.pairCalls, 10, "retries must survive the former attempt budget")
					before = test.store.pairCalls
				})
				time.Sleep(30 * time.Second)
				synctest.Wait()
				backend.inspect(func() {
					require.Equal(t, before+1, test.store.pairCalls, "the poller must not bypass capped backoff")
				})
				failed := collections.Reconciliations.GetKey(initial.ResourceName())
				require.Nil(t, failed.CompilationFailure)
				require.NotNil(t, failed.Target)
				require.NotNil(t, failed.PreparationFailure)

				// Recover without changing any Kubernetes input or manually queuing work.
				backend.inspect(func() {
					test.templates.createErr = nil
					test.store.pairErr, test.store.revisionErr = nil, nil
				})
				time.Sleep(30 * time.Second)
				synctest.Wait()
				require.Nil(t, collections.Reconciliations.GetKey(initial.ResourceName()).PreparationFailure)
				backend.inspect(func() {
					require.NotNil(t, test.templates.template)
					require.False(t, test.store.markedSuccessful)
					test.templates.template = proto.CloneOf(test.templates.template)
					test.templates.template.Phase = "Ready"
				})
				time.Sleep(time.Second)
				synctest.Wait()
				backend.inspect(func() {
					require.True(t, test.store.markedSuccessful, "polling must still observe pending golden snapshots")
					before = test.store.pairCalls
				})
				published, err := statusClient.Agents(initial.Agent.Namespace).Get(t.Context(), initial.Agent.Name, metav1.GetOptions{})
				require.NoError(t, err)
				require.True(t, apimeta.IsStatusConditionTrue(published.Status.Conditions, kagentv1alpha3.AgentConditionReady))
				time.Sleep(time.Minute)
				synctest.Wait()
				backend.inspect(func() {
					require.Equal(t, before, test.store.pairCalls, "a ready revision needs no more polling")
				})
				cancel()
				synctest.Wait()
				require.NoError(t, r.agents.WaitForClose(time.Second))
			})
		})
	}
}

// Waiting for quiescence synchronizes worker writes with test reads, but test
// mutations also need synchronization with the next timer-driven attempt.
type preparationTestBackend struct {
	ax.AXClient
	mu        sync.Mutex
	templates *fakePreparedRuntimes
	store     *fakeRuntimeRevisionStore
}

var _ ax.AXClient = (*preparationTestBackend)(nil)
var _ runtimeRevisionStore = (*preparationTestBackend)(nil)

func (p *preparationTestBackend) inspect(f func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f()
}

func (p *preparationTestBackend) GetTaskGroup(ctx context.Context, req *ax.GetTaskGroupRequest, opts ...grpc.CallOption) (*ax.TaskGroup, error) {
	// This fixture controls preparation only; group state is supplied by its KRT inputs.
	return nil, status.Error(codes.Unavailable, "group observation unavailable in preparation fixture")
}

func (p *preparationTestBackend) GetPreparedRuntime(ctx context.Context, req *ax.GetPreparedRuntimeRequest, opts ...grpc.CallOption) (*ax.PreparedRuntime, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.templates.GetPreparedRuntime(ctx, req, opts...)
}
func (p *preparationTestBackend) PrepareRuntime(ctx context.Context, req *ax.PrepareRuntimeRequest, opts ...grpc.CallOption) (*ax.PreparedRuntime, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.templates.PrepareRuntime(ctx, req, opts...)
}

func (p *preparationTestBackend) UpsertAgentDefinition(ctx context.Context, definition database.AgentDefinition) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.store.UpsertAgentDefinition(ctx, definition)
}

func (p *preparationTestBackend) RecordRuntimeRevision(ctx context.Context, revision database.RuntimeRevision, ready bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.store.RecordRuntimeRevision(ctx, revision, ready)
}

func (p *preparationTestBackend) RetireAgentIdentities(ctx context.Context, namespace, name string, except *database.AgentDefinition) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.store.RetireAgentIdentities(ctx, namespace, name, except)
}

func TestPreparationDesiredChangesBypassBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		collections, harnesses := newPreparationTestCollections(t, "microvm")
		initial := collections.Reconciliations.List()[0]
		templates := &fakePreparedRuntimes{createErr: status.Error(codes.FailedPrecondition, "missing config")}
		store := &fakeRuntimeRevisionStore{}
		r := newReconciler(collections, templates, store, kagentfake.NewSimpleClientset(initial.Agent.DeepCopy()).ApiV1alpha3())
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go r.Run(ctx.Done())
		synctest.Wait()
		time.Sleep(10 * time.Second)
		synctest.Wait()
		require.Equal(t, 4, store.pairCalls)
		published, err := r.status.Agents(initial.Agent.Namespace).Get(t.Context(), initial.Agent.Name, metav1.GetOptions{})
		require.NoError(t, err)
		agents := collections.Agents.(krt.StaticCollection[*kagentv1alpha3.Agent])
		agents.UpdateObject(published)
		synctest.Wait()
		require.Equal(t, 4, store.pairCalls, "status feedback must not bypass backoff")
		templates.createErr = nil
		updated := harnesses.List()[0].DeepCopy()
		updated.Spec.Workload.Args = []string{"changed"}
		harnesses.UpdateObject(updated)
		synctest.Wait()
		require.Equal(t, 5, store.pairCalls, "a new desired revision must prepare immediately")
		require.NotEqual(t, initial.Target.RevisionID.String(), store.pair.DesiredRevision)
		require.Nil(t, collections.Reconciliations.GetKey(initial.ResourceName()).PreparationFailure)

		harnesses.DeleteObject("team-a/byo")
		synctest.Wait()
		require.Empty(t, store.pair.DesiredRevision, "unresolved inputs must clear the desired runtime immediately")
		require.Empty(t, collections.AgentRuntimeObservations.List())
		agents.DeleteObject(initial.ResourceName())
		synctest.Wait()
		require.Equal(t, initial.ResourceName(), store.retired, "deletion must retire the Agent immediately")
		cancel()
		synctest.Wait()
	})
}

func TestPreparationTerminalFailures(t *testing.T) {
	for _, reason := range []string{"PreparedRuntimeRejected", "PreparedRuntimeConflict", "PreparedRuntimeFailed"} {
		t.Run(reason, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				collections, harnesses := newPreparationTestCollections(t, "microvm")
				initial := collections.Reconciliations.List()[0]
				templates := &fakePreparedRuntimes{}
				switch reason {
				case "PreparedRuntimeRejected":
					templates.createErr = status.Error(codes.InvalidArgument, "private-config-value")
				case "PreparedRuntimeConflict":
					templates.template = proto.CloneOf(initial.Target.Runtime)
					templates.template.Spec.Args = []string{"unexpected"}
				case "PreparedRuntimeFailed":
					templates.template = proto.CloneOf(initial.Target.Runtime)
					templates.template.Phase, templates.template.Message = "Failed", "snapshot failed"
				}
				store := &fakeRuntimeRevisionStore{}
				statusClient := kagentfake.NewSimpleClientset(initial.Agent.DeepCopy()).ApiV1alpha3()
				r := newReconciler(collections, templates, store, statusClient)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				go r.Run(ctx.Done())
				synctest.Wait()
				require.Equal(t, 1, store.pairCalls)
				state := collections.Reconciliations.GetKey(initial.ResourceName())
				require.Nil(t, state.CompilationFailure)
				require.NotNil(t, state.Target)
				require.Equal(t, reason, state.PreparationFailure.Reason)
				published, err := statusClient.Agents(initial.Agent.Namespace).Get(t.Context(), initial.Agent.Name, metav1.GetOptions{})
				require.NoError(t, err)
				ready := apimeta.FindStatusCondition(published.Status.Conditions, kagentv1alpha3.AgentConditionReady)
				require.Equal(t, reason, ready.Reason)
				require.NotContains(t, ready.Message, "private-config-value")
				time.Sleep(5 * time.Minute)
				synctest.Wait()
				require.Equal(t, 1, store.pairCalls, "neither the queue nor the poller may retry terminal failures")

				store.pairErr = errors.New("database unavailable")
				require.ErrorIs(t, r.reconcileAgent(t.Context(), initial.ResourceName()), store.pairErr)
				synctest.Wait()
				require.Equal(t, state.PreparationFailure, collections.Reconciliations.GetKey(initial.ResourceName()).PreparationFailure,
					"a storage failure must preserve the terminal preparation diagnostic")
				store.pairErr = nil
				templates.getErr = errors.New("Substrate must not be called after a terminal failure")
				require.NoError(t, r.reconcileAgent(t.Context(), initial.ResourceName()))
				templates.getErr = nil

				templates.createErr, templates.template = nil, nil
				updated := harnesses.List()[0].DeepCopy()
				updated.Spec.Workload.Args = []string{"changed"}
				harnesses.UpdateObject(updated)
				synctest.Wait()
				require.Equal(t, 4, store.pairCalls)
				require.NotNil(t, templates.template, "a changed revision must prepare without the old failure")
				require.Nil(t, collections.Reconciliations.GetKey(initial.ResourceName()).PreparationFailure)
				cancel()
				synctest.Wait()
			})
		})
	}
}

func TestInvalidActorTemplateHasNoCompiledTarget(t *testing.T) {
	collections, harnesses := newPreparationTestCollections(t, "microvm")
	initial := collections.Reconciliations.List()[0]
	updated := harnesses.List()[0].DeepCopy()
	updated.Spec.Env = []kagentv1alpha3.RuntimeEnvVar{{Name: "EXTRA", Value: new(strings.Repeat("x", 32769))}}
	harnesses.UpdateObject(updated)
	waitFor(t, func() bool {
		return collections.Reconciliations.GetKey(initial.ResourceName()).CompilationFailure != nil
	})
	state := collections.Reconciliations.GetKey(initial.ResourceName())
	require.Equal(t, "PreparedRuntimeInvalid", state.CompilationFailure.Reason)
	require.Nil(t, state.Target, "a revision and digest must not be published when ActorTemplate construction fails")
	require.Nil(t, state.PreparationFailure)
	store := &fakeRuntimeRevisionStore{}
	templates := &fakePreparedRuntimes{getErr: errors.New("Substrate must not be called for an invalid target")}
	r := &Reconciler{collections: collections, templates: templates, store: store}
	require.NoError(t, r.reconcileAgent(t.Context(), initial.ResourceName()))
	require.Empty(t, store.pair.DesiredRevision)
	require.Nil(t, templates.template)
	require.Nil(t, store.revision)
	agentStatus := statusForAgent(*state, 1, initial.Target.RevisionID.String())
	require.Empty(t, agentStatus.DesiredRevision)
	require.Equal(t, initial.Target.RevisionID.String(), agentStatus.LatestSuccessfulRevision)
	compatible := apimeta.FindStatusCondition(agentStatus.Conditions, kagentv1alpha3.AgentConditionCompatible)
	require.Equal(t, "PreparedRuntimeInvalid", compatible.Reason)
}

func newPreparationTestCollections(t *testing.T, workerPool string) (Collections, krt.StaticCollection[*kagentv1alpha3.Harness]) {
	t.Helper()
	opts := krt.NewOptionsBuilder(t.Context().Done(), "test-preparation", nil)
	template := &kagentv1alpha3.AgentTemplate{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "assistant", UID: "template-uid"}}
	runtimeHarness := harness("team-a", "byo", nil)
	runtimeHarness.UID = "harness-uid"
	runtimeHarness.Spec.BYO = &kagentv1alpha3.BYOHarness{}
	runtimeHarness.Spec.Workload.Image = "example.com/agent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	runtimeHarness.Spec.Workload.Command = []string{"/agent"}
	runtimeHarness.Spec.AX = kagentv1alpha3.RuntimeAXPolicy{
		TaskGroupRef: corev1.LocalObjectReference{Name: workerPool}, SnapshotLocationOverride: "s3://snapshots",
	}
	harnesses := krt.NewStaticCollection(nil, []*kagentv1alpha3.Harness{runtimeHarness}, opts.WithName("Harnesses")...)
	mock := krttest.NewMock(t, []any{
		template,
		testTaskGroup("team-a", "gvisor", "gvisor-uid"),
		testTaskGroup("team-a", "microvm", "microvm-uid"),
	})
	collections := Collections{
		AgentTemplates:           krttest.GetMockCollection[*kagentv1alpha3.AgentTemplate](mock),
		Harnesses:                harnesses,
		ResolvedModelConfigs:     krttest.GetMockCollection[v2translator.ResolvedModelConfig](mock),
		RemoteMCPServers:         krttest.GetMockCollection[*kagentv1alpha3.RemoteMCPServer](mock),
		ConfigMaps:               krttest.GetMockCollection[*corev1.ConfigMap](mock),
		Secrets:                  krttest.GetMockCollection[*corev1.Secret](mock),
		TaskGroups:               krt.NewStaticCollection(nil, krttest.GetMockCollection[v2translator.TaskGroupObservation](mock).List(), opts.WithName("AXTaskGroups")...),
		AgentRuntimeObservations: krt.NewStaticCollection[AgentRuntimeObservation](nil, nil, opts.WithName("AgentRuntimeObservations")...),
		ModelConfigStatuses:      krttest.GetMockCollection[krt.ObjectWithStatus[*kagentv1alpha3.ModelConfig, kagentv1alpha3.ModelConfigStatus]](mock),
	}
	collections.Agents = krt.NewStaticCollection(nil, []*kagentv1alpha3.Agent{testAgent(template, runtimeHarness)}, opts.WithName("Agents")...)
	collections.Reconciliations = newAgentReconciliations(collections.Agents, v2translator.Collections{
		Harnesses: collections.Harnesses, AgentTemplates: collections.AgentTemplates, ResolvedModelConfigs: collections.ResolvedModelConfigs,
		RemoteMCPServers: collections.RemoteMCPServers, ConfigMaps: collections.ConfigMaps,
		Secrets: collections.Secrets, TaskGroups: collections.TaskGroups,
	}, collections.AgentRuntimeObservations, opts)
	collections.AgentStatuses = newAgentStatuses(collections.Agents, collections.Reconciliations, opts)
	waitFor(t, func() bool {
		states := collections.Reconciliations.List()
		return len(states) == 1 && states[0].CompilationFailure == nil && states[0].Target != nil
	})
	return collections, harnesses
}
