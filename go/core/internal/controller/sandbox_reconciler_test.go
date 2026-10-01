package controller

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	kagentfake "github.com/kagent-dev/kagent/go/api/clientset/versioned/fake"
	kagentv1alpha3 "github.com/kagent-dev/kagent/go/api/v1alpha3"
	"github.com/kagent-dev/kagent/go/core/internal/axruntime"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"istio.io/istio/pkg/kube/krt"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
)

// These fakes are also used by the informer/queue integration test.
type sandboxTestStore struct {
	mu             sync.Mutex
	desired        database.SandboxTemplateDefinition
	revision       database.SandboxRevision
	ready          bool
	retired        bool
	retireAttempts int
	retireErr      error
	recordErr      error
}

func (s *sandboxTestStore) UpsertSandboxTemplateDefinition(_ context.Context, value database.SandboxTemplateDefinition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.desired = value
	return nil
}
func (s *sandboxTestStore) RecordSandboxRevision(_ context.Context, value database.SandboxRevision, ready bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.recordErr != nil {
		return s.recordErr
	}
	s.revision, s.ready = value, ready
	return nil
}
func (s *sandboxTestStore) RetireSandboxTemplateIdentities(context.Context, string, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retireAttempts++
	if s.retireErr != nil {
		return s.retireErr
	}
	s.retired = true
	return nil
}

var _ sandboxRevisionStore = (*sandboxTestStore)(nil)

type sandboxTestActors struct {
	ax.AXClient
	groups    krt.StaticCollection[sandboxGroupObservation]
	mu        sync.Mutex
	store     *sandboxTestStore
	templates map[string]*ax.PreparedRuntime
	autoReady bool
}

func (s *sandboxTestActors) GetTaskGroup(_ context.Context, req *ax.GetTaskGroupRequest, _ ...grpc.CallOption) (*ax.TaskGroup, error) {
	g := s.groups.GetKey(req.Atespace + "/" + req.Name)
	if g == nil {
		return nil, status.Error(codes.NotFound, "missing group")
	}
	return &ax.TaskGroup{Metadata: &ax.ObjectMeta{Atespace: g.Ref.Atespace, Name: g.Ref.Name, Uid: g.Ref.Uid}}, nil
}
func (s *sandboxTestActors) GetPreparedRuntime(_ context.Context, req *ax.GetPreparedRuntimeRequest, _ ...grpc.CallOption) (*ax.PreparedRuntime, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if template := s.templates[req.Ref.Atespace+"/"+req.Ref.Name]; template != nil {
		return proto.CloneOf(template), nil
	}
	return nil, status.Error(codes.NotFound, "missing")
}
func (s *sandboxTestActors) PrepareRuntime(_ context.Context, req *ax.PrepareRuntimeRequest, _ ...grpc.CallOption) (*ax.PreparedRuntime, error) {
	template := &ax.PreparedRuntime{Metadata: req.Metadata, Spec: req.Spec, Phase: "Preparing"}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store.mu.Lock()
	desired := s.store.desired.DesiredRevision
	s.store.mu.Unlock()
	if len(desired) != 64 || template.Metadata.Name != "sandbox-"+desired[:40] {
		return nil, errors.New("backend mutation before pinning inputs")
	}
	template = proto.CloneOf(template)
	template.Metadata.Uid = "backend-uid"
	if s.autoReady {
		template.Phase = "Ready"
	}
	s.templates[template.Metadata.Atespace+"/"+template.Metadata.Name] = template
	return proto.CloneOf(template), nil
}

func sandboxTestTemplate() *kagentv1alpha3.SandboxTemplate {
	return &kagentv1alpha3.SandboxTemplate{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "scratch", UID: "template-uid", Generation: 1}, Spec: kagentv1alpha3.SandboxTemplateSpec{
		Workload: kagentv1alpha3.SandboxTemplateWorkload{Image: "tools@sha256:" + strings.Repeat("a", 64)},
		AX:       kagentv1alpha3.RuntimeAXPolicy{TaskGroupRef: corev1.LocalObjectReference{Name: "default"}, SnapshotLocationOverride: "s3://snapshots/"},
	}}
}

func newSandboxTestReconciler(t *testing.T, guestImage string) (*SandboxReconciler, krt.StaticCollection[*kagentv1alpha3.SandboxTemplate], krt.StaticCollection[sandboxGroupObservation]) {
	t.Helper()
	opts := krt.NewOptionsBuilder(t.Context().Done(), "test-sandbox", nil)
	template := sandboxTestTemplate()
	pool := sandboxGroupObservation{Ref: &ax.ResourceRef{Atespace: template.Namespace, Name: "default", Uid: "group-uid"}}
	templates := krt.NewStaticCollection(nil, []*kagentv1alpha3.SandboxTemplate{template}, opts.WithName("SandboxTemplates")...)

	store := &sandboxTestStore{}
	actors := &sandboxTestActors{store: store, templates: map[string]*ax.PreparedRuntime{}}
	reconciler := &SandboxReconciler{
		collections: newSandboxCollections(Collections{SandboxTemplates: templates}, axruntime.SandboxPolicy{CPU: "1", Memory: "1Gi"}, opts),
		store:       store, ax: actors, client: kagentfake.NewSimpleClientset(template.DeepCopy()).ApiV1alpha3(),
	}
	pools := reconciler.collections.groups
	pools.UpdateObject(pool)
	actors.groups = pools
	waitFor(t, func() bool { return reconciler.collections.states.GetKey("team-a/scratch") != nil })
	return reconciler, templates, pools
}

func syncSandboxTemplate(t *testing.T, reconciler *SandboxReconciler, templates krt.StaticCollection[*kagentv1alpha3.SandboxTemplate]) *kagentv1alpha3.SandboxTemplate {
	t.Helper()
	template, err := reconciler.client.SandboxTemplates("team-a").Get(t.Context(), "scratch", metav1.GetOptions{})
	require.NoError(t, err)
	templates.UpdateObject(template)
	waitFor(t, func() bool {
		return reflect.DeepEqual(reconciler.collections.states.GetKey("team-a/scratch").Template, template)
	})
	return template
}

func TestSandboxPreparationPublishesAfterPersistence(t *testing.T) {
	guestImage := "unreachable.invalid/guest@sha256:" + strings.Repeat("b", 64)
	s, templates, _ := newSandboxTestReconciler(t, guestImage)
	store := s.store.(*sandboxTestStore)
	actors := s.ax.(*sandboxTestActors)
	const key = "team-a/scratch"
	require.NoError(t, s.reconcile(t.Context(), key))
	require.Empty(t, store.desired.DesiredRevision, "finalizer must persist before preparation")
	require.Empty(t, actors.templates)
	template := syncSandboxTemplate(t, s, templates)
	require.Contains(t, template.Finalizers, sandboxPreparationFinalizer)
	require.NoError(t, s.reconcile(t.Context(), key))
	state := s.collections.states.GetKey(key)
	ref := state.DesiredRuntime.Metadata
	observed, err := actors.GetPreparedRuntime(t.Context(), &ax.GetPreparedRuntimeRequest{Ref: ax.Ref(ref)})
	require.NoError(t, err)
	require.Equal(t, "Sandbox", observed.Spec.Kind)
	require.Contains(t, string(store.revision.SourceSnapshot), observed.Spec.Image)
	require.Equal(t, store.desired.DesiredRevision, store.revision.Revision)
	require.False(t, store.ready)

	actors.templates[ref.Atespace+"/"+ref.Name].Phase = "Ready"
	store.recordErr = errors.New("database unavailable with private details")
	require.ErrorContains(t, s.reconcile(t.Context(), key), "database unavailable")
	waitFor(t, func() bool { return s.collections.states.GetKey(key).Failure != nil })
	require.NoError(t, s.reconcileStatus(t.Context(), key))
	template = syncSandboxTemplate(t, s, templates)
	ready := apimeta.FindStatusCondition(template.Status.Conditions, "Ready")
	require.Equal(t, metav1.ConditionFalse, ready.Status)
	require.NotContains(t, ready.Message, "private details")
	store.recordErr = nil
	require.NoError(t, s.reconcile(t.Context(), key))
	waitFor(t, func() bool {
		return s.collections.states.GetKey(key).ObservedRuntime.GetPhase() == "Ready"
	})
	require.True(t, store.ready)
	require.NoError(t, s.reconcileStatus(t.Context(), key))
	template = syncSandboxTemplate(t, s, templates)
	ready = apimeta.FindStatusCondition(template.Status.Conditions, "Ready")
	require.Equal(t, metav1.ConditionTrue, ready.Status)
	before := ready.LastTransitionTime
	require.NoError(t, s.reconcileStatus(t.Context(), key))
	template = syncSandboxTemplate(t, s, templates)
	require.Equal(t, before, apimeta.FindStatusCondition(template.Status.Conditions, "Ready").LastTransitionTime)
}

func TestSandboxCollectionsTrackDependenciesAndRejectStaleReadiness(t *testing.T) {
	s, templates, pools := newSandboxTestReconciler(t, "guest@sha256:"+strings.Repeat("b", 64))
	const key = "team-a/scratch"
	require.NoError(t, s.reconcile(t.Context(), key))
	syncSandboxTemplate(t, s, templates)
	require.NoError(t, s.reconcile(t.Context(), key))
	waitFor(t, func() bool { return s.collections.states.GetKey(key).RevisionID != "" })
	original := s.collections.states.GetKey(key)
	observed := proto.CloneOf(original.DesiredRuntime)
	observed.Phase = "Ready"
	s.collections.observations.UpdateObject(sandboxRuntimeObservation{Key: key, RevisionID: original.RevisionID, Template: observed})
	waitFor(t, func() bool { return s.collections.states.GetKey(key).ObservedRuntime != nil })

	pool := sandboxGroupObservation{Ref: proto.CloneOf(pools.GetKey("team-a/default").Ref)}
	pool.Ref.Uid = "new-group-uid"
	pools.UpdateObject(pool)
	waitFor(t, func() bool { return s.collections.states.GetKey(key).RevisionID != original.RevisionID })
	changed := s.collections.states.GetKey(key)
	require.NotEmpty(t, changed.RevisionID)
	require.Nil(t, changed.ObservedRuntime)
	require.False(t, proto.Equal(original.DesiredRuntime.Spec.GroupRef, changed.DesiredRuntime.Spec.GroupRef))
	pools.DeleteObject("team-a/default")
	waitFor(t, func() bool { return s.collections.states.GetKey(key).Failure != nil })
	missing := s.collections.states.GetKey(key)
	require.Equal(t, "TaskGroupUnresolved", missing.Failure.Reason)
	require.Empty(t, missing.RevisionID)
	require.Nil(t, missing.ObservedRuntime)
	require.Error(t, s.reconcile(t.Context(), key))
	pools.UpdateObject(pool)
	waitFor(t, func() bool { return s.collections.states.GetKey(key).RevisionID == changed.RevisionID })
	require.Nil(t, s.collections.states.GetKey(key).Failure)

	// Editing the workload invalidates readiness even when the UID is unchanged.
	edited := (*templates.GetKey(key)).DeepCopy()
	edited.Generation++
	edited.Spec.Workload.Image = "tools@sha256:" + strings.Repeat("c", 64)
	templates.UpdateObject(edited)
	waitFor(t, func() bool { return s.collections.states.GetKey(key).Template.Generation == edited.Generation })
	require.NotEqual(t, changed.RevisionID, s.collections.states.GetKey(key).RevisionID)
	require.Nil(t, s.collections.states.GetKey(key).ObservedRuntime)

	// A recreated template cannot inherit the previous UID's readiness.
	template := (*templates.GetKey(key)).DeepCopy()
	template.UID = "replacement-uid"
	templates.UpdateObject(template)
	waitFor(t, func() bool { return s.collections.states.GetKey(key).Template.UID == template.UID })
	require.NotEqual(t, changed.RevisionID, s.collections.states.GetKey(key).RevisionID)
	require.Nil(t, s.collections.states.GetKey(key).ObservedRuntime)
}

func TestSandboxPreparationRetriesGoldenFailureAndRejectsImmutableConflict(t *testing.T) {
	s, templates, _ := newSandboxTestReconciler(t, "guest@sha256:"+strings.Repeat("b", 64))
	const key = "team-a/scratch"
	require.NoError(t, s.reconcile(t.Context(), key))
	syncSandboxTemplate(t, s, templates)
	require.NoError(t, s.reconcile(t.Context(), key))
	ref := s.collections.states.GetKey(key).DesiredRuntime.Metadata
	observed := s.ax.(*sandboxTestActors).templates[ref.Atespace+"/"+ref.Name]
	observed.Phase = "Failed"
	observed.Message = "snapshot backend unavailable"
	require.ErrorContains(t, s.reconcile(t.Context(), key), "preparation failed")
	waitFor(t, func() bool { return s.collections.states.GetKey(key).Failure != nil })
	require.False(t, s.collections.states.GetKey(key).canPrepare())
	observed.Phase = "Ready"
	s.collections.observations.DeleteObject(key)
	waitFor(t, func() bool { return s.collections.states.GetKey(key).canPrepare() })
	require.NoError(t, s.reconcile(t.Context(), key))
	waitFor(t, func() bool { return s.collections.states.GetKey(key).Failure == nil })
	observed.Spec.Image = "unexpected"
	require.ErrorContains(t, s.reconcile(t.Context(), key), "immutable inputs disagree")
	waitFor(t, func() bool { return s.collections.states.GetKey(key).Failure != nil })
	require.False(t, s.collections.states.GetKey(key).canPrepare())
	require.Nil(t, s.collections.states.GetKey(key).ObservedRuntime)
}

// A ready runtime may outlive an exhausted status queue. Recovery must not need
// another Kubernetes edit or backend readiness change to publish Ready.
func TestSandboxPendingStatusRecoversWithoutGraphEvent(t *testing.T) {
	s, templates, _ := newSandboxTestReconciler(t, "guest@sha256:"+strings.Repeat("b", 64))
	const key = "team-a/scratch"
	require.NoError(t, s.reconcile(t.Context(), key))
	syncSandboxTemplate(t, s, templates)
	s.ax.(*sandboxTestActors).autoReady = true
	require.NoError(t, s.reconcile(t.Context(), key))
	waitFor(t, func() bool { return s.collections.states.GetKey(key).ObservedRuntime != nil })
	client := kagentfake.NewSimpleClientset(s.collections.states.GetKey(key).Template.DeepCopy())
	failed := false
	client.PrependReactor("update", "sandboxtemplates", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "status" && !failed {
			failed = true
			return true, nil, errors.New("API unavailable")
		}
		return false, nil, nil
	})
	s.client = client.ApiV1alpha3()
	require.ErrorContains(t, s.reconcileStatus(t.Context(), key), "API unavailable")
	ctx, cancel := context.WithCancel(t.Context())
	statuses := newReconciliationQueue("test-sandbox-status", func(item any) error { return s.reconcileStatus(ctx, item.(string)) })
	preparations := newReconciliationQueue("test-sandbox-preparation", func(any) error { return nil })
	var workers sync.WaitGroup
	workers.Go(func() { s.pollPending(ctx, preparations, statuses) })
	workers.Go(func() { statuses.Run(ctx.Done()) })
	t.Cleanup(func() { cancel(); workers.Wait(); preparations.ShutDownEarly() })
	require.Eventually(t, func() bool {
		current, err := s.client.SandboxTemplates("team-a").Get(t.Context(), "scratch", metav1.GetOptions{})
		return err == nil && apimeta.IsStatusConditionTrue(current.Status.Conditions, "Ready")
	}, 5*time.Second, 10*time.Millisecond)
}
