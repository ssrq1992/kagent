package controller

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	kagentclient "github.com/kagent-dev/kagent/go/api/clientset/versioned/typed/api/v1alpha3"
	kagentv1alpha3 "github.com/kagent-dev/kagent/go/api/v1alpha3"
	"github.com/kagent-dev/kagent/go/core/internal/axruntime"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"istio.io/istio/pkg/kube/controllers"
	"istio.io/istio/pkg/kube/krt"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const (
	sandboxPreparationFinalizer      = "kagent.dev/sandbox-preparation"
	sandboxPreparationFailed         = "PreparationFailed"
	sandboxPreparationFailureMessage = "Sandbox preparation failed; inspect controller logs and template configuration"
)

type sandboxRevisionStore interface {
	UpsertSandboxTemplateDefinition(context.Context, database.SandboxTemplateDefinition) error
	RetireSandboxTemplateIdentities(context.Context, string, string) error
	RecordSandboxRevision(context.Context, database.SandboxRevision, bool) error
}

// SandboxReconciler owns the queued side effects of SandboxTemplate preparation.
// Sandbox instance lifecycle remains owned by the PostgreSQL-backed worker.
type SandboxReconciler struct {
	collections sandboxCollections
	store       sandboxRevisionStore
	ax          ax.AXClient
	client      kagentclient.ApiV1alpha3Interface
}

var _ manager.LeaderElectionRunnable = (*SandboxReconciler)(nil)

func NewSandboxReconciler(config *rest.Config, runtime *Runtime, store sandboxRevisionStore, runtimeClient ax.AXClient, policy axruntime.SandboxPolicy) (*SandboxReconciler, error) {
	client, err := kagentclient.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create SandboxTemplate client: %w", err)
	}
	return &SandboxReconciler{collections: newSandboxCollections(runtime.Collections, policy, runtime.Options), store: store, ax: runtimeClient, client: client}, nil
}

func (s *SandboxReconciler) NeedLeaderElection() bool { return true }

func (s *SandboxReconciler) Start(ctx context.Context) error {
	preparations := newReconciliationQueue("v2-sandbox-preparation", func(item any) error {
		return s.reconcile(ctx, item.(string))
	})
	statuses := newReconciliationQueue("v2-sandbox-status", func(item any) error {
		return s.reconcileStatus(ctx, item.(string))
	})
	handler := s.collections.states.Register(func(event krt.Event[sandboxReconciliation]) {
		key := event.Latest().ResourceName()
		preparations.Add(key)
		statuses.Add(key)
	})
	if !handler.WaitUntilSynced(ctx.Done()) {
		preparations.ShutDownEarly()
		statuses.ShutDownEarly()
		return nil
	}
	var workers sync.WaitGroup
	workers.Go(func() { statuses.Run(ctx.Done()) })
	workers.Go(func() { s.pollPending(ctx, preparations, statuses) })
	preparations.Run(ctx.Done())
	workers.Wait()
	return nil
}

// Kubernetes changes arrive through KRT. Poll only incomplete external work;
// this also recovers transient failures after the queue's retry budget expires.
func (s *SandboxReconciler) pollPending(ctx context.Context, preparations, statuses controllers.Queue) {
	groups := time.NewTicker(10 * time.Second)
	defer groups.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-groups.C:
			var refs []*ax.ResourceRef
			for _, state := range s.collections.states.List() {
				if state.Template.DeletionTimestamp.IsZero() {
					refs = append(refs, &ax.ResourceRef{Atespace: state.Template.Namespace, Name: state.Template.Spec.AX.TaskGroupRef.Name})
				}
			}
			refreshTaskGroups(ctx, s.ax, refs, func(group *ax.TaskGroup) {
				s.collections.groups.ConditionalUpdateObject(sandboxGroupObservation{Ref: ax.Ref(group.Metadata)})
			}, s.collections.groups.DeleteObject)
		case <-ticker.C:
			for _, state := range s.collections.states.List() {
				if state.Template.DeletionTimestamp.IsZero() && !apiequality.Semantic.DeepEqual(sandboxStatusWithTransitionTimes(state), state.Template.Status) {
					statuses.Add(state.ResourceName())
				}
				if !state.Template.DeletionTimestamp.IsZero() || !slices.Contains(state.Template.Finalizers, sandboxPreparationFinalizer) ||
					(state.Failure != nil && state.Failure.Retryable) || (state.canPrepare() && state.ObservedRuntime.GetPhase() != "Ready") {
					preparations.Add(state.ResourceName())
				}
			}
		}
	}
}

func (s *SandboxReconciler) reconcile(ctx context.Context, key string) error {
	state := s.collections.states.GetKey(key)
	if observation := s.collections.observations.GetKey(key); observation != nil && (state == nil || observation.RevisionID != state.desiredRevision()) {
		s.collections.observations.DeleteObject(key)
	}
	if state == nil || !state.Template.DeletionTimestamp.IsZero() {
		namespace, name, _ := strings.Cut(key, "/")
		if err := s.store.RetireSandboxTemplateIdentities(ctx, namespace, name); err != nil {
			return fmt.Errorf("retire SandboxTemplate %s: %w", key, err)
		}
		if state != nil && slices.Contains(state.Template.Finalizers, sandboxPreparationFinalizer) {
			updated := state.Template.DeepCopy()
			updated.Finalizers = slices.DeleteFunc(updated.Finalizers, func(value string) bool { return value == sandboxPreparationFinalizer })
			if _, err := s.client.SandboxTemplates(updated.Namespace).Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
				return fmt.Errorf("remove SandboxTemplate %s finalizer: %w", key, err)
			}
		}
		return nil
	}
	// Persist cleanup ownership before any database or backend allocation. A
	// deletion while this controller is offline must still be seen on restart.
	if !slices.Contains(state.Template.Finalizers, sandboxPreparationFinalizer) {
		updated := state.Template.DeepCopy()
		updated.Finalizers = append(updated.Finalizers, sandboxPreparationFinalizer)
		if _, err := s.client.SandboxTemplates(updated.Namespace).Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("add SandboxTemplate %s finalizer: %w", key, err)
		}
		return nil
	}
	if err := s.store.UpsertSandboxTemplateDefinition(ctx, database.SandboxTemplateDefinition{
		Namespace: state.Template.Namespace, SandboxTemplateName: state.Template.Name, SandboxTemplateUID: string(state.Template.UID), DesiredRevision: state.desiredRevision(),
	}); err != nil {
		return s.observeError(*state, fmt.Errorf("store SandboxTemplate %s definition: %w", key, err), true)
	}
	groupName := state.Template.Spec.AX.TaskGroupRef.Name
	group, groupErr := s.ax.GetTaskGroup(ctx, &ax.GetTaskGroupRequest{Atespace: state.Template.Namespace, Name: groupName})
	if groupErr != nil {
		if status.Code(groupErr) == codes.NotFound {
			s.collections.groups.DeleteObject(state.Template.Namespace + "/" + groupName)
			s.collections.observations.DeleteObject(key)
			return fmt.Errorf("resolve AX TaskGroup: %w", groupErr)
		}
		return s.observeError(*state, fmt.Errorf("resolve AX TaskGroup: %w", groupErr), true)
	}
	groupRef := ax.Ref(group.GetMetadata())
	if err := ax.ValidateRef(groupRef, true); err != nil {
		return err
	}
	if groupRef.Atespace != state.Template.Namespace || groupRef.Name != groupName {
		return fmt.Errorf("AX returned a different TaskGroup")
	}
	if old := s.collections.groups.GetKey(state.Template.Namespace + "/" + groupName); old == nil || !proto.Equal(old.Ref, groupRef) {
		s.collections.groups.ConditionalUpdateObject(sandboxGroupObservation{Ref: groupRef})
		return nil
	}
	if state.DesiredRuntime == nil {
		s.collections.observations.DeleteObject(key)
	}
	if state.CompilationError != "" {
		return fmt.Errorf("compile SandboxTemplate %s: %s", key, state.CompilationError)
	}
	if !state.canPrepare() {
		return nil
	}
	ref := state.DesiredRuntime.GetMetadata()
	observed, err := s.ax.GetPreparedRuntime(ctx, &ax.GetPreparedRuntimeRequest{Ref: ax.Ref(ref)})
	if status.Code(err) == codes.NotFound {
		observed, err = s.ax.PrepareRuntime(ctx, &ax.PrepareRuntimeRequest{Metadata: ref, Spec: state.DesiredRuntime.Spec, RequestId: state.RevisionID})
	}
	if err != nil {
		return s.observeError(*state, fmt.Errorf("prepare sandbox AX runtime: %w", err), true)
	}
	if !proto.Equal(observed.Spec, state.DesiredRuntime.Spec) {
		return s.observeError(*state, fmt.Errorf("sandbox AX runtime immutable inputs disagree"), false)
	}
	if observed.Phase == "Failed" {
		return s.observeError(*state, fmt.Errorf("sandbox preparation failed: %s", observed.Message), false)
	}
	ready := observed.Phase == "Ready"
	revision := database.SandboxRevision{
		RuntimeArtifact: database.RuntimeArtifact{Revision: state.RevisionID, Kind: "sandbox", Namespace: state.Template.Namespace,
			PreparedRuntimeAtespace: ref.GetAtespace(), PreparedRuntimeName: ref.GetName(), PreparedRuntimeUID: observed.GetMetadata().GetUid()},
		SandboxTemplateName: state.Template.Name, SandboxTemplateUID: string(state.Template.UID),
		SourceSnapshot: state.SourceSnapshot,
	}
	if err := s.store.RecordSandboxRevision(ctx, revision, ready); err != nil {
		return s.observeError(*state, fmt.Errorf("store sandbox revision %s: %w", state.RevisionID, err), true)
	}
	// Kubernetes readiness can only follow a revision that callers can select
	// from the store. Failed persistence must never publish a ready observation.
	s.collections.observations.ConditionalUpdateObject(sandboxRuntimeObservation{Key: key, RevisionID: state.RevisionID, Template: observed})
	return nil
}

func (s *SandboxReconciler) observeError(state sandboxReconciliation, err error, retryable bool) error {
	s.collections.observations.ConditionalUpdateObject(sandboxRuntimeObservation{
		Key: state.ResourceName(), RevisionID: state.desiredRevision(),
		Failure: &ReconciliationFailure{Reason: sandboxPreparationFailed, Message: sandboxPreparationFailureMessage, Retryable: retryable},
	})
	return err
}

func (s *SandboxReconciler) reconcileStatus(ctx context.Context, key string) error {
	state := s.collections.states.GetKey(key)
	if state == nil || !state.Template.DeletionTimestamp.IsZero() {
		return nil
	}
	updated := state.Template.DeepCopy()
	updated.Status = sandboxStatusWithTransitionTimes(*state)
	if apiequality.Semantic.DeepEqual(updated.Status, state.Template.Status) {
		return nil
	}
	if _, err := s.client.SandboxTemplates(updated.Namespace).UpdateStatus(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update SandboxTemplate %s status: %w", key, err)
	}
	return nil
}

// Transition timestamps are applied at the write boundary, never inside a KRT
// transform. Reusing existing conditions also makes pending status writes
// detectable after the status queue exhausts its transient-error retries.
func sandboxStatusWithTransitionTimes(state sandboxReconciliation) kagentv1alpha3.SandboxTemplateStatus {
	desired := *state.Template.Status.DeepCopy()
	condition := metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: "Preparing", Message: "Waiting for the prepared sandbox snapshot", ObservedGeneration: state.Template.Generation}
	if state.Failure != nil {
		condition.Reason, condition.Message = state.Failure.Reason, state.Failure.Message
	} else if state.ObservedRuntime.GetPhase() == "Ready" {
		condition.Status, condition.Reason, condition.Message = metav1.ConditionTrue, "Prepared", "Sandbox runtime is prepared"
	}
	desired.ObservedGeneration = state.Template.Generation
	apimeta.SetStatusCondition(&desired.Conditions, condition)
	return desired
}
