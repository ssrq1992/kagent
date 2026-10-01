package controller

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	kagentclient "github.com/kagent-dev/kagent/go/api/clientset/versioned/typed/api/v1alpha3"
	kagentv1alpha3 "github.com/kagent-dev/kagent/go/api/v1alpha3"
	"github.com/kagent-dev/kagent/go/core/internal/axruntime"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	v2translator "github.com/kagent-dev/kagent/go/core/internal/translator"
	byotranslator "github.com/kagent-dev/kagent/go/core/internal/translator/byo"
	claudetranslator "github.com/kagent-dev/kagent/go/core/internal/translator/claude"
	codextranslator "github.com/kagent-dev/kagent/go/core/internal/translator/codex"
	kagenttranslator "github.com/kagent-dev/kagent/go/core/internal/translator/kagent"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"istio.io/istio/pkg/kube/controllers"
	"istio.io/istio/pkg/kube/krt"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"
)

// AgentReconciliation is the complete desired and observed state for one
// Agent. Compilation failures are data so invalid Agents still produce
// status instead of disappearing from the graph.
type AgentReconciliation struct {
	Agent              *kagentv1alpha3.Agent
	Target             *compiledTarget
	Warnings           []string
	CompilationFailure *ReconciliationFailure
	RequiredGroup      *ax.ResourceRef
	ObservedRuntime    *ax.PreparedRuntime
	PreparationFailure *ReconciliationFailure
}

// compiledTarget is published only after compilation, hashing, and ActorTemplate
// construction all succeed. An absent target means there is no desired runtime.
type compiledTarget struct {
	Revision   v2translator.Revision
	RevisionID v2translator.RevisionID
	Runtime    *ax.PreparedRuntime
}

func (c *compiledTarget) equals(other *compiledTarget) bool {
	if c == nil || other == nil {
		return c == other
	}
	return c.RevisionID == other.RevisionID && c.Revision.Equals(other.Revision) &&
		proto.Equal(c.Runtime, other.Runtime)
}

func (r AgentReconciliation) ResourceName() string { return r.Agent.Namespace + "/" + r.Agent.Name }

var _ krt.Equaler[AgentReconciliation] = AgentReconciliation{}

// Equals keeps KRT from reflecting over protobuf caches that mutate during reads.
func (r AgentReconciliation) Equals(other AgentReconciliation) bool {
	if !r.Target.equals(other.Target) ||
		!proto.Equal(r.ObservedRuntime, other.ObservedRuntime) || !proto.Equal(r.RequiredGroup, other.RequiredGroup) {
		return false
	}
	r.RequiredGroup, other.RequiredGroup = nil, nil
	r.Target, other.Target = nil, nil
	r.ObservedRuntime, other.ObservedRuntime = nil, nil
	return reflect.DeepEqual(r, other)
}

func (r AgentReconciliation) desiredRevision() string {
	if r.Target == nil {
		return ""
	}
	return r.Target.RevisionID.String()
}

// ReconciliationFailure identifies the condition stage blocked by an Agent.
type ReconciliationFailure struct {
	Condition string
	Reason    string
	Message   string
	Retryable bool
}

func newAgentReconciliations(
	agents krt.Collection[*kagentv1alpha3.Agent],
	collections v2translator.Collections,
	agentRuntimeObservations krt.Collection[AgentRuntimeObservation],
	opts krt.OptionsBuilder,
) krt.Collection[AgentReconciliation] {
	return krt.NewCollection(agents, func(ctx krt.HandlerContext, agent *kagentv1alpha3.Agent) *AgentReconciliation {
		state := &AgentReconciliation{Agent: agent}
		compilation, err := v2translator.NewCompiler(ctx, collections, map[v2translator.HarnessType]v2translator.HarnessCompiler{
			v2translator.HarnessTypeKagent: kagenttranslator.NewCompiler(ctx, collections),
			v2translator.HarnessTypeCodex:  codextranslator.NewCompiler(ctx, collections),
			v2translator.HarnessTypeClaude: claudetranslator.NewCompiler(ctx, collections),
			v2translator.HarnessTypeBYO:    byotranslator.NewCompiler(ctx, collections),
		}).CompileAgent(context.Background(), agent)
		if err != nil {
			condition, reason := kagentv1alpha3.AgentConditionResolvedRefs, "ReferenceResolutionFailed"
			var validation *v2translator.ValidationError
			var missingPool *v2translator.TaskGroupNotFoundError
			switch {
			case errors.As(err, &validation):
				condition, reason = kagentv1alpha3.AgentConditionCompatible, "UnsupportedConfiguration"
			case errors.As(err, &missingPool):
				reason = "TaskGroupNotFound"
				state.RequiredGroup = &ax.ResourceRef{Atespace: missingPool.TaskGroup.Namespace, Name: missingPool.TaskGroup.Name}
			}
			state.CompilationFailure = &ReconciliationFailure{Condition: condition, Reason: reason, Message: err.Error()}
			return state
		}
		revision := &compilation.Revision
		state.Warnings = append([]string(nil), compilation.Warnings...)
		revisionID, err := revision.Digest()
		if err != nil {
			state.CompilationFailure = &ReconciliationFailure{Condition: kagentv1alpha3.AgentConditionCompatible, Reason: "RevisionInvalid", Message: err.Error()}
			return state
		}

		actorTemplate, err := axruntime.RuntimeForRevision(revision, revisionID)
		if err != nil {
			state.CompilationFailure = &ReconciliationFailure{Condition: kagentv1alpha3.AgentConditionCompatible, Reason: "PreparedRuntimeInvalid", Message: err.Error()}
			return state
		}
		state.Target = &compiledTarget{Revision: *revision, RevisionID: revisionID, Runtime: actorTemplate}

		observed := krt.FetchOne(ctx, agentRuntimeObservations, krt.FilterKey(state.ResourceName()))
		if observed == nil || observed.RevisionID != state.Target.RevisionID {
			return state
		}
		if observed.Failure != nil {
			state.PreparationFailure = observed.Failure
			return state
		}
		state.ObservedRuntime = (*observed).Template
		if !axruntime.RuntimeSpecEqual(state.ObservedRuntime, state.Target.Runtime) {
			state.PreparationFailure = &ReconciliationFailure{
				Condition: kagentv1alpha3.AgentConditionReady,
				Reason:    "PreparedRuntimeConflict",
				Message:   "existing immutable PreparedRuntime differs from the compiled revision",
			}
		} else if message := state.ObservedRuntime.GetMessage(); message != "" {
			state.PreparationFailure = &ReconciliationFailure{Condition: kagentv1alpha3.AgentConditionReady, Reason: "PreparedRuntimeFailed", Message: message}
		}
		return state
	}, opts.WithName("AgentReconciliations")...)
}

// runtimeRevisionStore is the controller's narrow view of the shared database.
// Substrate owns ActorTemplates; the database retains revisions while an Agent
// or a Session or checkpoint references them.
type runtimeRevisionStore interface {
	UpsertAgentDefinition(context.Context, database.AgentDefinition) error
	RecordRuntimeRevision(context.Context, database.RuntimeRevision, bool) error
	RetireAgentIdentities(ctx context.Context, namespace, name string, except *database.AgentDefinition) error
}

// Reconciler is the side-effect boundary for the pure KRT graph. Collection
// handlers enqueue stable keys; retries always read the latest derived state.
type Reconciler struct {
	collections Collections
	templates   ax.AXClient
	store       runtimeRevisionStore
	status      kagentclient.ApiV1alpha3Interface

	agents                   controllers.Queue
	agentStatuses            controllers.Queue
	modelConfigStatuses      controllers.Queue
	agentHandler             krt.HandlerRegistration
	agentStatusHandler       krt.HandlerRegistration
	modelConfigStatusHandler krt.HandlerRegistration
}

// NewReconciler creates the Kubernetes and database write boundary. Run starts
// its queues after the registered KRT handlers have received initial state.
func NewReconciler(config *rest.Config, collections Collections, store runtimeRevisionStore, templates ax.AXClient) (*Reconciler, error) {
	statusClient, err := kagentclient.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create kagent status client: %w", err)
	}
	return newReconciler(collections, templates, store, statusClient), nil
}

func newReconciler(
	collections Collections,
	templates ax.AXClient,
	store runtimeRevisionStore,
	status kagentclient.ApiV1alpha3Interface,
) *Reconciler {
	r := &Reconciler{
		collections: collections,
		templates:   templates,
		store:       store,
		status:      status,
	}
	r.agents = newReconciliationQueue("v2-agents", func(item any) error {
		return r.reconcileAgent(context.Background(), item.(string))
	})
	r.agentStatuses = newReconciliationQueue("v2-agent-status", func(item any) error {
		return r.reconcileAgentStatus(context.Background(), item.(string))
	})
	r.modelConfigStatuses = newReconciliationQueue("v2-model-config-status", func(item any) error {
		return r.reconcileModelConfigStatus(context.Background(), item.(string))
	})

	r.agentHandler = collections.Reconciliations.Register(func(event krt.Event[AgentReconciliation]) {
		// Observations and status writes must not bypass failure backoff. Only
		// a changed desired runtime or Agent identity needs immediate work.
		if event.Old != nil && event.New != nil && event.Old.Agent.UID == event.New.Agent.UID &&
			event.Old.desiredRevision() == event.New.desiredRevision() {
			return
		}
		r.agents.Add(krt.GetKey(event.Latest()))
	})
	r.agentStatusHandler = collections.AgentStatuses.Register(func(event krt.Event[krt.ObjectWithStatus[*kagentv1alpha3.Agent, kagentv1alpha3.AgentStatus]]) {
		status := event.Latest()
		if apiequality.Semantic.DeepEqual(statusWithTransitionTimes(status.Status, status.Obj.Status), status.Obj.Status) {
			return
		}
		r.agentStatuses.Add(status.ResourceName())
	})
	r.modelConfigStatusHandler = collections.ModelConfigStatuses.Register(func(event krt.Event[krt.ObjectWithStatus[*kagentv1alpha3.ModelConfig, kagentv1alpha3.ModelConfigStatus]]) {
		status := event.Latest()
		if apiequality.Semantic.DeepEqual(modelConfigStatusWithTransitionTimes(status.Status, status.Obj.Status), status.Obj.Status) {
			return
		}
		r.modelConfigStatuses.Add(status.ResourceName())
	})
	return r
}

// Keep retrying at a capped interval so prerequisites can recover without a
// Kubernetes event. Istio's queue requires a positive attempt limit; MaxInt
// makes that limit unreachable during a controller's lifetime.
func newReconciliationQueue(name string, reconcile func(any) error) controllers.Queue {
	return controllers.NewQueue(name, controllers.WithGenericReconciler(reconcile),
		controllers.WithMaxAttempts(math.MaxInt),
		controllers.WithRateLimiter(workqueue.NewTypedItemExponentialFailureRateLimiter[any](time.Second, 30*time.Second)),
	)
}

// Run waits for the graph boundary to observe initial state, then processes
// Agent and status writes until stop closes.
func (r *Reconciler) Run(stop <-chan struct{}) {
	if !r.agentHandler.WaitUntilSynced(stop) || !r.agentStatusHandler.WaitUntilSynced(stop) || !r.modelConfigStatusHandler.WaitUntilSynced(stop) {
		r.agents.ShutDownEarly()
		r.agentStatuses.ShutDownEarly()
		r.modelConfigStatuses.ShutDownEarly()
		return
	}
	go r.pollPendingTemplates(stop)
	go r.agentStatuses.Run(stop)
	go r.modelConfigStatuses.Run(stop)
	r.agents.Run(stop)
}

func (r *Reconciler) pollPendingTemplates(stop <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	groups := time.NewTicker(10 * time.Second)
	defer groups.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-groups.C:
			var refs []*ax.ResourceRef
			for _, state := range r.collections.Reconciliations.List() {
				if state.RequiredGroup != nil {
					refs = append(refs, state.RequiredGroup)
				}
				if state.Target != nil {
					refs = append(refs, state.Target.Revision.GroupRef)
				}
			}
			refreshTaskGroups(ctx, r.templates, refs, func(group *ax.TaskGroup) {
				r.collections.TaskGroups.ConditionalUpdateObject(v2translator.TaskGroupObservation{Group: group})
			}, r.collections.TaskGroups.DeleteObject)
		case <-ticker.C:
			for _, state := range r.collections.Reconciliations.List() {
				phase := state.ObservedRuntime.GetPhase()
				if state.Target != nil && state.PreparationFailure == nil && state.ObservedRuntime != nil && phase != "Ready" {
					r.agents.Add(state.ResourceName())
				}
			}
		}
	}
}

func (r *Reconciler) Start(ctx context.Context) error {
	r.Run(ctx.Done())
	return nil
}

func (r *Reconciler) NeedLeaderElection() bool { return true }

func (r *Reconciler) reconcileAgent(ctx context.Context, key string) error {
	state := r.collections.Reconciliations.GetKey(key)
	if observation := r.collections.AgentRuntimeObservations.GetKey(key); observation != nil &&
		(state == nil || state.Target == nil || observation.RevisionID != state.Target.RevisionID) {
		r.collections.AgentRuntimeObservations.DeleteObject(key)
	}
	if state == nil {
		parts := strings.Split(key, "/")
		if len(parts) != 2 {
			return fmt.Errorf("invalid Agent key %q", key)
		}
		if err := r.store.RetireAgentIdentities(ctx, parts[0], parts[1], nil); err != nil {
			return fmt.Errorf("retire Agent %s: %w", key, err)
		}
		return nil
	}
	definition := database.AgentDefinition{
		Namespace: state.Agent.Namespace, AgentName: state.Agent.Name,
		AgentUID: string(state.Agent.UID), DesiredRevision: state.desiredRevision(),
	}

	// Store the desired edge before creating compute so a concurrent collector
	// cannot mistake the revision for abandoned state. Unresolved inputs clear
	// the old desired edge without inventing a runtime revision;
	// the upsert preserves the current UID's last-good runtime in either case.
	if err := r.store.UpsertAgentDefinition(ctx, definition); err != nil {
		return r.observePreparationError(*state, fmt.Errorf("store Agent %s: %w", key, err))
	}
	if state.Target == nil {
		if state.RequiredGroup != nil {
			group, err := r.templates.GetTaskGroup(ctx, &ax.GetTaskGroupRequest{Atespace: state.RequiredGroup.Atespace, Name: state.RequiredGroup.Name})
			if err != nil {
				return fmt.Errorf("resolve AX TaskGroup: %w", err)
			}
			if err := ax.ValidateRef(ax.Ref(group.Metadata), true); err != nil {
				return err
			}
			if group.Metadata.Atespace != state.RequiredGroup.Atespace || group.Metadata.Name != state.RequiredGroup.Name {
				return fmt.Errorf("AX returned a different TaskGroup")
			}
			r.collections.TaskGroups.ConditionalUpdateObject(v2translator.TaskGroupObservation{Group: group})
		}
		return nil
	}
	if state.PreparationFailure != nil && !state.PreparationFailure.Retryable {
		return nil
	}
	target := state.Target
	desiredRef := target.Runtime.GetMetadata()
	observed, err := r.templates.GetPreparedRuntime(ctx, &ax.GetPreparedRuntimeRequest{Ref: ax.Ref(desiredRef)})
	if status.Code(err) == codes.NotFound {
		observed, err = r.templates.PrepareRuntime(ctx, &ax.PrepareRuntimeRequest{Metadata: desiredRef, Spec: target.Runtime.Spec, RequestId: target.RevisionID.String()})
		if status.Code(err) == codes.InvalidArgument {
			r.observePreparation(*state, nil, &ReconciliationFailure{Condition: kagentv1alpha3.AgentConditionReady, Reason: "PreparedRuntimeRejected", Message: "AX rejected the compiled runtime; verify the Agent and Harness configuration"})
			return nil
		}
	}
	if err != nil {
		return r.observePreparationError(*state, fmt.Errorf("reconcile AX runtime: %w", err))
	}
	if !axruntime.RuntimeSpecEqual(observed, target.Runtime) {
		r.observePreparation(*state, observed, nil)
		return nil
	}

	revision := database.RuntimeRevision{
		Revision: target.RevisionID.String(), Namespace: definition.Namespace,
		AgentName: definition.AgentName, AgentUID: definition.AgentUID,
		SourceSnapshot: target.Revision.Provenance, AgentCard: target.Revision.AgentCard,
		EgressDestinations:      target.Revision.EgressDestinations,
		Credentials:             target.Revision.Credentials,
		PreparedRuntimeAtespace: observed.GetMetadata().GetAtespace(), PreparedRuntimeName: observed.GetMetadata().GetName(), PreparedRuntimeUID: observed.GetMetadata().GetUid(),
	}
	ready := observed.GetPhase() == "Ready"
	if err := r.store.RecordRuntimeRevision(ctx, revision, ready); err != nil {
		return r.observePreparationError(*state, fmt.Errorf("store runtime revision %s: %w", target.RevisionID, err))
	}
	// This observation drives Kubernetes Ready status on a separate queue.
	// Publish it only after session creation can select the persisted revision.
	r.observePreparation(*state, observed, nil)
	return nil
}

func (r *Reconciler) observePreparationError(state AgentReconciliation, err error) error {
	if state.Target == nil {
		return err
	}
	// Preserve terminal diagnostics if persisting the desired edge fails.
	if state.PreparationFailure != nil && !state.PreparationFailure.Retryable {
		return err
	}
	r.observePreparation(state, nil, runtimePreparationFailure(err))
	return err
}

func runtimePreparationFailure(err error) *ReconciliationFailure {
	return &ReconciliationFailure{Condition: kagentv1alpha3.AgentConditionReady, Reason: "RuntimePreparationFailed", Message: fmt.Sprintf("AX runtime preparation failed (%s); check controller logs", status.Code(err)), Retryable: true}
}

// Observations belong to the Agent's current preparation, independently of how
// long sessions or checkpoints keep its old runtime alive in the database.
func (r *Reconciler) observePreparation(state AgentReconciliation, template *ax.PreparedRuntime, failure *ReconciliationFailure) {
	r.collections.AgentRuntimeObservations.ConditionalUpdateObject(AgentRuntimeObservation{
		Namespace: state.Agent.Namespace, AgentName: state.Agent.Name,
		RevisionID: state.Target.RevisionID,
		Template:   template,
		Failure:    failure,
	})
}

func (r *Reconciler) reconcileAgentStatus(ctx context.Context, key string) error {
	desired := r.collections.AgentStatuses.GetKey(key)
	template := r.collections.Agents.GetKey(key)
	if desired == nil || template == nil {
		return nil
	}
	updated := (*template).DeepCopy()
	updated.Status = statusWithTransitionTimes(desired.Status, updated.Status)
	if apiequality.Semantic.DeepEqual(updated.Status, (*template).Status) {
		return nil
	}
	if _, err := r.status.Agents(updated.Namespace).UpdateStatus(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update Agent %s status: %w", key, err)
	}
	return nil
}

func (r *Reconciler) reconcileModelConfigStatus(ctx context.Context, key string) error {
	desired := r.collections.ModelConfigStatuses.GetKey(key)
	modelConfig := r.collections.ModelConfigs.GetKey(key)
	if desired == nil || modelConfig == nil {
		return nil
	}
	updated := (*modelConfig).DeepCopy()
	updated.Status = modelConfigStatusWithTransitionTimes(desired.Status, updated.Status)
	if apiequality.Semantic.DeepEqual(updated.Status, (*modelConfig).Status) {
		return nil
	}
	if _, err := r.status.ModelConfigs(updated.Namespace).UpdateStatus(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update ModelConfig %s status: %w", key, err)
	}
	return nil
}

func statusWithTransitionTimes(desired, current kagentv1alpha3.AgentStatus) kagentv1alpha3.AgentStatus {
	desired.Conditions = append([]metav1.Condition(nil), desired.Conditions...)
	for i := range desired.Conditions {
		condition := &desired.Conditions[i]
		previous := apimeta.FindStatusCondition(current.Conditions, condition.Type)
		if previous != nil && previous.Status == condition.Status {
			condition.LastTransitionTime = previous.LastTransitionTime
		} else {
			condition.LastTransitionTime = metav1.Now()
		}
	}
	return desired
}

func modelConfigStatusWithTransitionTimes(desired, current kagentv1alpha3.ModelConfigStatus) kagentv1alpha3.ModelConfigStatus {
	desired.Conditions = append([]metav1.Condition(nil), desired.Conditions...)
	for conditionIndex := range desired.Conditions {
		condition := &desired.Conditions[conditionIndex]
		if previous := apimeta.FindStatusCondition(current.Conditions, condition.Type); previous != nil &&
			previous.Status == condition.Status && previous.Reason == condition.Reason &&
			previous.Message == condition.Message && previous.ObservedGeneration == condition.ObservedGeneration {
			condition.LastTransitionTime = previous.LastTransitionTime
			continue
		}
		condition.LastTransitionTime = metav1.Now()
	}
	return desired
}
