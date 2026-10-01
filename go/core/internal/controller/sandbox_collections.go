package controller

import (
	"encoding/json"
	"fmt"
	"reflect"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	kagentv1alpha3 "github.com/kagent-dev/kagent/go/api/v1alpha3"
	"github.com/kagent-dev/kagent/go/core/internal/axruntime"
	"google.golang.org/protobuf/proto"
	"istio.io/istio/pkg/kube/krt"
)

// Sandbox preparation uses the same informer graph as Agents. Only observations
// of external work are mutable; compilation never performs I/O.
type sandboxCollections struct {
	states       krt.Collection[sandboxReconciliation]
	observations krt.StaticCollection[sandboxRuntimeObservation]
	groups       krt.StaticCollection[sandboxGroupObservation]
}

type sandboxReconciliation struct {
	Template         *kagentv1alpha3.SandboxTemplate
	RevisionID       string
	SourceSnapshot   json.RawMessage
	DesiredRuntime   *ax.PreparedRuntime
	ObservedRuntime  *ax.PreparedRuntime
	CompilationError string
	Failure          *ReconciliationFailure
}

func (s sandboxReconciliation) ResourceName() string {
	return s.Template.Namespace + "/" + s.Template.Name
}

var _ krt.Equaler[sandboxReconciliation] = sandboxReconciliation{}

func (s sandboxReconciliation) Equals(other sandboxReconciliation) bool {
	if !proto.Equal(s.DesiredRuntime, other.DesiredRuntime) || !proto.Equal(s.ObservedRuntime, other.ObservedRuntime) {
		return false
	}
	s.DesiredRuntime, other.DesiredRuntime = nil, nil
	s.ObservedRuntime, other.ObservedRuntime = nil, nil
	return reflect.DeepEqual(s, other)
}

func (s sandboxReconciliation) desiredRevision() string {
	if s.RevisionID != "" {
		return s.RevisionID
	}
	// Unresolved inputs must replace the old desired edge without inventing a
	// runnable revision. Kubernetes generation identifies the requested spec.
	return fmt.Sprintf("pending:%s:%d", s.Template.UID, s.Template.Generation)
}

func (s sandboxReconciliation) canPrepare() bool {
	return s.DesiredRuntime != nil && (s.Failure == nil || s.Failure.Retryable)
}

type sandboxRuntimeObservation struct {
	Key        string
	RevisionID string
	Template   *ax.PreparedRuntime
	Failure    *ReconciliationFailure
}

func (s sandboxRuntimeObservation) ResourceName() string { return s.Key }

var _ krt.Equaler[sandboxRuntimeObservation] = sandboxRuntimeObservation{}

func (s sandboxRuntimeObservation) Equals(other sandboxRuntimeObservation) bool {
	if !proto.Equal(s.Template, other.Template) {
		return false
	}
	s.Template, other.Template = nil, nil
	return reflect.DeepEqual(s, other)
}

func newSandboxCollections(inputs Collections, policy axruntime.SandboxPolicy, opts krt.OptionsBuilder) sandboxCollections {
	groups := krt.NewStaticCollection[sandboxGroupObservation](nil, nil, opts.WithName("SandboxTaskGroups")...)
	observations := krt.NewStaticCollection[sandboxRuntimeObservation](nil, nil, opts.WithName("SandboxRuntimeObservations")...)
	states := krt.NewCollection(inputs.SandboxTemplates, func(ctx krt.HandlerContext, template *kagentv1alpha3.SandboxTemplate) *sandboxReconciliation {
		state := &sandboxReconciliation{Template: template}
		if !template.DeletionTimestamp.IsZero() {
			return state
		}
		pool := krt.FetchOne(ctx, groups, krt.FilterKey(template.Namespace+"/"+template.Spec.AX.TaskGroupRef.Name))
		if pool == nil {
			state.Failure = &ReconciliationFailure{Reason: "TaskGroupUnresolved", Message: "Waiting for AX TaskGroup identity", Retryable: true}
		} else {
			var err error
			state.DesiredRuntime, state.RevisionID, state.SourceSnapshot, err = axruntime.SandboxRuntime(template, pool.Ref, policy)
			if err != nil {
				state.CompilationError = err.Error()
				state.Failure = &ReconciliationFailure{Reason: sandboxPreparationFailed, Message: sandboxPreparationFailureMessage}
			}
		}
		observation := krt.FetchOne(ctx, observations, krt.FilterKey(state.ResourceName()))
		if observation == nil || observation.RevisionID != state.desiredRevision() {
			return state
		}
		if observation.Failure != nil {
			state.Failure = observation.Failure
		} else if state.Failure == nil {
			state.ObservedRuntime = observation.Template
		}
		return state
	}, opts.WithName("SandboxReconciliations")...)
	return sandboxCollections{states: states, observations: observations, groups: groups}
}

// Group observations keep network I/O outside the KRT compiler.
type sandboxGroupObservation struct{ Ref *ax.ResourceRef }

func (g sandboxGroupObservation) ResourceName() string { return g.Ref.Atespace + "/" + g.Ref.Name }
func (g sandboxGroupObservation) Equals(other sandboxGroupObservation) bool {
	return proto.Equal(g.Ref, other.Ref)
}
