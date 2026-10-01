package axruntime

import (
	"encoding/json"
	"fmt"
	"strings"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	apia2a "github.com/kagent-dev/kagent/go/api/a2a"
	"github.com/kagent-dev/kagent/go/core/internal/translator"
	"github.com/kagent-dev/kagent/go/pkg/tracing"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"google.golang.org/protobuf/proto"
)

// RuntimeForRevision renders business configuration into AX's public contract.
// AX owns compute placement, guest bootstrapping, readiness execution and trust.
func RuntimeForRevision(revision *translator.Revision, id translator.RevisionID) (*ax.PreparedRuntime, error) {
	if id.IsZero() {
		return nil, fmt.Errorf("runtime revision ID is required")
	}
	if err := ax.ValidateRef(revision.GroupRef, true); err != nil {
		return nil, err
	}
	if revision.GroupRef.Atespace != revision.Namespace {
		return nil, fmt.Errorf("cross-namespace TaskGroup")
	}
	card, err := apia2a.FromProtoAgentCard(revision.AgentCard)
	if err != nil {
		return nil, err
	}
	cardJSON, err := json.Marshal(card)
	if err != nil {
		return nil, err
	}
	spec := &ax.PreparedRuntimeSpec{Kind: "Service", GroupRef: proto.CloneOf(revision.GroupRef), Image: revision.Image, Command: append([]string(nil), revision.Command...), Args: append([]string(nil), revision.Args...), SnapshotLocationOverride: revision.SnapshotLocation, Readiness: &ax.Readiness{Path: "/readyz", Port: 8081, TimeoutSeconds: 30}, Egress: &ax.EgressSpec{Destinations: append([]string(nil), revision.EgressDestinations...)}}
	seen := map[string]bool{}
	for _, v := range revision.Environment {
		if v.Name == "" {
			continue
		}
		if v.ValueFrom != nil {
			return nil, fmt.Errorf("runtime environment %q must be resolved to a literal", v.Name)
		}
		if seen[v.Name] {
			continue
		}
		seen[v.Name] = true
		if v.Name == "KAGENT_CONFIG_JSON" || v.Name == "KAGENT_AGENT_CARD_JSON" {
			return nil, fmt.Errorf("environment %q is compiler-owned", v.Name)
		}
		value := v.Value
		if v.Name == tracing.ResourceEnvironmentVariable {
			value = tracing.MergeResourceAttributes(value, []attribute.KeyValue{semconv.ServiceVersion(id.Short())})
		}
		spec.Env = append(spec.Env, &ax.EnvVar{Name: v.Name, Value: value})
	}
	spec.Env = append(spec.Env, &ax.EnvVar{Name: "KAGENT_CONFIG_JSON", Value: string(revision.ConfigJSON)}, &ax.EnvVar{Name: "KAGENT_AGENT_CARD_JSON", Value: string(cardJSON)})
	for _, credential := range revision.Credentials {
		if credential.SecretKeyRef == nil || credential.SecretKeyRef.Namespace != revision.Namespace {
			return nil, fmt.Errorf("credential must reference a same-namespace Kubernetes Secret")
		}
		spec.Egress.Credentials = append(spec.Egress.Credentials, &ax.EgressCredential{Hostname: credential.Hostname, Header: credential.Header, Prefix: credential.Prefix, SecretKeyRef: proto.CloneOf(credential.SecretKeyRef)})
	}
	if err := ax.ValidatePreparedRuntime(spec); err != nil {
		return nil, err
	}
	base := strings.ToLower(strings.ReplaceAll(revision.AgentName, "_", "-"))
	if len(base) > 50 {
		base = strings.TrimRight(base[:50], "-")
	}
	return &ax.PreparedRuntime{Metadata: &ax.ObjectMeta{Atespace: revision.Namespace, Name: base + "-" + id.Short()}, Spec: spec}, nil
}
func RuntimeSpecEqual(left, right *ax.PreparedRuntime) bool {
	return left != nil && right != nil && left.GetMetadata().GetAtespace() == right.GetMetadata().GetAtespace() && left.GetMetadata().GetName() == right.GetMetadata().GetName() && proto.Equal(left.Spec, right.Spec)
}
