package translator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"

	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/kagent-dev/kagent/go/core/internal/egress"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
)

const shortRevisionBytes = 6

// RevisionID is the SHA-256 identity of a compiled runtime revision. Keeping
// the digest as a fixed-size value makes invalid lengths unrepresentable.
type RevisionID [sha256.Size]byte

// String returns the full database identity.
func (id RevisionID) String() string { return hex.EncodeToString(id[:]) }

// Short returns the readable prefix used in Kubernetes names and labels.
func (id RevisionID) Short() string { return hex.EncodeToString(id[:shortRevisionBytes]) }

// IsZero reports whether compilation has not produced an identity.
func (id RevisionID) IsZero() bool { return id == RevisionID{} }

// CompileResult contains one immutable runtime revision and the non-blocking
// diagnostics produced while compiling it. Diagnostics are deliberately kept
// outside Revision because they do not describe runtime behavior.
type CompileResult struct {
	Revision
	Warnings []string
}

// Revision is the resolved runtime configuration for one immutable revision.
type Revision struct {
	// These fields identify the Agent that produced the revision.
	AgentName string
	AgentUID  string
	Namespace string

	// Image and Environment describe the runtime container.
	Image       string
	Command     []string
	Args        []string
	Environment []corev1.EnvVar
	// ConfigJSON is injected into the runtime container verbatim.
	// AgentCard stays typed until a runtime or public protocol boundary renders it.
	ConfigJSON []byte
	AgentCard  *a2apb.AgentCard

	// GroupRef pins the AX capacity identity; SnapshotLocation is an optional override.
	TaskGroupName    string
	GroupRef         *ax.ResourceRef
	SnapshotLocation string

	// Provenance identifies non-secret Kubernetes inputs. Gateway-fetched
	// credential values are deliberately excluded from revision identity.
	Provenance json.RawMessage
	// Credentials contains references resolved by the egress gateway.
	Credentials []egress.Credential
	// EgressDestinations is the hostname allowlist required by this revision.
	EgressDestinations []string
}

// Equals compares the Agent Card's contents without inspecting protobuf caches.
func (r Revision) Equals(other Revision) bool {
	if !proto.Equal(r.AgentCard, other.AgentCard) || !proto.Equal(r.GroupRef, other.GroupRef) || !egress.CredentialsEqual(r.Credentials, other.Credentials) {
		return false
	}
	r.AgentCard, other.AgentCard = nil, nil
	r.GroupRef, other.GroupRef = nil, nil
	r.Credentials, other.Credentials = nil, nil
	return reflect.DeepEqual(r, other)
}

// Digest returns the immutable identity of every input that affects runtime
// behavior. The full digest is the database key; Kubernetes names use a short
// prefix only for readability.
func (r *Revision) Digest() (RevisionID, error) {
	raw, err := json.Marshal(struct {
		AgentName          string              `json:"agentName"`
		AgentUID           string              `json:"agentUID"`
		Namespace          string              `json:"namespace"`
		Image              string              `json:"image"`
		Command            []string            `json:"command,omitempty"`
		Args               []string            `json:"args,omitempty"`
		Environment        []corev1.EnvVar     `json:"environment"`
		ConfigJSON         json.RawMessage     `json:"config"`
		TaskGroupName      string              `json:"taskGroupName"`
		SnapshotLocation   string              `json:"snapshotLocation"`
		Provenance         json.RawMessage     `json:"provenance"`
		Credentials        []egress.Credential `json:"credentials,omitempty"`
		EgressDestinations []string            `json:"egressDestinations"`
		GroupRef           *ax.ResourceRef     `json:"groupRef"`
	}{
		AgentName: r.AgentName, AgentUID: r.AgentUID, Namespace: r.Namespace,
		Image: r.Image, Command: r.Command, Args: r.Args, Environment: r.Environment, ConfigJSON: r.ConfigJSON,
		TaskGroupName: r.TaskGroupName, SnapshotLocation: r.SnapshotLocation, Provenance: r.Provenance,
		Credentials: r.Credentials, EgressDestinations: r.EgressDestinations,
		GroupRef: r.GroupRef,
	})
	if err != nil {
		return RevisionID{}, fmt.Errorf("marshal runtime revision inputs: %w", err)
	}
	card, err := proto.MarshalOptions{Deterministic: true}.Marshal(r.AgentCard)
	if err != nil {
		return RevisionID{}, fmt.Errorf("marshal runtime revision Agent Card: %w", err)
	}
	return RevisionID(sha256.Sum256(append(raw, card...))), nil
}
