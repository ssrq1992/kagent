package axruntime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/kagent-dev/kagent/go/api/v1alpha3"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/resource"
)

type SandboxPolicy struct{ CPU, Memory string }

// SandboxRuntime is pure: group identity is supplied from an AX observation,
// and the AX platform owns its guest image, volumes, trust and sandbox class.
func SandboxRuntime(template *v1alpha3.SandboxTemplate, group *ax.ResourceRef, policy SandboxPolicy) (*ax.PreparedRuntime, string, json.RawMessage, error) {
	if template == nil || group.GetAtespace() != template.Namespace || group.GetName() != template.Spec.AX.TaskGroupRef.Name {
		return nil, "", nil, fmt.Errorf("sandbox TaskGroup must be in the template namespace")
	}
	if err := ax.ValidateRef(group, true); err != nil {
		return nil, "", nil, err
	}
	for name, value := range map[string]string{"cpu": policy.CPU, "memory": policy.Memory} {
		q, err := resource.ParseQuantity(value)
		if err != nil || q.Sign() <= 0 || (name == "cpu" && q.Cmp(resource.MustParse("1000")) >= 0) {
			return nil, "", nil, fmt.Errorf("invalid sandbox %s limit", name)
		}
	}
	spec := &ax.PreparedRuntimeSpec{Kind: "Sandbox", GroupRef: proto.CloneOf(group), Image: template.Spec.Workload.Image, Resources: &ax.ResourceReqs{Limits: &ax.ResourceList{Cpu: policy.CPU, Memory: policy.Memory}}, SnapshotLocationOverride: template.Spec.AX.SnapshotLocationOverride}
	for _, env := range template.Spec.Env {
		if env.CredentialRef != nil || env.Value == nil || strings.HasPrefix(env.Name, "KAGENT_") || strings.HasPrefix(env.Name, "AX_") || strings.HasPrefix(env.Name, "ATE_") {
			return nil, "", nil, fmt.Errorf("sandbox environment %q must be an unreserved literal", env.Name)
		}
		spec.Env = append(spec.Env, &ax.EnvVar{Name: env.Name, Value: *env.Value})
	}
	if err := ax.ValidatePreparedRuntime(spec); err != nil {
		return nil, "", nil, err
	}
	snapshot, err := json.Marshal(struct {
		Version              int
		Namespace, Name, UID string
		Spec                 *ax.PreparedRuntimeSpec
	}{2, template.Namespace, template.Name, string(template.UID), spec})
	if err != nil {
		return nil, "", nil, err
	}
	sum := sha256.Sum256(snapshot)
	revision := hex.EncodeToString(sum[:])
	return &ax.PreparedRuntime{Metadata: &ax.ObjectMeta{Atespace: template.Namespace, Name: "sandbox-" + revision[:40]}, Spec: spec}, revision, snapshot, nil
}
