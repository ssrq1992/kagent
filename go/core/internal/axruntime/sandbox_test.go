package axruntime

import (
	"strings"
	"testing"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/kagent-dev/kagent/go/api/v1alpha3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSandboxRuntimePinsInputsAndGroupUID(t *testing.T) {
	template := &v1alpha3.SandboxTemplate{ObjectMeta: metav1.ObjectMeta{Namespace: "team", Name: "sandbox", UID: "template-uid"}, Spec: v1alpha3.SandboxTemplateSpec{Workload: v1alpha3.SandboxTemplateWorkload{Image: "tools@sha256:" + strings.Repeat("a", 64)}, AX: v1alpha3.RuntimeAXPolicy{TaskGroupRef: corev1.LocalObjectReference{Name: "group"}}}}
	group := &ax.ResourceRef{Atespace: "team", Name: "group", Uid: "group-uid"}
	policy := SandboxPolicy{CPU: "1", Memory: "1Gi"}
	runtime, digest, snapshot, err := SandboxRuntime(template, group, policy)
	require.NoError(t, err)
	require.Equal(t, "Sandbox", runtime.Spec.Kind)
	require.Equal(t, "1Gi", runtime.Spec.Resources.Limits.Memory)
	require.Empty(t, runtime.Spec.Command)
	require.Contains(t, string(snapshot), group.Uid)
	_, same, _, err := SandboxRuntime(template, group, policy)
	require.NoError(t, err)
	require.Equal(t, digest, same)
	for _, change := range []string{"group", "template", "image", "resources", "storage"} {
		t.Run(change, func(t *testing.T) {
			input := template.DeepCopy()
			ref := proto.CloneOf(group)
			settings := policy
			switch change {
			case "group":
				ref.Uid = "replacement"
			case "template":
				input.UID = "replacement"
			case "image":
				input.Spec.Workload.Image = "tools@sha256:" + strings.Repeat("b", 64)
			case "resources":
				settings.Memory = "2Gi"
			case "storage":
				input.Spec.AX.SnapshotLocationOverride = "s3://bucket/snapshots"
			}
			_, changed, _, err := SandboxRuntime(input, ref, settings)
			require.NoError(t, err)
			require.NotEqual(t, digest, changed)
		})
	}
	template.Spec.Env = []v1alpha3.RuntimeEnvVar{{Name: "TOKEN", CredentialRef: &corev1.SecretKeySelector{}}}
	_, _, _, err = SandboxRuntime(template, group, policy)
	require.Error(t, err)
	template.Spec.Env = []v1alpha3.RuntimeEnvVar{{Name: "SSL_CERT_FILE", Value: new("/untrusted")}}
	_, _, _, err = SandboxRuntime(template, group, policy)
	require.ErrorContains(t, err, "reserved")
	group.Atespace = "other"
	_, _, _, err = SandboxRuntime(template, group, policy)
	require.Error(t, err)
}
