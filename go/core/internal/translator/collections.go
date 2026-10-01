package translator

import (
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/kagent-dev/kagent/go/api/v1alpha3"
	"google.golang.org/protobuf/proto"
	"istio.io/istio/pkg/kube/krt"
	corev1 "k8s.io/api/core/v1"
)

// Collections contains every typed input used while compiling a revision.
// Production supplies informer-backed collections; tests supply KRT mocks.
type Collections struct {
	Harnesses            krt.Collection[*v1alpha3.Harness]
	AgentTemplates       krt.Collection[*v1alpha3.AgentTemplate]
	ResolvedModelConfigs krt.Collection[ResolvedModelConfig]
	RemoteMCPServers     krt.Collection[*v1alpha3.RemoteMCPServer]
	ConfigMaps           krt.Collection[*corev1.ConfigMap]
	Secrets              krt.Collection[*corev1.Secret]
	TaskGroups           krt.Collection[TaskGroupObservation]
}

// TaskGroupObservation is an immutable AX observation in the pure input graph.
type TaskGroupObservation struct{ Group *ax.TaskGroup }

func (g TaskGroupObservation) ResourceName() string {
	return g.Group.Metadata.Atespace + "/" + g.Group.Metadata.Name
}
func (g TaskGroupObservation) Equals(other TaskGroupObservation) bool {
	return proto.Equal(g.Group, other.Group)
}
