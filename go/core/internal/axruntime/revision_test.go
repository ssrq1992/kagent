package axruntime

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/kagent-dev/kagent/go/core/internal/translator"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
)

func TestRuntimeForRevision(t *testing.T) {
	spec := &translator.Revision{
		Image: "agent.example/image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Namespace: "agents", AgentName: "helper", GroupRef: &ax.ResourceRef{Atespace: "agents", Name: "default", Uid: "group-uid"},
		Command:       []string{"/agent"},
		Args:          []string{"serve"},
		TaskGroupName: "default", SnapshotLocation: "s3://snapshots",
		ConfigJSON: []byte(`{"instruction":"help"}`), AgentCard: &a2apb.AgentCard{Name: "helper", Version: "v1", Capabilities: &a2apb.AgentCapabilities{Streaming: new(true)},
			SupportedInterfaces: []*a2apb.AgentInterface{{Url: "http://127.0.0.1:80", ProtocolBinding: "GRPC", ProtocolVersion: "1.0"}}, DefaultInputModes: []string{"text"}, DefaultOutputModes: []string{"text"}},
		Environment: []corev1.EnvVar{{Name: "API_KEY", Value: translator.CredentialPlaceholder}},
	}
	revisionID, err := spec.Digest()
	if err != nil {
		t.Fatal(err)
	}
	template, err := RuntimeForRevision(spec, revisionID)
	if err != nil {
		t.Fatal(err)
	}
	if template.GetMetadata().GetAtespace() != "agents" || template.GetMetadata().GetName() != "helper-"+revisionID.Short() {
		t.Fatalf("PreparedRuntime = %+v", template)
	}
	container := template.GetSpec()
	if !slices.Equal(container.Command, spec.Command) || !slices.Equal(container.Args, spec.Args) {
		t.Fatalf("container command/args = %v %v", container.Command, container.Args)
	}
	require.Equal(t, "Service", container.Kind)
	require.Equal(t, "/readyz", container.Readiness.Path)
	require.EqualValues(t, 8081, container.Readiness.Port)
	require.EqualValues(t, 30, container.Readiness.TimeoutSeconds)
	environment := map[string]*ax.EnvVar{}
	for _, v := range container.Env {
		environment[v.Name] = v
	}
	var rendered a2atype.AgentCard
	if err := json.Unmarshal([]byte(environment["KAGENT_AGENT_CARD_JSON"].Value), &rendered); err != nil {
		t.Fatal(err)
	}
	if rendered.Name != spec.AgentCard.Name || rendered.Description != "" || !rendered.Capabilities.Streaming || len(rendered.Skills) != 0 {
		t.Fatalf("runtime card = %#v", rendered)
	}
	if environment["KAGENT_CONFIG_JSON"].Value != string(spec.ConfigJSON) {
		t.Fatal("config was not embedded as a non-secret literal")
	}
}

func TestPreparedRuntimeStampsTheRevisionOnTheResource(t *testing.T) {
	spec := &translator.Revision{
		Image: "agent.example/image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Namespace: "agents", AgentName: "helper", GroupRef: &ax.ResourceRef{Atespace: "agents", Name: "default", Uid: "group-uid"}, TaskGroupName: "default",
		AgentCard: &a2apb.AgentCard{Name: "helper", Version: "v1", Capabilities: &a2apb.AgentCapabilities{},
			SupportedInterfaces: []*a2apb.AgentInterface{{Url: "http://127.0.0.1:80", ProtocolBinding: "GRPC", ProtocolVersion: "1.0"}},
			DefaultInputModes:   []string{"text"}, DefaultOutputModes: []string{"text"}},
		Environment: []corev1.EnvVar{{Name: "OTEL_RESOURCE_ATTRIBUTES", Value: "gen_ai.agent.name=helper-kagent,service.version=forged"}},
	}
	revisionID, err := spec.Digest()
	if err != nil {
		t.Fatal(err)
	}
	template, err := RuntimeForRevision(spec, revisionID)
	if err != nil {
		t.Fatal(err)
	}
	for _, variable := range template.GetSpec().Env {
		if variable.Name == "OTEL_RESOURCE_ATTRIBUTES" {
			if want := "gen_ai.agent.name=helper-kagent,service.version=" + revisionID.Short(); variable.Value != want {
				t.Fatalf("resource attributes = %q, want %q", variable.Value, want)
			}
			if spec.Environment[0].Value != "gen_ai.agent.name=helper-kagent,service.version=forged" {
				t.Fatal("stamping the revision changed the compiled revision")
			}
			return
		}
	}
	t.Fatal("resource attributes missing from the actor")
}

func TestRuntimeSpecEqualIgnoresServerFields(t *testing.T) {
	left := &ax.PreparedRuntime{Metadata: &ax.ObjectMeta{Atespace: "team-a", Name: "runtime"}, Spec: &ax.PreparedRuntimeSpec{Image: "image:v1"}}
	right := proto.CloneOf(left)
	right.Metadata.Uid = "uid"
	right.Metadata.ResourceVersion = 2
	right.Phase = "Ready"
	require.True(t, RuntimeSpecEqual(left, right))
	right.Spec.Image = "image:v2"
	require.False(t, RuntimeSpecEqual(left, right))
}

func TestPreparedRuntimeEnvironmentValueSize(t *testing.T) {
	const configOverhead = len(`{"instruction":""}`)
	for _, test := range []struct {
		name        string
		instruction string
		description string
		environment []corev1.EnvVar
		wantError   string
	}{
		{name: "ASCII boundary", instruction: strings.Repeat("a", 32768-configOverhead)},
		{name: "ASCII overflow", instruction: strings.Repeat("a", 32769-configOverhead), wantError: "invalid or duplicate runtime environment variable"},
		{name: "Unicode boundary", instruction: strings.Repeat("日", 32768-configOverhead)},
		{name: "Unicode overflow", instruction: strings.Repeat("日", 32769-configOverhead), wantError: "invalid or duplicate runtime environment variable"},
		{name: "JSON escaping", instruction: strings.Repeat("<>&", 2000), wantError: "invalid or duplicate runtime environment variable"},
		{name: "user environment", environment: []corev1.EnvVar{{Name: "EXTRA", Value: strings.Repeat("x", 32769)}}, wantError: "invalid or duplicate runtime environment variable"},
		{name: "agent card", description: strings.Repeat("x", 32769), wantError: "invalid or duplicate runtime environment variable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config, err := json.Marshal(struct {
				Instruction string `json:"instruction"`
			}{test.instruction})
			require.NoError(t, err)
			spec := &translator.Revision{
				Image: "agent.example/image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Namespace: "agents", AgentName: "helper", GroupRef: &ax.ResourceRef{Atespace: "agents", Name: "default", Uid: "group-uid"}, TaskGroupName: "default",
				ConfigJSON: config, Environment: test.environment,
				AgentCard: &a2apb.AgentCard{Name: "helper", Description: test.description, Version: "v1", Capabilities: &a2apb.AgentCapabilities{},
					SupportedInterfaces: []*a2apb.AgentInterface{{Url: "http://127.0.0.1:80", ProtocolBinding: "GRPC", ProtocolVersion: "1.0"}},
					DefaultInputModes:   []string{"text"}, DefaultOutputModes: []string{"text"}},
			}
			id, err := spec.Digest()
			require.NoError(t, err)
			template, err := RuntimeForRevision(spec, id)
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)
				require.Nil(t, template)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, template)
		})
	}
}
