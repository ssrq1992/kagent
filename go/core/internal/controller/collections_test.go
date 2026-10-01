package controller

import (
	"testing"
	"time"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	kagentv1alpha3 "github.com/kagent-dev/kagent/go/api/v1alpha3"
	v2translator "github.com/kagent-dev/kagent/go/core/internal/translator"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"istio.io/istio/pkg/kube/krt"
	"istio.io/istio/pkg/kube/krt/krttest"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestReconciliationCollectionsCompileAndObserveRevision(t *testing.T) {
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	opts := krt.NewOptionsBuilder(stop, "test", nil)

	template := &kagentv1alpha3.AgentTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "assistant", UID: "template-uid", Labels: map[string]string{"runtime": "python"}},
		Spec: kagentv1alpha3.AgentTemplateSpec{
			ModelConfig:  &corev1.LocalObjectReference{Name: "model"},
			SystemPrompt: "help",
		},
	}
	matchingHarness := harness("team-a", "kagent", map[string]string{"runtime": "python"})
	matchingHarness.UID = "harness-uid"
	matchingHarness.Spec.Kagent = &kagentv1alpha3.KagentHarness{}
	matchingHarness.Spec.Workload.Image = "example.com/kagent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	matchingHarness.Spec.AX = kagentv1alpha3.RuntimeAXPolicy{
		TaskGroupRef:             corev1.LocalObjectReference{Name: "default"},
		SnapshotLocationOverride: "s3://snapshots",
	}
	modelConfigs := krt.NewStaticCollection(nil, []*kagentv1alpha3.ModelConfig{{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model"}, Spec: kagentv1alpha3.ModelConfigSpec{Provider: kagentv1alpha3.ModelProviderOpenAI, Model: "gpt-5"}}}, opts.WithName("ModelConfigs")...)
	templates := krt.NewStaticCollection[*kagentv1alpha3.AgentTemplate](nil, nil, opts.WithName("AgentTemplates")...)
	mock := krttest.NewMock(t, []any{
		matchingHarness,
		testTaskGroup("team-a", "default", "default-uid"),
	})

	collections := Collections{
		AgentTemplates:           templates,
		Harnesses:                krttest.GetMockCollection[*kagentv1alpha3.Harness](mock),
		ModelConfigs:             modelConfigs,
		RemoteMCPServers:         krttest.GetMockCollection[*kagentv1alpha3.RemoteMCPServer](mock),
		ConfigMaps:               krttest.GetMockCollection[*corev1.ConfigMap](mock),
		Secrets:                  krttest.GetMockCollection[*corev1.Secret](mock),
		TaskGroups:               krt.NewStaticCollection(nil, krttest.GetMockCollection[v2translator.TaskGroupObservation](mock).List(), opts.WithName("AXTaskGroups")...),
		AgentRuntimeObservations: krt.NewStaticCollection[AgentRuntimeObservation](nil, nil, opts.WithName("AgentRuntimeObservations")...),
	}
	collections.ModelConfigStatuses, collections.ResolvedModelConfigs = newModelConfigReconciliations(collections.ModelConfigs, collections.ConfigMaps, collections.Secrets, opts)
	collections.Agents = krt.NewStaticCollection(nil, []*kagentv1alpha3.Agent{testAgent(template, matchingHarness)}, opts.WithName("Agents")...)
	collections.Reconciliations = newAgentReconciliations(
		collections.Agents, v2translator.Collections{
			Harnesses: collections.Harnesses, AgentTemplates: collections.AgentTemplates, ResolvedModelConfigs: collections.ResolvedModelConfigs,
			RemoteMCPServers: collections.RemoteMCPServers, ConfigMaps: collections.ConfigMaps,
			Secrets: collections.Secrets, TaskGroups: collections.TaskGroups,
		}, collections.AgentRuntimeObservations, opts,
	)
	collections.AgentStatuses = newAgentStatuses(collections.Agents, collections.Reconciliations, opts)

	// The Agent informer can observe a new Agent before the template informer
	// observes its reference. Adding the template must clear the failure without
	// changing or re-enqueuing the Agent explicitly.
	waitFor(t, func() bool {
		updates := collections.AgentStatuses.List()
		if len(updates) != 1 {
			return false
		}
		condition := apimeta.FindStatusCondition(updates[0].Status.Conditions, kagentv1alpha3.AgentConditionResolvedRefs)
		return condition != nil && condition.Status == metav1.ConditionFalse && condition.Reason == "ReferenceResolutionFailed"
	})
	templates.UpdateObject(template)

	waitFor(t, func() bool {
		states := collections.Reconciliations.List()
		return len(states) == 1 && states[0].CompilationFailure == nil && states[0].Target != nil
	})
	state := collections.Reconciliations.List()[0]
	if state.ObservedRuntime != nil {
		t.Fatal("ActorTemplate was observed before it existed")
	}
	waitFor(t, func() bool {
		updates := collections.AgentStatuses.List()
		if len(updates) != 1 {
			return false
		}
		ready := apimeta.FindStatusCondition(updates[0].Status.Conditions, kagentv1alpha3.AgentConditionReady)
		return ready != nil && ready.Status == metav1.ConditionFalse
	})

	observed := proto.CloneOf(state.Target.Runtime)
	observed.Metadata.Uid = "actor-template-uid"
	observed.Phase = "Ready"
	store := &fakeRuntimeRevisionStore{}
	reconciler := &Reconciler{
		collections: collections, templates: &fakePreparedRuntimes{template: observed}, store: store,
	}
	require.NoError(t, reconciler.reconcileAgent(t.Context(), state.ResourceName()))
	waitFor(t, func() bool {
		states := collections.Reconciliations.List()
		updates := collections.AgentStatuses.List()
		if len(states) != 1 || states[0].ObservedRuntime == nil || len(updates) != 1 {
			return false
		}
		ready := apimeta.FindStatusCondition(updates[0].Status.Conditions, kagentv1alpha3.AgentConditionReady)
		return ready != nil && ready.Status == metav1.ConditionTrue && updates[0].Status.LatestSuccessfulRevision == state.Target.RevisionID.String()
	})

	modelConfigs.UpdateObject(&kagentv1alpha3.ModelConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model"}, Spec: kagentv1alpha3.ModelConfigSpec{Provider: kagentv1alpha3.ModelProviderOpenAI, Model: "gpt-5.1"}})
	waitFor(t, func() bool {
		states := collections.Reconciliations.List()
		return len(states) == 1 && states[0].Target.RevisionID != state.Target.RevisionID && states[0].ObservedRuntime == nil
	})
}

func TestReconciliationWorkerPoolSandboxClass(t *testing.T) {
	for _, harnessType := range []v2translator.HarnessType{
		v2translator.HarnessTypeKagent, v2translator.HarnessTypeCodex, v2translator.HarnessTypeClaude, v2translator.HarnessTypeBYO,
	} {
		t.Run(string(harnessType), func(t *testing.T) {
			stop := make(chan struct{})
			t.Cleanup(func() { close(stop) })
			opts := krt.NewOptionsBuilder(stop, "test-sandbox", nil)
			template := &kagentv1alpha3.AgentTemplate{
				ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "assistant", UID: "template-uid"},
				Spec:       kagentv1alpha3.AgentTemplateSpec{ModelConfig: &corev1.LocalObjectReference{Name: "model"}, SystemPrompt: "help"},
			}
			runtimeHarness := harness("team-a", string(harnessType), nil)
			runtimeHarness.UID = "harness-uid"
			runtimeHarness.Spec.Workload.Image = "example.com/agent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			runtimeHarness.Spec.AX = kagentv1alpha3.RuntimeAXPolicy{
				TaskGroupRef: corev1.LocalObjectReference{Name: "selected"}, SnapshotLocationOverride: "s3://snapshots",
			}
			responses := kagentv1alpha3.OpenAIAPIFormatResponses
			model := &kagentv1alpha3.ModelConfig{
				ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model", UID: "model-uid"},
				Spec: kagentv1alpha3.ModelConfigSpec{
					Provider: kagentv1alpha3.ModelProviderOpenAI, Model: "gpt-5", APIKeySecret: "model-auth", APIKeySecretKey: "api-key",
					OpenAI: &kagentv1alpha3.OpenAIConfig{APIFormat: &responses},
				},
			}
			switch harnessType {
			case v2translator.HarnessTypeKagent:
				runtimeHarness.Spec.Kagent = &kagentv1alpha3.KagentHarness{}
			case v2translator.HarnessTypeCodex:
				runtimeHarness.Spec.Codex = &kagentv1alpha3.CodexHarness{}
			case v2translator.HarnessTypeClaude:
				runtimeHarness.Spec.Claude = &kagentv1alpha3.ClaudeHarness{}
				model.Spec.Provider, model.Spec.Model, model.Spec.OpenAI = kagentv1alpha3.ModelProviderAnthropic, "claude-sonnet-4-5", nil
			case v2translator.HarnessTypeBYO:
				runtimeHarness.Spec.BYO = &kagentv1alpha3.BYOHarness{}
				runtimeHarness.Spec.Workload.Command = []string{"/agent"}
				template.Spec.ModelConfig = nil
			}
			mock := krttest.NewMock(t, []any{
				template, runtimeHarness, model,
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model-auth"}, Data: map[string][]byte{"api-key": []byte("secret")}},
			})
			templates := krttest.GetMockCollection[*kagentv1alpha3.AgentTemplate](mock)
			agents := krt.NewStaticCollection(nil, []*kagentv1alpha3.Agent{testAgent(template, runtimeHarness)}, opts.WithName("Agents")...)
			configMaps := krttest.GetMockCollection[*corev1.ConfigMap](mock)
			secrets := krttest.GetMockCollection[*corev1.Secret](mock)
			_, resolvedModels := newModelConfigReconciliations(krttest.GetMockCollection[*kagentv1alpha3.ModelConfig](mock), configMaps, secrets, opts)
			workerPools := krt.NewStaticCollection(nil, []v2translator.TaskGroupObservation{
				testTaskGroup("team-b", "selected", "foreign-uid"), testTaskGroup("team-a", "unselected", "unselected-uid"),
			}, opts.WithName("AXTaskGroups")...)

			observations := krt.NewStaticCollection[AgentRuntimeObservation](nil, nil, opts.WithName("AgentRuntimeObservations")...)
			reconciliations := newAgentReconciliations(agents, v2translator.Collections{
				Harnesses: krttest.GetMockCollection[*kagentv1alpha3.Harness](mock), AgentTemplates: templates, ResolvedModelConfigs: resolvedModels,
				RemoteMCPServers: krttest.GetMockCollection[*kagentv1alpha3.RemoteMCPServer](mock),
				ConfigMaps:       configMaps, Secrets: secrets, TaskGroups: workerPools,
			}, observations, opts)
			key := "team-a/assistant"
			waitFor(t, func() bool {
				state := reconciliations.GetKey(key)
				return state != nil && state.CompilationFailure != nil
			})
			missingPool := &ReconciliationFailure{
				Condition: kagentv1alpha3.AgentConditionResolvedRefs, Reason: "TaskGroupNotFound", Message: `TaskGroup "team-a/selected" not found`,
			}
			require.Equal(t, missingPool, reconciliations.GetKey(key).CompilationFailure)
			require.Nil(t, reconciliations.GetKey(key).Target, "missing capacity must fail compilation")

			workerPools.UpdateObject(testTaskGroup("team-a", "selected", "original-uid"))
			waitFor(t, func() bool { s := reconciliations.GetKey(key); return s != nil && s.Target != nil })
			baseline := reconciliations.GetKey(key)
			observed := proto.CloneOf(baseline.Target.Runtime)
			observed.Metadata.Uid = "runtime-uid"
			observations.UpdateObject(AgentRuntimeObservation{Namespace: template.Namespace, AgentName: template.Name, RevisionID: baseline.Target.RevisionID, Template: observed})
			waitFor(t, func() bool { return reconciliations.GetKey(key).ObservedRuntime != nil })
			for _, uid := range []string{"replacement-uid", "original-uid"} {
				workerPools.UpdateObject(testTaskGroup("team-a", "selected", uid))
				waitFor(t, func() bool {
					s := reconciliations.GetKey(key)
					return s.Target != nil && s.Target.Revision.GroupRef.Uid == uid
				})
				current := reconciliations.GetKey(key)
				if uid == "replacement-uid" {
					require.NotEqual(t, baseline.Target.RevisionID, current.Target.RevisionID)
					require.Nil(t, current.ObservedRuntime)
				} else {
					require.Equal(t, baseline.Target.RevisionID, current.Target.RevisionID)
				}
				require.Equal(t, uid, current.Target.Runtime.Spec.GroupRef.Uid)
			}
			workerPools.DeleteObject("team-a/selected")
			waitFor(t, func() bool { s := reconciliations.GetKey(key); return s.Target == nil && s.CompilationFailure != nil })
			current := reconciliations.GetKey(key)
			require.Equal(t, missingPool, current.CompilationFailure)
			require.Nil(t, current.ObservedRuntime)
			pairStatus := statusForAgent(*current, 1, baseline.Target.RevisionID.String())
			require.Equal(t, baseline.Target.RevisionID.String(), pairStatus.LatestSuccessfulRevision)

		})
	}
}

func TestClaudeReconciliationCompilesActorTemplate(t *testing.T) {
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	opts := krt.NewOptionsBuilder(stop, "test-claude", nil)

	template := &kagentv1alpha3.AgentTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "assistant", UID: "template-uid", Labels: map[string]string{"runtime": "claude"}},
		Spec:       kagentv1alpha3.AgentTemplateSpec{ModelConfig: &corev1.LocalObjectReference{Name: "model"}, SystemPrompt: "help"},
	}
	claudeHarness := harness("team-a", "claude", map[string]string{"runtime": "claude"})
	claudeHarness.UID = "harness-uid"
	claudeHarness.Spec.Claude = &kagentv1alpha3.ClaudeHarness{}
	claudeHarness.Spec.Workload.Image = "example.com/claude@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	claudeHarness.Spec.AX = kagentv1alpha3.RuntimeAXPolicy{
		TaskGroupRef: corev1.LocalObjectReference{Name: "default"}, SnapshotLocationOverride: "s3://snapshots",
	}
	model := &kagentv1alpha3.ModelConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model", UID: "model-uid"}, Spec: kagentv1alpha3.ModelConfigSpec{
		Provider: kagentv1alpha3.ModelProviderAnthropic, Model: "claude-sonnet-4-5", APIKeySecret: "model-auth", APIKeySecretKey: "api-key",
	}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model-auth", UID: "secret-uid"}, Data: map[string][]byte{"api-key": []byte("secret")}}
	mock := krttest.NewMock(t, []any{
		template,
		claudeHarness,
		model,
		secret,
		testTaskGroup("team-a", "default", "default-uid"),
	})
	templates := krttest.GetMockCollection[*kagentv1alpha3.AgentTemplate](mock)
	agents := krt.NewStaticCollection(nil, []*kagentv1alpha3.Agent{testAgent(template, claudeHarness)}, opts.WithName("Agents")...)
	configMaps := krttest.GetMockCollection[*corev1.ConfigMap](mock)
	secrets := krttest.GetMockCollection[*corev1.Secret](mock)
	_, resolvedModelConfigs := newModelConfigReconciliations(
		krttest.GetMockCollection[*kagentv1alpha3.ModelConfig](mock), configMaps, secrets, opts,
	)
	reconciliations := newAgentReconciliations(
		agents, v2translator.Collections{
			Harnesses: krttest.GetMockCollection[*kagentv1alpha3.Harness](mock), AgentTemplates: templates, ResolvedModelConfigs: resolvedModelConfigs,
			RemoteMCPServers: krttest.GetMockCollection[*kagentv1alpha3.RemoteMCPServer](mock),
			ConfigMaps:       configMaps, Secrets: secrets,
			TaskGroups: krt.NewStaticCollection(nil, krttest.GetMockCollection[v2translator.TaskGroupObservation](mock).List(), opts.WithName("AXTaskGroups")...),
		}, krttest.GetMockCollection[AgentRuntimeObservation](mock), opts,
	)
	waitFor(t, func() bool {
		states := reconciliations.List()
		return len(states) == 1 && states[0].CompilationFailure == nil && states[0].Target != nil
	})
	state := reconciliations.List()[0]
	if state.Target == nil || state.Target.Revision.Environment[0].Name != "ANTHROPIC_API_KEY" || state.Target.Revision.Environment[0].Value != v2translator.CredentialPlaceholder {
		t.Fatalf("Claude revision environment = %#v", state.Target.Revision)
	}
	if state.Target.Runtime.GetSpec().GetReadiness().GetPort() != 8081 {
		t.Fatalf("Claude ActorTemplate readiness = %#v", state.Target.Runtime.GetSpec().GetReadiness())
	}
}

func TestCodexReconciliationCompilesActorTemplate(t *testing.T) {
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	opts := krt.NewOptionsBuilder(stop, "test-codex", nil)
	responses := kagentv1alpha3.OpenAIAPIFormatResponses
	template := &kagentv1alpha3.AgentTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "assistant", UID: "template-uid", Labels: map[string]string{"runtime": "codex"}},
		Spec:       kagentv1alpha3.AgentTemplateSpec{ModelConfig: &corev1.LocalObjectReference{Name: "model"}, SystemPrompt: "help"},
	}
	codexHarness := harness("team-a", "codex", map[string]string{"runtime": "codex"})
	codexHarness.UID = "harness-uid"
	codexHarness.Spec.Codex = &kagentv1alpha3.CodexHarness{}
	codexHarness.Spec.Workload.Image = "example.com/codex@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	codexHarness.Spec.AX = kagentv1alpha3.RuntimeAXPolicy{
		TaskGroupRef: corev1.LocalObjectReference{Name: "default"}, SnapshotLocationOverride: "s3://snapshots",
	}
	model := &kagentv1alpha3.ModelConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model", UID: "model-uid"}, Spec: kagentv1alpha3.ModelConfigSpec{
		Provider: kagentv1alpha3.ModelProviderOpenAI, Model: "gpt-5.2-codex", APIKeySecret: "model-auth", APIKeySecretKey: "api-key",
		OpenAI: &kagentv1alpha3.OpenAIConfig{APIFormat: &responses},
	}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model-auth", UID: "secret-uid"}, Data: map[string][]byte{"api-key": []byte("secret")}}
	mock := krttest.NewMock(t, []any{
		template,
		codexHarness,
		model,
		secret,
		testTaskGroup("team-a", "default", "default-uid"),
	})
	templates := krttest.GetMockCollection[*kagentv1alpha3.AgentTemplate](mock)
	agents := krt.NewStaticCollection(nil, []*kagentv1alpha3.Agent{testAgent(template, codexHarness)}, opts.WithName("Agents")...)
	configMaps := krttest.GetMockCollection[*corev1.ConfigMap](mock)
	secrets := krttest.GetMockCollection[*corev1.Secret](mock)
	_, resolvedModelConfigs := newModelConfigReconciliations(
		krttest.GetMockCollection[*kagentv1alpha3.ModelConfig](mock), configMaps, secrets, opts,
	)
	reconciliations := newAgentReconciliations(
		agents, v2translator.Collections{
			Harnesses: krttest.GetMockCollection[*kagentv1alpha3.Harness](mock), AgentTemplates: templates, ResolvedModelConfigs: resolvedModelConfigs,
			RemoteMCPServers: krttest.GetMockCollection[*kagentv1alpha3.RemoteMCPServer](mock),
			ConfigMaps:       configMaps, Secrets: secrets,
			TaskGroups: krt.NewStaticCollection(nil, krttest.GetMockCollection[v2translator.TaskGroupObservation](mock).List(), opts.WithName("AXTaskGroups")...),
		}, krttest.GetMockCollection[AgentRuntimeObservation](mock), opts,
	)
	waitFor(t, func() bool {
		states := reconciliations.List()
		return len(states) == 1 && states[0].CompilationFailure == nil && states[0].Target != nil
	})
	state := reconciliations.List()[0]
	if state.Target == nil || state.Target.Revision.Environment[0].Name != "OPENAI_API_KEY" || state.Target.Revision.Environment[0].Value != v2translator.CredentialPlaceholder {
		t.Fatalf("Codex revision environment = %#v", state.Target.Revision)
	}
	if state.Target.Runtime.GetSpec().GetReadiness().GetPort() != 8081 {
		t.Fatalf("Codex ActorTemplate readiness = %#v", state.Target.Runtime.GetSpec().GetReadiness())
	}
}

func TestReconciliationTracksSharedAgentTemplate(t *testing.T) {
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	opts := krt.NewOptionsBuilder(stop, "test", nil)
	child := &kagentv1alpha3.AgentTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "child", Labels: map[string]string{"runtime": "python"}},
		Spec:       kagentv1alpha3.AgentTemplateSpec{ModelConfig: &corev1.LocalObjectReference{Name: "model"}, SystemPrompt: "before"},
	}
	root := &kagentv1alpha3.AgentTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "root", Labels: map[string]string{"runtime": "python"}},
		Spec: kagentv1alpha3.AgentTemplateSpec{
			ModelConfig: &corev1.LocalObjectReference{Name: "model"},
			Tools: []kagentv1alpha3.ToolBinding{{SubAgent: &kagentv1alpha3.SubAgentToolBinding{
				Name: "child", Description: "delegate", TemplateRef: &corev1.LocalObjectReference{Name: child.Name},
			}}},
		},
	}
	harness := harness("team-a", "kagent", map[string]string{"runtime": "python"})
	harness.Spec.Kagent = &kagentv1alpha3.KagentHarness{}
	harness.Spec.Workload.Image = "example.com/kagent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	harness.Spec.AX = kagentv1alpha3.RuntimeAXPolicy{TaskGroupRef: corev1.LocalObjectReference{Name: "default"}, SnapshotLocationOverride: "s3://snapshots"}
	templates := krt.NewStaticCollection(nil, []*kagentv1alpha3.AgentTemplate{root, child}, opts.WithName("AgentTemplates")...)
	mock := krttest.NewMock(t, []any{
		harness,
		&kagentv1alpha3.ModelConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model"}, Spec: kagentv1alpha3.ModelConfigSpec{Provider: kagentv1alpha3.ModelProviderOpenAI, Model: "gpt-5"}},
		testTaskGroup("team-a", "default", "default-uid"),
	})
	agents := krt.NewStaticCollection(nil, []*kagentv1alpha3.Agent{testAgent(root, harness)}, opts.WithName("Agents")...)
	modelConfigs := krttest.GetMockCollection[*kagentv1alpha3.ModelConfig](mock)
	configMaps := krttest.GetMockCollection[*corev1.ConfigMap](mock)
	secrets := krttest.GetMockCollection[*corev1.Secret](mock)
	_, resolvedModelConfigs := newModelConfigReconciliations(modelConfigs, configMaps, secrets, opts)
	reconciliations := newAgentReconciliations(
		agents, v2translator.Collections{
			Harnesses: krttest.GetMockCollection[*kagentv1alpha3.Harness](mock), AgentTemplates: templates, ResolvedModelConfigs: resolvedModelConfigs,
			RemoteMCPServers: krttest.GetMockCollection[*kagentv1alpha3.RemoteMCPServer](mock),
			ConfigMaps:       configMaps, Secrets: secrets,
			TaskGroups: krt.NewStaticCollection(nil, krttest.GetMockCollection[v2translator.TaskGroupObservation](mock).List(), opts.WithName("AXTaskGroups")...),
		}, krttest.GetMockCollection[AgentRuntimeObservation](mock), opts,
	)
	var initial string
	waitFor(t, func() bool {
		for _, state := range reconciliations.List() {
			if state.Agent.Name == root.Name && state.CompilationFailure == nil {
				initial = state.Target.RevisionID.String()
				return true
			}
		}
		return false
	})
	updated := child.DeepCopy()
	updated.Spec.SystemPrompt = "after"
	templates.UpdateObject(updated)
	waitFor(t, func() bool {
		for _, state := range reconciliations.List() {
			if state.Agent.Name == root.Name {
				return state.CompilationFailure == nil && state.Target.RevisionID.String() != initial
			}
		}
		return false
	})
}

func harness(namespace, name string, matchLabels map[string]string) *kagentv1alpha3.Harness {
	return &kagentv1alpha3.Harness{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       kagentv1alpha3.HarnessSpec{},
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func testAgent(template *kagentv1alpha3.AgentTemplate, harness *kagentv1alpha3.Harness) *kagentv1alpha3.Agent {
	return &kagentv1alpha3.Agent{ObjectMeta: metav1.ObjectMeta{Namespace: template.Namespace, Name: template.Name, UID: template.UID}, Spec: kagentv1alpha3.AgentSpec{
		TemplateRef: &corev1.LocalObjectReference{Name: template.Name}, HarnessRef: &corev1.LocalObjectReference{Name: harness.Name},
	}}
}

func testTaskGroup(space, name, uid string) v2translator.TaskGroupObservation {
	return v2translator.TaskGroupObservation{Group: &ax.TaskGroup{Metadata: &ax.ObjectMeta{Atespace: space, Name: name, Uid: uid}, Spec: &ax.TaskGroupSpec{Replicas: new(int32(1)), SandboxClass: "gvisor", SnapshotLocation: "s3://snapshots"}}}
}
