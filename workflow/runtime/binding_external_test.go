package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/runtimetest"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestBindRunRequiredUnknownDefaultsCoercionAndProvenance(t *testing.T) {
	plan := bindingPlan([]graph.InputSpec{
		{Name: "name", Required: true, Schema: graph.Schema{"type": "string"}, Source: bindingSource("inputs", "name", 5), Metadata: graph.Metadata{
			"redaction": "public", "retention": "external", "media_type": "text/plain",
		}},
		{Name: "enabled", Schema: graph.Schema{"type": "boolean"}, Default: &graph.Binding{
			Kind: graph.BindingLiteral, Literal: true, Source: bindingSource("inputs", "enabled", "default", 11),
		}, Source: bindingSource("inputs", "enabled", 8)},
		{Name: "large", Schema: graph.Schema{"type": "integer"}, Source: bindingSource("inputs", "large", 13)},
	}, nil, nil)
	base := runtimetest.NewStore()
	observed := &observedStateStore{StateStore: base}
	request := workflowruntime.BindRunRequest{
		ID: "run-bind", Plan: plan, Inputs: map[string]any{"unknown": true},
		CreatedAt: bindingTime(),
	}
	first, err := workflowruntime.BindRun(t.Context(), observed, request)
	if err != nil {
		t.Fatalf("BindRun invalid inputs returned operational error: %v", err)
	}
	assertBindingCodes(t, first.Diagnostics, workflowruntime.CodeUnknownWorkflowInput, workflowruntime.CodeMissingWorkflowInput)
	if first.Run != nil || observed.saveCalls != 0 {
		t.Fatalf("invalid binding persisted or returned run: %#v saves=%d", first.Run, observed.saveCalls)
	}
	for range 30 {
		repeated, repeatErr := workflowruntime.BindRun(t.Context(), observed, request)
		if repeatErr != nil || !reflect.DeepEqual(repeated.Diagnostics, first.Diagnostics) {
			t.Fatalf("binding diagnostics changed: %#v, %v", repeated.Diagnostics, repeatErr)
		}
	}

	request.Inputs = map[string]any{
		"name": "release", "large": ^uint64(0),
	}
	result, err := workflowruntime.BindRun(t.Context(), observed, request)
	if err != nil || result.Run == nil || len(result.Diagnostics) != 0 {
		t.Fatalf("BindRun valid = %#v, %v", result, err)
	}
	if validationErr := result.Run.Validate(); validationErr != nil {
		t.Fatalf("BoundRun.Validate: %v", validationErr)
	}
	if result.Run.Plan.ID != plan.ID || result.Run.Plan.Digest != plan.Digest ||
		result.Run.Plan.SchemaVersion != workflowcompile.ExecutionPlanSchemaVersion ||
		result.Run.Plan.Version != plan.Graph.Version ||
		!reflect.DeepEqual(result.Run.Provenance, plan.Provenance) {
		t.Fatalf("bound plan/provenance = %#v", result.Run)
	}
	persisted, err := base.LoadValues(t.Context(), result.Run.InputsRef)
	if err != nil {
		t.Fatalf("LoadValues: %v", err)
	}
	if persisted["large"].Inline != json.Number("18446744073709551615") {
		t.Fatalf("large integer lost precision: %#v", persisted["large"].Inline)
	}
	if enabled := persisted["enabled"]; enabled.Inline != true || enabled.Producer.Kind != "workflow_default" ||
		enabled.Redaction != values.RedactionPrivate || enabled.Retention != values.RetentionRun || enabled.MediaType != "application/json" {
		t.Fatalf("default envelope = %#v", enabled)
	}
	if name := persisted["name"]; name.Producer.Kind != "workflow_input" || name.Producer.Reference != "run-bind" || name.Producer.Output != "name" ||
		name.Redaction != values.RedactionPrivate || name.Retention != values.RetentionRun || name.MediaType != "application/json" {
		t.Fatalf("caller envelope/descriptive metadata isolation = %#v", name)
	}
	plan.Provenance.Metadata["channel"] = "mutated"
	if result.Run.Provenance.Metadata["channel"] != "stable" {
		t.Fatalf("bound provenance aliases plan: %#v", result.Run.Provenance)
	}
}

func TestBindRunRejectsNonLiteralDefaultsLossyCoercionAndSchemaFailures(t *testing.T) {
	tests := []struct {
		name  string
		spec  graph.InputSpec
		input map[string]any
		code  diagnostic.Code
	}{
		{
			name: "expression default",
			spec: graph.InputSpec{Name: "count", Schema: graph.Schema{"type": "integer"}, Default: &graph.Binding{
				Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "1", Source: bindingSource("inputs", "count", "default", 7)},
			}},
			code: workflowruntime.CodeInvalidWorkflowDefault,
		},
		{
			name:  "numeric string is not parsed",
			spec:  graph.InputSpec{Name: "count", Required: true, Schema: graph.Schema{"type": "integer"}, Source: bindingSource("inputs", "count", 4)},
			input: map[string]any{"count": "9007199254740993"}, code: workflowruntime.CodeWorkflowInputSchema,
		},
		{
			name:  "object string is not parsed",
			spec:  graph.InputSpec{Name: "payload", Required: true, Schema: graph.Schema{"type": "object"}, Source: bindingSource("inputs", "payload", 4)},
			input: map[string]any{"payload": `{"ready":true}`}, code: workflowruntime.CodeWorkflowInputSchema,
		},
		{
			name:  "fractional number is not integer",
			spec:  graph.InputSpec{Name: "count", Required: true, Schema: graph.Schema{"type": "integer"}, Source: bindingSource("inputs", "count", 4)},
			input: map[string]any{"count": 1.5}, code: workflowruntime.CodeWorkflowInputSchema,
		},
		{
			name:  "unsafe floating point integer is rejected before schema",
			spec:  graph.InputSpec{Name: "count", Required: true, Schema: graph.Schema{"type": "integer"}, Source: bindingSource("inputs", "count", 4)},
			input: map[string]any{"count": float64(9007199254740992)}, code: workflowruntime.CodeInvalidWorkflowInput,
		},
		{
			name:  "binary is not inline JSON",
			spec:  graph.InputSpec{Name: "payload", Required: true, Schema: graph.Schema{}, Source: bindingSource("inputs", "payload", 4)},
			input: map[string]any{"payload": []byte("bytes")}, code: workflowruntime.CodeInvalidWorkflowInput,
		},
		{
			name:  "invalid schema",
			spec:  graph.InputSpec{Name: "payload", Required: true, Schema: graph.Schema{"type": "not-a-json-type"}, Source: bindingSource("inputs", "payload", 4)},
			input: map[string]any{"payload": "value"}, code: workflowruntime.CodeWorkflowInputSchema,
		},
		{
			name: "invalid schema is rejected for absent optional input",
			spec: graph.InputSpec{Name: "payload", Schema: graph.Schema{"type": "not-a-json-type"}, Source: bindingSource("inputs", "payload", 4)},
			code: workflowruntime.CodeWorkflowInputSchema,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &observedStateStore{StateStore: runtimetest.NewStore()}
			result, err := workflowruntime.BindRun(t.Context(), store, workflowruntime.BindRunRequest{
				ID: "run-invalid", Plan: bindingPlan([]graph.InputSpec{test.spec}, nil, nil), Inputs: test.input, CreatedAt: bindingTime(),
			})
			if err != nil {
				t.Fatalf("BindRun: %v", err)
			}
			assertBindingCodes(t, result.Diagnostics, test.code)
			if result.Run != nil || store.saveCalls != 0 {
				t.Fatalf("invalid binding persisted: %#v saves=%d", result.Run, store.saveCalls)
			}
			if result.Diagnostics[0].Source == nil || result.Diagnostics[0].Source.Locator != "binding.workflow.yaml" {
				t.Fatalf("diagnostic source = %#v", result.Diagnostics[0].Source)
			}
		})
	}
}

func TestBindRunAcceptsAndPreservesPreEnvelopedAndArtifactInputs(t *testing.T) {
	plan := bindingPlan([]graph.InputSpec{
		{Name: "secret", Required: true, Schema: graph.Schema{"type": "secret_ref"}, Source: bindingSource("inputs", "secret", 4)},
		{Name: "typed-ref", Required: true, Schema: graph.Schema{"type": "secret_ref"}, Source: bindingSource("inputs", "typed-ref", 6)},
		{Name: "report", Required: true, Schema: graph.Schema{"type": "artifact"}, Source: bindingSource("inputs", "report", 8)},
	}, nil, nil)
	secret := bindingSecretRef(t, "secret://project/activation#token", "activation/secret", values.RetentionProject)
	typedRef, err := values.ParseSecretRef("secret://project/activation#secondary")
	if err != nil {
		t.Fatal(err)
	}
	artifactValue := bindingArtifact(t)
	artifactRef := *artifactValue.Artifact
	store := runtimetest.NewStore()
	result, err := workflowruntime.BindRun(t.Context(), store, workflowruntime.BindRunRequest{
		ID: "run-artifact-input", Plan: plan,
		Inputs: map[string]any{"secret": secret, "typed-ref": typedRef, "report": artifactRef}, CreatedAt: bindingTime(),
	})
	if err != nil || result.Run == nil || len(result.Diagnostics) != 0 {
		t.Fatalf("BindRun artifact inputs = %#v, %v", result, err)
	}
	persisted, err := store.LoadValues(t.Context(), result.Run.InputsRef)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(persisted["secret"], secret) || !reflect.DeepEqual(persisted["report"], artifactValue) {
		t.Fatalf("pre-enveloped inputs changed: %#v", persisted)
	}
	if typed := persisted["typed-ref"]; typed.Type != values.TypeSecretRef || typed.SecretRef == nil || *typed.SecretRef != typedRef ||
		typed.Redaction != values.RedactionSecret || typed.Retention != values.RetentionRun ||
		typed.MediaType != "application/json" ||
		typed.Producer.Kind != "workflow_input" || typed.Producer.Reference != "run-artifact-input" || typed.Producer.Output != "typed-ref" {
		t.Fatalf("typed SecretRef input = %#v", typed)
	}

	invalid := secret
	invalid.Digest = values.SHA256Digest([]byte("wrong"))
	observed := &observedStateStore{StateStore: runtimetest.NewStore()}
	rejected, err := workflowruntime.BindRun(t.Context(), observed, workflowruntime.BindRunRequest{
		ID: "run-invalid-envelope", Plan: plan,
		Inputs: map[string]any{"secret": invalid, "typed-ref": typedRef, "report": artifactRef}, CreatedAt: bindingTime(),
	})
	if err != nil {
		t.Fatalf("invalid envelope returned operational error: %v", err)
	}
	assertBindingCodes(t, rejected.Diagnostics, workflowruntime.CodeInvalidWorkflowInput)
	if observed.saveCalls != 0 {
		t.Fatalf("invalid pre-enveloped input was persisted")
	}
}

func TestBindRunPersistenceFailureAndStartStoreOwnedReplayConflict(t *testing.T) {
	plan := bindingPlan([]graph.InputSpec{{Name: "name", Required: true, Schema: graph.Schema{"type": "string"}}}, nil, nil)
	base := runtimetest.NewStore()
	failing := &observedStateStore{StateStore: base, saveErr: errors.New("value store unavailable")}
	request := workflowruntime.BindRunRequest{ID: "run-start", Plan: plan, Inputs: map[string]any{"name": "release"}, CreatedAt: bindingTime()}
	if _, err := workflowruntime.BindRun(t.Context(), failing, request); err == nil {
		t.Fatal("BindRun accepted SaveValues failure")
	}
	if _, err := base.LoadRun(t.Context(), request.ID); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("failed binding was mistaken for started run: %v", err)
	}

	boundResult, err := workflowruntime.BindRun(t.Context(), base, request)
	if err != nil || boundResult.Run == nil {
		t.Fatalf("BindRun: %#v, %v", boundResult, err)
	}
	created, outcome, err := workflowruntime.StartBoundRun(t.Context(), base, *boundResult.Run, "start-key")
	if err != nil || outcome != workflowruntime.IdempotencyApplied || created.Inputs == nil || *created.Inputs != boundResult.Run.InputsRef {
		t.Fatalf("StartBoundRun = %#v, %q, %v", created, outcome, err)
	}
	replayed, outcome, err := workflowruntime.StartBoundRun(t.Context(), base, *boundResult.Run, "start-key")
	if err != nil || outcome != workflowruntime.IdempotencyReplayed || replayed.Generation != created.Generation {
		t.Fatalf("StartBoundRun replay = %#v, %q, %v", replayed, outcome, err)
	}
	conflicting := *boundResult.Run
	differentValue := bindingInline(t, "different", "caller", values.RedactionPrivate, values.RetentionRun)
	differentRef, saveErr := base.SaveValues(t.Context(), workflowruntime.SaveValuesRequest{
		Owner:  workflowruntime.ValueOwner{Kind: "run-inputs", RunID: conflicting.ID},
		Values: values.ValueSet{"name": differentValue},
	})
	if saveErr != nil {
		t.Fatalf("SaveValues conflicting input: %v", saveErr)
	}
	conflicting.InputsRef = differentRef
	if _, _, err := workflowruntime.StartBoundRun(t.Context(), base, conflicting, "start-key"); !errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("StartBoundRun conflict = %v", err)
	}
}

func TestBoundRunValidateRejectsIncompleteEnvelope(t *testing.T) {
	valid := workflowruntime.BoundRun{
		ID: "run-valid",
		Plan: workflowruntime.PlanRef{
			ID: "binding-workflow", Version: "1.0.0",
			Digest: values.SHA256Digest([]byte("plan")), SchemaVersion: workflowcompile.ExecutionPlanSchemaVersion,
		},
		InputsRef: values.ValueSetRef{ID: "values-1", Digest: values.SHA256Digest([]byte("inputs"))},
		CreatedAt: bindingTime(), Provenance: graph.Provenance{Origin: "workflow-source"},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid BoundRun: %v", err)
	}
	tests := []struct {
		name string
		edit func(*workflowruntime.BoundRun)
	}{
		{"missing run id", func(run *workflowruntime.BoundRun) { run.ID = "" }},
		{"invalid plan", func(run *workflowruntime.BoundRun) { run.Plan.Digest = "bad" }},
		{"invalid input ref", func(run *workflowruntime.BoundRun) { run.InputsRef.ID = "" }},
		{"missing creation time", func(run *workflowruntime.BoundRun) { run.CreatedAt = time.Time{} }},
		{"non JSON provenance", func(run *workflowruntime.BoundRun) { run.Provenance.Metadata = graph.Metadata{"bad": make(chan int)} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			test.edit(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatalf("invalid BoundRun accepted: %#v", candidate)
			}
		})
	}
}

func TestFinalizeRunOutputsPreservesPassthroughAndPublishesCompleteSet(t *testing.T) {
	outputs := []graph.OutputSpec{
		bindingOutput("secret", graph.Schema{"type": "secret_ref"}, "steps.render.outputs.secret", 20),
		bindingOutput("report", graph.Schema{"type": "artifact"}, "steps.render.outputs.report", 24),
		bindingOutput("count", graph.Schema{"type": "integer"}, "steps.render.outputs.count + 1", 28),
	}
	outputs[2].Metadata = graph.Metadata{"redaction": "public", "retention": "external", "media_type": "text/plain"}
	plan := bindingPlan(nil, outputs, []graph.Node{{ID: "render", Kind: "test", Source: bindingSource("steps", "render", 12)}})
	store := runtimetest.NewStore()
	bound, running := startedBindingRun(t, store, plan, "run-output")
	secret := bindingSecretRef(t, "secret://project/render#token", "node-secret", values.RetentionProject)
	artifact := bindingArtifact(t)
	count := bindingInline(t, json.Number("2"), "node-count", values.RedactionPrivate, values.RetentionRun)
	expressionContext := values.ExpressionContext{Steps: map[string]values.StepContext{
		"render": {Status: string(workflowruntime.NodeSucceeded), Outputs: values.ValueSet{
			"secret": secret, "report": artifact, "count": count,
		}},
		"unrelated": {Status: string(workflowruntime.NodeSucceeded), Outputs: values.ValueSet{}},
	}}
	result, err := workflowruntime.FinalizeRunOutputs(t.Context(), store, workflowruntime.FinalizeRunRequest{
		BoundRun: bound, Run: running, Plan: plan, Context: expressionContext,
		At: bindingTime().Add(2 * time.Minute),
	})
	if err != nil || len(result.Diagnostics) != 0 || result.Outcome != workflowruntime.OutputFinalizationApplied || result.Run.Status != workflowruntime.RunSucceeded {
		t.Fatalf("FinalizeRunOutputs = %#v, %v", result, err)
	}
	if !reflect.DeepEqual(result.Outputs["secret"], secret) || !reflect.DeepEqual(result.Outputs["report"], artifact) {
		t.Fatalf("direct output envelopes changed: %#v", result.Outputs)
	}
	computed := result.Outputs["count"]
	if computed.Inline != json.Number("3") || computed.Producer.Kind != "workflow_output" ||
		computed.Redaction != values.RedactionPrivate || computed.Retention != values.RetentionRun {
		t.Fatalf("computed output = %#v", computed)
	}
	persisted, err := store.LoadValues(t.Context(), result.OutputsRef)
	if err != nil || !reflect.DeepEqual(persisted, result.Outputs) || result.Run.Outputs == nil || *result.Run.Outputs != result.OutputsRef {
		t.Fatalf("published outputs = %#v loaded=%#v err=%v", result.Run.Outputs, persisted, err)
	}
}

func TestFinalizeRunOutputsDiagnosticsDoNotPersistPartialOutputs(t *testing.T) {
	tests := []struct {
		name    string
		outputs []graph.OutputSpec
		context values.ExpressionContext
		code    diagnostic.Code
	}{
		{
			name:    "failed expression",
			outputs: []graph.OutputSpec{bindingOutput("result", graph.Schema{"type": "string"}, "steps.render.outputs.missing", 20)},
			context: completedRenderContext(values.ValueSet{}), code: values.CodeExpressionUnresolved,
		},
		{
			name:    "invisible unrelated step",
			outputs: []graph.OutputSpec{bindingOutput("result", graph.Schema{"type": "string"}, "steps.secret.outputs.value", 20)},
			context: values.ExpressionContext{Steps: map[string]values.StepContext{
				"render": {Status: string(workflowruntime.NodeSucceeded), Outputs: values.ValueSet{}},
				"secret": {Status: string(workflowruntime.NodeSucceeded), Outputs: values.ValueSet{"value": bindingInline(t, "hidden", "secret", values.RedactionPrivate, values.RetentionProject)}},
			}},
			code: values.CodeExpressionInvisibleStep,
		},
		{
			name:    "wrong schema",
			outputs: []graph.OutputSpec{bindingOutput("result", graph.Schema{"type": "integer"}, `"text"`, 20)},
			context: completedRenderContext(values.ValueSet{}), code: workflowruntime.CodeWorkflowOutputSchema,
		},
		{
			name:    "artifact cannot masquerade as object",
			outputs: []graph.OutputSpec{bindingOutput("result", graph.Schema{"type": "object"}, "steps.render.outputs.report", 20)},
			context: completedRenderContext(values.ValueSet{"report": bindingArtifact(t)}), code: workflowruntime.CodeWorkflowOutputSchema,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := bindingPlan(nil, append([]graph.OutputSpec{
				bindingOutput("first", graph.Schema{"type": "string"}, `"valid"`, 18),
			}, test.outputs...), []graph.Node{{ID: "render", Kind: "test", Source: bindingSource("steps", "render", 12)}})
			base := runtimetest.NewStore()
			bound, running := startedBindingRun(t, base, plan, "run-failed-output")
			observed := &observedStateStore{StateStore: base}
			result, err := workflowruntime.FinalizeRunOutputs(t.Context(), observed, workflowruntime.FinalizeRunRequest{
				BoundRun: bound, Run: running, Plan: plan, Context: test.context,
				At: bindingTime().Add(2 * time.Minute),
			})
			if err != nil {
				t.Fatalf("FinalizeRunOutputs returned operational error: %v", err)
			}
			assertBindingCodes(t, result.Diagnostics, test.code)
			if observed.saveCalls != 0 || observed.transitionCalls != 0 {
				t.Fatalf("failed output published partial data: saves=%d transitions=%d", observed.saveCalls, observed.transitionCalls)
			}
			if result.Diagnostics[0].Source == nil || result.Diagnostics[0].Source.StartLine != 20 {
				t.Fatalf("output diagnostic source = %#v", result.Diagnostics[0].Source)
			}
		})
	}
}

func TestFinalizeRunOutputsRequiresCompleteCanonicalGraphAndPreservesBasePolicy(t *testing.T) {
	plan := bindingPlan(nil, []graph.OutputSpec{
		bindingOutput("env-value", graph.Schema{"type": "string"}, "env.release", 20),
	}, []graph.Node{
		{ID: "prepare", Kind: "test", Source: bindingSource("steps", "prepare", 10)},
		{ID: "finish", Kind: "test", Source: bindingSource("steps", "finish", 14)},
	})
	base := runtimetest.NewStore()
	bound, running := startedBindingRun(t, base, plan, "run-completion")
	observed := &observedStateStore{StateStore: base}
	incomplete := values.ExpressionContext{Steps: map[string]values.StepContext{
		"prepare": {Status: string(workflowruntime.NodeSucceeded), Outputs: values.ValueSet{}},
	}, Env: values.ValueSet{"release": bindingInline(t, "v1", "env", values.RedactionPrivate, values.RetentionRun)}}
	result, err := workflowruntime.FinalizeRunOutputs(t.Context(), observed, workflowruntime.FinalizeRunRequest{
		BoundRun: bound, Run: running, Plan: plan, Context: incomplete,
		BaseOptions: values.ExpressionOptions{AllowEnv: true}, At: bindingTime().Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	assertBindingCodes(t, result.Diagnostics, workflowruntime.CodeGraphNotComplete)
	if observed.saveCalls != 0 {
		t.Fatalf("incomplete graph saved outputs")
	}

	incomplete.Steps["finish"] = values.StepContext{Status: "completed", Outputs: values.ValueSet{}}
	result, err = workflowruntime.FinalizeRunOutputs(t.Context(), observed, workflowruntime.FinalizeRunRequest{
		BoundRun: bound, Run: running, Plan: plan, Context: incomplete,
		BaseOptions: values.ExpressionOptions{AllowEnv: true}, At: bindingTime().Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	assertBindingCodes(t, result.Diagnostics, workflowruntime.CodeGraphNotComplete)

	incomplete.Steps["finish"] = values.StepContext{Status: string(workflowruntime.NodeSkipped), Outputs: values.ValueSet{}}
	result, err = workflowruntime.FinalizeRunOutputs(t.Context(), observed, workflowruntime.FinalizeRunRequest{
		BoundRun: bound, Run: running, Plan: plan, Context: incomplete,
		BaseOptions: values.ExpressionOptions{AllowEnv: true}, At: bindingTime().Add(2 * time.Minute),
	})
	if err != nil || len(result.Diagnostics) != 0 || result.Outputs["env-value"].Inline != "v1" {
		t.Fatalf("complete output with preserved AllowEnv = %#v, %v", result, err)
	}
}

func TestFinalizeRunOutputsRejectsBoundPlanProvenanceMismatch(t *testing.T) {
	plan := bindingPlan(nil, nil, []graph.Node{{ID: "render", Kind: "test", Source: bindingSource("steps", "render", 12)}})
	base := runtimetest.NewStore()
	bound, running := startedBindingRun(t, base, plan, "run-identity")
	bound.Provenance.Revision = "different"
	observed := &observedStateStore{StateStore: base}
	result, err := workflowruntime.FinalizeRunOutputs(t.Context(), observed, workflowruntime.FinalizeRunRequest{
		BoundRun: bound, Run: running, Plan: plan,
		Context: completedRenderContext(values.ValueSet{}), At: bindingTime().Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatalf("FinalizeRunOutputs returned operational error: %v", err)
	}
	assertBindingCodes(t, result.Diagnostics, workflowruntime.CodeInvalidRunBinding)
	if observed.saveCalls != 0 || observed.transitionCalls != 0 {
		t.Fatalf("identity mismatch mutated store: saves=%d transitions=%d", observed.saveCalls, observed.transitionCalls)
	}
}

func TestFinalizeRunOutputsPersistenceBoundariesAndReplay(t *testing.T) {
	plan := bindingPlan(nil, []graph.OutputSpec{
		bindingOutput("result", graph.Schema{"type": "string"}, "steps.render.outputs.value", 20),
	}, []graph.Node{{ID: "render", Kind: "test", Source: bindingSource("steps", "render", 12)}})
	contextValue := func(value string) values.ExpressionContext {
		return completedRenderContext(values.ValueSet{
			"value": bindingInline(t, value, "node-value", values.RedactionPrivate, values.RetentionRun),
		})
	}

	t.Run("save failure publishes nothing", func(t *testing.T) {
		base := runtimetest.NewStore()
		bound, running := startedBindingRun(t, base, plan, "run-save-failure")
		observed := &observedStateStore{StateStore: base, saveErr: errors.New("save failed")}
		_, err := workflowruntime.FinalizeRunOutputs(t.Context(), observed, workflowruntime.FinalizeRunRequest{
			BoundRun: bound, Run: running, Plan: plan, Context: contextValue("one"), At: bindingTime().Add(2 * time.Minute),
		})
		if err == nil || observed.transitionCalls != 0 {
			t.Fatalf("save failure = %v transitions=%d", err, observed.transitionCalls)
		}
		loaded, loadErr := base.LoadRun(t.Context(), running.ID)
		if loadErr != nil || loaded.Status != workflowruntime.RunRunning || loaded.Outputs != nil {
			t.Fatalf("save failure published outputs: %#v, %v", loaded, loadErr)
		}
	})

	t.Run("transition failure leaves no published ref", func(t *testing.T) {
		base := runtimetest.NewStore()
		bound, running := startedBindingRun(t, base, plan, "run-transition-failure")
		observed := &observedStateStore{StateStore: base, transitionErr: errors.New("transition failed")}
		_, err := workflowruntime.FinalizeRunOutputs(t.Context(), observed, workflowruntime.FinalizeRunRequest{
			BoundRun: bound, Run: running, Plan: plan, Context: contextValue("one"), At: bindingTime().Add(2 * time.Minute),
		})
		if err == nil || observed.saveCalls != 1 || observed.transitionCalls != 1 {
			t.Fatalf("transition failure = %v saves=%d transitions=%d", err, observed.saveCalls, observed.transitionCalls)
		}
		loaded, loadErr := base.LoadRun(t.Context(), running.ID)
		if loadErr != nil || loaded.Status != workflowruntime.RunRunning || loaded.Outputs != nil {
			t.Fatalf("transition failure published outputs: %#v, %v", loaded, loadErr)
		}
	})

	t.Run("exact replay reads before write and conflict is store-neutral", func(t *testing.T) {
		base := runtimetest.NewStore()
		bound, running := startedBindingRun(t, base, plan, "run-replay")
		applied, err := workflowruntime.FinalizeRunOutputs(t.Context(), base, workflowruntime.FinalizeRunRequest{
			BoundRun: bound, Run: running, Plan: plan, Context: contextValue("one"), At: bindingTime().Add(2 * time.Minute),
		})
		if err != nil {
			t.Fatal(err)
		}
		observed := &observedStateStore{StateStore: base}
		replay, err := workflowruntime.FinalizeRunOutputs(t.Context(), observed, workflowruntime.FinalizeRunRequest{
			BoundRun: bound, Run: applied.Run, Plan: plan, Context: contextValue("one"), At: bindingTime().Add(3 * time.Minute),
		})
		if err != nil || replay.Outcome != workflowruntime.OutputFinalizationReplayed || observed.saveCalls != 0 || observed.transitionCalls != 0 {
			t.Fatalf("replay = %#v, %v saves=%d transitions=%d", replay, err, observed.saveCalls, observed.transitionCalls)
		}
		_, err = workflowruntime.FinalizeRunOutputs(t.Context(), observed, workflowruntime.FinalizeRunRequest{
			BoundRun: bound, Run: applied.Run, Plan: plan, Context: contextValue("different"), At: bindingTime().Add(3 * time.Minute),
		})
		if !errors.Is(err, workflowruntime.ErrOutputConflict) || observed.saveCalls != 0 || observed.transitionCalls != 0 {
			t.Fatalf("conflicting replay = %v saves=%d transitions=%d", err, observed.saveCalls, observed.transitionCalls)
		}
	})
}

type observedStateStore struct {
	workflowruntime.StateStore
	saveCalls       int
	transitionCalls int
	saveErr         error
	transitionErr   error
}

func (s *observedStateStore) SaveValues(ctx context.Context, request workflowruntime.SaveValuesRequest) (values.ValueSetRef, error) {
	s.saveCalls++
	if s.saveErr != nil {
		return values.ValueSetRef{}, s.saveErr
	}
	return s.StateStore.SaveValues(ctx, request)
}

func (s *observedStateStore) TransitionRun(ctx context.Context, request workflowruntime.RunTransitionRequest) (workflowruntime.RunTransitionResult, error) {
	s.transitionCalls++
	if s.transitionErr != nil {
		return workflowruntime.RunTransitionResult{}, s.transitionErr
	}
	return s.StateStore.TransitionRun(ctx, request)
}

func bindingPlan(inputs []graph.InputSpec, outputs []graph.OutputSpec, nodes []graph.Node) *workflowcompile.ExecutionPlan {
	root := bindingSource("workflow", 1)
	return &workflowcompile.ExecutionPlan{
		SchemaVersion: workflowcompile.ExecutionPlanSchemaVersion,
		ID:            "binding-workflow", Digest: values.SHA256Digest([]byte("binding-plan")),
		Definition: graph.DefinitionRef{Authority: "project", Kind: "graph", ID: "binding-workflow", Version: "1.2.3", Digest: values.SHA256Digest([]byte("definition"))},
		Provenance: graph.Provenance{
			Authority: "project", Origin: "workflow-source", Locator: "binding.workflow.yaml",
			Digest: values.SHA256Digest([]byte("source")), Metadata: graph.Metadata{"channel": "stable"},
		},
		Graph: graph.Graph{
			ID: "binding-workflow", Version: "1.2.3", Digest: values.SHA256Digest([]byte("graph")),
			Inputs: inputs, Outputs: outputs, Nodes: nodes, Source: root,
		},
		SourceMap: graph.SourceMap{Graph: root},
	}
}

func bindingOutput(name string, schema graph.Schema, expression string, line int) graph.OutputSpec {
	source := bindingSource("outputs", name, line)
	return graph.OutputSpec{
		Name: name, Schema: schema, Source: source,
		Value: &graph.Binding{
			Kind: graph.BindingExpression, Source: source,
			Expression: &graph.Expression{Text: expression, Source: source},
		},
	}
}

func startedBindingRun(t *testing.T, store workflowruntime.StateStore, plan *workflowcompile.ExecutionPlan, id workflowruntime.RunID) (workflowruntime.BoundRun, workflowruntime.RunSnapshot) {
	t.Helper()
	boundResult, err := workflowruntime.BindRun(t.Context(), store, workflowruntime.BindRunRequest{ID: id, Plan: plan, CreatedAt: bindingTime()})
	if err != nil || boundResult.Run == nil || len(boundResult.Diagnostics) != 0 {
		t.Fatalf("BindRun = %#v, %v", boundResult, err)
	}
	pending, _, err := workflowruntime.StartBoundRun(t.Context(), store, *boundResult.Run, "start-"+string(id))
	if err != nil {
		t.Fatalf("StartBoundRun: %v", err)
	}
	transition, err := store.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{
		RunID: id, ExpectedGeneration: pending.Generation, To: workflowruntime.RunRunning,
		At: bindingTime().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("TransitionRun(running): %v", err)
	}
	return *boundResult.Run, transition.Snapshot
}

func completedRenderContext(outputs values.ValueSet) values.ExpressionContext {
	return values.ExpressionContext{Steps: map[string]values.StepContext{
		"render": {Status: string(workflowruntime.NodeSucceeded), Outputs: outputs},
	}}
}

func bindingInline(t *testing.T, inline any, reference string, redaction values.RedactionClass, retention values.RetentionClass) values.Value {
	t.Helper()
	value, err := values.NewInline(inline, values.Metadata{
		Producer:  values.Producer{Kind: "node_output", Reference: reference, Output: "value"},
		MediaType: "application/json", Redaction: redaction, Retention: retention,
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func bindingArtifact(t *testing.T) values.Value {
	t.Helper()
	value, err := values.NewArtifact(values.ArtifactRef{
		Store: "external", URI: "artifact://run/report.pdf",
		Digest: values.SHA256Digest([]byte("report")), MediaType: "application/pdf", SizeBytes: 6,
		Producer:  values.Producer{Kind: "node_output", Reference: "run/render", Output: "report"},
		Redaction: values.RedactionPrivate, Retention: values.RetentionExternal,
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func bindingSecretRef(t *testing.T, raw, reference string, retention values.RetentionClass) values.Value {
	t.Helper()
	ref, err := values.ParseSecretRef(raw)
	if err != nil {
		t.Fatal(err)
	}
	value, err := values.NewSecretRef(ref, values.Metadata{
		Producer:  values.Producer{Kind: "node_output", Reference: reference, Output: "value"},
		MediaType: "application/json", Redaction: values.RedactionSecret, Retention: retention,
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func bindingSource(path ...any) *graph.SourceRef {
	parts := make([]string, 0, len(path))
	line := 1
	for _, part := range path {
		switch value := part.(type) {
		case string:
			parts = append(parts, value)
		case int:
			line = value
		}
	}
	return &graph.SourceRef{
		Format: graph.SourceWorkflow, Locator: "binding.workflow.yaml",
		StartLine: line, StartColumn: 3, Path: parts,
	}
}

func bindingTime() time.Time {
	return time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
}

func assertBindingCodes(t *testing.T, findings []diagnostic.Diagnostic, want ...diagnostic.Code) {
	t.Helper()
	if len(findings) != len(want) {
		t.Fatalf("diagnostics = %#v, want codes %v", findings, want)
	}
	for index, code := range want {
		if findings[index].Code != code {
			t.Fatalf("diagnostics[%d].Code = %q, want %q; all=%#v", index, findings[index].Code, code, findings)
		}
		if err := findings[index].Validate(); err != nil {
			t.Fatalf("diagnostics[%d].Validate = %v", index, err)
		}
	}
}
