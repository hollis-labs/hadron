package graph

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestNormalizeID(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"already-normal":           "already-normal",
		"  Deploy Task  ":          "deploy-task",
		"PIPELINE_v2/DAG":          "pipeline-v2-dag",
		"many---separators___here": "many-separators-here",
		"mcp.call":                 "mcp-call",
		"---":                      "",
	}
	for input, want := range tests {
		input, want := input, want
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			if got := NormalizeID(input); got != want {
				t.Fatalf("NormalizeID(%q) = %q, want %q", input, got, want)
			}
		})
	}
}

func TestValidateID(t *testing.T) {
	t.Parallel()

	if err := ValidateID("deploy-task-2"); err != nil {
		t.Fatalf("ValidateID(valid) returned %v", err)
	}

	for _, invalid := range []string{"", "Deploy Task", "deploy_task", strings.Repeat("a", MaxIDLength+1)} {
		err := ValidateID(invalid)
		if err == nil {
			t.Fatalf("ValidateID(%q) unexpectedly succeeded", invalid)
		}
		var idErr *IDValidationError
		if !errors.As(err, &idErr) {
			t.Fatalf("ValidateID(%q) error type = %T, want *IDValidationError", invalid, err)
		}
	}
}

func TestClosedEnumValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		valid   bool
		invalid bool
	}{
		{name: "binding", valid: BindingExpression.Valid(), invalid: BindingKind("template").Valid()},
		{name: "edge", valid: EdgeControl.Valid(), invalid: EdgeKind("implicit").Valid()},
		{name: "readiness", valid: ReadyOneFailed.Valid(), invalid: ReadyRule("some_done").Valid()},
		{name: "effect", valid: EffectDestructive.Valid(), invalid: Effect("network").Valid()},
		{name: "backoff", valid: BackoffExponential.Valid(), invalid: BackoffStrategy("random").Valid()},
		{name: "idempotency", valid: IdempotencyKeyed.Valid(), invalid: IdempotencyMode("best_effort").Valid()},
		{name: "call mode", valid: CallRun.Valid(), invalid: CallMode("async").Valid()},
		{name: "parent close", valid: ParentCloseRequestCancel.Valid(), invalid: ParentClosePolicy("wait").Valid()},
		{name: "completion", valid: CompletionRunToCompletion.Valid(), invalid: RunCompletionMode("continue").Valid()},
		{name: "durability", valid: DurabilitySteps.Valid(), invalid: DurabilityMode("events").Valid()},
		{name: "source format", valid: SourceWorkflow.Valid(), invalid: SourceFormat("blueprint").Valid()},
		{name: "overlap", valid: OverlapForbid.Valid(), invalid: OverlapPolicy("queue").Valid()},
		{name: "run id reuse", valid: RunIDReuseReject.Valid(), invalid: RunIDReusePolicy("replace").Valid()},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if !test.valid {
				t.Fatal("declared enum constant is not valid")
			}
			if test.invalid {
				t.Fatal("unknown enum value is valid")
			}
		})
	}
}

func TestValidateEnumsReportsGraphPath(t *testing.T) {
	t.Parallel()

	graph := Graph{
		ID:      "invalid-enum-fixture",
		Version: "1.0.0",
		Digest:  "sha256:fixture",
		Nodes: []Node{{
			ID:      "child",
			Kind:    "call",
			Effects: EffectSet{Effect("unknown")},
			Call: &CallSpec{
				Definition: DefinitionRef{ID: "child"},
				Mode:       CallMode("detached"),
			},
		}},
	}

	err := graph.ValidateEnums()
	if err == nil {
		t.Fatal("ValidateEnums unexpectedly succeeded")
	}
	if !strings.Contains(err.Error(), "nodes[0].effects") || !strings.Contains(err.Error(), "nodes[0].call.mode") {
		t.Fatalf("ValidateEnums error lacks graph paths: %v", err)
	}
}

func TestValidateEnumsTraversesNestedSourceRefs(t *testing.T) {
	t.Parallel()

	invalidRef := func() *SourceRef {
		return &SourceRef{Format: SourceFormat("invalid")}
	}
	invalidExpression := func() Expression {
		return Expression{Text: "inputs.value", Source: invalidRef()}
	}

	graph := Graph{
		ID:      "nested-source-ref-fixture",
		Version: "1.0.0",
		Digest:  "sha256:nested-source-refs",
		Concurrency: ConcurrencySpec{
			Extension: Extension{Source: invalidRef()},
		},
		Completion: &RunCompletionPolicy{
			Mode:      CompletionFailFast,
			Extension: Extension{Source: invalidRef()},
		},
		Durability: &DurabilitySpec{
			Mode:      DurabilitySteps,
			Extension: Extension{Source: invalidRef()},
		},
		Extensions: map[string]Extension{
			"graph-extension": {Source: invalidRef()},
		},
		Activations: []ActivationDeclaration{{
			ID:   "activation",
			Kind: "schedule",
			Policy: ActivationPolicy{
				DeduplicationKey: pointer(invalidExpression()),
			},
		}},
		Nodes: []Node{{
			ID:      "nested",
			Kind:    "custom",
			If:      pointer(invalidExpression()),
			ForEach: &ForEachSpec{Items: invalidExpression()},
			Outputs: []OutputSpec{{
				Name:   "result",
				Schema: Schema{"type": "string"},
				Source: invalidRef(),
				Value: &Binding{
					Kind:       BindingExpression,
					Expression: pointer(invalidExpression()),
					Source:     invalidRef(),
				},
			}},
			Idempotency: &IdempotencySpec{Mode: IdempotencyKeyed, Key: pointer(invalidExpression())},
			Catch: []CatchRule{{
				When:   pointer(invalidExpression()),
				Source: invalidRef(),
			}},
			Switch: &SwitchSpec{Arms: []SwitchArm{{
				When:   invalidExpression(),
				Source: invalidRef(),
			}}},
			Verification: &VerificationSpec{
				Checks:    []VerificationCheck{{Kind: "check", Source: invalidRef()}},
				Extension: Extension{Source: invalidRef()},
			},
			Memoization: &MemoizationSpec{
				Key:       invalidExpression(),
				Extension: Extension{Source: invalidRef()},
			},
			Durability: &DurabilitySpec{
				Mode:      DurabilitySteps,
				Extension: Extension{Source: invalidRef()},
			},
			Service: &ServiceNodeSpec{
				ReadyCheck: &VerificationSpec{
					Checks:    []VerificationCheck{{Kind: "ready", Source: invalidRef()}},
					Extension: Extension{Source: invalidRef()},
				},
				Extension: Extension{Source: invalidRef()},
			},
			Compensation: &CompensationSpec{Extension: Extension{Source: invalidRef()}},
			Extensions: map[string]Extension{
				"node-extension": {Source: invalidRef()},
			},
		}},
	}

	err := graph.ValidateEnums()
	if err == nil {
		t.Fatal("ValidateEnums unexpectedly succeeded")
	}
	for _, path := range []string{
		"concurrency.extension.source.format",
		"completion.extension.source.format",
		"durability.extension.source.format",
		`extensions["graph-extension"].source.format`,
		"activations[0].policy.deduplication_key.source.format",
		"nodes[0].if.source.format",
		"nodes[0].for_each.items.source.format",
		"nodes[0].outputs[0].source.format",
		"nodes[0].outputs[0].value.source.format",
		"nodes[0].outputs[0].value.expression.source.format",
		"nodes[0].idempotency.key.source.format",
		"nodes[0].catch[0].when.source.format",
		"nodes[0].catch[0].source.format",
		"nodes[0].switch.arms[0].when.source.format",
		"nodes[0].switch.arms[0].source.format",
		"nodes[0].verify.checks[0].source.format",
		"nodes[0].verify.extension.source.format",
		"nodes[0].memoize.key.source.format",
		"nodes[0].memoize.extension.source.format",
		"nodes[0].durability.extension.source.format",
		"nodes[0].service.ready_check.checks[0].source.format",
		"nodes[0].service.ready_check.extension.source.format",
		"nodes[0].service.extension.source.format",
		"nodes[0].compensation.extension.source.format",
		`nodes[0].extensions["node-extension"].source.format`,
	} {
		if !strings.Contains(err.Error(), path) {
			t.Errorf("ValidateEnums error does not contain %q:\n%v", path, err)
		}
	}
}

func TestValidateEnumsHasStableMapErrorOrder(t *testing.T) {
	t.Parallel()

	invalidRef := func(value string) *SourceRef {
		return &SourceRef{Format: SourceFormat(value)}
	}
	graph := Graph{
		ID:      "stable-enum-order-fixture",
		Version: "1.0.0",
		Digest:  "sha256:stable-enum-order",
		SourceMap: SourceMap{Nodes: map[string]SourceRef{
			"z": {Format: SourceFormat("bad-z")},
			"a": {Format: SourceFormat("bad-a")},
		}},
		Extensions: map[string]Extension{
			"z": {Source: invalidRef("bad-z")},
			"a": {Source: invalidRef("bad-a")},
		},
		Activations: []ActivationDeclaration{{
			ID:   "activation",
			Kind: "schedule",
			Inputs: map[string]Binding{
				"z": {Kind: BindingKind("bad-z")},
				"a": {Kind: BindingKind("bad-a")},
			},
		}},
		Nodes: []Node{{
			ID:   "node",
			Kind: "custom",
			InputBindings: map[string]Binding{
				"z": {Kind: BindingKind("bad-z")},
				"a": {Kind: BindingKind("bad-a")},
			},
			Extensions: map[string]Extension{
				"z": {Source: invalidRef("bad-z")},
				"a": {Source: invalidRef("bad-a")},
			},
		}},
	}

	first := graph.ValidateEnums()
	if first == nil {
		t.Fatal("ValidateEnums unexpectedly succeeded")
	}
	wantOrder := []string{
		`source_map.nodes["a"]`,
		`source_map.nodes["z"]`,
		`extensions["a"]`,
		`extensions["z"]`,
		`activations[0].inputs["a"]`,
		`activations[0].inputs["z"]`,
		`nodes[0].with["a"]`,
		`nodes[0].with["z"]`,
		`nodes[0].extensions["a"]`,
		`nodes[0].extensions["z"]`,
	}
	position := -1
	for _, path := range wantOrder {
		next := strings.Index(first.Error(), path)
		if next <= position {
			t.Fatalf("error path %q is out of order:\n%v", path, first)
		}
		position = next
	}
	for i := 0; i < 100; i++ {
		if got := graph.ValidateEnums(); got == nil || got.Error() != first.Error() {
			t.Fatalf("ValidateEnums call %d is unstable:\nfirst:\n%v\ngot:\n%v", i+2, first, got)
		}
	}
}

func pointer[T any](value T) *T {
	return &value
}

func TestSequentialBlueprintRepresentation(t *testing.T) {
	t.Parallel()

	graph := Graph{
		ID:        "legacy-sequential-rewrite",
		Namespace: "examples",
		Version:   "1.0.0",
		Digest:    "sha256:sequential",
		Inputs: []InputSpec{{
			Name:     "project-id",
			Required: true,
			Schema:   Schema{"type": "string"},
		}},
		Nodes: []Node{
			{ID: "fetch", Kind: "http", Config: Config{"method": "GET"}},
			{
				ID:    "summarize",
				Kind:  "cmd",
				Needs: []Need{{Node: "fetch", Kind: EdgeControl}},
			},
		},
		Outputs: []OutputSpec{{
			Name:   "summary",
			Schema: Schema{"type": "string"},
			Value: &Binding{
				Kind:       BindingExpression,
				Expression: &Expression{Text: "steps.summarize.outputs.stdout"},
			},
		}},
	}

	if err := graph.ValidateEnums(); err != nil {
		t.Fatalf("ValidateEnums returned %v", err)
	}
	if got := graph.Nodes[1].Needs; !reflect.DeepEqual(got, []Need{{Node: "fetch", Kind: EdgeControl}}) {
		t.Fatalf("sequential dependency = %#v", got)
	}
}

func TestPipelineStagesRepresentedAsRunCalls(t *testing.T) {
	t.Parallel()

	source := &SourceRef{
		Format:    SourceArchivedPipeline,
		Locator:   "examples/archive/legacy-blueprints-pipelines/pipeline-v2-dag/pipeline.yaml",
		StageName: "deploy",
	}
	graph := Graph{
		ID:      "legacy-pipeline-rewrite",
		Version: "1.0.0",
		Digest:  "sha256:pipeline",
		Nodes: []Node{
			{
				ID:   "build",
				Kind: "call",
				Call: &CallSpec{Definition: DefinitionRef{Locator: "build.yaml"}, Mode: CallRun},
			},
			{
				ID:    "lint",
				Kind:  "call",
				Needs: []Need{{Node: "build", Kind: EdgeControl}},
				Call:  &CallSpec{Definition: DefinitionRef{Locator: "lint.yaml"}, Mode: CallRun},
			},
			{
				ID:     "deploy",
				Kind:   "call",
				Needs:  []Need{{Node: "lint", Kind: EdgeControl}, {Node: "build", Kind: EdgeData}},
				Call:   &CallSpec{Definition: DefinitionRef{Locator: "deploy.yaml"}, Mode: CallRun},
				Source: source,
				InputBindings: map[string]Binding{
					"build-version": {
						Kind:       BindingExpression,
						Expression: &Expression{Text: "steps.build.outputs.version"},
					},
				},
			},
		},
	}

	if err := graph.ValidateEnums(); err != nil {
		t.Fatalf("ValidateEnums returned %v", err)
	}
	for _, node := range graph.Nodes {
		if node.Kind != "call" || node.Call == nil || node.Call.Mode != CallRun {
			t.Fatalf("pipeline node %q is not a run call: %#v", node.ID, node.Call)
		}
	}
	if got := graph.Nodes[2].InputBindings["build-version"].Expression.Text; got != "steps.build.outputs.version" {
		t.Fatalf("pipeline output binding = %q", got)
	}
}

func TestCanonicalExtensionEnvelopesRemainApplicationNeutral(t *testing.T) {
	t.Parallel()

	graph := Graph{
		ID:      "extension-envelope-fixture",
		Version: "1.0.0",
		Digest:  "sha256:extensions",
		Concurrency: ConcurrencySpec{
			Resources: []ConcurrencyResource{{Name: "deploy-slot", Limit: 1}},
		},
		Completion: &RunCompletionPolicy{Mode: CompletionFailFast},
		Durability: &DurabilitySpec{Mode: DurabilitySteps},
		Policy:     []PolicyRequirement{{Name: "change-approval"}},
		Target: ExecutionTargetRequirements{
			Kinds:        []string{"remote-runner"},
			Capabilities: []string{"network"},
		},
		Activations: []ActivationDeclaration{{
			ID:   "weekday",
			Kind: "schedule",
			Config: Config{
				"cron": "0 9 * * 1-5",
			},
			Policy: ActivationPolicy{Overlap: OverlapForbid, RunIDReuse: RunIDReuseReject},
		}},
		Nodes: []Node{{
			ID:          "deploy",
			Kind:        "custom-adapter",
			Config:      Config{"adapter-owned": map[string]any{"value": true}},
			Effects:     EffectSet{EffectMutate},
			Concurrency: []ConcurrencyClaim{{Resource: "deploy-slot", Amount: 1}},
			Retry: &RetryPolicy{
				Attempts: 3,
				Backoff:  BackoffPolicy{Strategy: BackoffExponential, InitialDelay: "1s"},
			},
			Idempotency: &IdempotencySpec{
				Mode: IdempotencyKeyed,
				Key:  &Expression{Text: "inputs.request_id"},
			},
			Timeout:      &TimeoutPolicy{Execution: "5m"},
			Verification: &VerificationSpec{Checks: []VerificationCheck{{Kind: "output_schema"}}},
			Memoization:  &MemoizationSpec{Key: Expression{Text: "inputs.request_id"}, MaxAge: "1h"},
			Durability:   &DurabilitySpec{Mode: DurabilitySteps},
			Service:      &ServiceNodeSpec{HeartbeatTimeout: "30s", TeardownNodes: []string{"cleanup"}},
			Compensation: &CompensationSpec{Extension: Extension{Version: "unresolved", Config: Config{"reserved": true}}},
			Policy:       []PolicyRequirement{{Name: "mutate"}},
			Target:       ExecutionTargetRequirements{Capabilities: []string{"network"}},
		}},
	}

	if err := graph.ValidateEnums(); err != nil {
		t.Fatalf("ValidateEnums returned %v", err)
	}
	if got := graph.Nodes[0].Config["adapter-owned"]; got == nil {
		t.Fatal("opaque adapter config was not retained")
	}
	if graph.Nodes[0].Compensation.Extension.Config["reserved"] != true {
		t.Fatal("compensation extension envelope was not retained")
	}
}

func TestMemoizationPolicyRequiresPositiveAgeAndCanonicalOptionalDigest(t *testing.T) {
	t.Parallel()
	base := Graph{ID: "memo-policy", Version: "1", Digest: "sha256:fixture", Nodes: []Node{{ID: "work", Kind: "transform", Memoization: &MemoizationSpec{Key: Expression{Text: "inputs.key"}, MaxAge: "1h", OutputDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}}}
	if err := base.ValidateEnums(); err != nil {
		t.Fatalf("valid memo policy: %v", err)
	}
	for _, mutate := range []func(*MemoizationSpec){func(spec *MemoizationSpec) { spec.MaxAge = "0s" }, func(spec *MemoizationSpec) { spec.MaxAge = "later" }, func(spec *MemoizationSpec) { spec.OutputDigest = "SHA256:BAD" }} {
		candidate := base
		node := base.Nodes[0]
		spec := *node.Memoization
		mutate(&spec)
		node.Memoization = &spec
		candidate.Nodes = []Node{node}
		if err := candidate.ValidateEnums(); err == nil {
			t.Fatalf("invalid memo policy accepted: %#v", spec)
		}
	}
}
