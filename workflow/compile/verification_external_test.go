package compile_test

import (
	"context"
	"slices"
	"testing"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/verification"
)

func TestCompileLowersVerificationAndValidationPreservesCheckSource(t *testing.T) {
	const source = `workflow:
  name: verified
steps:
  - name: inspect
    kind: fixture
    verify:
      - type: predicate
        config:
          expression: inputs.ok
      - type: expected_tool_call
        config:
          server: github
          tool: issues.get
`
	loaded := workflowcompile.LoadBytes("verified.workflow.yaml", []byte(source))
	if loaded.Source == nil || len(loaded.Diagnostics) != 0 {
		t.Fatalf("LoadBytes() = %#v", loaded)
	}
	compiled := workflowcompile.Compile(loaded.Source)
	if compiled.Plan == nil || len(compiled.Diagnostics) != 0 {
		t.Fatalf("Compile() = %#v", compiled)
	}
	modifier := compiled.Plan.Graph.Nodes[0].Verification
	if modifier == nil || len(modifier.Checks) != 2 || modifier.Checks[0].Kind != verification.CheckPredicate || modifier.Checks[1].Kind != verification.CheckExpectedToolCall {
		t.Fatalf("verification = %#v", modifier)
	}
	if source := modifier.Checks[0].Source; source == nil || source.StartLine != 7 || !slices.Equal(source.Path, []string{"steps", "0", "verify", "0"}) {
		t.Fatalf("predicate source = %#v", source)
	}
	kinds := stepkind.NewRegistry()
	if err := kinds.Register(stepkindtest.NewNoopKind("fixture", "v1")); err != nil {
		t.Fatal(err)
	}
	compiled.Plan.Graph.Nodes[0].KindVersion = "v1"
	findings := workflowcompile.ValidateGraph(context.Background(), compiled.Plan.Graph, workflowcompile.ValidationOptions{StepKinds: kinds, Verifiers: verification.NewDefaultRegistry()})
	if len(findings) != 0 {
		t.Fatalf("ValidateGraph() = %#v", findings)
	}

	compiled.Plan.Graph.Nodes[0].Verification.Checks[0].Kind = "missing-reviewer"
	findings = workflowcompile.ValidateGraph(context.Background(), compiled.Plan.Graph, workflowcompile.ValidationOptions{StepKinds: kinds, Verifiers: verification.NewDefaultRegistry()})
	if len(findings) != 1 || findings[0].Code != verification.CodeUnknownCheck || findings[0].Source == nil || findings[0].Source.StartLine != 7 {
		t.Fatalf("unknown verification diagnostics = %#v", findings)
	}
}

func TestCompileVerificationShapeFailsClosedAtExactItem(t *testing.T) {
	const source = `workflow:
  name: invalid-verification
steps:
  - name: inspect
    kind: fixture
    verify:
      - type: no_error
        kind: predicate
`
	loaded := workflowcompile.LoadBytes("invalid-verification.workflow.yaml", []byte(source))
	compiled := workflowcompile.Compile(loaded.Source)
	if compiled.Plan != nil || len(compiled.Diagnostics) != 1 || compiled.Diagnostics[0].Code != workflowcompile.CodeInvalidWorkflowShape || compiled.Diagnostics[0].Source == nil || compiled.Diagnostics[0].Source.StartLine != 7 {
		t.Fatalf("Compile() = %#v", compiled)
	}
}

func TestVerificationRetryStillPassesThroughUnsafeEffectPolicy(t *testing.T) {
	kind := stepkindtest.NewNoopKind("mcp", "v1")
	kind.SpecValue.Effects = graph.EffectSet{graph.EffectDestructive}
	kind.SpecValue.Idempotency = graph.IdempotencyNone
	kind.SpecValue.RetrySafety = stepkind.RetryUnsupported
	kinds := stepkind.NewRegistry()
	if err := kinds.Register(kind); err != nil {
		t.Fatal(err)
	}
	node := validationNode("delete", "mcp", "v1", 8)
	node.Retry = &graph.RetryPolicy{Attempts: 2}
	node.Verification = &graph.VerificationSpec{Checks: []graph.VerificationCheck{{Kind: verification.CheckExpectedToolCall, Config: graph.Config{"tool": "delete"}, Source: validationRef(11, "steps", "0", "verify", "0")}}}
	hook := workflowcompile.PolicyHookFunc(func(_ context.Context, input workflowcompile.NodeValidation) []diagnostic.Diagnostic {
		if input.Kind == nil || input.Kind.RetrySafety != stepkind.RetryUnsupported || !slices.Contains(input.Kind.Effects, graph.EffectDestructive) {
			t.Fatalf("policy input = %#v", input)
		}
		return []diagnostic.Diagnostic{{Severity: diagnostic.SeverityError, Code: diagnostic.CodeUnsafeEffectRetry, Message: "unsafe verification retry"}}
	})
	findings := workflowcompile.ValidateGraph(context.Background(), validationGraph(node), workflowcompile.ValidationOptions{
		StepKinds: kinds, Verifiers: verification.NewDefaultRegistry(), PolicyHooks: []workflowcompile.PolicyHook{hook},
	})
	if len(findings) != 1 || findings[0].Code != diagnostic.CodeUnsafeEffectRetry {
		t.Fatalf("ValidateGraph() = %#v", findings)
	}
}

func TestVerificationDiagnosticCodesDoNotReuseExpansionAllocation(t *testing.T) {
	if verification.CodeInvalidCheck != "HADR-SOURCE-034" || verification.CodeUnknownCheck != "HADR-SOURCE-035" ||
		verification.CodeInvalidCheck == "HADR-SOURCE-033" || verification.CodeUnknownCheck == "HADR-SOURCE-033" {
		t.Fatalf("verification diagnostic allocations = %q / %q", verification.CodeInvalidCheck, verification.CodeUnknownCheck)
	}
}

func TestMemoizationEffectSafetyFailsClosedWithNodeSource(t *testing.T) {
	kind := stepkindtest.NewNoopKind("writer", "v1")
	kind.SpecValue.Effects = graph.EffectSet{graph.EffectMutate}
	kinds := stepkind.NewRegistry()
	if err := kinds.Register(kind); err != nil {
		t.Fatal(err)
	}
	node := validationNode("write", "writer", "v1", 23)
	node.Memoization = &graph.MemoizationSpec{Key: graph.Expression{Text: "inputs.key"}, MaxAge: "1h"}
	findings := workflowcompile.ValidateGraph(context.Background(), validationGraph(node), workflowcompile.ValidationOptions{StepKinds: kinds, Verifiers: verification.NewDefaultRegistry()})
	if len(findings) != 1 || findings[0].Code != workflowcompile.CodeInvalidMemoization || findings[0].Source == nil || findings[0].Source.StartLine != 23 {
		t.Fatalf("memoization diagnostics = %#v", findings)
	}
}
