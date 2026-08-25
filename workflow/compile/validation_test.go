package compile_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
)

func TestValidateGraphAcceptsExplicitAcyclicGraphWithoutExpressionInference(t *testing.T) {
	registry := validationRegistry(t, stepkindtest.NewNoopKind("noop", "v1"))
	first := validationNode("first", "noop", "v1", 4)
	second := validationNode("second", "noop", "v1", 8)
	second.Needs = []graph.Need{{Node: "first", Kind: graph.EdgeData, Source: validationRef(9, "steps", "1", "needs", "0")}}
	second.InputBindings = map[string]graph.Binding{
		"unresolved-later": {
			Kind:       graph.BindingExpression,
			Expression: &graph.Expression{Text: "steps.not-a-dependency.outputs.value", Source: validationRef(11, "steps", "1", "with", "unresolved-later")},
		},
	}
	value := validationGraph(first, second)
	value.Edges = []graph.Edge{{From: "first", To: "second", Kind: graph.EdgeData, Source: validationRef(9, "steps", "1", "needs", "0")}}

	findings := workflowcompile.ValidateGraph(t.Context(), value, workflowcompile.ValidationOptions{StepKinds: registry})
	if len(findings) != 0 {
		t.Fatalf("ValidateGraph() diagnostics = %#v, want none", findings)
	}
}

func TestValidateGraphReportsNormalizedDuplicateWithRelatedSource(t *testing.T) {
	registry := validationRegistry(t, stepkindtest.NewNoopKind("noop", "v1"))
	value := validationGraph(
		validationNode("Build Step", "noop", "v1", 4),
		validationNode("build-step", "noop", "v1", 9),
	)
	findings := workflowcompile.ValidateGraph(t.Context(), value, workflowcompile.ValidationOptions{StepKinds: registry})
	assertCodes(t, findings, workflowcompile.CodeDuplicateNodeID)
	finding := findings[0]
	if finding.Source == nil || finding.Source.StartLine != 9 || len(finding.Related) != 1 || finding.Related[0].Source.StartLine != 4 {
		t.Fatalf("duplicate sources = %+v / %+v", finding.Source, finding.Related)
	}
	assertDiagnosticContract(t, finding)
}

func TestValidateGraphReportsUnknownDependencyAndCycle(t *testing.T) {
	registry := validationRegistry(t, stepkindtest.NewNoopKind("noop", "v1"))
	t.Run("unknown dependency", func(t *testing.T) {
		node := validationNode("consumer", "noop", "v1", 4)
		node.Needs = []graph.Need{{Node: "missing", Source: validationRef(6, "steps", "0", "needs", "0")}}
		findings := workflowcompile.ValidateGraph(t.Context(), validationGraph(node), workflowcompile.ValidationOptions{StepKinds: registry})
		assertCodes(t, findings, workflowcompile.CodeUnknownDependency)
		if findings[0].Source == nil || findings[0].Source.StartLine != 6 {
			t.Fatalf("unknown dependency source = %+v", findings[0].Source)
		}
		assertDiagnosticContract(t, findings[0])
	})

	t.Run("cycle", func(t *testing.T) {
		first := validationNode("first", "noop", "v1", 4)
		first.Needs = []graph.Need{{Node: "second", Source: validationRef(6, "steps", "0", "needs", "0")}}
		second := validationNode("second", "noop", "v1", 9)
		second.Needs = []graph.Need{{Node: "first", Source: validationRef(11, "steps", "1", "needs", "0")}}
		findings := workflowcompile.ValidateGraph(t.Context(), validationGraph(first, second), workflowcompile.ValidationOptions{StepKinds: registry})
		assertCodes(t, findings, workflowcompile.CodeGraphCycle)
		if findings[0].Source == nil || findings[0].Source.StartLine != 6 || !strings.Contains(findings[0].Message, "first -> second -> first") {
			t.Fatalf("cycle diagnostic = %+v", findings[0])
		}
		assertDiagnosticContract(t, findings[0])
	})
}

func TestValidateGraphStepKindVersionResolution(t *testing.T) {
	var typedNil *stepkind.MemoryRegistry
	empty := stepkind.NewRegistry()
	partial := validationRegistry(t, stepkindtest.NewNoopKind("other", "v1"))
	one := validationRegistry(t, stepkindtest.NewNoopKind("noop", "v1"))
	multiple := validationRegistry(t,
		stepkindtest.NewNoopKind("noop", "v1"),
		stepkindtest.NewNoopKind("noop", "v2"),
	)
	cases := []struct {
		name        string
		lookup      workflowcompile.StepKindLookup
		version     string
		wantUnknown bool
		wantText    string
	}{
		{name: "nil", wantUnknown: true},
		{name: "typed nil", lookup: typedNil, wantUnknown: true},
		{name: "zero versions", lookup: empty, wantUnknown: true},
		{name: "partial registry", lookup: partial, wantUnknown: true},
		{name: "one version fallback", lookup: one},
		{name: "multiple versions require pin", lookup: multiple, wantUnknown: true, wantText: "multiple registered versions"},
		{name: "exact pin", lookup: multiple, version: "v2"},
		{name: "missing exact pin", lookup: multiple, version: "v3", wantUnknown: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			node := validationNode("node", "noop", test.version, 4)
			findings := workflowcompile.ValidateGraph(t.Context(), validationGraph(node), workflowcompile.ValidationOptions{StepKinds: test.lookup})
			if test.wantUnknown {
				assertCodes(t, findings, workflowcompile.CodeUnknownStepKind)
				if test.wantText != "" && !strings.Contains(findings[0].Message, test.wantText) {
					t.Fatalf("unknown kind message = %q, want substring %q", findings[0].Message, test.wantText)
				}
				return
			}
			if len(findings) != 0 {
				t.Fatalf("ValidateGraph() diagnostics = %#v, want none", findings)
			}
		})
	}
}

func TestValidateGraphReportsClosedShapeFailures(t *testing.T) {
	registry := validationRegistry(t, stepkindtest.NewNoopKind("call", "v1"))
	node := validationNode("invalid-shape", "call", "v1", 8)
	node.ReadyWhen = "some_done"
	node.Call = &graph.CallSpec{Mode: "fork"}
	node.ForEach = &graph.ForEachSpec{Items: graph.Expression{Text: " ", Source: validationRef(12, "steps", "0", "for_each", "items")}}
	findings := workflowcompile.ValidateGraph(t.Context(), validationGraph(node), workflowcompile.ValidationOptions{StepKinds: registry})
	assertCodes(t, findings,
		workflowcompile.CodeInvalidCallMode,
		workflowcompile.CodeUnsupportedReadinessRule,
		workflowcompile.CodeInvalidForEach,
	)
	for _, finding := range findings {
		assertDiagnosticContract(t, finding)
	}
}

func TestValidateGraphRejectsFanOutSyntheticInputBindingCollisions(t *testing.T) {
	registry := validationRegistry(t, stepkindtest.NewNoopKind("noop", "v1"))
	for _, test := range []struct {
		name      string
		forEach   graph.ForEachSpec
		collision string
	}{
		{name: "default item", forEach: graph.ForEachSpec{Items: graph.Expression{Text: "inputs.items"}}, collision: "item"},
		{name: "default index", forEach: graph.ForEachSpec{Items: graph.Expression{Text: "inputs.items"}}, collision: "index"},
		{name: "custom item", forEach: graph.ForEachSpec{Items: graph.Expression{Text: "inputs.items"}, ItemName: "entry", IndexName: "position"}, collision: "entry"},
		{name: "custom index", forEach: graph.ForEachSpec{Items: graph.Expression{Text: "inputs.items"}, ItemName: "entry", IndexName: "position"}, collision: "position"},
	} {
		t.Run(test.name, func(t *testing.T) {
			node := validationNode("fan", "noop", "v1", 4)
			node.ForEach = &test.forEach
			node.InputBindings = map[string]graph.Binding{test.collision: {Kind: graph.BindingLiteral, Literal: "collision"}}
			findings := workflowcompile.ValidateGraph(t.Context(), validationGraph(node), workflowcompile.ValidationOptions{StepKinds: registry})
			assertCodes(t, findings, workflowcompile.CodeInvalidForEach)
			if !strings.Contains(findings[0].Message, "collides") {
				t.Fatalf("collision diagnostic = %#v", findings[0])
			}
		})
	}
}

func TestValidateGraphRejectsMismatchedCallNodeShape(t *testing.T) {
	registry := validationRegistry(t,
		stepkindtest.NewNoopKind("call", "v1"),
		stepkindtest.NewNoopKind("transform", "v1"),
	)
	tests := []struct {
		name string
		node graph.Node
	}{
		{"call without declaration", validationNode("missing-call", "call", "v1", 8)},
		{"non-call with declaration", func() graph.Node {
			node := validationNode("misplaced-call", "transform", "v1", 12)
			node.Call = &graph.CallSpec{Definition: graph.DefinitionRef{Locator: "child.workflow.yaml"}, Mode: graph.CallInline}
			return node
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			findings := workflowcompile.ValidateGraph(t.Context(), validationGraph(test.node), workflowcompile.ValidationOptions{
				StepKinds: registry,
				Definitions: workflowcompile.DefinitionResolverFunc(func(context.Context, graph.DefinitionRef) (workflowcompile.ResolvedDefinition, error) {
					t.Fatal("malformed call shape reached definition resolution")
					return workflowcompile.ResolvedDefinition{}, nil
				}),
			})
			assertCodes(t, findings, workflowcompile.CodeInvalidCallShape)
			assertDiagnosticContract(t, findings[0])
		})
	}
}

func TestValidateGraphChecksRegisteredConfigSchemaAndAdapterDiagnostics(t *testing.T) {
	t.Run("schema", func(t *testing.T) {
		kind := stepkindtest.NewNoopKind("configured", "v1")
		kind.SpecValue.ConfigSchema = graph.Schema{
			"type": "object",
			"properties": map[string]any{
				"enabled": map[string]any{"type": "boolean"},
			},
			"required":             []any{"enabled"},
			"additionalProperties": false,
		}
		registry := validationRegistry(t, kind)
		node := validationNode("configured", "configured", "v1", 7)
		node.Config = graph.Config{"enabled": "yes"}
		findings := workflowcompile.ValidateGraph(t.Context(), validationGraph(node), workflowcompile.ValidationOptions{StepKinds: registry})
		assertCodes(t, findings, workflowcompile.CodeInvalidStepConfig)
		if findings[0].Source == nil || findings[0].Source.StartLine != 7 || !strings.Contains(findings[0].Message, "/enabled") {
			t.Fatalf("schema diagnostic = %+v", findings[0])
		}
		assertDiagnosticContract(t, findings[0])
	})

	t.Run("external schema registration stays offline", func(t *testing.T) {
		kind := stepkindtest.NewNoopKind("external-schema", "v1")
		kind.SpecValue.ConfigSchema = graph.Schema{"$ref": "https://schemas.example.test/config.json"}
		err := stepkind.NewRegistry().Register(kind)
		var specErr *stepkind.SpecError
		if !errors.As(err, &specErr) || !strings.Contains(err.Error(), "external schema resource") {
			t.Fatalf("Register(external schema) error = %T %v", err, err)
		}
	})

	t.Run("structured adapter diagnostic", func(t *testing.T) {
		kind := stepkindtest.NewNoopKind("adapter", "v1")
		kind.ValidateConfigFunc = func(context.Context, graph.Config) []diagnostic.Diagnostic {
			return []diagnostic.Diagnostic{{
				Severity: diagnostic.SeverityError,
				Code:     stepkind.CodeInvalidConfig,
				Message:  "adapter mode is invalid",
			}}
		}
		registry := validationRegistry(t, kind)
		node := validationNode("adapter", "adapter", "v1", 15)
		findings := workflowcompile.ValidateGraph(t.Context(), validationGraph(node), workflowcompile.ValidationOptions{StepKinds: registry})
		assertCodes(t, findings, stepkind.CodeInvalidConfig)
		if findings[0].Message != "adapter mode is invalid" || findings[0].Source == nil || findings[0].Source.StartLine != 15 {
			t.Fatalf("adapter diagnostic conversion = %+v", findings[0])
		}
		assertDiagnosticContract(t, findings[0])
	})
}

func TestValidateGraphInvokesEffectRetryPolicyHook(t *testing.T) {
	registry := validationRegistry(t, stepkindtest.NewNoopKind("mutator", "v1"))
	node := validationNode("delete", "mutator", "v1", 12)
	node.Effects = graph.EffectSet{graph.EffectDestructive}
	node.Retry = &graph.RetryPolicy{Attempts: 3}
	called := 0
	hook := workflowcompile.PolicyHookFunc(func(_ context.Context, input workflowcompile.NodeValidation) []diagnostic.Diagnostic {
		called++
		if input.GraphID != "validation" || input.Kind == nil || input.Kind.Name != "mutator" {
			t.Fatalf("policy input = %+v", input)
		}
		return []diagnostic.Diagnostic{{
			Severity: diagnostic.SeverityError,
			Code:     diagnostic.CodeUnsafeEffectRetry,
			Message:  "destructive retry needs an idempotency proof",
		}}
	})
	findings := workflowcompile.ValidateGraph(t.Context(), validationGraph(node), workflowcompile.ValidationOptions{
		StepKinds:   registry,
		PolicyHooks: []workflowcompile.PolicyHook{hook},
	})
	if called != 1 {
		t.Fatalf("policy hook calls = %d, want 1", called)
	}
	assertCodes(t, findings, diagnostic.CodeUnsafeEffectRetry)
	if findings[0].Source == nil || findings[0].Source.StartLine != 12 {
		t.Fatalf("policy source = %+v", findings[0].Source)
	}
	assertDiagnosticContract(t, findings[0])
}

func TestValidateGraphCallCyclesDepthAndResolverFailures(t *testing.T) {
	registry := validationRegistry(t, stepkindtest.NewNoopKind("call", "v1"))
	options := func(resolver workflowcompile.DefinitionResolver) workflowcompile.ValidationOptions {
		return workflowcompile.ValidationOptions{StepKinds: registry, Definitions: resolver}
	}

	t.Run("direct cycle by resolved digest", func(t *testing.T) {
		root := validationGraph(validationCallNode("self", "root.workflow.yaml", 8))
		root.Digest = "sha256:root"
		resolver := workflowcompile.DefinitionResolverFunc(func(context.Context, graph.DefinitionRef) (workflowcompile.ResolvedDefinition, error) {
			return workflowcompile.ResolvedDefinition{Definition: graph.DefinitionRef{Digest: "sha256:root"}, Graph: root}, nil
		})
		findings := workflowcompile.ValidateGraph(t.Context(), root, options(resolver))
		assertCodes(t, findings, workflowcompile.CodeCallCycle)
		if findings[0].Source == nil || findings[0].Source.StartLine != 8 {
			t.Fatalf("direct cycle source = %+v", findings[0].Source)
		}
	})

	t.Run("indirect cycle by resolved graph digest", func(t *testing.T) {
		root := validationGraph(validationCallNode("child", "child.workflow.yaml", 8))
		root.Digest = "sha256:root"
		child := validationGraph(validationCallNode("parent", "root.workflow.yaml", 20))
		child.ID, child.Digest = "child", "sha256:child"
		resolver := workflowcompile.DefinitionResolverFunc(func(_ context.Context, ref graph.DefinitionRef) (workflowcompile.ResolvedDefinition, error) {
			switch ref.Locator {
			case "child.workflow.yaml":
				return workflowcompile.ResolvedDefinition{Definition: graph.DefinitionRef{ID: "child"}, Graph: child}, nil
			case "root.workflow.yaml":
				return workflowcompile.ResolvedDefinition{Definition: graph.DefinitionRef{ID: "validation"}, Graph: root}, nil
			default:
				return workflowcompile.ResolvedDefinition{}, errors.New("not found")
			}
		})
		findings := workflowcompile.ValidateGraph(t.Context(), root, options(resolver))
		assertCodes(t, findings, workflowcompile.CodeCallCycle)
		if findings[0].Source == nil || findings[0].Source.StartLine != 20 {
			t.Fatalf("indirect cycle source = %+v", findings[0].Source)
		}
	})

	t.Run("depth limit", func(t *testing.T) {
		root := validationGraph(validationCallNode("child", "child.workflow.yaml", 8))
		root.Digest = "sha256:root"
		child := validationGraph(validationCallNode("grandchild", "grandchild.workflow.yaml", 20))
		child.ID, child.Digest = "child", "sha256:child"
		grandchild := validationGraph(validationNode("done", "call", "v1", 30))
		grandchild.ID, grandchild.Digest = "grandchild", "sha256:grandchild"
		resolver := workflowcompile.DefinitionResolverFunc(func(_ context.Context, ref graph.DefinitionRef) (workflowcompile.ResolvedDefinition, error) {
			switch ref.Locator {
			case "child.workflow.yaml":
				return workflowcompile.ResolvedDefinition{Definition: graph.DefinitionRef{Digest: child.Digest}, Graph: child}, nil
			case "grandchild.workflow.yaml":
				return workflowcompile.ResolvedDefinition{Definition: graph.DefinitionRef{Digest: grandchild.Digest}, Graph: grandchild}, nil
			default:
				return workflowcompile.ResolvedDefinition{}, errors.New("not found")
			}
		})
		validationOptions := options(resolver)
		validationOptions.MaxCallDepth = 1
		findings := workflowcompile.ValidateGraph(t.Context(), root, validationOptions)
		assertCodes(t, findings, workflowcompile.CodeCallDepthExceeded)
		if findings[0].Source == nil || findings[0].Source.StartLine != 20 {
			t.Fatalf("depth source = %+v", findings[0].Source)
		}
	})

	t.Run("resolver failure has definition context", func(t *testing.T) {
		root := validationGraph(validationCallNode("missing", "missing.workflow.yaml", 14))
		resolver := workflowcompile.DefinitionResolverFunc(func(context.Context, graph.DefinitionRef) (workflowcompile.ResolvedDefinition, error) {
			return workflowcompile.ResolvedDefinition{}, errors.New("registry unavailable")
		})
		findings := workflowcompile.ValidateGraph(t.Context(), root, options(resolver))
		assertCodes(t, findings, workflowcompile.CodeDefinitionResolution)
		if findings[0].Source == nil || findings[0].Source.StartLine != 14 || len(findings[0].Related) != 1 || findings[0].Related[0].Source.Locator != "missing.workflow.yaml" {
			t.Fatalf("resolver diagnostic = %+v", findings[0])
		}
		assertDiagnosticContract(t, findings[0])
	})
}

func TestValidatePlanCallIdentityDoesNotCollideAcrossDefinitionTuples(t *testing.T) {
	registry := validationRegistry(t, stepkindtest.NewNoopKind("call", "v1"))
	rootGraph := validationGraph(validationCallNode("other-authority", "shared-b.workflow.yaml", 8))
	rootGraph.Digest = ""
	plan := &workflowcompile.ExecutionPlan{
		Definition: graph.DefinitionRef{Authority: "authority-a", Kind: "workflow", ID: "shared", Version: "v1"},
		Graph:      rootGraph,
	}
	resolver := workflowcompile.DefinitionResolverFunc(func(context.Context, graph.DefinitionRef) (workflowcompile.ResolvedDefinition, error) {
		return workflowcompile.ResolvedDefinition{
			Definition: graph.DefinitionRef{Authority: "authority-b", Kind: "workflow", ID: "shared", Locator: "shared-b.workflow.yaml", Version: "v2"},
			Graph:      graph.Graph{ID: "shared", Version: "v2"},
		}, nil
	})
	findings := workflowcompile.ValidatePlan(t.Context(), plan, workflowcompile.ValidationOptions{StepKinds: registry, Definitions: resolver})
	if len(findings) != 0 {
		t.Fatalf("distinct definition tuples false-cycled: %#v", findings)
	}
}

func TestValidateGraphDiagnosticsAreDeterministicallyOrdered(t *testing.T) {
	first := validationNode("Same Node", "missing", "", 20)
	first.Needs = []graph.Need{{Node: "absent", Source: validationRef(22, "steps", "0", "needs", "0")}}
	second := validationNode("same-node", "missing", "", 7)
	second.ReadyWhen = "unsupported"
	value := validationGraph(first, second)
	firstRun := workflowcompile.ValidateGraph(t.Context(), value, workflowcompile.ValidationOptions{})
	if len(firstRun) < 4 {
		t.Fatalf("diagnostics = %#v, want multiple independent findings", firstRun)
	}
	for range 20 {
		if repeated := workflowcompile.ValidateGraph(t.Context(), value, workflowcompile.ValidationOptions{}); !reflect.DeepEqual(firstRun, repeated) {
			t.Fatalf("diagnostics changed order:\nfirst: %#v\nagain: %#v", firstRun, repeated)
		}
	}
	for _, finding := range firstRun {
		assertDiagnosticContract(t, finding)
	}
}

func TestValidatePlanRejectsNilPlan(t *testing.T) {
	findings := workflowcompile.ValidatePlan(t.Context(), nil, workflowcompile.ValidationOptions{})
	assertCodes(t, findings, workflowcompile.CodeInvalidValidationInput)
	assertDiagnosticContract(t, findings[0])
}

func TestExternalValidationSeamsAreImplementable(t *testing.T) {
	var _ workflowcompile.StepKindLookup = stepkind.NewRegistry()
	var _ workflowcompile.PolicyHook = workflowcompile.PolicyHookFunc(nil)
	var _ workflowcompile.DefinitionResolver = workflowcompile.DefinitionResolverFunc(nil)
}

func validationRegistry(t *testing.T, kinds ...stepkind.StepKind) *stepkind.MemoryRegistry {
	t.Helper()
	registry := stepkind.NewRegistry()
	for _, kind := range kinds {
		if err := registry.Register(kind); err != nil {
			t.Fatalf("Register(%s@%s) error = %v", kind.Spec().Name, kind.Spec().Version, err)
		}
	}
	return registry
}

func validationGraph(nodes ...graph.Node) graph.Graph {
	return graph.Graph{
		ID:      "validation",
		Version: "v1",
		Digest:  "sha256:validation",
		Source:  validationRef(1, "workflow"),
		SourceMap: graph.SourceMap{
			Nodes: map[string]graph.SourceRef{},
		},
		Nodes: nodes,
	}
}

func validationNode(id, kind, version string, line int) graph.Node {
	return graph.Node{
		ID:          id,
		Kind:        kind,
		KindVersion: version,
		Source:      validationRef(line, "steps", fmt.Sprintf("%d", line)),
	}
}

func validationCallNode(id, locator string, line int) graph.Node {
	node := validationNode(id, "call", "v1", line)
	node.Call = &graph.CallSpec{
		Definition: graph.DefinitionRef{Kind: "workflow", Locator: locator},
		Mode:       graph.CallInline,
	}
	return node
}

func validationRef(line int, path ...string) *graph.SourceRef {
	return &graph.SourceRef{
		Format:      graph.SourceWorkflow,
		Locator:     "validation.workflow.yaml",
		StartLine:   line,
		StartColumn: 3,
		Path:        append([]string(nil), path...),
	}
}

func assertCodes(t *testing.T, findings []diagnostic.Diagnostic, want ...diagnostic.Code) {
	t.Helper()
	got := make([]diagnostic.Code, len(findings))
	for i, finding := range findings {
		got[i] = finding.Code
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("diagnostic codes = %v, want %v; diagnostics = %#v", got, want, findings)
	}
}

func assertDiagnosticContract(t *testing.T, finding diagnostic.Diagnostic) {
	t.Helper()
	if finding.Remediation == nil || strings.TrimSpace(finding.Remediation.Message) == "" {
		t.Fatalf("diagnostic lacks remediation: %+v", finding)
	}
	if err := finding.Validate(); err != nil {
		t.Fatalf("diagnostic.Validate() = %v for %+v", err, finding)
	}
}
