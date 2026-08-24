package generatedchild_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"

	generatedchild "github.com/hollis-labs/hadron/workflow/adapters/generatedchild"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestGeneratedChildUsesEffectivePolicyAndDurableExactResolver(t *testing.T) {
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewNoopKind("hostile", "v1")
	kind.SpecValue.Effects = graph.EffectSet{graph.EffectMaterialize, graph.EffectMutate}
	kind.SpecValue.RequiredCapabilities = []string{"hostile.execute"}
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	processor := generatedchild.NewProcessor(generatedchild.ProcessorOptions{Validation: workflowcompile.ValidationOptions{StepKinds: registry}})
	journal := &definitionJournal{entries: make(map[string]workflowcompile.ResolvedDefinition)}
	var authorized generatedchild.AuthorizationRequest
	executor, err := generatedchild.New(generatedchild.Options{
		Processor: processor,
		Authorizer: generatedchild.AuthorizerFunc(func(_ context.Context, request generatedchild.AuthorizationRequest) (generatedchild.AuthorizationDecision, error) {
			authorized = request
			return generatedchild.AuthorizationDecision{Allow: true, Code: "approved", Reason: "fixture policy approved"}, nil
		}),
		Registrar: journal,
		Resolver:  definitionResolver{journal: journal},
	})
	if err != nil {
		t.Fatal(err)
	}
	material := generatedGraphValue(t, graph.Graph{ID: "generated-child", Version: "v1", Nodes: []graph.Node{{ID: "danger", Kind: "hostile", KindVersion: "v1", Config: graph.Config{}}}})
	result, err := executor.Execute(context.Background(), stepkind.PreparedInvocation{Invocation: stepkind.Invocation{
		Identity: stepkind.InvocationIdentity{RunID: "parent", NodeID: "generate", Attempt: 1},
		Config:   graph.Config{"format": string(generatedchild.FormatGraphIR), "input": "definition", "authority": "project"},
		Inputs:   values.ValueSet{"definition": material}, IdempotencyKey: "generate-child-1",
	}})
	if err != nil || result.Outcome != stepkind.StepCompleted {
		t.Fatalf("Execute() = %#v, %v", result, err)
	}
	if fmt.Sprint(authorized.Effects) != "[materialize mutate]" || fmt.Sprint(authorized.RequiredCapabilities) != "[hostile.execute]" || authorized.ConfigDigests["danger"] == "" {
		t.Fatalf("effective authorization summary = %#v", authorized)
	}
	ref, ok := result.Outputs[generatedchild.OutputDefinition].Inline.(graph.DefinitionRef)
	if !ok {
		// JSON-owned value envelopes may materialize the public struct as a map;
		// decode it exactly for the ordinary resolver assertion.
		encoded, _ := json.Marshal(result.Outputs[generatedchild.OutputDefinition].Inline)
		_ = json.Unmarshal(encoded, &ref)
	}
	// A newly constructed resolver over the durable journal must resolve the
	// exact registered graph; no executor-local closure is involved.
	restarted := definitionResolver{journal: journal}
	resolved, err := restarted.ResolveDefinition(context.Background(), ref)
	if err != nil || resolved.Definition.Digest != ref.Digest || resolved.Graph.Nodes[0].Kind != "hostile" {
		t.Fatalf("restart resolution = %#v, %v", resolved, err)
	}
}

func TestGeneratedChildDenialPrecedesRegistrationAndResolverMismatchFailsClosed(t *testing.T) {
	processed := generatedchild.ProcessedDefinition{Definition: generatedDefinition(), Policy: generatedchild.PolicySummary{Effects: graph.EffectSet{graph.EffectMaterialize}, ConfigDigests: map[string]string{"n": values.SHA256Digest([]byte("config"))}}}
	processor := generatedchild.ProcessorFunc(func(context.Context, generatedchild.ProcessRequest) (generatedchild.ProcessedDefinition, error) {
		return processed, nil
	})
	registrations := 0
	registrar := generatedchild.RegistrarFunc(func(context.Context, generatedchild.RegistrationRequest) (workflowcompile.ResolvedDefinition, generatedchild.RegistrationOutcome, error) {
		registrations++
		return processed.Definition, generatedchild.RegistrationApplied, nil
	})
	denied, err := generatedchild.New(generatedchild.Options{
		Processor: processor, Registrar: registrar,
		Authorizer: generatedchild.AuthorizerFunc(func(context.Context, generatedchild.AuthorizationRequest) (generatedchild.AuthorizationDecision, error) {
			return generatedchild.AuthorizationDecision{Allow: false, Code: "denied", Reason: "fixture policy denied"}, nil
		}),
		Resolver: workflowcompile.DefinitionResolverFunc(func(context.Context, graph.DefinitionRef) (workflowcompile.ResolvedDefinition, error) {
			return processed.Definition, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := denied.Execute(context.Background(), generatedInvocation(t)); err == nil || registrations != 0 {
		t.Fatalf("denied execution registered material: calls=%d err=%v", registrations, err)
	}

	mismatch, _ := generatedchild.New(generatedchild.Options{
		Processor: processor, Registrar: registrar,
		Authorizer: generatedchild.AuthorizerFunc(func(context.Context, generatedchild.AuthorizationRequest) (generatedchild.AuthorizationDecision, error) {
			return generatedchild.AuthorizationDecision{Allow: true, Code: "approved", Reason: "fixture policy approved"}, nil
		}),
		Resolver: workflowcompile.DefinitionResolverFunc(func(context.Context, graph.DefinitionRef) (workflowcompile.ResolvedDefinition, error) {
			return workflowcompile.ResolvedDefinition{}, nil
		}),
	})
	if _, err := mismatch.Execute(context.Background(), generatedInvocation(t)); err == nil || registrations != 1 {
		t.Fatalf("resolver mismatch = calls=%d err=%v", registrations, err)
	}
}

func TestGeneratedWorkflowSourceUsesOrdinaryDependencyInference(t *testing.T) {
	registry := stepkind.NewRegistry()
	if err := registry.Register(stepkindtest.NewNoopKind("fixture", "v1")); err != nil {
		t.Fatal(err)
	}
	processor := generatedchild.NewProcessor(generatedchild.ProcessorOptions{Validation: workflowcompile.ValidationOptions{StepKinds: registry}})
	const source = `workflow: {id: generated-source, version: v1}
steps:
  - {id: producer, kind: fixture, config: {}}
  - id: consumer
    kind: fixture
    config: {}
    with:
      value: steps.producer.outputs.value
`
	material, err := values.NewInline(source, values.Metadata{Producer: values.Producer{Kind: "test", Reference: "source"}, MediaType: "text/plain", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	processed, err := processor.ProcessGenerated(context.Background(), generatedchild.ProcessRequest{
		Format: generatedchild.FormatWorkflowSource, Value: material, Authority: "project",
		Identity: stepkind.InvocationIdentity{RunID: "parent", NodeID: "generate", Attempt: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, edge := range processed.Definition.Graph.Edges {
		if edge.From == "producer" && edge.To == "consumer" && edge.Kind == graph.EdgeData {
			found = true
		}
	}
	if !found {
		t.Fatalf("generated source omitted inferred data edge: %#v", processed.Definition.Graph.Edges)
	}
}

func generatedInvocation(t *testing.T) stepkind.PreparedInvocation {
	t.Helper()
	value, err := values.NewInline(map[string]any{"graph": "placeholder"}, values.Metadata{Producer: values.Producer{Kind: "test", Reference: "material"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	return stepkind.PreparedInvocation{Invocation: stepkind.Invocation{Identity: stepkind.InvocationIdentity{RunID: "run", NodeID: "generate", Attempt: 1}, Config: graph.Config{"format": string(generatedchild.FormatGraphIR), "input": "definition", "authority": "project"}, Inputs: values.ValueSet{"definition": value}, IdempotencyKey: "key"}}
}

func generatedGraphValue(t *testing.T, value graph.Graph) values.Value {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var object map[string]any
	if decodeErr := decoder.Decode(&object); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	result, err := values.NewInline(object, values.Metadata{Producer: values.Producer{Kind: "test", Reference: "graph"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func generatedDefinition() workflowcompile.ResolvedDefinition {
	digest := values.SHA256Digest([]byte("generated"))
	provenance := graph.Provenance{Authority: "project", Origin: "generated-child", Locator: "generated:test", Digest: digest}
	ref := graph.DefinitionRef{Authority: "project", Kind: "workflow", ID: "child", Version: "v1", Digest: digest, Provenance: &provenance}
	return workflowcompile.ResolvedDefinition{Definition: ref, Graph: graph.Graph{ID: "child", Version: "v1", Digest: digest, Provenance: provenance, Nodes: []graph.Node{{ID: "n", Kind: "fixture", KindVersion: "v1", Config: graph.Config{}}}}}
}

type definitionJournal struct {
	mu      sync.Mutex
	entries map[string]workflowcompile.ResolvedDefinition
}

func (j *definitionJournal) RegisterGenerated(_ context.Context, request generatedchild.RegistrationRequest) (workflowcompile.ResolvedDefinition, generatedchild.RegistrationOutcome, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if prior, ok := j.entries[request.Definition.Definition.Digest]; ok {
		return prior, generatedchild.RegistrationReplayed, nil
	}
	j.entries[request.Definition.Definition.Digest] = request.Definition
	return request.Definition, generatedchild.RegistrationApplied, nil
}

type definitionResolver struct{ journal *definitionJournal }

func (r definitionResolver) ResolveDefinition(_ context.Context, ref graph.DefinitionRef) (workflowcompile.ResolvedDefinition, error) {
	r.journal.mu.Lock()
	defer r.journal.mu.Unlock()
	resolved, ok := r.journal.entries[ref.Digest]
	if !ok || !reflect.DeepEqual(resolved.Definition, ref) {
		return workflowcompile.ResolvedDefinition{}, fmt.Errorf("exact generated definition is unavailable")
	}
	encoded, _ := json.Marshal(resolved)
	var clone workflowcompile.ResolvedDefinition
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	_ = decoder.Decode(&clone)
	return clone, nil
}
