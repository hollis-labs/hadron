package appworkflow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	hadronregistry "github.com/hollis-labs/hadron/internal/registry"
	agentadapter "github.com/hollis-labs/hadron/workflow/adapters/agent"
	"github.com/hollis-labs/hadron/workflow/adapters/transform"
	"github.com/hollis-labs/hadron/workflow/authoring"
	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/values"
)

var testAgentHostIdentity = AgentAuthoringHostIdentity{
	Authority: "host:agent-ingress", TrustClass: "agent-reviewed", Principal: "principal:host-agent",
}

func TestAgentAuthoringUsesPolicyContractAndRegistryPath(t *testing.T) {
	stager := NewAuthoringSourceStager()
	catalog := hadronregistry.NewWorkflowIndex()
	kinds := stepkind.NewRegistry()
	if err := kinds.Register(transform.New()); err != nil {
		t.Fatal(err)
	}
	policyCalls := 0
	resolver, err := NewDefinitionResolver(DefinitionResolverOptions{
		Roots: []string{t.TempDir()}, Authoring: stager, Registry: catalog,
		Authorizer: DefinitionAuthorizerFunc(func(context.Context, DefinitionAuthorization) error { return nil }),
		Compile: DefinitionCompileOptions{
			StepKinds: kinds, SemanticRevision: "agent-authoring-test-v1",
			PolicyHooks: []compile.PolicyHook{compile.PolicyHookFunc(func(_ context.Context, input compile.NodeValidation) []diagnostic.Diagnostic {
				policyCalls++
				if input.Node.ID != "echo" || len(input.Node.Effects) != 1 || input.Node.Effects[0] != graph.EffectCompute {
					return []diagnostic.Diagnostic{{Severity: diagnostic.SeverityError, Code: compile.CodePolicyViolation, Message: "unexpected agent graph policy facts"}}
				}
				return nil
			})},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var namespaceAuthorizations []NamespaceAuthorization
	contracts, err := NewContractRegistrationService(ContractRegistrationOptions{
		Definitions: resolver, StepKinds: kinds, Catalog: catalog,
		Authorizer: NamespaceAuthorizerFunc(func(_ context.Context, request NamespaceAuthorization) error {
			namespaceAuthorizations = append(namespaceAuthorizations, request)
			return nil
		}),
		Attestor: testContractAttestor{}, Policy: ContractTestPolicy{MinimumCases: 1, Repetitions: 1},
		Now: func() time.Time { return time.Date(2026, time.August, 24, 18, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewAgentAuthoringService(AgentAuthoringOptions{Stager: stager, Contracts: contracts, HostIdentity: testAgentHostIdentity})
	if err != nil {
		t.Fatal(err)
	}
	envelope := agentGraphEnvelopeWithSpoofedProvenance(t)
	request := AgentAuthoringRequest{
		Envelope: envelope, ID: "agent-demo", Version: "v1", Namespace: "team", MakeCurrent: true,
	}
	draft, err := service.Author(t.Context(), request)
	if err != nil || draft.Plan == nil || draft.Scaffold == nil || draft.Registration != nil || len(draft.Diagnostics) != 0 {
		t.Fatalf("draft Author() = %#v, %v", draft, err)
	}
	request.Suite = completedAgentSuite(t, *draft.Scaffold, draft.Plan.ID)
	registered, err := service.Author(t.Context(), request)
	if err != nil || registered.Plan == nil || registered.Registration == nil || registered.Report == nil || !registered.Report.Passed {
		t.Fatalf("registered Author() = %#v, %v", registered, err)
	}
	if registered.Registration.SourceFormat != graph.SourceAgent || registered.Registration.SchemaID == "" || registered.Registration.SchemaVersion == "" || policyCalls == 0 {
		t.Fatalf("registration/policy = %#v calls=%d", registered.Registration, policyCalls)
	}
	if len(namespaceAuthorizations) < 4 || namespaceAuthorizations[0].Stage != NamespaceAuthorizationRequested {
		t.Fatalf("namespace authorizations = %#v", namespaceAuthorizations)
	}
	for _, authorization := range namespaceAuthorizations {
		if authorization.Principal != testAgentHostIdentity.Principal {
			t.Fatalf("agent material influenced authorization identity: %#v", authorization)
		}
	}
	resolved, err := catalog.ResolveWorkflow(t.Context(), hadronregistry.WorkflowQuery{Name: "team/agent-demo", Version: "v1", Digest: registered.Registration.Digest})
	if err != nil || resolved.Record.SourceFormat != graph.SourceAgent || resolved.Record.Authority != testAgentHostIdentity.Authority || resolved.Record.TrustClass != testAgentHostIdentity.TrustClass || resolved.Record.PublisherPrincipal != testAgentHostIdentity.Principal || !resolved.Record.TestsPassed {
		t.Fatalf("catalog record = %#v, %v", resolved, err)
	}
	if _, resolveErr := stager.ResolveAuthoringSource(t.Context(), *registered.Definition); !errors.Is(resolveErr, ErrDefinitionUnresolved) {
		t.Fatalf("qualified material remained staged: %v", resolveErr)
	}
	if stats := resolver.CacheStats(); stats.Plans != 0 || stats.ExactSources != 0 {
		t.Fatalf("ephemeral authoring material entered a resolver cache: %#v", stats)
	}
	if loaded, loadErr := resolver.LoadPlan(t.Context(), registered.Plan.Digest); !errors.Is(loadErr, ErrDefinitionUnresolved) {
		t.Fatalf("ephemeral authoring plan entered the digest cache: %#v, %v", loaded, loadErr)
	}
	if _, resolveErr := resolver.ResolvePlan(t.Context(), *registered.Definition); !errors.Is(resolveErr, ErrDefinitionUnresolved) {
		t.Fatalf("removed authoring definition remained resolver-addressable: %v", resolveErr)
	}
	resolvedPlan, err := resolver.ResolvePlan(t.Context(), graph.DefinitionRef{
		Kind: DefinitionKindRegistry, ID: "team/agent-demo", Version: "v1", Digest: registered.Registration.Digest,
	})
	if err != nil || resolvedPlan == nil || resolvedPlan.Graph.ID != "agent-demo" || resolvedPlan.Definition.ID != "agent-demo" {
		t.Fatalf("registered agent definition did not resolve through registry authority: %#v, %v", resolvedPlan, err)
	}
	assertAgentGraphHostBound(t, resolvedPlan.Graph, resolved.Record.Provenance.Locator)
}

func TestAuthoringResolverDoesNotRetainExpandedBundledChildren(t *testing.T) {
	stager := NewAuthoringSourceStager()
	raw := agentBundledGraphEnvelope(t)
	envelope, findings := authoring.DecodeEnvelope(raw, authoring.Limits{})
	if len(findings) != 0 {
		t.Fatal(findings)
	}
	request := AgentAuthoringRequest{
		Envelope: raw, ID: "agent-bundle-demo", Version: "v1", Namespace: "team",
	}
	source, ref, err := stagedAgentSource(envelope, request, testAgentHostIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if stageErr := stager.Stage(t.Context(), source); stageErr != nil {
		t.Fatal(stageErr)
	}
	kinds := stepkind.NewRegistry()
	for _, kind := range []stepkind.StepKind{
		stepkindtest.NewNoopKind("call", "v1"),
		stepkindtest.NewNoopKind(agentadapter.KindName, agentadapter.KindVersion),
	} {
		if registerErr := kinds.Register(kind); registerErr != nil {
			t.Fatal(registerErr)
		}
	}
	resolver, err := NewDefinitionResolver(DefinitionResolverOptions{
		Roots: []string{t.TempDir()}, Authoring: stager,
		Authorizer: DefinitionAuthorizerFunc(func(context.Context, DefinitionAuthorization) error { return nil }),
		Compile: DefinitionCompileOptions{
			StepKinds: kinds, SemanticRevision: "agent-authoring-bundle-cache-test-v1",
			NodeExpanders: []compile.NodeExpander{agentadapter.SourceExpander{}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := resolver.ResolvePlan(t.Context(), ref)
	if err != nil || plan == nil || len(plan.BundledDefinitions) != 1 || len(plan.Graph.Nodes) != 1 || plan.Graph.Nodes[0].Call == nil {
		t.Fatalf("ResolvePlan(authoring bundle) = %#v, %v", plan, err)
	}
	child := plan.Graph.Nodes[0].Call.Definition
	if stats := resolver.CacheStats(); stats.Plans != 0 || stats.ExactSources != 0 {
		t.Fatalf("ephemeral authoring plan entered a resolver cache: %#v", stats)
	}
	if loaded, loadErr := resolver.LoadPlan(t.Context(), plan.Digest); !errors.Is(loadErr, ErrDefinitionUnresolved) {
		t.Fatalf("ephemeral authoring plan entered the digest cache: %#v, %v", loaded, loadErr)
	}

	stager.Remove(ref)
	if _, resolveErr := resolver.ResolvePlan(t.Context(), ref); !errors.Is(resolveErr, ErrDefinitionUnresolved) {
		t.Fatalf("removed authoring definition remained resolver-addressable: %v", resolveErr)
	}
	if resolved, resolveErr := resolver.ResolveDefinition(t.Context(), child); resolveErr == nil {
		t.Fatalf("unregistered bundled child remained resolver-addressable: %#v", resolved)
	}
	if stats := resolver.CacheStats(); stats.Plans != 0 || stats.ExactSources != 0 {
		t.Fatalf("failed post-removal lookups populated resolver caches: %#v", stats)
	}
}

func TestAuthoringResolverEnforcesHostSourceByteBound(t *testing.T) {
	stager := NewAuthoringSourceStager()
	raw := agentGraphEnvelope(t)
	envelope, findings := authoring.DecodeEnvelope(raw, authoring.Limits{})
	if len(findings) != 0 {
		t.Fatal(findings)
	}
	request := AgentAuthoringRequest{Envelope: raw, ID: "agent-demo", Version: "v1", Namespace: "team"}
	source, ref, err := stagedAgentSource(envelope, request, testAgentHostIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if stageErr := stager.Stage(t.Context(), source); stageErr != nil {
		t.Fatal(stageErr)
	}
	kinds := stepkind.NewRegistry()
	if registerErr := kinds.Register(transform.New()); registerErr != nil {
		t.Fatal(registerErr)
	}
	resolver, err := NewDefinitionResolver(DefinitionResolverOptions{
		Roots: []string{t.TempDir()}, Authoring: stager, MaxSourceBytes: int64(len(source.Bytes) - 1),
		Authorizer: DefinitionAuthorizerFunc(func(context.Context, DefinitionAuthorization) error { return nil }),
		Compile:    DefinitionCompileOptions{StepKinds: kinds, SemanticRevision: "agent-source-bound-test-v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, resolveErr := resolver.ResolveSource(t.Context(), ref); !errors.Is(resolveErr, ErrUnsafeDefinitionSource) {
		t.Fatalf("oversized authoring source was accepted: %v", resolveErr)
	}
	exactResolver, err := NewDefinitionResolver(DefinitionResolverOptions{
		Roots: []string{t.TempDir()}, Authoring: stager, MaxSourceBytes: int64(len(source.Bytes)),
		Authorizer: DefinitionAuthorizerFunc(func(context.Context, DefinitionAuthorization) error { return nil }),
		Compile:    DefinitionCompileOptions{StepKinds: kinds, SemanticRevision: "agent-source-exact-bound-test-v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan, resolveErr := exactResolver.ResolvePlan(t.Context(), ref); resolveErr != nil || plan == nil {
		t.Fatalf("raw graph at exact host byte bound was rejected by internal envelope overhead: %#v, %v", plan, resolveErr)
	}
}

func TestAgentAuthoringRejectsUnknownEnvelopeBeforeCatalogMutation(t *testing.T) {
	service, catalog := agentAuthoringFixture(t)
	valid := string(agentGraphEnvelope(t))
	invalid := []byte(valid[:len(valid)-1] + `,"unknown":true}`)
	result, err := service.Author(t.Context(), AgentAuthoringRequest{
		Envelope: invalid, ID: "agent-demo", Version: "v1", Namespace: "team",
	})
	if err != nil || len(result.Diagnostics) != 1 || result.Diagnostics[0].Code != string(authoring.CodeInvalidEnvelope) {
		t.Fatalf("Author(invalid) = %#v, %v", result, err)
	}
	if records, searchErr := catalog.SearchWorkflows(t.Context(), "team", ""); searchErr != nil || len(records) != 0 {
		t.Fatalf("invalid envelope mutated catalog: %#v, %v", records, searchErr)
	}
}

func TestStagedAgentSourceRebindsNestedSourceAndProvenance(t *testing.T) {
	raw := agentGraphEnvelopeWithSpoofedProvenance(t)
	envelope, findings := authoring.DecodeEnvelope(raw, authoring.Limits{})
	if len(findings) != 0 {
		t.Fatal(findings)
	}
	request := AgentAuthoringRequest{Envelope: raw, ID: "agent-demo", Version: "v1", Namespace: "team"}
	source, ref, err := stagedAgentSource(envelope, request, testAgentHostIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Authority != testAgentHostIdentity.Authority || source.TrustClass != testAgentHostIdentity.TrustClass || !strings.HasSuffix(ref.Locator, ".workflow.yaml") {
		t.Fatalf("host source identity = source %#v ref %#v", source, ref)
	}
	var persisted graph.Graph
	if err := json.Unmarshal(source.Bytes, &persisted); err != nil {
		t.Fatal(err)
	}
	assertAgentGraphHostBound(t, persisted, ref.Locator)
	if encoded, err := json.Marshal(persisted); err != nil || strings.Contains(string(encoded), "spoof") || strings.Contains(string(encoded), "../../") {
		t.Fatalf("untrusted provenance survived canonical staging: %s, %v", encoded, err)
	}
}

func TestAgentAuthoringRejectsCompiledPlanOverflowBeforeScaffoldOrRegistration(t *testing.T) {
	service, catalog := agentAuthoringFixtureWithLimits(t, authoring.Limits{MaximumNodes: 1})
	result, err := service.Author(t.Context(), AgentAuthoringRequest{
		Envelope: workflowSourceEnvelope(t, `workflow: {id: agent-demo, version: v1}
steps:
  - {id: one, kind: transform, kind_version: v1, config: {result: one}}
  - {id: two, kind: transform, kind_version: v1, config: {result: two}}
`),
		ID: "agent-demo", Version: "v1", Namespace: "team",
	})
	if err != nil || result.Plan != nil || result.Scaffold != nil || result.Report != nil || result.Registration != nil || len(result.Diagnostics) != 1 || result.Diagnostics[0].Code != string(authoring.CodeInvalidEnvelope) {
		t.Fatalf("Author(overflow) = %#v, %v", result, err)
	}
	if records, searchErr := catalog.SearchWorkflows(t.Context(), "team", ""); searchErr != nil || len(records) != 0 {
		t.Fatalf("overflow envelope mutated catalog: %#v, %v", records, searchErr)
	}
}

func TestAuthoringStagerRejectsConflictingExactIdentity(t *testing.T) {
	stager := NewAuthoringSourceStager()
	envelope, findings := authoring.DecodeEnvelope(agentGraphEnvelope(t), authoring.Limits{})
	if len(findings) != 0 {
		t.Fatal(findings)
	}
	request := AgentAuthoringRequest{Envelope: agentGraphEnvelope(t), ID: "agent-demo", Version: "v1", Namespace: "team"}
	source, ref, err := stagedAgentSource(envelope, request, testAgentHostIdentity)
	if err != nil || stager.Stage(t.Context(), source) != nil {
		t.Fatalf("stage initial = %v", err)
	}
	if replayErr := stager.Stage(t.Context(), source); replayErr != nil {
		t.Fatalf("stage identical replay = %v", replayErr)
	}
	conflict := source
	conflict.TrustClass = "different"
	if conflictErr := stager.Stage(t.Context(), conflict); !errors.Is(conflictErr, ErrAuthoringStageConflict) {
		t.Fatalf("conflicting stage = %v", conflictErr)
	}
	resolved, err := stager.ResolveAuthoringSource(t.Context(), ref)
	if err != nil || resolved.TrustClass != source.TrustClass {
		t.Fatalf("conflict changed stage = %#v, %v", resolved, err)
	}
	stager.Remove(ref)
	if _, err := stager.ResolveAuthoringSource(t.Context(), ref); err != nil {
		t.Fatalf("first replay removal dropped a live stage: %v", err)
	}
	stager.Remove(ref)
	if _, err := stager.ResolveAuthoringSource(t.Context(), ref); !errors.Is(err, ErrDefinitionUnresolved) {
		t.Fatalf("final replay removal retained the stage: %v", err)
	}
}

func agentAuthoringFixture(t *testing.T) (*AgentAuthoringService, *hadronregistry.WorkflowIndex) {
	return agentAuthoringFixtureWithLimits(t, authoring.Limits{})
}

func agentAuthoringFixtureWithLimits(t *testing.T, limits authoring.Limits) (*AgentAuthoringService, *hadronregistry.WorkflowIndex) {
	t.Helper()
	stager := NewAuthoringSourceStager()
	catalog := hadronregistry.NewWorkflowIndex()
	kinds := stepkind.NewRegistry()
	if err := kinds.Register(transform.New()); err != nil {
		t.Fatal(err)
	}
	resolver, err := NewDefinitionResolver(DefinitionResolverOptions{
		Roots: []string{t.TempDir()}, Authoring: stager,
		Authorizer: DefinitionAuthorizerFunc(func(context.Context, DefinitionAuthorization) error { return nil }),
		Compile:    DefinitionCompileOptions{StepKinds: kinds, SemanticRevision: "agent-authoring-test-v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	contracts, err := NewContractRegistrationService(ContractRegistrationOptions{
		Definitions: resolver, StepKinds: kinds, Catalog: catalog,
		Authorizer: NamespaceAuthorizerFunc(func(context.Context, NamespaceAuthorization) error { return nil }),
		Attestor:   testContractAttestor{}, Policy: ContractTestPolicy{MinimumCases: 1, Repetitions: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewAgentAuthoringService(AgentAuthoringOptions{Stager: stager, Contracts: contracts, HostIdentity: testAgentHostIdentity, Limits: limits})
	if err != nil {
		t.Fatal(err)
	}
	return service, catalog
}

func workflowSourceEnvelope(t *testing.T, source string) []byte {
	t.Helper()
	encoded, err := json.Marshal(authoring.Envelope{
		SchemaID: authoring.EnvelopeSchemaID, SchemaVersion: authoring.EnvelopeSchemaVersion,
		MaterialSchemaID: authoring.WorkflowSourceSchemaID, MaterialSchemaVersion: authoring.WorkflowSourceSchemaVersion,
		Format: authoring.FormatWorkflowSource, Source: source,
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func agentGraphEnvelope(t *testing.T) []byte {
	t.Helper()
	binding := graph.Binding{Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "inputs.message"}}
	outputBinding := graph.Binding{Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "steps.echo.outputs.result"}}
	builder := authoring.New("agent-demo", "v1").
		Input(graph.InputSpec{Name: "message", Schema: graph.Schema{"type": "string"}, Required: true}).
		Node(graph.Node{
			ID: "echo", Kind: "transform", KindVersion: "v1", Config: graph.Config{"result": "inputs.message"},
			InputBindings: map[string]graph.Binding{"message": binding},
			Outputs:       []graph.OutputSpec{{Name: "result", Schema: graph.Schema{"type": "string"}}},
			Effects:       graph.EffectSet{graph.EffectCompute},
		}).
		Output(graph.OutputSpec{Name: "result", Schema: graph.Schema{"type": "string"}, Value: &outputBinding})
	value := builder.Envelope()
	value.Graph.Source.Format = graph.SourceAgent
	value.Graph.SourceMap.Graph.Format = graph.SourceAgent
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func agentBundledGraphEnvelope(t *testing.T) []byte {
	t.Helper()
	value := authoring.New("agent-bundle-demo", "v1").
		Node(graph.Node{
			ID: "review", Kind: agentadapter.SugarKindName, KindVersion: agentadapter.KindVersion,
			Config: graph.Config{"substrate": "remote", "logical_agent_id": "reviewer", "wait": false},
		}).Envelope()
	value.Graph.Source.Format = graph.SourceAgent
	value.Graph.SourceMap.Graph.Format = graph.SourceAgent
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func agentGraphEnvelopeWithSpoofedProvenance(t *testing.T) []byte {
	t.Helper()
	var envelope authoring.Envelope
	if err := json.Unmarshal(agentGraphEnvelope(t), &envelope); err != nil {
		t.Fatal(err)
	}
	evilSource := func(path ...string) *graph.SourceRef {
		return &graph.SourceRef{Format: graph.SourceWorkflow, Locator: "../../spoof.workflow.yaml", StartLine: 9, Path: path}
	}
	evilProvenance := graph.Provenance{
		Authority: "agent:spoof", Origin: "spoof", Locator: "../../spoof.workflow.yaml",
		Revision: "forged", Digest: values.SHA256Digest([]byte("spoof")),
		Metadata: graph.Metadata{"trust_class": "operator"},
	}
	value := envelope.Graph
	value.Source = evilSource()
	value.SourceMap.Graph = evilSource()
	value.SourceMap.Inputs = map[string]graph.SourceRef{"message": *evilSource("inputs", "message")}
	value.SourceMap.Outputs = map[string]graph.SourceRef{"result": *evilSource("outputs", "result")}
	value.SourceMap.Nodes = map[string]graph.SourceRef{"echo": *evilSource("nodes", "echo")}
	value.Provenance = evilProvenance
	value.Inputs[0].Source = evilSource("inputs", "message")
	value.Outputs[0].Source = evilSource("outputs", "result")
	value.Outputs[0].Value.Source = evilSource("outputs", "result", "value")
	value.Outputs[0].Value.Expression.Source = evilSource("outputs", "result", "value", "expression")
	value.Nodes[0].Source = evilSource("nodes", "echo")
	value.Nodes[0].Provenance = evilProvenance
	binding := value.Nodes[0].InputBindings["message"]
	binding.Source = evilSource("nodes", "echo", "with", "message")
	binding.Expression.Source = evilSource("nodes", "echo", "with", "message", "expression")
	value.Nodes[0].InputBindings["message"] = binding
	value.Nodes[0].Outputs[0].Source = evilSource("nodes", "echo", "outputs", "result")
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func assertAgentGraphHostBound(t *testing.T, value graph.Graph, locator string) {
	t.Helper()
	assertSource := func(name string, source *graph.SourceRef) {
		t.Helper()
		if source == nil || source.Format != graph.SourceAgent || source.Locator != locator {
			t.Fatalf("%s source was not host rebound: %#v", name, source)
		}
	}
	assertProvenance := func(name string, provenance graph.Provenance) {
		t.Helper()
		if provenance.Authority != testAgentHostIdentity.Authority || provenance.Locator != locator || provenance.Metadata["trust_class"] != testAgentHostIdentity.TrustClass {
			t.Fatalf("%s provenance was not host rebound: %#v", name, provenance)
		}
	}
	assertSource("graph", value.Source)
	assertSource("source map graph", value.SourceMap.Graph)
	inputSource := value.SourceMap.Inputs["message"]
	assertSource("source map input", &inputSource)
	outputSource := value.SourceMap.Outputs["result"]
	assertSource("source map output", &outputSource)
	nodeSource := value.SourceMap.Nodes["echo"]
	assertSource("source map node", &nodeSource)
	assertProvenance("graph", value.Provenance)
	assertSource("input", value.Inputs[0].Source)
	assertSource("workflow output", value.Outputs[0].Source)
	assertSource("workflow output binding", value.Outputs[0].Value.Source)
	assertSource("workflow output expression", value.Outputs[0].Value.Expression.Source)
	node := value.Nodes[0]
	assertSource("node", node.Source)
	assertProvenance("node", node.Provenance)
	assertSource("node output", node.Outputs[0].Source)
	binding := node.InputBindings["message"]
	assertSource("node binding", binding.Source)
	assertSource("node expression", binding.Expression.Source)
}

func completedAgentSuite(t *testing.T, scaffold WorkflowContractSuite, planID string) *WorkflowContractSuite {
	t.Helper()
	input, err := values.NewInline("hello", values.Metadata{Producer: values.Producer{Kind: "contract-input", Reference: planID, Output: "message"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	output, err := values.NewInline("hello", values.Metadata{Producer: values.Producer{Kind: "contract-output", Reference: planID, Output: "result"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	current := &scaffold.Cases[0]
	current.Name, current.Editable = "echo", false
	current.Inputs, current.ExpectedOutputs = values.ValueSet{"message": input}, values.ValueSet{"result": output}
	current.Mocks[0].ExpectedInputs = values.ValueSet{"message": input}
	current.Mocks[0].ExpectedInputsEditable = false
	current.Mocks[0].Results = []ContractMockResult{{Outputs: values.ValueSet{"result": output}}}
	return &scaffold
}
