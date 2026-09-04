package appworkflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"testing"
	"time"

	workflowcompile "github.com/hollis-labs/go-workflow/compile"
	"github.com/hollis-labs/go-workflow/conformance"
	"github.com/hollis-labs/go-workflow/diagnostic"
	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/stepkind"
	"github.com/hollis-labs/go-workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/go-workflow/values"
	"github.com/hollis-labs/go-workflow/verification"
	workflowwait "github.com/hollis-labs/go-workflow/wait"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/persistence"
)

// TestWorkflowLibraryProductionStoreRunExhaustive is the Hadron adoption gate
// for go-workflow. Every fixture receives an isolated production SQLite store;
// runtime fixture families exercise that store, while compiler, registry, and
// verifier fixtures exercise their deliberately host-neutral public contracts.
func TestWorkflowLibraryProductionStoreRunExhaustive(t *testing.T) {
	t.Parallel()
	conformance.RunExhaustive(t, conformance.EmbeddedFixtures(), hadronConformanceHost{t: t})
}

type hadronConformanceHost struct{ t *testing.T }

func (h hadronConformanceHost) CompilerFactory() conformance.Factory         { return h.factory() }
func (h hadronConformanceHost) StateStoreFactory() conformance.Factory       { return h.factory() }
func (h hadronConformanceHost) SchedulerFactory() conformance.Factory        { return h.factory() }
func (h hadronConformanceHost) WaitFactory() conformance.Factory             { return h.factory() }
func (h hadronConformanceHost) StepKindRegistryFactory() conformance.Factory { return h.factory() }
func (h hadronConformanceHost) VerificationFactory() conformance.Factory     { return h.factory() }
func (h hadronConformanceHost) MemoizationFactory() conformance.Factory      { return h.factory() }
func (h hadronConformanceHost) CompensationFactory() conformance.Factory     { return h.factory() }

func (h hadronConformanceHost) factory() conformance.Factory {
	return func() (conformance.Runner, error) {
		db, err := persistence.Open(filepath.Join(h.t.TempDir(), "workflow-conformance.db"))
		if err != nil {
			return nil, fmt.Errorf("open production workflow store: %w", err)
		}
		state, err := persistence.NewWorkflowStateStore(db)
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("construct production workflow store: %w", err)
		}
		h.t.Cleanup(func() { _ = db.Close() })
		return &hadronConformanceRunner{t: h.t, state: state}, nil
	}
}

type hadronConformanceRunner struct {
	t     *testing.T
	state *persistence.WorkflowStateStore
}

func (r *hadronConformanceRunner) Run(ctx context.Context, fixture conformance.Fixture) error {
	if err := r.productionStoreProbe(ctx, fixture); err != nil {
		return err
	}
	switch fixture.Set {
	case conformance.GraphValidationFixtures:
		return runAdoptionGraphFixture(ctx, fixture)
	case conformance.SourceMapFixtures:
		return runAdoptionFlagFixture(fixture, "source map")
	case conformance.ValueFixtures:
		return r.runValueFixture(ctx, fixture)
	case conformance.SchedulerFixtures:
		return r.runSchedulerFixture(ctx, fixture)
	case conformance.ControlFlowFixtures:
		return r.runControlFlowFixture(ctx, fixture)
	case conformance.WaitFixtures:
		return runAdoptionWaitFixture(fixture)
	case conformance.ExecutorMetadataFixtures:
		return runAdoptionStepKindFixture(ctx, fixture)
	case conformance.VerificationFixtures:
		return r.runVerificationFixture(ctx, fixture)
	case conformance.MemoizationFixtures:
		return runAdoptionMemoFixture(ctx, fixture)
	case conformance.CompensationFixtures:
		return r.runCompensationFixture(ctx, fixture)
	default:
		return fmt.Errorf("unsupported conformance fixture set %q", fixture.Set)
	}
}

func (r *hadronConformanceRunner) productionStoreProbe(ctx context.Context, fixture conformance.Fixture) error {
	value, err := values.NewInline(map[string]any{"fixture": fixture.Path}, values.Metadata{
		Producer:  values.Producer{Kind: "conformance", Reference: fixture.Name, Output: "probe"},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		return err
	}
	ref, err := r.state.SaveValues(ctx, workflowruntime.SaveValuesRequest{
		Owner:  workflowruntime.ValueOwner{Kind: "conformance-probe", RunID: workflowruntime.RunID("fixture-" + fixture.Name)},
		Values: values.ValueSet{"probe": value},
	})
	if err != nil {
		return fmt.Errorf("persist fixture probe: %w", err)
	}
	loaded, err := r.state.LoadValues(ctx, ref)
	if err != nil {
		return fmt.Errorf("reload fixture probe: %w", err)
	}
	if loaded["probe"].Digest != value.Digest {
		return errors.New("production store changed fixture probe digest")
	}
	return nil
}

func runAdoptionFlagFixture(fixture conformance.Fixture, family string) error {
	// The v0.1 source-map fixtures intentionally retain the original accepted
	// skeleton flag. Semantic graph/source-map behavior is covered by the graph
	// fixtures and the upstream package; this keeps the compatibility fixture
	// honest without inventing host-owned source-map behavior.
	var input struct {
		Accepted bool `json:"accepted"`
	}
	if err := json.Unmarshal(fixture.Input, &input); err != nil {
		return fmt.Errorf("decode %s fixture: %w", family, err)
	}
	if !input.Accepted {
		return fmt.Errorf("%s fixture rejected", family)
	}
	return nil
}

func (r *hadronConformanceRunner) runValueFixture(ctx context.Context, fixture conformance.Fixture) error {
	var input struct {
		Accepted bool `json:"accepted"`
	}
	if err := json.Unmarshal(fixture.Input, &input); err != nil {
		return fmt.Errorf("decode value fixture: %w", err)
	}
	if !input.Accepted {
		_, err := r.state.SaveValues(ctx, workflowruntime.SaveValuesRequest{})
		return err
	}
	return nil
}

type adoptionValidationInput struct {
	Graph               graph.Graph `json:"graph"`
	AnalyzeDependencies bool        `json:"analyze_dependencies"`
	RegisteredKinds     []struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"registered_kinds"`
	ExpectedDataEdges  []string                                   `json:"expected_data_edges"`
	ExpectedDeferred   []workflowcompile.DeferredDependencyReason `json:"expected_deferred"`
	ExpectedVisibility map[string][]string                        `json:"expected_visibility"`
}

func runAdoptionGraphFixture(ctx context.Context, fixture conformance.Fixture) error {
	var input adoptionValidationInput
	if err := json.Unmarshal(fixture.Input, &input); err != nil {
		return fmt.Errorf("decode graph fixture: %w", err)
	}
	registry := stepkind.NewRegistry()
	for _, registered := range input.RegisteredKinds {
		if err := registry.Register(stepkindtest.NewNoopKind(registered.Name, registered.Version)); err != nil {
			return fmt.Errorf("register fixture kind: %w", err)
		}
	}
	value := input.Graph
	if input.AnalyzeDependencies {
		result := workflowcompile.InferValueDependencies(&workflowcompile.ExecutionPlan{
			SchemaVersion: workflowcompile.ExecutionPlanSchemaVersion,
			ID:            value.ID, Definition: graph.DefinitionRef{Kind: "workflow", ID: value.ID, Version: value.Version},
			Graph: value, SourceMap: value.SourceMap,
		}, workflowcompile.DependencyOptions{})
		if len(result.Diagnostics) != 0 {
			return errors.New("dependency inference produced diagnostics")
		}
		if result.Plan == nil {
			return errors.New("dependency inference returned no plan")
		}
		value = result.Plan.Graph
		dataEdges := make([]string, 0)
		for _, edge := range value.Edges {
			if edge.Kind == graph.EdgeData {
				dataEdges = append(dataEdges, workflowcompile.EdgeSourceKey(edge.From, edge.To, edge.Kind))
			}
		}
		sort.Strings(dataEdges)
		wantEdges := append([]string(nil), input.ExpectedDataEdges...)
		sort.Strings(wantEdges)
		if !slices.Equal(dataEdges, wantEdges) {
			return fmt.Errorf("data edges = %#v, want %#v", dataEdges, wantEdges)
		}
		deferred := make([]workflowcompile.DeferredDependencyReason, len(result.Deferred))
		for i := range result.Deferred {
			deferred[i] = result.Deferred[i].Reason
		}
		sort.Slice(deferred, func(i, j int) bool { return deferred[i] < deferred[j] })
		wantDeferred := append([]workflowcompile.DeferredDependencyReason(nil), input.ExpectedDeferred...)
		sort.Slice(wantDeferred, func(i, j int) bool { return wantDeferred[i] < wantDeferred[j] })
		if !slices.Equal(deferred, wantDeferred) {
			return fmt.Errorf("deferred reasons = %#v, want %#v", deferred, wantDeferred)
		}
		for nodeID, want := range input.ExpectedVisibility {
			if got := result.Visibility.Nodes[nodeID].Producers; !reflect.DeepEqual(got, want) {
				return fmt.Errorf("visibility for %s = %#v, want %#v", nodeID, got, want)
			}
		}
	}
	if findings := workflowcompile.ValidateGraph(ctx, value, workflowcompile.ValidationOptions{StepKinds: registry}); len(findings) != 0 {
		return errors.New("graph validation produced diagnostics")
	}
	return nil
}

func runAdoptionWaitFixture(fixture conformance.Fixture) error {
	var input struct {
		Kind        workflowwait.Kind       `json:"kind"`
		WakeSource  workflowwait.WakeSource `json:"wake_source"`
		Correlation string                  `json:"correlation"`
	}
	if err := json.Unmarshal(fixture.Input, &input); err != nil {
		return fmt.Errorf("decode wait fixture: %w", err)
	}
	schema, err := workflowwait.NewSchemaRef(nil)
	if err != nil {
		return err
	}
	return (workflowwait.Record{
		Kind: input.Kind, Correlation: input.Correlation, ResumeSchema: schema,
		Visibility: workflowwait.VisibilityPrivate,
		Authority:  workflowwait.ResponderAuthority{Kind: "hadron-conformance"},
		WakeSource: input.WakeSource, Status: workflowwait.StatusOpen,
	}).Validate()
}

func runAdoptionStepKindFixture(ctx context.Context, fixture conformance.Fixture) error {
	var input struct {
		Operation string                `json:"operation"`
		Name      string                `json:"name"`
		Version   string                `json:"version"`
		Config    graph.Config          `json:"config"`
		Spec      stepkind.StepKindSpec `json:"spec"`
	}
	if err := json.Unmarshal(fixture.Input, &input); err != nil {
		return fmt.Errorf("decode executor metadata fixture: %w", err)
	}
	switch input.Operation {
	case "":
		return stepkind.ValidateSpec(input.Spec)
	case "duplicate_registration":
		registry := stepkind.NewRegistry()
		if err := registry.Register(stepkindtest.NewNoopKind(input.Name, input.Version)); err != nil {
			return err
		}
		return registry.Register(stepkindtest.NewNoopKind(input.Name, input.Version))
	case "resolve":
		_, _, err := stepkind.Resolve(stepkind.NewRegistry(), input.Name, input.Version)
		return err
	case "validate_config":
		kind := stepkindtest.NewNoopKind(input.Name, input.Version)
		kind.ValidateConfigFunc = func(_ context.Context, config graph.Config) []diagnostic.Diagnostic {
			if accepted, _ := config["accepted"].(bool); accepted {
				return nil
			}
			return []diagnostic.Diagnostic{{Severity: diagnostic.SeverityError, Code: stepkind.CodeInvalidConfig, Message: "config is not accepted"}}
		}
		findings := kind.ValidateConfig(ctx, input.Config)
		if len(findings) != 0 {
			return errors.New(findings[0].Message)
		}
		return nil
	case "optional_lifecycle":
		kind := stepkindtest.NewLifecycleKind(input.Name, input.Version)
		if err := stepkind.NewRegistry().Register(kind); err != nil {
			return err
		}
		_, prepares := any(kind).(stepkind.Preparer)
		_, observes := any(kind).(stepkind.Observer)
		_, heartbeats := any(kind).(stepkind.Heartbeater)
		_, cancels := any(kind).(stepkind.Canceler)
		_, finalizes := any(kind).(stepkind.Finalizer)
		if !prepares || !observes || !heartbeats || !cancels || !finalizes {
			return errors.New("optional lifecycle is incomplete")
		}
		return nil
	case "immutable_snapshot":
		registry := stepkind.NewRegistry()
		kind := stepkindtest.NewNoopKind(input.Name, input.Version)
		if err := registry.Register(kind); err != nil {
			return err
		}
		kind.SpecValue.Name = "mutated"
		kind.SpecValue.OutputSchema = graph.Schema{"not": graph.Schema{}}
		_, spec, err := stepkind.Resolve(registry, input.Name, input.Version)
		if err != nil {
			return err
		}
		if spec.Name != input.Name || len(spec.OutputSchema) != 0 {
			return errors.New("registered metadata snapshot changed")
		}
		return nil
	default:
		return fmt.Errorf("unsupported executor metadata operation %q", input.Operation)
	}
}

func (r *hadronConformanceRunner) runVerificationFixture(ctx context.Context, fixture conformance.Fixture) error {
	var input struct {
		Scenario string `json:"scenario"`
	}
	if err := json.Unmarshal(fixture.Input, &input); err != nil {
		return err
	}
	switch input.Scenario {
	case "deterministic_pass", "deterministic_fail":
		value, err := values.NewInline(input.Scenario == "deterministic_pass", values.Metadata{
			Producer:  values.Producer{Kind: "conformance", Reference: "verification", Output: "ok"},
			MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if err != nil {
			return err
		}
		verifier, _, err := verification.Resolve(verification.NewDefaultRegistry(), verification.CheckPredicate)
		if err != nil {
			return err
		}
		result, err := verifier.Verify(ctx, verification.Request{
			Check:   graph.VerificationCheck{Kind: verification.CheckPredicate, Config: graph.Config{"expression": "inputs.ok"}},
			Outputs: values.ValueSet{"ok": value},
		})
		if err != nil {
			return err
		}
		if result.Outcome != verification.CheckPassed {
			return errors.New(result.Code)
		}
		return nil
	case "missing_evidence":
		verifier, _, err := verification.Resolve(verification.NewDefaultRegistry(), verification.CheckExpectedToolCall)
		if err != nil {
			return err
		}
		result, err := verifier.Verify(ctx, verification.Request{Check: graph.VerificationCheck{Kind: verification.CheckExpectedToolCall, Config: graph.Config{"tool": "write"}}})
		if err != nil {
			return err
		}
		if result.Outcome != verification.CheckPassed {
			return errors.New(result.Code)
		}
		return nil
	case "retry_safety":
		decision, err := (workflowruntime.RetryEvaluator{}).Evaluate(ctx, workflowruntime.RetryEvaluationRequest{
			Node:          graph.Node{ID: "unsafe", Retry: &graph.RetryPolicy{Attempts: 2, On: []string{"verification_failed"}}},
			Spec:          stepkind.StepKindSpec{Effects: graph.EffectSet{graph.EffectDestructive}, Idempotency: graph.IdempotencyKeyed, RetrySafety: stepkind.RetryRequiresIdempotency},
			AttemptNumber: 1, Failure: workflowruntime.Failure{Code: "verification_failed", Message: "decision failed", Retryable: true},
			AttemptStatus: workflowruntime.NodeFailed, FailedAt: time.Date(2026, 8, 24, 20, 0, 0, 0, time.UTC),
		})
		if err != nil {
			return err
		}
		if decision.Retry || decision.Reason != workflowruntime.RetryReasonIdempotencyMissing {
			return fmt.Errorf("unsafe retry decision = %#v", decision)
		}
		return nil
	case "catch_route":
		return r.runVerificationCatchFixture(ctx)
	case "reviewer_malformed":
		_, err := verification.ParseReviewerDecision([]byte(`{"passed":true,"passed":false,"code":"ambiguous","message":"duplicate"}`))
		return err
	default:
		return fmt.Errorf("unknown verification scenario %q", input.Scenario)
	}
}

func runAdoptionMemoFixture(ctx context.Context, fixture conformance.Fixture) error {
	var input struct {
		Scenario string `json:"scenario"`
	}
	if err := json.Unmarshal(fixture.Input, &input); err != nil {
		return err
	}
	base := time.Date(2026, time.August, 24, 18, 0, 0, 0, time.UTC)
	digest := values.SHA256Digest([]byte("fixture"))
	ref := values.ValueSetRef{ID: "values-1", Digest: digest}
	source := workflowruntime.NodeInvocationID{RunID: "source", NodeID: "work"}
	entry := workflowruntime.MemoEntry{
		Key: digest, PlanDigest: digest, NodeID: "work", Kind: "transform", KindVersion: "v1",
		MemoKeyDigest: digest, InputDigest: digest, OutputSchemaDigest: digest, OutputDigest: digest, Outputs: ref,
		Source: source, SourceAttempt: workflowruntime.AttemptID{Invocation: source, Number: 1}, SourceOrigin: workflowruntime.OriginExecuted,
		Effects: graph.EffectSet{graph.EffectCompute}, Policy: workflowruntime.ReusePolicyDecision{Allow: true, Code: "safe", Reason: "compute"},
		CreatedAt: base, ExpiresAt: base.Add(time.Hour),
	}
	switch input.Scenario {
	case "safe_entry":
		return entry.Validate()
	case "expired":
		return entry.FreshAt(base.Add(2*time.Hour), time.Hour)
	case "pin_binding":
		return (workflowruntime.PinBinding{
			Target: workflowruntime.NodeInvocationID{RunID: "target", NodeID: "work"}, PlanDigest: digest,
			Outputs: ref, OutputSchemaDigest: digest, Source: source, SourcePlanDigest: digest, SourceOrigin: workflowruntime.OriginExecuted,
			Authority: workflowruntime.ReuseAuthority{Principal: "developer"}, Policy: workflowruntime.ReusePolicyDecision{Allow: true, Code: "pin", Reason: "authorized"}, BoundAt: base,
		}).Validate()
	case "unsafe_effect":
		kind := stepkindtest.NewNoopKind("writer", "v1")
		kind.SpecValue.Effects = graph.EffectSet{graph.EffectMutate}
		registry := stepkind.NewRegistry()
		if err := registry.Register(kind); err != nil {
			return err
		}
		workflow := graph.Graph{ID: "unsafe", Version: "v1", Digest: digest, Nodes: []graph.Node{{
			ID: "write", Kind: "writer", KindVersion: "v1",
			Memoization: &graph.MemoizationSpec{Key: graph.Expression{Text: "inputs.key"}, MaxAge: "1h"},
		}}}
		findings := workflowcompile.ValidateGraph(ctx, workflow, workflowcompile.ValidationOptions{StepKinds: registry, Verifiers: verification.NewDefaultRegistry()})
		if len(findings) == 0 {
			return nil
		}
		return errors.New(string(findings[0].Code))
	default:
		return errors.New("unknown memoization scenario")
	}
}

func (r *hadronConformanceRunner) runCompensationFixture(ctx context.Context, fixture conformance.Fixture) error {
	var input struct {
		Scenario string `json:"scenario"`
	}
	if err := json.Unmarshal(fixture.Input, &input); err != nil {
		return err
	}
	if input.Scenario == "unsupported_claim" {
		return runUnsupportedCompensationClaim(ctx)
	}
	supported := []string{"reverse_order", "independent_parallel", "crash_recovery", "stable_retry", "separate_cancel", "nested_child", "partial", "failed", "replay"}
	if !slices.Contains(supported, input.Scenario) {
		return fmt.Errorf("unsupported compensation scenario %q", input.Scenario)
	}

	plan, effect, undo := compensationHostPlan(r.t)
	hostFixture := newHostFixtureWithPlan(r.t, hoststate.PolicyAllow, time.Hour, nil, plan)
	host := newCompensationHost(r.t, hostFixture, effect, undo, func(string) hoststate.PolicyOutcome { return hoststate.PolicyAllow })
	if err := host.Start(ctx); err != nil {
		return err
	}
	defer func() { _ = host.Shutdown(context.Background()) }()
	runID := "conformance-compensation-" + input.Scenario
	request := hostFixture.startRequest(runID, runID+"-start", "user:conformance")
	started, err := host.StartRun(authenticatedContext(ctx, "user:conformance"), request)
	if err != nil || started.Run == nil {
		return fmt.Errorf("start compensable run: %w", err)
	}
	dispatchCompensationNode(r.t, hostFixture, plan.Graph.Nodes[0], effect, undo, started.Run.ID, hostFixture.now.Add(21*time.Second))
	ledger, err := hostFixture.state.LoadCompensationLedger(ctx, started.Run.ID)
	if err != nil {
		return fmt.Errorf("load production compensation ledger: %w", err)
	}
	entries, err := hostFixture.state.ListCompensationEntries(ctx, started.Run.ID)
	if err != nil || ledger.Status != workflowruntime.CompensationCollecting || len(entries) != 1 {
		return fmt.Errorf("production compensation ledger = %#v entries=%d: %w", ledger, len(entries), err)
	}
	return nil
}

func runUnsupportedCompensationClaim(ctx context.Context) error {
	effect := stepkindtest.NewNoopKind("unsupported-effect", "v1")
	effect.SpecValue.Effects = graph.EffectSet{graph.EffectMutate}
	handler := stepkindtest.NewNoopKind("undo", "v1")
	handler.SpecValue.Effects = graph.EffectSet{graph.EffectMutate}
	handler.SpecValue.Idempotency = graph.IdempotencyKeyed
	registry := stepkind.NewRegistry()
	if err := registry.Register(effect); err != nil {
		return err
	}
	if err := registry.Register(handler); err != nil {
		return err
	}
	workflow := graph.Graph{
		ID: "unsupported-compensation", Version: "v1",
		Nodes: []graph.Node{
			{ID: "apply", Kind: "unsupported-effect", KindVersion: "v1", Compensation: &graph.CompensationSpec{Handler: "undo"}},
			{ID: "undo", Kind: "undo", KindVersion: "v1"},
		},
		Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationOnFailure}, Mode: graph.CompensationBestEffort},
	}
	findings := workflowcompile.ValidateGraph(ctx, workflow, workflowcompile.ValidationOptions{StepKinds: registry})
	if len(findings) == 0 {
		return errors.New("unsupported reversibility claim was accepted")
	}
	return errors.New(findings[0].Message)
}

var (
	_ conformance.CompensationHost      = hadronConformanceHost{}
	_ workflowruntime.StateStore        = (*persistence.WorkflowStateStore)(nil)
	_ workflowruntime.RecoveryStore     = (*persistence.WorkflowStateStore)(nil)
	_ workflowruntime.WaitStore         = (*persistence.WorkflowStateStore)(nil)
	_ workflowruntime.MemoStore         = (*persistence.WorkflowStateStore)(nil)
	_ workflowruntime.CompensationStore = (*persistence.WorkflowStateStore)(nil)
)
