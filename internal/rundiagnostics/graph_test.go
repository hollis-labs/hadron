package rundiagnostics

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

type diagnosticFixture struct {
	runErr      error
	run         workflowruntime.RunSnapshot
	nodes       []workflowruntime.NodeInvocationSnapshot
	attempts    map[workflowruntime.NodeInvocationID][]workflowruntime.AttemptSnapshot
	waits       map[workflowruntime.WaitID]workflowruntime.WaitSnapshot
	sets        map[string]values.ValueSet
	events      []workflowruntime.Event
	plan        workflowruntime.RecoveryPlan
	control     map[workflowruntime.ControlDecisionID]workflowruntime.ControlDecisionSnapshot
	intent      *workflowruntime.TerminalIntentSnapshot
	replay      *workflowruntime.ReplayProvenance
	pins        map[workflowruntime.NodeInvocationID]workflowruntime.PinBinding
	resource    workflowruntime.SchedulerResourceState
	activations []ActivationFireAttempt
	start       *hoststate.StartSnapshot
}

func (f *diagnosticFixture) LoadRun(context.Context, workflowruntime.RunID) (workflowruntime.RunSnapshot, error) {
	if f.runErr != nil {
		return workflowruntime.RunSnapshot{}, f.runErr
	}
	return f.run, nil
}
func (f *diagnosticFixture) ListRunInvocations(context.Context, workflowruntime.RunID) ([]workflowruntime.NodeInvocationSnapshot, error) {
	return append([]workflowruntime.NodeInvocationSnapshot(nil), f.nodes...), nil
}
func (f *diagnosticFixture) ListAttempts(_ context.Context, id workflowruntime.NodeInvocationID) ([]workflowruntime.AttemptSnapshot, error) {
	return append([]workflowruntime.AttemptSnapshot(nil), f.attempts[id]...), nil
}
func (f *diagnosticFixture) LoadWait(_ context.Context, id workflowruntime.WaitID) (workflowruntime.WaitSnapshot, error) {
	result, exists := f.waits[id]
	if !exists {
		return workflowruntime.WaitSnapshot{}, workflowruntime.ErrNotFound
	}
	return result, nil
}
func (f *diagnosticFixture) LoadValues(_ context.Context, ref values.ValueSetRef) (values.ValueSet, error) {
	result, exists := f.sets[ref.ID]
	if !exists {
		return nil, workflowruntime.ErrNotFound
	}
	return result, nil
}
func (f *diagnosticFixture) ListEvents(_ context.Context, query workflowruntime.EventQuery) ([]workflowruntime.Event, error) {
	result := append([]workflowruntime.Event(nil), f.events...)
	if query.Limit > 0 && len(result) > query.Limit {
		result = result[:query.Limit]
	}
	return result, nil
}
func (f *diagnosticFixture) Recovery(context.Context, workflowruntime.RecoveryQuery) (workflowruntime.RecoverySnapshot, error) {
	return workflowruntime.RecoverySnapshot{}, nil
}
func (f *diagnosticFixture) LoadRecoveryPlan(context.Context, workflowruntime.RunSnapshot) (workflowruntime.RecoveryPlan, error) {
	return f.plan, nil
}
func (f *diagnosticFixture) LoadControlDecision(_ context.Context, id workflowruntime.ControlDecisionID) (workflowruntime.ControlDecisionSnapshot, error) {
	result, exists := f.control[id]
	if !exists {
		return workflowruntime.ControlDecisionSnapshot{}, workflowruntime.ErrNotFound
	}
	return result, nil
}
func (f *diagnosticFixture) LoadTerminalIntent(context.Context, workflowruntime.RunID) (workflowruntime.TerminalIntentSnapshot, error) {
	if f.intent == nil {
		return workflowruntime.TerminalIntentSnapshot{}, workflowruntime.ErrNotFound
	}
	return *f.intent, nil
}
func (f *diagnosticFixture) LoadReplayProvenance(context.Context, workflowruntime.RunID) (workflowruntime.ReplayProvenance, error) {
	if f.replay == nil {
		return workflowruntime.ReplayProvenance{}, workflowruntime.ErrNotFound
	}
	return *f.replay, nil
}
func (f *diagnosticFixture) LoadPin(_ context.Context, id workflowruntime.NodeInvocationID) (workflowruntime.PinBinding, error) {
	result, exists := f.pins[id]
	if !exists {
		return workflowruntime.PinBinding{}, workflowruntime.ErrNotFound
	}
	return result, nil
}
func (f *diagnosticFixture) InspectSchedulerResources(context.Context, workflowruntime.SchedulerResourceQuery) (workflowruntime.SchedulerResourceState, error) {
	return f.resource, nil
}
func (f *diagnosticFixture) ListRunActivationAttempts(context.Context, workflowruntime.RunID, int) ([]ActivationFireAttempt, bool, error) {
	return append([]ActivationFireAttempt(nil), f.activations...), false, nil
}
func (f *diagnosticFixture) LoadStart(context.Context, workflowruntime.RunID) (hoststate.StartSnapshot, error) {
	if f.start == nil {
		return hoststate.StartSnapshot{}, workflowruntime.ErrNotFound
	}
	return *f.start, nil
}

func TestSafeLocatorUsesSharedCredentialFailClosedProjection(t *testing.T) {
	tests := map[string]string{
		"https://user:password@example.test/workflow.yaml?token=secret#fragment": "https://example.test/workflow.yaml",
		"//user:password@example.test/workflow.yaml?token=secret#fragment":       "//example.test/workflow.yaml",
		"workflow.yaml?token=secret#fragment":                                    "workflow.yaml",
		"user:opaque-secret@example.test/path":                                   "user:<redacted-locator>",
		"SeCrEt://vault/private-reference":                                       values.RedactedMarker,
		"%zz-malformed-secret":                                                   "<invalid-locator>",
	}
	for input, want := range tests {
		if got := safeLocator(input); got != want {
			t.Fatalf("safeLocator(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestGraphDiagnosticsExplainPersistedWorkflowState(t *testing.T) {
	fixture := newDiagnosticFixture(t)
	service := Service{State: fixture, Plans: fixture, Control: fixture, Replay: fixture, Pins: fixture, Resources: fixture, Starts: fixture, Activations: fixture}
	masked, err := service.Inspect(t.Context(), Query{RunID: fixture.run.ID, Now: fixture.run.UpdatedAt})
	if err != nil {
		t.Fatalf("Inspect(masked): %v", err)
	}
	if masked.SchemaVersion != "1" || masked.Plan.Source == nil || masked.Plan.Source.StartLine != 1 || masked.Plan.Source.Locator != "file:///workspace/workflow.yaml" {
		t.Fatalf("plan diagnostics = %#v", masked.Plan)
	}
	if masked.StartPolicy == nil || !reflect.DeepEqual(masked.StartPolicy.Effects, graph.EffectSet{graph.EffectCompute}) || masked.StartPolicy.ExposureRef != "diagnostic-route" || !masked.StartPolicy.ConfirmationAdvised {
		t.Fatalf("start policy diagnostics = %#v", masked.StartPolicy)
	}
	byID := make(map[string]NodeDiagnostic, len(masked.Nodes))
	for _, node := range masked.Nodes {
		byID[node.ID.NodeID] = node
	}
	checks := map[string]string{
		"pending": "upstream_terminal", "skipped": workflowruntime.ReasonPredicateFalse,
		"failed": "node_failed", "timeout": "node_timed_out", "waiting": "wait_open",
		"blocked": workflowruntime.ReasonReadinessWaiting, "crashed": "node_crashed",
		"finally": "finalizer_pending", "memo": "succeeded_memoized", "pin": "succeeded_pinned", "replay": "succeeded_replayed",
	}
	for id, code := range checks {
		if got := byID[id].Explanation.Code; got != code {
			t.Errorf("%s explanation = %q, want %q", id, got, code)
		}
	}
	if byID["skipped"].Explanation.Message != values.RedactedMarker || !byID["skipped"].Explanation.Masked {
		t.Fatalf("masked skipped explanation = %#v", byID["skipped"].Explanation)
	}
	if byID["waiting"].Wait == nil || byID["waiting"].Wait.ID != "wait-one" || byID["waiting"].Wait.Resolution != nil {
		t.Fatalf("wait diagnostic = %#v", byID["waiting"].Wait)
	}
	if byID["pending"].Lease == nil || byID["pending"].Lease.Owner != values.RedactedMarker || !byID["pending"].Lease.Masked {
		t.Fatalf("masked lease owner = %#v", byID["pending"].Lease)
	}
	encoded, marshalErr := json.Marshal(masked)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	for _, credential := range []string{"lease-owner-secret", "lease-token-secret", "operator:private", "workflow.run", "trusted-operator"} {
		if strings.Contains(string(encoded), credential) {
			t.Fatalf("diagnostics serialized lease credential %q: %s", credential, encoded)
		}
	}
	if byID["waiting"].Wait.ID == "" || byID["waiting"].Wait.Payload == nil {
		t.Fatal("wait payload reference missing")
	}
	if byID["pin"].Pin == nil || byID["pin"].Pin.Source.NodeID != "memo" {
		t.Fatalf("pin diagnostic = %#v", byID["pin"].Pin)
	}
	foundDownstream := false
	for _, downstream := range byID["failed"].Downstream {
		foundDownstream = foundDownstream || downstream.NodeID == "pending" && reflect.DeepEqual(downstream.Effects, graph.EffectSet{graph.EffectCompute})
	}
	if !foundDownstream {
		t.Fatalf("failed downstream effects = %#v", byID["failed"].Downstream)
	}
	if len(masked.Control.Decisions) != 2 || masked.Control.TerminalIntent == nil || len(masked.Control.TerminalIntent.Finalizers) != 1 {
		t.Fatalf("control diagnostics = %#v", masked.Control)
	}
	if masked.Replay == nil || masked.Replay.SourceRunID != "source-run" {
		t.Fatalf("replay = %#v", masked.Replay)
	}
	if masked.Resources == nil || len(masked.Resources.Waiters) != 1 || byID["blocked"].Resources.Waiter == nil {
		t.Fatalf("resources = %#v", masked.Resources)
	}
	if len(masked.Activations) != 1 || !masked.Activations[0].ScheduledAt.Before(masked.Activations[0].FiredAt) {
		t.Fatalf("activations = %#v", masked.Activations)
	}
	if !masked.Capabilities.ActivationAttempts || containsString(masked.Omissions, "activation_attempts") {
		t.Fatalf("capabilities = %#v omissions=%v", masked.Capabilities, masked.Omissions)
	}
	secretFound := false
	for _, set := range masked.Values {
		if value, exists := set.Values["secret"]; exists {
			secretFound = value.Masked && value.Payload == values.RedactedMarker
		}
	}
	if !secretFound {
		t.Fatal("secret typed value was not safely rendered")
	}
	if len(masked.Events) == 0 || !masked.Events[0].Masked {
		t.Fatalf("events = %#v", masked.Events)
	}

	revealed, err := service.Inspect(t.Context(), Query{RunID: fixture.run.ID, Now: fixture.run.UpdatedAt, Display: values.DisplayPolicy{Private: values.PrivateDisplayReveal}})
	if err != nil {
		t.Fatalf("Inspect(revealed): %v", err)
	}
	byID = make(map[string]NodeDiagnostic, len(revealed.Nodes))
	for _, node := range revealed.Nodes {
		byID[node.ID.NodeID] = node
	}
	if byID["skipped"].Explanation.Message != "predicate evaluated false" {
		t.Fatalf("revealed skip = %#v", byID["skipped"].Explanation)
	}
	if byID["failed"].Attempts[0].Failure.Details["provider"] != "remote" {
		t.Fatalf("revealed failure = %#v", byID["failed"].Attempts[0].Failure)
	}
	if got := byID["failed"].Attempts[0].Executor.Target; got != "https://example.test/path" {
		t.Fatalf("sanitized executor target = %q", got)
	}

	// Returned maps and slices are isolated from persisted fixture state.
	changedNode := byID["failed"]
	changedNode.Definition.Effects[0] = graph.EffectDestructive
	revealed.Values[0].Roles[0] = "changed"
	again, err := service.Inspect(t.Context(), Query{RunID: fixture.run.ID, Now: fixture.run.UpdatedAt})
	if err != nil {
		t.Fatal(err)
	}
	againByID := make(map[string]NodeDiagnostic, len(again.Nodes))
	for _, node := range again.Nodes {
		againByID[node.ID.NodeID] = node
	}
	if reflect.DeepEqual(againByID["failed"].Definition.Effects, changedNode.Definition.Effects) || again.Values[0].Roles[0] == "changed" {
		t.Fatal("diagnostic result was not defensively owned")
	}
}

func TestPlanProjectionIncludesOnlyCanonicalGraphLayoutAndEdges(t *testing.T) {
	edgeSource := graph.SourceRef{Format: graph.SourceWorkflow, Locator: "workflow.yaml", StartLine: 22}
	plan := compile.ExecutionPlan{Graph: graph.Graph{
		Nodes: []graph.Node{
			{ID: "authored", Kind: "transform", Metadata: graph.Metadata{"position": map[string]any{"x": json.Number("125.5"), "y": -48}}, Retry: &graph.RetryPolicy{Attempts: 3, Backoff: graph.BackoffPolicy{Strategy: graph.BackoffExponential, InitialDelay: "1s", MaxDelay: "30s"}}},
			{ID: "invalid", Kind: "transform", Metadata: graph.Metadata{"position": map[string]any{"x": "125", "y": 48}, "private": "not-projected"}},
		},
		Edges: []graph.Edge{{From: "authored", To: "invalid", Kind: graph.EdgeData, Metadata: graph.Metadata{"private": "not-projected"}}},
	}, SourceMap: graph.SourceMap{Edges: map[string]graph.SourceRef{compile.EdgeSourceKey("authored", "invalid", graph.EdgeData): edgeSource}}}
	projected, truncated := projectPlan(plan, 10)
	if truncated {
		t.Fatal("single edge was truncated")
	}
	if len(projected.Nodes) != 2 || projected.Nodes[0].Position == nil || projected.Nodes[0].Position.X != 125.5 || projected.Nodes[0].Position.Y != -48 || projected.Nodes[1].Position != nil {
		t.Fatalf("positions = %#v", projected.Nodes)
	}
	if projected.Nodes[0].Retry == nil || projected.Nodes[0].Retry.Attempts != 3 || projected.Nodes[0].Retry.Strategy != graph.BackoffExponential {
		t.Fatalf("retry = %#v", projected.Nodes[0].Retry)
	}
	if len(projected.Edges) != 1 || projected.Edges[0].From != "authored" || projected.Edges[0].To != "invalid" || projected.Edges[0].Kind != graph.EdgeData || projected.Edges[0].Source == nil || projected.Edges[0].Source.StartLine != 22 {
		t.Fatalf("edges = %#v", projected.Edges)
	}
	encoded, err := json.Marshal(projected)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "not-projected") {
		t.Fatalf("projection leaked arbitrary graph metadata: %s", encoded)
	}
}

func TestPlanProjectionBoundsEdgesToVisibleNodesAndLimit(t *testing.T) {
	plan := compile.ExecutionPlan{Graph: graph.Graph{
		Nodes: []graph.Node{{ID: "a", Kind: "transform"}, {ID: "b", Kind: "transform"}, {ID: "c", Kind: "transform"}, {ID: "d", Kind: "transform"}},
		Edges: []graph.Edge{
			{From: "a", To: "b", Kind: graph.EdgeControl},
			{From: "a", To: "b", Kind: graph.EdgeData},
			{From: "a", To: "c", Kind: graph.EdgeControl},
			{From: "b", To: "c", Kind: graph.EdgeData},
			{From: "b", To: "d", Kind: graph.EdgeControl},
		},
	}}
	projected, truncated := projectPlan(plan, 3)
	if !truncated || len(projected.Edges) != 3 {
		t.Fatalf("bounded edges = %#v, truncated=%v", projected.Edges, truncated)
	}
	for _, edge := range projected.Edges {
		if edge.From == "d" || edge.To == "d" {
			t.Fatalf("edge references omitted node: %#v", edge)
		}
	}
	omittedOnly := compile.ExecutionPlan{Graph: graph.Graph{
		Nodes: []graph.Node{{ID: "a", Kind: "transform"}, {ID: "b", Kind: "transform"}},
		Edges: []graph.Edge{{From: "a", To: "b", Kind: graph.EdgeControl}},
	}}
	projected, truncated = projectPlan(omittedOnly, 1)
	if !truncated || len(projected.Edges) != 0 {
		t.Fatalf("endpoint omission = %#v, truncated=%v", projected.Edges, truncated)
	}
}

func TestEdgeValueFlowUsesOnlyRenderedBoundedValueSets(t *testing.T) {
	ref := values.ValueSetRef{ID: "visible-values", Digest: values.SHA256Digest([]byte("visible"))}
	omitted := values.ValueSetRef{ID: "omitted-values", Digest: values.SHA256Digest([]byte("omitted"))}
	nodes := []NodeDiagnostic{
		{ID: workflowruntime.NodeInvocationID{RunID: "run", NodeID: "source"}, Outputs: &ref},
		{ID: workflowruntime.NodeInvocationID{RunID: "run", NodeID: "target"}, Inputs: &omitted},
	}
	rendered := []ValueSetDiagnostic{{Ref: ref, Values: values.RenderedValueSet{"secret": {Payload: values.RedactedMarker, Masked: true}}}}
	edges := projectEdgeValueFlows([]PlanEdgeDiagnostic{{From: "source", To: "target", Kind: graph.EdgeData}}, nodes, rendered)
	if len(edges) != 1 || edges[0].ValueFlow == nil || len(edges[0].ValueFlow.SourceOutputs) != 1 || len(edges[0].ValueFlow.TargetInputs) != 0 || !edges[0].ValueFlow.ValuesOmitted {
		t.Fatalf("edge value flow = %#v", edges)
	}
	encoded, err := json.Marshal(edges)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), values.RedactedMarker) || strings.Contains(string(encoded), "secret") || strings.Contains(string(encoded), omitted.ID) {
		t.Fatalf("edge association duplicated or exposed rendered payloads: %s", encoded)
	}
}

func TestStartPolicyProjectionIsNarrowAndRejectsUnsafeFacts(t *testing.T) {
	record := hoststate.StartRecord{
		Identity: hoststate.IdentityBinding{Principal: "operator:private", Trust: "trusted-operator", Grants: []string{"workflow.run"}, Extension: map[string]string{"exposure_ref": "reviewers"}},
		Facts:    hoststate.PolicyFacts{Effects: graph.EffectSet{graph.EffectRead, graph.EffectMutate}, RequiredCapabilities: []string{"network"}, BlastRadius: map[string]int{"mutate": 2}, NodeCount: 3, DryRunAvailable: true, ConfirmationAdvised: true},
		Decision: hoststate.PolicyDecision{Outcome: hoststate.PolicyConfirm},
	}
	projected, err := projectStartPolicy(record)
	if err != nil {
		t.Fatal(err)
	}
	if projected.ExposureRef != "reviewers" || projected.Decision != hoststate.PolicyConfirm || len(projected.RequiredCapabilities) != 1 || projected.BlastRadius["mutate"] != 2 {
		t.Fatalf("start policy = %#v", projected)
	}
	encoded, err := json.Marshal(projected)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"operator:private", "workflow.run", "trusted-operator"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("start policy leaked %q: %s", forbidden, encoded)
		}
	}
	record.Facts.RequiredCapabilities = []string{"Bearer private-token"}
	if _, err := projectStartPolicy(record); err == nil {
		t.Fatal("unsafe capability was accepted")
	}
}

func TestGraphDiagnosticsBoundsOmissionsAndCorruption(t *testing.T) {
	fixture := newDiagnosticFixture(t)
	service := Service{State: fixture, Plans: fixture}
	result, err := service.Inspect(t.Context(), Query{RunID: fixture.run.ID, Now: fixture.run.UpdatedAt, NodeLimit: 5, AttemptLimit: 1, EventLimit: 1, ValueLimit: 1})
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if len(result.Nodes) != 5 || !result.Truncated.Nodes || len(result.Events) != 1 || !result.Truncated.Events || len(result.Values) != 1 || !result.Truncated.Values {
		t.Fatalf("bounded result = nodes=%d events=%d values=%d truncated=%#v", len(result.Nodes), len(result.Events), len(result.Values), result.Truncated)
	}
	for _, capability := range []string{"control_decisions", "replay_provenance", "pin_bindings", "concurrency_state", "start_binding", "activation_attempts"} {
		if !containsString(result.Omissions, capability) {
			t.Errorf("omissions missing %q: %v", capability, result.Omissions)
		}
	}
	if _, err := service.Inspect(t.Context(), Query{RunID: fixture.run.ID, Now: fixture.run.UpdatedAt, EventLimit: maximumEventLimit + 1}); !errors.Is(err, ErrInvalidGraphQuery) {
		t.Fatalf("oversized query error = %v", err)
	}
	var typedNilFixture *diagnosticFixture
	if _, err := (Service{State: typedNilFixture, Plans: typedNilFixture}).Inspect(t.Context(), Query{RunID: fixture.run.ID, Now: fixture.run.UpdatedAt}); !errors.Is(err, ErrInvalidGraphQuery) {
		t.Fatalf("typed-nil service error = %v", err)
	}

	corruptFixture := newDiagnosticFixture(t)
	corruptFixture.run.Inputs = &values.ValueSetRef{ID: "run-values", Digest: values.SHA256Digest([]byte("forged"))}
	if _, err := (Service{State: corruptFixture, Plans: corruptFixture}).Inspect(t.Context(), Query{RunID: corruptFixture.run.ID, Now: corruptFixture.run.UpdatedAt}); !errors.Is(err, ErrCorruptRunState) {
		t.Fatalf("corrupt value error = %v", err)
	}
	missingWait := newDiagnosticFixture(t)
	delete(missingWait.waits, "wait-one")
	if _, err := (Service{State: missingWait, Plans: missingWait}).Inspect(t.Context(), Query{RunID: missingWait.run.ID, Now: missingWait.run.UpdatedAt}); !errors.Is(err, ErrCorruptRunState) {
		t.Fatalf("missing referenced wait error = %v", err)
	}
	missingRun := newDiagnosticFixture(t)
	missingRun.runErr = workflowruntime.ErrNotFound
	if _, err := (Service{State: missingRun, Plans: missingRun}).Inspect(t.Context(), Query{RunID: missingRun.run.ID, Now: missingRun.run.UpdatedAt}); !errors.Is(err, workflowruntime.ErrNotFound) || errors.Is(err, ErrCorruptRunState) {
		t.Fatalf("missing run error = %v", err)
	}
	malformedSkip := newDiagnosticFixture(t)
	malformedSkip.events[0].Attributes["explanation"] = "{"
	if _, err := (Service{State: malformedSkip, Plans: malformedSkip}).Inspect(t.Context(), Query{RunID: malformedSkip.run.ID, Now: malformedSkip.run.UpdatedAt}); !errors.Is(err, ErrCorruptRunState) {
		t.Fatalf("malformed skip explanation error = %v", err)
	}
	sensitiveActivation := newDiagnosticFixture(t)
	sensitiveActivation.activations[0].FireID = "token=credential"
	if _, err := (Service{State: sensitiveActivation, Plans: sensitiveActivation, Activations: sensitiveActivation}).Inspect(t.Context(), Query{RunID: sensitiveActivation.run.ID, Now: sensitiveActivation.run.UpdatedAt}); !errors.Is(err, ErrCorruptRunState) {
		t.Fatalf("credential-shaped activation error = %v", err)
	}
}

func newDiagnosticFixture(t *testing.T) *diagnosticFixture {
	t.Helper()
	base := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	planDigest := values.SHA256Digest([]byte("diagnostic-plan"))
	graphDigest := values.SHA256Digest([]byte("diagnostic-graph"))
	ref := workflowruntime.PlanRef{ID: "diagnostic-workflow", Version: "1.0.0", Digest: planDigest, SchemaVersion: compile.ExecutionPlanSchemaVersion}
	public, err := values.NewInline("visible", values.Metadata{Producer: values.Producer{Kind: "fixture", Reference: "public"}, MediaType: "application/json", Redaction: values.RedactionPublic, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	secretRef, err := values.ParseSecretRef("secret://project/diagnostic")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := values.NewSecretRef(secretRef, values.Metadata{Producer: values.Producer{Kind: "fixture", Reference: "secret"}, MediaType: "application/json", Redaction: values.RedactionSecret, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	set := values.ValueSet{"result": public, "secret": secret}
	setRef, err := values.NewValueSetRef("run-values", set)
	if err != nil {
		t.Fatal(err)
	}
	errorRef, err := values.NewValueSetRef("error-values", values.ValueSet{"error": public})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := workflowwait.NewSchemaRef(graph.Schema{"type": "object"})
	if err != nil {
		t.Fatal(err)
	}
	nodeDefs := []graph.Node{
		{ID: "failed", Kind: "fixture", KindVersion: "v1", Effects: graph.EffectSet{graph.EffectRead}},
		{ID: "pending", Kind: "fixture", KindVersion: "v1", Needs: []graph.Need{{Node: "failed"}}, Effects: graph.EffectSet{graph.EffectCompute}},
		{ID: "skipped", Kind: "fixture", KindVersion: "v1"},
		{ID: "timeout", Kind: "fixture", KindVersion: "v1"},
		{ID: "waiting", Kind: "fixture", KindVersion: "v1"},
		{ID: "blocked", Kind: "fixture", KindVersion: "v1"},
		{ID: "crashed", Kind: "fixture", KindVersion: "v1"},
		{ID: "route", Kind: "fixture", KindVersion: "v1", Catch: []graph.CatchRule{{Errors: []string{"failed"}, Targets: []string{"skipped"}}}, Switch: &graph.SwitchSpec{Default: []string{"pending"}}},
		{ID: "finally", Kind: "fixture", KindVersion: "v1", Finally: &graph.FinallySpec{}},
		{ID: "memo", Kind: "fixture", KindVersion: "v1", Effects: graph.EffectSet{graph.EffectCompute}},
		{ID: "pin", Kind: "fixture", KindVersion: "v1", Effects: graph.EffectSet{graph.EffectRead}},
		{ID: "replay", Kind: "fixture", KindVersion: "v1"},
	}
	sourceMap := graph.SourceMap{Graph: &graph.SourceRef{Format: graph.SourceWorkflow, Locator: "file:///workspace/workflow.yaml?credential=hidden", StartLine: 1}, Nodes: map[string]graph.SourceRef{}}
	visibility := compile.ValueVisibilityPlan{Nodes: map[string]compile.ValueScope{}}
	for index := range nodeDefs {
		source := graph.SourceRef{Format: graph.SourceWorkflow, Locator: "file:///workspace/workflow.yaml", StartLine: index + 10, StartColumn: 3, Path: []string{"nodes", nodeDefs[index].ID}}
		nodeDefs[index].Source = &source
		sourceMap.Nodes[nodeDefs[index].ID] = source
		visibility.Nodes[nodeDefs[index].ID] = compile.ValueScope{}
	}
	plan := compile.ExecutionPlan{SchemaVersion: compile.ExecutionPlanSchemaVersion, ID: ref.ID, Digest: ref.Digest,
		Definition:    graph.DefinitionRef{Authority: "project", Kind: "file", ID: ref.ID, Locator: "file:///workspace/workflow.yaml?token=hidden", Version: ref.Version, Digest: values.SHA256Digest([]byte("source"))},
		Provenance:    graph.Provenance{Authority: "project", Origin: "file", Locator: "file:///workspace/workflow.yaml?token=hidden", Digest: values.SHA256Digest([]byte("source"))},
		SourceDigests: []compile.SourceDigest{{Format: graph.SourceWorkflow, Digest: values.SHA256Digest([]byte("source"))}},
		Graph:         graph.Graph{ID: ref.ID, Version: ref.Version, Digest: graphDigest, Nodes: nodeDefs, SourceMap: sourceMap, Activations: []graph.ActivationDeclaration{{ID: "nightly", Kind: "cron", Source: &graph.SourceRef{Format: graph.SourceWorkflow, Locator: "file:///workspace/workflow.yaml", StartLine: 90}}}}, SourceMap: sourceMap}
	fixture := &diagnosticFixture{run: workflowruntime.RunSnapshot{ID: "run-diagnostic", Plan: ref, Status: workflowruntime.RunFailed, Inputs: &setRef, Generation: 4, CreatedAt: base, UpdatedAt: base.Add(time.Hour)}, attempts: map[workflowruntime.NodeInvocationID][]workflowruntime.AttemptSnapshot{}, waits: map[workflowruntime.WaitID]workflowruntime.WaitSnapshot{}, sets: map[string]values.ValueSet{"run-values": set, "error-values": {"error": public}}, plan: workflowruntime.RecoveryPlan{Ref: ref, Plan: plan, Visibility: visibility}, control: map[workflowruntime.ControlDecisionID]workflowruntime.ControlDecisionSnapshot{}, pins: map[workflowruntime.NodeInvocationID]workflowruntime.PinBinding{}}
	node := func(id string, status workflowruntime.NodeStatus) workflowruntime.NodeInvocationSnapshot {
		return workflowruntime.NodeInvocationSnapshot{ID: workflowruntime.NodeInvocationID{RunID: fixture.run.ID, NodeID: id}, Status: status, Generation: 1, CreatedAt: base, UpdatedAt: base.Add(10 * time.Minute)}
	}
	failed := node("failed", workflowruntime.NodeFailed)
	failed.LatestAttempt = 1
	failed.Origin = workflowruntime.OriginExecuted
	failed.Inputs = &errorRef
	timed := node("timeout", workflowruntime.NodeTimedOut)
	timed.LatestAttempt = 1
	timed.Origin = workflowruntime.OriginExecuted
	crashed := node("crashed", workflowruntime.NodeCrashed)
	crashed.LatestAttempt = 1
	crashed.Origin = workflowruntime.OriginExecuted
	waiting := node("waiting", workflowruntime.NodeWaiting)
	waiting.LatestAttempt = 1
	waiting.Wait = &workflowruntime.WaitRef{ID: "wait-one"}
	blocked := node("blocked", workflowruntime.NodeBlocked)
	blocked.Blocked = &workflowruntime.BlockedReason{Code: workflowruntime.ReasonReadinessWaiting, Message: "capacity unavailable", Dependencies: []workflowruntime.NodeInvocationID{failed.ID}, Details: map[string]string{"terminal": "1"}}
	memo := node("memo", workflowruntime.NodeSucceeded)
	memo.Origin = workflowruntime.OriginMemoized
	memo.Outputs = &setRef
	memo.MemoKeyDigest = values.SHA256Digest([]byte("memo-key"))
	pin := node("pin", workflowruntime.NodeSucceeded)
	pin.Origin = workflowruntime.OriginPinned
	pin.Outputs = &setRef
	replayNode := node("replay", workflowruntime.NodeSucceeded)
	replayNode.Origin = workflowruntime.OriginReplayed
	pending := node("pending", workflowruntime.NodePending)
	pending.ClaimGeneration = 1
	pending.Lease = &workflowruntime.ClaimLease{Owner: "Bearer lease-owner-secret", Token: "lease-token-secret", Generation: 1, ExpiresAt: base.Add(time.Hour)}
	fixture.nodes = []workflowruntime.NodeInvocationSnapshot{failed, pending, node("skipped", workflowruntime.NodeSkipped), timed, waiting, blocked, crashed, node("route", workflowruntime.NodeSucceeded), node("finally", workflowruntime.NodePending), memo, pin, replayNode}
	makeAttempt := func(id workflowruntime.NodeInvocationID, status workflowruntime.NodeStatus, code string) workflowruntime.AttemptSnapshot {
		failure := &workflowruntime.Failure{Code: code, Message: "durable failure", Retryable: status == workflowruntime.NodeCrashed, Details: map[string]string{"provider": "remote"}}
		return workflowruntime.AttemptSnapshot{ID: workflowruntime.AttemptID{Invocation: id, Number: 1}, Status: status, Executor: workflowruntime.ExecutorMetadata{Kind: "fixture", Version: "v1", Target: "https://user:password@example.test/path?token=hidden", Attributes: map[string]string{"operation": "provider"}}, Failure: failure, StartedAt: base.Add(time.Minute), FinishedAt: base.Add(2 * time.Minute), Generation: 1, CreatedAt: base.Add(time.Minute), UpdatedAt: base.Add(2 * time.Minute)}
	}
	fixture.attempts[failed.ID] = []workflowruntime.AttemptSnapshot{makeAttempt(failed.ID, workflowruntime.NodeFailed, "provider_failed")}
	fixture.attempts[timed.ID] = []workflowruntime.AttemptSnapshot{makeAttempt(timed.ID, workflowruntime.NodeTimedOut, "execution_timeout")}
	fixture.attempts[crashed.ID] = []workflowruntime.AttemptSnapshot{makeAttempt(crashed.ID, workflowruntime.NodeCrashed, "worker_crashed")}
	fixture.waits["wait-one"] = workflowruntime.WaitSnapshot{Ref: workflowruntime.WaitRef{ID: "wait-one"}, Invocation: waiting.ID, Record: workflowwait.Record{Kind: workflowwait.KindMessage, Correlation: "secret-correlation", Payload: &setRef, ResumeSchema: schema, ResumeTokenDigest: values.SHA256Digest([]byte("token")), ResumeURL: "https://example.test/resume", Visibility: workflowwait.VisibilityPrivate, Authority: workflowwait.ResponderAuthority{Kind: "policy", Reference: "credential-principal"}, WakeSource: workflowwait.WakeMessage, Status: workflowwait.StatusOpen}, Generation: 1, CreatedAt: base, UpdatedAt: base}
	reason := workflowruntime.BlockedReason{Code: workflowruntime.ReasonPredicateFalse, Message: "predicate evaluated false", Details: map[string]string{"expression": "if"}}
	encodedReason, _ := json.Marshal(reason)
	fixture.events = []workflowruntime.Event{
		{Sequence: 1, RunID: fixture.run.ID, Invocation: &fixture.nodes[2].ID, Type: workflowruntime.EventNodeStatusChanged, OccurredAt: base.Add(3 * time.Minute), Attributes: map[string]string{"from_status": "pending", "to_status": "skipped", "explanation": string(encodedReason)}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
		{Sequence: 2, RunID: fixture.run.ID, Invocation: &failed.ID, Attempt: &fixture.attempts[failed.ID][0].ID, Type: workflowruntime.EventNodeAttemptFinished, OccurredAt: base.Add(4 * time.Minute), Attributes: map[string]string{"failure_code": "provider_failed"}, Values: &errorRef, Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
	}
	routeID := workflowruntime.NodeInvocationID{RunID: fixture.run.ID, NodeID: "route"}
	selected := 0
	fixture.control[workflowruntime.ControlDecisionID{Source: routeID, Kind: workflowruntime.ControlSwitch}] = workflowruntime.ControlDecisionSnapshot{ID: workflowruntime.ControlDecisionID{Source: routeID, Kind: workflowruntime.ControlSwitch}, Outcome: workflowruntime.ControlDefault, Targets: []workflowruntime.NodeInvocationID{{RunID: fixture.run.ID, NodeID: "pending"}}, SourceGeneration: 1, Generation: 1, CreatedAt: base.Add(5 * time.Minute)}
	fixture.control[workflowruntime.ControlDecisionID{Source: routeID, Kind: workflowruntime.ControlCatch}] = workflowruntime.ControlDecisionSnapshot{ID: workflowruntime.ControlDecisionID{Source: routeID, Kind: workflowruntime.ControlCatch}, Outcome: workflowruntime.ControlSelected, RuleIndex: &selected, Targets: []workflowruntime.NodeInvocationID{{RunID: fixture.run.ID, NodeID: "skipped"}}, BindAs: "error", Error: &errorRef, SourceGeneration: 1, Generation: 1, CreatedAt: base.Add(6 * time.Minute)}
	fixture.intent = &workflowruntime.TerminalIntentSnapshot{RunID: fixture.run.ID, IntendedStatus: workflowruntime.RunFailed, Reason: &workflowruntime.Failure{Code: "workflow_failed", Message: "workflow failed"}, Error: &errorRef, IdempotencyKey: "terminal-one", Finalizers: []workflowruntime.FinalizerScope{{Invocation: workflowruntime.NodeInvocationID{RunID: fixture.run.ID, NodeID: "finally"}, Scope: []workflowruntime.NodeInvocationID{failed.ID}, Order: 0}}, Status: workflowruntime.TerminalIntentCompleted, Generation: 2, CreatedAt: base.Add(7 * time.Minute), UpdatedAt: base.Add(9 * time.Minute), CompletedAt: base.Add(9 * time.Minute)}
	fixture.replay = &workflowruntime.ReplayProvenance{RunID: fixture.run.ID, SourceRunID: "source-run", FromNodeID: "failed", PlanDigest: planDigest, IdempotencyKey: "replay-one", Policy: []workflowruntime.ReplayNodePolicy{{Invocation: workflowruntime.NodeInvocationID{RunID: "source-run", NodeID: "failed"}, Decision: workflowruntime.RepeatPolicyDecision{Allow: true, Code: "safe", Reason: "exact replay"}}}, CreatedAt: base.Add(10 * time.Minute)}
	fixture.pins[pin.ID] = workflowruntime.PinBinding{Target: pin.ID, PlanDigest: planDigest, Outputs: setRef, OutputSchemaDigest: values.SHA256Digest([]byte("schema")), Source: memo.ID, SourcePlanDigest: planDigest, SourceOrigin: workflowruntime.OriginMemoized, Authority: workflowruntime.ReuseAuthority{Principal: "operator"}, Policy: workflowruntime.ReusePolicyDecision{Allow: true, Code: "pin_allowed", Reason: "operator approved"}, BoundAt: base.Add(11 * time.Minute)}
	worker := workflowruntime.SchedulerResourceID{Kind: workflowruntime.SchedulerResourceWorker, Name: "global"}
	fixture.resource = workflowruntime.SchedulerResourceState{Waiters: []workflowruntime.SchedulerResourceWaiter{{Invocation: blocked.ID, Requirements: []workflowruntime.SchedulerResourceRequirement{{Resource: worker, Units: 1, Limit: 1}}, Blocked: []workflowruntime.SchedulerResourceID{worker}, EnqueuedAt: base, UpdatedAt: base}}}
	fixture.activations = []ActivationFireAttempt{{FireID: "fire-nightly", ActivationID: "nightly", RunID: fixture.run.ID, ScheduledAt: base.Add(-time.Minute), FiredAt: base, Attempt: 1, Status: "succeeded", Source: "scheduler"}}
	identity := hoststate.IdentityBinding{
		Principal: "operator:private", SourceAuthority: "desktop", Trust: "trusted-operator", Grants: []string{"workflow.run"},
		RunScope:  hoststate.RunScope{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeProject, ID: "diagnostic-project"},
		Extension: map[string]string{"exposure_ref": "diagnostic-route"},
	}
	facts := hoststate.PolicyFacts{
		Operation: "start", RunID: fixture.run.ID, Plan: ref, Identity: identity, RunScope: identity.RunScope,
		Effects: graph.EffectSet{graph.EffectCompute}, NodeCount: len(nodeDefs), BlastRadius: map[string]int{"compute": 2},
		DryRunAvailable: true, ConfirmationAdvised: true,
	}
	record := hoststate.StartRecord{
		Run:  workflowruntime.BoundRun{ID: fixture.run.ID, Plan: ref, InputsRef: setRef, CreatedAt: base, Provenance: plan.Provenance},
		Plan: plan, Requested: plan.Definition, StartKey: "diagnostic-start", RequestDigest: values.SHA256Digest([]byte("diagnostic-start-request")),
		CallerInputHash: values.SHA256Digest([]byte("diagnostic-start-input")), Identity: identity, Facts: facts,
		Decision:   hoststate.PolicyDecision{ID: "diagnostic-decision", RunID: fixture.run.ID, Operation: "start", Outcome: hoststate.PolicyAllow, Reason: "allowed", DecidedAt: base},
		RecordedAt: base,
	}
	fixture.start = &hoststate.StartSnapshot{Record: record, Phase: hoststate.StartRunning, Generation: 1, UpdatedAt: base}
	return fixture
}
