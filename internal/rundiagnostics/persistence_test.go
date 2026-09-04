package rundiagnostics

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/go-workflow/compile"
	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
)

type fixedPlanSource struct{ plan workflowruntime.RecoveryPlan }

var (
	_ StateReader        = (*persistence.WorkflowStateStore)(nil)
	_ BoundedStateReader = (*persistence.WorkflowStateStore)(nil)
)

func (s fixedPlanSource) LoadRecoveryPlan(context.Context, workflowruntime.RunSnapshot) (workflowruntime.RecoveryPlan, error) {
	return s.plan, nil
}

func TestGraphDiagnosticsSQLiteReopenSourceRedactionAndCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "diagnostics.db")
	database, openErr := persistence.Open(path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	state, stateErr := persistence.NewWorkflowStateStore(database)
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	base := time.Date(2026, 8, 24, 15, 0, 0, 0, time.UTC)
	planDigest := values.SHA256Digest([]byte("sqlite-diagnostics-plan"))
	ref := workflowruntime.PlanRef{ID: "sqlite-diagnostics", Version: "v1", Digest: planDigest, SchemaVersion: compile.ExecutionPlanSchemaVersion}
	if recordPlanErr := state.RecordPlan(t.Context(), ref); recordPlanErr != nil {
		t.Fatal(recordPlanErr)
	}
	run, outcome, createRunErr := state.CreateRun(t.Context(), workflowruntime.CreateRunRequest{ID: "sqlite-diagnostics-run", Plan: ref, Status: workflowruntime.RunPending, StartIdempotencyKey: "sqlite-diagnostics-start", CreatedAt: base})
	if createRunErr != nil || outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("CreateRun = %#v, %q, %v", run, outcome, createRunErr)
	}
	publicValue, valueErr := values.NewInline("diagnostic", values.Metadata{Producer: values.Producer{Kind: "fixture", Reference: "sqlite"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if valueErr != nil {
		t.Fatal(valueErr)
	}
	valueRef, saveValuesErr := state.SaveValues(t.Context(), workflowruntime.SaveValuesRequest{Owner: workflowruntime.ValueOwner{Kind: "run_inputs", RunID: run.ID}, Values: values.ValueSet{"input": publicValue}})
	if saveValuesErr != nil {
		t.Fatal(saveValuesErr)
	}
	for _, nodeID := range []string{"pending", "skipped", "blocked"} {
		if _, createNodeErr := state.CreateNodeInvocation(t.Context(), workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{ID: workflowruntime.NodeInvocationID{RunID: run.ID, NodeID: nodeID}, Status: workflowruntime.NodePending, CreatedAt: base, UpdatedAt: base}}); createNodeErr != nil {
			t.Fatal(createNodeErr)
		}
	}
	skippedReason := &workflowruntime.BlockedReason{Code: workflowruntime.ReasonPredicateFalse, Message: "persisted predicate false", Details: map[string]string{"expression": "private"}}
	if _, transitionErr := state.TransitionNode(t.Context(), workflowruntime.NodeTransitionRequest{InvocationID: workflowruntime.NodeInvocationID{RunID: run.ID, NodeID: "skipped"}, ExpectedGeneration: 1, To: workflowruntime.NodeSkipped, Explanation: skippedReason, At: base.Add(time.Second)}); transitionErr != nil {
		t.Fatal(transitionErr)
	}
	blockedReason := &workflowruntime.BlockedReason{Code: workflowruntime.ReasonReadinessWaiting, Message: "persisted dependency pending", Dependencies: []workflowruntime.NodeInvocationID{{RunID: run.ID, NodeID: "pending"}}}
	if _, transitionErr := state.TransitionNode(t.Context(), workflowruntime.NodeTransitionRequest{InvocationID: workflowruntime.NodeInvocationID{RunID: run.ID, NodeID: "blocked"}, ExpectedGeneration: 1, To: workflowruntime.NodeBlocked, Blocked: blockedReason, At: base.Add(2 * time.Second)}); transitionErr != nil {
		t.Fatal(transitionErr)
	}
	if _, appendEventErr := state.AppendEvent(t.Context(), workflowruntime.AppendEventRequest{RunID: run.ID, Type: "adapter.observation", OccurredAt: base.Add(3 * time.Second), Attributes: map[string]string{"credential": "must-not-render"}, Values: &valueRef, Redaction: values.RedactionSecret, Retention: values.RetentionRun}); appendEventErr != nil {
		t.Fatal(appendEventErr)
	}
	if closeErr := database.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}

	reopened, reopenErr := persistence.Open(path)
	if reopenErr != nil {
		t.Fatal(reopenErr)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	reopenedState, reopenedStateErr := persistence.NewWorkflowStateStore(reopened)
	if reopenedStateErr != nil {
		t.Fatal(reopenedStateErr)
	}
	source := graph.SourceRef{Format: graph.SourceWorkflow, Locator: "file:///project/workflow.yaml", StartLine: 8, StartColumn: 5, Path: []string{"nodes", "pending"}}
	definitions := []graph.Node{{ID: "pending", Kind: "transform", KindVersion: "v1", Source: &source}, {ID: "skipped", Kind: "transform", KindVersion: "v1", Source: &source}, {ID: "blocked", Kind: "transform", KindVersion: "v1", Source: &source}}
	visibility := compile.ValueVisibilityPlan{Nodes: map[string]compile.ValueScope{"pending": {}, "skipped": {}, "blocked": {}}}
	plan := compile.ExecutionPlan{SchemaVersion: ref.SchemaVersion, ID: ref.ID, Digest: ref.Digest, Definition: graph.DefinitionRef{ID: ref.ID, Version: ref.Version, Digest: values.SHA256Digest([]byte("sqlite-source"))}, Graph: graph.Graph{ID: ref.ID, Version: ref.Version, Digest: values.SHA256Digest([]byte("sqlite-graph")), Nodes: definitions}, SourceMap: graph.SourceMap{Graph: &source, Nodes: map[string]graph.SourceRef{"pending": source, "skipped": source, "blocked": source}}}
	service := Service{State: reopenedState, Plans: fixedPlanSource{plan: workflowruntime.RecoveryPlan{Ref: ref, Plan: plan, Visibility: visibility}}}
	result, err := service.Inspect(t.Context(), Query{RunID: run.ID, Now: base.Add(time.Hour), Display: values.DisplayPolicy{Private: values.PrivateDisplayReveal}})
	if err != nil {
		t.Fatalf("Inspect(reopened): %v", err)
	}
	if result.Plan.Source == nil || result.Plan.Source.StartLine != 8 || len(result.Nodes) != 3 {
		t.Fatalf("reopened diagnostic = %#v", result)
	}
	if result.Events[len(result.Events)-1].Attributes["credential"] != values.RedactedMarker || !result.Events[len(result.Events)-1].Masked {
		t.Fatalf("secret event = %#v", result.Events[len(result.Events)-1])
	}
	var foundSkipped, foundBlocked bool
	for _, node := range result.Nodes {
		foundSkipped = foundSkipped || node.ID.NodeID == "skipped" && node.Explanation.Code == workflowruntime.ReasonPredicateFalse
		foundBlocked = foundBlocked || node.ID.NodeID == "blocked" && node.Explanation.Code == workflowruntime.ReasonReadinessWaiting
	}
	if !foundSkipped || !foundBlocked {
		t.Fatalf("persisted explanations = %#v", result.Nodes)
	}
	bounded, err := service.Inspect(t.Context(), Query{RunID: run.ID, Now: base.Add(time.Hour), NodeLimit: 2})
	if err != nil || len(bounded.Nodes) != 2 || !bounded.Truncated.Nodes {
		t.Fatalf("SQLite bounded nodes = %d, truncated=%v, err=%v", len(bounded.Nodes), bounded.Truncated.Nodes, err)
	}

	if _, err := reopened.DB().Exec(`DROP TRIGGER workflow_events_reject_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.DB().Exec(`UPDATE workflow_events SET attributes_json = '{' WHERE run_id = ? AND event_type = 'adapter.observation'`, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Inspect(t.Context(), Query{RunID: run.ID, Now: base.Add(time.Hour)}); !errors.Is(err, ErrCorruptRunState) {
		t.Fatalf("corrupt persisted event error = %v", err)
	}
}
