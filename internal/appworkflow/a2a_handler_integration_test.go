package appworkflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/a2a"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestA2AOwnerScopedCallerKeyStartsAndReplaysIndependentHostRuns(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	correlationStore, err := persistence.NewWorkflowA2ATaskStore(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	correlations, err := appworkflow.NewA2ATaskCorrelations(appworkflow.A2ATaskCorrelationsOptions{Host: fixture.host, Store: correlationStore})
	if err != nil {
		t.Fatal(err)
	}
	operator, err := appworkflow.NewWorkflowOperator(appworkflow.WorkflowOperatorOptions{
		Host: fixture.host,
		Diagnostics: graphInspectorFunc(func(context.Context, rundiagnostics.Query) (rundiagnostics.Result, error) {
			return rundiagnostics.Result{}, nil
		}),
		Replay: replayRunnerFunc(func(context.Context, workflowruntime.ReplayRequest) (workflowruntime.BeginReplayResult, error) {
			return workflowruntime.BeginReplayResult{}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := a2a.NewHandler(a2a.Options{Correlations: correlations, Workflows: operator, Reads: operator})
	if err != nil {
		t.Fatal(err)
	}
	definition := graph.DefinitionRef{Kind: "registry", ID: "team/host-fixture", Version: "v1", Digest: values.SHA256Digest([]byte("host-fixture"))}
	request := a2a.TaskRequest{Skill: definition, Input: map[string]any{"message": "hello"}, IdempotencyKey: "shared-caller-key"}
	firstContext := authenticatedContext(t.Context(), "user:first")
	secondContext := authenticatedContext(t.Context(), "user:second")
	first, err := handler.SubmitTask(firstContext, request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := handler.SubmitTask(secondContext, request)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID || first.RunID == second.RunID || first.Outcome != workflowruntime.IdempotencyApplied || second.Outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("owner-scoped starts = %#v / %#v", first, second)
	}
	firstStart, err := fixture.journal.LoadStart(t.Context(), first.RunID)
	if err != nil {
		t.Fatal(err)
	}
	secondStart, err := fixture.journal.LoadStart(t.Context(), second.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if firstStart.Record.StartKey == request.IdempotencyKey || secondStart.Record.StartKey == request.IdempotencyKey || firstStart.Record.StartKey == secondStart.Record.StartKey {
		t.Fatalf("host start keys were not owner isolated: %q / %q", firstStart.Record.StartKey, secondStart.Record.StartKey)
	}
	replayed, err := handler.SubmitTask(firstContext, request)
	if err != nil || replayed.ID != first.ID || replayed.RunID != first.RunID || replayed.Outcome != workflowruntime.IdempotencyReplayed {
		t.Fatalf("same-owner replay = %#v, %v", replayed, err)
	}
	changed := request
	changed.Input = map[string]any{"message": "changed"}
	if _, err := handler.SubmitTask(firstContext, changed); !errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("changed owner intent error = %v", err)
	}
}
