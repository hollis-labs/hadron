package main

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/settings"
	"github.com/hollis-labs/libs/workflow/engine/graph"
	workflowruntime "github.com/hollis-labs/libs/workflow/engine/runtime"
)

// runTransformToSuccess runs the production-transform workflow to completion with
// the given settings and returns the store.
func runTransformToSuccess(t *testing.T, sett *settings.Settings) *persistence.Store {
	t.Helper()
	runtime, cfg, store := newTestProductionWorkflowRuntimeWithSettings(t, sett)
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://127.0.0.1:8095/v1/workflows/runs", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.RemoteAddr, request.Host = "127.0.0.1:43123", "127.0.0.1:8095"
	request.Header.Set("Authorization", operatorBearer(t, cfg))
	ctx, err := runtime.auth.AuthenticateWorkflowRequest(request, appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessRun})
	if err != nil {
		t.Fatal(err)
	}
	started, err := runtime.operations.RunWorkflow(ctx, appworkflow.RunWorkflowRequest{
		RunID: "outbox-run", IdempotencyKey: "outbox-run",
		Definition: graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: "production-transform", Locator: "production-transform.workflow.yaml", Version: "v1"},
		Inputs:     map[string]any{"message": "hello"}, Identity: appworkflow.IdentityRequest{SourceAuthority: "http"},
	})
	if err != nil || started.Run == nil {
		t.Fatalf("RunWorkflow = %#v, %v", started, err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		inspected, inspectErr := runtime.operations.InspectWorkflowRun(ctx, appworkflow.InspectWorkflowRunRequest{RunID: started.Run.ID, Identity: appworkflow.IdentityRequest{SourceAuthority: "http"}})
		if inspectErr == nil && inspected.Run.Status == workflowruntime.RunSucceeded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run did not succeed: %#v, %v", inspected, inspectErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	return store
}

func outboxKinds(t *testing.T, store *persistence.Store) []string {
	t.Helper()
	rows, err := store.DB().Query(`SELECT json_extract(envelope_json, '$.kind') FROM workflow_event_outbox ORDER BY outbox_seq`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var kinds []string
	for rows.Next() {
		var kind string
		if err := rows.Scan(&kind); err != nil {
			t.Fatal(err)
		}
		kinds = append(kinds, kind)
	}
	return kinds
}

func TestProductionRuntimeWritesTheEventOutboxOnlyWhenSettingsEnableIt(t *testing.T) {
	off := runTransformToSuccess(t, testWorkflowSettings())
	if kinds := outboxKinds(t, off); len(kinds) != 0 {
		t.Fatalf("outbox with the setting off: %v", kinds)
	}

	sett := testWorkflowSettings()
	sett.Events.Outbox = true
	on := runTransformToSuccess(t, sett)
	kinds := strings.Join(outboxKinds(t, on), ",")
	if !strings.Contains(kinds, "run.state_changed") || strings.Contains(kinds, "step.executed") {
		t.Fatalf("outbox kinds = %q: want run.state_changed and no step.executed (node.* stays local)", kinds)
	}
	// The run succeeded, so the last run.state_changed says so.
	var to string
	if err := on.DB().QueryRow(`SELECT json_extract(envelope_json, '$.payload.to') FROM workflow_event_outbox WHERE json_extract(envelope_json, '$.kind') = 'run.state_changed' ORDER BY outbox_seq DESC LIMIT 1`).Scan(&to); err != nil || to != "succeeded" {
		t.Fatalf("last run.state_changed to=%q err=%v", to, err)
	}
}
