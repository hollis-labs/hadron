package persistence

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
)

func TestWorkflowFailureHookDurablyFencesRecursiveHandler(t *testing.T) {
	store, _ := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "failure-hooks.db"))
	journal, err := NewWorkflowHostStore(store)
	if err != nil {
		t.Fatal(err)
	}
	base := workflowTestTime()
	handlerDigest := values.SHA256Digest([]byte("handler"))
	handler := graph.DefinitionRef{Authority: "project", Kind: "workflow", ID: "failure-handler", Version: "v1", Digest: handlerDigest}
	rootPlan := workflowTestPlan("failure-root")
	firstRequest := hoststate.BindFailureHookRequest{SourceRunID: "failed-root", SourcePlan: rootPlan, HandlerRunID: "handler-one", Handler: handler, Identity: workflowHostIdentity(), MaximumDepth: 1, At: base}
	first, outcome, err := journal.BindFailureHook(context.Background(), firstRequest)
	if err != nil || outcome != workflowruntime.IdempotencyApplied || first.Status != hoststate.FailureHookPending || first.Binding.Depth != 0 {
		t.Fatalf("BindFailureHook = %#v / %s, %v", first, outcome, err)
	}
	replayed, outcome, err := journal.BindFailureHook(context.Background(), firstRequest)
	if err != nil || outcome != workflowruntime.IdempotencyReplayed || replayed.Binding.HandlerRunID != first.Binding.HandlerRunID {
		t.Fatalf("BindFailureHook replay = %#v / %s, %v", replayed, outcome, err)
	}
	// Expanding the host configuration after restart must not expand the depth
	// fence already bound to this lineage.
	second := hoststate.BindFailureHookRequest{SourceRunID: first.Binding.HandlerRunID, SourcePlan: workflowTestPlan("handler"), HandlerRunID: "handler-two", Handler: handler, Identity: workflowHostIdentity(), MaximumDepth: 16, At: base.Add(time.Second)}
	suppressed, outcome, err := journal.BindFailureHook(context.Background(), second)
	if err != nil || outcome != workflowruntime.IdempotencyApplied || suppressed.Status != hoststate.FailureHookSuppressed || suppressed.Binding.Depth != 1 || suppressed.Binding.MaximumDepth != 1 {
		t.Fatalf("recursive BindFailureHook = %#v / %s, %v", suppressed, outcome, err)
	}
	started, err := journal.CompleteFailureHook(context.Background(), first.Binding.SourceRunID, first.Generation, hoststate.FailureHookStarted, "", base.Add(2*time.Second))
	if err != nil || started.Status != hoststate.FailureHookStarted {
		t.Fatalf("CompleteFailureHook = %#v, %v", started, err)
	}
	pending, err := journal.RecoverFailureHooks(context.Background(), 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("RecoverFailureHooks = %#v, %v", pending, err)
	}
}
