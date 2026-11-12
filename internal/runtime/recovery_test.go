package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hollis-labs/libs/workflow/engine/graph"
	workflowruntime "github.com/hollis-labs/libs/workflow/engine/runtime"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/persistence"
)

// A run admitted just before the daemon stops, with its first node ready and no
// worker having claimed it, must run to completion after the restart.
func TestRunAdmittedBeforeARestartCompletesAfterIt(t *testing.T) {
	first, cfg, store := newTestRuntime(t)
	// Bring the host up without the workers, so the start is admitted and nothing runs it.
	first.activation.Prepare()
	if err := first.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	first.activation.Start()
	ctx := localCtx(t.Context())
	ref := graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: "production-transform", Locator: "production-transform.workflow.yaml", Version: "v1"}
	started, err := first.operations.RunWorkflow(ctx, appworkflow.RunWorkflowRequest{
		RunID: "in-flight", IdempotencyKey: "in-flight", Definition: ref,
		Inputs: map[string]any{"message": "hello"}, Identity: appworkflow.IdentityRequest{SourceAuthority: "http"},
	})
	if err != nil || started.Run == nil {
		t.Fatalf("RunWorkflow = %#v, %v", started, err)
	}
	// The daemon stops before any worker claims the node.
	if err := first.host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	first.activation.Stop()

	second, err := newTestRuntimeOn(store, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Shutdown(context.Background()) })
	deadline := time.Now().Add(15 * time.Second)
	for {
		got, inspectErr := second.operations.InspectWorkflowRun(localCtx(t.Context()), appworkflow.InspectWorkflowRunRequest{RunID: "in-flight", Identity: appworkflow.IdentityRequest{SourceAuthority: "http"}})
		if inspectErr == nil && got.Run.Status == workflowruntime.RunSucceeded {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the run admitted before the restart did not complete: %#v, %v", got.Run.Status, inspectErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

const slowWorkflow = `workflow: {id: slow, version: v1}
inputs:
  - {name: message, type: string, required: true}
steps:
  - id: spin
    kind: script
    kind_version: v1
    config:
      runtime: goja
      code: |
        function main(input) {
          var s = 0;
          for (var i = 0; i < 10000000; i++) { s += i; }
          return { n: input.message + ":" + s };
        }
      input_schema: {type: object, additionalProperties: false, required: [message], properties: {message: {type: string}}}
      output_schema: {type: object, additionalProperties: false, required: [n], properties: {n: {type: string}}}
    with: {message: inputs.message}
    outputs: {n: {type: string}}
    effects: [compute]
outputs:
  n: {type: string, value: steps.spin.outputs.n}
`

// A step that is running when the daemon is told to stop finishes first: stopping
// drains the workers rather than cancelling the step, which the engine would
// otherwise mark crashed once its lease lapsed.
func TestShutdownLetsARunningStepFinish(t *testing.T) {
	rt, cfg, store := newTestRuntime(t)
	if err := os.WriteFile(filepath.Join(cfg.SourceRoot(), "slow.workflow.yaml"), []byte(slowWorkflow), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := rt.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx := localCtx(t.Context())
	ref := graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: "slow", Locator: "slow.workflow.yaml", Version: "v1"}
	if _, err := rt.operations.RunWorkflow(ctx, appworkflow.RunWorkflowRequest{
		RunID: "slow-run", IdempotencyKey: "slow-run", Definition: ref,
		Inputs: map[string]any{"message": "x"}, Identity: appworkflow.IdentityRequest{SourceAuthority: "http"},
	}); err != nil {
		t.Fatal(err)
	}
	state, err := persistence.NewWorkflowStateStore(store)
	if err != nil {
		t.Fatal(err)
	}
	// Wait until the step is executing, then stop.
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		node, nodeErr := state.LoadNodeInvocation(t.Context(), workflowruntime.NodeInvocationID{RunID: "slow-run", NodeID: "spin"})
		if nodeErr == nil && node.Status == workflowruntime.NodeRunning {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the step never started: %#v, %v", node, nodeErr)
		}
	}
	stop, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := rt.Shutdown(stop); err != nil {
		t.Fatal(err)
	}
	// The step itself finished (it was not cancelled mid-attempt, which would leave
	// it to be marked crashed). The run's own completion is picked up by the next
	// process, which must not have to wait out the step's lease.
	node, err := state.LoadNodeInvocation(t.Context(), workflowruntime.NodeInvocationID{RunID: "slow-run", NodeID: "spin"})
	if err != nil || node.Status != workflowruntime.NodeSucceeded {
		t.Fatalf("after a graceful stop the step is %q (err %v), want succeeded", node.Status, err)
	}
	second, err := newTestRuntimeOn(store, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Shutdown(context.Background()) })
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		run, loadErr := state.LoadRun(t.Context(), "slow-run")
		if loadErr == nil && run.Status == workflowruntime.RunSucceeded {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the run did not complete after the restart: %q (err %v)", run.Status, loadErr)
		}
	}
}
