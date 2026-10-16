package persistence

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/hollis-labs/libs/workflow/engine/graph"
	workflowruntime "github.com/hollis-labs/libs/workflow/engine/runtime"
	"github.com/hollis-labs/libs/workflow/engine/values"
	workflowwait "github.com/hollis-labs/libs/workflow/engine/wait"
)

// A gate wait.resumed event records which option was chosen and that option's
// declared kind, taken from the gate's private checkpoint, and nothing else about
// the decision. A non-gate wait, or a decision that is not a declared option,
// leaves the event as it was.
func TestGateResumeRecordsTheChosenOptionAndItsKind(t *testing.T) {
	type want struct{ option, kind string }
	for name, test := range map[string]struct {
		gate     bool
		decision string
		want     want
	}{
		"decision option": {true, "approve", want{"approve", "decision"}},
		"skip option":     {true, "skip-it", want{"skip-it", "skip"}},
		"kind defaults":   {true, "plain", want{"plain", "decision"}},
		"unknown option":  {true, "nope", want{}},
		"not a gate":      {false, "approve", want{}},
	} {
		t.Run(name, func(t *testing.T) {
			_, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "gate.db"))
			fixture := prepareWorkflowSQLiteWait(t, state, "gate", workflowTestTime(), time.Hour)
			schema, err := workflowwait.NewSchemaRef(graph.Schema{"type": "object"})
			if err != nil {
				t.Fatal(err)
			}
			record := &fixture.request.Wait.Record
			record.ResumeSchema = schema
			if test.gate {
				checkpoint, checkpointErr := values.NewInline(map[string]any{"options": []any{
					map[string]any{"id": "approve", "label": "Approve", "kind": "decision"},
					map[string]any{"id": "skip-it", "label": "Skip", "kind": "skip"},
					map[string]any{"id": "plain", "label": "Plain"},
				}}, values.Metadata{Producer: values.Producer{Kind: "human_gate", Reference: "wait-node", Output: "checkpoint"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
				if checkpointErr != nil {
					t.Fatal(checkpointErr)
				}
				invocation := fixture.invocation
				ref, saveErr := state.SaveValues(t.Context(), workflowruntime.SaveValuesRequest{
					Owner:  workflowruntime.ValueOwner{Kind: "gate-payload", RunID: invocation.RunID, Invocation: &invocation},
					Values: values.ValueSet{"checkpoint": checkpoint},
				})
				if saveErr != nil {
					t.Fatal(saveErr)
				}
				record.Kind, record.WakeSource, record.Payload = workflowwait.KindGate, workflowwait.WakeGate, &ref
				fixture.source = workflowwait.WakeGate
			}
			coordinator := workflowruntime.WaitCoordinator{Store: state}
			if _, suspendErr := coordinator.Suspend(t.Context(), workflowruntime.SuspendCommand{Request: fixture.request, ResumeToken: fixture.token}); suspendErr != nil {
				t.Fatal(suspendErr)
			}
			command := fixture.resumeCommand(t, "resume-gate", fixture.base.Add(4*time.Second))
			command.Payload, err = values.NewInline(map[string]any{"decision": test.decision}, values.Metadata{Producer: values.Producer{Kind: "wait_response", Reference: string(fixture.waitID)}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
			if err != nil {
				t.Fatal(err)
			}
			resumed, err := coordinator.Resume(context.Background(), command)
			if err != nil || len(resumed.Events) == 0 {
				t.Fatalf("Resume = %#v, %v", resumed, err)
			}
			attributes := resumed.Events[0].Attributes
			if attributes["decision_option"] != test.want.option || attributes["decision_kind"] != test.want.kind {
				t.Fatalf("attributes = %v; want option %q kind %q", attributes, test.want.option, test.want.kind)
			}
		})
	}
}
