package gate_test

import (
	"context"
	"testing"

	"github.com/hollis-labs/hadron/workflow/gate"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

func TestCheckpointVocabularyAndOptionalSemantics(t *testing.T) {
	required := checkpoint(t)
	if err := required.Validate(); err != nil {
		t.Fatalf("required checkpoint: %v", err)
	}
	optional := required
	optional.Behavior.Optional = true
	optional.Options = append(append([]gate.Option(nil), required.Options...), gate.Option{ID: "skip", Label: "Skip", Kind: gate.OptionSkip})
	if err := optional.Validate(); err != nil {
		t.Fatalf("optional blocking checkpoint: %v", err)
	}
	nonblocking := optional
	nonblocking.Behavior.Blocking = false
	if err := nonblocking.Validate(); err != nil {
		t.Fatalf("portable optional non-blocking vocabulary: %v", err)
	}
	incoherent := required
	incoherent.Behavior.Blocking = false
	if err := incoherent.Validate(); err == nil {
		t.Fatal("required non-blocking checkpoint accepted")
	}
	missingSkip := required
	missingSkip.Behavior.Optional = true
	if err := missingSkip.Validate(); err == nil {
		t.Fatal("optional checkpoint without explicit skip accepted")
	}
	duplicate := required
	duplicate.Options = append(duplicate.Options, duplicate.Options[0])
	if err := duplicate.Validate(); err == nil {
		t.Fatal("duplicate option accepted")
	}
}

func TestEscalationOrderingAndDefensiveClone(t *testing.T) {
	checkpoint := checkpoint(t)
	checkpoint.Escalations = []gate.Escalation{
		{After: "1h", Subject: gate.PolicySubject{Kind: "notification", Reference: "team-a", Attributes: map[string]string{"channel": "ops"}}},
		{After: "2h", Subject: gate.PolicySubject{Kind: "notification", Reference: "team-b"}},
	}
	if err := checkpoint.Validate(); err != nil {
		t.Fatal(err)
	}
	cloned := gate.CloneCheckpoint(checkpoint)
	cloned.Options[0].Label = "changed"
	cloned.Subject.Attributes["tier"] = "changed"
	cloned.Escalations[0].Subject.Attributes["channel"] = "changed"
	cloned.ResumeSchema.Schema["type"] = "array"
	if checkpoint.Options[0].Label == "changed" || checkpoint.Subject.Attributes["tier"] == "changed" ||
		checkpoint.Escalations[0].Subject.Attributes["channel"] == "changed" || checkpoint.ResumeSchema.Schema["type"] != "object" {
		t.Fatal("CloneCheckpoint shared mutable state")
	}
	reversed := checkpoint
	reversed.Escalations[0], reversed.Escalations[1] = reversed.Escalations[1], reversed.Escalations[0]
	if err := reversed.Validate(); err == nil {
		t.Fatal("out-of-order escalations accepted")
	}
}

func TestGateHostSeamsAreExternalPackageUsable(t *testing.T) {
	authority := gate.AuthorityResolverFunc(func(_ context.Context, request gate.AuthorizationRequest) (workflowwait.ResponderAuthority, error) {
		request.Checkpoint.Options[0].Label = "host-copy"
		return workflowwait.ResponderAuthority{Kind: gate.AuthorityGatePolicy, Reference: request.Checkpoint.Subject.Reference}, nil
	})
	payloads := gate.PayloadStoreFunc(func(_ context.Context, request gate.PayloadRequest) (values.ValueSetRef, error) {
		request.Checkpoint.Options[0].Label = "store-copy"
		return values.ValueSetRef{ID: "gate-payload", Digest: values.SHA256Digest([]byte("payload"))}, nil
	})
	if _, err := authority.ResolveGateAuthority(t.Context(), gate.AuthorizationRequest{RunID: "run-1", NodeID: "gate", Attempt: 1, Checkpoint: checkpoint(t)}); err != nil {
		t.Fatal(err)
	}
	if ref, err := payloads.StoreGatePayload(t.Context(), gate.PayloadRequest{RunID: "run-1", NodeID: "gate", Attempt: 1, Checkpoint: checkpoint(t)}); err != nil || ref.ID != "gate-payload" {
		t.Fatalf("payload seam = %#v, %v", ref, err)
	}
}

func checkpoint(t *testing.T) gate.Checkpoint {
	t.Helper()
	schema, err := workflowwait.NewSchemaRef(graph.Schema{
		"type": "object", "required": []any{"decision"}, "additionalProperties": false,
		"properties": map[string]any{"decision": map[string]any{"type": "string", "enum": []any{"approve", "reject"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return gate.Checkpoint{
		Prompt: "Release?", Options: []gate.Option{{ID: "approve", Label: "Approve"}, {ID: "reject", Label: "Reject"}},
		ResumeSchema: schema, Subject: gate.PolicySubject{Kind: gate.PolicyEnvironment, Reference: "production", Attributes: map[string]string{"tier": "critical"}},
		Correlation: "release-1", Behavior: gate.Behavior{Blocking: true},
	}
}
