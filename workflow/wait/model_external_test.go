package wait_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

func TestRecordJSONRoundTripAndSchemaDigest(t *testing.T) {
	schemaA, schemaErr := workflowwait.NewSchemaRef(graph.Schema{"type": "object", "properties": map[string]any{"count": map[string]any{"type": "integer"}, "name": map[string]any{"type": "string"}}})
	if schemaErr != nil {
		t.Fatal(schemaErr)
	}
	schemaB, schemaErr := workflowwait.NewSchemaRef(graph.Schema{"properties": map[string]any{"name": map[string]any{"type": "string"}, "count": map[string]any{"type": "integer"}}, "type": "object"})
	if schemaErr != nil {
		t.Fatal(schemaErr)
	}
	if schemaA.Digest != schemaB.Digest {
		t.Fatalf("schema key order changed digest: %q != %q", schemaA.Digest, schemaB.Digest)
	}
	tokenDigest, tokenErr := workflowwait.DigestToken("one-time-secret")
	if tokenErr != nil {
		t.Fatal(tokenErr)
	}
	record := workflowwait.Record{Kind: workflowwait.KindCallback, Correlation: "callback:invoice-42", Deadline: time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC), ResumeSchema: schemaA, ResumeTokenDigest: tokenDigest, ResumeURL: "https://example.test/waits/invoice-42", Visibility: workflowwait.VisibilityPrivate, Authority: workflowwait.ResponderAuthority{Kind: "principal", Reference: "approvers", Attributes: map[string]string{"role": "reviewer"}}, WakeSource: workflowwait.WakeCallback, Status: workflowwait.StatusOpen}
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}
	encoded, marshalErr := json.Marshal(record)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	var decoded workflowwait.Record
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(record, decoded) {
		t.Fatalf("round trip changed record:\nwant %#v\ngot  %#v", record, decoded)
	}
}

func TestRecordRejectsTokenBearingURLAndIncompatibleTerminals(t *testing.T) {
	schema, err := workflowwait.NewSchemaRef(nil)
	if err != nil {
		t.Fatal(err)
	}
	base := workflowwait.Record{Kind: workflowwait.KindSignal, Correlation: "signal-1", ResumeSchema: schema, Visibility: workflowwait.VisibilityPrivate, Authority: workflowwait.ResponderAuthority{Kind: "test"}, WakeSource: workflowwait.WakeSignal, Status: workflowwait.StatusOpen}
	for _, raw := range []string{"https://secret@example.test/wait", "https://example.test/wait?token=secret", "https://example.test/wait#secret"} {
		candidate := base
		candidate.ResumeURL = raw
		if err := candidate.Validate(); err == nil {
			t.Fatalf("token-capable URL %q validated", raw)
		}
	}
	badTimeout := base
	badTimeout.Status = workflowwait.StatusTimedOut
	badTimeout.Resolution = &workflowwait.Resolution{Source: workflowwait.WakeSignal, Responder: workflowwait.Responder{Kind: "person", Reference: "user"}, ResolvedAt: time.Now()}
	if err := badTimeout.Validate(); err == nil {
		t.Fatal("timed_out without deadline and timer/system provenance validated")
	}
	badResume := base
	badResume.Status = workflowwait.StatusResumed
	badResume.Resolution = &workflowwait.Resolution{Source: workflowwait.WakeMessage, Responder: workflowwait.Responder{Kind: "test", Reference: "adapter"}, ResolvedAt: time.Now()}
	if err := badResume.Validate(); err == nil {
		t.Fatal("resume with mismatched wake source validated")
	}
}

func TestActivationValidationOrderIsStable(t *testing.T) {
	activation := workflowwait.Activation{}
	for range 20 {
		err := activation.Validate()
		if err == nil || err.Error() != "activation id is required" {
			t.Fatalf("validation error = %v", err)
		}
	}
	if _, err := workflowwait.DigestToken("  "); err == nil {
		t.Fatal("blank resume token validated")
	}
	if err := values.ValidateDigest(values.SHA256Digest(nil)); err != nil && !errors.Is(err, values.ErrInvalidValue) {
		t.Fatal(err)
	}
}

func TestTokenDigestComparisonRejectsMismatchesAndInvalidDigests(t *testing.T) {
	left, err := workflowwait.DigestToken("credential-a")
	if err != nil {
		t.Fatal(err)
	}
	right, err := workflowwait.DigestToken("credential-b")
	if err != nil {
		t.Fatal(err)
	}
	if !workflowwait.EqualTokenDigest(left, left) {
		t.Fatal("identical canonical token digests did not compare equal")
	}
	if workflowwait.EqualTokenDigest(left, right) {
		t.Fatal("different token digests compared equal")
	}
	if workflowwait.EqualTokenDigest(left, "not-a-digest") || workflowwait.EqualTokenDigest("", "") {
		t.Fatal("invalid token digests compared equal")
	}
}

func TestResponderAttributeValidationOrderIsDeterministic(t *testing.T) {
	authority := workflowwait.ResponderAuthority{
		Kind: "test",
		Attributes: map[string]string{
			"z-last":  string([]byte{0xff}),
			"a-first": string([]byte{0xff}),
		},
	}
	const expected = `responder authority attributes["a-first"] must contain valid UTF-8`
	for range 200 {
		err := authority.Validate()
		if err == nil || err.Error() != expected {
			t.Fatalf("attribute validation error = %v, want %q", err, expected)
		}
	}
}
