package stepkind_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

type retainedReversibilityProvider struct{ schema graph.Schema }

func (p *retainedReversibilityProvider) DescribeReversibility(context.Context, stepkind.ReversibilityRequest) (stepkind.ReversibilityEvidence, error) {
	return stepkind.ReversibilityEvidence{Operation: "fixture.retained", ReceiptSchema: p.schema}, nil
}

func TestResolveReversibilityOwnsProviderEvidence(t *testing.T) {
	provider := &retainedReversibilityProvider{schema: graph.Schema{"type": "object", "properties": map[string]any{"token": map[string]any{"type": "string"}}}}
	evidence, err := stepkind.ResolveReversibility(t.Context(), provider, stepkind.ReversibilityRequest{Config: graph.Config{"operation": "create"}})
	if err != nil {
		t.Fatal(err)
	}
	provider.schema["type"] = "array"
	provider.schema["properties"].(map[string]any)["token"].(map[string]any)["type"] = "integer"
	if evidence.ReceiptSchema["type"] != "object" || evidence.ReceiptSchema["properties"].(map[string]any)["token"].(map[string]any)["type"] != "string" {
		t.Fatalf("retained provider mutated admitted evidence = %#v", evidence)
	}
}

func TestExecutionContractsValidateTypedEnvelopes(t *testing.T) {
	value := stepKindValue(t, "input", "hello")
	invocation := stepkind.Invocation{
		Identity: stepkind.InvocationIdentity{RunID: "run-1", NodeID: "node", Attempt: 1},
		Config:   graph.Config{}, Inputs: values.ValueSet{"input": value},
		IdempotencyKey: "attempt-1", Deadline: time.Now().Add(time.Minute),
	}
	if err := invocation.Validate(); err != nil {
		t.Fatalf("Invocation.Validate() = %v", err)
	}
	result := stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": value}}
	if err := result.Validate(); err != nil {
		t.Fatalf("StepResult.Validate() = %v", err)
	}

	invalidInvocation := invocation
	invalidInvocation.Identity.Attempt = 0
	if err := invalidInvocation.Validate(); err == nil {
		t.Fatal("Invocation.Validate() accepted zero attempt")
	}
	invalidResult := result
	invalidResult.Outputs["result"] = values.Value{}
	if err := invalidResult.Validate(); err == nil {
		t.Fatal("StepResult.Validate() accepted malformed typed output")
	}
	ephemeral, err := values.NewInline("process-only", values.Metadata{
		Producer:  values.Producer{Kind: "fixture", Reference: "step", Output: "ephemeral"},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionNone,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := (stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"ephemeral": ephemeral}}).Validate(); !errors.Is(err, values.ErrRetentionViolation) {
		t.Fatalf("StepResult.Validate() retention error = %v", err)
	}
}

func TestStepResultOutcomesAndWaitContinuationAreExact(t *testing.T) {
	resumeValue := stepKindValue(t, "resume", "accepted")
	resumeValues := values.ValueSet{"resume": resumeValue}
	resumeRef, err := values.NewValueSetRef("resume-values", resumeValues)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := workflowwait.NewSchemaRef(graph.Schema{"type": "string"})
	if err != nil {
		t.Fatal(err)
	}
	resolvedAt := time.Now().UTC()
	record := workflowwait.Record{
		Kind: workflowwait.KindSignal, Correlation: "signal-1", ResumeSchema: schema,
		Visibility: workflowwait.VisibilityPrivate, Authority: workflowwait.ResponderAuthority{Kind: "test"},
		WakeSource: workflowwait.WakeSignal, Status: workflowwait.StatusResumed, ResumeValues: &resumeRef,
		Resolution: &workflowwait.Resolution{Source: workflowwait.WakeSignal, Responder: workflowwait.Responder{Kind: "test", Reference: "responder"}, PayloadDigest: resumeRef.Digest, ResolvedAt: resolvedAt},
	}
	continuation := stepkind.WaitContinuation{ID: "wait-1", Record: record, Values: resumeValues}
	if err := continuation.Validate(); err != nil {
		t.Fatalf("WaitContinuation.Validate() = %v", err)
	}
	corrupt := continuation
	corrupt.Values = values.ValueSet{"resume": stepKindValue(t, "resume", "different")}
	if err := corrupt.Validate(); err == nil {
		t.Fatal("WaitContinuation.Validate() accepted digest-mismatched values")
	}
	openRecord := record
	openRecord.Status, openRecord.ResumeValues, openRecord.Resolution = workflowwait.StatusOpen, nil, nil
	waiting := stepkind.StepResult{Outcome: stepkind.StepWaiting, Wait: &stepkind.WaitResult{ID: "wait-2", Record: openRecord}}
	if err := waiting.Validate(); err != nil {
		t.Fatalf("waiting StepResult.Validate() = %v", err)
	}
	waiting.Outputs = values.ValueSet{}
	if err := waiting.Validate(); err == nil {
		t.Fatal("StepResult.Validate() accepted competing wait and outputs")
	}
	external := stepkind.StepResult{Outcome: stepkind.StepExternal, External: &stepkind.ExternalOperationRef{Kind: "job", ID: "job-1"}}
	if err := external.Validate(); err != nil {
		t.Fatalf("external StepResult.Validate() = %v", err)
	}
}

func TestInvocationRejectsNonCanonicalOptionalIdentityText(t *testing.T) {
	valid := stepkind.Invocation{
		Identity: stepkind.InvocationIdentity{RunID: "run-1", NodeID: "node", Attempt: 1},
		Config:   graph.Config{}, Inputs: values.ValueSet{},
	}
	tests := []struct {
		name   string
		mutate func(*stepkind.Invocation)
	}{
		{name: "iteration leading whitespace", mutate: func(invocation *stepkind.Invocation) { invocation.Identity.Iteration = " shard" }},
		{name: "iteration trailing whitespace", mutate: func(invocation *stepkind.Invocation) { invocation.Identity.Iteration = "shard " }},
		{name: "iteration control", mutate: func(invocation *stepkind.Invocation) { invocation.Identity.Iteration = "shard\n2" }},
		{name: "iteration invalid utf8", mutate: func(invocation *stepkind.Invocation) { invocation.Identity.Iteration = string([]byte{0xff}) }},
		{name: "key whitespace only", mutate: func(invocation *stepkind.Invocation) { invocation.IdempotencyKey = "\t" }},
		{name: "key surrounding whitespace", mutate: func(invocation *stepkind.Invocation) { invocation.IdempotencyKey = " key" }},
		{name: "key control", mutate: func(invocation *stepkind.Invocation) { invocation.IdempotencyKey = "key\x00" }},
		{name: "key invalid utf8", mutate: func(invocation *stepkind.Invocation) { invocation.IdempotencyKey = string([]byte{0xff}) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			test.mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatal("Invocation.Validate() accepted non-canonical optional text")
			}
		})
	}
	for _, candidate := range []stepkind.Invocation{valid, func() stepkind.Invocation {
		candidate := valid
		candidate.Identity.Iteration = "shard 2"
		candidate.IdempotencyKey = "attempt key"
		return candidate
	}()} {
		if err := candidate.Validate(); err != nil {
			t.Fatalf("Invocation.Validate() rejected canonical optional text: %v", err)
		}
	}
}

func TestInvocationCarriesVerificationButNeverPersistsActivityRecorder(t *testing.T) {
	recorder := verification.NewActivityRecorder()
	invocation := stepkind.Invocation{
		Identity: stepkind.InvocationIdentity{RunID: "run-1", NodeID: "node", Attempt: 1},
		Config:   graph.Config{}, Inputs: values.ValueSet{},
		Verification: &graph.VerificationSpec{Checks: []graph.VerificationCheck{{Kind: verification.CheckNoError, Config: graph.Config{}}}},
		Activity:     recorder,
	}
	if err := invocation.Validate(); err != nil {
		t.Fatalf("Invocation.Validate() = %v", err)
	}
	encoded, err := json.Marshal(invocation)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"verification"`) || strings.Contains(string(encoded), "activities") || strings.Contains(string(encoded), "frozen") {
		t.Fatalf("durable invocation JSON = %s", encoded)
	}
	var replay stepkind.Invocation
	if err := json.Unmarshal(encoded, &replay); err != nil {
		t.Fatal(err)
	}
	if replay.Verification == nil || replay.Activity != nil {
		t.Fatalf("replayed invocation = %#v", replay)
	}
}

func TestExecutionErrorClassificationAndObservationValidation(t *testing.T) {
	classified := &stepkind.ExecutionError{
		Code: "temporary", Message: "try again", Classification: stepkind.Retryable,
		Details: map[string]string{"provider": "fixture"}, Cause: errors.New("cause"),
	}
	if err := classified.Validate(); err != nil {
		t.Fatalf("ExecutionError.Validate() = %v", err)
	}
	if got := stepkind.ClassifyError(fmtWrapped(classified)); got != stepkind.Retryable {
		t.Fatalf("ClassifyError() = %q, want retryable", got)
	}
	if got := stepkind.ClassifyError(errors.New("plain")); got != stepkind.RetryUnspecified {
		t.Fatalf("plain ClassifyError() = %q", got)
	}

	ref := stepkind.ExternalOperationRef{Kind: "job", ID: "operation-1", Metadata: map[string]string{"region": "test"}}
	if err := ref.Validate(); err != nil {
		t.Fatalf("ExternalOperationRef.Validate() = %v", err)
	}
	result := stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{}}
	observation := stepkind.Observation{State: stepkind.ObservationSucceeded, Result: &result}
	if err := observation.Validate(); err != nil {
		t.Fatalf("Observation.Validate() = %v", err)
	}
	observation.Failure = classified
	if err := observation.Validate(); err == nil {
		t.Fatal("Observation.Validate() accepted competing result and failure")
	}
}

func TestResolveUsesRegisteredSnapshotAndHandlesClosedResolutionFailures(t *testing.T) {
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewNoopKind("stable", "v1")
	kind.SpecValue.OutputSchema = graph.Schema{"type": "object"}
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	kind.SpecValue.Name = "mutated"
	kind.SpecValue.OutputSchema = graph.Schema{"not": graph.Schema{}}

	resolved, spec, err := stepkind.Resolve(registry, "stable", "v1")
	if err != nil || resolved != kind || spec.Name != "stable" || spec.OutputSchema["type"] != "object" {
		t.Fatalf("Resolve() = %T, %#v, %v; want immutable registered snapshot", resolved, spec, err)
	}
	if err := registry.Register(stepkindtest.NewNoopKind("stable", "v2")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := stepkind.Resolve(registry, "stable", ""); !errors.Is(err, stepkind.ErrAmbiguousStepKind) {
		t.Fatalf("ambiguous Resolve() error = %v", err)
	}
	if _, _, err := stepkind.Resolve(registry, "missing", "v1"); !errors.Is(err, stepkind.ErrUnknownStepKind) {
		t.Fatalf("unknown Resolve() error = %v", err)
	}
	var typedNil *stepkind.MemoryRegistry
	if _, _, err := stepkind.Resolve(typedNil, "stable", "v1"); !errors.Is(err, stepkind.ErrUnknownStepKind) {
		t.Fatalf("typed-nil Resolve() error = %v", err)
	}
}

func TestRegistryRequiresHeartbeatMetadataAndInterfaceAgreement(t *testing.T) {
	kind := stepkindtest.NewLifecycleKind("heartbeat", "v1")
	if err := stepkind.NewRegistry().Register(kind); err != nil {
		t.Fatalf("Register(heartbeat lifecycle) = %v", err)
	}
	kind.SpecValue.Observation.Heartbeat = false
	if err := stepkind.NewRegistry().Register(kind); err == nil {
		t.Fatal("Register() accepted hidden Heartbeater implementation")
	}
}

func stepKindValue(t *testing.T, output string, payload any) values.Value {
	t.Helper()
	value, err := values.NewInline(payload, values.Metadata{
		Producer:  values.Producer{Kind: "fixture", Reference: "step", Output: output},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func fmtWrapped(err error) error { return &wrappedError{err: err} }

type wrappedError struct{ err error }

func (e *wrappedError) Error() string { return "wrapped: " + e.err.Error() }
func (e *wrappedError) Unwrap() error { return e.err }
