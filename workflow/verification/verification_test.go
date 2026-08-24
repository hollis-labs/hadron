package verification_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
)

func TestDefaultVerifiersDeterministicPassFailAndMissingEvidence(t *testing.T) {
	registry := verification.NewDefaultRegistry()
	outputs := values.ValueSet{"answer": inlineValue(t, "answer", json.Number("9007199254740993"))}
	schema := graph.Schema{"type": "object", "required": []any{"answer"}, "properties": map[string]any{"answer": map[string]any{"type": "integer"}}, "additionalProperties": false}
	evidence := []verification.Activity{
		{Sequence: 1, Kind: verification.ActivityToolCall, ToolCall: &verification.ToolCall{Server: "github", Tool: "issues.get", Outcome: verification.ActivitySucceeded}},
		{Sequence: 2, Kind: verification.ActivityTest, Test: &verification.TestRun{Name: "unit", Outcome: verification.ActivitySucceeded}},
		{Sequence: 3, Kind: verification.ActivityLint, Lint: &verification.LintRun{Name: "go-vet", Outcome: verification.ActivitySucceeded}},
	}
	tests := []struct {
		name     string
		check    graph.VerificationCheck
		want     verification.CheckOutcome
		wantCode string
	}{
		{name: "no error", check: graph.VerificationCheck{Kind: verification.CheckNoError, Config: graph.Config{}}, want: verification.CheckPassed, wantCode: "verification_passed"},
		{name: "schema", check: graph.VerificationCheck{Kind: verification.CheckOutputSchema, Config: graph.Config{}}, want: verification.CheckPassed, wantCode: "verification_passed"},
		{name: "predicate exact number", check: graph.VerificationCheck{Kind: verification.CheckPredicate, Config: graph.Config{"expression": "inputs.answer == 9007199254740993"}}, want: verification.CheckPassed, wantCode: "verification_passed"},
		{name: "predicate false", check: graph.VerificationCheck{Kind: verification.CheckPredicate, Config: graph.Config{"expression": "inputs.answer < 2"}}, want: verification.CheckFailed, wantCode: "verification_predicate_failed"},
		{name: "tool evidence", check: graph.VerificationCheck{Kind: verification.CheckExpectedToolCall, Config: graph.Config{"server": "github", "tool": "issues.get"}}, want: verification.CheckPassed, wantCode: "verification_passed"},
		{name: "missing tool evidence", check: graph.VerificationCheck{Kind: verification.CheckExpectedToolCall, Config: graph.Config{"tool": "issues.delete"}}, want: verification.CheckFailed, wantCode: "verification_tool_call_missing"},
		{name: "tests", check: graph.VerificationCheck{Kind: verification.CheckTests, Config: graph.Config{"required": []any{"unit"}}}, want: verification.CheckPassed, wantCode: "verification_passed"},
		{name: "lint", check: graph.VerificationCheck{Kind: verification.CheckLint, Config: graph.Config{"required": []any{"go-vet"}}}, want: verification.CheckPassed, wantCode: "verification_passed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			verifier, _, err := verification.Resolve(registry, test.check.Kind)
			if err != nil {
				t.Fatal(err)
			}
			result, err := verifier.Verify(context.Background(), verification.Request{Check: test.check, OutputSchema: schema, Outputs: outputs, Evidence: evidence})
			if err != nil || result.Outcome != test.want || result.Code != test.wantCode {
				t.Fatalf("Verify() = %#v, %v", result, err)
			}
		})
	}
}

func TestOutputSchemaAcceptsProgrammaticGraphSchema(t *testing.T) {
	verifier, _, err := verification.Resolve(verification.NewDefaultRegistry(), verification.CheckOutputSchema)
	if err != nil {
		t.Fatal(err)
	}
	schema := graph.Schema{
		"type": "object", "required": []any{"answer"},
		"properties":           map[string]any{"answer": map[string]any{"type": "integer"}},
		"additionalProperties": false,
	}
	check := graph.VerificationCheck{Kind: verification.CheckOutputSchema, Config: graph.Config{"schema": schema}}
	if diagnostics := verifier.ValidateConfig(context.Background(), check); len(diagnostics) != 0 {
		t.Fatalf("ValidateConfig(graph.Schema) = %#v", diagnostics)
	}
	result, err := verifier.Verify(context.Background(), verification.Request{
		Check: check,
		Outputs: values.ValueSet{
			"answer": inlineValue(t, "answer", json.Number("9007199254740993")),
		},
	})
	if err != nil || result.Outcome != verification.CheckPassed {
		t.Fatalf("Verify(graph.Schema) = %#v, %v", result, err)
	}
}

func TestNamedEvidenceRejectsAmbiguousDuplicateLiteralRecords(t *testing.T) {
	verifier, _, err := verification.Resolve(verification.NewDefaultRegistry(), verification.CheckTests)
	if err != nil {
		t.Fatal(err)
	}
	result, err := verifier.Verify(context.Background(), verification.Request{
		Check: graph.VerificationCheck{Kind: verification.CheckTests, Config: graph.Config{"required": []any{"unit"}}},
		Evidence: []verification.Activity{
			{Sequence: 1, Kind: verification.ActivityTest, Test: &verification.TestRun{Name: "unit", Outcome: verification.ActivitySucceeded}},
			{Sequence: 2, Kind: verification.ActivityTest, Test: &verification.TestRun{Name: "unit", Outcome: verification.ActivityFailed}},
		},
	})
	if err != nil || result.Outcome != verification.CheckFailed || result.Code != "verification_evidence_ambiguous" {
		t.Fatalf("Verify() = %#v, %v", result, err)
	}
}

func TestActivityRecorderConcurrentFreezeAndDefensiveCopies(t *testing.T) {
	recorder := verification.NewActivityRecorder()
	const calls = 64
	var group sync.WaitGroup
	group.Add(calls)
	for range calls {
		go func() {
			defer group.Done()
			if err := recorder.RecordToolCall(context.Background(), verification.ToolCall{Server: "local", Tool: "read", Outcome: verification.ActivitySucceeded}); err != nil {
				t.Errorf("RecordToolCall() = %v", err)
			}
		}()
	}
	group.Wait()
	first, err := recorder.Freeze()
	if err != nil || len(first) != calls {
		t.Fatalf("Freeze() = %d, %v", len(first), err)
	}
	for index, activity := range first {
		if activity.Sequence != uint64(index+1) {
			t.Fatalf("activity[%d] = %#v", index, activity)
		}
	}
	first[0].ToolCall.Tool = "mutated"
	second, err := recorder.Freeze()
	if err != nil || second[0].ToolCall.Tool != "read" {
		t.Fatalf("replayed Freeze() = %#v, %v", second, err)
	}
	if err := recorder.RecordLint(context.Background(), verification.LintRun{Name: "vet", Outcome: verification.ActivitySucceeded}); !errors.Is(err, verification.ErrRecorderFrozen) {
		t.Fatalf("RecordLint(after Freeze) = %v", err)
	}
}

func TestRegistryIsDeterministicImmutableAndConcurrent(t *testing.T) {
	registry := verification.NewDefaultRegistry()
	want := []string{"expected_tool_call", "lint", "no_error", "output_schema", "predicate", "tests"}
	for range 20 {
		listed := registry.List()
		got := make([]string, len(listed))
		for index := range listed {
			got[index] = listed[index].Kind
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("List() = %v", got)
		}
		listed[0].Kind = "mutated"
	}
	if registry.List()[0].Kind != want[0] {
		t.Fatal("registry snapshot was mutable")
	}
}

func TestSnapshotRegistryFreezesRegistrationsAndRejectsLookupListMismatch(t *testing.T) {
	source := verification.NewRegistry()
	first := simpleVerifier{spec: verification.VerifierSpec{Kind: "first", Version: "v1", Mode: verification.ModeDeterministic, ConfigSchema: graph.Schema{}}}
	if err := source.Register(first); err != nil {
		t.Fatal(err)
	}
	snapshot, err := verification.SnapshotRegistry(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Register(simpleVerifier{spec: verification.VerifierSpec{Kind: "second", Version: "v1", Mode: verification.ModeDeterministic, ConfigSchema: graph.Schema{}}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := snapshot.Lookup("second"); ok || len(snapshot.List()) != 1 {
		t.Fatalf("late registration changed snapshot: %#v", snapshot.List())
	}
	if err := snapshot.Register(first); !errors.Is(err, verification.ErrInvalidSpec) {
		t.Fatalf("Register(frozen) = %v", err)
	}

	advertised := first.spec
	mismatch := inconsistentRegistry{listed: []verification.VerifierSpec{advertised}, verifier: simpleVerifier{spec: verification.VerifierSpec{Kind: "first", Version: "v2", Mode: verification.ModeReviewer, ConfigSchema: graph.Schema{}}}}
	if _, err := verification.SnapshotRegistry(mismatch); !errors.Is(err, verification.ErrInvalidSpec) {
		t.Fatalf("SnapshotRegistry(mismatch) = %v", err)
	}
}

func TestValidateSpecEnforcesAdvertisedConfigSchemaBeforeVerifier(t *testing.T) {
	source := &graph.SourceRef{
		Format: graph.SourceWorkflow, Locator: "verified.workflow.yaml",
		Path: []string{"steps", "0", "verify", "0"}, StartLine: 7, StartColumn: 5, EndLine: 7, EndColumn: 25,
	}
	verifier := &schemaVerifier{spec: verification.VerifierSpec{
		Kind: "configured", Version: "v1", Mode: verification.ModeDeterministic,
		ConfigSchema: graph.Schema{
			"type": "object", "required": []any{"enabled"},
			"properties":           map[string]any{"enabled": map[string]any{"type": "boolean"}},
			"additionalProperties": false,
		},
	}}
	registry := verification.NewRegistry()
	if err := registry.Register(verifier); err != nil {
		t.Fatal(err)
	}
	diagnostics := verification.ValidateSpec(context.Background(), registry, &graph.VerificationSpec{Checks: []graph.VerificationCheck{{
		Kind: "configured", Config: graph.Config{"enabled": "not-a-boolean"}, Source: source,
	}}})
	if len(diagnostics) != 1 || diagnostics[0].Code != verification.CodeInvalidCheck || !reflect.DeepEqual(diagnostics[0].Source, source) {
		t.Fatalf("ValidateSpec() = %#v", diagnostics)
	}
	if verifier.validationCalls != 0 {
		t.Fatalf("ValidateConfig called %d times for schema-invalid config", verifier.validationCalls)
	}
}

func TestReviewerDecisionParsingFailsClosed(t *testing.T) {
	valid, err := verification.ParseReviewerDecision([]byte(`{"passed":false,"code":"review_rejected","message":"unsafe"}`))
	if err != nil || valid.Passed || valid.Code != "review_rejected" {
		t.Fatalf("ParseReviewerDecision(valid) = %#v, %v", valid, err)
	}
	for _, input := range []string{
		`{"code":"missing","message":"passed omitted"}`,
		`{"passed":true,"passed":false,"code":"duplicate","message":"ambiguous"}`,
		`{"passed":true,"code":"unknown","message":"field","claim":true}`,
		`{"passed":true,"code":"trailing","message":"document"} {}`,
	} {
		if _, err := verification.ParseReviewerDecision([]byte(input)); !errors.Is(err, verification.ErrInvalidDecision) {
			t.Fatalf("ParseReviewerDecision(%q) = %v", input, err)
		}
	}
}

func TestSpecDigestIgnoresSourceButBindsSemanticConfig(t *testing.T) {
	first := graph.VerificationSpec{Checks: []graph.VerificationCheck{{Kind: verification.CheckPredicate, Config: graph.Config{"expression": "inputs.ok"}, Source: &graph.SourceRef{Format: graph.SourceWorkflow, Locator: "one.workflow.yaml", Path: []string{"steps", "0", "verify", "0"}, StartLine: 1, StartColumn: 1, EndLine: 1, EndColumn: 2}}}}
	second := first
	second.Checks = append([]graph.VerificationCheck(nil), first.Checks...)
	second.Checks[0].Source = &graph.SourceRef{Format: graph.SourceWorkflow, Locator: "two.workflow.yaml", Path: []string{"steps", "9"}, StartLine: 99, StartColumn: 1, EndLine: 99, EndColumn: 2}
	digestOne, err := verification.SpecDigest(first)
	if err != nil {
		t.Fatal(err)
	}
	digestTwo, _ := verification.SpecDigest(second)
	if digestOne != digestTwo {
		t.Fatalf("relocated digests differ: %s / %s", digestOne, digestTwo)
	}
	second.Checks[0].Config = graph.Config{"expression": "!inputs.ok"}
	digestThree, _ := verification.SpecDigest(second)
	if digestThree == digestOne {
		t.Fatal("semantic change did not change digest")
	}
}

func inlineValue(t *testing.T, name string, payload any) values.Value {
	t.Helper()
	value, err := values.NewInline(payload, values.Metadata{
		Producer:  values.Producer{Kind: "verification-test", Reference: "node", Output: name},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

type simpleVerifier struct{ spec verification.VerifierSpec }

func (v simpleVerifier) Spec() verification.VerifierSpec { return v.spec }
func (simpleVerifier) ValidateConfig(context.Context, graph.VerificationCheck) []diagnostic.Diagnostic {
	return nil
}
func (v simpleVerifier) Verify(context.Context, verification.Request) (verification.CheckResult, error) {
	return verification.CheckResult{Kind: v.spec.Kind, Version: v.spec.Version, Outcome: verification.CheckPassed, Code: "passed", Message: "passed"}, nil
}

type schemaVerifier struct {
	spec            verification.VerifierSpec
	validationCalls int
}

func (v *schemaVerifier) Spec() verification.VerifierSpec { return v.spec }
func (v *schemaVerifier) ValidateConfig(context.Context, graph.VerificationCheck) []diagnostic.Diagnostic {
	v.validationCalls++
	return nil
}
func (v *schemaVerifier) Verify(context.Context, verification.Request) (verification.CheckResult, error) {
	return verification.CheckResult{Kind: v.spec.Kind, Version: v.spec.Version, Outcome: verification.CheckPassed, Code: "passed", Message: "passed"}, nil
}

type inconsistentRegistry struct {
	listed   []verification.VerifierSpec
	verifier verification.Verifier
}

func (inconsistentRegistry) Register(verification.Verifier) error          { return errors.New("unsupported") }
func (r inconsistentRegistry) Lookup(string) (verification.Verifier, bool) { return r.verifier, true }
func (r inconsistentRegistry) List() []verification.VerifierSpec           { return r.listed }
