package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
)

const (
	EventNodeVerificationCompleted = "node.verification_completed"
	verificationValueName          = "verification"
)

var ErrVerificationConflict = errors.New("durable verification report conflicts with replay")

// VerificationRecord is the durable, attempt-bound verification outcome
// returned by dispatch. Event.Values is the authoritative report reference.
type VerificationRecord struct {
	Report   verification.Report `json:"report"`
	Ref      values.ValueSetRef  `json:"ref"`
	Event    Event               `json:"event"`
	Replayed bool                `json:"replayed,omitempty"`
}

// VerificationPersistenceError reports a report value set that was written
// but could not be linked through its attempt event. Ref lets retention/repair
// tooling find the immutable orphan without treating verification as applied.
type VerificationPersistenceError struct {
	Ref   *values.ValueSetRef
	Cause error
}

type pendingExternalVerification struct {
	Report     verification.Report
	Ref        values.ValueSetRef
	Completion ExternalVerificationCompletion
}

func (e *VerificationPersistenceError) Error() string {
	if e == nil {
		return "verification persistence failed"
	}
	if e.Ref != nil {
		return fmt.Sprintf("verification persistence failed after value set %s: %v", e.Ref.ID, e.Cause)
	}
	return fmt.Sprintf("verification persistence failed: %v", e.Cause)
}

func (e *VerificationPersistenceError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

type verificationFailureKind string

const (
	verificationDecisionFailure verificationFailureKind = "decision"
	verificationProviderFailure verificationFailureKind = "provider"
	verificationResultFailure   verificationFailureKind = "result"
)

type verificationFailure struct {
	kind  verificationFailureKind
	index int
	check verification.CheckResult
	cause error
}

func executeVerification(
	ctx context.Context,
	registry verification.Registry,
	spec graph.VerificationSpec,
	outputSchema graph.Schema,
	outputs values.ValueSet,
	evidence []verification.Activity,
) (verification.Report, *verificationFailure) {
	digest, err := verification.SpecDigest(spec)
	if err != nil {
		return verification.Report{}, &verificationFailure{kind: verificationResultFailure, cause: err}
	}
	report := verification.Report{
		SchemaVersion: verification.ReportSchemaVersion,
		SpecDigest:    digest,
		Status:        verification.ReportPassed,
		Evidence:      evidence,
	}
	for index, check := range spec.Checks {
		verifier, verifierSpec, resolveErr := verification.Resolve(registry, check.Kind)
		if resolveErr != nil {
			failed := runtimeCheckError(index, check, verification.VerifierSpec{Kind: check.Kind, Version: "unknown"}, "verification_result_invalid", "verification check could not be resolved")
			report.Status, report.Checks = verification.ReportFailed, append(report.Checks, failed)
			return report, &verificationFailure{kind: verificationResultFailure, index: index, check: failed, cause: resolveErr}
		}
		if missing := missingRequiredEvidence(verifierSpec.RequiredEvidence, evidence); missing != "" {
			failed := verification.CheckResult{
				Index: index, Kind: check.Kind, Version: verifierSpec.Version, Outcome: verification.CheckFailed,
				Code: "verification_evidence_missing", Message: "required literal activity evidence is missing",
				Details: map[string]string{"evidence_kind": string(missing)}, Source: cloneDispatchSource(check.Source),
			}
			report.Status, report.Checks = verification.ReportFailed, append(report.Checks, failed)
			return report, &verificationFailure{kind: verificationDecisionFailure, index: index, check: failed}
		}
		clonedCheck, checkErr := verification.CloneCheck(check)
		clonedSchema, schemaErr := verification.CloneSchema(outputSchema)
		clonedOutputs, outputErr := verification.CloneOutputs(outputs)
		clonedEvidence, evidenceErr := verification.CloneActivities(evidence)
		if cloneErr := errors.Join(checkErr, schemaErr, outputErr, evidenceErr); cloneErr != nil {
			failed := runtimeCheckError(index, check, verifierSpec, "verification_result_invalid", "verification input could not be isolated")
			report.Status, report.Checks = verification.ReportFailed, append(report.Checks, failed)
			return report, &verificationFailure{kind: verificationResultFailure, index: index, check: failed, cause: cloneErr}
		}
		result, verifyErr := verifier.Verify(ctx, verification.Request{
			Check: clonedCheck, OutputSchema: clonedSchema, Outputs: clonedOutputs, Evidence: clonedEvidence,
		})
		if verifyErr != nil {
			kind := verificationProviderFailure
			code, message := "verification_provider_failed", "verification provider or evaluator failed"
			if errors.Is(verifyErr, verification.ErrInvalidDecision) {
				kind, code, message = verificationResultFailure, "verification_result_invalid", "verification result could not be parsed"
			}
			failed := runtimeCheckError(index, check, verifierSpec, code, message)
			report.Status, report.Checks = verification.ReportFailed, append(report.Checks, failed)
			return report, &verificationFailure{kind: kind, index: index, check: failed, cause: verifyErr}
		}
		normalized, normalizeErr := normalizeCheckResult(index, check, verifierSpec, result)
		if normalizeErr != nil {
			failed := runtimeCheckError(index, check, verifierSpec, "verification_result_invalid", "verifier returned a malformed result")
			report.Status, report.Checks = verification.ReportFailed, append(report.Checks, failed)
			return report, &verificationFailure{kind: verificationResultFailure, index: index, check: failed, cause: normalizeErr}
		}
		report.Checks = append(report.Checks, normalized)
		if normalized.Outcome == verification.CheckFailed {
			report.Status = verification.ReportFailed
			return report, &verificationFailure{kind: verificationDecisionFailure, index: index, check: normalized}
		}
	}
	if err := report.Validate(); err != nil {
		return report, &verificationFailure{kind: verificationResultFailure, cause: err}
	}
	return report, nil
}

func missingRequiredEvidence(required []verification.ActivityKind, evidence []verification.Activity) verification.ActivityKind {
	for _, requiredKind := range required {
		found := false
		for _, activity := range evidence {
			if activity.Kind == requiredKind {
				found = true
				break
			}
		}
		if !found {
			return requiredKind
		}
	}
	return ""
}

func normalizeCheckResult(index int, check graph.VerificationCheck, spec verification.VerifierSpec, result verification.CheckResult) (verification.CheckResult, error) {
	if result.Kind != "" && result.Kind != check.Kind {
		return verification.CheckResult{}, fmt.Errorf("verifier kind %q differs from check %q", result.Kind, check.Kind)
	}
	if result.Version != "" && result.Version != spec.Version {
		return verification.CheckResult{}, fmt.Errorf("verifier version %q differs from registered %q", result.Version, spec.Version)
	}
	if result.Index != 0 && result.Index != index {
		return verification.CheckResult{}, fmt.Errorf("verifier index %d differs from runtime index %d", result.Index, index)
	}
	if result.Outcome == verification.CheckError {
		return verification.CheckResult{}, errors.New("CheckError is reserved for runtime contract failures")
	}
	result.Index, result.Kind, result.Version = index, check.Kind, spec.Version
	result.Source = cloneDispatchSource(check.Source)
	if err := result.Validate(); err != nil {
		return verification.CheckResult{}, err
	}
	return result, nil
}

func runtimeCheckError(index int, check graph.VerificationCheck, spec verification.VerifierSpec, code, message string) verification.CheckResult {
	return verification.CheckResult{
		Index: index, Kind: check.Kind, Version: spec.Version, Outcome: verification.CheckError,
		Code: code, Message: message, Source: cloneDispatchSource(check.Source),
	}
}

func verificationExecutionError(failure *verificationFailure) error {
	if failure == nil {
		return nil
	}
	code, message := "verification_failed", "node verification rejected executor output"
	classification := stepkind.RetryPermanent
	switch failure.kind {
	case verificationDecisionFailure:
		// Defaults above are the durable model/decision failure contract.
	case verificationProviderFailure:
		code, message = "verification_provider_failed", "verification provider or evaluator failed"
		classification = stepkind.ClassifyError(failure.cause)
	case verificationResultFailure:
		code, message = "verification_result_invalid", "verification result is malformed"
	}
	details := map[string]string{"failure_kind": string(failure.kind), "check_index": strconv.Itoa(failure.index)}
	if failure.check.Kind != "" {
		details["check_kind"] = failure.check.Kind
		details["check_code"] = failure.check.Code
	}
	return &stepkind.ExecutionError{Code: code, Message: message, Classification: classification, Details: details, Cause: failure.cause}
}

func persistVerification(
	ctx context.Context,
	store StateStore,
	retention RetentionHook,
	redactor *values.Redactor,
	attempt AttemptID,
	report verification.Report,
	at time.Time,
) (VerificationRecord, error) {
	masked, set, attributes, err := prepareVerificationReport(attempt, report, redactor)
	if err != nil {
		return VerificationRecord{}, &VerificationPersistenceError{Cause: err}
	}
	digest, err := values.DigestValueSet(set)
	if err != nil {
		return VerificationRecord{}, &VerificationPersistenceError{Cause: err}
	}
	existing, err := findVerificationEvent(ctx, store, attempt)
	if err != nil {
		return VerificationRecord{}, &VerificationPersistenceError{Cause: err}
	}
	if existing != nil {
		if existing.Values == nil || existing.Values.Digest != digest || !reflect.DeepEqual(existing.Attributes, attributes) {
			return VerificationRecord{}, ErrVerificationConflict
		}
		loaded, loadErr := store.LoadValues(ctx, *existing.Values)
		if loadErr != nil {
			return VerificationRecord{}, &VerificationPersistenceError{Cause: loadErr}
		}
		persisted, decodeErr := reportFromValueSet(loaded)
		if decodeErr != nil || !reflect.DeepEqual(persisted, masked) {
			return VerificationRecord{}, ErrVerificationConflict
		}
		return VerificationRecord{Report: persisted, Ref: *existing.Values, Event: cloneEventValue(*existing), Replayed: true}, nil
	}
	ref, err := SaveValuesWithRetention(ctx, store, retention, SaveValuesRequest{
		Owner:  ValueOwner{Kind: "node-attempt-verification", RunID: attempt.Invocation.RunID, Invocation: &attempt.Invocation, Attempt: &attempt},
		Values: set,
	})
	if err != nil {
		return VerificationRecord{}, &VerificationPersistenceError{Cause: err}
	}
	event, err := AppendMaskedEvent(ctx, store, AppendEventRequest{
		RunID: attempt.Invocation.RunID, Invocation: &attempt.Invocation, Attempt: &attempt,
		Type: EventNodeVerificationCompleted, OccurredAt: at, Attributes: attributes, Values: &ref,
		Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	}, redactor)
	if err != nil {
		copyRef := ref
		return VerificationRecord{}, &VerificationPersistenceError{Ref: &copyRef, Cause: err}
	}
	return VerificationRecord{Report: masked, Ref: ref, Event: event}, nil
}

func prepareExternalVerification(
	ctx context.Context,
	store StateStore,
	retention RetentionHook,
	redactor *values.Redactor,
	attempt AttemptID,
	report verification.Report,
) (pendingExternalVerification, error) {
	masked, set, attributes, err := prepareVerificationReport(attempt, report, redactor)
	if err != nil {
		return pendingExternalVerification{}, &VerificationPersistenceError{Cause: err}
	}
	ref, err := SaveValuesWithRetention(ctx, store, retention, SaveValuesRequest{
		Owner:  ValueOwner{Kind: "node-attempt-verification", RunID: attempt.Invocation.RunID, Invocation: &attempt.Invocation, Attempt: &attempt},
		Values: set,
	})
	if err != nil {
		return pendingExternalVerification{}, &VerificationPersistenceError{Cause: err}
	}
	return pendingExternalVerification{
		Report: masked, Ref: ref,
		Completion: ExternalVerificationCompletion{Values: ref, Attributes: attributes},
	}, nil
}

func verificationFromExternalApply(pending pendingExternalVerification, events []Event) (VerificationRecord, error) {
	var found *Event
	for index := range events {
		event := events[index]
		if event.Type != EventNodeVerificationCompleted {
			continue
		}
		if found != nil || event.Values == nil || *event.Values != pending.Ref || !reflect.DeepEqual(event.Attributes, pending.Completion.Attributes) {
			return VerificationRecord{}, ErrVerificationConflict
		}
		copyEvent := cloneEventValue(event)
		found = &copyEvent
	}
	if found == nil {
		return VerificationRecord{}, ErrVerificationConflict
	}
	return VerificationRecord{Report: pending.Report, Ref: pending.Ref, Event: *found}, nil
}

func prepareVerificationReport(attempt AttemptID, report verification.Report, redactor *values.Redactor) (verification.Report, values.ValueSet, map[string]string, error) {
	masked, err := maskVerificationReport(report, redactor)
	if err != nil {
		return verification.Report{}, nil, nil, err
	}
	set, err := verificationValueSet(attempt, masked)
	if err != nil {
		return verification.Report{}, nil, nil, err
	}
	return masked, set, verificationEventAttributes(masked), nil
}

func findVerificationEvent(ctx context.Context, store StateStore, attempt AttemptID) (*Event, error) {
	events, err := store.ListEvents(ctx, EventQuery{RunID: attempt.Invocation.RunID})
	if err != nil {
		return nil, err
	}
	var found *Event
	for index := range events {
		event := events[index]
		if event.Type != EventNodeVerificationCompleted || event.Attempt == nil || *event.Attempt != attempt {
			continue
		}
		if found != nil {
			return nil, ErrVerificationConflict
		}
		copyEvent := cloneEventValue(event)
		found = &copyEvent
	}
	return found, nil
}

func verificationValueSet(attempt AttemptID, report verification.Report) (values.ValueSet, error) {
	encoded, err := json.Marshal(report)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var inline any
	if decodeErr := decoder.Decode(&inline); decodeErr != nil {
		return nil, decodeErr
	}
	value, err := values.NewInline(inline, values.Metadata{
		Producer: values.Producer{
			Kind:      "workflow-verification",
			Reference: fmt.Sprintf("%s/%s/%s/%d", attempt.Invocation.RunID, attempt.Invocation.NodeID, attempt.Invocation.Iteration, attempt.Number),
			Output:    verificationValueName,
		},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		return nil, err
	}
	return values.ValueSet{verificationValueName: value}, nil
}

func reportFromValueSet(set values.ValueSet) (verification.Report, error) {
	if len(set) != 1 {
		return verification.Report{}, errors.New("verification value set must contain exactly one report")
	}
	value, exists := set[verificationValueName]
	if !exists || value.Type != values.TypeObject {
		return verification.Report{}, errors.New("verification value set is missing its object report")
	}
	encoded, err := json.Marshal(value.Inline)
	if err != nil {
		return verification.Report{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	var report verification.Report
	if err := decoder.Decode(&report); err != nil {
		return verification.Report{}, err
	}
	if err := report.Validate(); err != nil {
		return verification.Report{}, err
	}
	return report, nil
}

func verificationEventAttributes(report verification.Report) map[string]string {
	return map[string]string{
		"status": string(report.Status), "spec_digest": report.SpecDigest,
		"check_count": strconv.Itoa(len(report.Checks)), "evidence_count": strconv.Itoa(len(report.Evidence)),
	}
}

func maskVerificationReport(report verification.Report, redactor *values.Redactor) (verification.Report, error) {
	cloned, err := verification.CloneReport(report)
	if err != nil {
		return verification.Report{}, err
	}
	if redactor == nil {
		return cloned, cloned.Validate()
	}
	cloned.Reason = redactor.MaskString(cloned.Reason)
	for index := range cloned.Checks {
		check := &cloned.Checks[index]
		if redactor.MaskString(check.Kind) != check.Kind || redactor.MaskString(check.Version) != check.Version || redactor.MaskString(check.Code) != check.Code {
			return verification.Report{}, fmt.Errorf("%w: verifier identity contains resolved secret material", verification.ErrInvalidResult)
		}
		check.Message = redactor.MaskString(check.Message)
		if check.Details != nil {
			masked := make(map[string]string, len(check.Details))
			for key, value := range check.Details {
				maskedKey := redactor.MaskString(key)
				if _, collision := masked[maskedKey]; collision {
					return verification.Report{}, fmt.Errorf("%w: redacted verification detail keys collide", verification.ErrInvalidResult)
				}
				masked[maskedKey] = redactor.MaskString(value)
			}
			check.Details = masked
		}
	}
	for index := range cloned.Evidence {
		activity := &cloned.Evidence[index]
		if activity.ToolCall != nil {
			activity.ToolCall.Server = redactor.MaskString(activity.ToolCall.Server)
			activity.ToolCall.Tool = redactor.MaskString(activity.ToolCall.Tool)
		}
		if activity.Test != nil {
			activity.Test.Name = redactor.MaskString(activity.Test.Name)
		}
		if activity.Lint != nil {
			activity.Lint.Name = redactor.MaskString(activity.Lint.Name)
		}
	}
	return cloned, cloned.Validate()
}

func cloneEventValue(event Event) Event {
	cloned := event
	if event.Invocation != nil {
		invocation := *event.Invocation
		cloned.Invocation = &invocation
	}
	if event.Attempt != nil {
		attempt := *event.Attempt
		cloned.Attempt = &attempt
	}
	if event.Values != nil {
		ref := *event.Values
		cloned.Values = &ref
	}
	cloned.Attributes = cloneDispatchStringMap(event.Attributes)
	return cloned
}

func nilVerificationRegistry(registry verification.Registry) bool {
	if registry == nil {
		return true
	}
	value := reflect.ValueOf(registry)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func verificationDiagnosticsError(findings []diagnostic.Diagnostic) error {
	var failures []error
	for _, finding := range findings {
		if finding.Severity == diagnostic.SeverityError {
			failures = append(failures, fmt.Errorf("%s: %s", finding.Code, finding.Message))
		}
	}
	return errors.Join(failures...)
}

func verificationFailureMessage(code string) string {
	switch code {
	case "verification_failed":
		return "node verification rejected executor output"
	case "verification_provider_failed":
		return "verification provider or evaluator failed"
	case "verification_result_invalid":
		return "verification result is malformed"
	case "verification_persistence_failed":
		return "verification result persistence failed"
	case "verification_evidence_not_durable":
		return "verification evidence cannot cross this suspension boundary"
	default:
		return strings.ReplaceAll(code, "_", " ")
	}
}
