package stepkind

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

// RetryClassification describes an individual execution failure. It is
// distinct from RetrySafety: the former classifies what happened, while the
// latter declares whether repeating the kind can ever be safe.
type RetryClassification string

const (
	RetryUnspecified RetryClassification = "unspecified"
	Retryable        RetryClassification = "retryable"
	RetryPermanent   RetryClassification = "permanent"
)

// Valid reports whether c is a supported retry classification.
func (c RetryClassification) Valid() bool {
	switch c {
	case RetryUnspecified, Retryable, RetryPermanent:
		return true
	default:
		return false
	}
}

// RetryClassifier may be implemented by adapter errors without requiring the
// concrete error type to cross the core boundary.
type RetryClassifier interface {
	RetryClassification() RetryClassification
}

// ExecutionError is the structured application-neutral adapter failure used
// for persistence and retry policy. Message and Details are declarations that
// are safe to persist after known-secret masking. Cause is process-local and
// never serialized. Adapters should return an unstructured error when they
// cannot make that persistence-safety guarantee.
type ExecutionError struct {
	Code           string              `json:"code"`
	Message        string              `json:"message"`
	Classification RetryClassification `json:"retry_classification"`
	Details        map[string]string   `json:"details,omitempty"`
	Cause          error               `json:"-"`
}

// Error implements error.
func (e *ExecutionError) Error() string {
	if e == nil {
		return ""
	}
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

// Unwrap exposes the process-local cause to errors.Is/errors.As.
func (e *ExecutionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// RetryClassification implements RetryClassifier.
func (e *ExecutionError) RetryClassification() RetryClassification {
	if e == nil {
		return RetryUnspecified
	}
	return e.Classification
}

// Validate reports malformed structured execution failure metadata.
func (e ExecutionError) Validate() error {
	if err := validateRuntimeText("execution error code", e.Code); err != nil {
		return err
	}
	if err := validateRuntimeText("execution error message", e.Message); err != nil {
		return err
	}
	if !e.Classification.Valid() {
		return fmt.Errorf("unsupported retry classification %q", e.Classification)
	}
	return validateRuntimeStringMap("execution error details", e.Details)
}

// ClassifyError returns the first valid retry classification exposed by err's
// chain. Unknown, nil, or malformed classifiers remain unspecified.
func ClassifyError(err error) RetryClassification {
	if err == nil {
		return RetryUnspecified
	}
	var classified RetryClassifier
	if errors.As(err, &classified) {
		classification := classified.RetryClassification()
		if classification.Valid() {
			return classification
		}
	}
	return RetryUnspecified
}

// Validate checks runtime identity, JSON-compatible config, and typed inputs.
func (i Invocation) Validate() error {
	if err := i.Identity.Validate(); err != nil {
		return err
	}
	if i.Config == nil {
		return fmt.Errorf("invocation config must be an object, not null")
	}
	if err := validateRuntimeJSON(i.Config); err != nil {
		return fmt.Errorf("invocation config must be JSON-compatible: %w", err)
	}
	if err := i.Inputs.Validate(); err != nil {
		return fmt.Errorf("invocation inputs: %w", err)
	}
	if i.Call != nil {
		if !i.Call.Spec.Mode.Valid() {
			return fmt.Errorf("invocation call has unsupported mode %q", i.Call.Spec.Mode)
		}
		if i.Call.Spec.OnParentClose != "" && !i.Call.Spec.OnParentClose.Valid() {
			return fmt.Errorf("invocation call has unsupported parent-close policy %q", i.Call.Spec.OnParentClose)
		}
		if err := validateRuntimeJSON(i.Call); err != nil {
			return fmt.Errorf("invocation call must be JSON-compatible: %w", err)
		}
	}
	if i.Continuation != nil {
		if err := i.Continuation.Validate(); err != nil {
			return fmt.Errorf("invocation continuation: %w", err)
		}
	}
	if err := validateOptionalRuntimeText("invocation idempotency key", i.IdempotencyKey); err != nil {
		return err
	}
	return nil
}

// Validate requires an exact resumed wait and digest-bound persistable values.
func (c WaitContinuation) Validate() error {
	if err := validateRuntimeText("wait continuation id", c.ID); err != nil {
		return err
	}
	if err := c.Record.Validate(); err != nil {
		return fmt.Errorf("wait continuation record: %w", err)
	}
	if c.Record.Status != workflowwait.StatusResumed || c.Record.ResumeValues == nil {
		return fmt.Errorf("wait continuation requires resumed state and resume values")
	}
	if err := values.ValidatePersistableSet(c.Values); err != nil {
		return fmt.Errorf("wait continuation values: %w", err)
	}
	ref, err := values.NewValueSetRef(c.Record.ResumeValues.ID, c.Values)
	if err != nil {
		return fmt.Errorf("wait continuation values: %w", err)
	}
	if ref != *c.Record.ResumeValues {
		return fmt.Errorf("wait continuation values must match resume_values reference")
	}
	return nil
}

// Validate reports a malformed application-neutral invocation identity.
func (i InvocationIdentity) Validate() error {
	if err := validateRuntimeText("invocation run id", i.RunID); err != nil {
		return err
	}
	if err := graph.ValidateID(i.NodeID); err != nil {
		return fmt.Errorf("invocation node id: %w", err)
	}
	if err := validateOptionalRuntimeText("invocation iteration", i.Iteration); err != nil {
		return err
	}
	if i.Attempt < 1 {
		return fmt.Errorf("invocation attempt must be positive")
	}
	return nil
}

// Validate checks that a completed result is safe to persist.
func (r StepResult) Validate() error {
	if !r.Outcome.Valid() {
		return fmt.Errorf("unsupported step outcome %q", r.Outcome)
	}
	switch r.Outcome {
	case StepCompleted:
		if r.Outputs == nil || r.Wait != nil || r.External != nil {
			return fmt.Errorf("completed outcome requires only outputs")
		}
		if err := values.ValidatePersistableSet(r.Outputs); err != nil {
			return fmt.Errorf("step outputs: %w", err)
		}
	case StepWaiting:
		if r.Outputs != nil || r.Wait == nil || r.External != nil {
			return fmt.Errorf("waiting outcome requires only a wait")
		}
		if err := r.Wait.Validate(); err != nil {
			return err
		}
	case StepExternal:
		if r.Outputs != nil || r.Wait != nil || r.External == nil {
			return fmt.Errorf("external outcome requires only an external operation reference")
		}
		if err := r.External.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Validate checks the canonical open wait handoff without persisting the raw
// resume token. Token/digest equality is enforced by runtime.WaitCoordinator.
func (r WaitResult) Validate() error {
	if err := validateRuntimeText("wait id", r.ID); err != nil {
		return err
	}
	if err := r.Record.Validate(); err != nil {
		return fmt.Errorf("wait record: %w", err)
	}
	if r.Record.Status != workflowwait.StatusOpen {
		return fmt.Errorf("step wait record must be open")
	}
	if !utf8.ValidString(r.ResumeToken) {
		return fmt.Errorf("wait resume token must contain valid UTF-8")
	}
	return nil
}

// Validate reports malformed durable external-operation identity.
func (r ExternalOperationRef) Validate() error {
	if err := validateRuntimeText("external operation kind", r.Kind); err != nil {
		return err
	}
	if err := validateRuntimeText("external operation id", r.ID); err != nil {
		return err
	}
	return validateRuntimeStringMap("external operation metadata", r.Metadata)
}

// Validate checks the observation's closed state and mutually exclusive
// terminal outcome fields.
func (o Observation) Validate() error {
	if !o.State.Valid() {
		return fmt.Errorf("unsupported observation state %q", o.State)
	}
	if err := validateRuntimeStringMap("observation progress", o.Progress); err != nil {
		return err
	}
	switch o.State {
	case ObservationPending:
		if o.Result != nil || o.Failure != nil {
			return fmt.Errorf("pending observation must not contain a terminal outcome")
		}
	case ObservationSucceeded:
		if o.Result == nil || o.Failure != nil {
			return fmt.Errorf("succeeded observation requires only a result")
		}
		if err := o.Result.Validate(); err != nil {
			return err
		}
		if o.Result.Outcome != StepCompleted {
			return fmt.Errorf("succeeded observation must produce a completed result")
		}
	case ObservationFailed, ObservationCanceled:
		if o.Result != nil || o.Failure == nil {
			return fmt.Errorf("unsuccessful observation requires only a failure")
		}
		if err := o.Failure.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func validateRuntimeText(name, value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s must contain valid UTF-8", name)
	}
	if strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) {
		return fmt.Errorf("%s is required without surrounding whitespace", name)
	}
	return nil
}

func validateOptionalRuntimeText(name, value string) error {
	if value == "" {
		return nil
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s must contain valid UTF-8", name)
	}
	if value != strings.TrimSpace(value) {
		return fmt.Errorf("%s must not contain surrounding whitespace", name)
	}
	for _, current := range value {
		if unicode.IsControl(current) {
			return fmt.Errorf("%s must not contain control characters", name)
		}
	}
	return nil
}

func validateRuntimeStringMap(name string, values map[string]string) error {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := values[key]
		if err := validateRuntimeText(name+" key", key); err != nil {
			return err
		}
		if !utf8.ValidString(value) {
			return fmt.Errorf("%s[%q] must contain valid UTF-8", name, key)
		}
	}
	return nil
}

func validateRuntimeJSON(value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.UseNumber()
	var cloned any
	if err := decoder.Decode(&cloned); err != nil {
		return err
	}
	return nil
}
