package verification

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	ReportSchemaVersion = "1"

	CodeInvalidCheck diagnostic.Code = "HADR-SOURCE-034"
	CodeUnknownCheck diagnostic.Code = "HADR-SOURCE-035"
)

var (
	ErrInvalidSpec     = errors.New("invalid verification spec")
	ErrUnknownVerifier = errors.New("unknown verifier")
	ErrInvalidResult   = errors.New("invalid verification result")
	ErrInvalidDecision = errors.New("invalid reviewer decision")
	ErrRecorderFrozen  = errors.New("verification activity recorder is frozen")
)

type VerifierMode string

const (
	ModeDeterministic VerifierMode = "deterministic"
	ModeReviewer      VerifierMode = "reviewer"
)

func (m VerifierMode) Valid() bool { return m == ModeDeterministic || m == ModeReviewer }

// VerifierSpec is immutable registry metadata. ConfigSchema describes the
// opaque graph check config; RequiredEvidence is an inspectable fail-closed
// declaration used by compilers and policy before execution.
type VerifierSpec struct {
	Kind             string         `json:"kind"`
	Version          string         `json:"version"`
	Mode             VerifierMode   `json:"mode"`
	ConfigSchema     graph.Schema   `json:"config_schema"`
	RequiredEvidence []ActivityKind `json:"required_evidence,omitempty"`
}

func (s VerifierSpec) Validate() error {
	if err := validateText("verifier kind", s.Kind, true); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSpec, err)
	}
	if err := validateText("verifier version", s.Version, true); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSpec, err)
	}
	if !s.Mode.Valid() {
		return fmt.Errorf("%w: unsupported verifier mode %q", ErrInvalidSpec, s.Mode)
	}
	if s.ConfigSchema == nil {
		return fmt.Errorf("%w: config schema is required", ErrInvalidSpec)
	}
	if err := values.ValidateSchema(s.ConfigSchema); err != nil {
		return fmt.Errorf("%w: config schema: %w", ErrInvalidSpec, err)
	}
	seen := make(map[ActivityKind]struct{}, len(s.RequiredEvidence))
	for _, kind := range s.RequiredEvidence {
		if !kind.Valid() {
			return fmt.Errorf("%w: unsupported required evidence %q", ErrInvalidSpec, kind)
		}
		if _, duplicate := seen[kind]; duplicate {
			return fmt.Errorf("%w: duplicate required evidence %q", ErrInvalidSpec, kind)
		}
		seen[kind] = struct{}{}
	}
	return nil
}

type ActivityKind string

const (
	ActivityToolCall ActivityKind = "tool_call"
	ActivityTest     ActivityKind = "test"
	ActivityLint     ActivityKind = "lint"
)

func (k ActivityKind) Valid() bool {
	switch k {
	case ActivityToolCall, ActivityTest, ActivityLint:
		return true
	default:
		return false
	}
}

type ActivityOutcome string

const (
	ActivitySucceeded ActivityOutcome = "succeeded"
	ActivityFailed    ActivityOutcome = "failed"
	ActivitySkipped   ActivityOutcome = "skipped"
)

func (o ActivityOutcome) Valid() bool {
	switch o {
	case ActivitySucceeded, ActivityFailed, ActivitySkipped:
		return true
	default:
		return false
	}
}

// ToolCall is literal evidence emitted at an actual tool client boundary. It
// intentionally excludes arguments, results, credentials, and arbitrary
// transport metadata.
type ToolCall struct {
	Server  string          `json:"server"`
	Tool    string          `json:"tool"`
	Outcome ActivityOutcome `json:"outcome"`
}

// TestRun is literal evidence emitted by a trusted test harness boundary.
type TestRun struct {
	Name    string          `json:"name"`
	Outcome ActivityOutcome `json:"outcome"`
}

// LintRun is literal evidence emitted by a trusted linter boundary.
type LintRun struct {
	Name    string          `json:"name"`
	Outcome ActivityOutcome `json:"outcome"`
}

// Activity is a closed, unambiguous evidence envelope. Sequence is assigned by
// the runtime-issued recorder and defines stable observation order.
type Activity struct {
	Sequence uint64       `json:"sequence"`
	Kind     ActivityKind `json:"kind"`
	ToolCall *ToolCall    `json:"tool_call,omitempty"`
	Test     *TestRun     `json:"test,omitempty"`
	Lint     *LintRun     `json:"lint,omitempty"`
}

func (a Activity) Validate() error {
	if a.Sequence == 0 {
		return errors.New("activity sequence must be positive")
	}
	if !a.Kind.Valid() {
		return fmt.Errorf("unsupported activity kind %q", a.Kind)
	}
	payloads := 0
	if a.ToolCall != nil {
		payloads++
	}
	if a.Test != nil {
		payloads++
	}
	if a.Lint != nil {
		payloads++
	}
	if payloads != 1 {
		return errors.New("activity requires exactly one typed payload")
	}
	switch a.Kind {
	case ActivityToolCall:
		if a.ToolCall == nil || a.Test != nil || a.Lint != nil {
			return errors.New("tool_call activity requires only tool_call payload")
		}
		if err := validateText("tool-call server", a.ToolCall.Server, true); err != nil {
			return err
		}
		if err := validateText("tool-call tool", a.ToolCall.Tool, true); err != nil {
			return err
		}
		if !a.ToolCall.Outcome.Valid() {
			return fmt.Errorf("unsupported tool-call outcome %q", a.ToolCall.Outcome)
		}
	case ActivityTest:
		if a.Test == nil || a.ToolCall != nil || a.Lint != nil {
			return errors.New("test activity requires only test payload")
		}
		if err := validateText("test name", a.Test.Name, true); err != nil {
			return err
		}
		if !a.Test.Outcome.Valid() {
			return fmt.Errorf("unsupported test outcome %q", a.Test.Outcome)
		}
	case ActivityLint:
		if a.Lint == nil || a.ToolCall != nil || a.Test != nil {
			return errors.New("lint activity requires only lint payload")
		}
		if err := validateText("lint name", a.Lint.Name, true); err != nil {
			return err
		}
		if !a.Lint.Outcome.Valid() {
			return fmt.Errorf("unsupported lint outcome %q", a.Lint.Outcome)
		}
	}
	return nil
}

type CheckOutcome string

const (
	CheckPassed CheckOutcome = "passed"
	CheckFailed CheckOutcome = "failed"
	CheckError  CheckOutcome = "error"
)

func (o CheckOutcome) Valid() bool { return o == CheckPassed || o == CheckFailed || o == CheckError }

// CheckResult is one durable, source-mapped verifier decision.
type CheckResult struct {
	Index   int               `json:"index"`
	Kind    string            `json:"kind"`
	Version string            `json:"version"`
	Outcome CheckOutcome      `json:"outcome"`
	Code    string            `json:"code"`
	Message string            `json:"message"`
	Details map[string]string `json:"details,omitempty"`
	Source  *graph.SourceRef  `json:"source,omitempty"`
}

func (r CheckResult) Validate() error {
	if r.Index < 0 {
		return fmt.Errorf("%w: check index must not be negative", ErrInvalidResult)
	}
	if err := validateText("check kind", r.Kind, true); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidResult, err)
	}
	if err := validateText("check version", r.Version, true); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidResult, err)
	}
	if !r.Outcome.Valid() {
		return fmt.Errorf("%w: unsupported check outcome %q", ErrInvalidResult, r.Outcome)
	}
	if err := validateText("check code", r.Code, true); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidResult, err)
	}
	if err := validateText("check message", r.Message, true); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidResult, err)
	}
	if err := validateStringMap("check details", r.Details); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidResult, err)
	}
	if r.Source != nil {
		probe := diagnostic.Diagnostic{Severity: diagnostic.SeverityError, Code: CodeInvalidCheck, Message: "source", Source: r.Source}
		if err := probe.Validate(); err != nil {
			return fmt.Errorf("%w: source: %w", ErrInvalidResult, err)
		}
	}
	return nil
}

type ReportStatus string

const (
	ReportPassed ReportStatus = "passed"
	ReportFailed ReportStatus = "failed"
	ReportNotRun ReportStatus = "not_run"
)

func (s ReportStatus) Valid() bool {
	return s == ReportPassed || s == ReportFailed || s == ReportNotRun
}

// Report is the durable verification data-plane payload. SpecDigest binds it
// to the source declaration; Evidence contains only frozen literal activity.
type Report struct {
	SchemaVersion string        `json:"schema_version"`
	SpecDigest    string        `json:"spec_digest"`
	Status        ReportStatus  `json:"status"`
	Reason        string        `json:"reason,omitempty"`
	Checks        []CheckResult `json:"checks,omitempty"`
	Evidence      []Activity    `json:"evidence,omitempty"`
}

func (r Report) Validate() error {
	if r.SchemaVersion != ReportSchemaVersion {
		return fmt.Errorf("%w: unsupported report schema version %q", ErrInvalidResult, r.SchemaVersion)
	}
	if err := values.ValidateDigest(r.SpecDigest); err != nil {
		return fmt.Errorf("%w: report spec digest: %w", ErrInvalidResult, err)
	}
	if !r.Status.Valid() {
		return fmt.Errorf("%w: unsupported report status %q", ErrInvalidResult, r.Status)
	}
	if r.Status == ReportNotRun {
		if len(r.Checks) != 0 || strings.TrimSpace(r.Reason) == "" {
			return fmt.Errorf("%w: not_run report requires only a reason", ErrInvalidResult)
		}
	} else if len(r.Checks) == 0 || r.Reason != "" {
		return fmt.Errorf("%w: completed report requires checks and no not-run reason", ErrInvalidResult)
	}
	failed := false
	for index, check := range r.Checks {
		if err := check.Validate(); err != nil {
			return fmt.Errorf("check[%d]: %w", index, err)
		}
		if check.Index != index {
			return fmt.Errorf("%w: check[%d] index is %d", ErrInvalidResult, index, check.Index)
		}
		failed = failed || check.Outcome != CheckPassed
	}
	if r.Status == ReportPassed && failed {
		return fmt.Errorf("%w: passed report contains unsuccessful check", ErrInvalidResult)
	}
	if r.Status == ReportFailed && !failed {
		return fmt.Errorf("%w: failed report contains only passed checks", ErrInvalidResult)
	}
	for index, activity := range r.Evidence {
		if err := activity.Validate(); err != nil {
			return fmt.Errorf("evidence[%d]: %w", index, err)
		}
		if activity.Sequence != uint64(index+1) {
			return fmt.Errorf("%w: evidence sequence is not contiguous", ErrInvalidResult)
		}
	}
	return nil
}

// Request is a defensive verifier input. OutputSchema is the registered kind's
// trusted output schema; Outputs and Evidence are typed runtime observations.
type Request struct {
	Check        graph.VerificationCheck
	OutputSchema graph.Schema
	Outputs      values.ValueSet
	Evidence     []Activity
}

type Verifier interface {
	Spec() VerifierSpec
	ValidateConfig(context.Context, graph.VerificationCheck) []diagnostic.Diagnostic
	Verify(context.Context, Request) (CheckResult, error)
}

type Registry interface {
	Register(Verifier) error
	Lookup(kind string) (Verifier, bool)
	List() []VerifierSpec
}

// SpecDigest hashes only semantic verification fields. Source locations do not
// affect replay identity.
func SpecDigest(spec graph.VerificationSpec) (string, error) {
	copySpec, err := cloneJSON(spec)
	if err != nil {
		return "", err
	}
	for index := range copySpec.Checks {
		copySpec.Checks[index].Source = nil
	}
	copySpec.Extension.Source = nil
	encoded, err := json.Marshal(copySpec)
	if err != nil {
		return "", err
	}
	return values.SHA256Digest(encoded), nil
}

func CloneReport(report Report) (Report, error)            { return cloneJSON(report) }
func CloneActivities(input []Activity) ([]Activity, error) { return cloneJSON(input) }
func CloneCheck(check graph.VerificationCheck) (graph.VerificationCheck, error) {
	return cloneJSON(check)
}
func CloneOutputs(output values.ValueSet) (values.ValueSet, error) { return cloneJSON(output) }
func CloneSchema(schema graph.Schema) (graph.Schema, error)        { return cloneJSON(schema) }

func cloneJSON[T any](input T) (T, error) {
	var zero T
	encoded, err := json.Marshal(input)
	if err != nil {
		return zero, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	var output T
	if err := decoder.Decode(&output); err != nil {
		return zero, err
	}
	if err := requireEOF(decoder); err != nil {
		return zero, err
	}
	return output, nil
}

func requireEOF(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New("multiple JSON values")
}

func validateText(name, value string, required bool) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s must contain valid UTF-8", name)
	}
	if required && (strings.TrimSpace(value) == "" || value != strings.TrimSpace(value)) {
		return fmt.Errorf("%s is required without surrounding whitespace", name)
	}
	if !required && value != "" && value != strings.TrimSpace(value) {
		return fmt.Errorf("%s must not contain surrounding whitespace", name)
	}
	for _, current := range value {
		if unicode.IsControl(current) {
			return fmt.Errorf("%s must not contain control characters", name)
		}
	}
	return nil
}

func validateStringMap(name string, input map[string]string) error {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := validateText(name+" key", key, true); err != nil {
			return err
		}
		if err := validateText(name+"["+key+"]", input[key], false); err != nil {
			return err
		}
	}
	return nil
}
