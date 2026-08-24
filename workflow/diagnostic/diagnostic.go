package diagnostic

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/hollis-labs/hadron/workflow/graph"
)

// Severity describes how a diagnostic affects the requested operation.
type Severity string

const (
	// SeverityInfo reports actionable context that does not affect validity.
	SeverityInfo Severity = "info"
	// SeverityWarning reports accepted input that should be changed.
	SeverityWarning Severity = "warning"
	// SeverityError reports input or state that prevents the operation.
	SeverityError Severity = "error"
)

// Valid reports whether s is a supported severity.
func (s Severity) Valid() bool {
	switch s {
	case SeverityInfo, SeverityWarning, SeverityError:
		return true
	default:
		return false
	}
}

// Diagnostic is the stable transport representation of a workflow finding.
// Source and Related reuse graph.SourceRef so graph and diagnostics share one
// source-location contract.
type Diagnostic struct {
	Severity    Severity           `json:"severity"`
	Code        Code               `json:"code"`
	Message     string             `json:"message"`
	Source      *graph.SourceRef   `json:"source,omitempty"`
	Related     []RelatedReference `json:"related,omitempty"`
	Remediation *Remediation       `json:"remediation,omitempty"`
}

// RelatedReference points to source material that helps explain a diagnostic.
type RelatedReference struct {
	Message string          `json:"message,omitempty"`
	Source  graph.SourceRef `json:"source"`
}

// Remediation describes an author action and, when useful, target syntax.
type Remediation struct {
	Message         string `json:"message"`
	SuggestedSyntax string `json:"suggested_syntax,omitempty"`
	Documentation   string `json:"documentation,omitempty"`
}

// ValidationError identifies one malformed diagnostic transport field.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("diagnostic %s: %s", e.Field, e.Reason)
}

// Validate checks the invariants required to safely present or transport d.
// It intentionally does not attempt compiler, policy, or runtime validation.
func (d Diagnostic) Validate() error {
	var errs []error
	if !d.Severity.Valid() {
		errs = append(errs, fieldError("severity", "must be info, warning, or error"))
	}
	if err := d.Code.Validate(); err != nil {
		errs = append(errs, fieldError("code", err.Error()))
	}
	if strings.TrimSpace(d.Message) == "" {
		errs = append(errs, fieldError("message", "must not be empty"))
	}
	if d.Source != nil {
		errs = append(errs, validateSource("source", *d.Source)...)
	}
	for i, related := range d.Related {
		errs = append(errs, validateSource(fmt.Sprintf("related[%d].source", i), related.Source)...)
	}
	if d.Remediation != nil && strings.TrimSpace(d.Remediation.Message) == "" {
		errs = append(errs, fieldError("remediation.message", "must not be empty"))
	}
	return errors.Join(errs...)
}

// EncodeJSON validates and encodes one diagnostic for a transport boundary.
func EncodeJSON(d Diagnostic) ([]byte, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(d)
}

// DecodeJSON decodes and validates one diagnostic from a transport boundary.
// Unknown fields and trailing JSON values are rejected to keep the contract
// explicit and malformed transport payloads detectable.
func DecodeJSON(data []byte) (Diagnostic, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	var diagnostic Diagnostic
	if err := decoder.Decode(&diagnostic); err != nil {
		return Diagnostic{}, fmt.Errorf("decode diagnostic JSON: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return Diagnostic{}, err
	}
	if err := diagnostic.Validate(); err != nil {
		return Diagnostic{}, err
	}
	return diagnostic, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("decode diagnostic JSON: %w", err)
	}
	return errors.New("decode diagnostic JSON: multiple JSON values")
}

func validateSource(field string, source graph.SourceRef) []error {
	var errs []error
	if !source.Format.Valid() {
		errs = append(errs, fieldError(field+".format", "must be a supported graph source format"))
	}
	if strings.TrimSpace(source.Locator) == "" {
		errs = append(errs, fieldError(field+".locator", "must not be empty"))
	}
	if source.StartLine < 0 || source.StartColumn < 0 || source.EndLine < 0 || source.EndColumn < 0 {
		errs = append(errs, fieldError(field, "line and column positions must not be negative"))
	}
	if source.EndLine > 0 && source.StartLine == 0 {
		errs = append(errs, fieldError(field+".start_line", "must be set when end_line is set"))
	}
	if source.StartColumn > 0 && source.StartLine == 0 {
		errs = append(errs, fieldError(field+".start_column", "must not be set when start_line is absent"))
	}
	if source.EndColumn > 0 && source.EndLine == 0 {
		errs = append(errs, fieldError(field+".end_column", "must not be set when end_line is absent"))
	}
	if source.StartLine > 0 && source.EndLine > 0 && source.EndLine < source.StartLine {
		errs = append(errs, fieldError(field+".end_line", "must not precede start_line"))
	}
	if source.StartLine > 0 && source.EndLine == source.StartLine && source.StartColumn > 0 && source.EndColumn > 0 && source.EndColumn < source.StartColumn {
		errs = append(errs, fieldError(field+".end_column", "must not precede start_column on the same line"))
	}
	return errs
}

func fieldError(field, reason string) error {
	return &ValidationError{Field: field, Reason: reason}
}
