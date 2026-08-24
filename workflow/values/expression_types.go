package values

import (
	"fmt"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
)

const (
	// CodeExpressionSyntax identifies malformed expression-language source.
	CodeExpressionSyntax diagnostic.Code = "HADR-VALUE-001"
	// CodeExpressionUnresolved identifies a name or member absent from the
	// supplied typed evaluation context.
	CodeExpressionUnresolved diagnostic.Code = "HADR-VALUE-002"
	// CodeExpressionType identifies an expression type mismatch.
	CodeExpressionType diagnostic.Code = "HADR-VALUE-003"
	// CodeExpressionEnvDenied identifies an env reference rejected by policy.
	CodeExpressionEnvDenied diagnostic.Code = "HADR-VALUE-004"
	// CodeExpressionInvisibleStep identifies a step reference outside the
	// caller-declared visibility set.
	CodeExpressionInvisibleStep diagnostic.Code = "HADR-VALUE-005"
	// CodeInterpolation identifies malformed interpolation or a result that
	// cannot be interpolated implicitly.
	CodeInterpolation diagnostic.Code = "HADR-VALUE-006"
	// CodeExpressionValue identifies an invalid context or non-JSON result.
	CodeExpressionValue diagnostic.Code = "HADR-VALUE-007"
)

// StepContext is the expression-visible state of one completed or observable
// node. Outputs remain typed Value envelopes until the engine unwraps a private
// evaluation copy. Error must be nil or a JSON-compatible structured value.
type StepContext struct {
	Outputs ValueSet
	Status  string
	Error   any
	Items   []StepContext
}

// ExpressionContext contains the standard expression roots. Inputs, step
// outputs, the optional fan-out item, and env enter through Value envelopes.
// Run, RunScope, and ExecutionTarget are host-provided JSON-compatible
// metadata. Env is never populated from the ambient process environment.
type ExpressionContext struct {
	Inputs          ValueSet
	Steps           map[string]StepContext
	Item            *Value
	Index           *int
	Run             map[string]any
	RunScope        map[string]any
	ExecutionTarget map[string]any
	Env             ValueSet
}

// ExpressionOptions carries caller policy that must be enforced for every
// evaluation, including cache hits. AllowEnv exposes only ExpressionContext.Env.
// A nil VisibleSteps permits every step in the context; a non-nil slice is the
// complete visibility allowlist, including when it is empty.
type ExpressionOptions struct {
	AllowEnv     bool
	VisibleSteps []string
}

// Reference is one structural reference to a standard expression root. Path
// contains statically named members. Dynamic is true when at least one member
// is selected by a computed expression. The contract intentionally reports
// references without inferring graph edges.
type Reference struct {
	Root    string
	Path    []string
	Dynamic bool
}

// ExpressionError couples a stable workflow diagnostic with the underlying
// parser, checker, evaluator, or value-validation failure.
type ExpressionError struct {
	Diagnostic diagnostic.Diagnostic
	Cause      error
}

func (e *ExpressionError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Cause == nil {
		return e.Diagnostic.Message
	}
	return fmt.Sprintf("%s: %v", e.Diagnostic.Message, e.Cause)
}

// Unwrap exposes the underlying engine or value-validation error.
func (e *ExpressionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func expressionError(code diagnostic.Code, message string, source *graph.SourceRef, cause error) error {
	return &ExpressionError{
		Diagnostic: diagnostic.Diagnostic{
			Severity: diagnostic.SeverityError,
			Code:     code,
			Message:  message,
			Source:   cloneSourceRef(source),
		},
		Cause: cause,
	}
}

func cloneSourceRef(source *graph.SourceRef) *graph.SourceRef {
	if source == nil {
		return nil
	}
	clone := *source
	clone.Path = append([]string(nil), source.Path...)
	return &clone
}
