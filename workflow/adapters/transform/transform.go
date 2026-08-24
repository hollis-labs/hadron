package transform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/parser"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	// Name is the canonical registered transform kind name.
	Name = "transform"
	// Version is the immutable initial transform contract version.
	Version = "v1"

	// CodeContextUnavailable identifies an invalid or unavailable invocation
	// expression context. Its process-local cause is never copied into Message.
	CodeContextUnavailable = "transform_context_unavailable"
	// CodeExpressionFailed identifies failure of one named output expression.
	CodeExpressionFailed = "transform_expression_failed"
	// CodeOutputInvalid identifies an evaluated result that cannot become a
	// persistable typed workflow value.
	CodeOutputInvalid = "transform_output_invalid"
)

// ContextProvider derives a fresh, deterministic, invocation-scoped expression
// context. Implementations must be concurrency-safe, must not mutate invocation,
// and must not consult ambient process state. In particular, environment values
// are always removed by Executor. Runtime/compiler visibility policy must scope
// Steps before returning the context.
type ContextProvider interface {
	ExpressionContext(context.Context, stepkind.Invocation) (values.ExpressionContext, error)
}

// ContextProviderFunc adapts a function to ContextProvider.
type ContextProviderFunc func(context.Context, stepkind.Invocation) (values.ExpressionContext, error)

// ExpressionContext implements ContextProvider.
func (f ContextProviderFunc) ExpressionContext(ctx context.Context, invocation stepkind.Invocation) (values.ExpressionContext, error) {
	return f(ctx, invocation)
}

// Executor is the pure transform StepKind. The zero value uses an Inputs-only
// expression context; New is preferred when registering the kind.
type Executor struct {
	engine   *values.ExpressionEngine
	provider ContextProvider
}

// New returns a transform executor whose expression context contains only the
// invocation's typed Inputs.
func New() *Executor {
	return &Executor{engine: values.NewExpressionEngine()}
}

// NewWithContextProvider returns a transform executor that augments invocation
// Inputs with a pre-scoped expression context. The provider cannot enable env.
func NewWithContextProvider(provider ContextProvider) (*Executor, error) {
	if nilInterface(provider) {
		return nil, errors.New("transform context provider is required")
	}
	return &Executor{engine: values.NewExpressionEngine(), provider: provider}, nil
}

// Spec returns immutable metadata for transform@v1. Runtime validates the
// actual named output set against the graph node's declared output schemas.
func (e *Executor) Spec() stepkind.StepKindSpec {
	return stepkind.StepKindSpec{
		Name:    Name,
		Version: Version,
		ConfigSchema: graph.Schema{
			"type":          "object",
			"minProperties": json.Number("1"),
			"propertyNames": map[string]any{
				"pattern":   `^[a-z0-9]+(?:-[a-z0-9]+)*$`,
				"maxLength": json.Number(strconv.Itoa(graph.MaxIDLength)),
			},
			"additionalProperties": map[string]any{"type": "string", "minLength": json.Number("1")},
		},
		InputSchema:           graph.Schema{"type": "object"},
		OutputSchema:          graph.Schema{"type": "object"},
		Effects:               graph.EffectSet{graph.EffectCompute},
		Idempotency:           graph.IdempotencyIntrinsic,
		RetrySafety:           stepkind.RetrySafe,
		Cancellation:          stepkind.CancellationSpec{Mode: stepkind.CancellationContext},
		Observation:           stepkind.ObservationSpec{Mode: stepkind.ObservationNone},
		CanSuspend:            false,
		EmbeddedModeSupported: true,
	}
}

// ValidateConfig validates normalized named outputs and raw expression syntax.
// Config paths identify the expression so compiler-owned source maps can attach
// the corresponding SourceRef without the adapter inventing source locations.
func (e *Executor) ValidateConfig(_ context.Context, config graph.Config) []diagnostic.Diagnostic {
	_, findings := parseConfig(config)
	return findings
}

// Execute evaluates every named expression against one immutable context and
// returns typed, persistable outputs. Outputs are evaluated in normalized name
// order; sibling outputs are not expression roots and cannot observe each other.
func (e *Executor) Execute(ctx context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	if ctx == nil {
		return stepkind.StepResult{}, contextFailure(errors.New("context is required"), stepkind.RetryPermanent)
	}
	if err := ctx.Err(); err != nil {
		return stepkind.StepResult{}, err
	}
	if err := prepared.Invocation.Validate(); err != nil {
		return stepkind.StepResult{}, contextFailure(err, stepkind.RetryPermanent)
	}
	expressions, findings := parseConfig(prepared.Invocation.Config)
	if len(findings) != 0 {
		return stepkind.StepResult{}, &stepkind.ExecutionError{
			Code: CodeExpressionFailed, Message: "transform config contains invalid output expressions",
			Classification: stepkind.RetryPermanent,
			Details:        map[string]string{"expression": findings[0].Message},
		}
	}

	invocation, err := cloneInvocation(prepared.Invocation)
	if err != nil {
		return stepkind.StepResult{}, contextFailure(err, stepkind.RetryPermanent)
	}
	authoritativeInputs := invocation.Inputs
	expressionContext := values.ExpressionContext{Inputs: authoritativeInputs}
	if e != nil && !nilInterface(e.provider) {
		providerInvocation, cloneErr := cloneInvocation(invocation)
		if cloneErr != nil {
			return stepkind.StepResult{}, contextFailure(cloneErr, stepkind.RetryPermanent)
		}
		expressionContext, err = e.provider.ExpressionContext(ctx, providerInvocation)
		if err != nil {
			return stepkind.StepResult{}, contextFailure(err, stepkind.Retryable)
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return stepkind.StepResult{}, ctxErr
		}
		// Invocation inputs are authoritative even when a provider supplies the
		// remaining roots. Env is never available to transform expressions.
		expressionContext.Inputs = authoritativeInputs
		expressionContext.Env = nil
	}
	expressionContext, err = cloneContext(expressionContext)
	if err != nil {
		return stepkind.StepResult{}, contextFailure(err, stepkind.RetryPermanent)
	}

	engine := values.NewExpressionEngine()
	if e != nil && e.engine != nil {
		engine = e.engine
	}
	visibleSteps := sortedStepNames(expressionContext.Steps)
	options := values.ExpressionOptions{VisibleSteps: visibleSteps}
	outputs := make(values.ValueSet, len(expressions))
	for _, output := range expressions {
		if err := ctx.Err(); err != nil {
			return stepkind.StepResult{}, err
		}
		value, evaluationErr := evaluateContext(ctx, engine, graph.Expression{Text: output.text}, expressionContext, options)
		if evaluationErr != nil {
			return stepkind.StepResult{}, expressionFailure(output, evaluationErr)
		}
		typed, valueErr := values.NewInline(value, outputMetadata(invocation.Identity, output.name))
		if valueErr != nil {
			return stepkind.StepResult{}, outputFailure(output, valueErr)
		}
		if persistErr := values.ValidatePersistable(typed); persistErr != nil {
			return stepkind.StepResult{}, outputFailure(output, persistErr)
		}
		outputs[output.name] = typed
	}
	return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: outputs}, nil
}

type namedExpression struct {
	name string
	text string
}

func parseConfig(config graph.Config) ([]namedExpression, []diagnostic.Diagnostic) {
	if len(config) == 0 {
		return nil, []diagnostic.Diagnostic{configDiagnostic("transform config must declare at least one named output expression", "config")}
	}
	names := make([]string, 0, len(config))
	for name := range config {
		names = append(names, name)
	}
	sort.Strings(names)
	expressions := make([]namedExpression, 0, len(names))
	var findings []diagnostic.Diagnostic
	for _, name := range names {
		path := "config." + name
		if err := graph.ValidateID(name); err != nil {
			findings = append(findings, configDiagnostic(fmt.Sprintf("transform output %q must use a normalized identifier", name), path))
			continue
		}
		text, ok := config[name].(string)
		if !ok {
			findings = append(findings, configDiagnostic(fmt.Sprintf("transform output %q expression must be a string", name), path))
			continue
		}
		expression := graph.Expression{Text: text}
		references, err := values.ParseReferences(expression)
		if err != nil {
			findings = append(findings, configDiagnostic(fmt.Sprintf("transform output %q expression syntax is invalid", name), path))
			continue
		}
		unsupported := unsupportedExpression(expression)
		for _, reference := range references {
			if reference.Root == "env" {
				unsupported = "env references"
				break
			}
		}
		if unsupported != "" {
			findings = append(findings, configDiagnostic(fmt.Sprintf("transform output %q expression uses unsupported %s", name, unsupported), path))
			continue
		}
		expressions = append(expressions, namedExpression{name: name, text: text})
	}
	return expressions, findings
}

func configDiagnostic(message, path string) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError,
		Code:     stepkind.CodeInvalidConfig,
		Message:  message,
		Remediation: &diagnostic.Remediation{
			Message: fmt.Sprintf("Use a pure raw workflow expression at %s.", path),
		},
	}
}

func unsupportedExpression(expression graph.Expression) string {
	tree, err := parser.Parse(strings.TrimSpace(expression.Text))
	if err != nil {
		return "expression syntax"
	}
	visitor := unsupportedVisitor{}
	ast.Walk(&tree.Node, &visitor)
	return visitor.reason
}

type unsupportedVisitor struct{ reason string }

func (v *unsupportedVisitor) Visit(node *ast.Node) {
	if v.reason != "" {
		return
	}
	switch current := (*node).(type) {
	case *ast.BuiltinNode:
		if current.Name == "now" {
			v.reason = "non-deterministic now()"
		}
	case *ast.CallNode:
		identifier, ok := current.Callee.(*ast.IdentifierNode)
		if !ok {
			v.reason = "function call"
			return
		}
		switch identifier.Value {
		case "string", "int", "float":
			// These conversions are replaced with deterministic exact variants
			// by values.ExpressionEngine.
		case "now":
			v.reason = "non-deterministic now()"
		default:
			v.reason = fmt.Sprintf("function call %s()", identifier.Value)
		}
	}
}

type evaluation struct {
	value any
	err   error
}

func evaluateContext(ctx context.Context, engine *values.ExpressionEngine, expression graph.Expression, expressionContext values.ExpressionContext, options values.ExpressionOptions) (any, error) {
	completed := make(chan evaluation, 1)
	go func() {
		value, err := engine.EvaluateRaw(expression, expressionContext, options)
		completed <- evaluation{value: value, err: err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-completed:
		return result.value, result.err
	}
}

func outputMetadata(identity stepkind.InvocationIdentity, name string) values.Metadata {
	reference := identity.RunID + "/" + identity.NodeID
	if identity.Iteration != "" {
		reference += "/" + identity.Iteration
	}
	return values.Metadata{
		Producer:  values.Producer{Kind: "node_output", Reference: reference, Output: name},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	}
}

func contextFailure(cause error, classification stepkind.RetryClassification) error {
	return &stepkind.ExecutionError{
		Code: CodeContextUnavailable, Message: "transform expression context is unavailable",
		Classification: classification, Cause: cause,
	}
}

func expressionFailure(output namedExpression, cause error) error {
	return &stepkind.ExecutionError{
		Code:           CodeExpressionFailed,
		Message:        fmt.Sprintf("transform output %q expression failed", output.name),
		Classification: stepkind.RetryPermanent,
		Details:        map[string]string{"output": output.name, "expression": "config." + output.name},
		Cause:          cause,
	}
}

func outputFailure(output namedExpression, cause error) error {
	return &stepkind.ExecutionError{
		Code:           CodeOutputInvalid,
		Message:        fmt.Sprintf("transform output %q is not a persistable workflow value", output.name),
		Classification: stepkind.RetryPermanent,
		Details:        map[string]string{"output": output.name, "expression": "config." + output.name},
		Cause:          cause,
	}
}

func cloneInvocation(invocation stepkind.Invocation) (stepkind.Invocation, error) {
	var cloned stepkind.Invocation
	if err := cloneJSON(invocation, &cloned); err != nil {
		return stepkind.Invocation{}, fmt.Errorf("clone invocation: %w", err)
	}
	return cloned, nil
}

func cloneContext(expressionContext values.ExpressionContext) (values.ExpressionContext, error) {
	if expressionContext.Inputs == nil {
		expressionContext.Inputs = values.ValueSet{}
	}
	if expressionContext.Env == nil {
		expressionContext.Env = values.ValueSet{}
	}
	if expressionContext.Steps != nil {
		steps := make(map[string]values.StepContext, len(expressionContext.Steps))
		for name, step := range expressionContext.Steps {
			steps[name] = cloneableStepContext(step)
		}
		expressionContext.Steps = steps
	}
	var cloned values.ExpressionContext
	if err := cloneJSON(expressionContext, &cloned); err != nil {
		return values.ExpressionContext{}, fmt.Errorf("clone expression context: %w", err)
	}
	return cloned, nil
}

func cloneableStepContext(step values.StepContext) values.StepContext {
	if step.Outputs == nil {
		step.Outputs = values.ValueSet{}
	}
	if step.Items != nil {
		items := make([]values.StepContext, len(step.Items))
		for index, item := range step.Items {
			items[index] = cloneableStepContext(item)
		}
		step.Items = items
	}
	return step
}

func cloneJSON(input, output any) error {
	encoded, err := json.Marshal(input)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func sortedStepNames(steps map[string]values.StepContext) []string {
	names := make([]string, 0, len(steps))
	for name := range steps {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

var _ stepkind.StepKind = (*Executor)(nil)
