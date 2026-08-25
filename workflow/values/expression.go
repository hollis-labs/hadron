package values

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/expr-lang/expr"
	exprtypes "github.com/expr-lang/expr/types"
	"github.com/expr-lang/expr/vm"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
)

// ExpressionEngine evaluates workflow expressions and owns a concurrency-safe
// cache of immutable expr programs. The zero value is ready for use.
type ExpressionEngine struct {
	programs sync.Map
}

// NewExpressionEngine returns an expression engine with an empty program cache.
func NewExpressionEngine() *ExpressionEngine {
	return &ExpressionEngine{}
}

// EvaluateRaw evaluates one raw graph expression and returns a normalized
// JSON-compatible native value. It rejects interpolation syntax.
func (e *ExpressionEngine) EvaluateRaw(
	expression graph.Expression,
	context ExpressionContext,
	options ExpressionOptions,
) (any, error) {
	return e.evaluate(expression, context, options, resultAny)
}

// EvaluateBool evaluates one raw graph expression as a boolean. It is the
// predicate path used by if and other conditional workflow constructs.
func (e *ExpressionEngine) EvaluateBool(
	expression graph.Expression,
	context ExpressionContext,
	options ExpressionOptions,
) (bool, error) {
	result, err := e.evaluate(expression, context, options, resultBool)
	if err != nil {
		return false, err
	}
	boolean, ok := result.(bool)
	if !ok {
		return false, expressionError(
			CodeExpressionType,
			"expression result must be boolean",
			expression.Source,
			fmt.Errorf("got %T", result),
		)
	}
	return boolean, nil
}

type resultMode uint8

const (
	resultAny resultMode = iota
	resultBool
)

type programKey struct {
	text       string
	schema     string
	mode       resultMode
	allowEnv   bool
	visibility string
}

func (e *ExpressionEngine) evaluate(
	expression graph.Expression,
	context ExpressionContext,
	options ExpressionOptions,
	mode resultMode,
) (any, error) {
	references, err := ParseReferences(expression)
	if err != nil {
		return nil, err
	}
	if taintErr := rejectSecretReferenceUse(references, context); taintErr != nil {
		return nil, expressionError(
			CodeExpressionValue,
			"computed expressions cannot unwrap or derive from secret references",
			expression.Source,
			taintErr,
		)
	}
	visibility, visible := visibilitySet(options.VisibleSteps)
	if policyErr := enforceReferencePolicy(references, context, options, visibility, visible, expression.Source); policyErr != nil {
		return nil, policyErr
	}

	environment, err := prepareEnvironment(context, options, visibility, visible)
	if err != nil {
		return nil, expressionError(CodeExpressionValue, "expression context is invalid", expression.Source, err)
	}
	schema, schemaKey := schemaForMap(environment)
	key := programKey{
		text: strings.TrimSpace(expression.Text), schema: schemaKey, mode: mode,
		allowEnv: options.AllowEnv, visibility: visibilityKey(options.VisibleSteps),
	}
	program, err := e.loadProgram(key, schema)
	if err != nil {
		code := classifyExpressionFailure(err)
		return nil, expressionError(code, expressionFailureMessage(code), expression.Source, err)
	}

	output, err := expr.Run(program, environment)
	if err != nil {
		code := classifyExpressionFailure(err)
		return nil, expressionError(code, expressionFailureMessage(code), expression.Source, err)
	}
	if mode == resultBool {
		if _, ok := output.(bool); !ok {
			return nil, expressionError(
				CodeExpressionType,
				"expression result must be boolean",
				expression.Source,
				fmt.Errorf("got %T", output),
			)
		}
		return output, nil
	}
	normalized, _, err := normalizeInline(output)
	if err != nil {
		return nil, expressionError(
			CodeExpressionValue,
			"expression result is not JSON-compatible",
			expression.Source,
			err,
		)
	}
	return normalized, nil
}

func (e *ExpressionEngine) loadProgram(key programKey, schema exprtypes.Map) (*vm.Program, error) {
	if cached, ok := e.programs.Load(key); ok {
		return cached.(*vm.Program), nil
	}
	compileOptions := []expr.Option{
		expr.Env(schema),
		expr.DisableBuiltin("now"),
		expr.DisableBuiltin("string"),
		expr.DisableBuiltin("int"),
		expr.DisableBuiltin("float"),
		expr.Function("string", deterministicStringFunction, new(func(any) string)),
		expr.Function("int", deterministicIntFunction, new(func(any) int)),
		expr.Function("float", deterministicFloatFunction, new(func(any) float64)),
	}
	if key.mode == resultBool {
		compileOptions = append(compileOptions, expr.AsBool())
	} else {
		compileOptions = append(compileOptions, expr.AsAny())
	}
	program, err := expr.Compile(key.text, compileOptions...)
	if err != nil {
		return nil, err
	}
	actual, _ := e.programs.LoadOrStore(key, program)
	return actual.(*vm.Program), nil
}

func enforceReferencePolicy(
	references []Reference,
	context ExpressionContext,
	options ExpressionOptions,
	visibility map[string]struct{},
	visibilityExplicit bool,
	source *graph.SourceRef,
) error {
	for _, reference := range references {
		if reference.Root == "env" && !options.AllowEnv {
			return expressionError(
				CodeExpressionEnvDenied,
				"env references are disabled by expression policy",
				source,
				errors.New("set AllowEnv only after supplying an explicit env value set"),
			)
		}
		if reference.Root != "steps" || !visibilityExplicit || len(reference.Path) == 0 {
			continue
		}
		stepID := reference.Path[0]
		if _, exists := context.Steps[stepID]; !exists {
			continue
		}
		if _, allowed := visibility[stepID]; !allowed {
			return expressionError(
				CodeExpressionInvisibleStep,
				fmt.Sprintf("step %q is outside the caller-declared expression visibility", stepID),
				source,
				fmt.Errorf("step %q is not visible", stepID),
			)
		}
	}
	return nil
}

func visibilitySet(steps []string) (map[string]struct{}, bool) {
	if steps == nil {
		return nil, false
	}
	set := make(map[string]struct{}, len(steps))
	for _, step := range steps {
		set[step] = struct{}{}
	}
	return set, true
}

func visibilityKey(steps []string) string {
	if steps == nil {
		return "*"
	}
	copyOfSteps := append([]string(nil), steps...)
	sort.Strings(copyOfSteps)
	return strings.Join(copyOfSteps, "\x00")
}

func prepareEnvironment(
	context ExpressionContext,
	options ExpressionOptions,
	visibility map[string]struct{},
	visibilityExplicit bool,
) (map[string]any, error) {
	inputs, err := unwrapValueSet(context.Inputs)
	if err != nil {
		return nil, fmt.Errorf("inputs: %w", err)
	}
	outputs, err := unwrapValueSet(context.Outputs)
	if err != nil {
		return nil, fmt.Errorf("outputs: %w", err)
	}
	steps := make(map[string]any, len(context.Steps))
	stepIDs := make([]string, 0, len(context.Steps))
	for stepID := range context.Steps {
		if visibilityExplicit {
			if _, allowed := visibility[stepID]; !allowed {
				continue
			}
		}
		stepIDs = append(stepIDs, stepID)
	}
	sort.Strings(stepIDs)
	for _, stepID := range stepIDs {
		prepared, prepareErr := prepareStep(context.Steps[stepID])
		if prepareErr != nil {
			return nil, fmt.Errorf("steps[%q]: %w", stepID, prepareErr)
		}
		steps[stepID] = prepared
	}

	run, err := prepareHostMap("run", context.Run)
	if err != nil {
		return nil, err
	}
	runScope, err := prepareHostMap("run_scope", context.RunScope)
	if err != nil {
		return nil, err
	}
	executionTarget, err := prepareHostMap("execution_target", context.ExecutionTarget)
	if err != nil {
		return nil, err
	}
	environment := map[string]any{
		"inputs": inputs, "outputs": outputs, "steps": steps,
		"item": nil, "index": nil,
		"run": run, "run_scope": runScope, "execution_target": executionTarget,
	}
	localNames := make([]string, 0, len(context.Locals))
	for name := range context.Locals {
		localNames = append(localNames, name)
	}
	sort.Strings(localNames)
	for _, name := range localNames {
		if err := ValidateExpressionLocalName(name); err != nil {
			return nil, fmt.Errorf("locals[%q]: %w", name, err)
		}
		value, unwrapErr := unwrapValue(context.Locals[name])
		if unwrapErr != nil {
			return nil, fmt.Errorf("locals[%q]: %w", name, unwrapErr)
		}
		environment[name] = value
	}
	if context.Item != nil {
		item, err := unwrapValue(*context.Item)
		if err != nil {
			return nil, fmt.Errorf("item: %w", err)
		}
		environment["item"] = item
	}
	if context.Index != nil {
		environment["index"] = *context.Index
	}
	if options.AllowEnv {
		env, err := unwrapValueSet(context.Env)
		if err != nil {
			return nil, fmt.Errorf("env: %w", err)
		}
		environment["env"] = env
	}
	return environment, nil
}

func prepareStep(step StepContext) (map[string]any, error) {
	outputs, err := unwrapValueSet(step.Outputs)
	if err != nil {
		return nil, fmt.Errorf("outputs: %w", err)
	}
	status, _, err := normalizeInline(step.Status)
	if err != nil {
		return nil, fmt.Errorf("status: %w", err)
	}
	var errorValue any
	if step.Error != nil {
		errorValue, err = unwrapValue(*step.Error)
		if err != nil {
			return nil, fmt.Errorf("error: %w", err)
		}
	}
	errorValue = prepareExpressionValue(errorValue)
	items := make([]any, len(step.Items))
	for index, item := range step.Items {
		items[index], err = prepareStep(item)
		if err != nil {
			return nil, fmt.Errorf("items[%d]: %w", index, err)
		}
	}
	return map[string]any{
		"outputs": outputs,
		"status":  status,
		"error":   errorValue,
		"items":   items,
	}, nil
}

func prepareHostMap(name string, input map[string]any) (map[string]any, error) {
	if input == nil {
		return map[string]any{}, nil
	}
	normalized, valueType, err := normalizeInline(input)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	if valueType != TypeObject {
		return nil, fmt.Errorf("%s: expected object", name)
	}
	return prepareExpressionValue(normalized).(map[string]any), nil
}

func unwrapValueSet(set ValueSet) (map[string]any, error) {
	if set == nil {
		return map[string]any{}, nil
	}
	if err := set.Validate(); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)
	values := make(map[string]any, len(set))
	for _, name := range names {
		value, err := unwrapValue(set[name])
		if err != nil {
			return nil, fmt.Errorf("value-set[%q]: %w", name, err)
		}
		values[name] = value
	}
	return values, nil
}

func unwrapValue(value Value) (any, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	if value.Type == TypeSecretRef {
		// Reference policy rejects every actual use before evaluation. Keeping a
		// non-secret placeholder in the environment lets unrelated expressions
		// operate without exposing the opaque URI to expr.
		return RedactedMarker, nil
	}
	if value.Type != TypeArtifact {
		return prepareExpressionValue(value.Inline), nil
	}
	artifact := value.Artifact
	return map[string]any{
		"store":      artifact.Store,
		"uri":        artifact.URI,
		"digest":     artifact.Digest,
		"media_type": artifact.MediaType,
		"size_bytes": prepareExpressionValue(artifact.SizeBytes),
		"producer": map[string]any{
			"kind": artifact.Producer.Kind, "reference": artifact.Producer.Reference, "output": artifact.Producer.Output,
		},
		"redaction": string(artifact.Redaction),
		"retention": string(artifact.Retention),
	}, nil
}

func rejectSecretReferenceUse(references []Reference, context ExpressionContext) error {
	// Locals are expression roots but intentionally are not returned by
	// ParseReferences, whose public contract is limited to standard graph roots.
	// Reject any secret local conservatively so a catch alias cannot be unwrapped
	// into a redacted marker and then used for derivation.
	if valueSetContainsSecret(context.Locals) {
		return ErrSecretDerivation
	}
	for _, reference := range references {
		switch reference.Root {
		case "inputs":
			if reference.Dynamic || len(reference.Path) == 0 {
				if valueSetContainsSecret(context.Inputs) {
					return ErrSecretDerivation
				}
				continue
			}
			if value, ok := context.Inputs[reference.Path[0]]; ok && value.Redaction == RedactionSecret {
				return ErrSecretDerivation
			}
		case "outputs":
			if reference.Dynamic || len(reference.Path) == 0 {
				if valueSetContainsSecret(context.Outputs) {
					return ErrSecretDerivation
				}
				continue
			}
			if value, ok := context.Outputs[reference.Path[0]]; ok && value.Redaction == RedactionSecret {
				return ErrSecretDerivation
			}
		case "steps":
			if reference.Dynamic || len(reference.Path) == 0 {
				if stepsContainSecret(context.Steps) {
					return ErrSecretDerivation
				}
				continue
			}
			step, ok := context.Steps[reference.Path[0]]
			if !ok {
				continue
			}
			if len(reference.Path) == 1 {
				if stepContainsSecret(step) {
					return ErrSecretDerivation
				}
				continue
			}
			switch reference.Path[1] {
			case "outputs":
				if len(reference.Path) == 2 {
					if valueSetContainsSecret(step.Outputs) {
						return ErrSecretDerivation
					}
					continue
				}
				if value, exists := step.Outputs[reference.Path[2]]; exists && value.Redaction == RedactionSecret {
					return ErrSecretDerivation
				}
			case "items":
				for _, item := range step.Items {
					if stepContainsSecret(item) {
						return ErrSecretDerivation
					}
				}
			case "error":
				if step.Error != nil && step.Error.Redaction == RedactionSecret {
					return ErrSecretDerivation
				}
			}
		case "item":
			if context.Item != nil && context.Item.Redaction == RedactionSecret {
				return ErrSecretDerivation
			}
		case "env":
			if reference.Dynamic || len(reference.Path) == 0 {
				if valueSetContainsSecret(context.Env) {
					return ErrSecretDerivation
				}
				continue
			}
			if value, ok := context.Env[reference.Path[0]]; ok && value.Redaction == RedactionSecret {
				return ErrSecretDerivation
			}
		}
	}
	return nil
}

func valueSetContainsSecret(set ValueSet) bool {
	for _, value := range set {
		if value.Redaction == RedactionSecret {
			return true
		}
	}
	return false
}

func stepsContainSecret(steps map[string]StepContext) bool {
	for _, step := range steps {
		if stepContainsSecret(step) {
			return true
		}
	}
	return false
}

func stepContainsSecret(step StepContext) bool {
	if valueSetContainsSecret(step.Outputs) {
		return true
	}
	if step.Error != nil && step.Error.Redaction == RedactionSecret {
		return true
	}
	for _, item := range step.Items {
		if stepContainsSecret(item) {
			return true
		}
	}
	return false
}

func prepareExpressionValue(value any) any {
	switch value := value.(type) {
	case json.Number:
		return prepareExpressionNumber(value)
	case []any:
		prepared := make([]any, len(value))
		for index, item := range value {
			prepared[index] = prepareExpressionValue(item)
		}
		return prepared
	case map[string]any:
		prepared := make(map[string]any, len(value))
		for key, item := range value {
			prepared[key] = prepareExpressionValue(item)
		}
		return prepared
	default:
		return value
	}
}

func prepareExpressionNumber(number json.Number) any {
	text := number.String()
	if !strings.ContainsAny(text, ".eE") {
		if integer, err := strconv.Atoi(text); err == nil {
			return integer
		}
		return number
	}
	float, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsNaN(float) || math.IsInf(float, 0) {
		return number
	}
	exact, ok := new(big.Rat).SetString(text)
	if !ok {
		return number
	}
	if exact.Cmp(new(big.Rat).SetFloat64(float)) != 0 {
		return number
	}
	return float
}

func schemaForMap(environment map[string]any) (exprtypes.Map, string) {
	schema, signature := schemaForValue(environment)
	return schema.(exprtypes.Map), signature
}

func schemaForValue(value any) (exprtypes.Type, string) {
	switch value := value.(type) {
	case nil:
		return exprtypes.Nil, "null"
	case bool:
		return exprtypes.Bool, "bool"
	case string:
		return exprtypes.String, "string"
	case int:
		return exprtypes.Int, "int"
	case float64:
		return exprtypes.Float64, "float64"
	case json.Number:
		return exprtypes.Any, "json-number"
	case []any:
		if len(value) == 0 {
			return exprtypes.Array(exprtypes.Any), "array<any>"
		}
		elementType, elementSignature := schemaForValue(value[0])
		for _, item := range value[1:] {
			candidateType, candidateSignature := schemaForValue(item)
			if !elementType.Equal(candidateType) || candidateSignature != elementSignature {
				return exprtypes.Array(exprtypes.Any), "array<any>"
			}
		}
		return exprtypes.Array(elementType), "array<" + elementSignature + ">"
	case map[string]any:
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		schema := make(exprtypes.Map, len(value))
		var signature strings.Builder
		signature.WriteString("map{")
		for _, key := range keys {
			fieldType, fieldSignature := schemaForValue(value[key])
			schema[key] = fieldType
			signature.WriteString(strconv.QuoteToASCII(key))
			signature.WriteByte(':')
			signature.WriteString(fieldSignature)
			signature.WriteByte(';')
		}
		signature.WriteByte('}')
		return schema, signature.String()
	default:
		return exprtypes.TypeOf(value), reflect.TypeOf(value).String()
	}
}

func deterministicStringFunction(params ...any) (any, error) {
	if len(params) != 1 {
		return nil, fmt.Errorf("string expects one argument")
	}
	normalized, _, err := normalizeInline(params[0])
	if err != nil {
		return nil, err
	}
	switch value := normalized.(type) {
	case nil:
		return "null", nil
	case string:
		return value, nil
	case bool:
		return strconv.FormatBool(value), nil
	case json.Number:
		return value.String(), nil
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return string(encoded), nil
	}
}

func deterministicIntFunction(params ...any) (any, error) {
	if len(params) != 1 {
		return nil, fmt.Errorf("int expects one argument")
	}
	switch value := params[0].(type) {
	case int:
		return value, nil
	case int8:
		return int(value), nil
	case int16:
		return int(value), nil
	case int32:
		return int(value), nil
	case int64:
		if strconv.IntSize == 32 && (value < math.MinInt32 || value > math.MaxInt32) {
			return nil, fmt.Errorf("int conversion overflows")
		}
		return int(value), nil
	case uint, uint8, uint16, uint32, uint64:
		unsigned := reflect.ValueOf(value).Convert(reflect.TypeOf(uint64(0))).Uint()
		if unsigned > uint64(^uint(0)>>1) {
			return nil, fmt.Errorf("int conversion overflows")
		}
		return int(unsigned), nil
	case float32:
		return checkedFloatToInt(float64(value))
	case float64:
		return checkedFloatToInt(value)
	case string:
		return strconv.Atoi(value)
	case json.Number:
		return strconv.Atoi(value.String())
	default:
		return nil, fmt.Errorf("cannot convert %T to int", params[0])
	}
}

func checkedFloatToInt(value float64) (int, error) {
	maximum := int(^uint(0) >> 1)
	minimum := -maximum - 1
	if math.IsNaN(value) || math.IsInf(value, 0) || value < float64(minimum) || value > float64(maximum) {
		return 0, fmt.Errorf("int conversion overflows")
	}
	return int(value), nil
}

func deterministicFloatFunction(params ...any) (any, error) {
	if len(params) != 1 {
		return nil, fmt.Errorf("float expects one argument")
	}
	var result float64
	switch value := params[0].(type) {
	case float32:
		result = float64(value)
	case float64:
		result = value
	case int:
		result = float64(value)
	case int8:
		result = float64(value)
	case int16:
		result = float64(value)
	case int32:
		result = float64(value)
	case int64:
		result = float64(value)
	case uint:
		result = float64(value)
	case uint8:
		result = float64(value)
	case uint16:
		result = float64(value)
	case uint32:
		result = float64(value)
	case uint64:
		result = float64(value)
	case string:
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, err
		}
		result = parsed
	case json.Number:
		parsed, err := strconv.ParseFloat(value.String(), 64)
		if err != nil {
			return nil, err
		}
		result = parsed
	default:
		return nil, fmt.Errorf("cannot convert %T to float", params[0])
	}
	if math.IsNaN(result) || math.IsInf(result, 0) {
		return nil, fmt.Errorf("float conversion produced a non-finite number")
	}
	return result, nil
}

func classifyExpressionFailure(err error) diagnostic.Code {
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "unknown name"),
		strings.Contains(message, "unknown field"),
		strings.Contains(message, "cannot fetch"),
		strings.Contains(message, "cannot get"):
		return CodeExpressionUnresolved
	case strings.Contains(message, "mismatched type"),
		strings.Contains(message, "expected bool"),
		strings.Contains(message, "invalid operation"),
		strings.Contains(message, "invalid argument"),
		strings.Contains(message, "has no field"),
		strings.Contains(message, "must be"):
		return CodeExpressionType
	default:
		return CodeExpressionSyntax
	}
}

func expressionFailureMessage(code diagnostic.Code) string {
	switch code {
	case CodeExpressionUnresolved:
		return "expression contains an unresolved reference"
	case CodeExpressionType:
		return "expression has a type mismatch"
	default:
		return "expression could not be compiled or evaluated"
	}
}
