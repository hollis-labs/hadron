package mcptools

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/libs/mcp/plugin-mcp/go-mcp/budget"
	"github.com/hollis-labs/libs/workflow/engine/diagnostic"
	"github.com/hollis-labs/libs/workflow/engine/values"
)

const maximumWorkflowMCPArguments = 1 << 20

// workflowInvocationResult is the immediate asynchronous contract of every
// generated workflow tool. Outputs are intentionally absent at start time;
// the nested output schema reserves the canonical terminal workflow contract
// for an await-capable path without misdescribing the run handle itself.
type workflowInvocationResult struct {
	RunID       appworkflow.RunID       `json:"run_id"`
	Status      string                  `json:"status"`
	Outcome     string                  `json:"outcome,omitempty"`
	Diagnostics []diagnostic.Diagnostic `json:"diagnostics,omitempty"`
	Outputs     map[string]any          `json:"outputs,omitempty"`
}

func workflowValueArgument(args map[string]any, key string) (values.Value, error) {
	arguments, err := boundedWorkflowArguments(args)
	if err != nil {
		return values.Value{}, err
	}
	raw, ok := arguments[key]
	if !ok {
		return values.Value{}, errors.New("typed workflow payload is required")
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return values.Value{}, errors.New("typed workflow payload is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	var payload values.Value
	if err := decoder.Decode(&payload); err != nil {
		return values.Value{}, errors.New("typed workflow payload is invalid")
	}
	if err := payload.Validate(); err != nil {
		return values.Value{}, errors.New("typed workflow payload is invalid")
	}
	return payload, nil
}

func boundedWorkflowArguments(args map[string]any) (map[string]any, error) {
	if args == nil {
		args = map[string]any{}
	}
	if err := validateWorkflowArgumentNumbers(reflect.ValueOf(args), 0); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(args)
	if err != nil || len(encoded) > maximumWorkflowMCPArguments {
		return nil, errors.New("workflow tool arguments are invalid or exceed the supported bound")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var result map[string]any
	if err := decoder.Decode(&result); err != nil {
		return nil, errors.New("workflow tool arguments are invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("workflow tool arguments contain trailing data")
	}
	return result, nil
}

func validateWorkflowArgumentNumbers(value reflect.Value, depth int) error {
	if !value.IsValid() {
		return nil
	}
	if depth > 100 {
		return errors.New("workflow tool arguments exceed the supported nesting bound")
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil
		}
		return validateWorkflowArgumentNumbers(value.Elem(), depth+1)
	}
	switch value.Kind() {
	case reflect.Float32, reflect.Float64:
		number := value.Float()
		maximumExactInteger := float64(1<<53 - 1)
		if value.Kind() == reflect.Float32 {
			maximumExactInteger = float64(1<<24 - 1)
		}
		if math.Trunc(number) == number && math.Abs(number) > maximumExactInteger {
			return errors.New("workflow tool arguments contain an integer that cannot be represented losslessly")
		}
	case reflect.Array, reflect.Slice:
		for index := 0; index < value.Len(); index++ {
			if err := validateWorkflowArgumentNumbers(value.Index(index), depth+1); err != nil {
				return err
			}
		}
	case reflect.Map:
		iterator := value.MapRange()
		for iterator.Next() {
			if err := validateWorkflowArgumentNumbers(iterator.Value(), depth+1); err != nil {
				return err
			}
		}
	default:
		return nil
	}
	return nil
}

func exactWorkflowLimitArgument(args map[string]any, defaultValue int) (int, error) {
	return exactWorkflowIntegerArgument(args, "limit", defaultValue)
}

func exactWorkflowIntegerArgument(args map[string]any, key string, defaultValue int) (int, error) {
	value, exists := args[key]
	if !exists {
		return defaultValue, nil
	}
	if number, ok := value.(json.Number); ok {
		parsed, err := number.Int64()
		if err != nil {
			return 0, fmt.Errorf("workflow argument %q must be an exact integer", key)
		}
		return checkedWorkflowInteger(parsed)
	}
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() {
		return 0, fmt.Errorf("workflow argument %q must be an exact integer", key)
	}
	switch reflected.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return checkedWorkflowInteger(reflected.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		maximum := uint64(^uint(0) >> 1)
		if reflected.Uint() > maximum {
			return 0, fmt.Errorf("workflow argument %q exceeds the supported integer range", key)
		}
		return int(reflected.Uint()), nil // #nosec G115 -- range checked against the platform int maximum above.
	case reflect.Float32, reflect.Float64:
		number := reflected.Float()
		maximumExactInteger := float64(1<<53 - 1)
		if reflected.Kind() == reflect.Float32 {
			maximumExactInteger = float64(1<<24 - 1)
		}
		maximumInteger := float64(^uint(0) >> 1)
		minimumInteger := -maximumInteger - 1
		if math.IsNaN(number) || math.IsInf(number, 0) || math.Trunc(number) != number || math.Abs(number) > maximumExactInteger || number > maximumInteger || number < minimumInteger {
			return 0, fmt.Errorf("workflow argument %q must be an exact integer", key)
		}
		return int(number), nil
	default:
		return 0, fmt.Errorf("workflow argument %q must be an exact integer", key)
	}
}

func checkedWorkflowInteger(value int64) (int, error) {
	maximum := int64(^uint(0) >> 1)
	minimum := -maximum - 1
	if value > maximum || value < minimum {
		return 0, errors.New("workflow argument \"limit\" exceeds the supported integer range")
	}
	return int(value), nil
}

func decodeWorkflowArguments(args map[string]any, target any) error {
	arguments, err := boundedWorkflowArguments(args)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(arguments)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("workflow tool arguments contain trailing data")
	}
	return nil
}

func workflowResult(value any, err error) (any, error) {
	if err != nil {
		return nil, workflowFailure(err)
	}
	return workflowSuccess(value), nil
}

func workflowStartResult(runID appworkflow.RunID, value appworkflow.StartRunResult, err error) (any, error) {
	if err != nil {
		safe := appworkflow.SafeWorkflowOperationError(err, &value)
		return nil, workflowOperationErr{safe: safe}
	}
	status := "not_admitted"
	if value.Run != nil {
		status = string(value.Run.Status)
	} else if value.Bound != nil {
		status = "bound"
	}
	return workflowSuccess(workflowInvocationResult{RunID: runID, Status: status, Outcome: string(value.Outcome), Diagnostics: append([]diagnostic.Diagnostic(nil), value.Diagnostics...)}), nil
}

func workflowSuccess(value any) any { return value }

// workflowOperationErr adapts appworkflow.WorkflowOperationError to
// budget.StructuredError: the type deliberately carries no human-readable
// message ("Message text is intentionally not transported"), so it can't
// satisfy budget.ToolError's required Message field, but still needs
// StructuredContent+IsError treatment for its own safe, bounded shape.
type workflowOperationErr struct {
	safe appworkflow.WorkflowOperationError
}

func workflowFailure(err error) error {
	if errors.Is(err, appworkflow.ErrHostNotReady) || errors.Is(err, appworkflow.ErrInvalidHost) {
		return workflowOperationErr{safe: appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeUnavailable}}
	}
	return workflowOperationErr{safe: appworkflow.SafeWorkflowOperationError(err, nil)}
}

func boundedWorkflowLimit(value int) int {
	if value < 1 {
		return 100
	}
	if value > 1000 {
		return 1000
	}
	return value
}

func nilInterfaceValue(value any) bool {
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

func (e workflowOperationErr) Error() string         { return "workflow error: " + e.safe.Code }
func (e workflowOperationErr) ToolErrorContent() any { return map[string]any{"error": e.safe} }

var _ budget.StructuredError = workflowOperationErr{}
