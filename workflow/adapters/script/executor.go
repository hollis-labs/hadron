package script

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dop251/goja"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

// Execute runs validated JavaScript in a fresh capability-free Goja runtime.
func (e *Executor) Execute(ctx context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	if ctx == nil {
		return stepkind.StepResult{}, executionError(CodeRuntimeUnavailable, "script runtime context is unavailable", stepkind.RetryPermanent, ErrRuntimeUnavailable, nil)
	}
	if e == nil || e.limits.Validate() != nil {
		return stepkind.StepResult{}, executionError(CodeRuntimeUnavailable, "script runtime is unavailable", stepkind.RetryPermanent, ErrRuntimeUnavailable, nil)
	}
	if err := ctx.Err(); err != nil {
		return stepkind.StepResult{}, interruptionError(err)
	}
	if err := prepared.Invocation.Validate(); err != nil {
		return stepkind.StepResult{}, executionError(CodeInputInvalid, "script invocation is invalid", stepkind.RetryPermanent, err, nil)
	}
	parsed, finding := e.parseConfig(prepared.Invocation.Config)
	if finding != nil {
		details := map[string]string{"path": "config"}
		if finding.Source != nil {
			details["line"] = strconv.Itoa(finding.Source.StartLine)
			details["column"] = strconv.Itoa(finding.Source.StartColumn)
		}
		return stepkind.StepResult{}, executionError(CodeInvalidConfig, "script config is invalid", stepkind.RetryPermanent, errors.New(finding.Message), details)
	}
	if err := values.ValidateValueSetSchema(parsed.inputSchema, prepared.Invocation.Inputs); err != nil {
		return stepkind.StepResult{}, executionError(CodeInputInvalid, "script inputs do not satisfy input_schema", stepkind.RetryPermanent, err, map[string]string{"path": "config.input_schema"})
	}
	encodedInput, err := inputPayload(prepared.Invocation.Inputs, e.limits)
	if err != nil {
		return stepkind.StepResult{}, classifyBoundaryFailure(err, true)
	}

	executionCtx, cancel := executionContext(ctx, prepared.Invocation.Deadline, e.limits.WallTime)
	defer cancel()
	if err := executionCtx.Err(); err != nil {
		return stepkind.StepResult{}, interruptionError(err)
	}

	vm := goja.New()
	vm.SetMaxCallStackSize(e.limits.MaxCallStack)
	done := make(chan struct{})
	go func() {
		select {
		case <-executionCtx.Done():
			vm.Interrupt(executionCtx.Err())
		case <-done:
		}
	}()
	defer close(done)

	inputValue, objectPrototype, setupErr := prepareRuntime(vm, encodedInput)
	if setupErr != nil {
		return stepkind.StepResult{}, classifyExecutionFailure(executionCtx, setupErr)
	}
	if _, err := vm.RunProgram(parsed.program); err != nil {
		return stepkind.StepResult{}, classifyExecutionFailure(executionCtx, err)
	}
	callable, ok := goja.AssertFunction(vm.Get(parsed.entrypoint))
	if !ok {
		return stepkind.StepResult{}, executionError(
			CodeEntrypointInvalid, "script entrypoint is not a function", stepkind.RetryPermanent,
			fmt.Errorf("entrypoint %q is not callable", parsed.entrypoint), map[string]string{"path": "config.entrypoint"},
		)
	}
	returned, callErr := callable(goja.Undefined(), inputValue)
	if callErr != nil {
		return stepkind.StepResult{}, classifyExecutionFailure(executionCtx, callErr)
	}
	if err := executionCtx.Err(); err != nil {
		return stepkind.StepResult{}, interruptionError(err)
	}
	outputPayload, _, exportErr := exportResult(vm, returned, e.limits, objectPrototype)
	if exportErr != nil {
		return stepkind.StepResult{}, classifyBoundaryFailure(exportErr, false)
	}
	outputs, outputErr := outputValueSet(outputPayload, prepared.Invocation.Identity)
	if outputErr != nil {
		return stepkind.StepResult{}, classifyBoundaryFailure(outputErr, false)
	}
	if schemaErr := values.ValidateValueSetSchema(parsed.outputSchema, outputs); schemaErr != nil {
		return stepkind.StepResult{}, executionError(CodeOutputInvalid, "script outputs do not satisfy output_schema", stepkind.RetryPermanent, schemaErr, map[string]string{"path": "config.output_schema"})
	}
	result := stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: outputs}
	if err := result.Validate(); err != nil {
		return stepkind.StepResult{}, executionError(CodeOutputInvalid, "script outputs are not persistable", stepkind.RetryPermanent, err, nil)
	}
	return result, nil
}

func prepareRuntime(vm *goja.Runtime, encodedInput []byte) (goja.Value, *goja.Object, error) {
	jsonObject := vm.Get("JSON").ToObject(vm)
	parse, ok := goja.AssertFunction(jsonObject.Get("parse"))
	if !ok {
		return nil, nil, fmt.Errorf("%w: JSON.parse is unavailable", ErrRuntimeUnavailable)
	}
	input, err := parse(jsonObject, vm.ToValue(string(encodedInput)))
	if err != nil {
		return nil, nil, fmt.Errorf("%w: parse canonical input: %w", ErrRuntimeUnavailable, err)
	}
	objectConstructor := vm.Get("Object").ToObject(vm)
	objectPrototype := objectConstructor.Get("prototype").ToObject(vm)
	freeze, ok := goja.AssertFunction(objectConstructor.Get("freeze"))
	if !ok {
		return nil, nil, fmt.Errorf("%w: Object.freeze is unavailable", ErrRuntimeUnavailable)
	}
	if err := freezeTree(freeze, input, make(map[*goja.Object]bool)); err != nil {
		return nil, nil, fmt.Errorf("%w: freeze canonical input: %w", ErrRuntimeUnavailable, err)
	}
	if err := sanitizeRuntime(vm, freeze); err != nil {
		return nil, nil, fmt.Errorf("%w: install sandbox: %w", ErrRuntimeUnavailable, err)
	}
	return input, objectPrototype, nil
}

func freezeTree(freeze goja.Callable, value goja.Value, seen map[*goja.Object]bool) error {
	object, ok := value.(*goja.Object)
	if !ok || seen[object] {
		return nil
	}
	seen[object] = true
	for _, key := range object.Keys() {
		if err := freezeTree(freeze, object.Get(key), seen); err != nil {
			return err
		}
	}
	_, err := freeze(goja.Undefined(), object)
	return err
}

func sanitizeRuntime(vm *goja.Runtime, freeze goja.Callable) error {
	for _, constructor := range []string{
		"Function", "Object", "Array", "String", "Number", "Boolean",
		"Error", "TypeError", "RangeError", "ReferenceError", "SyntaxError", "EvalError", "URIError",
	} {
		value := vm.Get(constructor)
		if goja.IsUndefined(value) || goja.IsNull(value) {
			continue
		}
		prototype := value.ToObject(vm).Get("prototype")
		if !goja.IsUndefined(prototype) && !goja.IsNull(prototype) {
			if err := prototype.ToObject(vm).Delete("constructor"); err != nil {
				return err
			}
		}
	}
	mathObject := vm.Get("Math").ToObject(vm)
	if err := mathObject.Delete("random"); err != nil {
		return err
	}
	if _, err := freeze(goja.Undefined(), mathObject); err != nil {
		return err
	}
	objectConstructor := vm.Get("Object").ToObject(vm)
	for _, method := range []string{"defineProperty", "defineProperties", "getOwnPropertyDescriptor", "getOwnPropertyDescriptors", "getPrototypeOf", "setPrototypeOf"} {
		if err := objectConstructor.Delete(method); err != nil {
			return err
		}
	}
	for _, prototype := range []struct {
		constructor string
		methods     []string
	}{
		{constructor: "Object", methods: []string{"__proto__", "__defineGetter__", "__defineSetter__", "__lookupGetter__", "__lookupSetter__", "toLocaleString"}},
		{constructor: "Array", methods: []string{"concat", "flat", "flatMap", "join", "toLocaleString"}},
		{constructor: "String", methods: []string{"repeat", "padStart", "padEnd", "replace", "replaceAll", "match", "matchAll", "search", "split", "localeCompare", "toLocaleLowerCase", "toLocaleUpperCase"}},
		{constructor: "Function"},
		{constructor: "Number", methods: []string{"toLocaleString"}},
		{constructor: "Boolean"},
	} {
		value := vm.Get(prototype.constructor)
		if goja.IsUndefined(value) || goja.IsNull(value) {
			continue
		}
		object := value.ToObject(vm).Get("prototype").ToObject(vm)
		for _, method := range prototype.methods {
			if err := object.Delete(method); err != nil {
				return err
			}
		}
		if _, err := freeze(goja.Undefined(), object); err != nil {
			return err
		}
	}
	global := vm.GlobalObject()
	for _, name := range deniedGlobalNames() {
		if err := global.Delete(name); err != nil {
			return err
		}
	}
	if err := global.Delete("JSON"); err != nil {
		return err
	}
	return nil
}

func executionContext(parent context.Context, invocationDeadline time.Time, maximum time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(parent, maximum)
	if invocationDeadline.IsZero() {
		return ctx, cancel
	}
	deadlineCtx, deadlineCancel := context.WithDeadline(ctx, invocationDeadline)
	return deadlineCtx, func() {
		deadlineCancel()
		cancel()
	}
}

func classifyBoundaryFailure(err error, input bool) error {
	if errors.Is(err, ErrCapabilityDenied) {
		return executionError(CodeCapabilityDenied, "script input requires a denied capability", stepkind.RetryPermanent, err, nil)
	}
	if errors.Is(err, ErrResourceLimit) || errors.Is(err, ErrUnsafeJSONNumber) {
		return executionError(CodeResourceLimit, "script data exceeds a deterministic resource limit", stepkind.RetryPermanent, err, nil)
	}
	if input {
		return executionError(CodeInputInvalid, "script input is invalid", stepkind.RetryPermanent, err, nil)
	}
	return executionError(CodeOutputInvalid, "script output is invalid", stepkind.RetryPermanent, err, nil)
}

func classifyExecutionFailure(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return interruptionError(ctxErr)
	}
	var interrupted *goja.InterruptedError
	if errors.As(err, &interrupted) {
		if cause, ok := interrupted.Value().(error); ok {
			return interruptionError(cause)
		}
	}
	var overflow *goja.StackOverflowError
	if errors.As(err, &overflow) {
		return executionError(CodeResourceLimit, "script call stack exceeds max_call_stack", stepkind.RetryPermanent, err, sourceDetails(overflow.Stack()))
	}
	var exception *goja.Exception
	if errors.As(err, &exception) {
		return executionError(CodeExecutionFailed, "script execution failed", stepkind.RetryPermanent, err, sourceDetails(exception.Stack()))
	}
	return executionError(CodeRuntimeUnavailable, "script runtime failed", stepkind.RetryPermanent, err, nil)
}

func interruptionError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return executionError(CodeExecutionTimeout, "script execution exceeded its wall-time limit", stepkind.RetryPermanent, err, nil)
	}
	return executionError(CodeExecutionCanceled, "script execution was canceled", stepkind.RetryUnspecified, err, nil)
}

func sourceDetails(stack []goja.StackFrame) map[string]string {
	for _, frame := range stack {
		position := frame.Position()
		if position.Filename == scriptFilename && position.Line > 0 {
			return map[string]string{
				"source": scriptFilename,
				"path":   "config.code",
				"line":   strconv.Itoa(position.Line),
				"column": strconv.Itoa(position.Column),
			}
		}
	}
	return nil
}

func executionError(code, message string, classification stepkind.RetryClassification, cause error, details map[string]string) error {
	return &stepkind.ExecutionError{Code: code, Message: message, Classification: classification, Details: details, Cause: cause}
}

var _ stepkind.StepKind = (*Executor)(nil)
