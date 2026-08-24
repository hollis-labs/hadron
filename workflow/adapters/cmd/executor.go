package cmd

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
	"time"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

// Options supplies all host-sensitive command boundaries. Policy is required;
// Runner defaults to OSProcessRunner. Other seams are required only when the
// corresponding config feature is used.
type Options struct {
	Policy    Policy
	Runner    ProcessRunner
	Secrets   values.SecretResolver
	Artifacts ArtifactSink
	Events    EventSink
}

// Executor implements cmd@v1. It is immutable and concurrency-safe when its
// injected seams honor their documented concurrency contracts.
type Executor struct {
	policy    Policy
	runner    ProcessRunner
	secrets   values.SecretResolver
	artifacts ArtifactSink
	events    EventSink
}

// New constructs a fail-closed command executor.
func New(options Options) (*Executor, error) {
	if nilInterface(options.Policy) {
		return nil, errors.New("cmd policy is required")
	}
	runner := options.Runner
	if nilInterface(runner) {
		runner = OSProcessRunner{}
	}
	return &Executor{
		policy: options.Policy, runner: runner, secrets: options.Secrets,
		artifacts: options.Artifacts, events: options.Events,
	}, nil
}

// Spec returns conservative metadata for arbitrary command execution. Author
// declarations can never narrow these static policy inputs.
func (*Executor) Spec() stepkind.StepKindSpec {
	return stepkind.StepKindSpec{
		Name: Name, Version: Version,
		ConfigSchema: configSchema(), InputSchema: graph.Schema{"type": "object"}, OutputSchema: outputSchema(),
		Effects:              graph.EffectSet{graph.EffectDestructive},
		RequiredCapabilities: []string{CapabilityProcessExecute},
		Idempotency:          graph.IdempotencyNone,
		RetrySafety:          stepkind.RetryUnsupported,
		Cancellation:         stepkind.CancellationSpec{Mode: stepkind.CancellationContext},
		Observation:          stepkind.ObservationSpec{Mode: stepkind.ObservationNone},
		CanSuspend:           false, EmbeddedModeSupported: false,
	}
}

// ValidateConfig returns deterministic config diagnostics. Compatibility
// set-output is accepted only with an explicit warning.
func (*Executor) ValidateConfig(_ context.Context, config graph.Config) []diagnostic.Diagnostic {
	_, findings := parseConfig(config)
	return findings
}

// Execute launches one direct command and returns only fully parsed,
// persistable outputs on a zero exit status.
func (e *Executor) Execute(ctx context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	if ctx == nil {
		return stepkind.StepResult{}, executionFailure(CodeInvalidInvocation, "command invocation is invalid", stepkind.RetryPermanent, nil, errors.New("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return stepkind.StepResult{}, contextExecutionFailure(err)
	}
	if err := prepared.Invocation.Validate(); err != nil {
		return stepkind.StepResult{}, executionFailure(CodeInvalidInvocation, "command invocation is invalid", stepkind.RetryPermanent, nil, err)
	}
	config, findings := parseConfig(prepared.Invocation.Config)
	if hasErrors(findings) {
		return stepkind.StepResult{}, executionFailure(CodeInvalidInvocation, "command configuration is invalid", stepkind.RetryPermanent, nil, errors.New(findings[0].Message))
	}
	if e == nil || nilInterface(e.policy) || nilInterface(e.runner) {
		return stepkind.StepResult{}, executionFailure(CodeInvalidInvocation, "command execution boundary is unavailable", stepkind.RetryPermanent, nil, errors.New("missing policy or runner"))
	}
	runContext, cancel := commandContext(ctx, prepared.Invocation.Deadline, config.timeout)
	defer cancel()
	if config.captures.stdout != nil && config.captures.stdout.Mode == CaptureArtifact && nilInterface(e.artifacts) ||
		config.captures.stderr != nil && config.captures.stderr.Mode == CaptureArtifact && nilInterface(e.artifacts) {
		return stepkind.StepResult{}, executionFailure(CodeArtifactFailed, "command artifact capture is unavailable", stepkind.RetryPermanent, nil, errors.New("artifact sink is required"))
	}
	if config.captures.stdout != nil && config.captures.stdout.Mode == CaptureEvent && nilInterface(e.events) ||
		config.captures.stderr != nil && config.captures.stderr.Mode == CaptureEvent && nilInterface(e.events) {
		return stepkind.StepResult{}, executionFailure(CodeEventFailed, "command event stream is unavailable", stepkind.RetryPermanent, nil, errors.New("event sink is required"))
	}
	if len(config.environment) != 0 && nilInterface(e.secrets) {
		return stepkind.StepResult{}, executionFailure(CodeSecretFailed, "command environment secret resolution failed", stepkind.RetryPermanent, nil, errors.New("secret resolver is required"))
	}

	description := config.description()
	resolved, err := e.policy.AuthorizeCommand(runContext, cloneDescription(description))
	if err != nil {
		if contextErr := runContext.Err(); contextErr != nil {
			return stepkind.StepResult{}, contextExecutionFailure(contextErr)
		}
		return stepkind.StepResult{}, executionFailure(CodePolicyDenied, "command launch was denied by policy", stepkind.RetryPermanent, nil, err)
	}
	if validationErr := validateResolved(resolved); validationErr != nil {
		return stepkind.StepResult{}, executionFailure(CodePolicyDenied, "command policy returned an invalid launch", stepkind.RetryPermanent, nil, validationErr)
	}
	resolved = cloneResolved(resolved)

	environment, secrets, err := e.resolveEnvironment(runContext, config.environment)
	if err != nil {
		if contextErr := runContext.Err(); contextErr != nil {
			return stepkind.StepResult{}, contextExecutionFailure(contextErr)
		}
		return stepkind.StepResult{}, executionFailure(CodeSecretFailed, "command environment secret resolution failed", stepkind.Retryable, nil, err)
	}
	defer forgetEnvironment(environment, secrets)
	redactor, err := values.NewRedactor(secrets...)
	if err != nil {
		return stepkind.StepResult{}, executionFailure(CodeSecretFailed, "command environment secret resolution failed", stepkind.RetryPermanent, nil, err)
	}

	stdout, err := newStreamCollector(runContext, prepared.Invocation.Identity, StreamStdout, config.captures.stdout, e.artifacts, e.events)
	if err != nil {
		return stepkind.StepResult{}, executionFailure(CodeInvalidInvocation, "command output capture is invalid", stepkind.RetryPermanent, map[string]string{"stream": string(StreamStdout)}, err)
	}
	stderr, err := newStreamCollector(runContext, prepared.Invocation.Identity, StreamStderr, config.captures.stderr, e.artifacts, e.events)
	if err != nil {
		_ = stdout.close()
		return stepkind.StepResult{}, executionFailure(CodeInvalidInvocation, "command output capture is invalid", stepkind.RetryPermanent, map[string]string{"stream": string(StreamStderr)}, err)
	}
	stdoutWriter := newBoundedRedactingWriter(redactor.Writer(stdout), stdout)
	stderrWriter := newBoundedRedactingWriter(redactor.Writer(stderr), stderr)

	request := ProcessRequest{
		Executable: resolved.Executable, Arguments: append([]string(nil), resolved.Arguments...), CWD: resolved.CWD,
		Environment: cloneEnvironment(environment), Sandbox: resolved.Sandbox,
	}
	processResult, processErr := e.runner.Run(runContext, request, stdoutWriter, stderrWriter)
	forgetEnvironment(request.Environment, nil)
	stdoutCloseErr := stdoutWriter.Close()
	stderrCloseErr := stderrWriter.Close()
	stdoutResult := stdout.close()
	stderrResult := stderr.close()

	if contextErr := runContext.Err(); contextErr != nil {
		return stepkind.StepResult{}, contextExecutionFailure(contextErr)
	}
	if processErr != nil {
		return stepkind.StepResult{}, executionFailure(CodeProcessFailed, "command process failed", stepkind.Retryable, nil, processErr)
	}
	if stdoutCloseErr != nil || stderrCloseErr != nil {
		return stepkind.StepResult{}, executionFailure(CodeProcessFailed, "command output stream failed", stepkind.Retryable, nil, errors.Join(stdoutCloseErr, stderrCloseErr))
	}
	if processResult.EnforcedSandbox != resolved.Sandbox {
		return stepkind.StepResult{}, executionFailure(CodePolicyDenied, "command sandbox attestation did not match policy", stepkind.RetryPermanent, nil, ErrPolicyDenied)
	}
	if processResult.ExitCode < 0 {
		return stepkind.StepResult{}, executionFailure(CodeProcessFailed, "command runner returned an invalid exit status", stepkind.RetryPermanent, nil, ErrProcessFailed)
	}
	if stdoutResult.overflow {
		return stepkind.StepResult{}, truncationFailure(StreamStdout)
	}
	if stderrResult.overflow {
		return stepkind.StepResult{}, truncationFailure(StreamStderr)
	}
	if stdoutResult.err != nil {
		return stepkind.StepResult{}, collectorFailure(StreamStdout, config.captures.stdout, stdoutResult.err)
	}
	if stderrResult.err != nil {
		return stepkind.StepResult{}, collectorFailure(StreamStderr, config.captures.stderr, stderrResult.err)
	}
	if processResult.ExitCode != 0 {
		return stepkind.StepResult{}, executionFailure(
			CodeNonZeroExit, "command exited with a non-zero status", stepkind.RetryPermanent,
			map[string]string{"exit_code": strconv.Itoa(processResult.ExitCode)}, nil,
		)
	}

	outputs, err := collectOutputs(prepared.Invocation.Identity, config.captures, stdoutResult, stderrResult, processResult.ExitCode)
	if err != nil {
		return stepkind.StepResult{}, err
	}
	if err := values.ValidatePersistableSet(outputs); err != nil {
		return stepkind.StepResult{}, executionFailure(CodeParseFailed, "command outputs are not persistable", stepkind.RetryPermanent, nil, err)
	}
	return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: outputs}, nil
}

func (e *Executor) resolveEnvironment(ctx context.Context, environment map[string]values.SecretRef) ([]EnvironmentVariable, []*values.ResolvedSecret, error) {
	names := make([]string, 0, len(environment))
	for name := range environment {
		names = append(names, name)
	}
	sort.Strings(names)
	variables := make([]EnvironmentVariable, 0, len(names))
	resolved := make([]*values.ResolvedSecret, 0, len(names))
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			forgetEnvironment(variables, resolved)
			return nil, nil, err
		}
		secret, err := e.secrets.ResolveSecret(ctx, environment[name])
		if err != nil || secret == nil || secret.Reference() != environment[name] {
			forgetEnvironment(variables, resolved)
			if err == nil {
				err = values.ErrSecretMaterial
			}
			return nil, nil, err
		}
		material := secret.Bytes()
		if len(material) == 0 {
			secret.Forget()
			forgetEnvironment(variables, resolved)
			return nil, nil, values.ErrSecretMaterial
		}
		resolved = append(resolved, secret)
		variables = append(variables, EnvironmentVariable{Name: name, Value: material})
	}
	return variables, resolved, nil
}

func commandContext(parent context.Context, invocationDeadline time.Time, timeout time.Duration) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(timeout)
	if !invocationDeadline.IsZero() && invocationDeadline.Before(deadline) {
		deadline = invocationDeadline
	}
	return context.WithDeadline(parent, deadline)
}

func collectOutputs(
	identity stepkind.InvocationIdentity,
	captures streamCaptures,
	stdout, stderr collectedStream,
	exitCode int,
) (values.ValueSet, error) {
	exit, err := values.NewInline(json.Number(strconv.Itoa(exitCode)), outputMetadata(identity, "exit_code", "application/json", "node_output"))
	if err != nil {
		return nil, executionFailure(CodeParseFailed, "command exit status is not persistable", stepkind.RetryPermanent, map[string]string{"output": "exit_code"}, err)
	}
	outputs := values.ValueSet{"exit_code": exit}
	for _, item := range []struct {
		stream  Stream
		capture *CaptureConfig
		result  collectedStream
	}{{StreamStdout, captures.stdout, stdout}, {StreamStderr, captures.stderr, stderr}} {
		if item.capture == nil || item.capture.Mode == CaptureEvent {
			continue
		}
		if item.capture.Mode == CaptureArtifact {
			if err := validateArtifactValue(item.result.artifact, identity, item.capture); err != nil {
				return nil, executionFailure(CodeArtifactFailed, "command artifact output is invalid", stepkind.RetryPermanent, map[string]string{"stream": string(item.stream), "output": item.capture.Name}, err)
			}
			if _, exists := outputs[item.capture.Name]; exists {
				return nil, outputCollision(item.capture.Name)
			}
			artifact := item.result.artifact
			artifactRef := *artifact.Artifact
			artifact.Artifact = &artifactRef
			outputs[item.capture.Name] = artifact
			continue
		}
		if item.capture.Parse == ParseSetOutput {
			parsed, err := parseSetOutput(item.result.content)
			if err != nil {
				return nil, parseFailure(item.stream, "compatibility", err)
			}
			names := make([]string, 0, len(parsed))
			for name := range parsed {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				if _, exists := outputs[name]; exists {
					return nil, outputCollision(name)
				}
				value, valueErr := values.NewInline(parsed[name], outputMetadata(identity, name, "text/plain; charset=utf-8", "compatibility_set_output"))
				if valueErr != nil {
					return nil, parseFailure(item.stream, name, valueErr)
				}
				outputs[name] = value
			}
			continue
		}
		parsed, err := parseOutput(item.capture.Parse, item.result.content)
		if err != nil {
			return nil, parseFailure(item.stream, item.capture.Name, err)
		}
		mediaType := item.capture.MediaType
		if mediaType == "" {
			if item.capture.Parse == ParseText {
				mediaType = "text/plain; charset=utf-8"
			} else {
				mediaType = "application/json"
			}
		}
		value, err := values.NewInline(parsed, outputMetadata(identity, item.capture.Name, mediaType, "node_output"))
		if err != nil {
			return nil, parseFailure(item.stream, item.capture.Name, err)
		}
		if _, exists := outputs[item.capture.Name]; exists {
			return nil, outputCollision(item.capture.Name)
		}
		outputs[item.capture.Name] = value
	}
	return outputs, nil
}

func parseOutput(mode ParseMode, content []byte) (any, error) {
	switch mode {
	case ParseText:
		if !utf8.Valid(content) {
			return nil, fmt.Errorf("%w: text is not UTF-8", ErrParseFailed)
		}
		return string(content), nil
	case ParseJSON:
		decoder := json.NewDecoder(bytes.NewReader(content))
		decoder.UseNumber()
		var decoded any
		if err := decoder.Decode(&decoded); err != nil {
			return nil, fmt.Errorf("%w: invalid JSON", ErrParseFailed)
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%w: JSON must contain exactly one document", ErrParseFailed)
		}
		return decoded, nil
	case ParseLines:
		return parseLines(content)
	case ParseKV:
		return parseKV(content)
	default:
		return nil, fmt.Errorf("%w: unsupported parser", ErrParseFailed)
	}
}

func parseLines(content []byte) ([]any, error) {
	if !utf8.Valid(content) {
		return nil, fmt.Errorf("%w: lines are not UTF-8", ErrParseFailed)
	}
	if len(content) == 0 {
		return []any{}, nil
	}
	lines := strings.Split(string(content), "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	result := make([]any, len(lines))
	for index, line := range lines {
		result[index] = strings.TrimSuffix(line, "\r")
	}
	return result, nil
}

func parseKV(content []byte) (map[string]any, error) {
	lines, err := parseLines(content)
	if err != nil {
		return nil, err
	}
	result := make(map[string]any, len(lines))
	for _, raw := range lines {
		line := raw.(string)
		key, value, ok := strings.Cut(line, "=")
		if !ok || graph.ValidateID(key) != nil {
			return nil, fmt.Errorf("%w: kv line is malformed", ErrParseFailed)
		}
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("%w: kv key is duplicated", ErrParseFailed)
		}
		result[key] = value
	}
	return result, nil
}

func parseSetOutput(content []byte) (map[string]string, error) {
	lines, err := parseLines(content)
	if err != nil {
		return nil, err
	}
	result := map[string]string{}
	for _, raw := range lines {
		line := raw.(string)
		if !strings.HasPrefix(line, "::set-output ") {
			continue
		}
		name, value, ok := strings.Cut(strings.TrimPrefix(line, "::set-output "), "=")
		if !ok || graph.ValidateID(name) != nil {
			return nil, fmt.Errorf("%w: set-output directive is malformed", ErrParseFailed)
		}
		if _, exists := result[name]; exists {
			return nil, fmt.Errorf("%w: set-output name is duplicated", ErrParseFailed)
		}
		result[name] = value
	}
	return result, nil
}

func outputMetadata(identity stepkind.InvocationIdentity, name, mediaType, origin string) values.Metadata {
	reference := identity.RunID + "/" + identity.NodeID
	if identity.Iteration != "" {
		reference += "/" + identity.Iteration
	}
	if mediaType == "" {
		mediaType = "application/octet-stream"
	}
	return values.Metadata{
		Producer:  values.Producer{Kind: origin, Reference: reference, Output: name},
		MediaType: mediaType, Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	}
}

func cloneDescription(input ConfigDescription) ConfigDescription {
	input.Arguments = append([]string(nil), input.Arguments...)
	input.EnvironmentNames = append([]string(nil), input.EnvironmentNames...)
	input.DeclaredEffects = append(graph.EffectSet(nil), input.DeclaredEffects...)
	input.DeclaredCapabilities = append([]string(nil), input.DeclaredCapabilities...)
	input.ConservativeEffects = append(graph.EffectSet(nil), input.ConservativeEffects...)
	input.RequiredCapabilities = append([]string(nil), input.RequiredCapabilities...)
	return input
}

func cloneResolved(input ResolvedCommand) ResolvedCommand {
	input.Arguments = append([]string(nil), input.Arguments...)
	input.EffectiveEffects = append(graph.EffectSet(nil), input.EffectiveEffects...)
	input.EffectiveCapabilities = append([]string(nil), input.EffectiveCapabilities...)
	return input
}

func cloneEnvironment(input []EnvironmentVariable) []EnvironmentVariable {
	result := make([]EnvironmentVariable, len(input))
	for index, variable := range input {
		result[index] = EnvironmentVariable{Name: variable.Name, Value: append([]byte(nil), variable.Value...)}
	}
	return result
}

func forgetEnvironment(environment []EnvironmentVariable, secrets []*values.ResolvedSecret) {
	for index := range environment {
		for byteIndex := range environment[index].Value {
			environment[index].Value[byteIndex] = 0
		}
		environment[index].Value = nil
	}
	for _, secret := range secrets {
		secret.Forget()
	}
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

func contextExecutionFailure(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return executionFailure(CodeTimeout, "command execution timed out", stepkind.Retryable, nil, err)
	}
	return executionFailure(CodeCanceled, "command execution was canceled", stepkind.RetryPermanent, nil, err)
}

func truncationFailure(stream Stream) error {
	return executionFailure(CodeOutputTruncated, "command output exceeded its configured byte bound", stepkind.RetryPermanent, map[string]string{"stream": string(stream)}, ErrOutputTruncated)
}

func collectorFailure(stream Stream, capture *CaptureConfig, cause error) error {
	code, message := CodeEventFailed, "command operational event emission failed"
	classification := stepkind.Retryable
	if capture != nil && capture.Mode == CaptureArtifact {
		code, message = CodeArtifactFailed, "command artifact capture failed"
	}
	return executionFailure(code, message, classification, map[string]string{"stream": string(stream)}, cause)
}

func parseFailure(stream Stream, output string, cause error) error {
	return executionFailure(CodeParseFailed, "command output parsing failed", stepkind.RetryPermanent, map[string]string{"stream": string(stream), "output": output}, cause)
}

func outputCollision(name string) error {
	return executionFailure(CodeParseFailed, "command output name collision", stepkind.RetryPermanent, map[string]string{"output": name}, ErrParseFailed)
}

func executionFailure(code, message string, classification stepkind.RetryClassification, details map[string]string, cause error) error {
	return &stepkind.ExecutionError{Code: code, Message: message, Classification: classification, Details: details, Cause: cause}
}
