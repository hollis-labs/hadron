package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const externalKind = "agent-session"

// ParentCorrelationInput is the generated child input carrying a stable
// parent-run-scoped correlation value.
const ParentCorrelationInput = "parent-correlation"

var conservativeEffects = graph.EffectSet{
	graph.EffectMaterialize,
	graph.EffectMutate,
	graph.EffectDestructive,
}

type sessionConfig struct {
	Substrate      string
	LaunchID       string
	LogicalAgentID string
	Prompt         string
	Correlation    string
	IdempotencyKey string
}

// Kind implements agent_session@v1 as an ordinary recoverable external
// operation. agent_launch composition places this kind in a child workflow.
type Kind struct{ host SessionHost }

// New constructs a fail-closed agent-session executor.
func New(options Options) (*Kind, error) {
	if nilInterface(options.Host) {
		return nil, fmt.Errorf("%w: session host is required", ErrInvalidOptions)
	}
	return &Kind{host: options.Host}, nil
}

// Register constructs and registers agent_session@v1.
func Register(registry stepkind.Registry, options Options) (*Kind, error) {
	if nilInterface(registry) {
		return nil, fmt.Errorf("%w: registry is required", ErrInvalidOptions)
	}
	kind, err := New(options)
	if err != nil {
		return nil, err
	}
	if err := registry.Register(kind); err != nil {
		return nil, err
	}
	return kind, nil
}

// Spec returns conservative metadata for arbitrary hosted agents. Launch and
// cancellation can create, mutate, and terminate external resources; retries
// require a durable idempotency key. Embedded mode cannot promise a durable
// remote/session host.
func (*Kind) Spec() stepkind.StepKindSpec {
	return stepkind.StepKindSpec{
		Name: KindName, Version: KindVersion,
		ConfigSchema: configSchema(), InputSchema: graph.Schema{"type": "object"}, OutputSchema: outputSchema(),
		Effects: append(graph.EffectSet(nil), conservativeEffects...),
		RequiredCapabilities: []string{
			"agent.session.launch", "agent.session.observe", "agent.session.cancel",
		},
		Idempotency: graph.IdempotencyKeyed, RetrySafety: stepkind.RetryRequiresIdempotency,
		Cancellation: stepkind.CancellationSpec{Mode: stepkind.CancellationExplicit},
		Observation:  stepkind.ObservationSpec{Mode: stepkind.ObservationPoll, Heartbeat: true},
		CanSuspend:   false, EmbeddedModeSupported: false,
	}
}

// ValidateConfig reports one deterministic, source-addressable config
// diagnostic without contacting a host.
func (*Kind) ValidateConfig(_ context.Context, input graph.Config) []diagnostic.Diagnostic {
	if _, err := parseConfig(input); err != nil {
		return []diagnostic.Diagnostic{{
			Severity: diagnostic.SeverityError,
			Code:     stepkind.CodeInvalidConfig,
			Message:  "invalid agent session configuration: " + err.Error(),
		}}
	}
	return nil
}

// Execute starts or exactly replays one session, then hands the immutable ref
// to the runtime's ordinary external-operation coordinator.
func (k *Kind) Execute(ctx context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	if ctx == nil {
		return stepkind.StepResult{}, permanent(CodeInvalidInvocation, "agent session invocation is invalid", errors.New("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return stepkind.StepResult{}, err
	}
	if k == nil || nilInterface(k.host) {
		return stepkind.StepResult{}, permanent(CodeInvalidInvocation, "agent session executor is not initialized", ErrInvalidOptions)
	}
	if err := prepared.Invocation.Validate(); err != nil {
		return stepkind.StepResult{}, permanent(CodeInvalidInvocation, "agent session invocation is invalid", err)
	}
	if prepared.Invocation.Continuation != nil {
		return stepkind.StepResult{}, permanent(CodeInvalidInvocation, "agent session does not accept wait continuations", ErrInvalidConfig)
	}
	config, err := parseConfig(prepared.Invocation.Config)
	if err != nil {
		return stepkind.StepResult{}, permanent(CodeInvalidInvocation, "agent session configuration is invalid", err)
	}
	key, err := effectiveIdempotencyKey(config.IdempotencyKey, prepared.Invocation.IdempotencyKey)
	if err != nil {
		return stepkind.StepResult{}, permanent(CodeLaunchConflict, "agent launch idempotency declarations conflict", err)
	}
	identity := prepared.Invocation.Identity
	correlation, err := effectiveCorrelation(config.Correlation, prepared.Invocation.Inputs, identity, config.LaunchID)
	if err != nil {
		return stepkind.StepResult{}, permanent(CodeInvalidInvocation, "agent launch correlation is invalid", err)
	}
	clonedInputs, err := cloneValueSet(prepared.Invocation.Inputs)
	if err != nil {
		return stepkind.StepResult{}, permanent(CodeInvalidInvocation, "agent launch inputs could not be copied safely", err)
	}
	// The generated parent correlation is an adapter control input, not agent
	// prompt data. It is represented by the dedicated Correlation field and
	// must not be delivered a second time as a host input payload.
	delete(clonedInputs, ParentCorrelationInput)
	request := LaunchRequest{
		Identity:  LogicalIdentity{RunID: identity.RunID, NodeID: identity.NodeID, Iteration: identity.Iteration},
		Substrate: config.Substrate, LaunchID: config.LaunchID, LogicalAgentID: config.LogicalAgentID,
		Prompt: config.Prompt, Correlation: correlation, Inputs: clonedInputs, IdempotencyKey: key,
	}
	if validationErr := request.Validate(); validationErr != nil {
		return stepkind.StepResult{}, permanent(CodeInvalidInvocation, "agent launch request is invalid", validationErr)
	}
	launched, err := k.host.LaunchSession(ctx, request)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return stepkind.StepResult{}, contextErr
		}
		if errors.Is(err, ErrLaunchConflict) {
			return stepkind.StepResult{}, permanent(CodeLaunchConflict, "agent launch replay conflicts with durable state", err)
		}
		return stepkind.StepResult{}, classifyHostFailure(CodeLaunchFailed, "agent session launch failed", err)
	}
	if err := ctx.Err(); err != nil {
		return stepkind.StepResult{}, err
	}
	if err := launched.Validate(request); err != nil {
		return stepkind.StepResult{}, permanent(CodeInvalidResult, "agent launch returned an invalid durable handle", err)
	}
	ref := externalRef(launched.Ref, identity)
	if err := ref.Validate(); err != nil {
		return stepkind.StepResult{}, permanent(CodeInvalidResult, "agent launch returned an invalid external reference", err)
	}
	return stepkind.StepResult{Outcome: stepkind.StepExternal, External: &ref}, nil
}

// Observe maps host lifecycle state to the ordinary external-operation
// observation contract.
func (k *Kind) Observe(ctx context.Context, ref stepkind.ExternalOperationRef) (stepkind.Observation, error) {
	if ctx == nil {
		return stepkind.Observation{}, permanent(CodeObserveFailed, "agent session observation failed", errors.New("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return stepkind.Observation{}, err
	}
	if k == nil || nilInterface(k.host) {
		return stepkind.Observation{}, permanent(CodeObserveFailed, "agent session executor is not initialized", ErrInvalidOptions)
	}
	session, identity, err := parseExternalRef(ref)
	if err != nil {
		return stepkind.Observation{}, permanent(CodeObserveFailed, "agent session reference is invalid", err)
	}
	observed, err := k.host.ObserveSession(ctx, session)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return stepkind.Observation{}, contextErr
		}
		return stepkind.Observation{}, classifyHostFailure(CodeObserveFailed, "agent session observation failed", err)
	}
	if err := ctx.Err(); err != nil {
		return stepkind.Observation{}, err
	}
	if err := validateObservation(session, observed); err != nil {
		return stepkind.Observation{}, permanent(CodeInvalidResult, "agent session observation is invalid", err)
	}
	switch observed.State {
	case SessionPending:
		return stepkind.Observation{State: stepkind.ObservationPending, Progress: cloneStringMap(observed.Progress)}, nil
	case SessionSucceeded:
		result, err := floorResult(*observed.Result)
		if err != nil {
			return stepkind.Observation{}, permanent(CodeInvalidResult, "agent session result is invalid", err)
		}
		outputs, err := completionOutputs(identity, observed.Handle, result)
		if err != nil {
			return stepkind.Observation{}, permanent(CodeInvalidResult, "agent session outputs are invalid", err)
		}
		stepResult := stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: outputs}
		return stepkind.Observation{State: stepkind.ObservationSucceeded, Progress: cloneStringMap(observed.Progress), Result: &stepResult}, nil
	case SessionFailed, SessionCanceled:
		classification := stepkind.RetryPermanent
		if observed.Failure.Retryable {
			classification = stepkind.Retryable
		}
		failure := &stepkind.ExecutionError{
			Code: observed.Failure.Code, Message: observed.Failure.Message,
			Classification: classification, Cause: observed.Failure.Cause,
		}
		state := stepkind.ObservationFailed
		if observed.State == SessionCanceled {
			state = stepkind.ObservationCanceled
		}
		return stepkind.Observation{State: state, Progress: cloneStringMap(observed.Progress), Failure: failure}, nil
	default:
		panic("validated session observation has unsupported state")
	}
}

// Heartbeat probes the durable session without retaining host data.
func (k *Kind) Heartbeat(ctx context.Context, ref stepkind.ExternalOperationRef) error {
	if ctx == nil {
		return permanent(CodeHeartbeatFailed, "agent session heartbeat failed", errors.New("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if k == nil || nilInterface(k.host) {
		return permanent(CodeHeartbeatFailed, "agent session executor is not initialized", ErrInvalidOptions)
	}
	session, _, err := parseExternalRef(ref)
	if err != nil {
		return permanent(CodeHeartbeatFailed, "agent session reference is invalid", err)
	}
	if err := k.host.HeartbeatSession(ctx, session); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		return classifyHostFailure(CodeHeartbeatFailed, "agent session heartbeat failed", err)
	}
	return ctx.Err()
}

// Cancel requests host cancellation. Exact duplicate/terminal handling belongs
// to the host and runtime's durable cancellation coordinator.
func (k *Kind) Cancel(ctx context.Context, ref stepkind.ExternalOperationRef) error {
	if ctx == nil {
		return permanent(CodeCancelFailed, "agent session cancellation failed", errors.New("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if k == nil || nilInterface(k.host) {
		return permanent(CodeCancelFailed, "agent session executor is not initialized", ErrInvalidOptions)
	}
	session, _, err := parseExternalRef(ref)
	if err != nil {
		return permanent(CodeCancelFailed, "agent session reference is invalid", err)
	}
	if err := k.host.CancelSession(ctx, session); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		return classifyHostFailure(CodeCancelFailed, "agent session cancellation failed", err)
	}
	return ctx.Err()
}

func parseConfig(input graph.Config) (sessionConfig, error) {
	if input == nil {
		return sessionConfig{}, fmt.Errorf("%w: config must be an object", ErrInvalidConfig)
	}
	encoded, err := canonicalJSON(input)
	if err != nil {
		return sessionConfig{}, fmt.Errorf("%w: config must be JSON-compatible", ErrInvalidConfig)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil {
		return sessionConfig{}, fmt.Errorf("%w: config must be an object", ErrInvalidConfig)
	}
	allowed := map[string]struct{}{
		"substrate": {}, "launch_id": {}, "logical_agent_id": {}, "prompt": {}, "correlation": {}, "idempotency_key": {},
	}
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, ok := allowed[key]; !ok {
			return sessionConfig{}, fmt.Errorf("%w: config.%s is unsupported", ErrInvalidConfig, key)
		}
	}
	parsed := sessionConfig{}
	for _, field := range []struct {
		name     string
		required bool
		target   *string
	}{
		{"substrate", true, &parsed.Substrate}, {"launch_id", true, &parsed.LaunchID}, {"logical_agent_id", true, &parsed.LogicalAgentID},
		{"prompt", false, &parsed.Prompt}, {"correlation", false, &parsed.Correlation}, {"idempotency_key", false, &parsed.IdempotencyKey},
	} {
		value, exists := object[field.name]
		if !exists {
			if field.required {
				return sessionConfig{}, fmt.Errorf("%w: config.%s is required", ErrInvalidConfig, field.name)
			}
			continue
		}
		text, ok := value.(string)
		if !ok {
			return sessionConfig{}, fmt.Errorf("%w: config.%s must be a string", ErrInvalidConfig, field.name)
		}
		*field.target = text
	}
	for _, field := range []struct {
		name, value string
	}{
		{"substrate", parsed.Substrate}, {"launch_id", parsed.LaunchID}, {"logical_agent_id", parsed.LogicalAgentID},
	} {
		if !stableText(field.value, 4096) {
			return sessionConfig{}, fmt.Errorf("%w: config.%s is required as stable text", ErrInvalidConfig, field.name)
		}
	}
	if !optionalText(parsed.Prompt, 1<<20) || !optionalText(parsed.Correlation, 4096) || !optionalText(parsed.IdempotencyKey, 4096) {
		return sessionConfig{}, fmt.Errorf("%w: optional text fields must be stable UTF-8", ErrInvalidConfig)
	}
	return parsed, nil
}

func effectiveIdempotencyKey(config, invocation string) (string, error) {
	if config != "" && invocation != "" && config != invocation {
		return "", ErrLaunchConflict
	}
	if invocation != "" {
		return invocation, nil
	}
	if config != "" {
		return config, nil
	}
	return "", fmt.Errorf("%w: idempotency key is required", ErrInvalidConfig)
}

func stableCorrelation(identity stepkind.InvocationIdentity, launchID string) string {
	seed, _ := canonicalJSON([]string{identity.RunID, identity.NodeID, identity.Iteration, launchID})
	return "agent:" + strings.TrimPrefix(values.SHA256Digest(seed), "sha256:")
}

func effectiveCorrelation(config string, inputs values.ValueSet, identity stepkind.InvocationIdentity, launchID string) (string, error) {
	input := ""
	if value, exists := inputs[ParentCorrelationInput]; exists {
		resolved, err := inlineStableString(ParentCorrelationInput, value)
		if err != nil {
			return "", err
		}
		input = resolved
	}
	if config != "" && input != "" && config != input {
		return "", fmt.Errorf("configured and bound correlations conflict")
	}
	if input != "" {
		return input, nil
	}
	if config != "" {
		return config, nil
	}
	return stableCorrelation(identity, launchID), nil
}

func inlineStableString(name string, value values.Value) (string, error) {
	if err := values.ValidatePersistable(value); err != nil {
		return "", fmt.Errorf("%s input is not persistable", name)
	}
	if value.Redaction == values.RedactionSecret || value.Artifact != nil || value.SecretRef != nil || value.Type != values.TypeString {
		return "", fmt.Errorf("%s input must be an inline non-secret string", name)
	}
	text, ok := value.Inline.(string)
	if !ok || !stableText(text, 4096) {
		return "", fmt.Errorf("%s input must contain stable text", name)
	}
	return text, nil
}

func externalRef(session SessionRef, identity stepkind.InvocationIdentity) stepkind.ExternalOperationRef {
	return stepkind.ExternalOperationRef{Kind: externalKind, ID: session.ID, Metadata: map[string]string{
		"substrate": session.Substrate, "correlation": session.Correlation, "request_digest": session.RequestDigest,
		"run_id": identity.RunID, "node_id": identity.NodeID, "iteration": identity.Iteration, "attempt": strconv.Itoa(identity.Attempt),
	}}
}

func parseExternalRef(ref stepkind.ExternalOperationRef) (SessionRef, stepkind.InvocationIdentity, error) {
	if err := ref.Validate(); err != nil || ref.Kind != externalKind {
		return SessionRef{}, stepkind.InvocationIdentity{}, fmt.Errorf("external operation ref is invalid")
	}
	allowed := map[string]struct{}{
		"substrate": {}, "correlation": {}, "request_digest": {}, "run_id": {}, "node_id": {}, "iteration": {}, "attempt": {},
	}
	for key := range ref.Metadata {
		if _, ok := allowed[key]; !ok {
			return SessionRef{}, stepkind.InvocationIdentity{}, fmt.Errorf("external operation metadata is ambiguous")
		}
	}
	attempt, err := strconv.Atoi(ref.Metadata["attempt"])
	if err != nil {
		return SessionRef{}, stepkind.InvocationIdentity{}, fmt.Errorf("external operation attempt is invalid")
	}
	session := SessionRef{ID: ref.ID, Substrate: ref.Metadata["substrate"], Correlation: ref.Metadata["correlation"], RequestDigest: ref.Metadata["request_digest"]}
	identity := stepkind.InvocationIdentity{
		RunID: ref.Metadata["run_id"], NodeID: ref.Metadata["node_id"], Iteration: ref.Metadata["iteration"], Attempt: attempt,
	}
	if err := session.Validate(); err != nil {
		return SessionRef{}, stepkind.InvocationIdentity{}, err
	}
	if err := identity.Validate(); err != nil {
		return SessionRef{}, stepkind.InvocationIdentity{}, err
	}
	return session, identity, nil
}

func validateObservation(ref SessionRef, observation SessionObservation) error {
	if !observation.State.valid() {
		return fmt.Errorf("unsupported session state %q", observation.State)
	}
	if err := validateSafeStringMap(observation.Progress); err != nil {
		return err
	}
	switch observation.State {
	case SessionPending:
		if observation.Result != nil || observation.Failure != nil {
			return fmt.Errorf("pending observation cannot contain terminal outcome")
		}
		if err := validateOptionalMatchingHandle(ref, observation.Handle); err != nil {
			return err
		}
	case SessionSucceeded:
		if observation.Result == nil || observation.Failure != nil {
			return fmt.Errorf("succeeded observation requires only a typed result")
		}
		if err := observation.Handle.Validate(); err != nil {
			return err
		}
		if observation.Handle.SessionID != ref.ID || observation.Handle.Substrate != ref.Substrate || observation.Handle.Correlation != ref.Correlation {
			return fmt.Errorf("terminal handle differs from durable session ref")
		}
		if err := observation.Result.Validate(); err != nil {
			return err
		}
	case SessionFailed, SessionCanceled:
		if observation.Result != nil || observation.Failure == nil || !stableText(observation.Failure.Code, 128) || !stableText(observation.Failure.Message, 4096) {
			return fmt.Errorf("unsuccessful observation requires safe failure metadata")
		}
		if err := validateOptionalMatchingHandle(ref, observation.Handle); err != nil {
			return err
		}
	}
	return nil
}

func validateOptionalMatchingHandle(ref SessionRef, handle SessionHandle) error {
	if handle == (SessionHandle{}) {
		return nil
	}
	if err := handle.Validate(); err != nil {
		return err
	}
	if handle.SessionID != ref.ID || handle.Substrate != ref.Substrate || handle.Correlation != ref.Correlation {
		return fmt.Errorf("observation handle differs from durable session ref")
	}
	return nil
}

func completionOutputs(identity stepkind.InvocationIdentity, handle SessionHandle, result values.Value) (values.ValueSet, error) {
	metadata := func(output string) values.Metadata {
		reference := identity.RunID + "/" + identity.NodeID
		if identity.Iteration != "" {
			reference += "/" + identity.Iteration
		}
		reference += "/attempt-" + strconv.Itoa(identity.Attempt)
		return values.Metadata{
			Producer:  values.Producer{Kind: KindName, Reference: reference, Output: output},
			MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		}
	}
	handleValue, err := values.NewInline(map[string]any{
		"session_id": handle.SessionID, "session_uri": nullableString(handle.SessionURI), "mailbox": nullableString(handle.Mailbox),
		"substrate": handle.Substrate, "correlation": handle.Correlation,
	}, metadata(OutputHandle))
	if err != nil {
		return nil, err
	}
	status, err := values.NewInline("succeeded", metadata(OutputStatus))
	if err != nil {
		return nil, err
	}
	return values.ValueSet{OutputHandle: handleValue, OutputStatus: status, OutputResult: result}, nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func floorResult(input values.Value) (values.Value, error) {
	cloned, err := cloneValue(input)
	if err != nil {
		return values.Value{}, err
	}
	redaction := cloned.Redaction
	if redaction == values.RedactionPublic {
		redaction = values.RedactionPrivate
	}
	retention := cloned.Retention
	if retention == values.RetentionNone {
		retention = values.RetentionRun
	}
	metadata := values.Metadata{Producer: cloned.Producer, MediaType: cloned.MediaType, Redaction: redaction, Retention: retention}
	switch {
	case cloned.Artifact != nil:
		artifact := *cloned.Artifact
		artifact.Redaction, artifact.Retention = redaction, retention
		return values.NewArtifact(artifact)
	case cloned.SecretRef != nil:
		return values.NewSecretRef(*cloned.SecretRef, metadata)
	default:
		return values.NewInline(cloned.Inline, metadata)
	}
}

func cloneValue(input values.Value) (values.Value, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return values.Value{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var cloned values.Value
	if err := decoder.Decode(&cloned); err != nil {
		return values.Value{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return values.Value{}, fmt.Errorf("value clone contains trailing JSON")
	}
	return cloned, cloned.Validate()
}

func cloneValueSet(input values.ValueSet) (values.ValueSet, error) {
	if input == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var cloned values.ValueSet
	if err := decoder.Decode(&cloned); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("value set clone contains trailing JSON")
		}
		return nil, err
	}
	if err := values.ValidatePersistableSet(cloned); err != nil {
		return nil, err
	}
	return cloned, nil
}

func permanent(code, message string, cause error) error {
	return &stepkind.ExecutionError{Code: code, Message: message, Classification: stepkind.RetryPermanent, Cause: cause}
}

func configSchema() graph.Schema {
	return graph.Schema{
		"type": "object", "additionalProperties": false,
		"required": []any{"substrate", "launch_id", "logical_agent_id"},
		"properties": map[string]any{
			"substrate":        map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("4096")},
			"launch_id":        map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("4096")},
			"logical_agent_id": map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("4096")},
			"prompt":           map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number(strconv.Itoa(1 << 20))},
			"correlation":      map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("4096")},
			"idempotency_key":  map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("4096")},
		},
	}
}

func outputSchema() graph.Schema {
	return graph.Schema{
		"type": "object", "additionalProperties": false,
		"required": []any{OutputHandle, OutputStatus, OutputResult},
		"properties": map[string]any{
			OutputHandle: map[string]any{
				"type": "object", "additionalProperties": false,
				"required": []any{"session_id", "session_uri", "mailbox", "substrate", "correlation"},
				"properties": map[string]any{
					"session_id": map[string]any{"type": "string"}, "session_uri": map[string]any{"type": []any{"string", "null"}},
					"mailbox": map[string]any{"type": []any{"string", "null"}}, "substrate": map[string]any{"type": "string"},
					"correlation": map[string]any{"type": "string"},
				},
			},
			OutputStatus: map[string]any{"const": "succeeded"},
			OutputResult: map[string]any{},
		},
	}
}
