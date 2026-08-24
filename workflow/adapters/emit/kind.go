package emit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const maxConfigBytes = 64 << 10

type config struct {
	Destination  Destination
	EventType    string
	PayloadInput string
	Correlation  string
}

// Kind implements emit@v1. It is concurrency-safe when its injected policy
// and publisher are concurrency-safe.
type Kind struct {
	policy    Policy
	publisher Publisher
	observer  Observer
}

// New constructs a fail-closed emit executor.
func New(options Options) (*Kind, error) {
	if nilInterface(options.Policy) || nilInterface(options.Publisher) {
		return nil, fmt.Errorf("%w: policy and publisher are required", ErrInvalidOptions)
	}
	return &Kind{policy: options.Policy, publisher: options.Publisher, observer: options.Observer}, nil
}

// Register constructs and registers emit@v1.
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

// Spec returns the conservative static publication contract.
func (*Kind) Spec() stepkind.StepKindSpec {
	return stepkind.StepKindSpec{
		Name: KindName, Version: KindVersion,
		ConfigSchema: configSchema(), InputSchema: graph.Schema{"type": "object"}, OutputSchema: outputSchema(),
		Effects: graph.EffectSet{graph.EffectMutate}, RequiredCapabilities: []string{CapabilityPublish},
		Idempotency: graph.IdempotencyKeyed, RetrySafety: stepkind.RetryRequiresIdempotency,
		Cancellation: stepkind.CancellationSpec{Mode: stepkind.CancellationContext},
		Observation:  stepkind.ObservationSpec{Mode: stepkind.ObservationNone},
		Memoization:  stepkind.MemoizationDisabled, EmbeddedModeSupported: false,
	}
}

// DescribeConfig exposes the immutable destination/effect policy projection.
func (*Kind) DescribeConfig(_ context.Context, input graph.Config) (ConfigDescription, error) {
	parsed, err := parseConfig(input)
	if err != nil {
		return ConfigDescription{}, err
	}
	return ConfigDescription{
		Destination: cloneDestination(parsed.Destination), EventType: parsed.EventType,
		RequiredCapabilities: []string{CapabilityPublish}, Effects: graph.EffectSet{graph.EffectMutate},
		Idempotency: graph.IdempotencyKeyed, RetrySafety: stepkind.RetryRequiresIdempotency,
	}, nil
}

// ValidateConfig reports deterministic source-addressable config failures.
func (*Kind) ValidateConfig(ctx context.Context, input graph.Config) []diagnostic.Diagnostic {
	_, err := parseConfig(input)
	if err == nil && ctx != nil {
		err = ctx.Err()
	}
	if err == nil {
		return nil
	}
	return []diagnostic.Diagnostic{{
		Severity: diagnostic.SeverityError, Code: stepkind.CodeInvalidConfig,
		Message: "invalid emit@v1 configuration: " + err.Error(),
	}}
}

// Execute authorizes and publishes one immutable typed envelope.
func (k *Kind) Execute(ctx context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	if ctx == nil {
		return stepkind.StepResult{}, permanent(CodeInvalidInvocation, "emit invocation is invalid", errors.New("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return stepkind.StepResult{}, err
	}
	if err := prepared.Invocation.Validate(); err != nil {
		return stepkind.StepResult{}, permanent(CodeInvalidInvocation, "emit invocation is invalid", err)
	}
	parsed, parseErr := parseConfig(prepared.Invocation.Config)
	if parseErr != nil {
		return stepkind.StepResult{}, permanent(CodeInvalidInvocation, "emit configuration is invalid", parseErr)
	}
	if k == nil || nilInterface(k.policy) || nilInterface(k.publisher) {
		return stepkind.StepResult{}, permanent(CodeInvalidInvocation, "emit execution boundary is unavailable", ErrInvalidOptions)
	}
	if err := stableText("invocation idempotency key", prepared.Invocation.IdempotencyKey, 512); err != nil {
		return stepkind.StepResult{}, permanent(CodeInvalidInvocation, "emit requires a stable runtime idempotency key", err)
	}
	payload, found := prepared.Invocation.Inputs[parsed.PayloadInput]
	if !found {
		return stepkind.StepResult{}, permanent(CodeInvalidInvocation, "emit payload input is missing", errors.New("configured payload input was not bound"))
	}
	payload, cloneErr := cloneValue(payload)
	if cloneErr != nil {
		return stepkind.StepResult{}, permanent(CodeInvalidInvocation, "emit payload input is invalid", cloneErr)
	}
	correlation := parsed.Correlation
	if correlation == "" {
		correlation = invocationCorrelation(prepared.Invocation.Identity)
	}
	envelope := Envelope{
		Destination: cloneDestination(parsed.Destination), EventType: parsed.EventType,
		Correlation: correlation, Payload: payload, IdempotencyKey: prepared.Invocation.IdempotencyKey,
	}
	computedEnvelopeID, identityErr := envelopeID(prepared.Invocation.Identity, envelope)
	if identityErr != nil {
		return stepkind.StepResult{}, permanent(CodeInvalidInvocation, "emit envelope identity is invalid", identityErr)
	}
	envelope.ID = computedEnvelopeID
	if err := envelope.Validate(); err != nil {
		return stepkind.StepResult{}, permanent(CodeInvalidInvocation, "emit envelope is invalid", err)
	}
	authorization := AuthorizationRequest{
		Identity: prepared.Invocation.Identity, Destination: cloneDestination(envelope.Destination),
		EventType: envelope.EventType, Correlation: envelope.Correlation, EnvelopeID: envelope.ID,
		PayloadType: payload.Type, PayloadDigest: payload.Digest, Redaction: payload.Redaction,
		Retention: payload.Retention, IdempotencyKey: envelope.IdempotencyKey,
	}
	if err := k.policy.AuthorizeEmit(ctx, authorization); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return stepkind.StepResult{}, contextErr
		}
		return stepkind.StepResult{}, permanent(CodePolicyDenied, "emit destination policy denied publication", err)
	}
	if err := ctx.Err(); err != nil {
		return stepkind.StepResult{}, err
	}
	baseObservation := observationFor(envelope)
	baseObservation.Phase = ObservationAuthorized
	k.observe(ctx, baseObservation)
	publisherEnvelope, cloneErr := cloneEnvelope(envelope)
	if cloneErr != nil {
		return stepkind.StepResult{}, permanent(CodeInvalidInvocation, "emit envelope copy is invalid", cloneErr)
	}
	result, publishErr := k.publisher.Publish(ctx, publisherEnvelope)
	if publishErr != nil {
		failed := baseObservation
		failed.Phase = ObservationFailed
		failed.Code = publicationFailureCode(publishErr)
		k.observe(ctx, failed)
		if contextErr := ctx.Err(); contextErr != nil {
			return stepkind.StepResult{}, contextErr
		}
		return stepkind.StepResult{}, publishFailure(publishErr)
	}
	if err := ctx.Err(); err != nil {
		failed := baseObservation
		failed.Phase = ObservationFailed
		failed.Code = "canceled"
		k.observe(ctx, failed)
		return stepkind.StepResult{}, err
	}
	result.Attributes = cloneStringMap(result.Attributes)
	if result.Attributes == nil {
		result.Attributes = map[string]string{}
	}
	result.PublishedAt = result.PublishedAt.UTC()
	resultErr := result.Validate()
	if resultErr != nil || result.EnvelopeID != envelope.ID {
		if resultErr == nil {
			resultErr = errors.New("publication result envelope identity mismatch")
		}
		failed := baseObservation
		failed.Phase = ObservationFailed
		failed.Code = "invalid_result"
		k.observe(ctx, failed)
		return stepkind.StepResult{}, permanent(CodeInvalidResult, "emit publisher returned an invalid receipt", resultErr)
	}
	published := baseObservation
	published.Phase = ObservationPublished
	published.Outcome = result.Outcome
	k.observe(ctx, published)
	output := map[string]any{
		"id": envelope.ID,
		"destination": map[string]any{
			"kind": envelope.Destination.Kind, "reference": envelope.Destination.Reference,
			"attributes": cloneStringMap(envelope.Destination.Attributes),
		},
		"event_type": envelope.EventType, "correlation": envelope.Correlation,
		"payload_digest": payload.Digest, "payload_type": string(payload.Type),
		"publication_id": result.PublicationID, "published_at": result.PublishedAt.Format("2006-01-02T15:04:05.999999999Z07:00"),
		"replayed": result.Outcome == PublicationReplayed, "attributes": cloneStringMap(result.Attributes),
	}
	value, err := values.NewInline(output, values.Metadata{
		Producer:  values.Producer{Kind: KindName, Reference: producerReference(prepared.Invocation.Identity), Output: "envelope"},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		return stepkind.StepResult{}, permanent(CodeInvalidResult, "emit receipt output is invalid", err)
	}
	return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"envelope": value}}, nil
}

func observationFor(envelope Envelope) Observation {
	encoded, _ := json.Marshal(envelope.Destination)
	return Observation{
		EnvelopeID: envelope.ID, EventType: envelope.EventType, DestinationKind: envelope.Destination.Kind,
		DestinationDigest: values.SHA256Digest(encoded), PayloadType: envelope.Payload.Type,
		PayloadDigest: envelope.Payload.Digest, Redaction: envelope.Payload.Redaction,
	}
}

func publicationFailureCode(err error) string {
	switch {
	case errors.Is(err, ErrPublicationConflict):
		return "conflict"
	case errors.Is(err, ErrPublicationTransient):
		return "transient"
	default:
		return "rejected"
	}
}

func (k *Kind) observe(ctx context.Context, observation Observation) {
	if k == nil || nilInterface(k.observer) {
		return
	}
	_ = k.observer.ObserveEmit(ctx, observation)
}

func parseConfig(input graph.Config) (config, error) {
	if input == nil {
		return config{}, fmt.Errorf("%w: config must be an object", ErrInvalidConfig)
	}
	encoded, marshalErr := json.Marshal(input)
	if marshalErr != nil || len(encoded) > maxConfigBytes {
		return config{}, fmt.Errorf("%w: config exceeds safe JSON bounds", ErrInvalidConfig)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var object map[string]any
	if decodeErr := decoder.Decode(&object); decodeErr != nil {
		return config{}, fmt.Errorf("%w: config is not JSON-compatible", ErrInvalidConfig)
	}
	if trailingErr := decoder.Decode(&struct{}{}); !errors.Is(trailingErr, io.EOF) {
		return config{}, fmt.Errorf("%w: config contains trailing JSON", ErrInvalidConfig)
	}
	allowed := map[string]struct{}{"destination": {}, "event_type": {}, "payload_input": {}, "correlation": {}}
	for _, key := range sortedKeys(object) {
		if _, found := allowed[key]; !found {
			return config{}, fmt.Errorf("%w: config.%s is not supported", ErrInvalidConfig, key)
		}
	}
	destinationObject, ok := object["destination"].(map[string]any)
	if !ok {
		return config{}, fmt.Errorf("%w: config.destination must be an object", ErrInvalidConfig)
	}
	destination, err := parseDestination(destinationObject)
	if err != nil {
		return config{}, err
	}
	eventType, ok := object["event_type"].(string)
	if !ok || stableIdentifier("config.event_type", eventType) != nil {
		return config{}, fmt.Errorf("%w: config.event_type must be a normalized identifier", ErrInvalidConfig)
	}
	payloadInput, ok := object["payload_input"].(string)
	if !ok || graph.ValidateID(payloadInput) != nil {
		return config{}, fmt.Errorf("%w: config.payload_input must be a normalized input name", ErrInvalidConfig)
	}
	correlation := ""
	if raw, found := object["correlation"]; found {
		correlation, ok = raw.(string)
		if !ok || safeOpaqueText("config.correlation", correlation, maxStableTextBytes) != nil {
			return config{}, fmt.Errorf("%w: config.correlation must be stable text", ErrInvalidConfig)
		}
	}
	return config{Destination: destination, EventType: eventType, PayloadInput: payloadInput, Correlation: correlation}, nil
}

func parseDestination(input map[string]any) (Destination, error) {
	allowed := map[string]struct{}{"kind": {}, "reference": {}, "attributes": {}}
	for _, key := range sortedKeys(input) {
		if _, found := allowed[key]; !found {
			return Destination{}, fmt.Errorf("%w: config.destination.%s is not supported", ErrInvalidConfig, key)
		}
	}
	kind, kindOK := input["kind"].(string)
	reference, referenceOK := input["reference"].(string)
	attributes := map[string]string{}
	if raw, found := input["attributes"]; found {
		object, ok := raw.(map[string]any)
		if !ok || len(object) > maxAttributes {
			return Destination{}, fmt.Errorf("%w: config.destination.attributes exceeds safe bounds", ErrInvalidConfig)
		}
		for _, key := range sortedKeys(object) {
			value, ok := object[key].(string)
			if !ok {
				return Destination{}, fmt.Errorf("%w: config.destination.attributes must contain strings", ErrInvalidConfig)
			}
			attributes[key] = value
		}
	}
	destination := Destination{Kind: kind, Reference: reference, Attributes: attributes}
	if !kindOK || !referenceOK || destination.Validate() != nil {
		return Destination{}, fmt.Errorf("%w: config.destination is invalid", ErrInvalidConfig)
	}
	return destination, nil
}

func configSchema() graph.Schema {
	return graph.Schema{
		"type": "object", "additionalProperties": false,
		"required": []any{"destination", "event_type", "payload_input"},
		"properties": map[string]any{
			"destination": map[string]any{
				"type": "object", "additionalProperties": false, "required": []any{"kind", "reference"},
				"properties": map[string]any{
					"kind":       map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("128")},
					"reference":  map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("4096")},
					"attributes": map[string]any{"type": "object", "maxProperties": json.Number("32"), "additionalProperties": map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("1024")}},
				},
			},
			"event_type":    map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("128")},
			"payload_input": map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("128")},
			"correlation":   map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("4096")},
		},
	}
}

func outputSchema() graph.Schema {
	return graph.Schema{
		"type": "object", "additionalProperties": false, "required": []any{"envelope"},
		"properties": map[string]any{"envelope": map[string]any{
			"type": "object", "additionalProperties": false,
			"required": []any{"id", "destination", "event_type", "correlation", "payload_digest", "payload_type", "publication_id", "published_at", "replayed", "attributes"},
			"properties": map[string]any{
				"id": map[string]any{"type": "string"}, "destination": map[string]any{"type": "object"},
				"event_type": map[string]any{"type": "string"}, "correlation": map[string]any{"type": "string"},
				"payload_digest": map[string]any{"type": "string"}, "payload_type": map[string]any{"type": "string"},
				"publication_id": map[string]any{"type": "string"}, "published_at": map[string]any{"type": "string"},
				"replayed": map[string]any{"type": "boolean"}, "attributes": map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}},
			},
		}},
	}
}

func envelopeID(identity stepkind.InvocationIdentity, envelope Envelope) (string, error) {
	seed := struct {
		Identity       stepkind.InvocationIdentity `json:"identity"`
		Destination    Destination                 `json:"destination"`
		EventType      string                      `json:"event_type"`
		Correlation    string                      `json:"correlation"`
		PayloadDigest  string                      `json:"payload_digest"`
		PayloadType    values.Type                 `json:"payload_type"`
		IdempotencyKey string                      `json:"idempotency_key"`
	}{identity, cloneDestination(envelope.Destination), envelope.EventType, envelope.Correlation, envelope.Payload.Digest, envelope.Payload.Type, envelope.IdempotencyKey}
	encoded, err := json.Marshal(seed)
	if err != nil {
		return "", err
	}
	return "emit-" + strings.TrimPrefix(values.SHA256Digest(encoded), "sha256:")[:32], nil
}

func cloneEnvelope(input Envelope) (Envelope, error) {
	input.Destination = cloneDestination(input.Destination)
	cloned, err := cloneValue(input.Payload)
	if err != nil {
		return Envelope{}, err
	}
	input.Payload = cloned
	return input, nil
}

func cloneValue(input values.Value) (values.Value, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return values.Value{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var output values.Value
	if err := decoder.Decode(&output); err != nil {
		return values.Value{}, err
	}
	return output, nil
}

func cloneDestination(input Destination) Destination {
	input.Attributes = cloneStringMap(input.Attributes)
	return input
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func sortedKeys(input map[string]any) []string {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func invocationCorrelation(identity stepkind.InvocationIdentity) string {
	value := identity.RunID + ":" + identity.NodeID
	if identity.Iteration != "" {
		value += ":" + identity.Iteration
	}
	return value
}

func producerReference(identity stepkind.InvocationIdentity) string {
	value := identity.RunID + "/" + identity.NodeID
	if identity.Iteration != "" {
		value += "/" + identity.Iteration
	}
	return value + fmt.Sprintf("/attempt-%d", identity.Attempt)
}

func permanent(code, message string, cause error) error {
	return &stepkind.ExecutionError{Code: code, Message: message, Classification: stepkind.RetryPermanent, Cause: cause}
}

func publishFailure(cause error) error {
	classification := stepkind.ClassifyError(cause)
	if errors.Is(cause, ErrPublicationTransient) {
		classification = stepkind.Retryable
	} else if classification == stepkind.RetryUnspecified {
		classification = stepkind.RetryPermanent
	}
	return &stepkind.ExecutionError{Code: CodePublishFailed, Message: "emit publisher failed", Classification: classification, Cause: cause}
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	current := reflect.ValueOf(value)
	switch current.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return current.IsNil()
	default:
		return false
	}
}
