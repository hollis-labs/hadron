package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

type Kind struct{ host Host }

func New(options Options) (*Kind, error) {
	if nilInterface(options.Host) {
		return nil, fmt.Errorf("%w: host is required", ErrInvalidOptions)
	}
	return &Kind{host: options.Host}, nil
}

func Register(registry stepkind.Registry, options Options) (*Kind, error) {
	if nilInterface(registry) {
		return nil, fmt.Errorf("%w: registry is required", ErrInvalidOptions)
	}
	kind, err := New(options)
	if err != nil {
		return nil, err
	}
	return kind, registry.Register(kind)
}

func (*Kind) Spec() stepkind.StepKindSpec {
	return stepkind.StepKindSpec{
		Name: KindName, Version: KindVersion,
		ConfigSchema: graph.Schema{"type": "object", "additionalProperties": false, "required": []any{"provider", "config"}, "properties": map[string]any{"provider": map[string]any{"type": "string", "minLength": 1, "maxLength": 128}, "config": map[string]any{"type": "object"}}},
		InputSchema:  graph.Schema{"type": "object"}, OutputSchema: graph.Schema{"type": "object"},
		Effects:              graph.EffectSet{graph.EffectMaterialize, graph.EffectMutate, graph.EffectDestructive},
		RequiredCapabilities: []string{"service.start", "service.observe", "service.stop"},
		Idempotency:          graph.IdempotencyKeyed, RetrySafety: stepkind.RetryRequiresIdempotency,
		Cancellation: stepkind.CancellationSpec{Mode: stepkind.CancellationExplicit},
		Observation:  stepkind.ObservationSpec{Mode: stepkind.ObservationPoll, Heartbeat: true},
		Lifecycle:    stepkind.LifecycleSpec{Service: true},
		CanSuspend:   false, EmbeddedModeSupported: false,
	}
}

func (*Kind) ValidateConfig(_ context.Context, config graph.Config) []diagnostic.Diagnostic {
	if _, _, err := parseConfig(config); err != nil {
		return []diagnostic.Diagnostic{{Severity: diagnostic.SeverityError, Code: stepkind.CodeInvalidConfig, Message: "invalid service configuration"}}
	}
	return nil
}

func (k *Kind) Execute(ctx context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	if ctx == nil || k == nil || nilInterface(k.host) {
		return stepkind.StepResult{}, permanent("service_invalid_invocation", "service invocation is invalid", ErrInvalidOptions)
	}
	if err := ctx.Err(); err != nil {
		return stepkind.StepResult{}, err
	}
	if err := prepared.Invocation.Validate(); err != nil {
		return stepkind.StepResult{}, permanent("service_invalid_invocation", "service invocation is invalid", err)
	}
	provider, config, err := parseConfig(prepared.Invocation.Config)
	if err != nil {
		return stepkind.StepResult{}, permanent("service_invalid_config", "service configuration is invalid", err)
	}
	if binding := prepared.Invocation.Service; binding != nil {
		if binding.Absent {
			return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{}}, nil
		}
		return stepkind.StepResult{Outcome: stepkind.StepExternal, External: cloneRef(&binding.Ref)}, nil
	}
	if strings.TrimSpace(prepared.Invocation.IdempotencyKey) == "" {
		return stepkind.StepResult{}, permanent("service_idempotency_required", "service start requires runtime idempotency", ErrInvalidConfig)
	}
	ref, err := k.host.Start(ctx, StartRequest{Identity: prepared.Invocation.Identity, Provider: provider, Config: config, IdempotencyKey: prepared.Invocation.IdempotencyKey})
	if err != nil {
		if ctx.Err() != nil {
			return stepkind.StepResult{}, ctx.Err()
		}
		return stepkind.StepResult{}, transient("service_start_failed", "service start failed", err)
	}
	if err := ctx.Err(); err != nil {
		return stepkind.StepResult{}, err
	}
	if err := ref.Validate(); err != nil {
		return stepkind.StepResult{}, permanent("service_reference_invalid", "service host returned an invalid reference", err)
	}
	return stepkind.StepResult{Outcome: stepkind.StepExternal, External: cloneRef(&ref)}, nil
}

func (k *Kind) ObserveService(ctx context.Context, ref stepkind.ExternalOperationRef) (stepkind.ServiceObservation, error) {
	if ctx == nil || k == nil || nilInterface(k.host) {
		return stepkind.ServiceObservation{}, permanent("service_observe_failed", "service observation failed", ErrInvalidOptions)
	}
	if err := ref.Validate(); err != nil {
		return stepkind.ServiceObservation{}, permanent("service_observe_failed", "service observation failed", err)
	}
	observation, err := k.host.Observe(ctx, *cloneRef(&ref))
	if err != nil {
		if ctx.Err() != nil {
			return stepkind.ServiceObservation{}, ctx.Err()
		}
		return stepkind.ServiceObservation{}, transient("service_observe_failed", "service observation failed", err)
	}
	if err := ctx.Err(); err != nil {
		return stepkind.ServiceObservation{}, err
	}
	if err := observation.Validate(); err != nil {
		return stepkind.ServiceObservation{}, permanent("service_observation_invalid", "service observation is invalid", err)
	}
	if err := safeObservation(observation); err != nil {
		return stepkind.ServiceObservation{}, permanent("service_observation_invalid", "service observation is invalid", err)
	}
	return cloneObservation(observation), nil
}

func (k *Kind) RequestStop(ctx context.Context, ref stepkind.ExternalOperationRef, key string) error {
	if ctx == nil || k == nil || nilInterface(k.host) || !stableText(key, 512) {
		return permanent("service_stop_failed", "service stop request is invalid", ErrInvalidOptions)
	}
	if err := ref.Validate(); err != nil {
		return permanent("service_stop_failed", "service stop request is invalid", err)
	}
	if err := k.host.RequestStop(ctx, *cloneRef(&ref), key); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return transient("service_stop_failed", "service stop request failed", err)
	}
	return ctx.Err()
}

func parseConfig(input graph.Config) (string, graph.Config, error) {
	if len(input) != 2 {
		return "", nil, ErrInvalidConfig
	}
	provider, ok := input["provider"].(string)
	if !ok || !stableText(provider, 128) {
		return "", nil, ErrInvalidConfig
	}
	var raw map[string]any
	switch configured := input["config"].(type) {
	case map[string]any:
		raw = configured
	case graph.Config:
		raw = map[string]any(configured)
	default:
		return "", nil, ErrInvalidConfig
	}
	encoded, err := json.Marshal(raw)
	if err != nil || len(encoded) > 256<<10 {
		return "", nil, ErrInvalidConfig
	}
	var cloned graph.Config
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&cloned); err != nil {
		return "", nil, ErrInvalidConfig
	}
	return provider, cloned, nil
}

func cloneRef(ref *stepkind.ExternalOperationRef) *stepkind.ExternalOperationRef {
	if ref == nil {
		return nil
	}
	copyRef := *ref
	copyRef.Metadata = make(map[string]string, len(ref.Metadata))
	for key, value := range ref.Metadata {
		copyRef.Metadata[key] = value
	}
	return &copyRef
}

func cloneObservation(input stepkind.ServiceObservation) stepkind.ServiceObservation {
	encoded, _ := json.Marshal(input)
	var output stepkind.ServiceObservation
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	_ = decoder.Decode(&output)
	if input.Outputs != nil && output.Outputs == nil {
		output.Outputs = values.ValueSet{}
	}
	if input.Progress != nil && output.Progress == nil {
		output.Progress = map[string]string{}
	}
	return output
}

func stableText(value string, limit int) bool {
	return value != "" && len(value) <= limit && value == strings.TrimSpace(value) && utf8.ValidString(value) && !strings.ContainsAny(value, "\x00\r\n\t")
}

func safeObservation(observation stepkind.ServiceObservation) error {
	for key, value := range observation.Progress {
		if !safeOperationalText(key) || !safeOperationalText(value) {
			return errors.New("service progress contains unsafe durable metadata")
		}
	}
	if observation.Failure != nil {
		if !safeOperationalText(observation.Failure.Code) || !safeOperationalText(observation.Failure.Message) {
			return errors.New("service failure contains unsafe durable metadata")
		}
		for key, value := range observation.Failure.Details {
			if !safeOperationalText(key) || !safeOperationalText(value) {
				return errors.New("service failure details contain unsafe durable metadata")
			}
		}
	}
	return nil
}

func safeOperationalText(value string) bool {
	lower := strings.ToLower(value)
	for _, marker := range []string{"secret://", "bearer ", "authorization", "password", "api-key", "api_key", "client-secret", "client_secret", "private-key", "private_key", "token=", "credential"} {
		if strings.Contains(lower, marker) {
			return false
		}
	}
	return !strings.Contains(value, "://") && !strings.ContainsAny(value, "?#")
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

func permanent(code, message string, cause error) error {
	return &stepkind.ExecutionError{Code: code, Message: message, Classification: stepkind.RetryPermanent, Cause: cause}
}
func transient(code, message string, cause error) error {
	return &stepkind.ExecutionError{Code: code, Message: message, Classification: stepkind.Retryable, Cause: cause}
}

var _ stepkind.StepKind = (*Kind)(nil)
var _ stepkind.ServiceController = (*Kind)(nil)
var _ = errors.Is
