package offline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
)

const (
	DriverRemoteDaemonHTTP = "hadron-remote-http-v1"
	defaultRemoteTimeout   = 30 * time.Second
	maximumRemoteTimeout   = 24 * time.Hour
	maximumRemoteResponse  = 8 << 20
)

type remoteBindingConfig struct {
	Endpoint string
	Timeout  time.Duration
}

// AdaptExecutionRegistry constructs the exact executable catalog for a plan.
// Unbound kinds preserve their registered implementations and specs. A kind
// selected by the versioned remote driver becomes an explicit polling proxy:
// its original immutable contract remains recorded on ResolvedBinding while
// this returned registry supplies the actual manifest/runtime contract.
func AdaptExecutionRegistry(plan compile.ExecutionPlan, source stepkind.Registry, input []ExternalBinding) (*stepkind.MemoryRegistry, error) {
	if nilRegistry(source) {
		return nil, fmt.Errorf("offline source registry is required")
	}
	byKind := make(map[string][]ExternalBinding)
	for _, binding := range input {
		byKind[binding.Kind+"\x00"+binding.Version] = append(byKind[binding.Kind+"\x00"+binding.Version], binding)
	}
	used := make(map[string]struct{})
	result := stepkind.NewRegistry()
	for _, node := range sortedNodes(plan.Graph.Nodes) {
		key := node.Kind + "\x00" + node.KindVersion
		if _, exists := used[key]; exists {
			continue
		}
		used[key] = struct{}{}
		kind, spec, err := stepkind.Resolve(source, node.Kind, node.KindVersion)
		if err != nil {
			return nil, err
		}
		bindings := byKind[key]
		if len(bindings) == 0 {
			if registerErr := result.Register(kind); registerErr != nil {
				return nil, registerErr
			}
			continue
		}
		if hasUnsafeEffects(spec.Effects) && spec.Name != "mcp" && spec.Name != "llm" {
			return nil, fmt.Errorf("unsafe source kind %s@%s is not eligible for remote-profile narrowing", spec.Name, spec.Version)
		}
		var effects graph.EffectSet
		for _, binding := range bindings {
			effects = unionEffects(effects, binding.Effects)
		}
		for _, candidate := range plan.Graph.Nodes {
			if candidate.Kind == node.Kind && candidate.KindVersion == node.KindVersion {
				effects = unionEffects(effects, candidate.Effects)
			}
		}
		if hasUnsafeEffects(effects) {
			return nil, fmt.Errorf("remote execution proxy %s@%s contains unsafe effects", spec.Name, spec.Version)
		}
		spec.Effects = effects
		spec.CanSuspend = false
		spec.EmbeddedModeSupported = true
		spec.Observation = stepkind.ObservationSpec{Mode: stepkind.ObservationPoll}
		spec.Cancellation = stepkind.CancellationSpec{Mode: stepkind.CancellationContext}
		resolved := make([]ResolvedBinding, 0, len(bindings))
		for _, binding := range bindings {
			node, ok := graphNode(plan.Graph, binding.NodeID)
			if !ok {
				return nil, fmt.Errorf("remote binding targets unknown node %q", binding.NodeID)
			}
			description := normalizeDescription(BindingDescription{EffectiveEffects: unionEffects(binding.Effects, node.Effects), Capabilities: binding.Capabilities, RemoteWait: kind.Spec().CanSuspend})
			profile, profileErr := buildExecutionProfile(node, binding, kind.Spec(), spec, description)
			if profileErr != nil {
				return nil, profileErr
			}
			resolved = append(resolved, ResolvedBinding{Binding: binding, Description: description, SourceSpec: kind.Spec(), ExecutionProfile: profile})
		}
		remote, err := newRemoteKind(spec, resolved, kind)
		if err != nil {
			return nil, err
		}
		if err := result.Register(remote); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// RegisterManifestKinds reconstructs the closed executable catalog embedded
// by a generator. The caller supplies concrete local implementations; all
// remaining accepted specs must be backed by the explicit remote-daemon HTTP
// binding selected at build time. Workflow core never imports those adapters.
func RegisterManifestKinds(registry stepkind.Registry, manifest Manifest, localKinds ...stepkind.StepKind) error {
	if nilRegistry(registry) {
		return fmt.Errorf("offline registry is required")
	}
	local := make(map[string]stepkind.StepKind, len(localKinds))
	for _, kind := range localKinds {
		if kind == nil {
			return fmt.Errorf("offline local kind is required")
		}
		spec := kind.Spec()
		key := spec.Name + "\x00" + spec.Version
		if _, exists := local[key]; exists {
			return fmt.Errorf("offline local kind %s@%s is duplicated", spec.Name, spec.Version)
		}
		local[key] = kind
	}
	bindings := make(map[string][]ResolvedBinding)
	for _, resolved := range manifest.Bindings {
		key := resolved.Binding.Kind + "\x00" + resolved.Binding.Version
		bindings[key] = append(bindings[key], resolved)
	}
	for _, spec := range manifest.StepKinds {
		key := spec.Name + "\x00" + spec.Version
		kind := local[key]
		if kind == nil {
			bound := bindings[spec.Name+"\x00"+spec.Version]
			if len(bound) == 0 {
				return fmt.Errorf("offline spec %s@%s has no reconstructible binding", spec.Name, spec.Version)
			}
			remote, err := newRemoteKind(spec, bound, nil)
			if err != nil {
				return err
			}
			kind = remote
		}
		if err := registry.Register(kind); err != nil {
			return err
		}
	}
	return nil
}

type remoteKind struct {
	spec      stepkind.StepKindSpec
	validator stepkind.StepKind
	bindings  map[string]remoteBindingConfig
	profiles  map[string]RemoteExecutionProfile
	client    *http.Client
}

func newRemoteKind(spec stepkind.StepKindSpec, input []ResolvedBinding, validator stepkind.StepKind) (*remoteKind, error) {
	cloned, err := cloneJSON(spec)
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	kind := &remoteKind{spec: cloned, validator: validator, bindings: make(map[string]remoteBindingConfig, len(input)), profiles: make(map[string]RemoteExecutionProfile, len(input)), client: &http.Client{
		Transport:     transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
	for _, resolved := range input {
		binding := resolved.Binding
		if binding.Driver != DriverRemoteDaemonHTTP {
			return nil, fmt.Errorf("unsupported offline driver %q", binding.Driver)
		}
		config, err := parseRemoteBinding(binding)
		if err != nil {
			return nil, err
		}
		if _, duplicate := kind.bindings[binding.NodeID]; duplicate {
			return nil, fmt.Errorf("remote binding for node %q is duplicated", binding.NodeID)
		}
		kind.bindings[binding.NodeID] = config
		kind.profiles[binding.NodeID] = resolved.ExecutionProfile
	}
	return kind, nil
}

func (k *remoteKind) Spec() stepkind.StepKindSpec {
	cloned, _ := cloneJSON(k.spec)
	return cloned
}

func (k *remoteKind) ValidateConfig(ctx context.Context, config graph.Config) []diagnostic.Diagnostic {
	if k.validator == nil {
		return nil
	}
	return k.validator.ValidateConfig(ctx, config)
}

type remoteExecutionRequest struct {
	Kind       string                 `json:"kind"`
	Version    string                 `json:"version"`
	Profile    RemoteExecutionProfile `json:"execution_profile"`
	Invocation stepkind.Invocation    `json:"invocation"`
}

type remoteExecutionResponse struct {
	Result  *stepkind.StepResult `json:"result,omitempty"`
	Pending *struct {
		OperationID string `json:"operation_id"`
	} `json:"pending,omitempty"`
	Error *struct {
		Code      string `json:"code"`
		Retryable bool   `json:"retryable,omitempty"`
	} `json:"error,omitempty"`
}

func (k *remoteKind) Execute(ctx context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	if ctx == nil || k == nil || k.client == nil {
		return stepkind.StepResult{}, remoteFailure("remote_bridge_unavailable", stepkind.RetryPermanent)
	}
	config, ok := k.bindings[prepared.Invocation.Identity.NodeID]
	if !ok {
		return stepkind.StepResult{}, remoteFailure("remote_binding_unavailable", stepkind.RetryPermanent)
	}
	profile, ok := k.profiles[prepared.Invocation.Identity.NodeID]
	if !ok {
		return stepkind.StepResult{}, remoteFailure("remote_profile_unavailable", stepkind.RetryPermanent)
	}
	operationCtx, cancel := context.WithTimeout(ctx, config.Timeout)
	defer cancel()
	payload, err := json.Marshal(remoteExecutionRequest{Kind: k.spec.Name, Version: k.spec.Version, Profile: profile, Invocation: prepared.Invocation})
	if err != nil {
		return stepkind.StepResult{}, remoteFailure("remote_request_invalid", stepkind.RetryPermanent)
	}
	request, err := http.NewRequestWithContext(operationCtx, http.MethodPost, config.Endpoint, bytes.NewReader(payload))
	if err != nil {
		return stepkind.StepResult{}, remoteFailure("remote_request_invalid", stepkind.RetryPermanent)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := k.client.Do(request)
	if err != nil {
		if errors.Is(operationCtx.Err(), context.DeadlineExceeded) {
			return stepkind.StepResult{}, remoteFailure("remote_timeout", stepkind.Retryable)
		}
		if errors.Is(operationCtx.Err(), context.Canceled) {
			return stepkind.StepResult{}, operationCtx.Err()
		}
		return stepkind.StepResult{}, remoteFailure("remote_transport_failed", stepkind.Retryable)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return stepkind.StepResult{}, remoteFailure("remote_status_rejected", retryForStatus(response.StatusCode))
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maximumRemoteResponse+1))
	if err != nil || len(body) > maximumRemoteResponse {
		return stepkind.StepResult{}, remoteFailure("remote_response_invalid", stepkind.Retryable)
	}
	var envelope remoteExecutionResponse
	if decodeErr := decodeStrictJSON(body, &envelope); decodeErr != nil || populatedRemoteOutcomes(envelope) != 1 {
		return stepkind.StepResult{}, remoteFailure("remote_response_invalid", stepkind.RetryPermanent)
	}
	if envelope.Error != nil {
		classification := stepkind.RetryPermanent
		if envelope.Error.Retryable {
			classification = stepkind.Retryable
		}
		return stepkind.StepResult{}, remoteFailure("remote_execution_failed", classification)
	}
	if envelope.Pending != nil {
		if identifierErr := validateIdentifier("remote operation id", envelope.Pending.OperationID); identifierErr != nil {
			return stepkind.StepResult{}, remoteFailure("remote_response_invalid", stepkind.RetryPermanent)
		}
		return stepkind.StepResult{Outcome: stepkind.StepExternal, External: &stepkind.ExternalOperationRef{
			Kind: "offline.remote", ID: envelope.Pending.OperationID,
			Metadata: map[string]string{"node_id": prepared.Invocation.Identity.NodeID},
		}}, nil
	}
	if validateErr := envelope.Result.Validate(); validateErr != nil || envelope.Result.Outcome != stepkind.StepCompleted {
		return stepkind.StepResult{}, remoteFailure("remote_result_invalid", stepkind.RetryPermanent)
	}
	result, err := cloneJSON(*envelope.Result)
	if err != nil {
		return stepkind.StepResult{}, remoteFailure("remote_result_invalid", stepkind.RetryPermanent)
	}
	return result, nil
}

func (k *remoteKind) Observe(ctx context.Context, ref stepkind.ExternalOperationRef) (stepkind.Observation, error) {
	if ctx == nil || k == nil || k.client == nil {
		return stepkind.Observation{}, remoteFailure("remote_bridge_unavailable", stepkind.RetryPermanent)
	}
	if err := ref.Validate(); err != nil || ref.Kind != "offline.remote" || len(ref.Metadata) != 1 {
		return stepkind.Observation{}, remoteFailure("remote_reference_invalid", stepkind.RetryPermanent)
	}
	nodeID := ref.Metadata["node_id"]
	config, ok := k.bindings[nodeID]
	if !ok {
		return stepkind.Observation{}, remoteFailure("remote_binding_unavailable", stepkind.RetryPermanent)
	}
	operationCtx, cancel := context.WithTimeout(ctx, config.Timeout)
	defer cancel()
	endpoint := strings.TrimRight(config.Endpoint, "/") + "/operations/" + url.PathEscape(ref.ID)
	request, err := http.NewRequestWithContext(operationCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		return stepkind.Observation{}, remoteFailure("remote_request_invalid", stepkind.RetryPermanent)
	}
	profile, ok := k.profiles[nodeID]
	if !ok {
		return stepkind.Observation{}, remoteFailure("remote_profile_unavailable", stepkind.RetryPermanent)
	}
	profileDigest, err := digestCanonical(profile)
	if err != nil {
		return stepkind.Observation{}, remoteFailure("remote_profile_invalid", stepkind.RetryPermanent)
	}
	request.Header.Set("X-Hadron-Execution-Profile", profileDigest)
	response, err := k.client.Do(request)
	if err != nil {
		if errors.Is(operationCtx.Err(), context.DeadlineExceeded) {
			return stepkind.Observation{}, remoteFailure("remote_timeout", stepkind.Retryable)
		}
		if errors.Is(operationCtx.Err(), context.Canceled) {
			return stepkind.Observation{}, operationCtx.Err()
		}
		return stepkind.Observation{}, remoteFailure("remote_transport_failed", stepkind.Retryable)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return stepkind.Observation{}, remoteFailure("remote_status_rejected", retryForStatus(response.StatusCode))
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maximumRemoteResponse+1))
	if err != nil || len(body) > maximumRemoteResponse {
		return stepkind.Observation{}, remoteFailure("remote_response_invalid", stepkind.Retryable)
	}
	var envelope remoteExecutionResponse
	if decodeErr := decodeStrictJSON(body, &envelope); decodeErr != nil || populatedRemoteOutcomes(envelope) != 1 {
		return stepkind.Observation{}, remoteFailure("remote_response_invalid", stepkind.RetryPermanent)
	}
	if envelope.Pending != nil {
		if envelope.Pending.OperationID != ref.ID {
			return stepkind.Observation{}, remoteFailure("remote_reference_invalid", stepkind.RetryPermanent)
		}
		return stepkind.Observation{State: stepkind.ObservationPending, Progress: map[string]string{"state": "pending"}}, nil
	}
	if envelope.Error != nil {
		classification := stepkind.RetryPermanent
		if envelope.Error.Retryable {
			classification = stepkind.Retryable
		}
		failure := remoteFailure("remote_execution_failed", classification)
		return stepkind.Observation{State: stepkind.ObservationFailed, Failure: failure}, nil
	}
	if validateErr := envelope.Result.Validate(); validateErr != nil || envelope.Result.Outcome != stepkind.StepCompleted {
		return stepkind.Observation{}, remoteFailure("remote_result_invalid", stepkind.RetryPermanent)
	}
	result, err := cloneJSON(*envelope.Result)
	if err != nil {
		return stepkind.Observation{}, remoteFailure("remote_result_invalid", stepkind.RetryPermanent)
	}
	return stepkind.Observation{State: stepkind.ObservationSucceeded, Result: &result}, nil
}

func populatedRemoteOutcomes(response remoteExecutionResponse) int {
	count := 0
	if response.Result != nil {
		count++
	}
	if response.Pending != nil {
		count++
	}
	if response.Error != nil {
		count++
	}
	return count
}

func retryForStatus(status int) stepkind.RetryClassification {
	if status == http.StatusRequestTimeout || status == http.StatusTooEarly || status == http.StatusTooManyRequests || status >= 500 {
		return stepkind.Retryable
	}
	return stepkind.RetryPermanent
}

func remoteFailure(code string, classification stepkind.RetryClassification) *stepkind.ExecutionError {
	return &stepkind.ExecutionError{Code: code, Message: "remote daemon workflow operation failed", Classification: classification}
}

func parseRemoteBinding(binding ExternalBinding) (remoteBindingConfig, error) {
	if binding.Driver != DriverRemoteDaemonHTTP {
		return remoteBindingConfig{}, fmt.Errorf("binding driver must be %s", DriverRemoteDaemonHTTP)
	}
	allowed := map[string]bool{"endpoint": true, "timeout": true}
	keys := make([]string, 0, len(binding.Config))
	for key := range binding.Config {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if !allowed[key] {
			return remoteBindingConfig{}, fmt.Errorf("remote binding config contains unsupported field %q", key)
		}
	}
	endpoint, ok := binding.Config["endpoint"].(string)
	if !ok {
		return remoteBindingConfig{}, fmt.Errorf("remote binding endpoint is required")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || !parsed.IsAbs() || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return remoteBindingConfig{}, fmt.Errorf("remote binding endpoint must be an absolute credential-free http(s) URL without query or fragment")
	}
	timeout := defaultRemoteTimeout
	if raw, exists := binding.Config["timeout"]; exists {
		text, ok := raw.(string)
		if !ok {
			return remoteBindingConfig{}, fmt.Errorf("remote binding timeout must be a duration string")
		}
		timeout, err = time.ParseDuration(text)
		if err != nil || timeout <= 0 || timeout > maximumRemoteTimeout {
			return remoteBindingConfig{}, fmt.Errorf("remote binding timeout is out of bounds")
		}
	}
	return remoteBindingConfig{Endpoint: parsed.String(), Timeout: timeout}, nil
}

func validateRemoteDescription(binding ExternalBinding, description BindingDescription) error {
	if _, err := parseRemoteBinding(binding); err != nil {
		return err
	}
	if binding.Driver != DriverRemoteDaemonHTTP {
		return fmt.Errorf("unsupported binding driver")
	}
	if err := validateDescription(description); err != nil {
		return fmt.Errorf("binding catalog description differs from the canonical remote binding: %w", err)
	}
	if !equalStrings(description.Capabilities, binding.Capabilities) {
		return fmt.Errorf("binding catalog capabilities differ from the canonical remote binding")
	}
	return nil
}

func equalEffects(left, right graph.EffectSet) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

var _ stepkind.StepKind = (*remoteKind)(nil)
var _ stepkind.Observer = (*remoteKind)(nil)
