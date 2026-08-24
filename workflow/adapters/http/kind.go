package http

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	nethttp "net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

// Kind is the registered HTTP step-kind implementation.
type Kind struct {
	resolver       Resolver
	policy         Policy
	transport      Transport
	secrets        values.SecretResolver
	artifacts      ArtifactSink
	observer       Observer
	maxHeaderBytes int64
}

// New constructs http@v1. Policy is required: the adapter never supplies a
// permissive destination policy. Resolver and transport receive secure local
// defaults when omitted.
func New(options Options) (*Kind, error) {
	if nilInterface(options.Policy) {
		return nil, fmt.Errorf("%w: destination policy is required", ErrInvalidOptions)
	}
	resolver := options.Resolver
	if nilInterface(resolver) {
		resolver = defaultResolver{resolver: netDefaultResolver()}
	}
	headerLimit := options.MaxHeaderBytes
	if headerLimit == 0 {
		headerLimit = defaultMaxResponseHeader
	}
	if headerLimit < 1024 || headerLimit > 16<<20 {
		return nil, fmt.Errorf("%w: max header bytes must be between 1 KiB and 16 MiB", ErrInvalidOptions)
	}
	transport := options.Transport
	if nilInterface(transport) {
		var err error
		transport, err = NewPinnedTransport(PinnedTransportOptions{MaxResponseHeaderBytes: headerLimit})
		if err != nil {
			return nil, err
		}
	}
	return &Kind{
		resolver: resolver, policy: options.Policy, transport: transport,
		secrets: options.Secrets, artifacts: options.Artifacts, observer: options.Observer,
		maxHeaderBytes: headerLimit,
	}, nil
}

// Register constructs and registers http@v1.
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

// Spec returns fail-safe metadata for arbitrary HTTP requests. Trusted host
// policy may provide a narrower per-config description through DescribeConfig.
func (*Kind) Spec() stepkind.StepKindSpec {
	return stepkind.StepKindSpec{
		Name: KindName, Version: KindVersion,
		ConfigSchema: configSchema(), InputSchema: graph.Schema{}, OutputSchema: outputSchema(),
		Effects:              append(graph.EffectSet(nil), conservativeEffects...),
		RequiredCapabilities: []string{"network.http"},
		Idempotency:          graph.IdempotencyKeyed, RetrySafety: stepkind.RetryRequiresIdempotency,
		Cancellation:          stepkind.CancellationSpec{Mode: stepkind.CancellationContext},
		Observation:           stepkind.ObservationSpec{Mode: stepkind.ObservationNone},
		EmbeddedModeSupported: true,
	}
}

// ValidateConfig reports deterministic adapter-specific findings without
// resolving DNS, policy, or secrets.
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
		Message: "invalid HTTP step configuration: " + err.Error(),
	}}
}

// DescribeConfig returns the deterministic non-secret policy view. Only a
// coherent trusted description for a safe method can narrow the conservative
// immutable metadata.
func (k *Kind) DescribeConfig(ctx context.Context, input graph.Config) (ConfigDescription, error) {
	if ctx == nil {
		return ConfigDescription{}, fmt.Errorf("%w: context is required", ErrInvalidConfig)
	}
	parsed, err := parseConfig(input)
	if err != nil {
		return ConfigDescription{}, err
	}
	return k.describeParsed(ctx, parsed)
}

func (k *Kind) describeParsed(ctx context.Context, parsed config) (ConfigDescription, error) {
	declaration := RequestDeclaration{
		Method: parsed.Method, Scheme: parsed.URL.Scheme, Host: parsed.URL.Hostname(),
		Port: portOf(parsed.URL), Path: parsed.URL.EscapedPath(),
		Effects: append(graph.EffectSet(nil), parsed.Effects...), Capabilities: append([]string(nil), parsed.Capabilities...),
		HasBody: parsed.HasBody, HasSecretRefs: parsed.HasSecretReferences,
		HasIdempotencyKey: parsed.IdempotencyKey != "", RedirectMode: parsed.Redirects.Mode,
	}
	policyDescription, err := k.policy.DescribeRequest(ctx, declaration)
	if err != nil {
		return ConfigDescription{}, fmt.Errorf("describe HTTP request policy: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return ConfigDescription{}, err
	}
	policyDescription = clonePolicyDescription(policyDescription)
	description := ConfigDescription{
		Method: parsed.Method, Origin: parsed.Origin,
		DeclaredEffects:       append(graph.EffectSet(nil), parsed.Effects...),
		DeclaredCapabilities:  append([]string(nil), parsed.Capabilities...),
		IdempotencyKeyPresent: parsed.IdempotencyKey != "",
		Policy:                policyDescription,
		EffectiveEffects:      append(graph.EffectSet(nil), conservativeEffects...),
		EffectiveIdempotency:  graph.IdempotencyNone, EffectiveRetrySafety: stepkind.RetryUnsupported,
	}
	if parsed.IdempotencyKey != "" {
		description.EffectiveIdempotency = graph.IdempotencyKeyed
		description.EffectiveRetrySafety = stepkind.RetryRequiresIdempotency
	}
	if safeMethod(parsed.Method) && validTrustedDescription(policyDescription) {
		description.EffectiveEffects = append(graph.EffectSet(nil), policyDescription.Effects...)
		if policyDescription.Idempotency == graph.IdempotencyIntrinsic && policyDescription.RetrySafety == stepkind.RetrySafe {
			description.EffectiveIdempotency = graph.IdempotencyIntrinsic
			description.EffectiveRetrySafety = stepkind.RetrySafe
		} else if parsed.IdempotencyKey != "" && policyDescription.Idempotency == graph.IdempotencyKeyed && policyDescription.RetrySafety == stepkind.RetryRequiresIdempotency {
			description.EffectiveIdempotency = graph.IdempotencyKeyed
			description.EffectiveRetrySafety = stepkind.RetryRequiresIdempotency
		}
	}
	return description, nil
}

// Execute performs exactly one exchange per authorized hop and never retries
// a transport failure transparently.
func (k *Kind) Execute(ctx context.Context, invocation stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	if ctx == nil {
		return stepkind.StepResult{}, permanent("http_invalid_invocation", "HTTP invocation is invalid", errors.New("context is required"), nil)
	}
	if err := invocation.Invocation.Validate(); err != nil {
		return stepkind.StepResult{}, permanent("http_invalid_invocation", "HTTP invocation is invalid", err, nil)
	}
	parsed, err := parseConfig(invocation.Invocation.Config)
	if err != nil {
		return stepkind.StepResult{}, permanent("http_invalid_config", "HTTP step configuration is invalid", err, nil)
	}
	if invocation.Invocation.IdempotencyKey != "" {
		if parsed.IdempotencyKey != "" && parsed.IdempotencyKey != invocation.Invocation.IdempotencyKey {
			return stepkind.StepResult{}, permanent("http_idempotency_conflict", "HTTP idempotency declarations conflict", nil, safeDetails(parsed, 0))
		}
		parsed.IdempotencyKey = invocation.Invocation.IdempotencyKey
	}
	operationDeadline := time.Now().Add(parsed.Timeout)
	if !invocation.Invocation.Deadline.IsZero() && invocation.Invocation.Deadline.Before(operationDeadline) {
		operationDeadline = invocation.Invocation.Deadline
	}
	operationCtx, cancel := context.WithDeadline(ctx, operationDeadline)
	defer cancel()
	if _, describeErr := k.describeParsed(operationCtx, parsed); describeErr != nil {
		return stepkind.StepResult{}, classifyFailure("http_policy_description", "HTTP request policy is unavailable", describeErr, parsed, 0)
	}
	resolved, err := resolveRequestWith(operationCtx, k.secrets, parsed, k.maxHeaderBytes, maximumMaxResponseBytes)
	if err != nil {
		return stepkind.StepResult{}, classifyFailure("http_secret_resolution", "HTTP request secret resolution failed", err, parsed, 0)
	}
	defer func() {
		zeroBytes(resolved.Body)
		clearHeaders(resolved.Headers)
		forgetSecrets(resolved.Secrets)
	}()

	requestCtx := operationCtx
	currentURL := cloneURL(parsed.URL)
	method := parsed.Method
	headers := resolved.Headers.Clone()
	defer clearHeaders(headers)
	body := resolved.Body
	visited := map[string]struct{}{canonicalRedirectKey(currentURL): {}}
	var redirect *RedirectContext

	for hop := 0; ; hop++ {
		hopConfig := parsed
		hopConfig.Method = method
		hopConfig.Origin = originOf(currentURL)
		destination, resolveErr := resolveDestination(requestCtx, k.resolver, k.policy, currentURL, method, hop, redirect)
		if resolveErr != nil {
			k.observe(requestCtx, Observation{Phase: ObservationError, Origin: originOf(currentURL), Method: method, Hop: hop, Code: "destination_denied"})
			return stepkind.StepResult{}, classifyFailure("http_destination", "HTTP destination resolution or authorization failed", resolveErr, hopConfig, 0)
		}
		if redirect != nil && redirect.MethodRewrite && (!parsed.Redirects.AllowMethodRewrite || !destination.RewriteOK) {
			return stepkind.StepResult{}, permanent("http_redirect_method", "HTTP redirect method rewrite was denied", nil, safeDetails(hopConfig, redirect.Status))
		}
		requestBody := append([]byte(nil), body...)
		request, requestErr := nethttp.NewRequestWithContext(requestCtx, method, currentURL.String(), bytes.NewReader(requestBody))
		if requestErr != nil {
			zeroBytes(requestBody)
			return stepkind.StepResult{}, permanent("http_invalid_request", "HTTP request could not be constructed", requestErr, safeDetails(hopConfig, 0))
		}
		request.Header = headers.Clone()
		request.Host = currentURL.Host
		k.observe(requestCtx, Observation{
			Phase: ObservationRequest, Origin: originOf(currentURL), Method: method, Hop: hop,
			Headers: sanitizeRequestHeaders(request.Header, resolved.Redactor, resolved.SecretHeaders), BodyBytes: int64(len(requestBody)),
		})
		response, exchangeErr := k.transport.RoundTrip(requestCtx, Exchange{Request: request, Destination: destination.Destination})
		clearRequest(request, requestBody)
		if contextErr := requestCtx.Err(); contextErr != nil {
			closeResponse(response)
			return stepkind.StepResult{}, classifyFailure("http_transport", "HTTP transport did not complete before the operation deadline", contextTransportError(contextErr), hopConfig, 0)
		}
		if exchangeErr != nil {
			closeResponse(response)
			k.observe(requestCtx, Observation{Phase: ObservationError, Origin: originOf(currentURL), Method: method, Hop: hop, Code: "transport"})
			return stepkind.StepResult{}, classifyFailure("http_transport", "HTTP transport failed", exchangeErr, hopConfig, 0)
		}
		if response == nil || response.Body == nil {
			closeResponse(response)
			return stepkind.StepResult{}, permanent("http_invalid_response", "HTTP transport returned an invalid response", ErrInvalidResult, safeDetails(hopConfig, 0))
		}
		if response.Request != nil && response.Request != request {
			clearRequest(response.Request, nil)
		}
		response.Request = nil
		if err := validateHeaderBound(response.Header, k.maxHeaderBytes); err != nil {
			closeResponse(response)
			return stepkind.StepResult{}, permanent("http_response_headers", "HTTP response headers exceeded the configured bound", err, safeDetails(hopConfig, response.StatusCode))
		}
		k.observe(requestCtx, Observation{
			Phase: ObservationResponse, Origin: originOf(currentURL), Method: method, Hop: hop,
			Status: response.StatusCode, Headers: sanitizeResponseHeaders(response.Header, resolved.Redactor),
		})

		if isRedirectStatus(response.StatusCode) {
			rawLocation := response.Header.Get("Location")
			closeResponse(response)
			if parsed.Redirects.Mode == RedirectDeny {
				return stepkind.StepResult{}, permanent("http_redirect_denied", "HTTP redirect was not allowed", nil, safeDetails(hopConfig, response.StatusCode))
			}
			if hop >= parsed.Redirects.MaxHops {
				return stepkind.StepResult{}, permanent("http_redirect_limit", "HTTP redirect limit was exceeded", nil, safeDetails(hopConfig, response.StatusCode))
			}
			target, targetErr := redirectTarget(currentURL, rawLocation, resolved.Redactor)
			if targetErr != nil {
				return stepkind.StepResult{}, permanent("http_redirect_invalid", "HTTP redirect target was invalid", targetErr, safeDetails(hopConfig, response.StatusCode))
			}
			key := canonicalRedirectKey(target)
			if _, exists := visited[key]; exists {
				return stepkind.StepResult{}, permanent("http_redirect_loop", "HTTP redirect loop was detected", nil, safeDetails(hopConfig, response.StatusCode))
			}
			visited[key] = struct{}{}
			crossOrigin := originOf(currentURL) != originOf(target)
			if crossOrigin && parsed.Redirects.Mode == RedirectSameOrigin {
				return stepkind.StepResult{}, permanent("http_redirect_origin", "Cross-origin HTTP redirect was denied", nil, safeDetails(hopConfig, response.StatusCode))
			}
			proposedMethod, rewritten := redirectMethod(response.StatusCode, method)
			if crossOrigin && parsed.HasBodySecretRefs && !rewritten && len(body) != 0 {
				return stepkind.StepResult{}, permanent("http_redirect_secret_body", "Cross-origin redirect cannot forward a secret-bearing body", nil, safeDetails(hopConfig, response.StatusCode))
			}
			if crossOrigin {
				stripCrossOriginCredentials(headers, resolved.SecretHeaders)
			}
			if rewritten {
				body = nil
				headers.Del("Content-Type")
				headers.Del("Content-Length")
			}
			redirect = &RedirectContext{
				Status: response.StatusCode, PreviousOrigin: originOf(currentURL), CrossOrigin: crossOrigin,
				Method: method, ProposedMethod: proposedMethod, MethodRewrite: rewritten,
			}
			k.observe(requestCtx, Observation{Phase: ObservationRedirect, Origin: originOf(target), Method: proposedMethod, Hop: hop + 1, Status: response.StatusCode})
			currentURL, method = target, proposedMethod
			continue
		}

		if !statusExpected(parsed.ExpectedStatuses, response.StatusCode) {
			closeResponse(response)
			return stepkind.StepResult{}, statusFailure(hopConfig, response.StatusCode)
		}
		outputs, mapErr := k.mapResponse(requestCtx, invocation.Invocation.Identity, parsed, response, resolved.Redactor, hop, method, originOf(currentURL))
		if mapErr != nil {
			return stepkind.StepResult{}, classifyFailure("http_invalid_response", "HTTP response could not be accepted", mapErr, hopConfig, response.StatusCode)
		}
		if contextErr := requestCtx.Err(); contextErr != nil {
			return stepkind.StepResult{}, classifyFailure("http_timeout", "HTTP operation did not complete before its deadline", contextTransportError(contextErr), hopConfig, response.StatusCode)
		}
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: outputs}, nil
	}
}

func netDefaultResolver() *net.Resolver { return net.DefaultResolver }

func portOf(parsedURL *url.URL) uint16 {
	port, _ := strconv.ParseUint(parsedURL.Port(), 10, 16)
	return uint16(port)
}

func cloneURL(input *url.URL) *url.URL { cloned := *input; return &cloned }

func safeMethod(method string) bool {
	return method == "GET" || method == "HEAD" || method == "OPTIONS"
}

func validTrustedDescription(description PolicyDescription) bool {
	if !description.Trusted || len(description.Effects) == 0 || !description.Idempotency.Valid() || !description.RetrySafety.Valid() {
		return false
	}
	seen := make(map[graph.Effect]bool)
	for _, effect := range description.Effects {
		if !effect.Valid() || seen[effect] {
			return false
		}
		seen[effect] = true
	}
	switch description.Idempotency {
	case graph.IdempotencyIntrinsic:
		return description.RetrySafety == stepkind.RetrySafe
	case graph.IdempotencyKeyed:
		return description.RetrySafety == stepkind.RetryRequiresIdempotency
	case graph.IdempotencyNone:
		return description.RetrySafety == stepkind.RetryUnsupported
	default:
		return false
	}
}

func clonePolicyDescription(input PolicyDescription) PolicyDescription {
	result := input
	result.Effects = append(graph.EffectSet(nil), input.Effects...)
	sort.Slice(result.Effects, func(left, right int) bool { return result.Effects[left] < result.Effects[right] })
	return result
}

func safeDetails(parsed config, status int) map[string]string {
	result := map[string]string{"method": parsed.Method, "origin": parsed.Origin}
	if status != 0 {
		result["status"] = strconv.Itoa(status)
	}
	return result
}

func permanent(code, message string, cause error, details map[string]string) error {
	return &stepkind.ExecutionError{Code: code, Message: message, Classification: stepkind.RetryPermanent, Details: details, Cause: cause}
}

func classifyFailure(code, message string, cause error, parsed config, status int) error {
	classification := stepkind.RetryPermanent
	var transportErr *TransportError
	if errors.As(cause, &transportErr) {
		switch transportErr.Failure {
		case FailureDNS, FailureConnect, FailureTimeout:
			classification = stepkind.Retryable
		case FailureCanceled, FailureTLS, FailureProtocol:
			classification = stepkind.RetryPermanent
		}
	} else if errors.Is(cause, context.DeadlineExceeded) {
		classification = stepkind.Retryable
	} else if errors.Is(cause, context.Canceled) {
		classification = stepkind.RetryPermanent
	}
	return &stepkind.ExecutionError{Code: code, Message: message, Classification: classification, Details: safeDetails(parsed, status), Cause: cause}
}

func statusFailure(parsed config, status int) error {
	classification := stepkind.RetryPermanent
	if status == 408 || status == 425 || status == 429 || status >= 500 {
		classification = stepkind.Retryable
	}
	return &stepkind.ExecutionError{
		Code: "http_unexpected_status", Message: "HTTP response status was not accepted",
		Classification: classification, Details: safeDetails(parsed, status),
	}
}

func statusExpected(configured map[int]struct{}, status int) bool {
	if len(configured) == 0 {
		return status >= 200 && status < 300
	}
	_, ok := configured[status]
	return ok
}

func clearRequest(request *nethttp.Request, body []byte) {
	zeroBytes(body)
	if request != nil {
		for name := range request.Header {
			request.Header.Del(name)
		}
		request.Body = io.NopCloser(strings.NewReader(""))
	}
}

func clearHeaders(headers nethttp.Header) {
	for name := range headers {
		headers.Del(name)
	}
}

func zeroBytes(content []byte) {
	for index := range content {
		content[index] = 0
	}
}

func closeResponse(response *nethttp.Response) {
	if response == nil {
		return
	}
	if response.Request != nil {
		clearRequest(response.Request, nil)
		response.Request = nil
	}
	if response.Body != nil {
		_ = response.Body.Close()
	}
}

func validateHeaderBound(headers nethttp.Header, limit int64) error {
	var size int64
	keys := make([]string, 0, len(headers))
	for key := range headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if !validHeaderName(key) {
			return fmt.Errorf("response contains an invalid header name")
		}
		size += int64(len(key))
		for _, entry := range headers[key] {
			if !validHeaderValue(entry) {
				return fmt.Errorf("response contains an invalid header value")
			}
			size += int64(len(entry))
		}
		if size > limit {
			return fmt.Errorf("response headers exceed configured limit")
		}
	}
	return nil
}

func (k *Kind) observe(ctx context.Context, observation Observation) {
	if nilInterface(k.observer) {
		return
	}
	cloned := observation
	if observation.Headers != nil {
		cloned.Headers = make(map[string][]string, len(observation.Headers))
		for key, entries := range observation.Headers {
			cloned.Headers[key] = append([]string(nil), entries...)
		}
	}
	_ = k.observer.ObserveHTTP(ctx, cloned)
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

var _ stepkind.StepKind = (*Kind)(nil)
