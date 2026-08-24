package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	nethttp "net/http"
	"net/http/httptest"
	"net/netip"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

type fakeResolver struct {
	mu        sync.Mutex
	addresses map[string][]netip.Addr
	err       error
	calls     []string
}

func (r *fakeResolver) LookupNetIP(ctx context.Context, _, host string) ([]netip.Addr, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, host)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return append([]netip.Addr(nil), r.addresses[host]...), r.err
}

type fakePolicy struct {
	mu           sync.Mutex
	description  PolicyDescription
	describeErr  error
	describe     func(context.Context, RequestDeclaration) (PolicyDescription, error)
	declarations []RequestDeclaration
	authorize    func(context.Context, DestinationRequest) (DestinationAuthorization, error)
	requests     []DestinationRequest
}

func (p *fakePolicy) DescribeRequest(ctx context.Context, declaration RequestDeclaration) (PolicyDescription, error) {
	p.mu.Lock()
	p.declarations = append(p.declarations, declaration)
	describe := p.describe
	p.mu.Unlock()
	if describe != nil {
		return describe(ctx, declaration)
	}
	if p.describeErr != nil {
		return PolicyDescription{}, p.describeErr
	}
	if err := ctx.Err(); err != nil {
		return PolicyDescription{}, err
	}
	result := p.description
	result.Effects = append(graph.EffectSet(nil), result.Effects...)
	return result, nil
}

func (p *fakePolicy) AuthorizeDestination(ctx context.Context, request DestinationRequest) (DestinationAuthorization, error) {
	p.mu.Lock()
	p.requests = append(p.requests, request)
	p.mu.Unlock()
	if p.authorize != nil {
		return p.authorize(ctx, request)
	}
	return DestinationAuthorization{}, ctx.Err()
}

type transportFunc func(context.Context, Exchange) (*nethttp.Response, error)

func (f transportFunc) RoundTrip(ctx context.Context, exchange Exchange) (*nethttp.Response, error) {
	return f(ctx, exchange)
}

type artifactSink struct {
	mu       sync.Mutex
	requests []ArtifactRequest
	bad      bool
	uri      string
}

func (s *artifactSink) CaptureArtifact(_ context.Context, request ArtifactRequest) (values.Value, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	request.Content = append([]byte(nil), request.Content...)
	s.requests = append(s.requests, request)
	metadata := request.Metadata
	if s.bad {
		metadata.Producer.Output = "wrong"
	}
	uri := s.uri
	if uri == "" {
		uri = "artifact://test/http-response"
	}
	return values.NewArtifact(values.ArtifactRef{
		Store: "test", URI: uri, Digest: values.SHA256Digest(request.Content),
		MediaType: metadata.MediaType, SizeBytes: int64(len(request.Content)), Producer: metadata.Producer,
		Redaction: metadata.Redaction, Retention: metadata.Retention,
	})
}

func allowPolicy() *fakePolicy {
	return &fakePolicy{authorize: func(context.Context, DestinationRequest) (DestinationAuthorization, error) {
		return DestinationAuthorization{AllowMethodRewrite: true}, nil
	}}
}

func baseConfig(rawURL string) graph.Config { return graph.Config{"url": rawURL} }

func invocation(config graph.Config) stepkind.PreparedInvocation {
	return stepkind.PreparedInvocation{Invocation: stepkind.Invocation{
		Identity: stepkind.InvocationIdentity{RunID: "run-1", NodeID: "http-node", Attempt: 1},
		Config:   config, Inputs: values.ValueSet{},
	}}
}

func response(status int, contentType, body string) *nethttp.Response {
	header := make(nethttp.Header)
	if contentType != "" {
		header.Set("Content-Type", contentType)
	}
	return &nethttp.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body))}
}

func newFakeKind(t *testing.T, transport Transport, extra Options) *Kind {
	t.Helper()
	resolver := extra.Resolver
	if resolver == nil {
		resolver = &fakeResolver{addresses: map[string][]netip.Addr{"example.test": {netip.MustParseAddr("192.0.2.10")}, "other.test": {netip.MustParseAddr("192.0.2.20")}}}
	}
	policy := extra.Policy
	if policy == nil {
		policy = allowPolicy()
	}
	kind, err := New(Options{
		Resolver: resolver, Policy: policy, Transport: transport, Secrets: extra.Secrets,
		Artifacts: extra.Artifacts, Observer: extra.Observer, MaxHeaderBytes: extra.MaxHeaderBytes,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return kind
}

func TestRegistrationDescriptionAndConfigValidation(t *testing.T) {
	policy := allowPolicy()
	policy.description = PolicyDescription{Trusted: true, Effects: graph.EffectSet{graph.EffectRead}, Idempotency: graph.IdempotencyIntrinsic, RetrySafety: stepkind.RetrySafe}
	kind := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
		return response(200, "text/plain", "ok"), nil
	}), Options{Policy: policy})
	registry := stepkind.NewRegistry()
	registered, err := Register(registry, Options{Resolver: &fakeResolver{addresses: map[string][]netip.Addr{}}, Policy: policy, Transport: kind.transport})
	if err != nil || registered.Spec().Name != KindName {
		t.Fatalf("Register() = %v, %v", registered, err)
	}
	if _, ok := registry.Lookup(KindName, KindVersion); !ok {
		t.Fatal("kind was not registered")
	}
	compiledConfig := graph.Config{
		"url": "https://example.test/", "method": "POST", "body": map[string]any{"value": true},
		"auth":         map[string]any{"type": "bearer", "secret_ref": "secret://project/http#token"},
		"redirects":    map[string]any{"mode": "policy", "max_hops": json.Number("3")},
		"capabilities": []any{"network.http", "egress.partner"},
	}
	findings := workflowcompile.ValidateGraph(t.Context(), graph.Graph{
		ID: "http-schema", Version: "v1", Nodes: []graph.Node{{ID: "http-call", Kind: KindName, KindVersion: KindVersion, Config: compiledConfig}},
	}, workflowcompile.ValidationOptions{StepKinds: registry})
	for _, finding := range findings {
		if finding.Code == workflowcompile.CodeInvalidStepConfig {
			t.Fatalf("registered config schema rejected valid config: %#v", findings)
		}
	}
	spec := kind.Spec()
	if spec.Idempotency != graph.IdempotencyKeyed || spec.RetrySafety != stepkind.RetryRequiresIdempotency || !reflect.DeepEqual(spec.Effects, conservativeEffects) {
		t.Fatalf("Spec() = %#v", spec)
	}
	description, err := kind.DescribeConfig(t.Context(), baseConfig("https://example.test/path?visible=yes"))
	if err != nil || !reflect.DeepEqual(description.EffectiveEffects, graph.EffectSet{graph.EffectRead}) || description.EffectiveIdempotency != graph.IdempotencyIntrinsic {
		t.Fatalf("DescribeConfig() = %#v, %v", description, err)
	}
	policy.description.Effects[0] = graph.EffectDestructive
	if description.Policy.Effects[0] != graph.EffectRead {
		t.Fatal("description was not defensively copied")
	}
	policy.description.Trusted = false
	description, err = kind.DescribeConfig(t.Context(), baseConfig("https://example.test/"))
	if err != nil || !reflect.DeepEqual(description.EffectiveEffects, conservativeEffects) || description.EffectiveIdempotency != graph.IdempotencyNone {
		t.Fatalf("untrusted policy did not fail closed = %#v, %v", description, err)
	}
	policy.description = PolicyDescription{Trusted: true, Effects: graph.EffectSet{graph.EffectRead}, Idempotency: graph.IdempotencyIntrinsic, RetrySafety: stepkind.RetryRequiresIdempotency}
	description, err = kind.DescribeConfig(t.Context(), baseConfig("https://example.test/"))
	if err != nil || !reflect.DeepEqual(description.EffectiveEffects, conservativeEffects) {
		t.Fatalf("incoherent policy did not fail closed = %#v, %v", description, err)
	}
	policy.description = PolicyDescription{Trusted: true, Effects: graph.EffectSet{graph.EffectRead}, Idempotency: graph.IdempotencyIntrinsic, RetrySafety: stepkind.RetrySafe}
	post := baseConfig("https://example.test/")
	post["method"] = "POST"
	description, err = kind.DescribeConfig(t.Context(), post)
	if err != nil || !reflect.DeepEqual(description.EffectiveEffects, conservativeEffects) {
		t.Fatalf("unsafe description narrowed = %#v, %v", description, err)
	}

	tests := []struct {
		name   string
		config graph.Config
	}{
		{"userinfo", baseConfig("https://user:pass@example.test/")},
		{"fragment", baseConfig("https://example.test/#fragment")},
		{"secret query ref", baseConfig("https://example.test/?value=secret%3A%2F%2Fproject%2Fkey")},
		{"malformed secret query key", baseConfig("https://example.test/?secret%3A%2F%2FBad%2Fkey=value")},
		{"sensitive query", baseConfig("https://example.test/?api_key=value")},
		{"unsafe method", graph.Config{"url": "https://example.test/", "method": "CONNECT"}},
		{"unknown field", graph.Config{"url": "https://example.test/", "unknown": true}},
		{"invalid header", graph.Config{"url": "https://example.test/", "headers": map[string]any{"Bad Header": "x"}}},
		{"literal auth header", graph.Config{"url": "https://example.test/", "headers": map[string]any{"Authorization": "Bearer literal"}}},
		{"auth collision", graph.Config{"url": "https://example.test/", "headers": map[string]any{"Authorization": "secret://project/first"}, "auth": map[string]any{"type": "bearer", "secret_ref": "secret://project/second"}}},
		{"nested malformed ref", graph.Config{"url": "https://example.test/", "body": map[string]any{"nested": []any{"secret://Project/key"}}}},
		{"inline over max", graph.Config{"url": "https://example.test/", "inline_limit": json.Number("100"), "max_response_bytes": json.Number("99")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			diagnostics := kind.ValidateConfig(t.Context(), test.config)
			if len(diagnostics) != 1 || diagnostics[0].Code != stepkind.CodeInvalidConfig {
				t.Fatalf("diagnostics = %#v", diagnostics)
			}
		})
	}
}

func TestDefaultPinnedTransportUsesApprovedAddressAndIgnoresProxy(t *testing.T) {
	server := httptest.NewServer(nethttp.HandlerFunc(func(writer nethttp.ResponseWriter, request *nethttp.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	serverURL := strings.TrimPrefix(server.URL, "http://")
	_, port, _ := net.SplitHostPort(serverURL)
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("NO_PROXY", "")
	resolver := &fakeResolver{addresses: map[string][]netip.Addr{"logical.test": {netip.MustParseAddr("127.0.0.1")}}}
	kind, err := New(Options{Resolver: resolver, Policy: allowPolicy()})
	if err != nil {
		t.Fatal(err)
	}
	result, err := kind.Execute(t.Context(), invocation(baseConfig("http://logical.test:"+port+"/safe?not_logged=yes")))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("result.Validate() = %v", err)
	}
	if result.Outputs[OutputBodyJSON].Inline.(map[string]any)["ok"] != true {
		t.Fatalf("outputs = %#v", result.Outputs)
	}
}

func TestDefaultPinnedTransportTLSAndFailureClassification(t *testing.T) {
	server := httptest.NewTLSServer(nethttp.HandlerFunc(func(writer nethttp.ResponseWriter, _ *nethttp.Request) {
		writer.Header().Set("Content-Type", "text/plain")
		_, _ = writer.Write([]byte("secure"))
	}))
	defer server.Close()
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(server.URL, "https://"))
	resolver := &fakeResolver{addresses: map[string][]netip.Addr{"example.com": {netip.MustParseAddr("127.0.0.1")}}}
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	pinned, err := NewPinnedTransport(PinnedTransportOptions{TLSConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}})
	if err != nil {
		t.Fatal(err)
	}
	kind := newFakeKind(t, pinned, Options{Resolver: resolver})
	if _, executeErr := kind.Execute(t.Context(), invocation(baseConfig("https://example.com:"+port+"/"))); executeErr != nil {
		t.Fatalf("TLS Execute() error = %v", executeErr)
	}

	untrusted, err := NewPinnedTransport(PinnedTransportOptions{TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}})
	if err != nil {
		t.Fatal(err)
	}
	kind = newFakeKind(t, untrusted, Options{Resolver: resolver})
	_, err = kind.Execute(t.Context(), invocation(baseConfig("https://example.com:"+port+"/")))
	assertCode(t, err, "http_transport", stepkind.RetryPermanent)
	var transportErr *TransportError
	if !errors.As(err, &transportErr) || transportErr.Failure != FailureTLS {
		t.Fatalf("TLS failure = %#v", err)
	}
}

func TestJSONResponseExactNumbersHeadersSchemaAndTrailingContent(t *testing.T) {
	transport := transportFunc(func(_ context.Context, _ Exchange) (*nethttp.Response, error) {
		result := response(201, "application/problem+json; charset=utf-8", `{"n":9007199254740993}`)
		result.Header["X-Mixed"] = []string{"z", "a"}
		result.Header.Set("Set-Cookie", "session=credential")
		result.Header.Set("Location", "https://signed.test/path?signature=value")
		return result, nil
	})
	config := baseConfig("https://example.test/path")
	config["expected_status"] = []any{json.Number("201")}
	config["expected_content_types"] = []any{"application/problem+json"}
	config["expected_json_schema"] = map[string]any{"type": "object", "required": []any{"n"}, "properties": map[string]any{"n": map[string]any{"type": "integer"}}}
	kind := newFakeKind(t, transport, Options{})
	result, err := kind.Execute(t.Context(), invocation(config))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	jsonOutput := result.Outputs[OutputBodyJSON].Inline.(map[string]any)
	if jsonOutput["n"].(json.Number).String() != "9007199254740993" {
		t.Fatalf("exact number = %#v", jsonOutput["n"])
	}
	headers := result.Outputs[OutputHeaders].Inline.(map[string]any)
	if !reflect.DeepEqual(headers["x-mixed"], []any{"a", "z"}) || headers["set-cookie"].([]any)[0] != values.RedactedMarker || headers["location"].([]any)[0] != values.RedactedMarker {
		t.Fatalf("headers = %#v", headers)
	}
	bad := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
		return response(200, "application/json", `{} {}`), nil
	}), Options{})
	_, err = bad.Execute(t.Context(), invocation(baseConfig("https://example.test/")))
	assertCode(t, err, "http_invalid_response", stepkind.RetryPermanent)
	mismatchConfig := baseConfig("https://example.test/")
	mismatchConfig["expected_json_schema"] = map[string]any{"type": "object", "required": []any{"missing"}}
	_, err = kind.Execute(t.Context(), invocation(mismatchConfig))
	assertCode(t, err, "http_invalid_response", stepkind.RetryPermanent)
}

func TestSecretsResolveOnlyAtBoundaryAndAreRedacted(t *testing.T) {
	ref, _ := values.ParseSecretRef("secret://project/http#token")
	var resolved *values.ResolvedSecret
	resolver := values.SecretResolverFunc(func(_ context.Context, requested values.SecretRef) (*values.ResolvedSecret, error) {
		if requested != ref {
			return nil, errors.New("wrong ref")
		}
		var err error
		resolved, err = values.NewResolvedSecret(ref, []byte("topsecret"))
		return resolved, err
	})
	var observed []Observation
	observer := ObserverFunc(func(_ context.Context, event Observation) error {
		observed = append(observed, event)
		return errors.New("ignored")
	})
	transport := transportFunc(func(_ context.Context, exchange Exchange) (*nethttp.Response, error) {
		body, _ := io.ReadAll(exchange.Request.Body)
		if !bytes.Contains(body, []byte(`"token":"topsecret"`)) || exchange.Request.Header.Get("Authorization") != "Bearer topsecret" {
			return nil, errors.New("secret was not injected")
		}
		result := response(200, "application/json", `{"echo":"topsecret","topsecret":"value"}`)
		result.Header.Set("X-Echo", "prefix-topsecret")
		return result, nil
	})
	config := graph.Config{
		"url": "https://example.test/", "body": map[string]any{"nested": map[string]any{"token": string(ref)}},
		"auth": map[string]any{"type": "bearer", "secret_ref": string(ref)},
	}
	kind := newFakeKind(t, transport, Options{Secrets: resolver, Observer: observer})
	result, err := kind.Execute(t.Context(), invocation(config))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	encoded, _ := json.Marshal(result)
	if bytes.Contains(encoded, []byte("topsecret")) {
		t.Fatalf("persisted output leaked secret: %s", encoded)
	}
	for _, event := range observed {
		encoded, _ := json.Marshal(event)
		if bytes.Contains(encoded, []byte("topsecret")) {
			t.Fatalf("observation leaked secret: %s", encoded)
		}
	}
	if resolved == nil || len(resolved.Bytes()) != 0 {
		t.Fatal("resolved secret was not forgotten")
	}
}

func TestResolvedSecretExpansionHonorsBodyAndConfiguredHeaderBounds(t *testing.T) {
	ref, _ := values.ParseSecretRef("secret://project/bounds#token")
	t.Run("body expansion", func(t *testing.T) {
		var secret *values.ResolvedSecret
		resolver := values.SecretResolverFunc(func(context.Context, values.SecretRef) (*values.ResolvedSecret, error) {
			var err error
			secret, err = values.NewResolvedSecret(ref, []byte(strings.Repeat("x", 128)))
			return secret, err
		})
		parsed, err := parseConfig(graph.Config{"url": "https://example.test/", "body": map[string]any{"value": string(ref)}})
		if err != nil {
			t.Fatal(err)
		}
		resolved, err := resolveRequestWith(t.Context(), resolver, parsed, defaultMaxResponseHeader, 64)
		if err == nil {
			t.Fatalf("resolveRequestWith() = %#v, nil", resolved)
		}
		if secret == nil || len(secret.Bytes()) != 0 {
			t.Fatal("oversized body secret was not forgotten")
		}
		if len(resolved.Body) != 0 {
			t.Fatal("oversized encoded body was returned")
		}
	})

	t.Run("configured header bound", func(t *testing.T) {
		var secret *values.ResolvedSecret
		resolver := values.SecretResolverFunc(func(context.Context, values.SecretRef) (*values.ResolvedSecret, error) {
			var err error
			secret, err = values.NewResolvedSecret(ref, []byte(strings.Repeat("h", 1100)))
			return secret, err
		})
		called := false
		kind := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) { called = true; return nil, nil }), Options{Secrets: resolver, MaxHeaderBytes: 1024})
		config := graph.Config{"url": "https://example.test/", "headers": map[string]any{"X-Secret": string(ref)}}
		_, err := kind.Execute(t.Context(), invocation(config))
		assertCode(t, err, "http_secret_resolution", stepkind.RetryPermanent)
		if called {
			t.Fatal("transport called with oversized resolved header")
		}
		if secret == nil || len(secret.Bytes()) != 0 {
			t.Fatal("oversized header secret was not forgotten")
		}
	})
}

func TestSchemaValidatesActualDataBeforeSecretMasking(t *testing.T) {
	ref, _ := values.ParseSecretRef("secret://project/schema#token")
	resolver := values.SecretResolverFunc(func(context.Context, values.SecretRef) (*values.ResolvedSecret, error) {
		return values.NewResolvedSecret(ref, []byte("exact-secret"))
	})
	kind := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
		return response(200, "application/json", `{"value":"exact-secret"}`), nil
	}), Options{Secrets: resolver})
	config := graph.Config{
		"url": "https://example.test/", "headers": map[string]any{"X-Secret": string(ref)},
		"expected_json_schema": map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"const": "exact-secret"}}},
	}
	result, err := kind.Execute(t.Context(), invocation(config))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result.Outputs[OutputBodyJSON].Inline.(map[string]any)["value"] != values.RedactedMarker {
		t.Fatalf("output = %#v", result.Outputs)
	}
}

func TestRedirectSecurityAndMethodPolicy(t *testing.T) {
	ref, _ := values.ParseSecretRef("secret://project/http#token")
	secretResolver := values.SecretResolverFunc(func(context.Context, values.SecretRef) (*values.ResolvedSecret, error) {
		return values.NewResolvedSecret(ref, []byte("reflected-secret"))
	})
	for _, location := range []string{
		"https://example.test/next?value=reflected-secret",
		"https://other.test/next?value=reflected-secret",
	} {
		t.Run(location, func(t *testing.T) {
			calls := 0
			kind := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
				calls++
				result := response(302, "text/plain", "")
				result.Header.Set("Location", location)
				return result, nil
			}), Options{Secrets: secretResolver})
			config := graph.Config{"url": "https://example.test/", "headers": map[string]any{"X-Secret": string(ref)}, "redirects": map[string]any{"mode": "policy"}}
			_, err := kind.Execute(t.Context(), invocation(config))
			assertCode(t, err, "http_redirect_invalid", stepkind.RetryPermanent)
			if calls != 1 {
				t.Fatalf("calls = %d", calls)
			}
		})
	}

	t.Run("percent encoded reflected secret is rejected", func(t *testing.T) {
		encodedRef, _ := values.ParseSecretRef("secret://project/http#encoded")
		resolver := values.SecretResolverFunc(func(context.Context, values.SecretRef) (*values.ResolvedSecret, error) {
			return values.NewResolvedSecret(encodedRef, []byte("a/b?c"))
		})
		calls := 0
		kind := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
			calls++
			result := response(302, "text/plain", "")
			result.Header.Set("Location", "https://example.test/next?value=a%2Fb%3Fc")
			return result, nil
		}), Options{Secrets: resolver})
		config := graph.Config{"url": "https://example.test/", "headers": map[string]any{"X-Secret": string(encodedRef)}, "redirects": map[string]any{"mode": "same_origin"}}
		_, err := kind.Execute(t.Context(), invocation(config))
		assertCode(t, err, "http_redirect_invalid", stepkind.RetryPermanent)
		if calls != 1 {
			t.Fatalf("calls = %d", calls)
		}
	})

	t.Run("cross origin strips credentials", func(t *testing.T) {
		calls := 0
		kind := newFakeKind(t, transportFunc(func(_ context.Context, exchange Exchange) (*nethttp.Response, error) {
			calls++
			if calls == 1 {
				if exchange.Request.Header.Get("Authorization") == "" {
					t.Fatal("initial auth missing")
				}
				result := response(307, "text/plain", "")
				result.Header.Set("Location", "https://other.test/next")
				return result, nil
			}
			if exchange.Request.Header.Get("Authorization") != "" {
				t.Fatal("credential crossed origin")
			}
			return response(200, "text/plain", "ok"), nil
		}), Options{Secrets: secretResolver})
		config := graph.Config{"url": "https://example.test/", "auth": map[string]any{"type": "bearer", "secret_ref": string(ref)}, "redirects": map[string]any{"mode": "policy"}}
		if _, err := kind.Execute(t.Context(), invocation(config)); err != nil {
			t.Fatal(err)
		}
		if calls != 2 {
			t.Fatalf("calls = %d", calls)
		}
	})

	t.Run("secret body cannot cross origin", func(t *testing.T) {
		calls := 0
		kind := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
			calls++
			result := response(307, "text/plain", "")
			result.Header.Set("Location", "https://other.test/next")
			return result, nil
		}), Options{Secrets: secretResolver})
		config := graph.Config{"url": "https://example.test/", "method": "POST", "body": map[string]any{"token": string(ref)}, "redirects": map[string]any{"mode": "policy"}}
		_, err := kind.Execute(t.Context(), invocation(config))
		assertCode(t, err, "http_redirect_secret_body", stepkind.RetryPermanent)
		if calls != 1 {
			t.Fatalf("calls = %d", calls)
		}
	})

	t.Run("method rewrite requires both declarations", func(t *testing.T) {
		policy := allowPolicy()
		policy.authorize = func(context.Context, DestinationRequest) (DestinationAuthorization, error) {
			return DestinationAuthorization{AllowMethodRewrite: false}, nil
		}
		calls := 0
		kind := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
			calls++
			result := response(302, "text/plain", "")
			result.Header.Set("Location", "https://example.test/next")
			return result, nil
		}), Options{Policy: policy})
		config := graph.Config{"url": "https://example.test/", "method": "POST", "body": map[string]any{"x": true}, "redirects": map[string]any{"mode": "same_origin", "allow_method_rewrite": true}}
		_, err := kind.Execute(t.Context(), invocation(config))
		assertCode(t, err, "http_redirect_method", stepkind.RetryPermanent)
		if calls != 1 {
			t.Fatalf("calls = %d", calls)
		}
	})

	t.Run("approved rewrite records final destination", func(t *testing.T) {
		calls := 0
		kind := newFakeKind(t, transportFunc(func(_ context.Context, exchange Exchange) (*nethttp.Response, error) {
			calls++
			if calls == 1 {
				result := response(302, "text/plain", "")
				result.Header.Set("Location", "https://other.test/final?signature=value")
				return result, nil
			}
			if exchange.Request.Method != "GET" {
				t.Fatalf("redirect method = %s", exchange.Request.Method)
			}
			body, _ := io.ReadAll(exchange.Request.Body)
			if len(body) != 0 {
				t.Fatalf("redirect body = %q", body)
			}
			return response(200, "application/json", `{"ok":true}`), nil
		}), Options{})
		config := graph.Config{"url": "https://example.test/start", "method": "POST", "body": map[string]any{"x": true}, "redirects": map[string]any{"mode": "policy", "allow_method_rewrite": true}}
		result, err := kind.Execute(t.Context(), invocation(config))
		if err != nil {
			t.Fatal(err)
		}
		metadata := result.Outputs[OutputMetadata].Inline.(map[string]any)
		if metadata["method"] != "GET" || metadata["origin"] != "https://other.test:443" || metadata["redirect_hops"].(json.Number).String() != "1" {
			t.Fatalf("metadata = %#v", metadata)
		}
	})

	t.Run("loop is rejected", func(t *testing.T) {
		kind := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
			result := response(307, "text/plain", "")
			result.Header.Set("Location", "https://example.test/start")
			return result, nil
		}), Options{})
		config := graph.Config{"url": "https://example.test/start", "redirects": map[string]any{"mode": "same_origin"}}
		_, err := kind.Execute(t.Context(), invocation(config))
		assertCode(t, err, "http_redirect_loop", stepkind.RetryPermanent)
	})
}

func TestArtifactsBoundsAndOversizedJSONDoesNotReinline(t *testing.T) {
	sink := &artifactSink{}
	kind := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
		return response(200, "application/json", `{"long":"abcdefghijklmnopqrstuvwxyz"}`), nil
	}), Options{Artifacts: sink})
	config := baseConfig("https://example.test/")
	config["inline_limit"] = json.Number("8")
	result, err := kind.Execute(t.Context(), invocation(config))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs[OutputBody].Type != values.TypeArtifact {
		t.Fatalf("body = %#v", result.Outputs[OutputBody])
	}
	if _, exists := result.Outputs[OutputBodyJSON]; exists {
		t.Fatal("oversized JSON was re-inlined")
	}
	if len(sink.requests) != 1 || string(sink.requests[0].Content) != `{"long":"abcdefghijklmnopqrstuvwxyz"}` {
		t.Fatalf("artifact requests = %#v", sink.requests)
	}

	bounded := baseConfig("https://example.test/")
	bounded["max_response_bytes"] = json.Number("4")
	bounded["inline_limit"] = json.Number("4")
	_, err = kind.Execute(t.Context(), invocation(bounded))
	assertCode(t, err, "http_invalid_response", stepkind.RetryPermanent)

	t.Run("binary response is artifact backed", func(t *testing.T) {
		binarySink := &artifactSink{}
		binary := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
			result := response(200, "application/octet-stream", "\x00\xff\x01")
			return result, nil
		}), Options{Artifacts: binarySink})
		result, err := binary.Execute(t.Context(), invocation(baseConfig("https://example.test/")))
		if err != nil {
			t.Fatal(err)
		}
		if result.Outputs[OutputBody].Type != values.TypeArtifact || len(binarySink.requests) != 1 {
			t.Fatalf("result = %#v, requests=%#v", result, binarySink.requests)
		}
	})

	t.Run("mismatched artifact is rejected", func(t *testing.T) {
		badSink := &artifactSink{bad: true}
		badKind := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
			return response(200, "application/octet-stream", "binary"), nil
		}), Options{Artifacts: badSink})
		_, err := badKind.Execute(t.Context(), invocation(baseConfig("https://example.test/")))
		assertCode(t, err, "http_invalid_response", stepkind.RetryPermanent)
	})

	t.Run("secret artifact URI is rejected", func(t *testing.T) {
		ref, _ := values.ParseSecretRef("secret://project/artifact#token")
		resolver := values.SecretResolverFunc(func(context.Context, values.SecretRef) (*values.ResolvedSecret, error) {
			return values.NewResolvedSecret(ref, []byte("signed-secret"))
		})
		leakingSink := &artifactSink{uri: "artifact://test/signed-secret"}
		leaking := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
			return response(200, "application/octet-stream", "binary"), nil
		}), Options{Artifacts: leakingSink, Secrets: resolver})
		config := graph.Config{"url": "https://example.test/", "headers": map[string]any{"X-Secret": string(ref)}}
		_, err := leaking.Execute(t.Context(), invocation(config))
		assertCode(t, err, "http_invalid_response", stepkind.RetryPermanent)
	})
}

func TestStatusTransportTimeoutAndCancellationClassification(t *testing.T) {
	for _, test := range []struct {
		name           string
		status         int
		classification stepkind.RetryClassification
	}{
		{"408", 408, stepkind.Retryable}, {"429", 429, stepkind.Retryable}, {"503", 503, stepkind.Retryable}, {"404", 404, stepkind.RetryPermanent},
	} {
		t.Run(test.name, func(t *testing.T) {
			kind := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
				return response(test.status, "text/plain", "diagnostic-secret-not-returned"), nil
			}), Options{})
			_, err := kind.Execute(t.Context(), invocation(baseConfig("https://example.test/")))
			assertCode(t, err, "http_unexpected_status", test.classification)
			if strings.Contains(err.Error(), "diagnostic-secret") {
				t.Fatal("status error contained response body")
			}
		})
	}
	for _, test := range []struct {
		failure        TransportFailure
		classification stepkind.RetryClassification
	}{
		{FailureDNS, stepkind.Retryable}, {FailureConnect, stepkind.Retryable}, {FailureTimeout, stepkind.Retryable},
		{FailureTLS, stepkind.RetryPermanent}, {FailureProtocol, stepkind.RetryPermanent}, {FailureCanceled, stepkind.RetryPermanent},
	} {
		t.Run(string(test.failure), func(t *testing.T) {
			kind := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
				return nil, &TransportError{Failure: test.failure}
			}), Options{})
			_, err := kind.Execute(t.Context(), invocation(baseConfig("https://example.test/")))
			assertCode(t, err, "http_transport", test.classification)
		})
	}
	t.Run("configured timeout", func(t *testing.T) {
		kind := newFakeKind(t, transportFunc(func(ctx context.Context, _ Exchange) (*nethttp.Response, error) { <-ctx.Done(); return nil, ctx.Err() }), Options{})
		config := baseConfig("https://example.test/")
		config["timeout"] = "1ms"
		_, err := kind.Execute(t.Context(), invocation(config))
		assertCode(t, err, "http_transport", stepkind.Retryable)
	})
	t.Run("resolver cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		kind := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
			t.Fatal("transport called")
			return nil, nil
		}), Options{})
		_, err := kind.Execute(ctx, invocation(baseConfig("https://example.test/")))
		if stepkind.ClassifyError(err) != stepkind.RetryPermanent {
			t.Fatalf("classification = %v, err=%v", stepkind.ClassifyError(err), err)
		}
	})
}

func TestTransportFailureIsNeverRetried(t *testing.T) {
	for _, keyed := range []bool{false, true} {
		calls := 0
		kind := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
			calls++
			return nil, &TransportError{Failure: FailureConnect}
		}), Options{})
		config := baseConfig("https://example.test/")
		if keyed {
			config["idempotency_key"] = "keyed-attempt"
		}
		_, err := kind.Execute(t.Context(), invocation(config))
		assertCode(t, err, "http_transport", stepkind.Retryable)
		if calls != 1 {
			t.Fatalf("keyed=%v transport calls=%d", keyed, calls)
		}
	}
}

func TestFullOperationDeadlineAndEffectiveIdempotencyPreflight(t *testing.T) {
	t.Run("runtime idempotency is policy visible but value is not", func(t *testing.T) {
		policy := allowPolicy()
		kind := newFakeKind(t, transportFunc(func(_ context.Context, exchange Exchange) (*nethttp.Response, error) {
			if exchange.Request.Header.Get("Idempotency-Key") != "runtime-key" {
				t.Fatalf("idempotency header = %q", exchange.Request.Header.Get("Idempotency-Key"))
			}
			return response(200, "text/plain", "ok"), nil
		}), Options{Policy: policy})
		call := invocation(baseConfig("https://example.test/"))
		call.Invocation.IdempotencyKey = "runtime-key"
		if _, err := kind.Execute(t.Context(), call); err != nil {
			t.Fatal(err)
		}
		if len(policy.declarations) != 1 || !policy.declarations[0].HasIdempotencyKey {
			t.Fatalf("declarations = %#v", policy.declarations)
		}
		encoded, _ := json.Marshal(policy.declarations[0])
		if bytes.Contains(encoded, []byte("runtime-key")) {
			t.Fatalf("policy declaration leaked key: %s", encoded)
		}
	})

	t.Run("configured timeout covers policy", func(t *testing.T) {
		policy := allowPolicy()
		policy.describe = func(ctx context.Context, _ RequestDeclaration) (PolicyDescription, error) {
			<-ctx.Done()
			return PolicyDescription{}, ctx.Err()
		}
		kind := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
			t.Fatal("transport called")
			return nil, nil
		}), Options{Policy: policy})
		config := baseConfig("https://example.test/")
		config["timeout"] = "1ms"
		_, err := kind.Execute(t.Context(), invocation(config))
		assertCode(t, err, "http_policy_description", stepkind.Retryable)
	})

	t.Run("successful collaborator return cannot bypass expiration", func(t *testing.T) {
		policy := allowPolicy()
		policy.describe = func(context.Context, RequestDeclaration) (PolicyDescription, error) {
			time.Sleep(5 * time.Millisecond)
			return PolicyDescription{}, nil
		}
		kind := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
			t.Fatal("transport called")
			return nil, nil
		}), Options{Policy: policy})
		config := baseConfig("https://example.test/")
		config["timeout"] = "1ms"
		_, err := kind.Execute(t.Context(), invocation(config))
		assertCode(t, err, "http_policy_description", stepkind.Retryable)
	})

	t.Run("successful transport return cannot bypass expiration", func(t *testing.T) {
		kind := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
			time.Sleep(5 * time.Millisecond)
			return response(200, "text/plain", "late"), nil
		}), Options{})
		config := baseConfig("https://example.test/")
		config["timeout"] = "1ms"
		_, err := kind.Execute(t.Context(), invocation(config))
		assertCode(t, err, "http_transport", stepkind.Retryable)
	})

	t.Run("configured timeout covers secret resolution", func(t *testing.T) {
		ref, _ := values.ParseSecretRef("secret://project/slow#token")
		resolver := values.SecretResolverFunc(func(ctx context.Context, _ values.SecretRef) (*values.ResolvedSecret, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})
		kind := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
			t.Fatal("transport called")
			return nil, nil
		}), Options{Secrets: resolver})
		config := graph.Config{"url": "https://example.test/", "headers": map[string]any{"X-Secret": string(ref)}, "timeout": "1ms"}
		_, err := kind.Execute(t.Context(), invocation(config))
		assertCode(t, err, "http_secret_resolution", stepkind.Retryable)
	})

	t.Run("invocation deadline is earlier", func(t *testing.T) {
		policy := allowPolicy()
		policy.describe = func(ctx context.Context, _ RequestDeclaration) (PolicyDescription, error) {
			<-ctx.Done()
			return PolicyDescription{}, ctx.Err()
		}
		kind := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
			t.Fatal("transport called")
			return nil, nil
		}), Options{Policy: policy})
		call := invocation(baseConfig("https://example.test/"))
		call.Invocation.Deadline = time.Now().Add(-time.Millisecond)
		_, err := kind.Execute(t.Context(), call)
		assertCode(t, err, "http_policy_description", stepkind.Retryable)
	})

	t.Run("resolver deadline and cancellation stay distinct", func(t *testing.T) {
		for _, test := range []struct {
			name           string
			err            error
			classification stepkind.RetryClassification
		}{
			{"deadline", context.DeadlineExceeded, stepkind.Retryable}, {"cancel", context.Canceled, stepkind.RetryPermanent},
		} {
			t.Run(test.name, func(t *testing.T) {
				resolver := &fakeResolver{err: test.err, addresses: map[string][]netip.Addr{}}
				kind := newFakeKind(t, transportFunc(func(context.Context, Exchange) (*nethttp.Response, error) {
					t.Fatal("transport called")
					return nil, nil
				}), Options{Resolver: resolver})
				_, err := kind.Execute(t.Context(), invocation(baseConfig("https://example.test/")))
				assertCode(t, err, "http_destination", test.classification)
			})
		}
	})

	t.Run("pinned boundary deadline and cancellation stay distinct", func(t *testing.T) {
		pinned, err := NewPinnedTransport(PinnedTransportOptions{})
		if err != nil {
			t.Fatal(err)
		}
		request, _ := nethttp.NewRequest("GET", "http://example.test:80/", nil)
		request.Host = request.URL.Host
		destination := DestinationRequest{Scheme: "http", Host: "example.test", Port: 80, Address: netip.MustParseAddr("192.0.2.1"), Path: "/", Method: "GET"}
		deadlineCtx, deadlineCancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Millisecond))
		defer deadlineCancel()
		_, err = pinned.RoundTrip(deadlineCtx, Exchange{Request: request, Destination: destination})
		var transportErr *TransportError
		if !errors.As(err, &transportErr) || transportErr.Failure != FailureTimeout {
			t.Fatalf("deadline error = %#v", err)
		}
		cancelCtx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = pinned.RoundTrip(cancelCtx, Exchange{Request: request, Destination: destination})
		if !errors.As(err, &transportErr) || transportErr.Failure != FailureCanceled {
			t.Fatalf("cancel error = %#v", err)
		}
	})
}

func TestAllDNSAnswersAuthorizedPinnedAndConcurrent(t *testing.T) {
	resolver := &fakeResolver{addresses: map[string][]netip.Addr{"example.test": {
		netip.MustParseAddr("192.0.2.2"), netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("192.0.2.2"),
	}}}
	policy := allowPolicy()
	var mu sync.Mutex
	var selected []netip.Addr
	transport := transportFunc(func(_ context.Context, exchange Exchange) (*nethttp.Response, error) {
		mu.Lock()
		selected = append(selected, exchange.Destination.Address)
		mu.Unlock()
		return response(200, "text/plain", "ok"), nil
	})
	kind := newFakeKind(t, transport, Options{Resolver: resolver, Policy: policy})
	const workers = 24
	var wait sync.WaitGroup
	errorsSeen := make(chan error, workers)
	for index := 0; index < workers; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := kind.Execute(t.Context(), invocation(baseConfig("https://example.test/")))
			errorsSeen <- err
		}()
	}
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(policy.requests) != workers*2 {
		t.Fatalf("authorization requests = %d", len(policy.requests))
	}
	for _, address := range selected {
		if address != netip.MustParseAddr("192.0.2.1") {
			t.Fatalf("selected = %v", selected)
		}
	}
}

func assertCode(t *testing.T, err error, code string, classification stepkind.RetryClassification) {
	t.Helper()
	var execution *stepkind.ExecutionError
	if !errors.As(err, &execution) || execution.Code != code || execution.Classification != classification {
		t.Fatalf("error = %#v, want code=%s classification=%s", err, code, classification)
	}
	encoded, marshalErr := json.Marshal(execution)
	if marshalErr != nil || bytes.Contains(encoded, []byte("secret://")) {
		t.Fatalf("unsafe error = %s, %v", encoded, marshalErr)
	}
}
