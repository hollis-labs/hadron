package mcp_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	mcpadapter "github.com/hollis-labs/hadron/workflow/adapters/mcp"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
)

type fakeClient struct {
	mu      sync.Mutex
	result  mcpadapter.CallResult
	err     error
	call    func(context.Context, mcpadapter.CallRequest) (mcpadapter.CallResult, error)
	calls   int
	request mcpadapter.CallRequest
}

func (f *fakeClient) ExecuteTool(ctx context.Context, request mcpadapter.CallRequest) (mcpadapter.CallResult, error) {
	f.mu.Lock()
	f.calls++
	f.request = request
	call := f.call
	result, err := f.result, f.err
	f.mu.Unlock()
	if call != nil {
		return call(ctx, request)
	}
	return result, err
}

type fakeDescriptor struct {
	mu         sync.Mutex
	descriptor mcpadapter.ToolDescriptor
	err        error
	calls      int
}

func (f *fakeDescriptor) DescribeTool(_ context.Context, server, tool string) (mcpadapter.ToolDescriptor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	result := f.descriptor
	if result.Server == "" {
		result.Server = server
	}
	if result.Tool == "" {
		result.Tool = tool
	}
	return result, f.err
}

type fakeArtifacts struct {
	mu       sync.Mutex
	requests []mcpadapter.ArtifactRequest
	bad      bool
}

func (f *fakeArtifacts) CaptureArtifact(_ context.Context, request mcpadapter.ArtifactRequest) (values.Value, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	request.Content = append([]byte(nil), request.Content...)
	f.requests = append(f.requests, request)
	metadata := request.Metadata
	if f.bad {
		metadata.Producer.Output = "wrong"
	}
	return values.NewArtifact(values.ArtifactRef{
		Store: "fixture", URI: "artifact://fixture/" + request.Name,
		Digest: values.SHA256Digest(request.Content), MediaType: metadata.MediaType,
		SizeBytes: int64(len(request.Content)), Producer: metadata.Producer,
		Redaction: metadata.Redaction, Retention: metadata.Retention,
	})
}

func TestRegistrationSpecAndConfigValidation(t *testing.T) {
	client := &fakeClient{}
	descriptor := &fakeDescriptor{}
	registry := stepkind.NewRegistry()
	kind, err := mcpadapter.Register(registry, mcpadapter.Options{Client: client, Descriptor: descriptor})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	registered, ok := registry.Lookup(mcpadapter.KindName, mcpadapter.KindVersion)
	if !ok || registered != kind {
		t.Fatalf("registered kind = %T, %v", registered, ok)
	}
	spec := kind.Spec()
	if spec.Idempotency != graph.IdempotencyKeyed || spec.RetrySafety != stepkind.RetryRequiresIdempotency ||
		spec.Cancellation.Mode != stepkind.CancellationContext || !reflect.DeepEqual(spec.Effects, graph.EffectSet{
		graph.EffectRead, graph.EffectMaterialize, graph.EffectMutate, graph.EffectDestructive,
	}) {
		t.Fatalf("Spec() = %#v", spec)
	}

	valid := config(map[string]any{"value": json.Number("9007199254740993")})
	if diagnostics := kind.ValidateConfig(t.Context(), valid); len(diagnostics) != 0 {
		t.Fatalf("valid config diagnostics = %#v", diagnostics)
	}
	tests := []struct {
		name   string
		mutate func(graph.Config)
	}{
		{"missing server", func(c graph.Config) { delete(c, "server") }},
		{"bad tool", func(c graph.Config) { c["tool"] = "bad.tool" }},
		{"arguments null", func(c graph.Config) { c["arguments"] = nil }},
		{"bad timeout", func(c graph.Config) { c["timeout"] = "0s" }},
		{"bad idempotency", func(c graph.Config) { c["idempotency_key"] = " secret " }},
		{"bad expected", func(c graph.Config) { c["expected_result"] = "xml" }},
		{"unknown field", func(c graph.Config) { c["endpoint"] = "hidden" }},
		{"malformed secret", func(c graph.Config) { c["arguments"] = map[string]any{"token": "secret://Project/key"} }},
		{"invalid utf8", func(c graph.Config) { c["arguments"] = map[string]any{"value": string([]byte{0xff})} }},
		{"unsupported value", func(c graph.Config) { c["arguments"] = map[string]any{"value": make(chan int)} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := config(map[string]any{})
			test.mutate(candidate)
			diagnostics := kind.ValidateConfig(t.Context(), candidate)
			if len(diagnostics) != 1 || diagnostics[0].Code != stepkind.CodeInvalidConfig {
				t.Fatalf("diagnostics = %#v", diagnostics)
			}
		})
	}
}

func TestPrepareInterpolatesArgumentsFromBoundInputsWithoutMutatingInvocation(t *testing.T) {
	client := &fakeClient{call: func(_ context.Context, request mcpadapter.CallRequest) (mcpadapter.CallResult, error) {
		want := map[string]any{
			"project": "project-9", "literal": "inputs.title", "closing": "}}",
			"nested": []any{"Task one", map[string]any{"description": "desc Task one"}},
		}
		if !reflect.DeepEqual(request.Arguments, want) {
			t.Fatalf("prepared arguments = %#v, want %#v", request.Arguments, want)
		}
		return mcpadapter.CallResult{HasStructured: true, Structured: map[string]any{"id": "task-1"}}, nil
	}}
	kind := mustKind(t, client, &fakeDescriptor{}, nil, 0, 0)
	inputMetadata := values.Metadata{Producer: values.Producer{Kind: "node-input", Reference: "run/create/item-1"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun}
	project, err := values.NewInline("project-9", inputMetadata)
	if err != nil {
		t.Fatal(err)
	}
	title, err := values.NewInline("Task one", inputMetadata)
	if err != nil {
		t.Fatal(err)
	}
	arguments := map[string]any{
		"project": "{{ inputs['project-id'] }}", "literal": "inputs.title", "closing": "}}",
		"nested": []any{"{{ inputs.title }}", map[string]any{"description": "desc {{ inputs.title }}"}},
	}
	configuration := config(arguments)
	invocation := stepkind.Invocation{
		Identity: stepkind.InvocationIdentity{RunID: "run", NodeID: "create", Iteration: "item-1", Attempt: 2},
		Config:   configuration, Inputs: values.ValueSet{"project-id": project, "title": title}, IdempotencyKey: "logical-create-1",
	}
	before, err := json.Marshal(invocation)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := kind.Prepare(t.Context(), invocation)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	after, err := json.Marshal(prepared.Invocation)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("Prepare mutated invocation: before=%s after=%s err=%v", before, after, err)
	}
	if _, err := kind.Execute(t.Context(), prepared); err != nil {
		t.Fatalf("Execute(prepared) error = %v", err)
	}
	if client.request.IdempotencyKey != invocation.IdempotencyKey {
		t.Fatalf("idempotency key = %q", client.request.IdempotencyKey)
	}
	if arguments["project"] != "{{ inputs['project-id'] }}" || arguments["nested"].([]any)[0] != "{{ inputs.title }}" {
		t.Fatalf("plan arguments mutated = %#v", arguments)
	}
}

func TestPrepareRejectsUnsafeOrMalformedArgumentInterpolation(t *testing.T) {
	kind := mustKind(t, &fakeClient{}, &fakeDescriptor{}, nil, 0, 0)
	secretRef, err := values.ParseSecretRef("secret://project/mcp#token")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := values.NewSecretRef(secretRef, values.Metadata{Producer: values.Producer{Kind: "test", Reference: "run"}, MediaType: "text/plain", Redaction: values.RedactionSecret, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		text   string
		inputs values.ValueSet
	}{
		{name: "secret derivation", text: "Bearer {{ inputs.token }}", inputs: values.ValueSet{"token": secret}},
		{name: "malformed", text: "{{ inputs.title", inputs: values.ValueSet{}},
		{name: "non-input root", text: "{{ run.id }}", inputs: values.ValueSet{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			invocation := stepkind.Invocation{Identity: stepkind.InvocationIdentity{RunID: "run", NodeID: "node", Attempt: 1}, Config: config(map[string]any{"value": test.text}), Inputs: test.inputs}
			_, prepareErr := kind.Prepare(t.Context(), invocation)
			if prepareErr == nil || stepkind.ClassifyError(prepareErr) != stepkind.RetryPermanent {
				t.Fatalf("Prepare error = %v", prepareErr)
			}
			if strings.Contains(prepareErr.Error(), string(secretRef)) {
				t.Fatalf("Prepare error leaked secret reference: %v", prepareErr)
			}
		})
	}
}

func TestDescribeConfigTrustedAndFailClosedAnnotationMapping(t *testing.T) {
	trueValue, falseValue := true, false
	tests := []struct {
		name        string
		trusted     bool
		annotations mcpadapter.ToolAnnotations
		idempotency string
		effects     graph.EffectSet
		mode        graph.IdempotencyMode
		retry       stepkind.RetrySafety
	}{
		{
			name: "trusted read and idempotent", trusted: true,
			annotations: mcpadapter.ToolAnnotations{ReadOnlyHint: &trueValue, DestructiveHint: &falseValue, IdempotentHint: &trueValue},
			effects:     graph.EffectSet{graph.EffectRead}, mode: graph.IdempotencyIntrinsic, retry: stepkind.RetrySafe,
		},
		{
			name: "trusted destructive", trusted: true,
			annotations: mcpadapter.ToolAnnotations{ReadOnlyHint: &falseValue, DestructiveHint: &trueValue},
			effects:     graph.EffectSet{graph.EffectDestructive}, mode: graph.IdempotencyNone, retry: stepkind.RetryUnsupported,
		},
		{
			name: "trusted mutating keyed", trusted: true, idempotency: "key-1",
			annotations: mcpadapter.ToolAnnotations{ReadOnlyHint: &falseValue, DestructiveHint: &falseValue},
			effects:     graph.EffectSet{graph.EffectMutate}, mode: graph.IdempotencyKeyed, retry: stepkind.RetryRequiresIdempotency,
		},
		{
			name: "untrusted hints stay conservative", trusted: false,
			annotations: mcpadapter.ToolAnnotations{ReadOnlyHint: &trueValue, DestructiveHint: &falseValue, IdempotentHint: &trueValue},
			effects:     conservative(), mode: graph.IdempotencyNone, retry: stepkind.RetryUnsupported,
		},
		{
			name: "partial trusted hints stay conservative", trusted: true,
			annotations: mcpadapter.ToolAnnotations{ReadOnlyHint: &trueValue},
			effects:     conservative(), mode: graph.IdempotencyNone, retry: stepkind.RetryUnsupported,
		},
		{
			name: "conflicting trusted hints stay conservative", trusted: true,
			annotations: mcpadapter.ToolAnnotations{ReadOnlyHint: &trueValue, DestructiveHint: &trueValue},
			effects:     conservative(), mode: graph.IdempotencyNone, retry: stepkind.RetryUnsupported,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			descriptor := &fakeDescriptor{descriptor: mcpadapter.ToolDescriptor{
				Server: "fixture", Tool: "echo", Trusted: test.trusted, Annotations: test.annotations,
			}}
			kind := mustKind(t, &fakeClient{}, descriptor, nil, 0, 0)
			input := config(map[string]any{})
			if test.idempotency != "" {
				input["idempotency_key"] = test.idempotency
			}
			description, err := kind.DescribeConfig(t.Context(), input)
			if err != nil {
				t.Fatalf("DescribeConfig() error = %v", err)
			}
			if !reflect.DeepEqual(description.Effects, test.effects) || description.Idempotency != test.mode || description.RetrySafety != test.retry {
				t.Fatalf("description = %#v", description)
			}
			// Mutating the result cannot modify a later descriptor projection.
			description.Effects[0] = graph.EffectCompute
			if description.Annotations.ReadOnlyHint != nil {
				*description.Annotations.ReadOnlyHint = !*description.Annotations.ReadOnlyHint
			}
			again, err := kind.DescribeConfig(t.Context(), input)
			if err != nil || !reflect.DeepEqual(again.Effects, test.effects) {
				t.Fatalf("second description = %#v, %v", again, err)
			}
		})
	}
}

func TestExecuteMapsTypedContentAndMasksResolvedSecrets(t *testing.T) {
	const secretMaterial = "super-secret-token"
	secretRef, _ := values.ParseSecretRef("secret://project/api#token")
	resolver := values.SecretResolverFunc(func(_ context.Context, ref values.SecretRef) (*values.ResolvedSecret, error) {
		if ref != secretRef {
			t.Fatalf("ResolveSecret ref = %q", ref)
		}
		return values.NewResolvedSecret(ref, []byte(secretMaterial))
	})
	trueValue, falseValue := true, false
	descriptor := &fakeDescriptor{descriptor: mcpadapter.ToolDescriptor{
		Trusted: true,
		Annotations: mcpadapter.ToolAnnotations{
			Title: "uses " + secretMaterial, ReadOnlyHint: &trueValue,
			DestructiveHint: &falseValue, IdempotentHint: &trueValue,
		},
	}}
	client := &fakeClient{call: func(_ context.Context, request mcpadapter.CallRequest) (mcpadapter.CallResult, error) {
		if request.Arguments["token"] != secretMaterial || request.Arguments["nested"].(map[string]any)["token"] != secretMaterial {
			t.Fatalf("client arguments = %#v", request.Arguments)
		}
		return mcpadapter.CallResult{
			HasStructured: true,
			Structured: map[string]any{
				"message":               "value " + secretMaterial,
				secretMaterial + "-key": "safe",
			},
			Content: []mcpadapter.Content{
				{Kind: mcpadapter.ContentText, Text: "text " + secretMaterial},
				{Kind: mcpadapter.ContentResourceLink, URI: "https://resource.invalid/" + secretMaterial, Name: secretMaterial, Description: "signed " + secretMaterial, MediaType: "text/plain"},
				{Kind: mcpadapter.ContentImage, Data: []byte("image-" + secretMaterial), MediaType: "image/png"},
				{Kind: mcpadapter.ContentResourceBlob, URI: "resource://" + secretMaterial, Data: []byte("blob-" + secretMaterial), MediaType: "application/octet-stream"},
			},
			Transport: mcpadapter.TransportMetadata{
				Transport: "stdio", AttemptCount: 2, RetryCount: 1, Reconnected: true,
				Attributes: map[string]string{"route": "through-" + secretMaterial, secretMaterial: "masked-key"},
			},
		}, nil
	}}
	artifacts := &fakeArtifacts{}
	kind, err := mcpadapter.New(mcpadapter.Options{
		Client: client, Descriptor: descriptor, Secrets: resolver, Artifacts: artifacts,
		InlineLimit: values.DefaultInlineLimit, MaxArtifacts: 8,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	input := config(map[string]any{
		"token":  string(secretRef),
		"nested": map[string]any{"token": string(secretRef)},
	})
	input["idempotency_key"] = "static-key"
	result, err := kind.Execute(t.Context(), invocation(input))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result.Outcome != stepkind.StepCompleted || len(result.Outputs) != 6 {
		t.Fatalf("result = %#v", result)
	}
	if validationErr := values.ValidatePersistableSet(result.Outputs); validationErr != nil {
		t.Fatalf("outputs not persistable: %v", validationErr)
	}
	if schemaErr := values.ValidateValueSetSchema(kind.Spec().OutputSchema, result.Outputs); schemaErr != nil {
		t.Fatalf("runtime output schema rejected outputs: %v", schemaErr)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	if strings.Contains(string(encoded), secretMaterial) || strings.Contains(string(encoded), string(secretRef)) {
		t.Fatalf("persisted result contains secret: %s", encoded)
	}
	if got := result.Outputs[mcpadapter.OutputStructured].Inline.(map[string]any)["message"]; got != "value "+values.RedactedMarker {
		t.Fatalf("structured message = %#v", got)
	}
	if _, ok := result.Outputs[mcpadapter.OutputStructured].Inline.(map[string]any)[values.RedactedMarker+"-key"]; !ok {
		t.Fatalf("structured keys = %#v", result.Outputs[mcpadapter.OutputStructured].Inline)
	}
	if len(artifacts.requests) != 2 {
		t.Fatalf("artifact requests = %d", len(artifacts.requests))
	}
	for _, request := range artifacts.requests {
		if strings.Contains(string(request.Content), secretMaterial) {
			t.Fatalf("artifact %q contains secret: %q", request.Name, request.Content)
		}
		if request.Metadata.Redaction != values.RedactionPrivate || request.Metadata.Retention != values.RetentionRun {
			t.Fatalf("artifact metadata = %#v", request.Metadata)
		}
	}
	if input["arguments"].(map[string]any)["token"] != string(secretRef) {
		t.Fatalf("invocation config was mutated: %#v", input)
	}
	if descriptor.calls != 1 || client.calls != 1 {
		t.Fatalf("descriptor/client calls = %d/%d", descriptor.calls, client.calls)
	}
}

func TestLargeTextAndResourceTextPromoteToArtifacts(t *testing.T) {
	client := &fakeClient{result: mcpadapter.CallResult{Content: []mcpadapter.Content{
		{Kind: mcpadapter.ContentText, Text: strings.Repeat("x", 64)},
		{Kind: mcpadapter.ContentResourceText, URI: "resource://fixture/report", Text: strings.Repeat("y", 64), MediaType: "text/plain"},
	}}}
	artifacts := &fakeArtifacts{}
	kind := mustKind(t, client, &fakeDescriptor{}, artifacts, 16, 4)
	result, err := kind.Execute(t.Context(), invocation(config(map[string]any{})))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result.Outputs[mcpadapter.OutputText].Type != values.TypeArtifact || result.Outputs["resource_001"].Type != values.TypeArtifact || len(artifacts.requests) != 2 {
		t.Fatalf("outputs/artifacts = %#v / %d", result.Outputs, len(artifacts.requests))
	}
}

func TestToolAndTransportFailuresAreClassifiedAndPersistSafe(t *testing.T) {
	const secret = "transport-secret"
	ref, _ := values.ParseSecretRef("secret://project/transport#token")
	resolver := values.SecretResolverFunc(func(context.Context, values.SecretRef) (*values.ResolvedSecret, error) {
		return values.NewResolvedSecret(ref, []byte(secret))
	})
	tests := []struct {
		name           string
		client         *fakeClient
		wantCode       string
		classification stepkind.RetryClassification
	}{
		{
			name: "tool declared error", client: &fakeClient{result: mcpadapter.CallResult{
				IsError: true, Content: []mcpadapter.Content{{Kind: mcpadapter.ContentText, Text: secret}},
			}}, wantCode: "mcp_tool_error", classification: stepkind.RetryPermanent,
		},
		{
			name: "retryable transport", client: &fakeClient{err: &mcpadapter.TransportError{
				Retryable: true, Cause: errors.New("connection failed with " + secret),
			}}, wantCode: "mcp_transport_error", classification: stepkind.Retryable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			kind, err := mcpadapter.New(mcpadapter.Options{Client: test.client, Descriptor: &fakeDescriptor{}, Secrets: resolver})
			if err != nil {
				t.Fatal(err)
			}
			input := config(map[string]any{"token": string(ref)})
			_, err = kind.Execute(t.Context(), invocation(input))
			var executionErr *stepkind.ExecutionError
			if !errors.As(err, &executionErr) || executionErr.Code != test.wantCode || executionErr.Classification != test.classification {
				t.Fatalf("error = %#v", err)
			}
			encoded, marshalErr := json.Marshal(executionErr)
			if marshalErr != nil || strings.Contains(string(encoded), secret) || strings.Contains(executionErr.Error(), secret) {
				t.Fatalf("persisted error = %s, %v / Error=%q", encoded, marshalErr, executionErr.Error())
			}
		})
	}
}

func TestTimeoutCancellationMalformedAndUnexpectedResults(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		client := &fakeClient{call: func(ctx context.Context, _ mcpadapter.CallRequest) (mcpadapter.CallResult, error) {
			<-ctx.Done()
			return mcpadapter.CallResult{}, ctx.Err()
		}}
		kind := mustKind(t, client, &fakeDescriptor{}, nil, 0, 0)
		input := config(map[string]any{})
		input["timeout"] = "1ms"
		_, err := kind.Execute(t.Context(), invocation(input))
		if stepkind.ClassifyError(err) != stepkind.Retryable {
			t.Fatalf("timeout classification = %q, %v", stepkind.ClassifyError(err), err)
		}
	})

	t.Run("canceled", func(t *testing.T) {
		client := &fakeClient{}
		kind := mustKind(t, client, &fakeDescriptor{}, nil, 0, 0)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := kind.Execute(ctx, invocation(config(map[string]any{})))
		if stepkind.ClassifyError(err) != stepkind.RetryPermanent || client.calls != 0 {
			t.Fatalf("canceled error/calls = %v / %d", err, client.calls)
		}
	})

	t.Run("malformed kind", func(t *testing.T) {
		client := &fakeClient{result: mcpadapter.CallResult{Content: []mcpadapter.Content{{Kind: "future"}}}}
		kind := mustKind(t, client, &fakeDescriptor{}, nil, 0, 0)
		_, err := kind.Execute(t.Context(), invocation(config(map[string]any{})))
		assertExecutionCode(t, err, "mcp_invalid_result")
	})

	t.Run("unexpected shape", func(t *testing.T) {
		client := &fakeClient{result: mcpadapter.CallResult{Content: []mcpadapter.Content{{Kind: mcpadapter.ContentText, Text: "ok"}}}}
		kind := mustKind(t, client, &fakeDescriptor{}, nil, 0, 0)
		input := config(map[string]any{})
		input["expected_result"] = "structured"
		_, err := kind.Execute(t.Context(), invocation(input))
		assertExecutionCode(t, err, "mcp_unexpected_result")
	})
}

func TestArtifactLimitAndSinkValidationFailBeforePersistence(t *testing.T) {
	t.Run("limit", func(t *testing.T) {
		client := &fakeClient{result: mcpadapter.CallResult{Content: []mcpadapter.Content{
			{Kind: mcpadapter.ContentImage, Data: []byte("one"), MediaType: "image/png"},
			{Kind: mcpadapter.ContentAudio, Data: []byte("two"), MediaType: "audio/mpeg"},
		}}}
		artifacts := &fakeArtifacts{}
		kind := mustKind(t, client, &fakeDescriptor{}, artifacts, 0, 1)
		_, err := kind.Execute(t.Context(), invocation(config(map[string]any{})))
		assertExecutionCode(t, err, "mcp_invalid_result")
		if len(artifacts.requests) != 1 {
			t.Fatalf("artifact writes = %d, want 1", len(artifacts.requests))
		}
	})

	t.Run("mismatched sink metadata", func(t *testing.T) {
		client := &fakeClient{result: mcpadapter.CallResult{Content: []mcpadapter.Content{{Kind: mcpadapter.ContentImage, Data: []byte("one"), MediaType: "image/png"}}}}
		artifacts := &fakeArtifacts{bad: true}
		kind := mustKind(t, client, &fakeDescriptor{}, artifacts, 0, 1)
		_, err := kind.Execute(t.Context(), invocation(config(map[string]any{})))
		assertExecutionCode(t, err, "mcp_invalid_result")
	})
}

func TestResultAndDescriptionAreDefensiveCopies(t *testing.T) {
	structured := map[string]any{"nested": map[string]any{"value": "original"}}
	contentData := []byte("binary")
	client := &fakeClient{result: mcpadapter.CallResult{
		HasStructured: true, Structured: structured,
		Content: []mcpadapter.Content{{Kind: mcpadapter.ContentImage, Data: contentData, MediaType: "image/png"}},
	}}
	artifacts := &fakeArtifacts{}
	kind := mustKind(t, client, &fakeDescriptor{}, artifacts, 0, 4)
	result, err := kind.Execute(t.Context(), invocation(config(map[string]any{})))
	if err != nil {
		t.Fatal(err)
	}
	result.Outputs[mcpadapter.OutputStructured].Inline.(map[string]any)["nested"].(map[string]any)["value"] = "mutated"
	artifacts.requests[0].Content[0] = 'X'
	if structured["nested"].(map[string]any)["value"] != "original" || string(contentData) != "binary" {
		t.Fatalf("source result mutated: %#v / %q", structured, contentData)
	}
}

func TestResultRedactionCollisionAndTransportCollisionFailClosed(t *testing.T) {
	const secret = "collision-secret"
	ref, _ := values.ParseSecretRef("secret://project/collision#token")
	resolver := values.SecretResolverFunc(func(context.Context, values.SecretRef) (*values.ResolvedSecret, error) {
		return values.NewResolvedSecret(ref, []byte(secret))
	})
	tests := []mcpadapter.CallResult{
		{HasStructured: true, Structured: map[string]any{secret: true, values.RedactedMarker: false}},
		{Transport: mcpadapter.TransportMetadata{Attributes: map[string]string{secret: "one", values.RedactedMarker: "two"}}},
	}
	for index, result := range tests {
		t.Run(fmt.Sprintf("case-%d", index), func(t *testing.T) {
			kind, err := mcpadapter.New(mcpadapter.Options{Client: &fakeClient{result: result}, Descriptor: &fakeDescriptor{}, Secrets: resolver})
			if err != nil {
				t.Fatal(err)
			}
			_, err = kind.Execute(t.Context(), invocation(config(map[string]any{"token": string(ref)})))
			assertExecutionCode(t, err, "mcp_invalid_result")
		})
	}
}

func TestExecuteRecordsOnlyLiteralToolBoundaryEvidence(t *testing.T) {
	tests := []struct {
		name    string
		client  *fakeClient
		outcome verification.ActivityOutcome
	}{
		{name: "success", client: &fakeClient{result: mcpadapter.CallResult{Content: []mcpadapter.Content{{Kind: mcpadapter.ContentText, Text: "model claims another tool ran"}}}}, outcome: verification.ActivitySucceeded},
		{name: "tool error", client: &fakeClient{result: mcpadapter.CallResult{IsError: true, Content: []mcpadapter.Content{{Kind: mcpadapter.ContentText, Text: "failure"}}}}, outcome: verification.ActivityFailed},
		{name: "transport error", client: &fakeClient{err: errors.New("unavailable")}, outcome: verification.ActivityFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			kind := mustKind(t, test.client, &fakeDescriptor{}, nil, 0, 0)
			recorder := verification.NewActivityRecorder()
			prepared := invocation(config(map[string]any{}))
			prepared.Invocation.Activity = recorder
			_, _ = kind.Execute(t.Context(), prepared)
			activity, err := recorder.Freeze()
			if err != nil || len(activity) != 1 || activity[0].ToolCall == nil ||
				activity[0].ToolCall.Server != "fixture" || activity[0].ToolCall.Tool != "echo" || activity[0].ToolCall.Outcome != test.outcome {
				t.Fatalf("Freeze() = %#v, %v", activity, err)
			}
			encoded, err := json.Marshal(activity)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), "model claims") || strings.Contains(string(encoded), "unavailable") {
				t.Fatalf("evidence contains executor/model data: %s", encoded)
			}
		})
	}
}

func config(arguments map[string]any) graph.Config {
	return graph.Config{"server": "fixture", "tool": "echo", "arguments": arguments}
}

func invocation(input graph.Config) stepkind.PreparedInvocation {
	return stepkind.PreparedInvocation{Invocation: stepkind.Invocation{
		Identity: stepkind.InvocationIdentity{RunID: "run-1", NodeID: "mcp-node", Attempt: 1},
		Config:   input, Inputs: values.ValueSet{},
	}}
}

func conservative() graph.EffectSet {
	return graph.EffectSet{graph.EffectRead, graph.EffectMaterialize, graph.EffectMutate, graph.EffectDestructive}
}

func mustKind(t *testing.T, client mcpadapter.Client, descriptor mcpadapter.Descriptor, artifacts mcpadapter.ArtifactSink, inlineLimit int64, maxArtifacts int) *mcpadapter.Kind {
	t.Helper()
	kind, err := mcpadapter.New(mcpadapter.Options{
		Client: client, Descriptor: descriptor, Artifacts: artifacts,
		InlineLimit: inlineLimit, MaxArtifacts: maxArtifacts,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return kind
}

func assertExecutionCode(t *testing.T, err error, code string) {
	t.Helper()
	var executionErr *stepkind.ExecutionError
	if !errors.As(err, &executionErr) || executionErr.Code != code {
		t.Fatalf("error = %#v, want code %q", err, code)
	}
}
