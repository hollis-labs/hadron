package llm_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"

	llm "github.com/hollis-labs/hadron/workflow/adapters/llm"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
)

type fakePolicy struct {
	mu      sync.Mutex
	request llm.PolicyRequest
	binding llm.ProviderBinding
	err     error
}

func (f *fakePolicy) Authorize(_ context.Context, request llm.PolicyRequest) (llm.ProviderBinding, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.request = request
	if f.binding.Profile == "" {
		f.binding = llm.ProviderBinding{Profile: request.Profile, Provider: "fixture", Model: "model-1", BindingID: "binding-1", Revision: "r1"}
	}
	return f.binding, f.err
}

type fakeProvider struct {
	mu        sync.Mutex
	calls     []llm.ProviderRequest
	receivers []bool
	complete  func(context.Context, llm.ProviderRequest, llm.StreamReceiver) (llm.ProviderResponse, error)
	responses []llm.ProviderResponse
	err       error
}

func (f *fakeProvider) Complete(ctx context.Context, request llm.ProviderRequest, receiver llm.StreamReceiver) (llm.ProviderResponse, error) {
	f.mu.Lock()
	f.calls = append(f.calls, request)
	f.receivers = append(f.receivers, receiver != nil)
	fn := f.complete
	err := f.err
	var response llm.ProviderResponse
	if len(f.responses) > 0 {
		response = f.responses[0]
		f.responses = f.responses[1:]
	}
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, request, receiver)
	}
	return response, err
}

type fakeTools struct {
	mu          sync.Mutex
	definitions []llm.ToolDefinition
	requests    []llm.ToolExecutionRequest
	result      llm.ToolExecutionResult
	err         error
}

func (f *fakeTools) ResolveTools(_ context.Context, allowed []string) ([]llm.ToolDefinition, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.definitions == nil {
		for _, name := range allowed {
			f.definitions = append(f.definitions, llm.ToolDefinition{Name: name, InputSchema: graph.Schema{"type": "object"}})
		}
	}
	return append([]llm.ToolDefinition(nil), f.definitions...), nil
}
func (f *fakeTools) ExecuteTool(_ context.Context, request llm.ToolExecutionRequest) (llm.ToolExecutionResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	request.Allowed = append([]string(nil), request.Allowed...)
	f.requests = append(f.requests, request)
	result := f.result
	if result.Tool == "" {
		result.Tool = request.Request.Name
	}
	return result, f.err
}

type streamSink struct {
	mu     sync.Mutex
	chunks []string
}

type failingStreamSink struct{ err error }

func (s failingStreamSink) Receive(context.Context, llm.StreamChunk) error { return s.err }

func (s *streamSink) Receive(_ context.Context, chunk llm.StreamChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chunks = append(s.chunks, chunk.Text)
	return nil
}

func validConfig() graph.Config {
	return graph.Config{
		"profile": "default", "messages": []any{map[string]any{"role": "user", "content": "produce JSON"}},
		"output_schema": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"answer"}, "properties": map[string]any{"answer": map[string]any{"type": "integer"}}},
	}
}

func validResponse(number string) llm.ProviderResponse {
	return llm.ProviderResponse{HasOutput: true, Output: map[string]any{"answer": json.Number(number)}, RawText: `{"answer":` + number + `}`, Usage: llm.Usage{InputTokens: 2, OutputTokens: 3, TotalTokens: 5}, StopReason: llm.StopCompleted, Audit: llm.ProviderAudit{RequestID: "request-1"}}
}

func invocation(config graph.Config) stepkind.PreparedInvocation {
	return stepkind.PreparedInvocation{Invocation: stepkind.Invocation{Identity: stepkind.InvocationIdentity{RunID: "run-1", NodeID: "generate", Attempt: 1}, Config: config, Inputs: values.ValueSet{}}}
}

func newKind(t *testing.T, provider *fakeProvider, tools *fakeTools, mutate ...func(*llm.Options)) *llm.Kind {
	t.Helper()
	options := llm.Options{Policy: &fakePolicy{}, Provider: provider, Tools: tools}
	for _, fn := range mutate {
		fn(&options)
	}
	kind, err := llm.New(options)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return kind
}

func executionError(t *testing.T, err error) *stepkind.ExecutionError {
	t.Helper()
	var result *stepkind.ExecutionError
	if !errors.As(err, &result) {
		t.Fatalf("error = %T %v", err, err)
	}
	return result
}

func TestRegistrationSpecAndClosedConfig(t *testing.T) {
	registry := stepkind.NewRegistry()
	provider := &fakeProvider{}
	policy := &fakePolicy{}
	kind, err := llm.Register(registry, llm.Options{Policy: policy, Provider: provider})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if registered, ok := registry.Lookup(llm.KindName, llm.KindVersion); !ok || registered != kind {
		t.Fatalf("lookup = %T, %v", registered, ok)
	}
	spec := kind.Spec()
	if spec.Idempotency != graph.IdempotencyKeyed || spec.RetrySafety != stepkind.RetryRequiresIdempotency || spec.Cancellation.Mode != stepkind.CancellationContext || !reflect.DeepEqual(spec.RequiredCapabilities, []string{"llm.provider"}) {
		t.Fatalf("spec = %#v", spec)
	}
	if findings := kind.ValidateConfig(t.Context(), validConfig()); len(findings) != 0 {
		t.Fatalf("valid findings = %#v", findings)
	}
	bad := validConfig()
	bad["credential"] = "secret"
	if findings := kind.ValidateConfig(t.Context(), bad); len(findings) != 1 || !strings.Contains(findings[0].Message, "unsupported field") {
		t.Fatalf("closed findings = %#v", findings)
	}
	bad = validConfig()
	bad["output_schema"] = map[string]any{"$ref": "https://example.test/schema"}
	if findings := kind.ValidateConfig(t.Context(), bad); len(findings) != 1 {
		t.Fatalf("external schema findings = %#v", findings)
	}
	var typedNil *fakeProvider
	if _, err := llm.New(llm.Options{Policy: policy, Provider: typedNil}); !errors.Is(err, llm.ErrInvalidOptions) {
		t.Fatalf("typed-nil New() error = %v", err)
	}
}

func TestConfigAdmissionBoundsContextInputsAndTools(t *testing.T) {
	kind := newKind(t, &fakeProvider{}, nil)
	for _, field := range []string{"context_inputs", "tools"} {
		t.Run(field, func(t *testing.T) {
			prefix := field
			if field == "context_inputs" {
				prefix = "context"
			}
			config := validConfig()
			config[field] = nameList(prefix, 128)
			if findings := kind.ValidateConfig(t.Context(), config); len(findings) != 0 {
				t.Fatalf("exact-bound findings = %#v", findings)
			}
			config[field] = nameList(prefix, 129)
			if findings := kind.ValidateConfig(t.Context(), config); len(findings) != 1 || !strings.Contains(findings[0].Message, "no more than 128") {
				t.Fatalf("over-bound findings = %#v", findings)
			}
		})
	}
	properties := kind.Spec().ConfigSchema["properties"].(map[string]any)
	for _, field := range []string{"context_inputs", "tools"} {
		if properties[field].(map[string]any)["maxItems"] != json.Number("128") {
			t.Fatalf("%s schema = %#v", field, properties[field])
		}
	}
}

func TestTypedOutputExactNumbersUsageStopAuditAndPolicyProjection(t *testing.T) {
	provider := &fakeProvider{responses: []llm.ProviderResponse{validResponse("9007199254740993")}}
	policy := &fakePolicy{}
	kind, err := llm.New(llm.Options{Policy: policy, Provider: provider})
	if err != nil {
		t.Fatal(err)
	}
	config := validConfig()
	config["system"] = "private prompt"
	result, err := kind.Execute(t.Context(), invocation(config))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result.Outcome != stepkind.StepCompleted || len(result.Outputs) != 6 {
		t.Fatalf("result = %#v", result)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("result validation = %v", err)
	}
	output := result.Outputs[llm.OutputValue]
	answer := output.Inline.(map[string]any)["answer"]
	if answer != json.Number("9007199254740993") {
		t.Fatalf("exact answer = %T(%v)", answer, answer)
	}
	for name, value := range result.Outputs {
		if value.Redaction != values.RedactionPrivate || value.Retention != values.RetentionRun {
			t.Fatalf("%s classification = %s/%s", name, value.Redaction, value.Retention)
		}
	}
	usage := result.Outputs[llm.OutputUsage].Inline.(map[string]any)
	if usage["total_tokens"] != json.Number("5") || usage["requests"] != json.Number("1") {
		t.Fatalf("usage = %#v", usage)
	}
	audit := result.Outputs[llm.OutputAudit].Inline.(map[string]any)
	providerCalls := audit["provider_calls"].([]any)
	if audit["provider"] != "fixture" || len(providerCalls) != 1 || providerCalls[0].(map[string]any)["request_id"] != "request-1" {
		t.Fatalf("audit = %#v", audit)
	}
	policy.mu.Lock()
	projected := policy.request
	policy.mu.Unlock()
	encoded, _ := json.Marshal(projected)
	if strings.Contains(string(encoded), "private prompt") {
		t.Fatalf("policy received prompt: %s", encoded)
	}
	provider.calls[0].Messages[0].Content = "mutated"
	if result.Outputs[llm.OutputRawText].Inline != `{"answer":9007199254740993}` {
		t.Fatal("provider mutation affected result")
	}
	rawProvider := &fakeProvider{responses: []llm.ProviderResponse{{RawText: `{"answer":9007199254740993}`, Usage: llm.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2}, StopReason: llm.StopLength}}}
	rawKind := newKind(t, rawProvider, nil)
	rawResult, rawErr := rawKind.Execute(t.Context(), invocation(validConfig()))
	if rawErr != nil || rawResult.Outputs[llm.OutputValue].Inline.(map[string]any)["answer"] != json.Number("9007199254740993") || rawResult.Outputs[llm.OutputStop].Inline != string(llm.StopLength) {
		t.Fatalf("raw exact result = %#v, %v", rawResult, rawErr)
	}
}

func TestToolAllowlistIsDoubleEnforcedAndCreatesLiteralEvidence(t *testing.T) {
	provider := &fakeProvider{responses: []llm.ProviderResponse{
		{ToolRequests: []llm.ToolRequest{{ID: "call-1", Name: "search.read", Arguments: map[string]any{"q": "x"}}}, Usage: llm.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2}, StopReason: llm.StopTool}, validResponse("7"),
	}}
	tools := &fakeTools{result: llm.ToolExecutionResult{Content: map[string]any{"ok": true}}}
	kind := newKind(t, provider, tools)
	config := validConfig()
	config["tools"] = []any{"search.read"}
	config["max_tool_iterations"] = json.Number("2")
	recorder := verification.NewActivityRecorder()
	request := invocation(config)
	request.Invocation.Activity = recorder
	result, err := kind.Execute(t.Context(), request)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	tools.mu.Lock()
	calls := append([]llm.ToolExecutionRequest(nil), tools.requests...)
	tools.mu.Unlock()
	if len(calls) != 1 || !reflect.DeepEqual(calls[0].Allowed, []string{"search.read"}) {
		t.Fatalf("tool requests = %#v", calls)
	}
	records := result.Outputs[llm.OutputToolCalls].Inline.([]any)
	if len(records) != 1 || records[0].(map[string]any)["tool"] != "search.read" {
		t.Fatalf("records = %#v", records)
	}
	activities, err := recorder.Freeze()
	if err != nil || len(activities) != 1 || activities[0].ToolCall.Tool != "search.read" || activities[0].ToolCall.Outcome != verification.ActivitySucceeded {
		t.Fatalf("activities = %#v, %v", activities, err)
	}
	if provider.calls[1].Messages[len(provider.calls[1].Messages)-1].ToolResult == nil {
		t.Fatal("tool result was not returned to provider")
	}
	verifier, ok := verification.NewDefaultRegistry().Lookup(verification.CheckExpectedToolCall)
	if !ok {
		t.Fatal("expected_tool_call verifier is unavailable")
	}
	check, verifyErr := verifier.Verify(t.Context(), verification.Request{Check: graph.VerificationCheck{Kind: verification.CheckExpectedToolCall, Config: graph.Config{"server": "llm", "tool": "search.read"}}, OutputSchema: kind.Spec().OutputSchema, Outputs: result.Outputs, Evidence: activities})
	if verifyErr != nil || check.Outcome != verification.CheckPassed {
		t.Fatalf("verification = %#v, %v", check, verifyErr)
	}
	failedCheck, verifyErr := verifier.Verify(t.Context(), verification.Request{Check: graph.VerificationCheck{Kind: verification.CheckExpectedToolCall, Config: graph.Config{"server": "llm", "tool": "admin.delete"}}, OutputSchema: kind.Spec().OutputSchema, Outputs: result.Outputs, Evidence: activities})
	if verifyErr != nil || failedCheck.Outcome != verification.CheckFailed {
		t.Fatalf("failed verification = %#v, %v", failedCheck, verifyErr)
	}
}

func TestModelCannotSelfReportOrEscapeToolAllowlist(t *testing.T) {
	t.Run("self-report is ordinary output", func(t *testing.T) {
		provider := &fakeProvider{responses: []llm.ProviderResponse{validResponse("1")}}
		response := provider.responses[0]
		response.Output = map[string]any{"answer": json.Number("1"), "tool_calls": []any{map[string]any{"tool": "admin.delete"}}}
		provider.responses[0] = response
		kind := newKind(t, provider, nil)
		_, err := kind.Execute(t.Context(), invocation(validConfig()))
		if executionError(t, err).Code != "llm_schema_failed" {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("unlisted request denied before host", func(t *testing.T) {
		provider := &fakeProvider{responses: []llm.ProviderResponse{{ToolRequests: []llm.ToolRequest{{ID: "1", Name: "admin.delete", Arguments: map[string]any{}}}, Usage: llm.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2}, StopReason: llm.StopTool}}}
		tools := &fakeTools{}
		kind := newKind(t, provider, tools)
		config := validConfig()
		config["tools"] = []any{"search.read"}
		_, err := kind.Execute(t.Context(), invocation(config))
		if executionError(t, err).Code != "llm_tool_denied" {
			t.Fatalf("error = %v", err)
		}
		if len(tools.requests) != 0 {
			t.Fatal("denied tool reached host")
		}
	})
	t.Run("resolver cannot widen", func(t *testing.T) {
		provider := &fakeProvider{}
		tools := &fakeTools{definitions: []llm.ToolDefinition{{Name: "admin.delete", InputSchema: graph.Schema{}}}}
		kind := newKind(t, provider, tools)
		config := validConfig()
		config["tools"] = []any{"search.read"}
		_, err := kind.Execute(t.Context(), invocation(config))
		if executionError(t, err).Code != "llm_tool_contract" || len(provider.calls) != 0 {
			t.Fatalf("error/calls = %v/%d", err, len(provider.calls))
		}
	})
	t.Run("result identity cannot change", func(t *testing.T) {
		provider := &fakeProvider{responses: []llm.ProviderResponse{{ToolRequests: []llm.ToolRequest{{ID: "1", Name: "search.read", Arguments: map[string]any{}}}, Usage: llm.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2}, StopReason: llm.StopTool}}}
		tools := &fakeTools{result: llm.ToolExecutionResult{Tool: "admin.delete", Content: map[string]any{}}}
		kind := newKind(t, provider, tools)
		config := validConfig()
		config["tools"] = []any{"search.read"}
		recorder := verification.NewActivityRecorder()
		request := invocation(config)
		request.Invocation.Activity = recorder
		_, err := kind.Execute(t.Context(), request)
		if executionError(t, err).Code != "llm_tool_result" {
			t.Fatalf("error = %v", err)
		}
		activities, freezeErr := recorder.Freeze()
		if freezeErr != nil || len(activities) != 1 || activities[0].ToolCall.Outcome != verification.ActivityFailed {
			t.Fatalf("activities = %#v, %v", activities, freezeErr)
		}
	})
	t.Run("arguments must satisfy trusted schema", func(t *testing.T) {
		provider := &fakeProvider{responses: []llm.ProviderResponse{{ToolRequests: []llm.ToolRequest{{ID: "1", Name: "search.read", Arguments: map[string]any{"query": json.Number("7")}}}, Usage: llm.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2}, StopReason: llm.StopTool}}}
		tools := &fakeTools{definitions: []llm.ToolDefinition{{Name: "search.read", InputSchema: graph.Schema{"type": "object", "required": []any{"query"}, "properties": map[string]any{"query": map[string]any{"type": "string"}}}}}}
		kind := newKind(t, provider, tools)
		config := validConfig()
		config["tools"] = []any{"search.read"}
		_, err := kind.Execute(t.Context(), invocation(config))
		failure := executionError(t, err)
		if failure.Code != "llm_tool_arguments_invalid" || failure.Classification != stepkind.RetryPermanent || len(tools.requests) != 0 {
			t.Fatalf("argument failure/calls = %#v/%d", failure, len(tools.requests))
		}
	})
}

func TestToolRequestBatchIsPreflightedAgainstRemainingBudget(t *testing.T) {
	t.Run("exact remaining", func(t *testing.T) {
		provider := &fakeProvider{responses: []llm.ProviderResponse{
			{ToolRequests: toolRequests(2), Usage: llm.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2}, StopReason: llm.StopTool},
			validResponse("1"),
		}}
		tools := &fakeTools{result: llm.ToolExecutionResult{Content: map[string]any{"ok": true}}}
		kind := newKind(t, provider, tools)
		config := validConfig()
		config["tools"] = []any{"search.read"}
		config["budget"] = map[string]any{"max_tool_calls": json.Number("2")}
		activity := verification.NewActivityRecorder()
		request := invocation(config)
		request.Invocation.Activity = activity
		result, err := kind.Execute(t.Context(), request)
		if err != nil || len(result.Outputs) == 0 || len(tools.requests) != 2 {
			t.Fatalf("exact batch result/error/tool calls = %#v, %v, %d", result, err, len(tools.requests))
		}
		activities, freezeErr := activity.Freeze()
		if freezeErr != nil || len(activities) != 2 {
			t.Fatalf("exact batch activities = %#v, %v", activities, freezeErr)
		}
	})

	for _, test := range []struct {
		name   string
		budget int
		count  int
	}{
		{"one over remaining", 2, 3},
		{"one over absolute cap", 128, 129},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := &fakeProvider{responses: []llm.ProviderResponse{{ToolRequests: toolRequests(test.count), Usage: llm.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2}, StopReason: llm.StopTool}}}
			tools := &fakeTools{result: llm.ToolExecutionResult{Content: map[string]any{"ok": true}}}
			kind := newKind(t, provider, tools)
			config := validConfig()
			config["tools"] = []any{"search.read"}
			config["budget"] = map[string]any{"max_tool_calls": json.Number(fmt.Sprint(test.budget))}
			activity := verification.NewActivityRecorder()
			request := invocation(config)
			request.Invocation.Activity = activity
			result, err := kind.Execute(t.Context(), request)
			if executionError(t, err).Code != "llm_budget_exceeded" || len(provider.calls) != 1 || len(tools.requests) != 0 || len(result.Outputs) != 0 {
				t.Fatalf("oversized batch result/error/provider/tool calls = %#v, %v, %d, %d", result, err, len(provider.calls), len(tools.requests))
			}
			activities, freezeErr := activity.Freeze()
			if freezeErr != nil || len(activities) != 0 {
				t.Fatalf("oversized batch activities = %#v, %v", activities, freezeErr)
			}
		})
	}
}

func TestSchemaRepairConsumesOneAggregateBudget(t *testing.T) {
	ref, _ := values.ParseSecretRef("secret://fixture/repair")
	resolved, _ := values.NewResolvedSecret(ref, []byte("repair-secret"))
	defer resolved.Forget()
	redactor, _ := values.NewRedactor(resolved)
	bad := validResponse("1")
	bad.Output = map[string]any{"answer": "repair-secret"}
	bad.RawText = ""
	provider := &fakeProvider{responses: []llm.ProviderResponse{bad, validResponse("2")}}
	kind := newKind(t, provider, nil, func(options *llm.Options) { options.Redactor = redactor })
	config := validConfig()
	config["repair"] = "once"
	result, err := kind.Execute(t.Context(), invocation(config))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(provider.calls) != 2 || !provider.calls[1].Repair || len(provider.calls[1].Tools) != 0 {
		t.Fatalf("requests = %#v", provider.calls)
	}
	repairAssistant := provider.calls[1].Messages[len(provider.calls[1].Messages)-2].Content
	if !strings.Contains(repairAssistant, `{"answer":"[REDACTED]"}`) || strings.Contains(repairAssistant, "repair-secret") {
		t.Fatalf("structured repair evidence = %q", repairAssistant)
	}
	usage := result.Outputs[llm.OutputUsage].Inline.(map[string]any)
	if usage["total_tokens"] != json.Number("10") || usage["requests"] != json.Number("2") {
		t.Fatalf("usage = %#v", usage)
	}
	if calls := result.Outputs[llm.OutputAudit].Inline.(map[string]any)["provider_calls"].([]any); len(calls) != 2 {
		t.Fatalf("repair audit calls = %#v", calls)
	}
	provider = &fakeProvider{responses: []llm.ProviderResponse{bad, validResponse("2")}}
	kind = newKind(t, provider, nil)
	config["budget"] = map[string]any{"max_total_tokens": json.Number("6")}
	_, err = kind.Execute(t.Context(), invocation(config))
	if executionError(t, err).Code != "llm_budget_exceeded" {
		t.Fatalf("budget error = %v", err)
	}
	provider = &fakeProvider{responses: []llm.ProviderResponse{bad}}
	kind = newKind(t, provider, nil)
	config = validConfig()
	_, err = kind.Execute(t.Context(), invocation(config))
	if executionError(t, err).Code != "llm_schema_failed" || len(provider.calls) != 1 {
		t.Fatalf("fail mode = %v, calls %d", err, len(provider.calls))
	}
	overflowToolTurn := llm.ProviderResponse{ToolRequests: []llm.ToolRequest{{ID: "overflow", Name: "search.read", Arguments: map[string]any{}}}, Usage: llm.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2, CostMicrounits: math.MaxInt64}, StopReason: llm.StopTool}
	overflowFinal := validResponse("2")
	overflowFinal.Usage.CostMicrounits = 1
	provider = &fakeProvider{responses: []llm.ProviderResponse{overflowToolTurn, overflowFinal}}
	kind = newKind(t, provider, &fakeTools{result: llm.ToolExecutionResult{Content: map[string]any{"ok": true}}})
	config = validConfig()
	config["tools"] = []any{"search.read"}
	_, err = kind.Execute(t.Context(), invocation(config))
	if executionError(t, err).Code != "llm_budget_exceeded" || len(provider.calls) != 2 {
		t.Fatalf("overflow accounting = %v, calls %d", err, len(provider.calls))
	}
}

func TestTimeoutCancellationProviderTaxonomyAndSafeErrors(t *testing.T) {
	secret := "do-not-persist"
	blocking := &fakeProvider{complete: func(ctx context.Context, _ llm.ProviderRequest, _ llm.StreamReceiver) (llm.ProviderResponse, error) {
		<-ctx.Done()
		return llm.ProviderResponse{}, ctx.Err()
	}}
	kind := newKind(t, blocking, nil)
	config := validConfig()
	config["timeout"] = "2ms"
	_, err := kind.Execute(t.Context(), invocation(config))
	if failure := executionError(t, err); failure.Code != "llm_timeout" || failure.Classification != stepkind.Retryable {
		t.Fatalf("timeout = %#v", failure)
	}
	provider := &fakeProvider{err: &llm.ProviderError{Kind: llm.ProviderInfrastructure, Retryable: true, Cause: errors.New(secret)}}
	kind = newKind(t, provider, nil)
	_, err = kind.Execute(t.Context(), invocation(validConfig()))
	failure := executionError(t, err)
	if failure.Code != "llm_infrastructure_error" || failure.Classification != stepkind.Retryable || strings.Contains(failure.Error(), secret) || strings.Contains(fmt.Sprint(failure.Details), secret) {
		t.Fatalf("unsafe taxonomy = %#v", failure)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = kind.Execute(ctx, invocation(validConfig()))
	if executionError(t, err).Code != "llm_canceled" {
		t.Fatalf("cancel = %v", err)
	}
	provider = &fakeProvider{responses: []llm.ProviderResponse{{Usage: llm.Usage{}, StopReason: llm.StopReason("invented")}}}
	kind = newKind(t, provider, nil)
	_, err = kind.Execute(t.Context(), invocation(validConfig()))
	if executionError(t, err).Code != "llm_model_result" {
		t.Fatalf("model result = %v", err)
	}
	provider = &fakeProvider{responses: []llm.ProviderResponse{{HasOutput: true, Output: map[string]any{"answer": json.Number("1")}, Usage: llm.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 3}, StopReason: llm.StopCompleted}}}
	kind = newKind(t, provider, nil)
	_, err = kind.Execute(t.Context(), invocation(validConfig()))
	if executionError(t, err).Code != "llm_model_result" {
		t.Fatalf("model accounting = %v", err)
	}
	provider = &fakeProvider{responses: []llm.ProviderResponse{{HasOutput: false, Output: map[string]any{"answer": json.Number("1")}, RawText: `{"answer":1}`, Usage: llm.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2}, StopReason: llm.StopCompleted}}}
	kind = newKind(t, provider, nil)
	result, err := kind.Execute(t.Context(), invocation(validConfig()))
	if executionError(t, err).Code != "llm_model_result" || len(result.Outputs) != 0 {
		t.Fatalf("ambiguous has_output result/error = %#v, %v", result, err)
	}
	provider = &fakeProvider{responses: []llm.ProviderResponse{{ToolRequests: []llm.ToolRequest{{ID: "1", Name: "search.read", Arguments: map[string]any{}}}, Usage: llm.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2}, StopReason: llm.StopTool}}}
	tools := &fakeTools{err: &llm.ToolError{Retryable: true, Cause: errors.New(secret)}}
	kind = newKind(t, provider, tools)
	toolConfig := validConfig()
	toolConfig["tools"] = []any{"search.read"}
	_, err = kind.Execute(t.Context(), invocation(toolConfig))
	toolFailure := executionError(t, err)
	if toolFailure.Code != "llm_tool_failed" || toolFailure.Classification != stepkind.Retryable || strings.Contains(toolFailure.Error(), secret) {
		t.Fatalf("tool failure = %#v", toolFailure)
	}
}

func TestStreamingAndFinalOutputAreBoundedAndRedacted(t *testing.T) {
	ref, _ := values.ParseSecretRef("secret://fixture/token")
	resolved, _ := values.NewResolvedSecret(ref, []byte("top-secret"))
	defer resolved.Forget()
	redactor, _ := values.NewRedactor(resolved)
	sink := &streamSink{}
	provider := &fakeProvider{complete: func(ctx context.Context, _ llm.ProviderRequest, receiver llm.StreamReceiver) (llm.ProviderResponse, error) {
		if receiver == nil {
			return llm.ProviderResponse{}, errors.New("missing receiver")
		}
		if err := receiver.Receive(ctx, llm.StreamChunk{Text: "value=top-"}); err != nil {
			return llm.ProviderResponse{}, err
		}
		if err := receiver.Receive(ctx, llm.StreamChunk{Text: "secret"}); err != nil {
			return llm.ProviderResponse{}, err
		}
		response := validResponse("1")
		response.Output = map[string]any{"answer": json.Number("1")}
		response.RawText = `{"answer":1,"echo":"top-secret"}`
		return response, nil
	}}
	kind := newKind(t, provider, nil, func(options *llm.Options) { options.Stream = sink; options.Redactor = redactor })
	config := validConfig()
	config["stream"] = true
	result, err := kind.Execute(t.Context(), invocation(config))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	streamed := strings.Join(sink.chunks, "")
	if strings.Contains(streamed, "top-secret") || !strings.Contains(streamed, values.RedactedMarker) || strings.Contains(result.Outputs[llm.OutputRawText].Inline.(string), "top-secret") {
		t.Fatalf("stream/raw = %#v/%v", sink.chunks, result.Outputs[llm.OutputRawText].Inline)
	}
	stringConfig := validConfig()
	stringConfig["output_schema"] = map[string]any{"type": "object", "required": []any{"answer"}, "properties": map[string]any{"answer": map[string]any{"type": "string"}}}
	provider = &fakeProvider{responses: []llm.ProviderResponse{{HasOutput: true, Output: map[string]any{"answer": "top-secret"}, RawText: `{"answer":"top-secret"}`, Usage: llm.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2}, StopReason: llm.StopCompleted}}}
	kind = newKind(t, provider, nil, func(options *llm.Options) { options.Redactor = redactor })
	result, err = kind.Execute(t.Context(), invocation(stringConfig))
	if err != nil || result.Outputs[llm.OutputValue].Inline.(map[string]any)["answer"] != values.RedactedMarker {
		t.Fatalf("structured redaction = %#v, %v", result, err)
	}
	concurrentSink := &streamSink{}
	provider = &fakeProvider{complete: func(ctx context.Context, _ llm.ProviderRequest, receiver llm.StreamReceiver) (llm.ProviderResponse, error) {
		var group sync.WaitGroup
		errorsCh := make(chan error, 8)
		for index := 0; index < 8; index++ {
			group.Add(1)
			go func() { defer group.Done(); errorsCh <- receiver.Receive(ctx, llm.StreamChunk{Text: "x"}) }()
		}
		group.Wait()
		close(errorsCh)
		for receiveErr := range errorsCh {
			if receiveErr != nil {
				return llm.ProviderResponse{}, receiveErr
			}
		}
		return validResponse("1"), nil
	}}
	kind = newKind(t, provider, nil, func(options *llm.Options) { options.Stream = concurrentSink })
	result, err = kind.Execute(t.Context(), invocation(config))
	if err != nil || len(concurrentSink.chunks) != 8 {
		t.Fatalf("concurrent stream = %d, %v", len(concurrentSink.chunks), err)
	}
	provider = &fakeProvider{complete: func(ctx context.Context, _ llm.ProviderRequest, receiver llm.StreamReceiver) (llm.ProviderResponse, error) {
		return llm.ProviderResponse{}, receiver.Receive(ctx, llm.StreamChunk{Text: strings.Repeat("x", 32)})
	}}
	kind = newKind(t, provider, nil)
	config = validConfig()
	config["stream"] = true
	config["budget"] = map[string]any{"max_output_bytes": json.Number("8")}
	_, err = kind.Execute(t.Context(), invocation(config))
	if executionError(t, err).Code != "llm_budget_exceeded" {
		t.Fatalf("stream bound = %v", err)
	}
	provider = &fakeProvider{complete: func(ctx context.Context, _ llm.ProviderRequest, receiver llm.StreamReceiver) (llm.ProviderResponse, error) {
		_ = receiver.Receive(ctx, llm.StreamChunk{Text: strings.Repeat("x", 1000)})
		return validResponse("1"), nil
	}}
	kind = newKind(t, provider, nil)
	config = validConfig()
	config["stream"] = true
	config["budget"] = map[string]any{"max_output_bytes": json.Number("1024")}
	_, err = kind.Execute(t.Context(), invocation(config))
	if executionError(t, err).Code != "llm_budget_exceeded" {
		t.Fatalf("combined stream/final bound = %v", err)
	}
	flushSink := &streamSink{}
	provider = &fakeProvider{complete: func(ctx context.Context, _ llm.ProviderRequest, receiver llm.StreamReceiver) (llm.ProviderResponse, error) {
		if receiveErr := receiver.Receive(ctx, llm.StreamChunk{Text: "tail"}); receiveErr != nil {
			return llm.ProviderResponse{}, receiveErr
		}
		return llm.ProviderResponse{}, &llm.ProviderError{Kind: llm.ProviderUnavailable, Retryable: true, Cause: errors.New("offline")}
	}}
	kind = newKind(t, provider, nil, func(options *llm.Options) { options.Stream = flushSink; options.Redactor = redactor })
	config = validConfig()
	config["stream"] = true
	_, err = kind.Execute(t.Context(), invocation(config))
	if executionError(t, err).Code != "llm_provider_unavailable" || strings.Join(flushSink.chunks, "") != "tail" {
		t.Fatalf("error flush = %#v, %v", flushSink.chunks, err)
	}
	provider = &fakeProvider{complete: func(ctx context.Context, _ llm.ProviderRequest, receiver llm.StreamReceiver) (llm.ProviderResponse, error) {
		_ = receiver.Receive(ctx, llm.StreamChunk{Text: "tail"})
		return validResponse("1"), nil
	}}
	kind = newKind(t, provider, nil, func(options *llm.Options) {
		options.Stream = failingStreamSink{err: errors.New("sink failed")}
		options.Redactor = redactor
	})
	_, err = kind.Execute(t.Context(), invocation(config))
	if executionError(t, err).Code != "llm_infrastructure_error" {
		t.Fatalf("close failure = %v", err)
	}
}

func TestProviderProvenanceRejectsCredentialShapedMetadata(t *testing.T) {
	tests := []struct {
		name    string
		binding llm.ProviderBinding
		audit   llm.ProviderAudit
		want    string
	}{
		{"binding key", llm.ProviderBinding{Profile: "default", Provider: "fixture", Model: "model", BindingID: "binding", Attributes: map[string]string{"api_token": "opaque"}}, llm.ProviderAudit{}, "llm_policy_binding"},
		{"binding secret locator", llm.ProviderBinding{Profile: "default", Provider: "fixture", Model: "model", BindingID: "secret://fixture/binding"}, llm.ProviderAudit{}, "llm_policy_binding"},
		{"audit secret locator", llm.ProviderBinding{Profile: "default", Provider: "fixture", Model: "model", BindingID: "binding"}, llm.ProviderAudit{Attributes: map[string]string{"region": "secret://fixture/token"}}, "llm_model_result"},
		{"audit credential URI", llm.ProviderBinding{Profile: "default", Provider: "fixture", Model: "model", BindingID: "binding"}, llm.ProviderAudit{Attributes: map[string]string{"endpoint": "https://user:pass@example.test"}}, "llm_model_result"},
		{"audit embedded bearer", llm.ProviderBinding{Profile: "default", Provider: "fixture", Model: "model", BindingID: "binding"}, llm.ProviderAudit{Attributes: map[string]string{"note": "prefix Bearer opaque suffix"}}, "llm_model_result"},
		{"audit embedded basic", llm.ProviderBinding{Profile: "default", Provider: "fixture", Model: "model", BindingID: "binding"}, llm.ProviderAudit{Attributes: map[string]string{"note": "prefix Basic opaque suffix"}}, "llm_model_result"},
		{"audit token assignment", llm.ProviderBinding{Profile: "default", Provider: "fixture", Model: "model", BindingID: "binding"}, llm.ProviderAudit{Attributes: map[string]string{"note": "token=opaque"}}, "llm_model_result"},
		{"audit password assignment", llm.ProviderBinding{Profile: "default", Provider: "fixture", Model: "model", BindingID: "binding"}, llm.ProviderAudit{Attributes: map[string]string{"note": "password = opaque"}}, "llm_model_result"},
		{"audit api key assignment", llm.ProviderBinding{Profile: "default", Provider: "fixture", Model: "model", BindingID: "binding"}, llm.ProviderAudit{Attributes: map[string]string{"note": "api_key: opaque"}}, "llm_model_result"},
		{"audit signature assignment", llm.ProviderBinding{Profile: "default", Provider: "fixture", Model: "model", BindingID: "binding"}, llm.ProviderAudit{Attributes: map[string]string{"note": "signature=opaque"}}, "llm_model_result"},
		{"audit malformed credential URI", llm.ProviderBinding{Profile: "default", Provider: "fixture", Model: "model", BindingID: "binding"}, llm.ProviderAudit{Attributes: map[string]string{"endpoint": "https://user:pass@/%zz"}}, "llm_model_result"},
		{"audit benign query material", llm.ProviderBinding{Profile: "default", Provider: "fixture", Model: "model", BindingID: "binding"}, llm.ProviderAudit{Attributes: map[string]string{"endpoint": "https://example.test/models?region=us"}}, "llm_model_result"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := validResponse("1")
			response.Audit = test.audit
			provider := &fakeProvider{responses: []llm.ProviderResponse{response}}
			policy := &fakePolicy{binding: test.binding}
			kind, kindErr := llm.New(llm.Options{Policy: policy, Provider: provider})
			if kindErr != nil {
				t.Fatal(kindErr)
			}
			_, err := kind.Execute(t.Context(), invocation(validConfig()))
			if executionError(t, err).Code != test.want {
				t.Fatalf("error = %v", err)
			}
			for _, raw := range []string{"opaque", "user:pass", "region=us", "%zz"} {
				if strings.Contains(err.Error(), raw) {
					t.Fatalf("error leaked provenance value %q: %v", raw, err)
				}
			}
			if test.want == "llm_policy_binding" && len(provider.calls) != 0 {
				t.Fatal("unsafe binding reached provider")
			}
		})
	}
}

func TestProviderProvenanceMetadataBoundsAndOutputCharging(t *testing.T) {
	exact := provenanceAttributes(llm.MaximumProvenanceAttributeCount, llm.MaximumProvenanceAttributeBytes)
	response := validResponse("1")
	response.Audit.Attributes = exact
	policy := &fakePolicy{binding: llm.ProviderBinding{Profile: "default", Provider: "fixture", Model: "model", BindingID: "binding", Attributes: exact}}
	kind, err := llm.New(llm.Options{Policy: policy, Provider: &fakeProvider{responses: []llm.ProviderResponse{response}}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := kind.Execute(t.Context(), invocation(validConfig()))
	if err != nil {
		t.Fatalf("exact-bound Execute() error = %v", err)
	}
	audit := result.Outputs[llm.OutputAudit].Inline.(map[string]any)
	if len(audit["binding_attributes"].(map[string]any)) != llm.MaximumProvenanceAttributeCount || len(audit["provider_calls"].([]any)[0].(map[string]any)["attributes"].(map[string]any)) != llm.MaximumProvenanceAttributeCount {
		t.Fatalf("exact-bound audit = %#v", audit)
	}

	overCount := provenanceAttributes(llm.MaximumProvenanceAttributeCount+1, (llm.MaximumProvenanceAttributeCount+1)*8)
	policy = &fakePolicy{binding: llm.ProviderBinding{Profile: "default", Provider: "fixture", Model: "model", BindingID: "binding", Attributes: overCount}}
	provider := &fakeProvider{responses: []llm.ProviderResponse{validResponse("1")}}
	kind, err = llm.New(llm.Options{Policy: policy, Provider: provider})
	if err != nil {
		t.Fatal(err)
	}
	result, err = kind.Execute(t.Context(), invocation(validConfig()))
	if executionError(t, err).Code != "llm_policy_binding" || len(provider.calls) != 0 || len(result.Outputs) != 0 {
		t.Fatalf("over-count binding result/error/calls = %#v, %v, %d", result, err, len(provider.calls))
	}

	overBytes := provenanceAttributes(llm.MaximumProvenanceAttributeCount, llm.MaximumProvenanceAttributeBytes+1)
	response = validResponse("1")
	response.Audit.Attributes = overBytes
	kind = newKind(t, &fakeProvider{responses: []llm.ProviderResponse{response}}, nil)
	result, err = kind.Execute(t.Context(), invocation(validConfig()))
	if executionError(t, err).Code != "llm_model_result" || len(result.Outputs) != 0 {
		t.Fatalf("over-byte audit result/error = %#v, %v", result, err)
	}

	config := validConfig()
	config["budget"] = map[string]any{"max_output_bytes": json.Number("1024")}
	kind = newKind(t, &fakeProvider{responses: []llm.ProviderResponse{validResponse("1")}}, nil)
	if _, err = kind.Execute(t.Context(), invocation(config)); err != nil {
		t.Fatalf("baseline output budget error = %v", err)
	}
	charged := validResponse("1")
	charged.Audit.Attributes = map[string]string{"first": strings.Repeat("x", 512), "second": strings.Repeat("y", 512)}
	kind = newKind(t, &fakeProvider{responses: []llm.ProviderResponse{charged}}, nil)
	result, err = kind.Execute(t.Context(), invocation(config))
	if executionError(t, err).Code != "llm_budget_exceeded" || len(result.Outputs) != 0 {
		t.Fatalf("charged audit result/error = %#v, %v", result, err)
	}
}

func TestBindingAuditIsChargedBeforeProviderCall(t *testing.T) {
	binding := llm.ProviderBinding{Profile: "default", Provider: "fixture", Model: "model", BindingID: "binding", Revision: "r1", Attributes: map[string]string{"material": strings.Repeat("x", 512)}}
	bindingBytes := bindingAuditProjectionBytes(t, binding)
	for _, test := range []struct {
		name  string
		limit int
	}{
		{"exactly exhausts budget", bindingBytes},
		{"exceeds budget", bindingBytes - 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := &fakeProvider{responses: []llm.ProviderResponse{validResponse("1")}}
			kind, err := llm.New(llm.Options{Policy: &fakePolicy{binding: binding}, Provider: provider})
			if err != nil {
				t.Fatal(err)
			}
			config := validConfig()
			config["budget"] = map[string]any{"max_output_bytes": json.Number(fmt.Sprint(test.limit))}
			result, err := kind.Execute(t.Context(), invocation(config))
			if executionError(t, err).Code != "llm_budget_exceeded" || len(provider.calls) != 0 || len(result.Outputs) != 0 {
				t.Fatalf("binding budget result/error/calls = %#v, %v, %d", result, err, len(provider.calls))
			}
		})
	}
}

func TestOversizedMalformedProviderPayloadFailsBudgetBeforeDeepValidation(t *testing.T) {
	binding := llm.ProviderBinding{Profile: "default", Provider: "fixture", Model: "model", BindingID: "binding", Revision: "r1"}
	provider := &fakeProvider{responses: []llm.ProviderResponse{{
		ToolRequests: []llm.ToolRequest{{ID: "", Name: "search.read", Arguments: map[string]any{"payload": strings.Repeat("x", 4096)}}},
		Usage:        llm.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2},
		StopReason:   llm.StopTool,
	}}}
	tools := &fakeTools{}
	kind, err := llm.New(llm.Options{Policy: &fakePolicy{binding: binding}, Provider: provider, Tools: tools})
	if err != nil {
		t.Fatal(err)
	}
	config := validConfig()
	config["tools"] = []any{"search.read"}
	config["budget"] = map[string]any{"max_output_bytes": json.Number(fmt.Sprint(bindingAuditProjectionBytes(t, binding) + 512))}
	result, err := kind.Execute(t.Context(), invocation(config))
	if executionError(t, err).Code != "llm_budget_exceeded" || len(provider.calls) != 1 || len(tools.requests) != 0 || len(result.Outputs) != 0 {
		t.Fatalf("oversized malformed result/error/provider/tool calls = %#v, %v, %d, %d", result, err, len(provider.calls), len(tools.requests))
	}
}

func TestTypedContextSecretFenceDefensiveCopyAndConcurrency(t *testing.T) {
	input, err := values.NewInline(map[string]any{"n": json.Number("9007199254740993")}, values.Metadata{Producer: values.Producer{Kind: "input", Reference: "run"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	provider := &fakeProvider{complete: func(_ context.Context, request llm.ProviderRequest, _ llm.StreamReceiver) (llm.ProviderResponse, error) {
		request.Context["context"].Inline.(map[string]any)["n"] = "mutated"
		return validResponse("3"), nil
	}}
	kind := newKind(t, provider, nil)
	config := validConfig()
	config["context_inputs"] = []any{"context"}
	request := invocation(config)
	request.Invocation.Inputs = values.ValueSet{"context": input}
	const workers = 16
	errorsCh := make(chan error, workers)
	var wait sync.WaitGroup
	for i := 0; i < workers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, executeErr := kind.Execute(context.Background(), request)
			errorsCh <- executeErr
		}()
	}
	wait.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatalf("concurrent Execute() = %v", err)
		}
	}
	if input.Inline.(map[string]any)["n"] != json.Number("9007199254740993") {
		t.Fatal("provider mutated caller input")
	}
	ref, _ := values.ParseSecretRef("secret://fixture/context")
	secret, _ := values.NewSecretRef(ref, values.Metadata{Producer: values.Producer{Kind: "input", Reference: "run"}, MediaType: "text/plain", Redaction: values.RedactionSecret, Retention: values.RetentionRun})
	request.Invocation.Inputs["context"] = secret
	_, err = kind.Execute(t.Context(), request)
	if executionError(t, err).Code != "llm_input_invalid" {
		t.Fatalf("secret input error = %v", err)
	}
}

func TestProviderRequestAndReturnedOutputsAreDefensiveCopies(t *testing.T) {
	provider := &fakeProvider{responses: []llm.ProviderResponse{validResponse("4")}}
	policy := &fakePolicy{binding: llm.ProviderBinding{Profile: "default", Provider: "fixture", Model: "model", BindingID: "binding", Attributes: map[string]string{"region": "test"}}}
	kind, err := llm.New(llm.Options{Policy: policy, Provider: provider})
	if err != nil {
		t.Fatal(err)
	}
	result, err := kind.Execute(t.Context(), invocation(validConfig()))
	if err != nil {
		t.Fatal(err)
	}
	policy.binding.Attributes["region"] = "changed"
	provider.calls[0].Binding.Attributes["region"] = "also-changed"
	audit := result.Outputs[llm.OutputAudit].Inline.(map[string]any)
	if audit["binding_attributes"].(map[string]any)["region"] != "test" {
		t.Fatalf("audit aliased = %#v", audit)
	}
}

var _ llm.Policy = (*fakePolicy)(nil)
var _ llm.Provider = (*fakeProvider)(nil)
var _ llm.ToolHost = (*fakeTools)(nil)
var _ llm.StreamSink = (*streamSink)(nil)

func nameList(prefix string, count int) []any {
	result := make([]any, count)
	for index := range result {
		result[index] = fmt.Sprintf("%s-%03d", prefix, index)
	}
	return result
}

func toolRequests(count int) []llm.ToolRequest {
	result := make([]llm.ToolRequest, count)
	for index := range result {
		result[index] = llm.ToolRequest{ID: fmt.Sprintf("call-%03d", index), Name: "search.read", Arguments: map[string]any{}}
	}
	return result
}

func provenanceAttributes(count, totalBytes int) map[string]string {
	result := make(map[string]string, count)
	keys := make([]string, count)
	keyBytes := 0
	for index := range keys {
		keys[index] = fmt.Sprintf("meta_%03d", index)
		keyBytes += len(keys[index])
	}
	remaining := totalBytes - keyBytes
	for index, key := range keys {
		entries := count - index
		length := 0
		if entries > 0 && remaining > 0 {
			length = remaining / entries
			if length > 512 {
				length = 512
			}
		}
		result[key] = strings.Repeat("x", length)
		remaining -= length
	}
	if remaining != 0 {
		panic("test provenance byte target cannot be represented")
	}
	return result
}

func bindingAuditProjectionBytes(t *testing.T, binding llm.ProviderBinding) int {
	t.Helper()
	encoded, err := json.Marshal(map[string]any{
		"profile":            binding.Profile,
		"provider":           binding.Provider,
		"model":              binding.Model,
		"binding_id":         binding.BindingID,
		"binding_revision":   binding.Revision,
		"binding_attributes": binding.Attributes,
	})
	if err != nil {
		t.Fatal(err)
	}
	return len(encoded)
}
