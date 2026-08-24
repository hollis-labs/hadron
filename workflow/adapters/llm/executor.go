package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
)

// Execute authorizes and binds a provider, constructs bounded typed context,
// runs the tool/repair loop, and returns only persistable classified values.
func (k *Kind) Execute(ctx context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	if k == nil || nilInterface(k.policy) || nilInterface(k.provider) || ctx == nil {
		return stepkind.StepResult{}, permanent("llm_invalid_invocation", "LLM invocation is invalid", errors.New("executor or context is unavailable"), nil)
	}
	if invocationErr := prepared.Invocation.Validate(); invocationErr != nil {
		return stepkind.StepResult{}, permanent("llm_invalid_invocation", "LLM invocation is invalid", invocationErr, nil)
	}
	parsed, configErr := parseConfig(prepared.Invocation.Config)
	if configErr != nil {
		return stepkind.StepResult{}, permanent("llm_invalid_config", "LLM step configuration is invalid", configErr, nil)
	}
	if err := ctx.Err(); err != nil {
		return stepkind.StepResult{}, contextFailure(err, nil)
	}
	callCtx, cancel := context.WithTimeout(ctx, parsed.Timeout)
	defer cancel()
	if deadline := prepared.Invocation.Deadline; !deadline.IsZero() {
		var deadlineCancel context.CancelFunc
		callCtx, deadlineCancel = context.WithDeadline(callCtx, deadline)
		defer deadlineCancel()
	}

	binding, authorizationErr := k.policy.Authorize(callCtx, PolicyRequest{Profile: parsed.Profile, Provider: parsed.Provider, Model: parsed.Model, Tools: append([]string(nil), parsed.Tools...), Budget: parsed.Budget, Streaming: parsed.Streaming})
	if authorizationErr != nil {
		if callCtx.Err() != nil {
			return stepkind.StepResult{}, contextFailure(callCtx.Err(), nil)
		}
		return stepkind.StepResult{}, permanent("llm_policy_denied", "LLM execution was denied by policy", authorizationErr, nil)
	}
	if callCtx.Err() != nil {
		return stepkind.StepResult{}, contextFailure(callCtx.Err(), nil)
	}
	if err := validateBinding(binding, parsed); err != nil {
		return stepkind.StepResult{}, permanent("llm_policy_binding", "LLM policy returned an invalid provider binding", err, nil)
	}
	binding = cloneBinding(binding)
	details := safeDetails(binding)
	state := executionState{budget: parsed.Budget}
	if accountingErr := state.consumeBinding(binding); accountingErr != nil {
		return stepkind.StepResult{}, permanent("llm_budget_exceeded", "LLM provider binding exhausted its output budget", accountingErr, details)
	}

	definitions, resolutionErr := k.resolveTools(callCtx, parsed.Tools)
	if resolutionErr != nil {
		if callCtx.Err() != nil {
			return stepkind.StepResult{}, contextFailure(callCtx.Err(), details)
		}
		return stepkind.StepResult{}, permanent("llm_tool_contract", "LLM tool declarations are unavailable", resolutionErr, details)
	}
	if callCtx.Err() != nil {
		return stepkind.StepResult{}, contextFailure(callCtx.Err(), details)
	}
	messages, contextValues, promptErr := buildPrompt(prepared.Invocation.Inputs, parsed)
	if promptErr != nil {
		return stepkind.StepResult{}, permanent("llm_input_invalid", "LLM typed inputs are invalid", promptErr, details)
	}

	var final ProviderResponse
	var calls []ToolCallRecord
	seenToolIDs := make(map[string]struct{})
	for iteration := 0; ; iteration++ {
		remaining, budgetErr := state.remaining()
		if budgetErr != nil {
			return stepkind.StepResult{}, permanent("llm_budget_exceeded", "LLM execution exhausted its aggregate budget", budgetErr, details)
		}
		request := ProviderRequest{Binding: cloneBinding(binding), Messages: cloneMessages(messages), Context: cloneValueSet(contextValues), Tools: cloneDefinitions(definitions), OutputSchema: cloneSchema(parsed.OutputSchema), Budget: remaining, Repair: false}
		response, callErr := k.complete(callCtx, request, parsed.Streaming, &state)
		if callErr != nil {
			return stepkind.StepResult{}, classifyProvider(callCtx, callErr, details)
		}
		if err := validateProviderResponseEnvelope(response); err != nil {
			return stepkind.StepResult{}, permanent("llm_model_result", "LLM model returned an invalid result", err, details)
		}
		if batchErr := state.preflightToolBatch(len(response.ToolRequests)); batchErr != nil {
			return stepkind.StepResult{}, permanent("llm_budget_exceeded", "LLM execution exceeded its tool-call budget", batchErr, details)
		}
		if accountingErr := state.consumeResponse(response); accountingErr != nil {
			if errors.Is(accountingErr, ErrBudgetExceeded) {
				return stepkind.StepResult{}, permanent("llm_budget_exceeded", "LLM execution exceeded its budget", accountingErr, details)
			}
			return stepkind.StepResult{}, permanent("llm_model_result", "LLM model returned invalid accounting or content", accountingErr, details)
		}
		if err := validateProviderResponse(response); err != nil {
			return stepkind.StepResult{}, permanent("llm_model_result", "LLM model returned an invalid result", err, details)
		}
		if len(response.ToolRequests) == 0 {
			final = response
			break
		}
		if response.StopReason != StopTool || iteration >= parsed.MaxToolIterations || len(parsed.Tools) == 0 {
			return stepkind.StepResult{}, permanent("llm_tool_limit", "LLM tool iteration limit was exceeded", ErrBudgetExceeded, details)
		}
		for _, requested := range response.ToolRequests {
			if _, duplicate := seenToolIDs[requested.ID]; duplicate {
				return stepkind.StepResult{}, permanent("llm_model_result", "LLM model reused a tool-call identity", ErrInvalidResult, details)
			}
			seenToolIDs[requested.ID] = struct{}{}
			if !containsExact(parsed.Tools, requested.Name) {
				return stepkind.StepResult{}, permanent("llm_tool_denied", "LLM requested a tool outside the exact allowlist", ErrToolDenied, withTool(details, requested.Name))
			}
			definition, found := toolDefinition(definitions, requested.Name)
			if !found {
				return stepkind.StepResult{}, permanent("llm_tool_denied", "LLM requested a tool without an exact trusted definition", ErrToolDenied, withTool(details, requested.Name))
			}
			if argumentsErr := validateToolArguments(requested, definition); argumentsErr != nil {
				return stepkind.StepResult{}, permanent("llm_tool_arguments_invalid", "LLM tool arguments do not satisfy the trusted schema", argumentsErr, withTool(details, requested.Name))
			}
			if state.usage.ToolCalls >= int64(parsed.Budget.MaxToolCalls) {
				return stepkind.StepResult{}, permanent("llm_budget_exceeded", "LLM execution exceeded its tool-call budget", ErrBudgetExceeded, details)
			}
			if callCtx.Err() != nil {
				return stepkind.StepResult{}, contextFailure(callCtx.Err(), details)
			}
			toolResponse, toolErr := k.tools.ExecuteTool(callCtx, ToolExecutionRequest{Allowed: append([]string(nil), parsed.Tools...), Request: cloneToolRequest(requested), Binding: cloneBinding(binding)})
			if toolErr == nil && callCtx.Err() != nil {
				toolErr = callCtx.Err()
			}
			var resultErr error
			var content any
			if toolErr == nil {
				if toolResponse.Tool != requested.Name || !containsExact(parsed.Tools, toolResponse.Tool) {
					resultErr = fmt.Errorf("%w: tool result identity does not match request", ErrInvalidResult)
				} else {
					content, resultErr = cloneJSON(toolResponse.Content)
				}
			}
			outcome := verification.ActivitySucceeded
			if toolErr != nil || resultErr != nil {
				outcome = verification.ActivityFailed
			}
			state.usage.ToolCalls++
			calls = append(calls, ToolCallRecord{Sequence: len(calls) + 1, Tool: requested.Name, Outcome: outcome})
			if activity := prepared.Invocation.Activity; activity != nil {
				if recordErr := activity.RecordToolCall(context.WithoutCancel(callCtx), verification.ToolCall{Server: "llm", Tool: requested.Name, Outcome: outcome}); recordErr != nil {
					return stepkind.StepResult{}, permanent("llm_activity_recording", "LLM tool activity could not be recorded", recordErr, details)
				}
			}
			if toolErr != nil {
				if callCtx.Err() != nil {
					return stepkind.StepResult{}, contextFailure(callCtx.Err(), details)
				}
				return stepkind.StepResult{}, classified("llm_tool_failed", "LLM tool execution failed", toolErr, withTool(details, requested.Name))
			}
			if resultErr != nil {
				return stepkind.StepResult{}, permanent("llm_tool_result", "LLM tool returned an invalid result", resultErr, withTool(details, requested.Name))
			}
			content = maskJSON(k.redactor, content)
			messages = append(messages, Message{Role: "assistant", Tool: requested.Name, ToolCallID: requested.ID, ToolRequest: ptrToolRequest(cloneToolRequest(requested))}, Message{Role: "tool", Tool: requested.Name, ToolCallID: requested.ID, ToolResult: content})
		}
	}

	output, raw, validationErr := k.terminalOutput(final, parsed.OutputSchema)
	if validationErr != nil && parsed.Repair == repairOnce {
		remaining, budgetErr := state.remaining()
		if budgetErr != nil {
			return stepkind.StepResult{}, permanent("llm_budget_exceeded", "LLM execution exhausted its aggregate budget", budgetErr, details)
		}
		evidence, evidenceErr := repairEvidence(final, k.redactor, remaining.MaxInputBytes)
		if errors.Is(evidenceErr, ErrBudgetExceeded) {
			return stepkind.StepResult{}, permanent("llm_budget_exceeded", "LLM execution exhausted its aggregate repair budget", evidenceErr, details)
		}
		if evidenceErr != nil {
			return stepkind.StepResult{}, permanent("llm_schema_failed", "LLM invalid output could not be represented safely for repair", evidenceErr, details)
		}
		repairMessages := append(cloneMessages(messages), Message{Role: "assistant", Content: evidence}, Message{Role: "user", Content: "Return only JSON that satisfies the supplied output schema."})
		request := ProviderRequest{Binding: cloneBinding(binding), Messages: repairMessages, Context: cloneValueSet(contextValues), OutputSchema: cloneSchema(parsed.OutputSchema), Budget: remaining, Repair: true}
		repaired, callErr := k.complete(callCtx, request, parsed.Streaming, &state)
		if callErr != nil {
			return stepkind.StepResult{}, classifyProvider(callCtx, callErr, details)
		}
		if err := validateProviderResponseEnvelope(repaired); err != nil {
			return stepkind.StepResult{}, permanent("llm_schema_repair", "LLM schema repair returned an invalid result", firstError(err, validationErr), details)
		}
		if batchErr := state.preflightToolBatch(len(repaired.ToolRequests)); batchErr != nil {
			return stepkind.StepResult{}, permanent("llm_budget_exceeded", "LLM schema repair exceeded its tool-call budget", batchErr, details)
		}
		if accountingErr := state.consumeResponse(repaired); accountingErr != nil {
			if errors.Is(accountingErr, ErrBudgetExceeded) {
				return stepkind.StepResult{}, permanent("llm_budget_exceeded", "LLM execution exceeded its budget", accountingErr, details)
			}
			return stepkind.StepResult{}, permanent("llm_schema_repair", "LLM schema repair returned an invalid result", accountingErr, details)
		}
		if err := validateProviderResponse(repaired); err != nil || len(repaired.ToolRequests) != 0 {
			return stepkind.StepResult{}, permanent("llm_schema_repair", "LLM schema repair returned an invalid result", firstError(err, validationErr), details)
		}
		output, raw, validationErr = k.terminalOutput(repaired, parsed.OutputSchema)
		final = repaired
	}
	if validationErr != nil {
		return stepkind.StepResult{}, permanent("llm_schema_failed", "LLM output does not satisfy its schema", validationErr, details)
	}
	outputs, outputErr := buildOutputs(prepared.Invocation.Identity, output, raw, calls, state.usage, final.StopReason, binding, state.audits)
	if outputErr != nil {
		return stepkind.StepResult{}, permanent("llm_output_invalid", "LLM typed output could not be persisted", outputErr, details)
	}
	return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: outputs}, nil
}

func (k *Kind) resolveTools(ctx context.Context, allowed []string) ([]ToolDefinition, error) {
	if len(allowed) == 0 {
		return nil, nil
	}
	if nilInterface(k.tools) {
		return nil, fmt.Errorf("%w: tool host is required", ErrInvalidOptions)
	}
	resolved, err := k.tools.ResolveTools(ctx, append([]string(nil), allowed...))
	if err != nil {
		return nil, err
	}
	byName := make(map[string]ToolDefinition, len(resolved))
	for _, definition := range resolved {
		if validateToolName(definition.Name) != nil || !containsExact(allowed, definition.Name) {
			return nil, fmt.Errorf("%w: resolved tool %q is outside allowlist", ErrToolDenied, definition.Name)
		}
		if _, duplicate := byName[definition.Name]; duplicate {
			return nil, fmt.Errorf("%w: duplicate resolved tool %q", ErrInvalidResult, definition.Name)
		}
		if !stableText(definition.Description, false) {
			return nil, fmt.Errorf("%w: invalid description for %q", ErrInvalidResult, definition.Name)
		}
		if err := values.ValidateSchema(definition.InputSchema); err != nil {
			return nil, fmt.Errorf("%w: invalid schema for %q: %w", ErrInvalidResult, definition.Name, err)
		}
		byName[definition.Name] = ToolDefinition{Name: definition.Name, Description: definition.Description, InputSchema: cloneSchema(definition.InputSchema)}
	}
	ordered := make([]ToolDefinition, 0, len(allowed))
	for _, name := range allowed {
		definition, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("%w: tool host omitted %q", ErrToolDenied, name)
		}
		ordered = append(ordered, definition)
	}
	return ordered, nil
}

func toolDefinition(definitions []ToolDefinition, name string) (ToolDefinition, bool) {
	for _, definition := range definitions {
		if definition.Name == name {
			return definition, true
		}
	}
	return ToolDefinition{}, false
}

func validateToolArguments(request ToolRequest, definition ToolDefinition) error {
	value, err := values.NewInline(request.Arguments, values.Metadata{Producer: values.Producer{Kind: "llm-tool-request", Reference: request.ID}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionNone})
	if err != nil {
		return err
	}
	return values.ValidateValueSchema(definition.InputSchema, value)
}

func buildPrompt(inputs values.ValueSet, parsed config) ([]Message, values.ValueSet, error) {
	messages := make([]Message, 0, len(parsed.Messages)+1)
	if parsed.System != "" {
		messages = append(messages, Message{Role: "system", Content: parsed.System})
	}
	for _, declaration := range parsed.Messages {
		content := declaration.Content
		if declaration.Input != "" {
			value, exists := inputs[declaration.Input]
			if !exists || value.Type != values.TypeString || value.Redaction == values.RedactionSecret {
				return nil, nil, fmt.Errorf("input message %q must be an inline non-secret string", declaration.Input)
			}
			content, _ = value.Inline.(string)
		}
		messages = append(messages, Message{Role: declaration.Role, Content: content})
	}
	selected := make(values.ValueSet, len(parsed.ContextInputs))
	for _, name := range parsed.ContextInputs {
		value, exists := inputs[name]
		if !exists {
			return nil, nil, fmt.Errorf("context input %q is missing", name)
		}
		if value.Type == values.TypeSecretRef || value.Redaction == values.RedactionSecret {
			return nil, nil, fmt.Errorf("context input %q cannot expose secret material", name)
		}
		selected[name] = cloneValue(value)
	}
	return messages, selected, nil
}

type executionState struct {
	budget      Budget
	inputBytes  int64
	outputBytes int64
	usage       Usage
	audits      []ProviderAudit
}

func (s *executionState) remaining() (Budget, error) {
	if s.inputBytes >= s.budget.MaxInputBytes || s.outputBytes >= s.budget.MaxOutputBytes || s.usage.TotalTokens >= s.budget.MaxTotalTokens || (s.budget.MaxCostMicrounits > 0 && s.usage.CostMicrounits >= s.budget.MaxCostMicrounits) {
		return Budget{}, ErrBudgetExceeded
	}
	result := s.budget
	result.MaxInputBytes -= s.inputBytes
	result.MaxOutputBytes -= s.outputBytes
	result.MaxTotalTokens -= s.usage.TotalTokens
	if result.MaxCostMicrounits > 0 {
		result.MaxCostMicrounits -= s.usage.CostMicrounits
	}
	result.MaxToolCalls -= int(s.usage.ToolCalls)
	return result, nil
}
func (s *executionState) consumeRequest(request ProviderRequest) error {
	encoded, err := json.Marshal(request)
	if err != nil {
		return err
	}
	next, addErr := checkedAdd(s.inputBytes, int64(len(encoded)))
	if addErr != nil || next > s.budget.MaxInputBytes {
		return ErrBudgetExceeded
	}
	s.inputBytes = next
	return nil
}
func (s *executionState) consumeBinding(binding ProviderBinding) error {
	encoded, err := json.Marshal(bindingAuditData(binding))
	if err != nil {
		return err
	}
	next, addErr := checkedAdd(s.outputBytes, int64(len(encoded)))
	if addErr != nil || next > s.budget.MaxOutputBytes {
		return ErrBudgetExceeded
	}
	s.outputBytes = next
	return nil
}
func (s *executionState) preflightToolBatch(count int) error {
	if count > maximumToolCalls {
		return ErrBudgetExceeded
	}
	remaining := int64(s.budget.MaxToolCalls) - s.usage.ToolCalls
	if remaining < 0 || int64(count) > remaining {
		return ErrBudgetExceeded
	}
	return nil
}
func (s *executionState) consumeResponse(response ProviderResponse) error {
	if err := validateProviderResponseEnvelope(response); err != nil {
		return err
	}
	stableAudit := ProviderAudit{RequestID: response.Audit.RequestID, Revision: response.Audit.Revision, Attributes: cloneStringMap(response.Audit.Attributes)}
	encoded, err := json.Marshal(struct {
		Output any           `json:"output"`
		Raw    string        `json:"raw_text"`
		Tools  []ToolRequest `json:"tool_requests"`
		Audit  ProviderAudit `json:"audit"`
	}{response.Output, response.RawText, response.ToolRequests, stableAudit})
	if err != nil {
		return err
	}
	nextOutputBytes, outputErr := checkedAdd(s.outputBytes, int64(len(encoded)))
	if outputErr != nil || nextOutputBytes > s.budget.MaxOutputBytes {
		return ErrBudgetExceeded
	}
	nextInputTokens, inputErr := checkedAdd(s.usage.InputTokens, response.Usage.InputTokens)
	nextOutputTokens, tokenErr := checkedAdd(s.usage.OutputTokens, response.Usage.OutputTokens)
	nextTotalTokens, totalErr := checkedAdd(s.usage.TotalTokens, response.Usage.TotalTokens)
	nextCost, costErr := checkedAdd(s.usage.CostMicrounits, response.Usage.CostMicrounits)
	nextRequests, requestErr := checkedAdd(s.usage.Requests, 1)
	if inputErr != nil || tokenErr != nil || totalErr != nil || costErr != nil || requestErr != nil || nextTotalTokens > s.budget.MaxTotalTokens || (s.budget.MaxCostMicrounits > 0 && nextCost > s.budget.MaxCostMicrounits) {
		return ErrBudgetExceeded
	}
	s.outputBytes = nextOutputBytes
	s.usage.InputTokens = nextInputTokens
	s.usage.OutputTokens = nextOutputTokens
	s.usage.TotalTokens = nextTotalTokens
	s.usage.CostMicrounits = nextCost
	s.usage.Requests = nextRequests
	s.audits = append(s.audits, stableAudit)
	return nil
}

func (u Usage) validate() error {
	total, addErr := checkedAdd(u.InputTokens, u.OutputTokens)
	if u.InputTokens < 0 || u.OutputTokens < 0 || u.TotalTokens < 0 || u.CostMicrounits < 0 || u.Requests != 0 || u.ToolCalls != 0 || addErr != nil || u.TotalTokens != total {
		return fmt.Errorf("%w: invalid usage accounting", ErrInvalidResult)
	}
	return nil
}

type boundedReceiver struct {
	mu      sync.Mutex
	ctx     context.Context
	state   *executionState
	writer  io.WriteCloser
	closed  bool
	failure error
}

func (r *boundedReceiver) Receive(ctx context.Context, chunk StreamChunk) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failure != nil {
		return r.failure
	}
	if ctx == nil || !utf8.ValidString(chunk.Text) {
		r.failure = fmt.Errorf("%w: invalid stream chunk", ErrInvalidResult)
		return r.failure
	}
	if err := r.ctx.Err(); err != nil {
		r.failure = err
		return err
	}
	if r.closed {
		return errors.New("LLM stream receiver is closed")
	}
	next, addErr := checkedAdd(r.state.outputBytes, int64(len(chunk.Text)))
	if addErr != nil || next > r.state.budget.MaxOutputBytes {
		r.failure = ErrBudgetExceeded
		return r.failure
	}
	r.state.outputBytes = next
	_, err := r.writer.Write([]byte(chunk.Text))
	if err != nil {
		if !errors.Is(err, ErrBudgetExceeded) {
			err = &ProviderError{Kind: ProviderInfrastructure, Retryable: true, Cause: err}
		}
		r.failure = err
	}
	return err
}

func (r *boundedReceiver) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return r.failure
	}
	r.closed = true
	return errors.Join(r.failure, r.writer.Close())
}

type streamSinkWriter struct {
	ctx  context.Context
	sink StreamSink
}

func (w *streamSinkWriter) Write(content []byte) (int, error) {
	if nilInterface(w.sink) {
		return len(content), nil
	}
	if err := w.sink.Receive(w.ctx, StreamChunk{Text: string(content)}); err != nil {
		return 0, err
	}
	return len(content), nil
}

func (k *Kind) complete(ctx context.Context, request ProviderRequest, streaming bool, state *executionState) (ProviderResponse, error) {
	if err := ctx.Err(); err != nil {
		return ProviderResponse{}, err
	}
	if err := state.consumeRequest(request); err != nil {
		return ProviderResponse{}, err
	}
	var receiver StreamReceiver
	var bounded *boundedReceiver
	if streaming {
		destination := &streamSinkWriter{ctx: ctx, sink: k.stream}
		bounded = &boundedReceiver{ctx: ctx, state: state, writer: k.redactor.Writer(destination)}
		receiver = bounded
	}
	response, err := k.provider.Complete(ctx, request, receiver)
	if bounded != nil {
		if closeErr := bounded.Close(); closeErr != nil {
			if !errors.Is(closeErr, ErrBudgetExceeded) && ctx.Err() == nil {
				closeErr = &ProviderError{Kind: ProviderInfrastructure, Retryable: true, Cause: closeErr}
			}
			err = errors.Join(err, closeErr)
		}
	}
	if err == nil && ctx.Err() != nil {
		return ProviderResponse{}, ctx.Err()
	}
	return response, err
}

func validateProviderResponseEnvelope(response ProviderResponse) error {
	if err := response.Usage.validate(); err != nil {
		return err
	}
	if err := validateAudit(response.Audit); err != nil {
		return err
	}
	if !response.HasOutput && response.Output != nil {
		return fmt.Errorf("%w: output payload requires has_output", ErrInvalidResult)
	}
	return nil
}

func validateProviderResponse(response ProviderResponse) error {
	if err := validateProviderResponseEnvelope(response); err != nil {
		return err
	}
	if !response.StopReason.Valid() {
		return fmt.Errorf("%w: invalid stop reason", ErrInvalidResult)
	}
	if len(response.ToolRequests) != 0 {
		if response.StopReason != StopTool || response.HasOutput || response.Output != nil || response.RawText != "" {
			return fmt.Errorf("%w: tool turn must contain only requests", ErrInvalidResult)
		}
		seen := map[string]struct{}{}
		for _, request := range response.ToolRequests {
			if !stableText(request.ID, true) || validateToolName(request.Name) != nil || request.Arguments == nil {
				return fmt.Errorf("%w: malformed tool request", ErrInvalidResult)
			}
			if _, duplicate := seen[request.ID]; duplicate {
				return fmt.Errorf("%w: duplicate tool request ID", ErrInvalidResult)
			}
			seen[request.ID] = struct{}{}
			if _, err := cloneJSON(request.Arguments); err != nil {
				return err
			}
		}
	} else if response.StopReason == StopTool {
		return fmt.Errorf("%w: tool stop requires requests", ErrInvalidResult)
	}
	return nil
}

func (k *Kind) terminalOutput(response ProviderResponse, schema graph.Schema) (any, string, error) {
	raw := k.redactor.MaskString(response.RawText)
	var output any
	var err error
	if response.HasOutput {
		output, err = cloneJSON(response.Output)
	} else if raw != "" {
		output, err = decodeExactJSON(raw)
	} else {
		return nil, raw, fmt.Errorf("%w: terminal response has no output", ErrInvalidResult)
	}
	if err != nil {
		return nil, raw, err
	}
	output = maskJSON(k.redactor, output)
	value, err := values.NewInline(output, values.Metadata{Producer: values.Producer{Kind: "llm", Reference: "schema-validation"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		return nil, raw, err
	}
	if err := values.ValidateValueSchema(schema, value); err != nil {
		return nil, raw, err
	}
	return output, raw, nil
}

const maximumRepairEvidenceBytes = 64 << 10

func repairEvidence(response ProviderResponse, redactor *values.Redactor, available int64) (string, error) {
	var content string
	if response.HasOutput {
		cloned, err := cloneJSON(response.Output)
		if err != nil {
			return "", err
		}
		encoded, err := json.Marshal(maskJSON(redactor, cloned))
		if err != nil {
			return "", err
		}
		content = string(encoded)
	} else {
		content = redactor.MaskString(response.RawText)
	}
	prefix := "Invalid output:\n"
	limit := int64(maximumRepairEvidenceBytes)
	if available < limit {
		limit = available
	}
	if limit <= int64(len(prefix)) {
		return "", ErrBudgetExceeded
	}
	contentLimit := int(limit) - len(prefix)
	if len(content) > contentLimit {
		content = content[:contentLimit]
		for !utf8.ValidString(content) {
			content = content[:len(content)-1]
		}
	}
	return prefix + content, nil
}

func validateBinding(binding ProviderBinding, parsed config) error {
	for _, field := range []struct{ name, value string }{{"profile", binding.Profile}, {"provider", binding.Provider}, {"model", binding.Model}, {"binding_id", binding.BindingID}} {
		name, value := field.name, field.value
		if !stableText(value, true) || len(value) > 512 || unsafeProvenanceValue(value) {
			return fmt.Errorf("invalid binding %s", name)
		}
	}
	if parsed.Profile != binding.Profile || (parsed.Provider != "" && parsed.Provider != binding.Provider) || (parsed.Model != "" && parsed.Model != binding.Model) {
		return fmt.Errorf("binding does not match requested provider identity")
	}
	if !stableText(binding.Revision, false) || len(binding.Revision) > 512 || unsafeProvenanceValue(binding.Revision) {
		return fmt.Errorf("invalid binding revision")
	}
	return validateStringMap(binding.Attributes)
}
func validateAudit(audit ProviderAudit) error {
	if !stableText(audit.RequestID, false) || !stableText(audit.Revision, false) || len(audit.RequestID) > 512 || len(audit.Revision) > 512 || unsafeProvenanceValue(audit.RequestID) || unsafeProvenanceValue(audit.Revision) {
		return fmt.Errorf("%w: invalid provider audit", ErrInvalidResult)
	}
	return validateStringMap(audit.Attributes)
}
func validateStringMap(input map[string]string) error {
	if len(input) > MaximumProvenanceAttributeCount {
		return fmt.Errorf("%w: invalid stable metadata", ErrInvalidResult)
	}
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	aggregateBytes := 0
	for _, key := range keys {
		value := input[key]
		if !stableText(key, true) || !stableText(value, false) || len(key) > 128 || len(value) > 512 || sensitiveMetadataKey(key) || unsafeProvenanceValue(value) {
			return fmt.Errorf("%w: invalid stable metadata", ErrInvalidResult)
		}
		aggregateBytes += len(key) + len(value)
		if aggregateBytes > MaximumProvenanceAttributeBytes {
			return fmt.Errorf("%w: invalid stable metadata", ErrInvalidResult)
		}
	}
	return nil
}

func checkedAdd(left, right int64) (int64, error) {
	if left < 0 || right < 0 || right > math.MaxInt64-left {
		return 0, ErrBudgetExceeded
	}
	return left + right, nil
}

func sensitiveMetadataKey(key string) bool {
	normalized := strings.ToLower(key)
	normalized = strings.NewReplacer("-", "", "_", "", ".", "", " ", "").Replace(normalized)
	for _, marker := range []string{"authorization", "credential", "password", "secret", "token", "apikey", "accesskey", "privatekey", "clientkey", "cookie", "bearer"} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func unsafeProvenanceValue(value string) bool {
	lower := strings.ToLower(value)
	if strings.Contains(lower, "secret://") || credentialAuthMarker.MatchString(value) || credentialAssignment.MatchString(value) {
		return true
	}
	if !uriShaped(value) {
		return false
	}
	parsed, err := url.ParseRequestURI(value)
	if err != nil || parsed.Scheme == "" {
		return true
	}
	if strings.Contains(value, "://") && parsed.Host == "" {
		return true
	}
	return parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != ""
}

var (
	credentialAuthMarker = regexp.MustCompile(`(?i)(^|[^a-z0-9])(bearer|basic)([^a-z0-9]|$)`)
	credentialAssignment = regexp.MustCompile(`(?i)(^|[^a-z0-9])(token|password|api[-_. ]?key|signature)[[:space:]]*[:=]`)
)

func uriShaped(value string) bool {
	separator := strings.IndexByte(value, ':')
	if separator < 1 {
		return false
	}
	for index := 0; index < separator; index++ {
		current := value[index]
		if (current >= 'a' && current <= 'z') || (current >= 'A' && current <= 'Z') || (index > 0 && current >= '0' && current <= '9') || (index > 0 && (current == '+' || current == '-' || current == '.')) {
			continue
		}
		return false
	}
	return true
}

func buildOutputs(identity stepkind.InvocationIdentity, output any, raw string, calls []ToolCallRecord, usage Usage, stop StopReason, binding ProviderBinding, providerAudits []ProviderAudit) (values.ValueSet, error) {
	referenceBytes, _ := json.Marshal(identity)
	producer := values.Producer{Kind: "llm", Reference: values.SHA256Digest(referenceBytes)}
	metadata := func(name, media string) values.Metadata {
		return values.Metadata{Producer: values.Producer{Kind: producer.Kind, Reference: producer.Reference, Output: name}, MediaType: media, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}
	}
	callData := make([]any, len(calls))
	for index, call := range calls {
		callData[index] = map[string]any{"sequence": json.Number(strconv.Itoa(call.Sequence)), "tool": call.Tool, "outcome": string(call.Outcome)}
	}
	usageData := map[string]any{"input_tokens": json.Number(strconv.FormatInt(usage.InputTokens, 10)), "output_tokens": json.Number(strconv.FormatInt(usage.OutputTokens, 10)), "total_tokens": json.Number(strconv.FormatInt(usage.TotalTokens, 10)), "cost_microunits": json.Number(strconv.FormatInt(usage.CostMicrounits, 10)), "requests": json.Number(strconv.FormatInt(usage.Requests, 10)), "tool_calls": json.Number(strconv.FormatInt(usage.ToolCalls, 10))}
	providerCalls := make([]any, len(providerAudits))
	for index, audit := range providerAudits {
		providerCalls[index] = map[string]any{"request_id": audit.RequestID, "revision": audit.Revision, "attributes": stringMapAny(audit.Attributes)}
	}
	auditData := bindingAuditData(binding)
	auditData["provider_calls"] = providerCalls
	data := map[string]any{OutputValue: output, OutputRawText: raw, OutputToolCalls: callData, OutputUsage: usageData, OutputStop: string(stop), OutputAudit: auditData}
	result := make(values.ValueSet, len(data))
	for _, name := range []string{OutputValue, OutputRawText, OutputToolCalls, OutputUsage, OutputStop, OutputAudit} {
		value, err := values.NewInline(data[name], metadata(name, "application/json"))
		if err != nil {
			return nil, err
		}
		result[name] = value
	}
	if err := values.ValidatePersistableSet(result); err != nil {
		return nil, err
	}
	if err := values.ValidateValueSetSchema(outputSchema(), result); err != nil {
		return nil, err
	}
	return result, nil
}

func bindingAuditData(binding ProviderBinding) map[string]any {
	return map[string]any{"profile": binding.Profile, "provider": binding.Provider, "model": binding.Model, "binding_id": binding.BindingID, "binding_revision": binding.Revision, "binding_attributes": stringMapAny(binding.Attributes)}
}

func decodeExactJSON(text string) (any, error) {
	decoder := json.NewDecoder(bytes.NewBufferString(text))
	decoder.UseNumber()
	var output any
	if err := decoder.Decode(&output); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("trailing JSON")
	}
	return output, nil
}
func cloneJSON(input any) (any, error) {
	if _, err := values.DigestInline(input); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	return decodeExactJSON(string(encoded))
}
func maskJSON(redactor *values.Redactor, input any) any {
	switch current := input.(type) {
	case string:
		return redactor.MaskString(current)
	case []any:
		result := make([]any, len(current))
		for i, v := range current {
			result[i] = maskJSON(redactor, v)
		}
		return result
	case map[string]any:
		result := make(map[string]any, len(current))
		for k, v := range current {
			result[k] = maskJSON(redactor, v)
		}
		return result
	default:
		return input
	}
}
func cloneValueSet(input values.ValueSet) values.ValueSet {
	result := make(values.ValueSet, len(input))
	for k, v := range input {
		result[k] = cloneValue(v)
	}
	return result
}
func cloneValue(v values.Value) values.Value {
	result := v
	if v.Artifact != nil {
		artifactCopy := *v.Artifact
		result.Artifact = &artifactCopy
	}
	if v.SecretRef != nil {
		secretRefCopy := *v.SecretRef
		result.SecretRef = &secretRefCopy
	}
	if clone, err := cloneJSON(v.Inline); err == nil {
		result.Inline = clone
	}
	return result
}
func cloneMessages(input []Message) []Message {
	result := make([]Message, len(input))
	for i, v := range input {
		result[i] = v
		if v.ToolRequest != nil {
			requestCopy := cloneToolRequest(*v.ToolRequest)
			result[i].ToolRequest = &requestCopy
		}
		if clone, err := cloneJSON(v.ToolResult); err == nil {
			result[i].ToolResult = clone
		}
	}
	return result
}
func cloneDefinitions(input []ToolDefinition) []ToolDefinition {
	result := make([]ToolDefinition, len(input))
	for i, v := range input {
		result[i] = v
		result[i].InputSchema = cloneSchema(v.InputSchema)
	}
	return result
}
func cloneSchema(input graph.Schema) graph.Schema {
	cloned, err := cloneJSON(input)
	if err != nil {
		return nil
	}
	object, _ := cloned.(map[string]any)
	return graph.Schema(object)
}
func cloneBinding(input ProviderBinding) ProviderBinding {
	input.Attributes = cloneStringMap(input.Attributes)
	return input
}
func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for k, v := range input {
		result[k] = v
	}
	return result
}
func cloneToolRequest(input ToolRequest) ToolRequest {
	input.Arguments, _ = cloneObject(input.Arguments)
	return input
}
func ptrToolRequest(input ToolRequest) *ToolRequest { return &input }
func containsExact(input []string, value string) bool {
	for _, item := range input {
		if item == value {
			return true
		}
	}
	return false
}
func safeDetails(binding ProviderBinding) map[string]string {
	return map[string]string{"profile": binding.Profile, "provider": binding.Provider, "model": binding.Model, "binding_id": binding.BindingID}
}
func withTool(input map[string]string, tool string) map[string]string {
	result := cloneStringMap(input)
	result["tool"] = tool
	return result
}
func stringMapAny(input map[string]string) map[string]any {
	result := make(map[string]any, len(input))
	for k, v := range input {
		result[k] = v
	}
	return result
}
func firstError(input, errorFallback error) error {
	if input != nil {
		return input
	}
	return errorFallback
}

func permanent(code, message string, cause error, details map[string]string) error {
	return &stepkind.ExecutionError{Code: code, Message: message, Classification: stepkind.RetryPermanent, Details: details, Cause: cause}
}
func classified(code, message string, cause error, details map[string]string) error {
	classification := stepkind.ClassifyError(cause)
	if classification == stepkind.RetryUnspecified {
		classification = stepkind.RetryPermanent
	}
	return &stepkind.ExecutionError{Code: code, Message: message, Classification: classification, Details: details, Cause: cause}
}
func classifyProvider(ctx context.Context, cause error, details map[string]string) error {
	if err := ctx.Err(); err != nil {
		return contextFailure(err, details)
	}
	if errors.Is(cause, ErrBudgetExceeded) {
		return permanent("llm_budget_exceeded", "LLM execution exceeded its budget", cause, details)
	}
	code, message := "llm_provider_error", "LLM provider call failed"
	var providerError *ProviderError
	if errors.As(cause, &providerError) && providerError.Kind.Valid() {
		switch providerError.Kind {
		case ProviderUnavailable:
			code, message = "llm_provider_unavailable", "LLM provider is unavailable"
		case ProviderRateLimited:
			code, message = "llm_provider_rate_limited", "LLM provider rate limit was reached"
		case ProviderRejected:
			code, message = "llm_provider_rejected", "LLM provider rejected the request"
		case ProviderInfrastructure:
			code, message = "llm_infrastructure_error", "LLM provider infrastructure failed"
		}
	}
	return classified(code, message, cause, details)
}
func contextFailure(cause error, details map[string]string) error {
	code, message, class := "llm_canceled", "LLM execution was canceled", stepkind.RetryPermanent
	if errors.Is(cause, context.DeadlineExceeded) {
		code, message, class = "llm_timeout", "LLM execution timed out", stepkind.Retryable
	}
	return &stepkind.ExecutionError{Code: code, Message: message, Classification: class, Details: details, Cause: cause}
}

var _ stepkind.StepKind = (*Kind)(nil)
