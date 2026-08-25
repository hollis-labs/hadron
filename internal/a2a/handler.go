package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	a2aSourceAuthority    = "a2a"
	maximumTaskInputBytes = 2 << 20
	taskNodeLimit         = 256
	taskAttemptLimit      = 256
	taskEventLimit        = 256
	taskValueLimit        = 256
)

type CorrelationService interface {
	Put(context.Context, appworkflow.IdentityRequest, hoststate.A2ATaskCorrelation) (hoststate.A2ATaskCorrelation, workflowruntime.IdempotencyOutcome, error)
	Get(context.Context, appworkflow.IdentityRequest, string, appworkflow.A2ATaskOperation) (hoststate.A2ATaskCorrelation, error)
}

type Options struct {
	Correlations CorrelationService
	Workflows    appworkflow.WorkflowOperations
	Reads        appworkflow.WorkflowRunReadOperations
}

// Handler is a bounded A2A transport adapter. It owns no workflow lifecycle;
// every state and control operation is projected through appworkflow.
type Handler struct {
	correlations CorrelationService
	workflows    appworkflow.WorkflowOperations
	reads        appworkflow.WorkflowRunReadOperations
}

func NewHandler(options Options) (*Handler, error) {
	if nilInterface(options.Correlations) || nilInterface(options.Workflows) || nilInterface(options.Reads) {
		return nil, errors.New("A2A correlations, workflow operations, and workflow reads are required")
	}
	return &Handler{correlations: options.Correlations, workflows: options.Workflows, reads: options.Reads}, nil
}

func (h *Handler) SubmitTask(ctx context.Context, request TaskRequest) (*TaskResponse, error) {
	if err := h.ready(ctx); err != nil {
		return nil, err
	}
	if err := hoststate.ValidateA2ADefinition(request.Skill); err != nil {
		return nil, invalidRequest(err)
	}
	if err := hoststate.ValidatePublicText(request.IdempotencyKey, 512, true); err != nil {
		return nil, invalidRequest(errors.New("A2A idempotency key is invalid"))
	}
	if request.RunScope != nil {
		if err := request.RunScope.Validate(); err != nil {
			return nil, invalidRequest(err)
		}
	}
	if request.ExecutionTarget != nil {
		if err := request.ExecutionTarget.Validate(); err != nil {
			return nil, invalidRequest(err)
		}
	}
	inputs, inputDigest, err := canonicalInputs(request.Input)
	if err != nil {
		return nil, invalidRequest(err)
	}
	taskID := request.ID
	if taskID != "" {
		if validationErr := hoststate.ValidateA2ATaskID(taskID); validationErr != nil {
			return nil, invalidRequest(validationErr)
		}
	}
	requestDigest, err := taskIntentDigest(request, inputDigest)
	if err != nil {
		return nil, invalidRequest(err)
	}
	identity := taskIdentity(request.RunScope, request.ExecutionTarget)
	correlation, _, err := h.correlations.Put(ctx, identity, hoststate.A2ATaskCorrelation{
		TaskID: taskID, Definition: request.Skill,
		RequestDigest: requestDigest, IdempotencyKey: request.IdempotencyKey,
	})
	if err != nil {
		return nil, err
	}
	result, err := h.workflows.RunWorkflow(ctx, appworkflow.RunWorkflowRequest{
		RunID: correlation.RunID, Definition: correlation.Definition, Inputs: inputs,
		IdempotencyKey: correlation.HostStartKey, Identity: identity, Confirmed: request.Confirmed,
	})
	response := taskFromStart(correlation, result)
	return response, err
}

func (h *Handler) GetTask(ctx context.Context, taskID string) (*TaskResponse, error) {
	if err := h.ready(ctx); err != nil {
		return nil, err
	}
	identity := taskIdentity(nil, nil)
	correlation, err := h.correlations.Get(ctx, identity, taskID, appworkflow.A2ATaskInspect)
	if err != nil {
		return nil, err
	}
	identity = correlatedOwnerIdentity(correlation)
	inspection, err := h.workflows.InspectWorkflowRun(ctx, appworkflow.InspectWorkflowRunRequest{
		RunID: correlation.RunID, Identity: identity, NodeLimit: taskNodeLimit,
		AttemptLimit: taskAttemptLimit, EventLimit: taskEventLimit, ValueLimit: taskValueLimit,
	})
	if err != nil {
		if workflowTemporarilyUnavailable(err) {
			return pendingTask(correlation), nil
		}
		return nil, err
	}
	readRequest := appworkflow.WorkflowRunReadRequest{
		RunID: correlation.RunID, Identity: identity, NodeLimit: taskNodeLimit,
		AttemptLimit: taskAttemptLimit, EventLimit: taskEventLimit, ValueLimit: taskValueLimit,
	}
	waits, err := h.reads.ListWorkflowWaits(ctx, readRequest)
	if err != nil {
		return nil, err
	}
	valueSets, err := h.reads.FetchWorkflowValues(ctx, readRequest)
	if err != nil {
		return nil, err
	}
	events, err := h.reads.FetchWorkflowEvents(ctx, readRequest)
	if err != nil {
		return nil, err
	}
	response := &TaskResponse{
		ID: correlation.TaskID, RunID: correlation.RunID, Definition: correlation.Definition,
		Available: true, Status: taskStatus(inspection.Run.Status), Waits: waits.Waits,
		Values: valueSets.Values, Events: events.Events,
		Truncated: TaskTruncation{Waits: waits.Truncated, Values: valueSets.Truncated, Events: events.Truncated},
	}
	if inspection.Run.Status == workflowruntime.RunSucceeded && inspection.Run.Outputs != nil {
		for _, valueSet := range valueSets.Values {
			if valueSet.Ref == *inspection.Run.Outputs {
				response.Result = &TaskResult{OutputType: "application/json", Output: valueSet.Values}
				break
			}
		}
		if response.Result == nil {
			response.Omissions = append(response.Omissions, "terminal_outputs_unavailable_within_value_bound")
		}
	}
	return response, nil
}

func (h *Handler) CancelTask(ctx context.Context, taskID string, request CancelTaskRequest) (*TaskResponse, error) {
	if err := h.ready(ctx); err != nil {
		return nil, err
	}
	if err := hoststate.ValidatePublicText(request.IdempotencyKey, 512, true); err != nil {
		return nil, invalidRequest(errors.New("A2A cancel idempotency key is invalid"))
	}
	identity := taskIdentity(nil, nil)
	correlation, err := h.correlations.Get(ctx, identity, taskID, appworkflow.A2ATaskCancel)
	if err != nil {
		return nil, err
	}
	identity = correlatedOwnerIdentity(correlation)
	_, err = h.workflows.CancelWorkflowRun(ctx, appworkflow.CancelWorkflowRunRequest{RunID: correlation.RunID, Identity: identity, IdempotencyKey: request.IdempotencyKey, Reason: request.Reason})
	if err != nil {
		return nil, err
	}
	return h.GetTask(ctx, taskID)
}

func (h *Handler) ResumeTask(ctx context.Context, taskID string, request ResumeTaskRequest) (*ResumeTaskResponse, error) {
	if err := h.ready(ctx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(string(request.WaitID)) == "" || strings.TrimSpace(request.Correlation) == "" || !request.WakeSource.Valid() {
		return nil, invalidRequest(errors.New("A2A resume requires a wait, correlation, and wake source"))
	}
	if err := request.Payload.Validate(); err != nil {
		return nil, invalidRequest(fmt.Errorf("A2A resume payload: %w", err))
	}
	if err := hoststate.ValidatePublicText(request.IdempotencyKey, 512, true); err != nil {
		return nil, invalidRequest(errors.New("A2A resume idempotency key is invalid"))
	}
	identity := taskIdentity(nil, nil)
	correlation, err := h.correlations.Get(ctx, identity, taskID, appworkflow.A2ATaskResume)
	if err != nil {
		return nil, err
	}
	result, err := h.workflows.ResumeWorkflowRun(ctx, appworkflow.ResumeWorkflowRunRequest{
		RunID: correlation.RunID, Identity: identity, WaitID: request.WaitID,
		Correlation: request.Correlation, Token: request.Token, WakeSource: request.WakeSource,
		Payload: request.Payload, IdempotencyKey: request.IdempotencyKey,
	})
	if err != nil {
		return nil, err
	}
	return &ResumeTaskResponse{Resume: result}, nil
}

func (h *Handler) ready(ctx context.Context) error {
	if ctx == nil || h == nil || nilInterface(h.correlations) || nilInterface(h.workflows) || nilInterface(h.reads) {
		return errors.New("A2A workflow handler is not initialized")
	}
	return ctx.Err()
}

func canonicalInputs(input map[string]any) (map[string]any, string, error) {
	if input == nil {
		input = map[string]any{}
	}
	encoded, err := json.Marshal(input)
	if err != nil || len(encoded) > maximumTaskInputBytes {
		return nil, "", errors.New("A2A task input is invalid or exceeds the supported bound")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var result map[string]any
	if decodeErr := decoder.Decode(&result); decodeErr != nil {
		return nil, "", errors.New("A2A task input is invalid")
	}
	if decodeErr := decoder.Decode(&struct{}{}); !errors.Is(decodeErr, io.EOF) {
		return nil, "", errors.New("A2A task input has trailing data")
	}
	digest, err := values.DigestInline(result)
	if err != nil {
		return nil, "", fmt.Errorf("A2A task input: %w", err)
	}
	return result, digest, nil
}

func taskIntentDigest(request TaskRequest, inputDigest string) (string, error) {
	intent := struct {
		Definition     any                                `json:"definition"`
		InputDigest    string                             `json:"input_digest"`
		IdempotencyKey string                             `json:"idempotency_key"`
		RunScope       *hoststate.RunScopeSelector        `json:"run_scope,omitempty"`
		Target         *hoststate.ExecutionTargetSelector `json:"execution_target,omitempty"`
	}{request.Skill, inputDigest, request.IdempotencyKey, request.RunScope, request.ExecutionTarget}
	encoded, err := json.Marshal(intent)
	if err != nil {
		return "", errors.New("encode A2A task intent")
	}
	return values.SHA256Digest(encoded), nil
}

func taskIdentity(scope *hoststate.RunScopeSelector, target *hoststate.ExecutionTargetSelector) appworkflow.IdentityRequest {
	return appworkflow.IdentityRequest{SourceAuthority: a2aSourceAuthority, RunScope: scope, ExecutionTarget: target}
}

func correlatedOwnerIdentity(correlation hoststate.A2ATaskCorrelation) appworkflow.IdentityRequest {
	scope := hoststate.RunScopeSelector{Version: correlation.Owner.RunScope.Version, Kind: correlation.Owner.RunScope.Kind, ID: correlation.Owner.RunScope.ID}
	request := taskIdentity(&scope, nil)
	if correlation.Owner.ExecutionTarget != nil {
		target := hoststate.ExecutionTargetSelector{Version: correlation.Owner.ExecutionTarget.Version, ID: correlation.Owner.ExecutionTarget.ID}
		request.ExecutionTarget = &target
	}
	return request
}

func taskFromStart(correlation hoststate.A2ATaskCorrelation, result appworkflow.StartRunResult) *TaskResponse {
	response := &TaskResponse{ID: correlation.TaskID, RunID: correlation.RunID, Definition: correlation.Definition, Outcome: result.Outcome, Status: TaskStatus{State: "submitted", Message: "workflow admission is pending"}}
	if result.Run != nil {
		response.Available = true
		response.Status = taskStatus(result.Run.Status)
	}
	if result.RenderedOutputs != nil {
		response.Result = &TaskResult{OutputType: "application/json", Output: result.RenderedOutputs}
	}
	return response
}

func pendingTask(correlation hoststate.A2ATaskCorrelation) *TaskResponse {
	return &TaskResponse{ID: correlation.TaskID, RunID: correlation.RunID, Definition: correlation.Definition, Available: false, Status: TaskStatus{State: "submitted", Message: "workflow admission or inspection is temporarily unavailable"}, Omissions: []string{"run_not_available"}}
}

func taskStatus(status workflowruntime.RunStatus) TaskStatus {
	switch status {
	case workflowruntime.RunPending:
		return TaskStatus{State: "submitted", Message: "workflow run is pending"}
	case workflowruntime.RunRunning:
		return TaskStatus{State: "working", Message: "workflow run is executing"}
	case workflowruntime.RunWaiting:
		return TaskStatus{State: "input-required", Message: "workflow run is waiting for authorized input"}
	case workflowruntime.RunSucceeded:
		return TaskStatus{State: "completed", Message: "workflow run completed"}
	case workflowruntime.RunFailed, workflowruntime.RunTimedOut, workflowruntime.RunCrashed:
		return TaskStatus{State: "failed", Message: "workflow run failed"}
	case workflowruntime.RunCanceled:
		return TaskStatus{State: "canceled", Message: "workflow run was canceled"}
	default:
		return TaskStatus{State: "working", Message: "workflow status is unavailable"}
	}
}

func workflowTemporarilyUnavailable(err error) bool {
	if errors.Is(err, appworkflow.ErrHostNotReady) {
		return true
	}
	code := appworkflow.SafeWorkflowOperationError(err, nil).Code
	return code == appworkflow.WorkflowErrorCodeNotFound || code == appworkflow.WorkflowErrorCodeUnavailable
}

func invalidRequest(err error) error {
	return fmt.Errorf("%w: %w", appworkflow.ErrWorkflowInvalidRequest, err)
}

func nilInterface(input any) bool {
	if input == nil {
		return true
	}
	value := reflect.ValueOf(input)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
