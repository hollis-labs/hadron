package mcptools

import (
	"context"
	"errors"

	"github.com/hollis-labs/hadron/internal/appworkflow"
)

type args = map[string]any

func (s *surface) validate(ctx context.Context, a args) (any, error) {
	if err := unavailable(s.deps.Operations); err != nil {
		return nil, err
	}
	r, err := s.deps.Operations.ValidateWorkflow(ctx, appworkflow.ValidateWorkflowRequest{Definition: refFromArgs(a), Identity: identity()})
	return workflowResult(r, err)
}

func (s *surface) run(ctx context.Context, a args) (any, error) {
	if err := unavailable(s.deps.Operations); err != nil {
		return nil, err
	}
	inputs := map[string]any{}
	if nested, ok := a["inputs"]; ok {
		inputs, ok = nested.(map[string]any)
		if !ok {
			return nil, workflowFailure(errors.New("workflow inputs must be an object"))
		}
	}
	id := argString(a, "run_id", "")
	if id == "" {
		id = s.generated("mcp-run")
	}
	key := argString(a, "idempotency_key", "")
	if key == "" {
		key = s.generated("mcp-start")
	}
	started, err := s.deps.Operations.RunWorkflow(ctx, appworkflow.RunWorkflowRequest{
		RunID: appworkflow.RunID(id), Definition: refFromArgs(a), Inputs: inputs, IdempotencyKey: key,
		Identity: identity(), Confirmed: argBool(a, "confirmed", false),
	})
	return workflowStartResult(appworkflow.RunID(id), started, err)
}

func (s *surface) inspect(ctx context.Context, a args) (any, error) {
	if err := unavailable(s.deps.Operations); err != nil {
		return nil, err
	}
	r, err := s.deps.Operations.InspectWorkflowRun(ctx, appworkflow.InspectWorkflowRunRequest{
		RunID: runID(a), Identity: identity(), Display: display(a),
		NodeLimit: 100, AttemptLimit: 100, EventLimit: 100, ValueLimit: 100, ResourceLimit: 100, ActivationLimit: 100,
	})
	return workflowResult(r, err)
}

func (s *surface) cancel(ctx context.Context, a args) (any, error) {
	if err := unavailable(s.deps.Operations); err != nil {
		return nil, err
	}
	r, err := s.deps.Operations.CancelWorkflowRun(ctx, appworkflow.CancelWorkflowRunRequest{
		RunID: runID(a), Identity: identity(), IdempotencyKey: argString(a, "idempotency_key", ""), Reason: argString(a, "reason", ""),
	})
	return workflowResult(r, err)
}

func (s *surface) events(ctx context.Context, a args) (any, error) {
	if err := unavailable(s.deps.Reads); err != nil {
		return nil, err
	}
	limit, err := exactWorkflowLimitArgument(a, 100)
	if err != nil {
		return nil, workflowFailure(err)
	}
	waitMS, err := exactWorkflowIntegerArgument(a, "wait_ms", 0)
	if err != nil {
		return nil, workflowFailure(err)
	}
	r, err := s.deps.Reads.FetchWorkflowEvents(ctx, appworkflow.WorkflowRunReadRequest{
		RunID: runID(a), Identity: identity(), Display: display(a), EventLimit: boundedWorkflowLimit(limit),
		After: argString(a, "after", ""), WaitMS: waitMS,
	})
	return workflowResult(r, err)
}

func (s *surface) resume(ctx context.Context, a args) (any, error) {
	if err := unavailable(s.deps.Operations); err != nil {
		return nil, err
	}
	source := appworkflow.WorkflowWakeCallback
	switch argString(a, "wake_source", "callback") {
	case "callback":
	case "gate":
		source = appworkflow.WorkflowWakeGate
	case "message":
		source = appworkflow.WorkflowWakeMessage
	default:
		return nil, workflowFailure(errors.New("wake_source must be callback, gate or message"))
	}
	payload, err := workflowValueArgument(a, "payload")
	if err != nil {
		return nil, workflowFailure(err)
	}
	r, err := s.deps.Operations.ResumeWorkflowRun(ctx, appworkflow.ResumeWorkflowRunRequest{
		RunID: runID(a), Identity: identity(), WaitID: appworkflow.WaitID(argString(a, "wait_id", "")),
		Correlation: argString(a, "correlation", ""), Token: argString(a, "token", ""), WakeSource: source,
		Payload: payload, IdempotencyKey: argString(a, "idempotency_key", ""),
	})
	return workflowResult(r, err)
}

func (s *surface) signal(ctx context.Context, a args) (any, error) {
	if err := unavailable(s.deps.Signals); err != nil {
		return nil, err
	}
	payload, err := workflowValueArgument(a, "payload")
	if err != nil {
		return nil, workflowFailure(err)
	}
	r, err := s.deps.Signals.SignalWorkflowRun(ctx, appworkflow.SignalWorkflowRunRequest{
		RunID: runID(a), Name: argString(a, "name", ""), Correlation: argString(a, "correlation", ""), Payload: payload,
		IdempotencyKey: argString(a, "idempotency_key", ""), Identity: identity(), Confirmed: argBool(a, "confirmed", false),
	})
	return workflowResult(r, err)
}
