package appworkflow

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

const maximumRunAuthorizationDepth = 64

// WorkflowOperations is the shared application-service contract used by
// transports. Transports collect caller intent; this boundary authenticates,
// authorizes, resolves, evaluates policy, and performs durable operations.
type WorkflowOperations interface {
	ValidateWorkflow(context.Context, ValidateWorkflowRequest) (ValidateWorkflowResult, error)
	ExplainWorkflow(context.Context, ExplainWorkflowRequest) (StartRunResult, error)
	RunWorkflow(context.Context, RunWorkflowRequest) (StartRunResult, error)
	InspectWorkflowRun(context.Context, InspectWorkflowRunRequest) (rundiagnostics.Result, error)
	CancelWorkflowRun(context.Context, CancelWorkflowRunRequest) (CancelWorkflowRunResult, error)
	ResumeWorkflowRun(context.Context, ResumeWorkflowRunRequest) (ResumeWorkflowRunResult, error)
	RerunWorkflow(context.Context, RerunWorkflowRequest) (RerunWorkflowResult, error)
}

type RunID = workflowruntime.RunID
type WaitID = workflowruntime.WaitID

type ValidateWorkflowRequest struct {
	Definition graph.DefinitionRef `json:"definition"`
	Identity   IdentityRequest     `json:"identity"`
}

type ValidateWorkflowResult struct {
	Definition  graph.DefinitionRef      `json:"definition"`
	Plan        *workflowruntime.PlanRef `json:"plan,omitempty"`
	Diagnostics []diagnostic.Diagnostic  `json:"diagnostics"`
}

type ExplainWorkflowRequest struct {
	RunID          workflowruntime.RunID `json:"run_id"`
	Definition     graph.DefinitionRef   `json:"definition"`
	Inputs         map[string]any        `json:"inputs,omitempty"`
	IdempotencyKey string                `json:"idempotency_key"`
	Identity       IdentityRequest       `json:"identity"`
	Confirmed      bool                  `json:"confirmed,omitempty"`
}

type RunWorkflowRequest struct {
	RunID          workflowruntime.RunID `json:"run_id"`
	Definition     graph.DefinitionRef   `json:"definition"`
	Inputs         map[string]any        `json:"inputs,omitempty"`
	IdempotencyKey string                `json:"idempotency_key"`
	Identity       IdentityRequest       `json:"identity"`
	Confirmed      bool                  `json:"confirmed,omitempty"`
	DryRun         bool                  `json:"dry_run,omitempty"`
	Pins           []hoststate.StartPin  `json:"pins,omitempty"`
}

type InspectWorkflowRunRequest struct {
	RunID           workflowruntime.RunID `json:"run_id"`
	Identity        IdentityRequest       `json:"identity"`
	Display         values.DisplayPolicy  `json:"display,omitempty"`
	NodeLimit       int                   `json:"node_limit,omitempty"`
	AttemptLimit    int                   `json:"attempt_limit,omitempty"`
	EventLimit      int                   `json:"event_limit,omitempty"`
	ValueLimit      int                   `json:"value_limit,omitempty"`
	ResourceLimit   int                   `json:"resource_limit,omitempty"`
	ActivationLimit int                   `json:"activation_limit,omitempty"`
}

type CancelWorkflowRunRequest struct {
	RunID          workflowruntime.RunID `json:"run_id"`
	Identity       IdentityRequest       `json:"identity"`
	IdempotencyKey string                `json:"idempotency_key"`
	Reason         string                `json:"reason,omitempty"`
}

type OperationIssue struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// WorkflowOperationError is the bounded, transport-safe error envelope shared
// by daemon routes and clients. Message text is intentionally not transported.
type WorkflowOperationError struct {
	Code        string                  `json:"code"`
	Diagnostics []diagnostic.Diagnostic `json:"diagnostics,omitempty"`
	Result      *StartRunResult         `json:"result,omitempty"`
}

const (
	WorkflowErrorCodeDryRunUnsupported    = "dry_run_unsupported"
	WorkflowErrorCodePolicyDenied         = "policy_denied"
	WorkflowErrorCodeConfirmationRequired = "confirmation_required"
	WorkflowErrorCodePinRejected          = "pin_rejected"
)

type CancelWorkflowRunResult struct {
	Cancellation workflowruntime.RequestRunCancellationResult `json:"cancellation"`
	Failures     []OperationIssue                             `json:"failures,omitempty"`
}

type ResumeWorkflowRunRequest struct {
	RunID          workflowruntime.RunID   `json:"run_id"`
	Identity       IdentityRequest         `json:"identity"`
	WaitID         workflowruntime.WaitID  `json:"wait_id"`
	Correlation    string                  `json:"correlation"`
	Token          string                  `json:"token"`
	WakeSource     workflowwait.WakeSource `json:"wake_source"`
	Payload        values.Value            `json:"payload"`
	IdempotencyKey string                  `json:"idempotency_key"`
}

type ResumeWorkflowRunResult struct {
	Outcome workflowruntime.ResumeOutcome `json:"outcome"`
	Wait    *ResumeWaitStatus             `json:"wait,omitempty"`
	Node    *ResumeNodeStatus             `json:"node,omitempty"`
	Attempt *ResumeAttemptStatus          `json:"attempt,omitempty"`
	Values  *values.ValueSetRef           `json:"values,omitempty"`
}

type ResumeWaitStatus struct {
	ID     workflowruntime.WaitID `json:"id"`
	Status workflowwait.Status    `json:"status"`
}

type ResumeNodeStatus struct {
	ID     workflowruntime.NodeInvocationID `json:"id"`
	Status workflowruntime.NodeStatus       `json:"status"`
}

type ResumeAttemptStatus struct {
	ID     workflowruntime.AttemptID  `json:"id"`
	Status workflowruntime.NodeStatus `json:"status"`
}

type RerunWorkflowResult = workflowruntime.BeginReplayResult

type RerunWorkflowRequest struct {
	SourceRunID    workflowruntime.RunID `json:"source_run_id"`
	RunID          workflowruntime.RunID `json:"run_id"`
	FromNodeID     string                `json:"from_node_id"`
	IdempotencyKey string                `json:"idempotency_key"`
	Identity       IdentityRequest       `json:"identity"`
}

type GraphRunInspector interface {
	Inspect(context.Context, rundiagnostics.Query) (rundiagnostics.Result, error)
}

type ReplayRunner interface {
	Rerun(context.Context, workflowruntime.ReplayRequest) (workflowruntime.BeginReplayResult, error)
}

type WorkflowOperatorOptions struct {
	Host        *Host
	Diagnostics GraphRunInspector
	Replay      ReplayRunner
	RunAccess   RunOperationAuthorizer
}

// WorkflowOperator is the transport-neutral Hadron operator facade. Its Host
// remains the authority for identity, policy, durable run state, and waits.
type WorkflowOperator struct {
	host        *Host
	diagnostics GraphRunInspector
	replay      ReplayRunner
	runAccess   RunOperationAuthorizer
}

type RunOperation string

const (
	RunOperationInspect RunOperation = "inspect"
	RunOperationCancel  RunOperation = "cancel"
	RunOperationRerun   RunOperation = "rerun"
)

type RunOperationAuthorization struct {
	Operation RunOperation              `json:"operation"`
	RunID     workflowruntime.RunID     `json:"run_id"`
	Caller    hoststate.IdentityBinding `json:"caller"`
	Owner     hoststate.IdentityBinding `json:"owner"`
	Display   *values.DisplayPolicy     `json:"display,omitempty"`
	FromNode  string                    `json:"from_node,omitempty"`
}

// RunOperationAuthorizer permits a host to add delegated run access without
// treating possession of a run ID as authority. Nil selects exact ownership.
type RunOperationAuthorizer interface {
	AuthorizeRunOperation(context.Context, RunOperationAuthorization) error
}

type RunOperationAuthorizerFunc func(context.Context, RunOperationAuthorization) error

func (f RunOperationAuthorizerFunc) AuthorizeRunOperation(ctx context.Context, request RunOperationAuthorization) error {
	return f(ctx, request)
}

func NewWorkflowOperator(options WorkflowOperatorOptions) (*WorkflowOperator, error) {
	if options.Host == nil || nilInterface(options.Diagnostics) || nilInterface(options.Replay) {
		return nil, fmt.Errorf("%w: host, diagnostics, and replay services are required", ErrInvalidHost)
	}
	authorizer := options.RunAccess
	if authorizer != nil && nilInterface(authorizer) {
		return nil, fmt.Errorf("%w: run operation authorizer must not be typed nil", ErrInvalidHost)
	}
	if authorizer == nil {
		authorizer = RunOperationAuthorizerFunc(func(_ context.Context, request RunOperationAuthorization) error {
			if !sameIdentity(request.Caller, request.Owner) {
				return fmt.Errorf("%w: current caller is not authorized for this workflow run", ErrPolicyDenied)
			}
			return nil
		})
	}
	return &WorkflowOperator{host: options.Host, diagnostics: options.Diagnostics, replay: options.Replay, runAccess: authorizer}, nil
}

func (s *WorkflowOperator) ValidateWorkflow(ctx context.Context, request ValidateWorkflowRequest) (ValidateWorkflowResult, error) {
	if err := s.ready(ctx); err != nil {
		return ValidateWorkflowResult{}, err
	}
	if _, err := s.host.bindIdentity(ctx, request.Identity); err != nil {
		return ValidateWorkflowResult{}, err
	}
	plan, err := s.host.definitions.ResolvePlan(ctx, request.Definition)
	if err != nil {
		var withDiagnostics interface {
			Diagnostics() []diagnostic.Diagnostic
		}
		if errors.As(err, &withDiagnostics) {
			return ValidateWorkflowResult{Definition: request.Definition, Diagnostics: withDiagnostics.Diagnostics()}, nil
		}
		return ValidateWorkflowResult{}, err
	}
	validated, err := cloneExecutionPlan(plan)
	if err != nil {
		return ValidateWorkflowResult{}, err
	}
	findings := validateResolvedPlan(ctx, s.host, validated)
	if findings == nil {
		findings = []diagnostic.Diagnostic{}
	}
	result := ValidateWorkflowResult{Definition: validated.Definition, Diagnostics: findings}
	if len(findings) == 0 {
		ref := workflowruntime.PlanRef{ID: validated.ID, Version: validated.Graph.Version, Digest: validated.Digest, SchemaVersion: validated.SchemaVersion}
		result.Plan = &ref
	}
	return result, nil
}

func (s *WorkflowOperator) ExplainWorkflow(ctx context.Context, request ExplainWorkflowRequest) (StartRunResult, error) {
	if err := s.ready(ctx); err != nil {
		return StartRunResult{}, err
	}
	return s.host.StartRun(ctx, StartRunRequest{RunID: request.RunID, Definition: request.Definition, Inputs: request.Inputs, IdempotencyKey: request.IdempotencyKey, Identity: request.Identity, Confirmed: request.Confirmed, DryRun: true})
}

func (s *WorkflowOperator) RunWorkflow(ctx context.Context, request RunWorkflowRequest) (StartRunResult, error) {
	if err := s.ready(ctx); err != nil {
		return StartRunResult{}, err
	}
	return s.host.StartRun(ctx, StartRunRequest{RunID: request.RunID, Definition: request.Definition, Inputs: request.Inputs, IdempotencyKey: request.IdempotencyKey, Identity: request.Identity, Confirmed: request.Confirmed, DryRun: request.DryRun, Pins: request.Pins})
}

func (s *WorkflowOperator) InspectWorkflowRun(ctx context.Context, request InspectWorkflowRunRequest) (rundiagnostics.Result, error) {
	if err := request.Display.Validate(); err != nil {
		return rundiagnostics.Result{}, fmt.Errorf("invalid workflow display policy: %w", err)
	}
	if err := s.authorizeRun(ctx, RunOperationAuthorization{Operation: RunOperationInspect, RunID: request.RunID, Display: &request.Display}, request.Identity); err != nil {
		return rundiagnostics.Result{}, err
	}
	return s.diagnostics.Inspect(ctx, rundiagnostics.Query{RunID: request.RunID, Now: s.host.now(), Display: request.Display, NodeLimit: request.NodeLimit, AttemptLimit: request.AttemptLimit, EventLimit: request.EventLimit, ValueLimit: request.ValueLimit, ResourceLimit: request.ResourceLimit, ActivationLimit: request.ActivationLimit})
}

func (s *WorkflowOperator) CancelWorkflowRun(ctx context.Context, request CancelWorkflowRunRequest) (CancelWorkflowRunResult, error) {
	if err := s.authorizeRun(ctx, RunOperationAuthorization{Operation: RunOperationCancel, RunID: request.RunID}, request.Identity); err != nil {
		return CancelWorkflowRunResult{}, err
	}
	result, failures, err := s.host.CancelRun(ctx, CancelRunRequest{RunID: request.RunID, IdempotencyKey: request.IdempotencyKey, Reason: request.Reason})
	response := CancelWorkflowRunResult{Cancellation: result}
	for range failures {
		response.Failures = append(response.Failures, OperationIssue{Code: "finalizer_cancel_failed", Message: "a workflow finalizer could not be canceled"})
	}
	return response, err
}

func (s *WorkflowOperator) ResumeWorkflowRun(ctx context.Context, request ResumeWorkflowRunRequest) (ResumeWorkflowRunResult, error) {
	if err := s.ready(ctx); err != nil {
		return ResumeWorkflowRunResult{}, err
	}
	identity, err := s.host.bindIdentity(ctx, request.Identity)
	if err != nil {
		return ResumeWorkflowRunResult{}, err
	}
	wait, err := s.host.state.LoadWait(ctx, request.WaitID)
	if err != nil {
		return ResumeWorkflowRunResult{}, err
	}
	if wait.Invocation.RunID != request.RunID {
		return ResumeWorkflowRunResult{}, fmt.Errorf("%w: wait does not belong to the requested run", ErrPolicyDenied)
	}
	result, resumeErr := s.host.ResumeWait(ctx, workflowruntime.ResumeCommand{WaitID: request.WaitID, Correlation: request.Correlation, Token: request.Token, WakeSource: request.WakeSource, Responder: workflowwait.Responder{Kind: "principal", Reference: identity.Principal}, Payload: request.Payload, IdempotencyKey: request.IdempotencyKey, ReceivedAt: s.host.now()})
	return safeResumeWorkflowRunResult(result), resumeErr
}

func safeResumeWorkflowRunResult(result workflowruntime.ResumeWaitResult) ResumeWorkflowRunResult {
	safe := ResumeWorkflowRunResult{Outcome: result.Outcome}
	if result.Wait.Ref.ID != "" {
		safe.Wait = &ResumeWaitStatus{ID: result.Wait.Ref.ID, Status: result.Wait.Status}
	}
	if result.Node.ID.RunID != "" {
		safe.Node = &ResumeNodeStatus{ID: result.Node.ID, Status: result.Node.Status}
	}
	if result.Attempt.ID.Invocation.RunID != "" && result.Attempt.ID.Number > 0 {
		safe.Attempt = &ResumeAttemptStatus{ID: result.Attempt.ID, Status: result.Attempt.Status}
	}
	if result.Values.Validate() == nil {
		valuesRef := result.Values
		safe.Values = &valuesRef
	}
	return safe
}

func (s *WorkflowOperator) RerunWorkflow(ctx context.Context, request RerunWorkflowRequest) (RerunWorkflowResult, error) {
	if err := s.authorizeRun(ctx, RunOperationAuthorization{Operation: RunOperationRerun, RunID: request.SourceRunID, FromNode: request.FromNodeID}, request.Identity); err != nil {
		return workflowruntime.BeginReplayResult{}, err
	}
	return s.replay.Rerun(ctx, workflowruntime.ReplayRequest{SourceRunID: request.SourceRunID, RunID: request.RunID, FromNodeID: request.FromNodeID, IdempotencyKey: request.IdempotencyKey, At: s.host.now()})
}

func (s *WorkflowOperator) ready(ctx context.Context) error {
	if ctx == nil || s == nil || s.host == nil || nilInterface(s.diagnostics) || nilInterface(s.replay) || nilInterface(s.runAccess) {
		return fmt.Errorf("%w: workflow operator is not initialized", ErrInvalidHost)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.host.requireReady()
}

func (s *WorkflowOperator) authorizeRun(ctx context.Context, authorization RunOperationAuthorization, request IdentityRequest) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	runID := authorization.RunID
	if strings.TrimSpace(string(runID)) == "" {
		return errors.New("workflow run id is required")
	}
	current, err := s.host.bindIdentity(ctx, request)
	if err != nil {
		return err
	}
	owner, err := s.host.runOwner(ctx, runID, make(map[workflowruntime.RunID]struct{}), 0)
	if err != nil {
		return err
	}
	authorization.Caller = current.Clone()
	authorization.Owner = owner.Clone()
	return s.runAccess.AuthorizeRunOperation(ctx, authorization)
}

func (h *Host) runOwner(ctx context.Context, runID workflowruntime.RunID, seen map[workflowruntime.RunID]struct{}, depth int) (hoststate.IdentityBinding, error) {
	if depth > maximumRunAuthorizationDepth {
		return hoststate.IdentityBinding{}, fmt.Errorf("%w: run authorization ancestry exceeds %d", ErrPolicyDenied, maximumRunAuthorizationDepth)
	}
	if _, duplicate := seen[runID]; duplicate {
		return hoststate.IdentityBinding{}, fmt.Errorf("%w: run authorization ancestry contains a cycle", ErrPolicyDenied)
	}
	seen[runID] = struct{}{}
	defer delete(seen, runID)
	start, err := h.journal.LoadStart(ctx, runID)
	if err == nil {
		return start.Record.Identity.Clone(), nil
	}
	if !errors.Is(err, workflowruntime.ErrNotFound) {
		return hoststate.IdentityBinding{}, err
	}
	if !nilInterface(h.childDefs) {
		child, childErr := h.childDefs.LoadChildRunRequest(ctx, runID)
		if childErr == nil {
			return h.runOwner(ctx, workflowruntime.RunID(child.Parent.RunID), seen, depth+1)
		}
		if !errors.Is(childErr, workflowruntime.ErrNotFound) {
			return hoststate.IdentityBinding{}, childErr
		}
	}
	replays, ok := h.state.(workflowruntime.ReplayStore)
	if !ok || nilInterface(replays) {
		return hoststate.IdentityBinding{}, fmt.Errorf("%w: run has no authenticated root binding", ErrPolicyDenied)
	}
	provenance, replayErr := replays.LoadReplayProvenance(ctx, runID)
	if replayErr != nil {
		if errors.Is(replayErr, workflowruntime.ErrNotFound) {
			return hoststate.IdentityBinding{}, fmt.Errorf("%w: run has no authenticated root binding", ErrPolicyDenied)
		}
		return hoststate.IdentityBinding{}, replayErr
	}
	return h.runOwner(ctx, provenance.SourceRunID, seen, depth+1)
}

func validateResolvedPlan(ctx context.Context, host *Host, plan *compile.ExecutionPlan) []diagnostic.Diagnostic {
	options := compile.ValidationOptions{StepKinds: host.registry, Verifiers: host.verifiers}
	if resolver, ok := host.definitions.(compile.DefinitionResolver); ok {
		options.Definitions = resolver
	}
	return compile.ValidatePlan(ctx, plan, options)
}
