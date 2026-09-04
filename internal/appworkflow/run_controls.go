package appworkflow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
	workflowwait "github.com/hollis-labs/go-workflow/wait"
)

type QueryRunRequest struct {
	Query     runtime.RunStateQuery
	Identity  IdentityRequest
	Confirmed bool
}

type SignalRunRequest struct {
	Selector       runtime.SignalSelector
	Payload        values.Value
	IdempotencyKey string
	Identity       IdentityRequest
	Confirmed      bool
}

type UpdateRunRequest = SignalRunRequest

// RunQueryView is the transport-safe operational projection returned by the
// host query boundary. It intentionally excludes claim leases, raw wait
// correlation/credentials/authority/resolution provenance, event attributes,
// and value references whose underlying data may be private or secret.
type RunQueryView struct {
	Run      RunQuerySummary       `json:"run"`
	Nodes    []NodeQuerySummary    `json:"nodes,omitempty"`
	Attempts []AttemptQuerySummary `json:"attempts,omitempty"`
	Waits    []WaitQuerySummary    `json:"waits,omitempty"`
	Events   []EventQuerySummary   `json:"events,omitempty"`
}

type RunQuerySummary struct {
	ID         runtime.RunID     `json:"id"`
	Plan       runtime.PlanRef   `json:"plan"`
	Status     runtime.RunStatus `json:"status"`
	HasInputs  bool              `json:"has_inputs"`
	HasOutputs bool              `json:"has_outputs"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

type NodeQuerySummary struct {
	ID            runtime.NodeInvocationID `json:"id"`
	Status        runtime.NodeStatus       `json:"status"`
	BlockedCode   string                   `json:"blocked_code,omitempty"`
	HasInputs     bool                     `json:"has_inputs"`
	HasOutputs    bool                     `json:"has_outputs"`
	HasWait       bool                     `json:"has_wait"`
	LatestAttempt int                      `json:"latest_attempt,omitempty"`
	CreatedAt     time.Time                `json:"created_at"`
	UpdatedAt     time.Time                `json:"updated_at"`
}

type AttemptQuerySummary struct {
	ID              runtime.AttemptID  `json:"id"`
	Status          runtime.NodeStatus `json:"status"`
	ExecutorKind    string             `json:"executor_kind"`
	ExecutorVersion string             `json:"executor_version"`
	FailureCode     string             `json:"failure_code,omitempty"`
	Retryable       bool               `json:"retryable,omitempty"`
	StartedAt       time.Time          `json:"started_at"`
	FinishedAt      time.Time          `json:"finished_at,omitempty"`
}

type WaitQuerySummary struct {
	ID         runtime.WaitID           `json:"id"`
	Invocation runtime.NodeInvocationID `json:"invocation"`
	Kind       workflowwait.Kind        `json:"kind"`
	SignalName string                   `json:"signal_name,omitempty"`
	WakeSource workflowwait.WakeSource  `json:"wake_source"`
	Status     workflowwait.Status      `json:"status"`
	CreatedAt  time.Time                `json:"created_at"`
	UpdatedAt  time.Time                `json:"updated_at"`
	ResolvedAt time.Time                `json:"resolved_at,omitempty"`
}

type EventQuerySummary struct {
	Sequence   uint64                    `json:"sequence"`
	Invocation *runtime.NodeInvocationID `json:"invocation,omitempty"`
	Attempt    *runtime.AttemptID        `json:"attempt,omitempty"`
	Type       string                    `json:"type"`
	OccurredAt time.Time                 `json:"occurred_at"`
	Redaction  values.RedactionClass     `json:"redaction"`
	Retention  values.RetentionClass     `json:"retention"`
	HasValues  bool                      `json:"has_values"`
}

// QueryRun is the bounded, read-only host query path. A run identifier only
// locates the immutable start binding; current identity and policy still
// authorize every query.
func (h *Host) QueryRun(ctx context.Context, request QueryRunRequest) (RunQueryView, error) {
	if err := h.requireReady(); err != nil {
		return RunQueryView{}, err
	}
	if err := request.Query.Validate(); err != nil {
		return RunQueryView{}, err
	}
	if _, err := h.authorizeRunControl(ctx, request.Query.RunID, "query", request.Identity, request.Confirmed); err != nil {
		return RunQueryView{}, err
	}
	if audit, ok := h.journal.(hoststate.NonDurableJournal); ok && !nilInterface(audit) {
		if record, err := audit.LoadNonDurableStart(ctx, request.Query.RunID); err == nil {
			result := projectRunQuery(runtime.RunStateView{Run: record.Run})
			result.Run.HasOutputs = record.Outputs != nil
			return result, nil
		} else if !errors.Is(err, runtime.ErrNotFound) {
			return RunQueryView{}, err
		}
	}
	store, ok := h.state.(runtime.RunControlStore)
	if !ok || nilInterface(store) {
		return RunQueryView{}, fmt.Errorf("%w: run controls are unavailable", ErrInvalidHost)
	}
	view, err := store.QueryRunState(ctx, request.Query)
	if err != nil {
		return RunQueryView{}, err
	}
	return projectRunQuery(view), nil
}

func projectRunQuery(source runtime.RunStateView) RunQueryView {
	result := RunQueryView{Run: RunQuerySummary{ID: source.Run.ID, Plan: source.Run.Plan, Status: source.Run.Status,
		HasInputs: source.Run.Inputs != nil, HasOutputs: source.Run.Outputs != nil, CreatedAt: source.Run.CreatedAt, UpdatedAt: source.Run.UpdatedAt}}
	for _, node := range source.Nodes {
		item := NodeQuerySummary{ID: node.ID, Status: node.Status, HasInputs: node.Inputs != nil, HasOutputs: node.Outputs != nil,
			HasWait: node.Wait != nil, LatestAttempt: node.LatestAttempt, CreatedAt: node.CreatedAt, UpdatedAt: node.UpdatedAt}
		if node.Blocked != nil {
			item.BlockedCode = node.Blocked.Code
		}
		result.Nodes = append(result.Nodes, item)
	}
	for _, attempt := range source.Attempts {
		item := AttemptQuerySummary{ID: attempt.ID, Status: attempt.Status, ExecutorKind: attempt.Executor.Kind,
			ExecutorVersion: attempt.Executor.Version, StartedAt: attempt.StartedAt, FinishedAt: attempt.FinishedAt}
		if attempt.Failure != nil {
			item.FailureCode, item.Retryable = attempt.Failure.Code, attempt.Failure.Retryable
		}
		result.Attempts = append(result.Attempts, item)
	}
	for _, wait := range source.Waits {
		result.Waits = append(result.Waits, WaitQuerySummary{ID: wait.Ref.ID, Invocation: wait.Invocation, Kind: wait.Kind,
			SignalName: wait.SignalName, WakeSource: wait.WakeSource, Status: wait.Status, CreatedAt: wait.CreatedAt,
			UpdatedAt: wait.UpdatedAt, ResolvedAt: wait.ResolvedAt})
	}
	for _, event := range source.Events {
		result.Events = append(result.Events, EventQuerySummary{Sequence: event.Sequence, Invocation: event.Invocation, Attempt: event.Attempt,
			Type: event.Type, OccurredAt: event.OccurredAt, Redaction: event.Redaction, Retention: event.Retention, HasValues: event.Values != nil})
	}
	return result
}

// SignalRun resolves a named signal and immediately uses the same canonical
// WaitCoordinator/ResumeNodeWait transaction as gates, callbacks, timers, and
// child completion. It intentionally adds no alternate signal state machine.
func (h *Host) SignalRun(ctx context.Context, request SignalRunRequest) (runtime.ResumeWaitResult, error) {
	if err := h.requireReady(); err != nil {
		return runtime.ResumeWaitResult{}, err
	}
	store, ok := h.state.(runtime.RunControlStore)
	if !ok || nilInterface(store) || h.waits == nil {
		return runtime.ResumeWaitResult{}, fmt.Errorf("%w: named signal controls are unavailable", ErrInvalidHost)
	}
	if strings.TrimSpace(request.IdempotencyKey) == "" {
		return runtime.ResumeWaitResult{}, errors.New("signal idempotency key is required")
	}
	identity, err := h.authorizeRunControl(ctx, request.Selector.RunID, "signal", request.Identity, request.Confirmed)
	if err != nil {
		return runtime.ResumeWaitResult{}, err
	}
	wait, err := store.FindSignalWait(ctx, request.Selector, request.IdempotencyKey)
	if err != nil {
		return runtime.ResumeWaitResult{}, err
	}
	return h.waits.Resume(ctx, namedResumeCommand(wait, request.Payload, request.IdempotencyKey, identity, maxTime(h.now(), wait.UpdatedAt)))
}

// UpdateRun durably records exact caller intent after resolving its target and
// before attempting the mutating resume. Recovery replays that same command
// and seals a compact receipt, so process loss cannot create an untracked or
// differently targeted update.
func (h *Host) UpdateRun(ctx context.Context, request UpdateRunRequest) (runtime.RunUpdateSnapshot, error) {
	return h.updateRunInternal(ctx, request, false)
}

func (h *Host) updateRunInternal(ctx context.Context, request UpdateRunRequest, allowRecovery bool) (runtime.RunUpdateSnapshot, error) {
	return h.updateRunAtWaitInternal(ctx, request, runtime.WaitSnapshot{}, allowRecovery)
}

func (h *Host) updateRunAtWaitInternal(ctx context.Context, request UpdateRunRequest, resolved runtime.WaitSnapshot, allowRecovery bool) (runtime.RunUpdateSnapshot, error) {
	if err := h.requireStartReady(allowRecovery); err != nil {
		return runtime.RunUpdateSnapshot{}, err
	}
	store, ok := h.state.(runtime.RunControlStore)
	if !ok || nilInterface(store) || h.waits == nil {
		return runtime.RunUpdateSnapshot{}, fmt.Errorf("%w: tracked updates are unavailable", ErrInvalidHost)
	}
	identity, err := h.authorizeRunControl(ctx, request.Selector.RunID, "update", request.Identity, request.Confirmed)
	if err != nil {
		return runtime.RunUpdateSnapshot{}, err
	}
	if prior, loadErr := store.LoadRunUpdate(ctx, request.IdempotencyKey); loadErr == nil {
		if prior.Request.Selector != request.Selector || !reflect.DeepEqual(prior.Request.Payload, request.Payload) || !reflect.DeepEqual(prior.Request.Responder, responderFromIdentity(identity)) {
			return runtime.RunUpdateSnapshot{}, &runtime.IdempotencyConflictError{Operation: "workflow update", Key: request.IdempotencyKey}
		}
		if prior.Status != runtime.RunUpdatePending {
			return prior, nil
		}
		return h.applyRunUpdate(context.WithoutCancel(ctx), store, prior)
	} else if !errors.Is(loadErr, runtime.ErrNotFound) {
		return runtime.RunUpdateSnapshot{}, loadErr
	}
	wait := resolved
	if wait.Ref.ID == "" {
		wait, err = store.FindOpenSignalWait(ctx, request.Selector)
		if err != nil {
			return runtime.RunUpdateSnapshot{}, err
		}
	} else if wait.Invocation.RunID != request.Selector.RunID || wait.Kind != workflowwait.KindSignal || wait.SignalName != request.Selector.Name ||
		wait.Correlation != request.Selector.Correlation || wait.WakeSource != workflowwait.WakeSignal {
		return runtime.RunUpdateSnapshot{}, errors.New("resolved workflow update wait does not match its exact named selector")
	}
	responder := responderFromIdentity(identity)
	receivedAt := maxTime(h.now(), wait.UpdatedAt)
	pending, _, err := store.BeginRunUpdate(context.WithoutCancel(ctx), runtime.BeginRunUpdateRequest{
		IdempotencyKey: request.IdempotencyKey, Selector: request.Selector, WaitID: wait.Ref.ID,
		Responder: responder, Payload: request.Payload, ReceivedAt: receivedAt,
	})
	if err != nil {
		return runtime.RunUpdateSnapshot{}, err
	}
	if pending.Status != runtime.RunUpdatePending {
		return pending, nil
	}
	return h.applyRunUpdate(context.WithoutCancel(ctx), store, pending)
}

func (h *Host) authorizeRunControl(ctx context.Context, runID runtime.RunID, operation string, request IdentityRequest, confirmed bool) (hoststate.IdentityBinding, error) {
	if ctx == nil {
		return hoststate.IdentityBinding{}, errors.New("workflow run control context is required")
	}
	start, err := h.journal.LoadStart(ctx, runID)
	var priorIdentity hoststate.IdentityBinding
	var facts hoststate.PolicyFacts
	if err == nil {
		priorIdentity, facts = start.Record.Identity, start.Record.Facts
	} else if errors.Is(err, runtime.ErrNotFound) {
		audit, ok := h.journal.(hoststate.NonDurableJournal)
		if !ok || nilInterface(audit) {
			return hoststate.IdentityBinding{}, err
		}
		prior, auditErr := audit.LoadNonDurableStart(ctx, runID)
		if auditErr != nil {
			return hoststate.IdentityBinding{}, auditErr
		}
		priorIdentity, facts = prior.Identity, prior.Facts
	} else {
		return hoststate.IdentityBinding{}, err
	}
	identity, err := h.bindIdentity(ctx, request)
	if err != nil {
		return hoststate.IdentityBinding{}, err
	}
	if !sameIdentity(identity, priorIdentity) {
		return hoststate.IdentityBinding{}, fmt.Errorf("%w: current caller does not own the immutable run binding", ErrPolicyDenied)
	}
	facts, err = clonePolicyFacts(facts)
	if err != nil {
		return hoststate.IdentityBinding{}, err
	}
	facts.Operation = operation
	decision, err := h.policy.EvaluatePolicy(ctx, facts)
	if err != nil {
		return hoststate.IdentityBinding{}, fmt.Errorf("evaluate workflow %s policy: %w", operation, err)
	}
	decision = normalizeDecision(decision, runID, h.now())
	decision.Operation = operation
	decision.ID = policyDecisionID(operation + ":" + string(runID) + ":" + identity.Principal)
	if err := decision.Validate(); err != nil {
		return hoststate.IdentityBinding{}, fmt.Errorf("invalid workflow %s policy decision: %w", operation, err)
	}
	if decision.Operation != facts.Operation {
		return hoststate.IdentityBinding{}, fmt.Errorf("invalid workflow %s policy decision: operation mismatch", operation)
	}
	if decision.Outcome == hoststate.PolicyDeny {
		return hoststate.IdentityBinding{}, ErrPolicyDenied
	}
	if decision.Outcome == hoststate.PolicyConfirm && !confirmed {
		return hoststate.IdentityBinding{}, ErrConfirmationRequired
	}
	return identity, nil
}

func namedResumeCommand(wait runtime.WaitSnapshot, payload values.Value, key string, identity hoststate.IdentityBinding, at time.Time) runtime.ResumeCommand {
	return runtime.ResumeCommand{WaitID: wait.Ref.ID, Correlation: wait.Correlation, WakeSource: workflowwait.WakeSignal,
		Responder: responderFromIdentity(identity), Payload: payload, IdempotencyKey: key, ReceivedAt: at}
}

func responderFromIdentity(identity hoststate.IdentityBinding) workflowwait.Responder {
	return workflowwait.Responder{Kind: "hadron_identity", Reference: identity.Principal, Attributes: map[string]string{"source_authority": identity.SourceAuthority, "trust": identity.Trust}}
}

func (h *Host) applyRunUpdate(ctx context.Context, store runtime.RunControlStore, pending runtime.RunUpdateSnapshot) (runtime.RunUpdateSnapshot, error) {
	result, resumeErr := h.waits.Resume(ctx, runtime.ResumeCommand{
		WaitID: pending.Request.WaitID, Correlation: pending.Request.Selector.Correlation, WakeSource: workflowwait.WakeSignal,
		Responder: pending.Request.Responder, Payload: pending.Request.Payload, IdempotencyKey: pending.Request.IdempotencyKey,
		ReceivedAt: pending.Request.ReceivedAt,
	})
	status := runtime.RunUpdateApplied
	if result.Outcome == runtime.ResumeClosed || errors.Is(resumeErr, runtime.ErrWaitClosed) {
		status = runtime.RunUpdateClosed
	}
	outcome := result.Outcome
	if status == runtime.RunUpdateClosed && outcome == "" {
		outcome = runtime.ResumeClosed
	}
	receipt := runtime.RunUpdateReceipt{Outcome: outcome, WaitID: pending.Request.WaitID, WaitStatus: result.Wait.Status, ResolvedAt: result.Wait.ResolvedAt}
	if result.Values.ID != "" {
		valuesRef := result.Values
		receipt.Values = &valuesRef
	}
	var postCommit *runtime.PostCommitError
	if resumeErr != nil && status != runtime.RunUpdateClosed && !errors.As(resumeErr, &postCommit) {
		return pending, resumeErr
	}
	sealed, sealErr := store.CompleteRunUpdate(ctx, runtime.CompleteRunUpdateRequest{
		IdempotencyKey: pending.Request.IdempotencyKey, ExpectedGeneration: pending.Generation,
		Status: status, Receipt: receipt, At: maxTime(h.now(), pending.UpdatedAt),
	})
	if sealErr != nil {
		return pending, sealErr
	}
	return sealed, resumeErr
}

func (h *Host) recoverRunUpdates(ctx context.Context) error {
	store, ok := h.state.(runtime.RunControlStore)
	if !ok || nilInterface(store) {
		return nil
	}
	for {
		pending, err := store.RecoverRunUpdates(ctx, h.batchLimit)
		if err != nil {
			return err
		}
		if len(pending) == 0 {
			return nil
		}
		for _, update := range pending {
			sealed, applyErr := h.applyRunUpdate(ctx, store, update)
			if applyErr != nil {
				var postCommit *runtime.PostCommitError
				if errors.As(applyErr, &postCommit) && sealed.Status != runtime.RunUpdatePending {
					continue
				}
				return fmt.Errorf("recover update %s: %w", update.Request.IdempotencyKey, applyErr)
			}
		}
	}
}

// recoverRunUpdateAtWait resumes a source-authorized reactor intent without
// re-evaluating mutable current policy. The exact selector, payload, responder,
// target wait, and idempotency key were already persisted; any divergence is
// an idempotency conflict and fails closed.
func (h *Host) recoverRunUpdateAtWait(ctx context.Context, selector runtime.SignalSelector, payload values.Value, key string,
	wait runtime.WaitSnapshot, responder workflowwait.Responder, receivedAt time.Time) (runtime.RunUpdateSnapshot, error) {
	store, ok := h.state.(runtime.RunControlStore)
	if !ok || nilInterface(store) || h.waits == nil {
		return runtime.RunUpdateSnapshot{}, fmt.Errorf("%w: tracked updates are unavailable", ErrInvalidHost)
	}
	if wait.Ref.ID == "" || wait.Invocation.RunID != selector.RunID || wait.Kind != workflowwait.KindSignal || wait.SignalName != selector.Name ||
		wait.Correlation != selector.Correlation || wait.WakeSource != workflowwait.WakeSignal {
		return runtime.RunUpdateSnapshot{}, errors.New("recovered workflow update wait does not match its exact named selector")
	}
	if prior, err := store.LoadRunUpdate(ctx, key); err == nil {
		if prior.Request.Selector != selector || prior.Request.WaitID != wait.Ref.ID || !reflect.DeepEqual(prior.Request.Payload, payload) ||
			!reflect.DeepEqual(prior.Request.Responder, responder) {
			return runtime.RunUpdateSnapshot{}, &runtime.IdempotencyConflictError{Operation: "workflow update recovery", Key: key}
		}
		if prior.Status != runtime.RunUpdatePending {
			return prior, nil
		}
		return h.applyRunUpdate(context.WithoutCancel(ctx), store, prior)
	} else if !errors.Is(err, runtime.ErrNotFound) {
		return runtime.RunUpdateSnapshot{}, err
	}
	pending, _, err := store.BeginRunUpdate(context.WithoutCancel(ctx), runtime.BeginRunUpdateRequest{IdempotencyKey: key, Selector: selector,
		WaitID: wait.Ref.ID, Responder: responder, Payload: payload, ReceivedAt: maxTime(receivedAt, wait.UpdatedAt)})
	if err != nil {
		return runtime.RunUpdateSnapshot{}, err
	}
	if pending.Status != runtime.RunUpdatePending {
		return pending, nil
	}
	return h.applyRunUpdate(context.WithoutCancel(ctx), store, pending)
}
