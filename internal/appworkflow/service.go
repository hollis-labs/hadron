package appworkflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/go-workflow/compile"
	"github.com/hollis-labs/go-workflow/graph"
	"github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/stepkind"
	"github.com/hollis-labs/go-workflow/values"
)

const cancellationCASLimit = 8

func (h *Host) StartRun(ctx context.Context, request StartRunRequest) (StartRunResult, error) {
	return h.startRunInternal(ctx, request, nil, false)
}

func (h *Host) startRun(ctx context.Context, request StartRunRequest, expectedIdentity *hoststate.IdentityBinding) (StartRunResult, error) {
	return h.startRunInternal(ctx, request, expectedIdentity, false)
}

func (h *Host) startRunInternal(ctx context.Context, request StartRunRequest, expectedIdentity *hoststate.IdentityBinding, allowRecovery bool) (StartRunResult, error) {
	if err := h.requireStartReady(allowRecovery); err != nil {
		return StartRunResult{}, err
	}
	if ctx == nil {
		return StartRunResult{}, fmt.Errorf("start workflow: context is required")
	}
	if strings.TrimSpace(string(request.RunID)) == "" || strings.TrimSpace(request.IdempotencyKey) == "" {
		return StartRunResult{}, fmt.Errorf("start workflow requires run id and idempotency key")
	}
	request.Identity = normalizeIdentityRequest(request.Identity)
	if err := validateIdentityRequest(request.Identity); err != nil {
		return StartRunResult{}, err
	}
	request.Pins = canonicalStartPins(request.Pins)
	if err := validateStartPins(request.Pins); err != nil {
		return StartRunResult{}, err
	}
	if request.DryRun && len(request.Pins) != 0 {
		return StartRunResult{}, errors.New("dry-run cannot bind pinned outputs")
	}
	inputHash, err := values.DigestInline(request.Inputs)
	if err != nil {
		return StartRunResult{}, fmt.Errorf("digest workflow inputs: %w", err)
	}
	requestDigest, err := digestStartIntent(request, inputHash)
	if err != nil {
		return StartRunResult{}, err
	}
	decisionID := policyDecisionID(request.IdempotencyKey)
	if audit, ok := h.journal.(hoststate.NonDurableJournal); ok && !nilInterface(audit) {
		if prior, loadErr := audit.LoadNonDurableStartByKey(ctx, request.IdempotencyKey); loadErr == nil {
			if authErr := h.authorizeNonDurableReplay(ctx, request.Identity, prior); authErr != nil {
				return StartRunResult{}, authErr
			}
			if expectedIdentity != nil && !sameIdentity(prior.Identity, expectedIdentity.Clone()) {
				return StartRunResult{}, fmt.Errorf("%w: replayed internal identity differs from its immutable source binding", ErrPolicyDenied)
			}
			if prior.RequestDigest != requestDigest {
				return StartRunResult{}, &runtime.IdempotencyConflictError{Operation: "start non-durable graph workflow", Key: request.IdempotencyKey}
			}
			return nonDurableStartResult(prior, runtime.IdempotencyReplayed)
		} else if !errors.Is(loadErr, runtime.ErrNotFound) {
			return StartRunResult{}, fmt.Errorf("load non-durable workflow replay: %w", loadErr)
		}
	}
	if prior, loadErr := h.journal.LoadStartByKey(ctx, request.IdempotencyKey); loadErr == nil {
		if authErr := h.authorizeStartReplay(ctx, request.Identity, prior.Record.Identity); authErr != nil {
			return StartRunResult{}, authErr
		}
		if expectedIdentity != nil && !sameIdentity(prior.Record.Identity, expectedIdentity.Clone()) {
			return StartRunResult{}, fmt.Errorf("%w: replayed activation identity differs from its immutable registration binding", ErrPolicyDenied)
		}
		if prior.Record.RequestDigest != requestDigest {
			return StartRunResult{}, &runtime.IdempotencyConflictError{Operation: "start graph workflow", Key: request.IdempotencyKey}
		}
		finished, materializeErr := h.materializeStart(ctx, prior)
		return h.startResult(ctx, finished, runtime.IdempotencyReplayed, materializeErr)
	} else if !errors.Is(loadErr, runtime.ErrNotFound) {
		return StartRunResult{}, fmt.Errorf("load workflow start replay: %w", loadErr)
	}

	identity, bindErr := h.bindIdentity(ctx, request.Identity)
	if bindErr != nil {
		return StartRunResult{}, bindErr
	}
	if expectedIdentity != nil && !sameIdentity(identity, expectedIdentity.Clone()) {
		return StartRunResult{}, fmt.Errorf("%w: activation identity does not match its immutable scope and execution target binding", ErrPolicyDenied)
	}
	var resolvedPlan *compile.ExecutionPlan
	var planSnapshot *hoststate.PlanSnapshot
	if provider, ok := h.definitions.(DefinitionSnapshotProvider); ok && !nilInterface(provider) {
		resolved, resolveErr := provider.ResolvePlanSnapshot(ctx, request.Definition)
		if resolveErr != nil {
			return StartRunResult{}, fmt.Errorf("resolve workflow definition snapshot: %w", resolveErr)
		}
		resolvedPlan = &resolved.Plan
		planSnapshot = &resolved
	} else {
		resolved, resolveErr := h.definitions.ResolvePlan(ctx, request.Definition)
		if resolveErr != nil {
			return StartRunResult{}, fmt.Errorf("resolve workflow definition: %w", resolveErr)
		}
		resolvedPlan = resolved
	}
	plan, err := cloneExecutionPlan(resolvedPlan)
	if err != nil {
		return StartRunResult{}, fmt.Errorf("clone resolved workflow definition: %w", err)
	}
	validationOptions := compile.ValidationOptions{StepKinds: h.registry, Verifiers: h.verifiers}
	if resolver, ok := h.definitions.(compile.DefinitionResolver); ok {
		validationOptions.Definitions = resolver
	}
	findings := compile.ValidatePlan(ctx, plan, validationOptions)
	if len(findings) != 0 {
		return StartRunResult{Diagnostics: findings}, nil
	}
	if plan.Graph.Compensation != nil {
		compensation, ok := h.state.(runtime.CompensationStore)
		if !ok || nilInterface(compensation) {
			return StartRunResult{}, fmt.Errorf("start workflow: compensated plan requires durable compensation state: %w", runtime.ErrInvalidCompensation)
		}
	}
	if planSnapshot == nil {
		sealed, sealErr := hoststate.SealPlanSnapshot(hoststate.PlanSnapshot{
			SchemaVersion: hoststate.PlanSnapshotSchemaVersion, Plan: *plan,
			SourceMap: plan.SourceMap,
			Compile:   hoststate.UnavailableCompileDescriptor("definition provider does not expose deterministic compile metadata or exact source material"),
		})
		if sealErr != nil {
			return StartRunResult{}, fmt.Errorf("seal workflow plan snapshot: %w", sealErr)
		}
		planSnapshot = &sealed
	}

	var facts hoststate.PolicyFacts
	var decision hoststate.PolicyDecision
	if prior, loadErr := h.journal.LoadPolicyEvaluationByStartKey(ctx, request.IdempotencyKey); loadErr == nil {
		if !sameIdentity(identity, prior.Facts.Identity) {
			return StartRunResult{}, fmt.Errorf("%w: current caller is not authorized to replay this start key", ErrPolicyDenied)
		}
		if prior.RequestDigest != requestDigest {
			return StartRunResult{}, &runtime.IdempotencyConflictError{Operation: "workflow policy evaluation", Key: request.IdempotencyKey}
		}
		facts, decision = prior.Facts, prior.Decision
		if facts.Plan.Digest != plan.Digest {
			return StartRunResult{}, errors.New("replayed policy plan differs from resolved plan")
		}
	} else if !errors.Is(loadErr, runtime.ErrNotFound) {
		return StartRunResult{}, loadErr
	} else {
		facts, err = h.policyFacts(ctx, request.RunID, plan, identity)
		if err != nil {
			return StartRunResult{}, err
		}
		policyInput, cloneErr := clonePolicyFacts(facts)
		if cloneErr != nil {
			return StartRunResult{}, fmt.Errorf("clone workflow policy facts: %w", cloneErr)
		}
		decision, err = h.policy.EvaluatePolicy(ctx, policyInput)
		if err != nil {
			return StartRunResult{}, fmt.Errorf("evaluate workflow start policy: %w", err)
		}
		decision = normalizeDecision(decision, request.RunID, h.now())
		decision.ID = decisionID
		if validationErr := decision.Validate(); validationErr != nil {
			return StartRunResult{}, fmt.Errorf("invalid workflow policy decision: %w", validationErr)
		}
		if decision.Operation != facts.Operation {
			return StartRunResult{}, errors.New("invalid workflow policy decision: operation mismatch")
		}
		persisted, outcome, persistErr := h.journal.RecordPolicyEvaluation(context.WithoutCancel(ctx), hoststate.PolicyEvaluation{StartKey: request.IdempotencyKey, RequestDigest: requestDigest, Facts: facts, Decision: decision})
		if persistErr != nil {
			return StartRunResult{}, fmt.Errorf("record workflow policy evaluation: %w", persistErr)
		}
		if outcome == runtime.IdempotencyReplayed && !sameIdentity(identity, persisted.Facts.Identity) {
			return StartRunResult{}, fmt.Errorf("%w: current caller is not authorized to replay this start key", ErrPolicyDenied)
		}
		if persisted.Facts.Plan.Digest != plan.Digest {
			return StartRunResult{}, errors.New("persisted policy plan differs from resolved plan")
		}
		facts, decision = persisted.Facts, persisted.Decision
	}
	if decision.Outcome == hoststate.PolicyDeny {
		return StartRunResult{Decision: decision, Facts: facts}, ErrPolicyDenied
	}
	if decision.Outcome == hoststate.PolicyConfirm && !request.Confirmed {
		return StartRunResult{Decision: decision, Facts: facts}, ErrConfirmationRequired
	}
	if request.DryRun && !facts.DryRunAvailable {
		return StartRunResult{Decision: decision, Facts: facts}, ErrDryRunUnsupported
	}
	if runtime.EffectiveDurability(plan.Graph) == graph.DurabilityNone && !request.DryRun {
		return h.executeNonDurable(ctx, request, requestDigest, plan, facts, decision)
	}

	boundResult, err := runtime.BindRun(ctx, h.state, runtime.BindRunRequest{ID: request.RunID, Plan: plan, Inputs: request.Inputs, CreatedAt: h.now()})
	if err != nil {
		return StartRunResult{}, fmt.Errorf("bind workflow run: %w", err)
	}
	if len(boundResult.Diagnostics) != 0 {
		return StartRunResult{Decision: decision, Facts: facts, Diagnostics: boundResult.Diagnostics}, nil
	}
	if boundResult.Run == nil {
		return StartRunResult{}, errors.New("workflow binding returned no run")
	}
	record := hoststate.StartRecord{
		Run: *boundResult.Run, Plan: *plan, Requested: request.Definition,
		StartKey: request.IdempotencyKey, RequestDigest: requestDigest, CallerInputHash: inputHash,
		Identity: facts.Identity, Facts: facts, Decision: decision,
		Activation: request.Activation, Pins: request.Pins, DryRun: request.DryRun, RecordedAt: h.now(),
		Snapshot: planSnapshot,
	}
	snapshot, outcome, err := h.journal.RecordStart(context.WithoutCancel(ctx), record)
	if err != nil {
		if errors.Is(err, runtime.ErrIdempotencyConflict) {
			prior, loadErr := h.journal.LoadStartByKey(context.WithoutCancel(ctx), request.IdempotencyKey)
			if loadErr == nil && prior.Record.RequestDigest == requestDigest && sameIdentity(identity, prior.Record.Identity) {
				finished, materializeErr := h.materializeStart(ctx, prior)
				return h.startResult(ctx, finished, runtime.IdempotencyReplayed, materializeErr)
			}
		}
		return StartRunResult{}, fmt.Errorf("record workflow host start: %w", err)
	}
	finished, err := h.materializeStart(ctx, snapshot)
	if err != nil {
		return h.startResult(ctx, finished, outcome, err)
	}
	h.observe(request.RunID, "workflow.started", map[string]string{"phase": string(finished.Phase), "outcome": string(outcome)})
	return h.startResult(ctx, finished, outcome, nil)
}

func (h *Host) requireStartReady(allowRecovery bool) error {
	if !allowRecovery {
		return h.requireReady()
	}
	if h == nil {
		return ErrInvalidHost
	}
	h.mu.RLock()
	started := h.health.Started
	h.mu.RUnlock()
	if !started {
		return ErrHostNotReady
	}
	return nil
}

func cloneExecutionPlan(input *compile.ExecutionPlan) (*compile.ExecutionPlan, error) {
	if input == nil {
		return nil, errors.New("resolved execution plan is required")
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var result compile.ExecutionPlan
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("resolved execution plan contains trailing JSON")
		}
		return nil, err
	}
	return &result, nil
}

// authorizeStartReplay authenticates the caller again before any durable
// allow, deny, or confirmation result is returned. IdentityRequest is merely
// caller-supplied context; only IdentityProvider binds the current principal.
// W05-T02 may replace exact authority equality with a richer delegated-access
// decision, but this narrow host binding deliberately fails closed.
func (h *Host) authorizeStartReplay(ctx context.Context, request IdentityRequest, prior hoststate.IdentityBinding) error {
	current, err := h.bindIdentity(ctx, request)
	if err != nil {
		return err
	}
	prior = normalizeIdentity(prior)
	if !sameIdentity(current, prior) {
		return fmt.Errorf("%w: current caller is not authorized to replay this start key", ErrPolicyDenied)
	}
	return nil
}

func sameIdentity(left, right hoststate.IdentityBinding) bool {
	leftJSON, leftErr := json.Marshal(normalizeIdentity(left))
	rightJSON, rightErr := json.Marshal(normalizeIdentity(right))
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}

func (h *Host) bindIdentity(ctx context.Context, request IdentityRequest) (hoststate.IdentityBinding, error) {
	request = normalizeIdentityRequest(request)
	identity, err := h.identity.BindIdentity(ctx, request)
	if err != nil {
		return hoststate.IdentityBinding{}, fmt.Errorf("bind workflow identity: %w", err)
	}
	identity = normalizeIdentity(identity)
	if err := identity.Validate(); err != nil {
		return hoststate.IdentityBinding{}, fmt.Errorf("invalid workflow identity binding: %w", err)
	}
	if request.RunScope != nil && !request.RunScope.Matches(identity.RunScope) {
		return hoststate.IdentityBinding{}, fmt.Errorf("%w: identity provider returned a different run scope", ErrPolicyDenied)
	}
	if request.ExecutionTarget != nil {
		if identity.ExecutionTarget == nil || !request.ExecutionTarget.Matches(*identity.ExecutionTarget) {
			return hoststate.IdentityBinding{}, fmt.Errorf("%w: identity provider returned an execution target outside the selector", ErrPolicyDenied)
		}
	}
	return identity, nil
}

func (h *Host) startResult(ctx context.Context, snapshot hoststate.StartSnapshot, outcome runtime.IdempotencyOutcome, resultErr error) (StartRunResult, error) {
	bound := snapshot.Record.Run
	result := StartRunResult{Bound: &bound, Decision: snapshot.Record.Decision, Facts: snapshot.Record.Facts, Outcome: outcome, Phase: snapshot.Phase, DryRun: snapshot.Record.DryRun, Durability: graph.DurabilitySteps}
	if !snapshot.Record.DryRun && phaseHasRun(snapshot.Phase) {
		run, err := h.state.LoadRun(context.WithoutCancel(ctx), snapshot.Record.Run.ID)
		if err == nil {
			result.Run = &run
		} else if resultErr == nil {
			resultErr = err
		}
	}
	return result, resultErr
}

func (h *Host) materializeStart(ctx context.Context, snapshot hoststate.StartSnapshot) (hoststate.StartSnapshot, error) {
	if err := snapshot.Validate(); err != nil {
		return snapshot, err
	}
	for {
		if snapshot.Phase.Terminal() {
			if snapshot.Phase == hoststate.StartPinsRejected {
				if err := h.verifyRejectedPinnedStart(ctx, snapshot.Record); err != nil {
					return snapshot, err
				}
				return snapshot, fmt.Errorf("%w: pinned workflow start was rejected", runtime.ErrReuseDenied)
			}
			if !snapshot.Record.DryRun {
				run, err := h.state.LoadRun(ctx, snapshot.Record.Run.ID)
				if err != nil {
					return snapshot, err
				}
				if run.Status == runtime.RunPending {
					return snapshot, errors.New("materialize workflow start: terminal journal still has a pending run")
				}
			}
			return snapshot, nil
		}
		if snapshot.Record.DryRun {
			if snapshot.Phase != hoststate.StartRecorded {
				return snapshot, fmt.Errorf("unsupported dry-run start phase %q", snapshot.Phase)
			}
			advanced, err := h.advance(ctx, snapshot, hoststate.StartDryRunComplete)
			if err != nil {
				return snapshot, err
			}
			snapshot = advanced
			continue
		}

		switch snapshot.Phase {
		case hoststate.StartRecorded:
			if activation := snapshot.Record.Activation; activation != nil {
				inputs := snapshot.Record.Run.InputsRef
				_, _, err := h.state.RecordExternalActivation(context.WithoutCancel(ctx), runtime.ExternalActivationRequest{
					ActivationID: activation.ActivationID, IdempotencyKey: activation.IdempotencyKey,
					RequestedRunID: snapshot.Record.Run.ID, Plan: snapshot.Record.Run.Plan,
					Inputs: &inputs, OccurredAt: activation.OccurredAt,
				})
				if err != nil {
					return snapshot, err
				}
			}
			_, _, err := runtime.StartBoundRun(context.WithoutCancel(ctx), h.state, snapshot.Record.Run, snapshot.Record.StartKey)
			if err != nil {
				return snapshot, err
			}
			advanced, err := h.advance(context.WithoutCancel(ctx), snapshot, hoststate.StartRunCreated)
			if err != nil {
				return snapshot, err
			}
			snapshot = advanced

		case hoststate.StartRunCreated:
			if err := h.materializeNodes(context.WithoutCancel(ctx), snapshot.Record); err != nil {
				return snapshot, err
			}
			advanced, err := h.advance(context.WithoutCancel(ctx), snapshot, hoststate.StartNodesMaterialized)
			if err != nil {
				return snapshot, err
			}
			snapshot = advanced

		case hoststate.StartNodesMaterialized:
			if err := h.bindStartPins(context.WithoutCancel(ctx), snapshot.Record); err != nil {
				if errors.Is(err, runtime.ErrReuseDenied) || errors.Is(err, runtime.ErrInvalidReuse) || errors.Is(err, runtime.ErrNotFound) {
					if rejectErr := h.rejectPinnedStart(context.WithoutCancel(ctx), snapshot.Record); rejectErr != nil {
						return snapshot, errors.Join(err, rejectErr)
					}
					advanced, advanceErr := h.advance(context.WithoutCancel(ctx), snapshot, hoststate.StartPinsRejected)
					if advanceErr != nil {
						return snapshot, errors.Join(err, advanceErr)
					}
					snapshot = advanced
				}
				return snapshot, err
			}
			advanced, err := h.advance(context.WithoutCancel(ctx), snapshot, hoststate.StartPinsBound)
			if err != nil {
				return snapshot, err
			}
			snapshot = advanced

		case hoststate.StartPinsBound:
			run, err := h.state.LoadRun(context.WithoutCancel(ctx), snapshot.Record.Run.ID)
			if err != nil {
				return snapshot, err
			}
			if run.Status == runtime.RunPending {
				transition, transitionErr := h.state.TransitionRun(context.WithoutCancel(ctx), runtime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: runtime.RunRunning, At: maxTime(h.now(), run.UpdatedAt)})
				if transitionErr != nil {
					if !errors.Is(transitionErr, runtime.ErrCASMismatch) {
						return snapshot, transitionErr
					}
					run, transitionErr = h.state.LoadRun(context.WithoutCancel(ctx), run.ID)
					if transitionErr != nil {
						return snapshot, transitionErr
					}
				} else {
					run = transition.Snapshot
				}
			}
			if run.Status != runtime.RunRunning {
				return snapshot, fmt.Errorf("materialize workflow start: run is %s", run.Status)
			}
			if readyErr := h.readyRootNodes(context.WithoutCancel(ctx), snapshot.Record); readyErr != nil {
				return snapshot, readyErr
			}
			advanced, err := h.advance(context.WithoutCancel(ctx), snapshot, hoststate.StartRunning)
			if err != nil {
				return snapshot, err
			}
			snapshot = advanced

		default:
			return snapshot, fmt.Errorf("unsupported workflow start phase %q", snapshot.Phase)
		}
	}
}

func (h *Host) bindStartPins(ctx context.Context, record hoststate.StartRecord) error {
	if len(record.Pins) == 0 {
		return nil
	}
	if h.pins == nil {
		return fmt.Errorf("%w: pinned outputs require a configured reuse authorizer", runtime.ErrReuseDenied)
	}
	authority, err := startReuseAuthority(record.Identity)
	if err != nil {
		return fmt.Errorf("%w: derive pin reuse authority: %w", runtime.ErrInvalidReuse, err)
	}
	for _, pin := range record.Pins {
		_, err := h.pins.Bind(ctx, runtime.PinNodeRequest{
			Target: runtime.NodeInvocationID{RunID: record.Run.ID, NodeID: pin.NodeID}, Outputs: pin.Outputs,
			Authority: authority, IdempotencyKey: "start-pin:" + record.StartKey + ":" + pin.NodeID, At: record.RecordedAt,
		})
		if err != nil {
			return fmt.Errorf("bind start pin %s: %w", pin.NodeID, err)
		}
	}
	return nil
}

func (h *Host) rejectPinnedStart(ctx context.Context, record hoststate.StartRecord) error {
	run, err := h.state.LoadRun(ctx, record.Run.ID)
	if err != nil {
		return err
	}
	if run.Status == runtime.RunCanceled {
		_, failures, recoverErr := h.cancellation.Recover(ctx, runtime.CancellationIntentQuery{RunID: record.Run.ID, Limit: h.batchLimit})
		if recoverErr != nil {
			return fmt.Errorf("recover rejected pinned start cancellation: %w", recoverErr)
		}
		if len(failures) != 0 {
			return fmt.Errorf("recover rejected pinned start cancellation: %w", errors.Join(failures...))
		}
		return h.verifyRejectedPinnedStart(ctx, record)
	}
	if run.Status != runtime.RunPending {
		return fmt.Errorf("reject pinned start: run is unexpectedly %s", run.Status)
	}
	result, failures, err := h.cancellation.Request(ctx, runtime.RequestRunCancellationRequest{
		RunID: record.Run.ID, ExpectedGeneration: run.Generation,
		IdempotencyKey: "pin-rejection:" + strings.TrimPrefix(values.SHA256Digest([]byte(record.StartKey)), "sha256:"),
		Reason:         runtime.Failure{Code: "pin_rejected", Message: "pinned outputs were rejected before workflow admission"},
		At:             maxTime(h.now(), run.UpdatedAt),
	})
	if err != nil {
		return fmt.Errorf("cancel rejected pinned start: %w", err)
	}
	if result.Run.Status != runtime.RunCanceled {
		return errors.New("cancel rejected pinned start: runtime run is not terminal")
	}
	if len(failures) != 0 {
		return fmt.Errorf("cancel rejected pinned start: %w", errors.Join(failures...))
	}
	return h.verifyRejectedPinnedStart(ctx, record)
}

func (h *Host) verifyRejectedPinnedStart(ctx context.Context, record hoststate.StartRecord) error {
	run, err := h.state.LoadRun(ctx, record.Run.ID)
	if err != nil {
		return err
	}
	if run.Status != runtime.RunCanceled {
		return errors.New("rejected pins require a canceled runtime run")
	}
	handlers := runtime.CompensationHandlers(record.Plan.Graph)
	for _, planNode := range record.Plan.Graph.Nodes {
		if _, dormant := handlers[graph.NormalizeID(planNode.ID)]; dormant {
			continue
		}
		node, loadErr := h.state.LoadNodeInvocation(ctx, runtime.NodeInvocationID{RunID: run.ID, NodeID: planNode.ID})
		if loadErr != nil {
			return fmt.Errorf("load rejected pinned start node %s: %w", planNode.ID, loadErr)
		}
		if !node.Status.Terminal() || node.Lease != nil {
			return fmt.Errorf("rejected pinned start node %s remains %s or leased", planNode.ID, node.Status)
		}
	}
	return nil
}

func startReuseAuthority(identity hoststate.IdentityBinding) (runtime.ReuseAuthority, error) {
	identity = normalizeIdentity(identity)
	encodedIdentity, err := json.Marshal(identity)
	if err != nil {
		return runtime.ReuseAuthority{}, err
	}
	encodedGrants, err := json.Marshal(identity.Grants)
	if err != nil {
		return runtime.ReuseAuthority{}, err
	}
	attributes := map[string]string{
		"identity_digest":  values.SHA256Digest(encodedIdentity),
		"source_authority": identity.SourceAuthority,
		"trust":            identity.Trust,
		"grants":           string(encodedGrants),
		"scope_version":    identity.RunScope.Version,
		"scope_kind":       string(identity.RunScope.Kind),
		"scope_id":         identity.RunScope.ID,
	}
	for key, value := range identity.RunScope.Attributes {
		attributes["scope.attribute."+key] = value
	}
	for key, value := range identity.Extension {
		attributes["identity.extension."+key] = value
	}
	if target := identity.ExecutionTarget; target != nil {
		encodedTarget, marshalErr := json.Marshal(target)
		if marshalErr != nil {
			return runtime.ReuseAuthority{}, marshalErr
		}
		encodedCapabilities, marshalErr := json.Marshal(target.Capabilities)
		if marshalErr != nil {
			return runtime.ReuseAuthority{}, marshalErr
		}
		attributes["target_digest"] = values.SHA256Digest(encodedTarget)
		attributes["target_version"] = target.Version
		attributes["target_id"] = target.ID
		attributes["target_kind"] = string(target.Kind)
		attributes["target_capabilities"] = string(encodedCapabilities)
		attributes["target_sandbox_mode"] = string(target.Sandbox.Mode)
		attributes["target_readiness"] = string(target.Readiness.State)
		attributes["target_provenance_authority"] = target.Provenance.Authority
		attributes["target_provenance_reference"] = target.Provenance.Reference
		if target.CWD != "" {
			attributes["target_cwd"] = target.CWD
		}
		if target.WorkspaceHandle != "" {
			attributes["target_workspace_handle"] = target.WorkspaceHandle
		}
		if target.Sandbox.Profile != "" {
			attributes["target_sandbox_profile"] = target.Sandbox.Profile
		}
		if target.Lease != nil {
			attributes["target_lease_id"] = target.Lease.ID
		}
		for key, value := range target.Labels {
			attributes["target.label."+key] = value
		}
	}
	authority := runtime.ReuseAuthority{Principal: identity.Principal, Scope: string(identity.RunScope.Kind) + ":" + identity.RunScope.ID, Attributes: attributes}
	if err := authority.Validate(); err != nil {
		return runtime.ReuseAuthority{}, fmt.Errorf("immutable identity exceeds reuse policy metadata bounds: %w", err)
	}
	return authority, nil
}

func (h *Host) advance(ctx context.Context, snapshot hoststate.StartSnapshot, next hoststate.StartPhase) (hoststate.StartSnapshot, error) {
	return h.journal.AdvanceStart(ctx, hoststate.AdvanceStartRequest{RunID: snapshot.Record.Run.ID, ExpectedGeneration: snapshot.Generation, From: snapshot.Phase, To: next, At: maxTime(h.now(), snapshot.UpdatedAt)})
}

func (h *Host) materializeNodes(ctx context.Context, record hoststate.StartRecord) error {
	inputs, err := h.state.LoadValues(ctx, record.Run.InputsRef)
	if err != nil {
		return fmt.Errorf("load workflow inputs: %w", err)
	}
	nodes := append([]graph.Node(nil), record.Plan.Graph.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	for _, node := range nodes {
		if _, dormant := runtime.CompensationHandlers(record.Plan.Graph)[graph.NormalizeID(node.ID)]; dormant {
			continue
		}
		id := runtime.NodeInvocationID{RunID: record.Run.ID, NodeID: node.ID}
		if _, loadErr := h.state.LoadNodeInvocation(ctx, id); loadErr == nil {
			continue
		} else if !errors.Is(loadErr, runtime.ErrNotFound) {
			return loadErr
		}
		var inputRef *values.ValueSetRef
		if !hasDependencies(record.Plan.Graph, node.ID) && node.ForEach == nil {
			bound, bindErr := bindNodeInputs(node, inputs, record.Run.ID)
			if bindErr != nil {
				return fmt.Errorf("bind root node %s: %w", node.ID, bindErr)
			}
			ref, saveErr := h.state.SaveValues(ctx, runtime.SaveValuesRequest{Owner: runtime.ValueOwner{Kind: "node-inputs", RunID: record.Run.ID, Invocation: &id}, Values: bound})
			if saveErr != nil {
				return saveErr
			}
			inputRef = &ref
		}
		at := record.Run.CreatedAt
		_, createErr := h.state.CreateNodeInvocation(ctx, runtime.CreateNodeInvocationRequest{Snapshot: runtime.NodeInvocationSnapshot{ID: id, Status: runtime.NodePending, Inputs: inputRef, CreatedAt: at, UpdatedAt: at}})
		if createErr != nil && !errors.Is(createErr, runtime.ErrAlreadyExists) {
			return createErr
		}
	}
	return nil
}

func (h *Host) readyRootNodes(ctx context.Context, record hoststate.StartRecord) error {
	inputs, err := h.state.LoadValues(ctx, record.Run.InputsRef)
	if err != nil {
		return err
	}
	coordinator := runtime.NewProgressionCoordinator(h.state, nil)
	nodes := append([]graph.Node(nil), record.Plan.Graph.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	for _, node := range nodes {
		if _, dormant := runtime.CompensationHandlers(record.Plan.Graph)[graph.NormalizeID(node.ID)]; dormant {
			continue
		}
		if hasDependencies(record.Plan.Graph, node.ID) {
			continue
		}
		id := runtime.NodeInvocationID{RunID: record.Run.ID, NodeID: node.ID}
		current, loadErr := h.state.LoadNodeInvocation(ctx, id)
		if loadErr != nil {
			return loadErr
		}
		if current.Status == runtime.NodeReady || current.Status == runtime.NodeSkipped {
			continue
		}
		_, progressErr := coordinator.ProgressNode(ctx, runtime.ProgressNodeRequest{InvocationID: id, Rule: node.ReadyWhen, Predicate: node.If, ExpressionContext: values.ExpressionContext{Inputs: inputs}, At: maxTime(h.now(), current.UpdatedAt)})
		if progressErr != nil {
			if errors.Is(progressErr, runtime.ErrCASMismatch) {
				replayed, reloadErr := h.state.LoadNodeInvocation(ctx, id)
				if reloadErr == nil && (replayed.Status == runtime.NodeReady || replayed.Status == runtime.NodeSkipped) {
					continue
				}
			}
			return progressErr
		}
	}
	return nil
}

func bindNodeInputs(node graph.Node, inputs values.ValueSet, runID runtime.RunID) (values.ValueSet, error) {
	engine := values.NewExpressionEngine()
	names := make([]string, 0, len(node.InputBindings))
	for name := range node.InputBindings {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make(values.ValueSet, len(names))
	for _, name := range names {
		value, err := engine.EvaluateBinding(node.InputBindings[name], values.ExpressionContext{Inputs: inputs}, values.ExpressionOptions{}, values.Metadata{
			Producer:  values.Producer{Kind: "hadron_host_binding", Reference: string(runID) + "/" + node.ID, Output: name},
			MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if err != nil {
			return nil, err
		}
		result[name] = value
	}
	return result, result.Validate()
}

func hasDependencies(value graph.Graph, nodeID string) bool {
	for _, node := range value.Nodes {
		if node.ID == nodeID && (len(node.Needs) != 0 || node.Finally != nil) {
			return true
		}
		for _, route := range node.Catch {
			for _, target := range route.Targets {
				if target == nodeID {
					return true
				}
			}
		}
		if node.Switch != nil {
			for _, target := range node.Switch.Default {
				if target == nodeID {
					return true
				}
			}
			for _, arm := range node.Switch.Arms {
				for _, target := range arm.Targets {
					if target == nodeID {
						return true
					}
				}
			}
		}
	}
	for _, edge := range value.Edges {
		if edge.To == nodeID {
			return true
		}
	}
	return false
}

func (h *Host) InspectRun(ctx context.Context, runID runtime.RunID) (InspectRunResult, error) {
	if err := h.requireReady(); err != nil {
		return InspectRunResult{}, err
	}
	run, err := h.state.LoadRun(ctx, runID)
	if err != nil {
		return InspectRunResult{}, err
	}
	binding, err := h.journal.LoadStart(ctx, runID)
	if err != nil {
		return InspectRunResult{}, err
	}
	nodes, err := h.journal.ListRunNodes(ctx, runID)
	if err != nil {
		return InspectRunResult{}, err
	}
	events, err := h.state.ListEvents(ctx, runtime.EventQuery{RunID: runID})
	if err != nil {
		return InspectRunResult{}, err
	}
	decisions, err := h.journal.ListPolicyDecisions(ctx, runID)
	if err != nil {
		return InspectRunResult{}, err
	}
	if binding.Record.Snapshot == nil {
		return InspectRunResult{}, errors.New("inspect workflow run: durable plan snapshot is unavailable")
	}
	planMetadata, err := binding.Record.Snapshot.Metadata()
	if err != nil {
		return InspectRunResult{}, fmt.Errorf("inspect workflow plan snapshot: %w", err)
	}
	// Raw exact source remains journal-internal. Ordinary inspection receives
	// only the safe metadata projection above.
	binding.Record.Snapshot = nil
	return InspectRunResult{Run: run, Binding: binding, Plan: planMetadata, Nodes: nodes, Events: events, Decisions: decisions}, nil
}

func (h *Host) CancelRun(ctx context.Context, request CancelRunRequest) (runtime.RequestRunCancellationResult, []error, error) {
	if err := h.requireReady(); err != nil {
		return runtime.RequestRunCancellationResult{}, nil, err
	}
	if ctx == nil {
		return runtime.RequestRunCancellationResult{}, nil, errors.New("cancel workflow: context is required")
	}
	if h.cancellation == nil {
		return runtime.RequestRunCancellationResult{}, nil, runtime.ErrCancellationUnsupported
	}
	reason := request.Reason
	if strings.TrimSpace(reason) == "" {
		reason = "canceled by Hadron workflow host"
	}
	binding, _, err := h.journal.BindCancellation(context.WithoutCancel(ctx), hoststate.BindCancellationRequest{
		Intent:    hoststate.CancellationIntent{RunID: request.RunID, IdempotencyKey: request.IdempotencyKey, Reason: reason, RequestedAt: request.At},
		DefaultAt: h.now(),
	})
	if err != nil {
		return runtime.RequestRunCancellationResult{}, nil, err
	}
	return h.applyCancellation(ctx, binding)
}

func (h *Host) applyCancellation(ctx context.Context, binding hoststate.CancellationBinding) (runtime.RequestRunCancellationResult, []error, error) {
	start, err := h.journal.LoadStart(ctx, binding.Intent.RunID)
	if err != nil {
		return runtime.RequestRunCancellationResult{}, nil, err
	}
	workflow := start.Record.Plan.Graph
	var lastCAS error
	for attempt := 0; attempt < cancellationCASLimit; attempt++ {
		if err := ctx.Err(); err != nil {
			return runtime.RequestRunCancellationResult{}, nil, err
		}
		request, err := h.journal.PrepareCancellation(ctx, binding)
		if err != nil {
			return runtime.RequestRunCancellationResult{}, nil, err
		}
		descendants, err := h.cancellationDescendants(ctx, binding.Intent.RunID)
		if err != nil {
			return runtime.RequestRunCancellationResult{}, nil, err
		}
		finalizers, err := runtime.PlanFinalizerScopes(workflow, binding.Intent.RunID)
		if err != nil {
			return runtime.RequestRunCancellationResult{}, nil, err
		}
		hasFinalizers := len(finalizers) != 0
		for _, descendant := range descendants {
			scopes, planErr := runtime.PlanFinalizerScopes(descendant.Graph, descendant.Run.ID)
			if planErr != nil {
				return runtime.RequestRunCancellationResult{}, nil, planErr
			}
			hasFinalizers = hasFinalizers || len(scopes) != 0
		}
		control, hasControl := h.state.(runtime.ControlFlowStore)
		var result runtime.RequestRunCancellationResult
		var failures []error
		if !hasControl || nilInterface(control) {
			if hasFinalizers {
				return runtime.RequestRunCancellationResult{}, nil, fmt.Errorf("%w: finalizer-aware cancellation requires a control-flow state store", ErrInvalidHost)
			}
			result, failures, err = h.cancellation.Request(ctx, request)
		} else {
			controlled, controlErr := runtime.NewControlFlowCoordinator(h.state, control, nil).RequestRunCancellationTree(ctx, workflow, request, descendants)
			if controlErr == nil {
				result = controlled.Cancellation
				runIDs := make([]runtime.RunID, 0, len(descendants)+1)
				runIDs = append(runIDs, request.RunID)
				for _, descendant := range descendants {
					runIDs = append(runIDs, descendant.Run.ID)
				}
				for _, runID := range runIDs {
					_, recoveredFailures, recoverErr := h.cancellation.Recover(ctx, runtime.CancellationIntentQuery{RunID: runID})
					failures = append(failures, recoveredFailures...)
					if recoverErr != nil {
						controlErr = errors.Join(controlErr, recoverErr)
					}
				}
			}
			err = controlErr
		}
		if errors.Is(err, runtime.ErrCASMismatch) || errors.Is(err, runtime.ErrIdempotencyConflict) {
			lastCAS = err
			if contextErr := ctx.Err(); contextErr != nil {
				return runtime.RequestRunCancellationResult{}, nil, contextErr
			}
			continue
		}
		return result, failures, err
	}
	return runtime.RequestRunCancellationResult{}, nil, fmt.Errorf("host cancellation CAS retry limit reached; durable intent remains pending: %w", lastCAS)
}

func (h *Host) cancellationDescendants(ctx context.Context, root runtime.RunID) ([]runtime.CancellationDescendantGraph, error) {
	seen := map[runtime.RunID]bool{root: true}
	result := make([]runtime.CancellationDescendantGraph, 0)
	var visit func(runtime.RunID) error
	visit = func(parent runtime.RunID) error {
		links, err := h.state.ListChildRuns(ctx, parent)
		if err != nil {
			return err
		}
		for _, link := range links {
			if link.Policy != graph.ParentCloseCancel {
				continue
			}
			if seen[link.ChildRunID] {
				return fmt.Errorf("%w: direct-cancel child graph contains a cycle or duplicate descendant", ErrInvalidHost)
			}
			seen[link.ChildRunID] = true
			if nilInterface(h.childDefs) {
				return fmt.Errorf("%w: child cancellation requires the durable child-definition source", ErrInvalidHost)
			}
			child, err := h.childDefs.LoadChildRunRequest(ctx, link.ChildRunID)
			if err != nil {
				return fmt.Errorf("load child workflow %s for cancellation: %w", link.ChildRunID, err)
			}
			if child.ChildRunID != link.ChildRunID {
				return fmt.Errorf("%w: child cancellation start record belongs to another run", ErrInvalidHost)
			}
			run, err := h.state.LoadRun(ctx, link.ChildRunID)
			if err != nil {
				return err
			}
			result = append(result, runtime.CancellationDescendantGraph{Run: run, Graph: child.Definition.Graph})
			if err := visit(link.ChildRunID); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(root); err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Run.ID < result[j].Run.ID })
	return result, nil
}

func (h *Host) ResumeWait(ctx context.Context, command runtime.ResumeCommand) (runtime.ResumeWaitResult, error) {
	if err := h.requireReady(); err != nil {
		return runtime.ResumeWaitResult{}, err
	}
	if h.waits == nil {
		return runtime.ResumeWaitResult{}, runtime.ErrWaitUnresumable
	}
	return h.waits.Resume(ctx, command)
}

func (h *Host) ExplainRun(ctx context.Context, runID runtime.RunID) (ExplainRunResult, error) {
	inspected, err := h.InspectRun(ctx, runID)
	if err != nil {
		return ExplainRunResult{}, err
	}
	blocked := make([]runtime.BlockedReason, 0)
	for _, node := range inspected.Nodes {
		if node.Blocked != nil {
			blocked = append(blocked, *node.Blocked)
		}
	}
	dryTruth := "unavailable: one or more adapters lack an explicit side-effect-free dry-run assertion"
	if inspected.Binding.Record.Facts.DryRunAvailable {
		dryTruth = "available: every participating adapter was explicitly approved for side-effect-free dry-run"
	}
	return ExplainRunResult{Run: inspected.Run, Facts: inspected.Binding.Record.Facts, Decision: inspected.Binding.Record.Decision, Decisions: inspected.Decisions, Nodes: inspected.Nodes, Blocked: blocked, DryRunTruth: dryTruth, Plan: inspected.Plan}, nil
}

func maxTime(candidate, floor time.Time) time.Time {
	candidate = candidate.UTC()
	if candidate.Before(floor) {
		return floor.UTC()
	}
	return candidate
}

func normalizeDecision(decision hoststate.PolicyDecision, runID runtime.RunID, at time.Time) hoststate.PolicyDecision {
	decision.Attributes = cloneStringMap(decision.Attributes)
	if decision.RunID == "" {
		decision.RunID = runID
	}
	if decision.Operation == "" {
		decision.Operation = "start"
	}
	if decision.DecidedAt.IsZero() {
		decision.DecidedAt = at
	}
	return decision
}

func (h *Host) policyFacts(ctx context.Context, runID runtime.RunID, plan *compile.ExecutionPlan, identity hoststate.IdentityBinding) (hoststate.PolicyFacts, error) {
	ref := runtime.PlanRef{ID: plan.ID, Version: plan.Graph.Version, Digest: plan.Digest, SchemaVersion: plan.SchemaVersion}
	if err := ref.Validate(); err != nil {
		return hoststate.PolicyFacts{}, err
	}
	effects := make(map[graph.Effect]struct{})
	capabilities := make(map[string]struct{})
	blast := make(map[string]int)
	targets := map[string]graph.ExecutionTargetRequirements{"$graph": plan.Graph.Target}
	unresolvedCalls := make([]string, 0)
	for _, capability := range plan.Graph.Target.Capabilities {
		capabilities[capability] = struct{}{}
	}
	dryAvailable := !nilInterface(h.dryRun)
	for _, node := range plan.Graph.Nodes {
		_, spec, err := stepkind.Resolve(h.registry, node.Kind, node.KindVersion)
		if err != nil {
			return hoststate.PolicyFacts{}, err
		}
		nodeEffects := make(map[graph.Effect]struct{}, len(spec.Effects)+len(node.Effects))
		for _, effect := range spec.Effects {
			effects[effect] = struct{}{}
			nodeEffects[effect] = struct{}{}
		}
		for _, capability := range spec.RequiredCapabilities {
			capabilities[capability] = struct{}{}
		}
		for _, effect := range node.Effects {
			effects[effect] = struct{}{}
			nodeEffects[effect] = struct{}{}
		}
		for effect := range nodeEffects {
			blast[string(effect)]++
		}
		for _, capability := range node.Target.Capabilities {
			capabilities[capability] = struct{}{}
		}
		targets[node.ID] = node.Target
		if node.Kind == "call" {
			unresolvedCalls = append(unresolvedCalls, node.ID)
			blast["unresolved_call"]++
		}
		if dryAvailable {
			supported, supportErr := h.dryRun.SupportsDryRun(ctx, spec)
			if supportErr != nil {
				return hoststate.PolicyFacts{}, supportErr
			}
			dryAvailable = supported
		}
	}
	effectList := make(graph.EffectSet, 0, len(effects))
	for effect := range effects {
		effectList = append(effectList, effect)
	}
	sort.Slice(effectList, func(i, j int) bool { return effectList[i] < effectList[j] })
	capabilityList := make([]string, 0, len(capabilities))
	for capability := range capabilities {
		capabilityList = append(capabilityList, capability)
	}
	sort.Strings(capabilityList)
	sort.Strings(unresolvedCalls)
	_, mutate := effects[graph.EffectMutate]
	_, destructive := effects[graph.EffectDestructive]
	scope := identity.RunScope.Clone()
	var target *hoststate.ExecutionTarget
	if identity.ExecutionTarget != nil {
		cloned := identity.ExecutionTarget.Clone()
		target = &cloned
	}
	if err := hoststate.ValidateExecutionTargetBinding(target, capabilityList, targets); err != nil {
		return hoststate.PolicyFacts{}, fmt.Errorf("%w: %w", ErrExecutionTarget, err)
	}
	facts := hoststate.PolicyFacts{Operation: "start", RunID: runID, Plan: ref, Identity: identity, RunScope: scope, ExecutionTarget: target, Effects: effectList, RequiredCapabilities: capabilityList, TargetRequirements: targets, UnresolvedCallNodes: unresolvedCalls, NodeCount: len(plan.Graph.Nodes), BlastRadius: blast, DryRunAvailable: dryAvailable, ConfirmationAdvised: mutate || destructive || len(unresolvedCalls) != 0}
	return facts, facts.Validate()
}

func digestStartIntent(request StartRunRequest, inputHash string) (string, error) {
	request.Identity = normalizeIdentityRequest(request.Identity)
	activation := request.Activation
	if activation != nil {
		copyActivation := *activation
		copyActivation.OccurredAt = copyActivation.OccurredAt.UTC()
		activation = &copyActivation
	}
	intent := struct {
		RunID          runtime.RunID                `json:"run_id"`
		Definition     graph.DefinitionRef          `json:"definition"`
		InputHash      string                       `json:"input_hash"`
		IdempotencyKey string                       `json:"idempotency_key"`
		Identity       IdentityRequest              `json:"identity"`
		DryRun         bool                         `json:"dry_run"`
		Activation     *hoststate.ActivationBinding `json:"activation,omitempty"`
		Pins           []hoststate.StartPin         `json:"pins,omitempty"`
	}{request.RunID, request.Definition, inputHash, request.IdempotencyKey, request.Identity, request.DryRun, activation, request.Pins}
	encoded, err := json.Marshal(intent)
	if err != nil {
		return "", fmt.Errorf("encode workflow start intent: %w", err)
	}
	return values.SHA256Digest(encoded), nil
}

func clonePolicyFacts(input hoststate.PolicyFacts) (hoststate.PolicyFacts, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return hoststate.PolicyFacts{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var result hoststate.PolicyFacts
	if err := decoder.Decode(&result); err != nil {
		return hoststate.PolicyFacts{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return hoststate.PolicyFacts{}, errors.New("policy facts contain trailing JSON")
	}
	return result, nil
}

func policyDecisionID(key string) string {
	digest := values.SHA256Digest([]byte(key))
	return "policy-" + strings.TrimPrefix(digest, "sha256:")
}

func phaseHasRun(phase hoststate.StartPhase) bool {
	return phase == hoststate.StartRunCreated || phase == hoststate.StartNodesMaterialized || phase == hoststate.StartPinsBound || phase == hoststate.StartPinsRejected || phase == hoststate.StartRunning
}

func canonicalStartPins(input []hoststate.StartPin) []hoststate.StartPin {
	result := append([]hoststate.StartPin(nil), input...)
	sort.Slice(result, func(i, j int) bool { return result[i].NodeID < result[j].NodeID })
	if len(result) == 0 {
		return nil
	}
	return result
}

func validateStartPins(input []hoststate.StartPin) error {
	for index, pin := range input {
		if err := pin.Validate(); err != nil {
			return fmt.Errorf("start pin[%d]: %w", index, err)
		}
		if index > 0 && input[index-1].NodeID == pin.NodeID {
			return fmt.Errorf("start pin node %q is duplicated", pin.NodeID)
		}
	}
	return nil
}
