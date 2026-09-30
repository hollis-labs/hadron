package appworkflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	calladapter "github.com/hollis-labs/go-workflow/adapters/call"
	"github.com/hollis-labs/go-workflow/compile"
	"github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/stepkind"
	"github.com/hollis-labs/go-workflow/values"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
)

const (
	// CodeChildRunPolicyDenied is the call step failure code when the host
	// refuses a call-started child run.
	CodeChildRunPolicyDenied = "child_run_policy_denied"
	childRunPolicyKeyPrefix  = "child-run:"
	maxChildLineageDepth     = 64
)

var _ calladapter.ChildRunExecutor = (*Host)(nil)

// StartChildRun implements calladapter.ChildRunExecutor for call@v1 mode run.
// Before the child run is created it passes the same gate as a top-level
// start: the child plan is validated against the Host kind registry and
// verifier catalog, policy facts are computed for the child plan under the
// root run's bound identity (execution-target capability checks included),
// the Host PolicyEvaluator decides, and the facts and decision are persisted
// through the journal's policy-evaluation record keyed by the child run ID.
// Exact retries replay the persisted decision. Only an allowed child reaches
// the journal's atomic child-run creation.
//
// A PolicyConfirm decision is satisfied only when the root start was itself
// confirmed (its recorded decision is PolicyConfirm). Every start whose plan
// contains a call node is confirmation-advised for its unresolved children,
// so that confirmation is the operator's consent to the child's effects; a
// root that was allowed without confirmation cannot launch a child that
// policy says needs it.
func (h *Host) StartChildRun(ctx context.Context, request calladapter.ChildRunRequest) (calladapter.ChildRunResult, error) {
	if ctx == nil {
		return calladapter.ChildRunResult{}, errors.New("start child run: context is required")
	}
	if h == nil {
		return calladapter.ChildRunResult{}, ErrInvalidHost
	}
	runs, ok := h.journal.(calladapter.ChildRunExecutor)
	if !ok || nilInterface(runs) {
		return calladapter.ChildRunResult{}, childPolicyRefusal("the workflow journal cannot create child runs", ErrInvalidHost)
	}
	if err := h.authorizeChildRun(ctx, request); err != nil {
		return calladapter.ChildRunResult{}, err
	}
	return runs.StartChildRun(ctx, request)
}

func (h *Host) authorizeChildRun(ctx context.Context, request calladapter.ChildRunRequest) error {
	root, err := h.rootStart(ctx, runtime.RunID(request.Parent.RunID), make(map[runtime.RunID]struct{}), 0)
	if err != nil {
		return fmt.Errorf("load root start for child run %s: %w", request.ChildRunID, err)
	}
	identity := normalizeIdentity(root.Record.Identity)
	plan := &compile.ExecutionPlan{
		SchemaVersion: request.Plan.SchemaVersion, ID: request.Plan.ID, Digest: request.Plan.Digest,
		Definition: request.Definition.Definition, Graph: request.Definition.Graph,
	}
	validationOptions := compile.ValidationOptions{StepKinds: h.registry, Verifiers: h.verifiers}
	if resolver, ok := h.definitions.(compile.DefinitionResolver); ok {
		validationOptions.Definitions = resolver
	}
	if findings := compile.ValidatePlan(ctx, plan, validationOptions); len(findings) != 0 {
		return childPolicyRefusal("child workflow is not runnable on this host: "+findings[0].Message, ErrPolicyDenied)
	}
	startKey := childRunPolicyKeyPrefix + string(request.ChildRunID)
	requestDigest, err := childRunRequestDigest(request)
	if err != nil {
		return err
	}
	var facts hoststate.PolicyFacts
	var decision hoststate.PolicyDecision
	if prior, loadErr := h.journal.LoadPolicyEvaluationByStartKey(ctx, startKey); loadErr == nil {
		if prior.RequestDigest != requestDigest {
			return &runtime.IdempotencyConflictError{Operation: "child workflow policy evaluation", Key: startKey}
		}
		facts, decision = prior.Facts, prior.Decision
	} else if !errors.Is(loadErr, runtime.ErrNotFound) {
		return loadErr
	} else {
		facts, err = h.policyFacts(ctx, request.ChildRunID, plan, identity)
		if err != nil {
			return childPolicyRefusal("child workflow run was refused by workflow policy: "+err.Error(), err)
		}
		policyInput, cloneErr := clonePolicyFacts(facts)
		if cloneErr != nil {
			return fmt.Errorf("clone child workflow policy facts: %w", cloneErr)
		}
		decision, err = h.policy.EvaluatePolicy(ctx, policyInput)
		if err != nil {
			return fmt.Errorf("evaluate child workflow policy: %w", err)
		}
		decision = normalizeDecision(decision, request.ChildRunID, h.now())
		decision.ID = policyDecisionID(startKey)
		if validationErr := decision.Validate(); validationErr != nil {
			return fmt.Errorf("invalid child workflow policy decision: %w", validationErr)
		}
		if decision.Operation != facts.Operation {
			return errors.New("invalid child workflow policy decision: operation mismatch")
		}
		persisted, _, persistErr := h.journal.RecordPolicyEvaluation(context.WithoutCancel(ctx), hoststate.PolicyEvaluation{
			StartKey: startKey, RequestDigest: requestDigest, Facts: facts, Decision: decision,
		})
		if persistErr != nil {
			return fmt.Errorf("record child workflow policy evaluation: %w", persistErr)
		}
		facts, decision = persisted.Facts, persisted.Decision
	}
	if facts.Plan.Digest != plan.Digest {
		return errors.New("persisted child policy plan differs from the pinned child plan")
	}
	switch decision.Outcome {
	case hoststate.PolicyAllow:
		return nil
	case hoststate.PolicyConfirm:
		if root.Record.Decision.Outcome == hoststate.PolicyConfirm {
			return nil
		}
		return childPolicyRefusal("child workflow run requires confirmation that its root start did not give: "+decision.Reason, ErrConfirmationRequired)
	default:
		return childPolicyRefusal("child workflow run was refused by workflow policy: "+decision.Reason, ErrPolicyDenied)
	}
}

// rootStart follows child-run and replay provenance to the recorded root
// start whose bound identity and decision govern runID.
func (h *Host) rootStart(ctx context.Context, runID runtime.RunID, seen map[runtime.RunID]struct{}, depth int) (hoststate.StartSnapshot, error) {
	if depth > maxChildLineageDepth {
		return hoststate.StartSnapshot{}, fmt.Errorf("workflow run lineage exceeds %d", maxChildLineageDepth)
	}
	if _, duplicate := seen[runID]; duplicate {
		return hoststate.StartSnapshot{}, errors.New("workflow run lineage contains a cycle")
	}
	seen[runID] = struct{}{}
	start, err := h.journal.LoadStart(ctx, runID)
	if err == nil {
		return start, nil
	}
	if !errors.Is(err, runtime.ErrNotFound) {
		return hoststate.StartSnapshot{}, err
	}
	if !nilInterface(h.childDefs) {
		child, childErr := h.childDefs.LoadChildRunRequest(ctx, runID)
		if childErr == nil {
			return h.rootStart(ctx, runtime.RunID(child.Parent.RunID), seen, depth+1)
		}
		if !errors.Is(childErr, runtime.ErrNotFound) {
			return hoststate.StartSnapshot{}, childErr
		}
	}
	replays, ok := h.state.(runtime.ReplayStore)
	if !ok || nilInterface(replays) {
		return hoststate.StartSnapshot{}, err
	}
	replay, replayErr := replays.LoadReplayProvenance(ctx, runID)
	if replayErr != nil {
		return hoststate.StartSnapshot{}, replayErr
	}
	return h.rootStart(ctx, replay.SourceRunID, seen, depth+1)
}

func childRunRequestDigest(request calladapter.ChildRunRequest) (string, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("encode child run request: %w", err)
	}
	return values.SHA256Digest(encoded), nil
}

// childPolicyRefusal is a permanent step failure: retrying the call cannot
// change a durable policy decision or the child plan it was made for.
func childPolicyRefusal(message string, cause error) error {
	return &stepkind.ExecutionError{Code: CodeChildRunPolicyDenied, Message: message, Classification: stepkind.RetryPermanent, Cause: cause}
}
