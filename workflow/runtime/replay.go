package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const EventReplayCreated = "run.replay_created"

var ErrInvalidReplay = errors.New("invalid workflow replay request")

// ReplayNodeBinding binds one exact immutable source invocation to its new run
// identity. Reuse means the terminal status and value references are journaled
// into an unclaimable replay node; otherwise the target starts pending with no
// attempt. Attempts remain claim/dispatch-owned, so "fresh attempts" means a
// fresh invocation history beginning at LatestAttempt zero.
type ReplayNodeBinding struct {
	Source   NodeInvocationSnapshot    `json:"source"`
	Target   NodeInvocationID          `json:"target"`
	Reuse    bool                      `json:"reuse"`
	Attempts []AttemptSnapshot         `json:"attempts,omitempty"`
	Control  []ControlDecisionSnapshot `json:"control,omitempty"`
}

func (b ReplayNodeBinding) Validate(source, target RunID) error {
	if err := b.Source.Validate(); err != nil {
		return err
	}
	if err := b.Target.Validate(); err != nil {
		return err
	}
	if b.Source.ID.RunID != source || b.Target.RunID != target || b.Source.ID.NodeID != b.Target.NodeID || b.Source.ID.Iteration != b.Target.Iteration {
		return fmt.Errorf("replay node identities do not match source and target runs")
	}
	if !b.Source.Status.Terminal() {
		return fmt.Errorf("replay source node %q is not terminal", b.Source.ID.NodeID)
	}
	if !b.Reuse && len(b.Attempts) != 0 {
		return fmt.Errorf("fresh replay node cannot carry prior attempts")
	}
	if !b.Reuse && len(b.Control) != 0 {
		return fmt.Errorf("fresh replay node cannot carry prior control decisions")
	}
	if b.Reuse && len(b.Attempts) != b.Source.LatestAttempt {
		return fmt.Errorf("reused replay node requires complete attempt history")
	}
	for index, attempt := range b.Attempts {
		if err := attempt.Validate(); err != nil || attempt.ID.Invocation != b.Source.ID || attempt.ID.Number != index+1 || !attempt.Status.Terminal() || attempt.FinishedAt.IsZero() {
			return fmt.Errorf("replay attempt history is not complete and terminal")
		}
	}
	for _, decision := range b.Control {
		if err := decision.Validate(); err != nil || decision.ID.Source != b.Source.ID {
			return fmt.Errorf("replay control decision does not match source invocation")
		}
	}
	return nil
}

// RebindReplayControlDecision preserves an immutable selected-route/error fact
// while moving only its run identity to the replay. The source decision remains
// authoritative history; this creates the exact replay-local routing fact.
func RebindReplayControlDecision(decision ControlDecisionSnapshot, targetRun RunID, at time.Time) (ControlDecisionSnapshot, error) {
	if err := decision.Validate(); err != nil {
		return ControlDecisionSnapshot{}, err
	}
	result := decision
	result.ID.Source.RunID = targetRun
	result.Targets = append([]NodeInvocationID(nil), decision.Targets...)
	for i := range result.Targets {
		result.Targets[i].RunID = targetRun
	}
	result.SourceGeneration, result.Generation, result.CreatedAt = 1, 1, at.UTC()
	if decision.RuleIndex != nil {
		value := *decision.RuleIndex
		result.RuleIndex = &value
	}
	if decision.Error != nil {
		value := *decision.Error
		result.Error = &value
	}
	if err := result.Validate(); err != nil {
		return ControlDecisionSnapshot{}, err
	}
	return result, nil
}

type ReplayNodePolicy struct {
	Invocation NodeInvocationID     `json:"invocation"`
	Attempt    *AttemptID           `json:"attempt,omitempty"`
	Decision   RepeatPolicyDecision `json:"decision"`
}

type ReplayFanOutBinding struct {
	Source  FanOutSnapshot     `json:"source"`
	Target  FanOutSnapshot     `json:"target"`
	Results []FanOutItemResult `json:"results"`
}

func (b ReplayFanOutBinding) Validate(source, target RunID) error {
	if err := b.Source.Validate(); err != nil {
		return err
	}
	if err := b.Target.Validate(); err != nil {
		return err
	}
	if b.Source.Parent.RunID != source || b.Target.Parent.RunID != target || b.Source.Parent.NodeID != b.Target.Parent.NodeID || len(b.Source.Items) != len(b.Target.Items) || len(b.Results) != len(b.Source.Items) {
		return fmt.Errorf("replay fan-out identities differ")
	}
	expected := cloneFanOut(b.Source)
	expected.Parent.RunID = target
	for index := range expected.Items {
		expected.Items[index].Invocation.RunID = target
	}
	if !reflect.DeepEqual(expected, b.Target) {
		return fmt.Errorf("replay fan-out target is not an exact run-identity rebind")
	}
	for i := range b.Source.Items {
		left, right := b.Source.Items[i], b.Target.Items[i]
		if left.Index != right.Index || left.Iteration != right.Iteration || left.Inputs != right.Inputs || right.Invocation.RunID != target || right.Invocation.NodeID != left.Invocation.NodeID || right.Invocation.Iteration != left.Invocation.Iteration {
			return fmt.Errorf("replay fan-out item differs")
		}
		if b.Results[i].Invocation != left.Invocation || b.Results[i].Index != i || b.Results[i].Iteration != left.Iteration {
			return fmt.Errorf("replay fan-out result differs")
		}
	}
	return nil
}

// ReplayProvenance is the immutable durable explanation of one replay run.
type ReplayProvenance struct {
	RunID                     RunID                            `json:"run_id"`
	SourceRunID               RunID                            `json:"source_run_id"`
	FromNodeID                string                           `json:"from_node_id"`
	PlanDigest                string                           `json:"plan_digest"`
	IdempotencyKey            string                           `json:"idempotency_key"`
	Policy                    []ReplayNodePolicy               `json:"policy"`
	CompensationAuthorization *ReplayCompensationAuthorization `json:"compensation_authorization,omitempty"`
	CreatedAt                 time.Time                        `json:"created_at"`
}

// ReplayCompensationAuthorization is the bounded host-issued proof for
// replaying a source whose rollback is indeterminate or did not succeed. It
// binds the exact immutable source-ledger observation without retaining the
// caller's free-form justification.
type ReplayCompensationAuthorization struct {
	LedgerGeneration uint64              `json:"ledger_generation"`
	LedgerOutcome    CompensationOutcome `json:"ledger_outcome,omitempty"`
	RiskDigest       string              `json:"risk_digest,omitempty"`
	Digest           string              `json:"digest"`
}

func (a ReplayCompensationAuthorization) Validate() error {
	ledgerProof := a.LedgerGeneration != 0 && (a.LedgerOutcome == "" || a.LedgerOutcome.Valid())
	if a.LedgerGeneration == 0 && a.LedgerOutcome != "" || !ledgerProof && a.RiskDigest == "" {
		return fmt.Errorf("replay compensation authorization requires exact ledger or risk evidence")
	}
	if a.RiskDigest != "" {
		if err := values.ValidateDigest(a.RiskDigest); err != nil {
			return err
		}
	}
	return values.ValidateDigest(a.Digest)
}

// ReplayCompensationRiskDigest returns a stable digest for terminal
// compensable attempts whose receipt contract left application indeterminate.
// An empty result proves that no such durable failure fact exists.
func ReplayCompensationRiskDigest(ctx context.Context, store StateStore, replay ReplayStore, runID RunID) (string, error) {
	invocations, err := replay.ListRunInvocations(ctx, runID)
	if err != nil {
		return "", err
	}
	var facts []string
	for _, invocation := range invocations {
		if invocation.Phase == InvocationCompensation {
			continue
		}
		attempts, loadErr := store.ListAttempts(ctx, invocation.ID)
		if loadErr != nil {
			return "", loadErr
		}
		for _, attempt := range attempts {
			if attempt.Failure == nil {
				continue
			}
			marker := attempt.Failure.Details["effect_applied"]
			if marker != "missing_receipt" && marker != "unverified_receipt" && marker != "unrequested_receipt" {
				continue
			}
			facts = append(facts, fmt.Sprintf("%s\x00%d\x00%s\x00%s\x00%s\x00%d", attempt.ID.Invocation, attempt.ID.Number, marker, attempt.Failure.Code, attempt.FinishedAt.UTC().Format(time.RFC3339Nano), attempt.Generation))
		}
	}
	if len(facts) == 0 {
		return "", nil
	}
	sort.Strings(facts)
	return values.SHA256Digest([]byte(strings.Join(facts, "\n"))), nil
}

func (p ReplayProvenance) Validate() error {
	if err := validateOpaqueID("replay run id", string(p.RunID)); err != nil {
		return err
	}
	if err := validateOpaqueID("replay source run id", string(p.SourceRunID)); err != nil {
		return err
	}
	if err := graph.ValidateID(p.FromNodeID); err != nil {
		return err
	}
	if err := validateRequiredText("replay plan digest", p.PlanDigest); err != nil {
		return err
	}
	if err := validateRequiredText("replay idempotency key", p.IdempotencyKey); err != nil {
		return err
	}
	if p.CreatedAt.IsZero() {
		return fmt.Errorf("replay created_at is required")
	}
	if p.CompensationAuthorization != nil {
		if err := p.CompensationAuthorization.Validate(); err != nil {
			return err
		}
	}
	var previous NodeInvocationID
	for index, item := range p.Policy {
		if err := item.Invocation.Validate(); err != nil || item.Invocation.RunID != p.SourceRunID {
			return fmt.Errorf("replay policy invocation is invalid")
		}
		if item.Attempt != nil && item.Attempt.Invocation != item.Invocation {
			return fmt.Errorf("replay policy attempt does not match invocation")
		}
		if err := item.Decision.Validate(); err != nil {
			return err
		}
		if index > 0 && !invocationLess(previous, item.Invocation) {
			return fmt.Errorf("replay policy must use canonical invocation order")
		}
		previous = item.Invocation
	}
	return nil
}

type BeginReplayRequest struct {
	Provenance               ReplayProvenance      `json:"provenance"`
	Plan                     PlanRef               `json:"plan"`
	Inputs                   *values.ValueSetRef   `json:"inputs,omitempty"`
	ExpectedSourceGeneration uint64                `json:"expected_source_generation"`
	Nodes                    []ReplayNodeBinding   `json:"nodes"`
	FanOuts                  []ReplayFanOutBinding `json:"fan_outs,omitempty"`
}

func (r BeginReplayRequest) Validate() error {
	if err := r.Provenance.Validate(); err != nil {
		return err
	}
	if err := r.Plan.Validate(); err != nil {
		return err
	}
	if r.Plan.Digest != r.Provenance.PlanDigest || r.ExpectedSourceGeneration == 0 || len(r.Nodes) == 0 {
		return fmt.Errorf("replay requires exact plan, source generation, and nodes")
	}
	if r.Inputs != nil {
		if err := r.Inputs.Validate(); err != nil {
			return err
		}
	}
	seen := make(map[NodeInvocationID]bool, len(r.Nodes))
	var previous NodeInvocationID
	for index, binding := range r.Nodes {
		if err := binding.Validate(r.Provenance.SourceRunID, r.Provenance.RunID); err != nil {
			return fmt.Errorf("replay node[%d]: %w", index, err)
		}
		if _, duplicate := seen[binding.Target]; duplicate {
			return fmt.Errorf("replay target is duplicated")
		}
		if index > 0 && !invocationLess(previous, binding.Target) {
			return fmt.Errorf("replay nodes must use canonical invocation order")
		}
		previous = binding.Target
		seen[binding.Target] = binding.Reuse
	}
	for index, binding := range r.FanOuts {
		if err := binding.Validate(r.Provenance.SourceRunID, r.Provenance.RunID); err != nil {
			return fmt.Errorf("replay fan_out[%d]: %w", index, err)
		}
		if index > 0 && !invocationLess(r.FanOuts[index-1].Target.Parent, binding.Target.Parent) {
			return fmt.Errorf("replay fan-outs must use canonical unique parent order")
		}
		if reused, exists := seen[binding.Target.Parent]; !exists || !reused {
			return fmt.Errorf("replay fan-out parent must be a present reused node binding")
		}
		for _, item := range binding.Target.Items {
			if reused, exists := seen[item.Invocation]; !exists || !reused {
				return fmt.Errorf("replay fan-out item must be a present reused node binding")
			}
		}
	}
	return nil
}

type BeginReplayResult struct {
	Outcome    IdempotencyOutcome       `json:"outcome"`
	Run        RunSnapshot              `json:"run"`
	Provenance ReplayProvenance         `json:"provenance"`
	Nodes      []NodeInvocationSnapshot `json:"nodes"`
	Event      Event                    `json:"event"`
}

// ReplayStore atomically binds provenance and creates the new run/node state.
// Failed or losing operations leave no run, event, or provenance residue.
type ReplayStore interface {
	BeginReplay(context.Context, BeginReplayRequest) (BeginReplayResult, error)
	LoadReplayProvenance(context.Context, RunID) (ReplayProvenance, error)
	ListRunInvocations(context.Context, RunID) ([]NodeInvocationSnapshot, error)
}

type ReplayRequest struct {
	SourceRunID               RunID
	RunID                     RunID
	FromNodeID                string
	IdempotencyKey            string
	CompensationAuthorization *ReplayCompensationAuthorization
	At                        time.Time
}

// ReplayService validates the complete downstream effect surface before the
// atomic store mutation and then rebuilds ready state from the pinned graph.
type ReplayService struct {
	Store     StateStore
	Replay    ReplayStore
	Inputs    NodeInputStore
	Control   ControlFlowStore
	Plans     RecoveryPlanSource
	Registry  stepkind.Registry
	Policy    RepeatPolicy
	Evaluator PredicateEvaluator
}

func (s *ReplayService) Rerun(ctx context.Context, request ReplayRequest) (BeginReplayResult, error) {
	if err := s.validate(ctx, request); err != nil {
		return BeginReplayResult{}, err
	}
	source, loadErr := s.Store.LoadRun(ctx, request.SourceRunID)
	if loadErr != nil {
		return BeginReplayResult{}, loadErr
	}
	if !source.Status.Terminal() {
		return BeginReplayResult{}, fmt.Errorf("%w: source run is not terminal", ErrInvalidReplay)
	}
	plan, loadErr := s.Plans.LoadRecoveryPlan(ctx, source)
	if loadErr != nil {
		return BeginReplayResult{}, loadErr
	}
	if validationErr := plan.Validate(); validationErr != nil {
		return BeginReplayResult{}, fmt.Errorf("%w: invalid pinned plan: %w", ErrInvalidReplay, validationErr)
	}
	if plan.Ref != source.Plan {
		return BeginReplayResult{}, fmt.Errorf("%w: pinned plan mismatch", ErrInvalidReplay)
	}
	if _, ok := graphNode(plan.Plan.Graph, request.FromNodeID); !ok {
		return BeginReplayResult{}, fmt.Errorf("%w: selected node is absent from graph", ErrInvalidReplay)
	}
	if _, dormant := CompensationHandlers(plan.Plan.Graph)[request.FromNodeID]; dormant {
		return BeginReplayResult{}, fmt.Errorf("%w: compensation handler cannot be a forward replay boundary", ErrInvalidReplay)
	}
	fresh := replayDownstream(plan.Plan.Graph, request.FromNodeID)
	riskDigest, riskErr := ReplayCompensationRiskDigest(ctx, s.Store, s.Replay, source.ID)
	if riskErr != nil {
		return BeginReplayResult{}, riskErr
	}
	requiresAuthorization := riskDigest != ""
	var requiredLedgerGeneration uint64
	var requiredLedgerOutcome CompensationOutcome
	compensation, compensationOK := s.Store.(CompensationStore)
	if plan.Plan.Graph.Compensation != nil && (!compensationOK || compensation == nil) {
		return BeginReplayResult{}, fmt.Errorf("%w: compensated source plan requires a compensation store", ErrRecoveryUnsafe)
	}
	if compensationOK && compensation != nil {
		ledger, ledgerErr := compensation.LoadCompensationLedger(ctx, source.ID)
		if ledgerErr == nil {
			entries, entryErr := compensation.ListCompensationEntries(ctx, source.ID)
			if entryErr != nil {
				return BeginReplayResult{}, entryErr
			}
			for _, entry := range entries {
				if entry.Status == CompensationSucceeded {
					markReplayDownstream(plan.Plan.Graph, entry.Source.NodeID, fresh)
				}
			}
			if ledger.Status != CompensationTerminal || ledger.Outcome == CompensationOutcomePartial || ledger.Outcome == CompensationOutcomeFailed || ledger.Outcome == CompensationOutcomeCanceled {
				requiresAuthorization = true
				requiredLedgerGeneration, requiredLedgerOutcome = ledger.Generation, ledger.Outcome
			}
		} else if !errors.Is(ledgerErr, ErrNotFound) {
			return BeginReplayResult{}, ledgerErr
		}
	}
	if requiresAuthorization {
		authorization := request.CompensationAuthorization
		if authorization == nil || authorization.Validate() != nil || authorization.LedgerGeneration != requiredLedgerGeneration || authorization.LedgerOutcome != requiredLedgerOutcome || authorization.RiskDigest != riskDigest {
			return BeginReplayResult{}, fmt.Errorf("%w: source compensation risk requires explicit exact attestation", ErrRecoveryUnsafe)
		}
	} else if request.CompensationAuthorization != nil {
		return BeginReplayResult{}, fmt.Errorf("%w: source compensation authorization is not applicable", ErrInvalidReplay)
	}
	bindings := make([]ReplayNodeBinding, 0, len(plan.Plan.Graph.Nodes))
	policies := make([]ReplayNodePolicy, 0)
	var fanOuts []ReplayFanOutBinding
	invocations, loadErr := s.Replay.ListRunInvocations(ctx, source.ID)
	if loadErr != nil {
		return BeginReplayResult{}, loadErr
	}
	for _, definition := range plan.Plan.Graph.Nodes {
		if definition.ForEach == nil || fresh[definition.ID] {
			continue
		}
		parent := NodeInvocationID{RunID: source.ID, NodeID: definition.ID}
		snapshot, fanOutErr := s.Store.LoadFanOut(ctx, parent)
		if errors.Is(fanOutErr, ErrNotFound) {
			continue
		}
		if fanOutErr != nil {
			return BeginReplayResult{}, fanOutErr
		}
		items, itemErr := s.Store.LoadFanOutItemResults(ctx, parent)
		if itemErr != nil {
			return BeginReplayResult{}, itemErr
		}
		target := cloneFanOut(snapshot)
		target.Parent.RunID = request.RunID
		for i := range target.Items {
			target.Items[i].Invocation.RunID = request.RunID
		}
		binding := ReplayFanOutBinding{Source: snapshot, Target: target, Results: items}
		if err := binding.Validate(source.ID, request.RunID); err != nil {
			return BeginReplayResult{}, err
		}
		fanOuts = append(fanOuts, binding)
	}
	sort.Slice(fanOuts, func(i, j int) bool { return invocationLess(fanOuts[i].Target.Parent, fanOuts[j].Target.Parent) })
	sort.Slice(invocations, func(i, j int) bool { return invocationLess(invocations[i].ID, invocations[j].ID) })
	for _, node := range invocations {
		if node.Phase == InvocationCompensation {
			continue
		}
		definition, exists := graphNode(plan.Plan.Graph, node.ID.NodeID)
		if !exists {
			return BeginReplayResult{}, fmt.Errorf("%w: source invocation is absent from pinned graph", ErrInvalidReplay)
		}
		id := node.ID
		if id.Iteration != "" {
			if fresh[definition.ID] {
				continue
			}
		}
		binding := ReplayNodeBinding{Source: node, Target: NodeInvocationID{RunID: request.RunID, NodeID: definition.ID, Iteration: id.Iteration}, Reuse: !fresh[definition.ID]}
		if binding.Reuse {
			binding.Attempts, loadErr = s.Store.ListAttempts(ctx, id)
			if loadErr != nil {
				return BeginReplayResult{}, loadErr
			}
			for _, kind := range []ControlDecisionKind{ControlSwitch, ControlCatch} {
				decision, decisionErr := s.Control.LoadControlDecision(ctx, ControlDecisionID{Source: id, Kind: kind})
				if decisionErr == nil {
					binding.Control = append(binding.Control, decision)
					continue
				}
				if !errors.Is(decisionErr, ErrNotFound) {
					return BeginReplayResult{}, decisionErr
				}
			}
		}
		if err := binding.Validate(source.ID, request.RunID); err != nil {
			return BeginReplayResult{}, fmt.Errorf("%w: %w", ErrInvalidReplay, err)
		}
		bindings = append(bindings, binding)
		if !fresh[definition.ID] {
			continue
		}
		_, spec, resolveErr := stepkind.Resolve(s.Registry, definition.Kind, definition.KindVersion)
		if resolveErr != nil {
			return BeginReplayResult{}, resolveErr
		}
		var priorAttempt *AttemptSnapshot
		if node.LatestAttempt > 0 {
			loaded, loadErr := s.Store.LoadAttempt(ctx, AttemptID{Invocation: id, Number: node.LatestAttempt})
			if loadErr != nil {
				return BeginReplayResult{}, loadErr
			}
			priorAttempt = &loaded
		}
		idempotencyKey, keyErr := (&RecoveryCoordinator{Store: s.Store, Control: s.Control}).recoveryIdempotencyKey(ctx, plan, node, definition)
		if keyErr != nil {
			return BeginReplayResult{}, keyErr
		}
		candidate := RepeatCandidate{Operation: RepeatReplay, Run: source, Node: node, Attempt: priorAttempt, Definition: definition, Spec: spec, Effects: effectiveEffects(definition.Effects, spec.Effects), IdempotencyKey: idempotencyKey}
		decision, safe := (&RecoveryCoordinator{Policy: s.Policy}).repeatDecision(ctx, candidate)
		if !safe || !decision.Allow {
			return BeginReplayResult{}, fmt.Errorf("%w: node %s: %s", ErrRecoveryUnsafe, definition.ID, decision.Code)
		}
		var attemptID *AttemptID
		if priorAttempt != nil {
			value := priorAttempt.ID
			attemptID = &value
		}
		policies = append(policies, ReplayNodePolicy{Invocation: id, Attempt: attemptID, Decision: decision})
	}
	provenance := ReplayProvenance{RunID: request.RunID, SourceRunID: source.ID, FromNodeID: request.FromNodeID, PlanDigest: source.Plan.Digest, IdempotencyKey: request.IdempotencyKey, Policy: policies, CompensationAuthorization: cloneReplayCompensationAuthorization(request.CompensationAuthorization), CreatedAt: request.At.UTC()}
	result, beginErr := s.Replay.BeginReplay(context.WithoutCancel(ctx), BeginReplayRequest{Provenance: provenance, Plan: source.Plan, Inputs: source.Inputs, ExpectedSourceGeneration: source.Generation, Nodes: bindings, FanOuts: fanOuts})
	if beginErr != nil {
		return BeginReplayResult{}, beginErr
	}
	// Readiness is intentionally post-commit and restart-safe. Attempts are not
	// fabricated here; normal claim/dispatch creates attempt one.
	coordinator := &RecoveryCoordinator{Store: s.Store, Inputs: s.Inputs, Control: s.Control, Plans: s.Plans, Registry: s.Registry, Evaluator: s.Evaluator}
	_, rebuildErr := coordinator.rebuildReady(ctx, result.Run, plan, RecoveryRequest{Now: request.At})
	if rebuildErr != nil {
		return result, rebuildErr
	}
	return result, nil
}

func cloneReplayCompensationAuthorization(input *ReplayCompensationAuthorization) *ReplayCompensationAuthorization {
	if input == nil {
		return nil
	}
	cloned := *input
	return &cloned
}

func (s *ReplayService) validate(ctx context.Context, request ReplayRequest) error {
	if ctx == nil || s == nil || nilStateStore(s.Store) || nilReflect(s.Replay) || nilNodeInputStore(s.Inputs) || nilControlFlowStore(s.Control) || nilRecoveryPlanSource(s.Plans) || nilStepKindRegistry(s.Registry) {
		return fmt.Errorf("%w: context, stores, plan source, and registry are required", ErrInvalidReplay)
	}
	if err := validateOpaqueID("source run id", string(request.SourceRunID)); err != nil {
		return err
	}
	if err := validateOpaqueID("replay run id", string(request.RunID)); err != nil {
		return err
	}
	if request.SourceRunID == request.RunID {
		return fmt.Errorf("%w: replay run must be new", ErrInvalidReplay)
	}
	if err := graph.ValidateID(request.FromNodeID); err != nil {
		return err
	}
	if err := validateRequiredText("replay idempotency key", request.IdempotencyKey); err != nil {
		return err
	}
	if request.At.IsZero() {
		return fmt.Errorf("%w: replay time is required", ErrInvalidReplay)
	}
	return ctx.Err()
}

func replayDownstream(workflow graph.Graph, from string) map[string]bool {
	adjacent := make(map[string]map[string]struct{})
	add := func(left, right string) {
		if adjacent[left] == nil {
			adjacent[left] = make(map[string]struct{})
		}
		adjacent[left][right] = struct{}{}
	}
	for _, node := range workflow.Nodes {
		for _, need := range node.Needs {
			add(need.Node, node.ID)
		}
		for _, rule := range node.Catch {
			for _, target := range rule.Targets {
				add(node.ID, target)
			}
		}
		if node.Switch != nil {
			for _, target := range node.Switch.Default {
				add(node.ID, target)
			}
			for _, arm := range node.Switch.Arms {
				for _, target := range arm.Targets {
					add(node.ID, target)
				}
			}
		}
	}
	for _, edge := range workflow.Edges {
		add(edge.From, edge.To)
	}
	result := map[string]bool{from: true}
	queue := []string{from}
	for len(queue) != 0 {
		current := queue[0]
		queue = queue[1:]
		for next := range adjacent[current] {
			if !result[next] {
				result[next] = true
				queue = append(queue, next)
			}
		}
	}
	// Cleanup is per replay scope and must never reuse a prior run's cleanup.
	for _, node := range workflow.Nodes {
		if node.Finally != nil {
			result[node.ID] = true
		}
	}
	return result
}

func markReplayDownstream(workflow graph.Graph, from string, result map[string]bool) {
	marked := replayDownstream(workflow, from)
	for nodeID := range marked {
		result[nodeID] = true
	}
}
