package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

// ErrInvalidReadiness identifies malformed readiness or progression input.
var ErrInvalidReadiness = errors.New("invalid workflow readiness input")

const (
	// ReasonReadinessWaiting identifies a dependency set that is not resolved.
	ReasonReadinessWaiting = "readiness_waiting"
	// ReasonReadinessUnsatisfied identifies a rule that can no longer become true.
	ReasonReadinessUnsatisfied = "readiness_unsatisfied"
	// ReasonPredicateFalse identifies a ready node skipped by its if predicate.
	ReasonPredicateFalse = "predicate_false"
)

// ReadinessDisposition distinguishes an unresolved dependency set from work
// that may run and work that must be skipped.
type ReadinessDisposition string

const (
	// ReadinessWaiting leaves the invocation durably blocked.
	ReadinessWaiting ReadinessDisposition = "waiting"
	// ReadinessReady permits dispatch after any predicate succeeds.
	ReadinessReady ReadinessDisposition = "ready"
	// ReadinessSkip durably prevents dispatch.
	ReadinessSkip ReadinessDisposition = "skip"
)

// DependencyState is one persisted upstream outcome considered by a readiness
// rule. FailureHandled is a narrow propagation signal: it makes an otherwise
// hard-failed dependency success-equivalent without implementing catch routes.
type DependencyState struct {
	InvocationID   NodeInvocationID
	Status         NodeStatus
	FailureHandled bool
}

// DependencyRef identifies an upstream invocation for ProgressNode. Status is
// always loaded from the durable StateStore. Refs are trusted plan-derived
// identities: callers must supply the complete normalized dependency set,
// including expanded invocation IDs, because StateStore does not expose graph
// edge or fan-out reconstruction queries.
type DependencyRef struct {
	InvocationID   NodeInvocationID
	FailureHandled bool
}

// ReadinessEvaluation is the deterministic result of one named rule.
type ReadinessEvaluation struct {
	Rule         graph.ReadyRule
	Disposition  ReadinessDisposition
	Dependencies []DependencyState
	Reason       *BlockedReason
}

// PredicateEvaluator is the narrow expression seam used after readiness is
// satisfied. values.ExpressionEngine implements this interface.
type PredicateEvaluator interface {
	EvaluateBool(graph.Expression, values.ExpressionContext, values.ExpressionOptions) (bool, error)
}

// ProgressNodeRequest supplies durable dependency identities and the typed
// expression context for one pending or blocked invocation.
type ProgressNodeRequest struct {
	InvocationID      NodeInvocationID
	Dependencies      []DependencyRef
	Rule              graph.ReadyRule
	Predicate         *graph.Expression
	ExpressionContext values.ExpressionContext
	ExpressionOptions values.ExpressionOptions
	At                time.Time
}

// ProgressNodeResult contains the applied or observed progression outcome.
// Event is nil for a durable no-op.
type ProgressNodeResult struct {
	Disposition        ReadinessDisposition
	Snapshot           NodeInvocationSnapshot
	Reason             *BlockedReason
	Event              *Event
	PredicateEvaluated bool
	PredicateResult    bool
}

// ProgressionCoordinator evaluates and persists readiness without retaining
// correctness-critical state between calls.
type ProgressionCoordinator struct {
	store     StateStore
	evaluator PredicateEvaluator
}

// NewProgressionCoordinator constructs a progression coordinator. A nil
// evaluator uses a fresh typed values.ExpressionEngine.
func NewProgressionCoordinator(store StateStore, evaluator PredicateEvaluator) *ProgressionCoordinator {
	if evaluator == nil {
		evaluator = values.NewExpressionEngine()
	}
	return &ProgressionCoordinator{store: store, evaluator: evaluator}
}

// EvaluateReadiness applies the D10 Airflow-style rules. A handled hard failure
// is success-equivalent for rule evaluation, while skipped remains a distinct
// terminal outcome. The exact truth table is:
//
//   - all_success: ready when all dependencies succeeded or were handled; skip
//     as soon as any other terminal outcome appears; otherwise wait.
//   - all_done: ready when every dependency is terminal; otherwise wait.
//   - one_failed: ready as soon as one unhandled hard failure appears; skip once
//     all dependencies are terminal without one; otherwise wait.
//   - all_failed: ready only for a non-empty, all-unhandled-failure set; skip as
//     soon as a non-failure terminal outcome appears; otherwise wait.
//   - none_failed: skip as soon as an unhandled hard failure appears; ready once
//     every dependency is terminal without one; otherwise wait.
//   - always: ready immediately without waiting for dependency outcomes.
//
// Empty dependency sets are ready for all_success, all_done, none_failed, and
// always, and skip for one_failed and all_failed. Hard failures are failed,
// timed_out, canceled, and crashed. This is progression only: FailureHandled
// is caller-supplied route state and does not implement catch/error routing.
func EvaluateReadiness(rule graph.ReadyRule, dependencies []DependencyState) (ReadinessEvaluation, error) {
	if rule == "" {
		rule = graph.ReadyAllSuccess
	}
	if !rule.Valid() {
		return ReadinessEvaluation{}, fmt.Errorf("%w: unsupported rule %q", ErrInvalidReadiness, rule)
	}
	canonical, err := canonicalDependencyStates(dependencies)
	if err != nil {
		return ReadinessEvaluation{}, err
	}

	counts := readinessCountsFor(canonical)
	disposition := ReadinessWaiting
	switch rule {
	case graph.ReadyAllSuccess:
		switch {
		case counts.disqualifying > 0:
			disposition = ReadinessSkip
		case counts.nonTerminal > 0:
			disposition = ReadinessWaiting
		default:
			disposition = ReadinessReady
		}
	case graph.ReadyAllDone:
		if counts.nonTerminal == 0 {
			disposition = ReadinessReady
		}
	case graph.ReadyOneFailed:
		switch {
		case counts.hardFailure > 0:
			disposition = ReadinessReady
		case counts.nonTerminal == 0:
			disposition = ReadinessSkip
		}
	case graph.ReadyAllFailed:
		switch {
		case len(canonical) == 0 || counts.nonFailureTerminal > 0:
			disposition = ReadinessSkip
		case counts.nonTerminal > 0:
			disposition = ReadinessWaiting
		default:
			disposition = ReadinessReady
		}
	case graph.ReadyNoneFailed:
		switch {
		case counts.hardFailure > 0:
			disposition = ReadinessSkip
		case counts.nonTerminal > 0:
			disposition = ReadinessWaiting
		default:
			disposition = ReadinessReady
		}
	case graph.ReadyAlways:
		disposition = ReadinessReady
	}

	result := ReadinessEvaluation{Rule: rule, Disposition: disposition, Dependencies: canonical}
	if disposition != ReadinessReady {
		result.Reason = readinessReason(rule, disposition, canonical, counts)
	}
	return result, nil
}

// ProgressNode loads authoritative dependency state, evaluates the readiness
// rule, then evaluates if only when ready. It persists blocked, ready, or
// skipped through lifecycle CAS and returns expression failures without a
// transition.
func (c *ProgressionCoordinator) ProgressNode(ctx context.Context, request ProgressNodeRequest) (ProgressNodeResult, error) {
	if ctx == nil {
		return ProgressNodeResult{}, fmt.Errorf("%w: context is required", ErrInvalidReadiness)
	}
	if err := ctx.Err(); err != nil {
		return ProgressNodeResult{}, err
	}
	if c == nil || c.store == nil {
		return ProgressNodeResult{}, fmt.Errorf("%w: state store is required", ErrInvalidReadiness)
	}
	if err := request.InvocationID.Validate(); err != nil {
		return ProgressNodeResult{}, fmt.Errorf("%w: target invocation: %w", ErrInvalidReadiness, err)
	}
	if request.At.IsZero() {
		return ProgressNodeResult{}, fmt.Errorf("%w: progression time is required", ErrInvalidReadiness)
	}

	target, err := c.store.LoadNodeInvocation(ctx, request.InvocationID)
	if err != nil {
		return ProgressNodeResult{}, err
	}
	if target.Status == NodeReady {
		return ProgressNodeResult{Disposition: ReadinessReady, Snapshot: target}, nil
	}
	if target.Status == NodeSkipped {
		return ProgressNodeResult{Disposition: ReadinessSkip, Snapshot: target}, nil
	}
	if target.Status.Terminal() {
		return ProgressNodeResult{}, fmt.Errorf("%w: target is terminal with status %q", ErrInvalidReadiness, target.Status)
	}
	if target.Status != NodePending && target.Status != NodeBlocked {
		return ProgressNodeResult{}, fmt.Errorf("%w: target status %q cannot be progressed", ErrInvalidReadiness, target.Status)
	}

	dependencies, err := c.loadDependencyStates(ctx, target.ID, request.Dependencies)
	if err != nil {
		return ProgressNodeResult{}, err
	}
	evaluation, err := EvaluateReadiness(request.Rule, dependencies)
	if err != nil {
		return ProgressNodeResult{}, err
	}
	result := ProgressNodeResult{Disposition: evaluation.Disposition, Snapshot: target, Reason: cloneProgressReason(evaluation.Reason)}
	if evaluation.Disposition == ReadinessWaiting {
		if target.Status == NodeBlocked && equalProgressReason(target.Blocked, evaluation.Reason) {
			return result, nil
		}
		transition, transitionErr := c.store.TransitionNode(ctx, NodeTransitionRequest{
			InvocationID: target.ID, ExpectedGeneration: target.Generation,
			To: NodeBlocked, Blocked: cloneProgressReason(evaluation.Reason), At: request.At,
		})
		if transitionErr != nil {
			return ProgressNodeResult{}, transitionErr
		}
		result.Snapshot = transition.Snapshot
		result.Event = transition.Event
		return result, nil
	}

	if evaluation.Disposition == ReadinessSkip {
		return c.skipNode(ctx, target, evaluation.Reason, false, false, request.At)
	}

	if request.Predicate != nil {
		predicate, predicateErr := c.evaluator.EvaluateBool(*request.Predicate, request.ExpressionContext, request.ExpressionOptions)
		if predicateErr != nil {
			return ProgressNodeResult{}, predicateErr
		}
		if !predicate {
			reason := &BlockedReason{
				Code: ReasonPredicateFalse, Message: "if predicate evaluated to false",
				Dependencies: dependencyIDs(dependencies),
				Details:      map[string]string{"rule": string(evaluation.Rule), "result": "false"},
			}
			return c.skipNode(ctx, target, reason, true, false, request.At)
		}
		result.PredicateEvaluated = true
		result.PredicateResult = true
	}

	transition, err := c.store.TransitionNode(ctx, NodeTransitionRequest{
		InvocationID: target.ID, ExpectedGeneration: target.Generation, To: NodeReady, At: request.At,
	})
	if err != nil {
		return ProgressNodeResult{}, err
	}
	result.Disposition = ReadinessReady
	result.Snapshot = transition.Snapshot
	result.Event = transition.Event
	return result, nil
}

func (c *ProgressionCoordinator) skipNode(
	ctx context.Context,
	target NodeInvocationSnapshot,
	reason *BlockedReason,
	predicateEvaluated bool,
	predicateResult bool,
	at time.Time,
) (ProgressNodeResult, error) {
	transition, err := c.store.TransitionNode(ctx, NodeTransitionRequest{
		InvocationID: target.ID, ExpectedGeneration: target.Generation, To: NodeSkipped,
		Explanation: cloneProgressReason(reason), At: at,
	})
	if err != nil {
		return ProgressNodeResult{}, err
	}
	return ProgressNodeResult{
		Disposition: ReadinessSkip, Snapshot: transition.Snapshot,
		Reason: cloneProgressReason(reason), Event: transition.Event,
		PredicateEvaluated: predicateEvaluated, PredicateResult: predicateResult,
	}, nil
}

func (c *ProgressionCoordinator) loadDependencyStates(ctx context.Context, target NodeInvocationID, refs []DependencyRef) ([]DependencyState, error) {
	seen := make(map[NodeInvocationID]struct{}, len(refs))
	states := make([]DependencyState, 0, len(refs))
	for _, ref := range refs {
		if err := ref.InvocationID.Validate(); err != nil {
			return nil, fmt.Errorf("%w: dependency invocation: %w", ErrInvalidReadiness, err)
		}
		if ref.InvocationID.RunID != target.RunID {
			return nil, fmt.Errorf("%w: dependency %v belongs to another run", ErrInvalidReadiness, ref.InvocationID)
		}
		if ref.InvocationID == target {
			return nil, fmt.Errorf("%w: target cannot depend on itself", ErrInvalidReadiness)
		}
		if _, duplicate := seen[ref.InvocationID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate dependency %v", ErrInvalidReadiness, ref.InvocationID)
		}
		seen[ref.InvocationID] = struct{}{}
		snapshot, err := c.store.LoadNodeInvocation(ctx, ref.InvocationID)
		if err != nil {
			return nil, err
		}
		states = append(states, DependencyState{
			InvocationID: ref.InvocationID, Status: snapshot.Status, FailureHandled: ref.FailureHandled,
		})
	}
	return states, nil
}

type readinessCounts struct {
	succeeded          int
	failures           int
	successEquivalent  int
	hardFailure        int
	skipped            int
	handled            int
	nonTerminal        int
	disqualifying      int
	nonFailureTerminal int
}

func readinessCountsFor(dependencies []DependencyState) readinessCounts {
	var counts readinessCounts
	for _, dependency := range dependencies {
		switch {
		case dependency.FailureHandled:
			counts.failures++
			counts.successEquivalent++
			counts.handled++
			counts.nonFailureTerminal++
		case dependency.Status == NodeSucceeded:
			counts.succeeded++
			counts.successEquivalent++
			counts.nonFailureTerminal++
		case hardFailure(dependency.Status):
			counts.failures++
			counts.hardFailure++
			counts.disqualifying++
		case dependency.Status == NodeSkipped:
			counts.skipped++
			counts.disqualifying++
			counts.nonFailureTerminal++
		default:
			counts.nonTerminal++
		}
	}
	return counts
}

func canonicalDependencyStates(input []DependencyState) ([]DependencyState, error) {
	result := append([]DependencyState(nil), input...)
	seen := make(map[NodeInvocationID]struct{}, len(result))
	var runID RunID
	for i, dependency := range result {
		if err := dependency.InvocationID.Validate(); err != nil {
			return nil, fmt.Errorf("%w: dependency[%d]: %w", ErrInvalidReadiness, i, err)
		}
		if i == 0 {
			runID = dependency.InvocationID.RunID
		} else if dependency.InvocationID.RunID != runID {
			return nil, fmt.Errorf("%w: dependencies must belong to one run", ErrInvalidReadiness)
		}
		if _, duplicate := seen[dependency.InvocationID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate dependency %v", ErrInvalidReadiness, dependency.InvocationID)
		}
		seen[dependency.InvocationID] = struct{}{}
		if !dependency.Status.Valid() {
			return nil, fmt.Errorf("%w: dependency %v has unsupported status %q", ErrInvalidReadiness, dependency.InvocationID, dependency.Status)
		}
		if dependency.FailureHandled && !hardFailure(dependency.Status) {
			return nil, fmt.Errorf("%w: dependency %v can be handled only from a hard-failure status", ErrInvalidReadiness, dependency.InvocationID)
		}
	}
	sort.Slice(result, func(i, j int) bool { return invocationLess(result[i].InvocationID, result[j].InvocationID) })
	return result, nil
}

func hardFailure(status NodeStatus) bool {
	switch status {
	case NodeFailed, NodeTimedOut, NodeCanceled, NodeCrashed:
		return true
	default:
		return false
	}
}

func readinessReason(rule graph.ReadyRule, disposition ReadinessDisposition, dependencies []DependencyState, counts readinessCounts) *BlockedReason {
	code := ReasonReadinessWaiting
	message := fmt.Sprintf("readiness rule %s is waiting for dependency outcomes", rule)
	details := map[string]string{
		"rule": string(rule), "terminal": strconv.Itoa(len(dependencies) - counts.nonTerminal),
		"nonterminal": strconv.Itoa(counts.nonTerminal), "succeeded": strconv.Itoa(counts.succeeded),
		"failed": strconv.Itoa(counts.failures), "skipped": strconv.Itoa(counts.skipped),
		"handled": strconv.Itoa(counts.handled), "success_equivalent": strconv.Itoa(counts.successEquivalent),
	}
	if disposition == ReadinessSkip {
		code = ReasonReadinessUnsatisfied
		message = fmt.Sprintf("readiness rule %s cannot be satisfied", rule)
	}
	return &BlockedReason{
		Code: code, Message: message, Dependencies: dependencyIDs(dependencies),
		Details: details,
	}
}

func dependencyIDs(dependencies []DependencyState) []NodeInvocationID {
	result := make([]NodeInvocationID, len(dependencies))
	for i := range dependencies {
		result[i] = dependencies[i].InvocationID
	}
	return result
}

func cloneProgressReason(reason *BlockedReason) *BlockedReason {
	if reason == nil {
		return nil
	}
	clone := *reason
	clone.Dependencies = append([]NodeInvocationID(nil), reason.Dependencies...)
	if reason.Details != nil {
		clone.Details = make(map[string]string, len(reason.Details))
		for key, value := range reason.Details {
			clone.Details[key] = value
		}
	}
	return &clone
}

func equalProgressReason(left, right *BlockedReason) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.Code != right.Code || left.Message != right.Message ||
		len(left.Dependencies) != len(right.Dependencies) || len(left.Details) != len(right.Details) {
		return false
	}
	for i := range left.Dependencies {
		if left.Dependencies[i] != right.Dependencies[i] {
			return false
		}
	}
	for key, value := range left.Details {
		if right.Details[key] != value {
			return false
		}
	}
	return true
}
