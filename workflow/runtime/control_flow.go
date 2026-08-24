package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	EventSwitchDecided  = "control.switch_decided"
	EventCatchDecided   = "control.catch_decided"
	EventTerminalIntent = "run.terminal_intent_recorded"
)

var (
	ErrInvalidControlFlow  = errors.New("invalid workflow control flow")
	ErrControlFlowPending  = errors.New("workflow control flow is pending")
	ErrControlFlowConflict = errors.New("workflow control flow conflict")
)

// ControlDecisionKind identifies one immutable routing fact for an invocation.
type ControlDecisionKind string

const (
	ControlSwitch ControlDecisionKind = "switch"
	ControlCatch  ControlDecisionKind = "catch"
)

func (k ControlDecisionKind) Valid() bool { return k == ControlSwitch || k == ControlCatch }

// ControlDecisionOutcome describes how an ordered route resolved.
type ControlDecisionOutcome string

const (
	ControlSelected  ControlDecisionOutcome = "selected"
	ControlDefault   ControlDecisionOutcome = "default"
	ControlUnmatched ControlDecisionOutcome = "unmatched"
	ControlContinued ControlDecisionOutcome = "continued"
)

func (o ControlDecisionOutcome) Valid() bool {
	switch o {
	case ControlSelected, ControlDefault, ControlUnmatched, ControlContinued:
		return true
	default:
		return false
	}
}

// ControlDecisionID is collision-free because stores persist its structured
// fields rather than a delimiter-concatenated primary key.
type ControlDecisionID struct {
	Source NodeInvocationID    `json:"source"`
	Kind   ControlDecisionKind `json:"kind"`
}

func (id ControlDecisionID) Validate() error {
	if err := id.Source.Validate(); err != nil {
		return err
	}
	if !id.Kind.Valid() {
		return fmt.Errorf("unsupported control decision kind %q", id.Kind)
	}
	return nil
}

// ControlDecisionSnapshot is the immutable, restart-durable result of one
// ordered switch or catch evaluation. Error is a digest-bound typed ValueSet
// reference; it never embeds resolved secret material.
type ControlDecisionSnapshot struct {
	ID               ControlDecisionID      `json:"id"`
	Outcome          ControlDecisionOutcome `json:"outcome"`
	RuleIndex        *int                   `json:"rule_index,omitempty"`
	Targets          []NodeInvocationID     `json:"targets,omitempty"`
	BindAs           string                 `json:"bind_as,omitempty"`
	Error            *values.ValueSetRef    `json:"error,omitempty"`
	SourceGeneration uint64                 `json:"source_generation"`
	Generation       uint64                 `json:"generation"`
	CreatedAt        time.Time              `json:"created_at"`
}

func (s ControlDecisionSnapshot) Validate() error {
	if err := s.ID.Validate(); err != nil {
		return err
	}
	if !s.Outcome.Valid() || s.SourceGeneration == 0 || s.Generation == 0 || s.CreatedAt.IsZero() {
		return fmt.Errorf("decision requires valid outcome, generations, and timestamp")
	}
	if s.RuleIndex != nil && *s.RuleIndex < 0 {
		return fmt.Errorf("decision rule index must not be negative")
	}
	if s.Outcome == ControlSelected && (s.RuleIndex == nil || len(s.Targets) == 0) {
		return fmt.Errorf("selected decision requires rule index and targets")
	}
	if s.Outcome == ControlDefault && len(s.Targets) == 0 {
		return fmt.Errorf("default decision requires targets")
	}
	if s.Outcome != ControlSelected && s.RuleIndex != nil {
		return fmt.Errorf("only selected decisions carry a rule index")
	}
	if (s.Outcome == ControlUnmatched || s.Outcome == ControlContinued) && len(s.Targets) != 0 {
		return fmt.Errorf("unmatched and continued decisions cannot carry targets")
	}
	switch s.ID.Kind {
	case ControlSwitch:
		if s.Error != nil || s.BindAs != "" {
			return fmt.Errorf("switch decision cannot carry catch-only fields")
		}
		if s.Outcome != ControlSelected && s.Outcome != ControlDefault && s.Outcome != ControlUnmatched {
			return fmt.Errorf("switch decision has incompatible outcome %q", s.Outcome)
		}
	case ControlCatch:
		if s.Error == nil {
			return fmt.Errorf("catch decision requires typed error reference")
		}
		if s.Outcome != ControlSelected && s.Outcome != ControlUnmatched && s.Outcome != ControlContinued {
			return fmt.Errorf("catch decision has incompatible outcome %q", s.Outcome)
		}
		if s.Outcome != ControlSelected && s.BindAs != "" {
			return fmt.Errorf("only selected catch decisions can carry a binding")
		}
	}
	if s.Error != nil {
		if err := s.Error.Validate(); err != nil {
			return err
		}
	}
	if s.BindAs != "" {
		if err := values.ValidateExpressionLocalName(s.BindAs); err != nil {
			return err
		}
	}
	seen := make(map[NodeInvocationID]struct{}, len(s.Targets))
	for _, target := range s.Targets {
		if err := target.Validate(); err != nil || target.RunID != s.ID.Source.RunID {
			return fmt.Errorf("decision target must be valid and belong to source run")
		}
		if _, duplicate := seen[target]; duplicate {
			return fmt.Errorf("decision target is duplicated")
		}
		seen[target] = struct{}{}
	}
	return nil
}

type RecordControlDecisionRequest struct {
	Decision ControlDecisionSnapshot
	// ErrorValues is atomically persisted and bound to catch decisions by the
	// store. Callers never preallocate or choose the durable ValueSet identity.
	ErrorValues              values.ValueSet
	ExpectedSourceGeneration uint64
	At                       time.Time
}

type RecordControlDecisionResult struct {
	Outcome  IdempotencyOutcome
	Decision ControlDecisionSnapshot
	Event    *Event
}

// FinalizerScope is one cleanup invocation and its complete ordered terminal
// prerequisite set. Order is an inner-to-outer topological layer.
type FinalizerScope struct {
	Invocation NodeInvocationID   `json:"invocation"`
	Scope      []NodeInvocationID `json:"scope"`
	Order      int                `json:"order"`
}

func (s FinalizerScope) Validate(runID RunID) error {
	if err := s.Invocation.Validate(); err != nil || s.Invocation.RunID != runID || s.Invocation.Iteration != "" || s.Order < 0 {
		return fmt.Errorf("finalizer requires base invocation in its run and nonnegative order")
	}
	seen := make(map[NodeInvocationID]struct{}, len(s.Scope))
	var previous NodeInvocationID
	for index, member := range s.Scope {
		if err := member.Validate(); err != nil || member.RunID != runID || member == s.Invocation {
			return fmt.Errorf("finalizer scope[%d] is invalid", index)
		}
		if _, duplicate := seen[member]; duplicate {
			return fmt.Errorf("finalizer scope member is duplicated")
		}
		if index > 0 && !invocationLess(previous, member) {
			return fmt.Errorf("finalizer scope must use canonical identity order")
		}
		previous = member
		seen[member] = struct{}{}
	}
	return nil
}

// TerminalIntentStatus is distinct from RunStatus: a run remains internally
// active while ordinary cleanup executes, but callers can observe the intended
// terminal outcome explicitly.
type TerminalIntentStatus string

const (
	TerminalIntentPending   TerminalIntentStatus = "pending"
	TerminalIntentCompleted TerminalIntentStatus = "completed"
)

// TerminalIntentSnapshot is the immutable intended outcome plus mutable CAS
// completion state. Finalizer ordering and typed originating error are fixed at
// creation and never inferred from events or timestamps.
type TerminalIntentSnapshot struct {
	RunID          RunID                `json:"run_id"`
	IntendedStatus RunStatus            `json:"intended_status"`
	Reason         *Failure             `json:"reason,omitempty"`
	Error          *values.ValueSetRef  `json:"error,omitempty"`
	IdempotencyKey string               `json:"idempotency_key"`
	Finalizers     []FinalizerScope     `json:"finalizers"`
	Status         TerminalIntentStatus `json:"status"`
	Generation     uint64               `json:"generation"`
	CreatedAt      time.Time            `json:"created_at"`
	UpdatedAt      time.Time            `json:"updated_at"`
	CompletedAt    time.Time            `json:"completed_at,omitempty"`
}

func (s TerminalIntentSnapshot) Validate() error {
	if err := validateOpaqueID("terminal intent run id", string(s.RunID)); err != nil {
		return err
	}
	if !s.IntendedStatus.Terminal() || s.IntendedStatus == RunSucceeded && s.Reason != nil {
		return fmt.Errorf("terminal intent requires coherent terminal status and reason")
	}
	if s.IntendedStatus == RunSucceeded && s.Error != nil {
		return fmt.Errorf("successful terminal intent cannot contain an error")
	}
	if s.IntendedStatus != RunSucceeded && (s.Reason == nil || s.Error == nil) {
		return fmt.Errorf("unsuccessful terminal intent requires reason and typed error")
	}
	if s.Reason != nil {
		if err := s.Reason.Validate(); err != nil {
			return err
		}
	}
	if s.Error != nil {
		if err := s.Error.Validate(); err != nil {
			return err
		}
	}
	if err := validateRequiredText("terminal intent idempotency key", s.IdempotencyKey); err != nil {
		return err
	}
	if len(s.Finalizers) == 0 || s.Generation == 0 || s.CreatedAt.IsZero() || s.UpdatedAt.Before(s.CreatedAt) {
		return fmt.Errorf("terminal intent requires finalizers, generation, and ordered timestamps")
	}
	seen := make(map[NodeInvocationID]struct{}, len(s.Finalizers))
	lastOrder := -1
	var lastInvocation NodeInvocationID
	for index, finalizer := range s.Finalizers {
		if err := finalizer.Validate(s.RunID); err != nil {
			return fmt.Errorf("finalizer[%d]: %w", index, err)
		}
		if _, duplicate := seen[finalizer.Invocation]; duplicate || finalizer.Order < lastOrder || finalizer.Order == lastOrder && !invocationLess(lastInvocation, finalizer.Invocation) {
			return fmt.Errorf("finalizers must be unique and ordered inner-to-outer")
		}
		seen[finalizer.Invocation] = struct{}{}
		lastOrder = finalizer.Order
		lastInvocation = finalizer.Invocation
	}
	switch s.Status {
	case TerminalIntentPending:
		if !s.CompletedAt.IsZero() {
			return fmt.Errorf("pending terminal intent cannot contain completed_at")
		}
	case TerminalIntentCompleted:
		if s.CompletedAt.IsZero() || !s.CompletedAt.Equal(s.UpdatedAt) {
			return fmt.Errorf("completed terminal intent requires completed_at equal to updated_at")
		}
	default:
		return fmt.Errorf("unsupported terminal intent status %q", s.Status)
	}
	return nil
}

type BeginTerminalIntentRequest struct {
	RunID                 RunID
	ExpectedRunGeneration uint64
	IntendedStatus        RunStatus
	Reason                *Failure
	// ErrorValues is atomically persisted and bound for unsuccessful intent.
	ErrorValues    values.ValueSet
	IdempotencyKey string
	Finalizers     []FinalizerScope
	At             time.Time
}

type BeginTerminalIntentResult struct {
	Outcome IdempotencyOutcome
	Run     RunSnapshot
	Intent  TerminalIntentSnapshot
	Event   *Event
}

type RequestRunCancellationWithFinalizersRequest struct {
	Cancellation RequestRunCancellationRequest
	Finalizers   []FinalizerScope
	ErrorValues  values.ValueSet
	// Descendants is the canonical RunID-ordered plan for every recursively
	// reachable ParentCloseCancel child. Entries with no finalizers are
	// explicit so the store can validate the complete local cancellation tree
	// before mutating any run.
	Descendants []CancellationDescendantPlan
}

// CancellationDescendantPlan is one locally owned direct-cancel descendant in
// an atomic cancellation tree. ErrorValues is required exactly when Finalizers
// is non-empty and is store-bound to that descendant's terminal intent.
type CancellationDescendantPlan struct {
	RunID                 RunID
	ExpectedRunGeneration uint64
	IdempotencyKey        string
	Finalizers            []FinalizerScope
	ErrorValues           values.ValueSet
}

// Validate reports malformed descendant plan metadata independently of the
// store-owned reachable-child and generation checks.
func (p CancellationDescendantPlan) Validate(reason Failure) error {
	if err := reason.Validate(); err != nil {
		return err
	}
	if err := validateOpaqueID("cancellation descendant run id", string(p.RunID)); err != nil || p.ExpectedRunGeneration == 0 {
		return fmt.Errorf("cancellation descendant requires run identity and generation")
	}
	if err := validateRequiredText("cancellation descendant idempotency key", p.IdempotencyKey); err != nil {
		return err
	}
	if len(p.Finalizers) == 0 {
		if len(p.ErrorValues) != 0 {
			return fmt.Errorf("cancellation descendant without finalizers cannot carry error values")
		}
		return nil
	}
	if err := validateFinalizerScopes(p.RunID, p.Finalizers); err != nil {
		return err
	}
	return ValidateRunControlErrorValues(p.ErrorValues, p.RunID, RunCanceled)
}

// Validate reports malformed cancellation-tree transport. State stores also
// compare Descendants with the exact reachable ParentCloseCancel closure.
func (r RequestRunCancellationWithFinalizersRequest) Validate() error {
	if err := r.Cancellation.Validate(); err != nil {
		return err
	}
	if len(r.Finalizers) != 0 {
		if err := validateFinalizerScopes(r.Cancellation.RunID, r.Finalizers); err != nil {
			return err
		}
		if err := ValidateRunControlErrorValues(r.ErrorValues, r.Cancellation.RunID, RunCanceled); err != nil {
			return err
		}
	} else if len(r.ErrorValues) != 0 {
		return fmt.Errorf("cancellation root without finalizers cannot carry error values")
	}
	keys := map[string]struct{}{r.Cancellation.IdempotencyKey: {}}
	var previous RunID
	for index, descendant := range r.Descendants {
		if err := descendant.Validate(r.Cancellation.Reason); err != nil {
			return fmt.Errorf("cancellation descendant[%d]: %w", index, err)
		}
		if descendant.RunID == r.Cancellation.RunID || index > 0 && descendant.RunID <= previous {
			return fmt.Errorf("cancellation descendants must be unique, exclude the root, and use canonical run order")
		}
		if _, duplicate := keys[descendant.IdempotencyKey]; duplicate {
			return fmt.Errorf("cancellation tree idempotency keys must be unique")
		}
		keys[descendant.IdempotencyKey] = struct{}{}
		previous = descendant.RunID
	}
	return nil
}

func validateFinalizerScopes(runID RunID, scopes []FinalizerScope) error {
	seen := make(map[NodeInvocationID]struct{}, len(scopes))
	lastOrder := -1
	var previous NodeInvocationID
	for index, scope := range scopes {
		if err := scope.Validate(runID); err != nil {
			return fmt.Errorf("finalizer[%d]: %w", index, err)
		}
		if _, duplicate := seen[scope.Invocation]; duplicate || scope.Order < lastOrder || scope.Order == lastOrder && index > 0 && !invocationLess(previous, scope.Invocation) {
			return fmt.Errorf("finalizers must be unique and ordered inner-to-outer")
		}
		seen[scope.Invocation] = struct{}{}
		lastOrder, previous = scope.Order, scope.Invocation
	}
	return nil
}

type RequestRunCancellationWithFinalizersResult struct {
	Cancellation RequestRunCancellationResult
	// Intent retains the root intent for the common root-finalizer case. It is
	// zero when only descendants declare cleanup.
	Intent          TerminalIntentSnapshot
	TerminalIntents []TerminalIntentSnapshot
}

// CancellationDescendantGraph supplies the exact stored graph and current run
// snapshot used to plan one direct-cancel descendant outside the store
// transaction. The store independently validates the complete reachable set.
type CancellationDescendantGraph struct {
	Run   RunSnapshot
	Graph graph.Graph
}

type CompleteTerminalIntentRequest struct {
	RunID                    RunID
	ExpectedRunGeneration    uint64
	ExpectedIntentGeneration uint64
	At                       time.Time
}

type CompleteTerminalIntentResult struct {
	Run    RunSnapshot
	Intent TerminalIntentSnapshot
	Event  Event
}

// ControlFlowStore is deliberately separate from StateStore while the Hadron
// SQLite persistence lane is serialized. Implementations must make each method
// atomic with its event and any ErrorValues persistence, assign those immutable
// ValueSet references themselves, and apply the same run/admission fencing as
// StateStore. A failed operation must leave no orphan error values or events.
type ControlFlowStore interface {
	LoadControlDecision(context.Context, ControlDecisionID) (ControlDecisionSnapshot, error)
	RecordControlDecision(context.Context, RecordControlDecisionRequest) (RecordControlDecisionResult, error)
	LoadTerminalIntent(context.Context, RunID) (TerminalIntentSnapshot, error)
	BeginTerminalIntent(context.Context, BeginTerminalIntentRequest) (BeginTerminalIntentResult, error)
	RequestRunCancellationWithFinalizers(context.Context, RequestRunCancellationWithFinalizersRequest) (RequestRunCancellationWithFinalizersResult, error)
	CompleteTerminalIntent(context.Context, CompleteTerminalIntentRequest) (CompleteTerminalIntentResult, error)
}

// ControlFlowCoordinator evaluates expressions outside storage transactions
// and persists only their deterministic selected-route facts.
type ControlFlowCoordinator struct {
	Store     StateStore
	Control   ControlFlowStore
	Evaluator PredicateEvaluator
}

func NewControlFlowCoordinator(store StateStore, control ControlFlowStore, evaluator PredicateEvaluator) *ControlFlowCoordinator {
	if evaluator == nil {
		evaluator = values.NewExpressionEngine()
	}
	return &ControlFlowCoordinator{Store: store, Control: control, Evaluator: evaluator}
}

type DecideSwitchRequest struct {
	Source            NodeInvocationID
	Node              graph.Node
	ExpressionContext values.ExpressionContext
	ExpressionOptions values.ExpressionOptions
	At                time.Time
}

func (c *ControlFlowCoordinator) DecideSwitch(ctx context.Context, request DecideSwitchRequest) (RecordControlDecisionResult, error) {
	if err := c.validate(ctx); err != nil {
		return RecordControlDecisionResult{}, err
	}
	if request.Node.Switch == nil || request.Node.ID != request.Source.NodeID || request.At.IsZero() {
		return RecordControlDecisionResult{}, fmt.Errorf("%w: switch request does not match source node", ErrInvalidControlFlow)
	}
	source, err := c.Store.LoadNodeInvocation(ctx, request.Source)
	if err != nil {
		return RecordControlDecisionResult{}, err
	}
	if source.Status != NodeSucceeded {
		return RecordControlDecisionResult{}, fmt.Errorf("%w: switch source must be succeeded", ErrControlFlowPending)
	}
	decision := ControlDecisionSnapshot{ID: ControlDecisionID{Source: request.Source, Kind: ControlSwitch}, Outcome: ControlUnmatched}
	for index, arm := range request.Node.Switch.Arms {
		matched, evalErr := c.Evaluator.EvaluateBool(arm.When, request.ExpressionContext, request.ExpressionOptions)
		if evalErr != nil {
			return RecordControlDecisionResult{}, evalErr
		}
		if matched {
			decision.Outcome, decision.RuleIndex = ControlSelected, intPointer(index)
			decision.Targets, err = routeInvocations(request.Source.RunID, arm.Targets)
			if err != nil {
				return RecordControlDecisionResult{}, err
			}
			break
		}
	}
	if decision.Outcome == ControlUnmatched && len(request.Node.Switch.Default) != 0 {
		decision.Outcome = ControlDefault
		decision.Targets, err = routeInvocations(request.Source.RunID, request.Node.Switch.Default)
		if err != nil {
			return RecordControlDecisionResult{}, err
		}
	}
	return c.Control.RecordControlDecision(context.WithoutCancel(ctx), RecordControlDecisionRequest{
		Decision: decision, ExpectedSourceGeneration: source.Generation, At: request.At,
	})
}

type DecideCatchRequest struct {
	Source            NodeInvocationID
	Node              graph.Node
	Failure           *Failure
	Timeout           TimeoutKind
	ExpressionContext values.ExpressionContext
	ExpressionOptions values.ExpressionOptions
	At                time.Time
}

func (c *ControlFlowCoordinator) DecideCatch(ctx context.Context, request DecideCatchRequest) (RecordControlDecisionResult, error) {
	if err := c.validate(ctx); err != nil {
		return RecordControlDecisionResult{}, err
	}
	if request.Node.ID != request.Source.NodeID || len(request.Node.Catch) == 0 || request.At.IsZero() {
		return RecordControlDecisionResult{}, fmt.Errorf("%w: catch request does not match source node", ErrInvalidControlFlow)
	}
	source, err := c.Store.LoadNodeInvocation(ctx, request.Source)
	if err != nil {
		return RecordControlDecisionResult{}, err
	}
	if !hardFailure(source.Status) {
		return RecordControlDecisionResult{}, fmt.Errorf("%w: catch source is not a hard failure", ErrControlFlowPending)
	}
	failure, attempt, err := c.originatingFailure(ctx, source, request.Failure)
	if err != nil {
		return RecordControlDecisionResult{}, err
	}
	timeout, err := coherentCatchTimeout(source.Status, request.Timeout, failure)
	if err != nil {
		return RecordControlDecisionResult{}, err
	}
	errorValue, err := NewFailureValue(source.ID, attempt, source.Status, timeout, failure)
	if err != nil {
		return RecordControlDecisionResult{}, err
	}
	errorSet := values.ValueSet{"error": errorValue}
	decision := ControlDecisionSnapshot{ID: ControlDecisionID{Source: source.ID, Kind: ControlCatch}, Outcome: ControlUnmatched}
	for index, rule := range request.Node.Catch {
		if !catchMatches(rule, failure, source.Status, timeout) {
			continue
		}
		contextCopy := request.ExpressionContext
		if rule.BindAs != "" {
			contextCopy.Locals = cloneValueSet(contextCopy.Locals)
			if contextCopy.Locals == nil {
				contextCopy.Locals = make(values.ValueSet)
			}
			contextCopy.Locals[rule.BindAs] = errorValue
		}
		contextCopy.Steps = cloneStepContexts(contextCopy.Steps)
		step := contextCopy.Steps[source.ID.NodeID]
		step.Error = valuePointer(errorValue)
		step.Status = string(source.Status)
		contextCopy.Steps[source.ID.NodeID] = step
		if rule.When != nil {
			matched, evalErr := c.Evaluator.EvaluateBool(*rule.When, contextCopy, request.ExpressionOptions)
			if evalErr != nil {
				return RecordControlDecisionResult{}, evalErr
			}
			if !matched {
				continue
			}
		}
		if rule.ContinueOnError() {
			decision.Outcome = ControlContinued
			break
		}
		decision.Outcome, decision.RuleIndex = ControlSelected, intPointer(index)
		decision.Targets, err = routeInvocations(source.ID.RunID, rule.Targets)
		if err != nil {
			return RecordControlDecisionResult{}, err
		}
		decision.BindAs = rule.BindAs
		break
	}
	return c.Control.RecordControlDecision(context.WithoutCancel(ctx), RecordControlDecisionRequest{
		Decision: decision, ErrorValues: errorSet, ExpectedSourceGeneration: source.Generation, At: request.At,
	})
}

func coherentCatchTimeout(status NodeStatus, supplied TimeoutKind, failure Failure) (TimeoutKind, error) {
	if supplied != "" && (!supplied.Valid() || status != NodeTimedOut) {
		return "", fmt.Errorf("%w: timeout kind %q is incoherent with source status %q", ErrInvalidControlFlow, supplied, status)
	}
	persisted := TimeoutKind(failure.Details["timeout_kind"])
	if persisted == "" {
		return supplied, nil
	}
	if !persisted.Valid() || status != NodeTimedOut {
		return "", fmt.Errorf("%w: persisted timeout kind %q is incoherent with source status %q", ErrInvalidControlFlow, persisted, status)
	}
	if supplied != "" && supplied != persisted {
		return "", fmt.Errorf("%w: supplied timeout kind %q differs from persisted %q", ErrControlFlowConflict, supplied, persisted)
	}
	return persisted, nil
}

// durableFailureTimeout reconstructs timeout identity from the durable
// failure envelope. Callers must use it whenever a persisted failure is
// projected back into a typed workflow value so timeout classification cannot
// be dropped or relabeled across restart.
func durableFailureTimeout(status NodeStatus, failure Failure) (TimeoutKind, error) {
	return coherentCatchTimeout(status, "", failure)
}

func (c *ControlFlowCoordinator) validate(ctx context.Context) error {
	if ctx == nil || c == nil || nilStateStore(c.Store) || nilControlFlowStore(c.Control) || c.Evaluator == nil {
		return fmt.Errorf("%w: context, stores, and evaluator are required", ErrInvalidControlFlow)
	}
	return ctx.Err()
}

func nilControlFlowStore(store ControlFlowStore) bool {
	if store == nil {
		return true
	}
	value := reflect.ValueOf(store)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (c *ControlFlowCoordinator) originatingFailure(ctx context.Context, source NodeInvocationSnapshot, supplied *Failure) (Failure, *AttemptID, error) {
	if source.LatestAttempt > 0 {
		id := AttemptID{Invocation: source.ID, Number: source.LatestAttempt}
		attempt, err := c.Store.LoadAttempt(ctx, id)
		if err != nil {
			return Failure{}, nil, err
		}
		if attempt.Failure == nil || attempt.Status != source.Status {
			return Failure{}, nil, fmt.Errorf("%w: latest attempt does not contain source failure", ErrInvalidControlFlow)
		}
		if supplied != nil && !equalFailure(*supplied, *attempt.Failure) {
			return Failure{}, nil, fmt.Errorf("%w: supplied failure differs from durable attempt", ErrControlFlowConflict)
		}
		return cloneFailureValue(*attempt.Failure), &id, nil
	}
	if supplied == nil {
		return Failure{}, nil, fmt.Errorf("%w: pre-attempt failure must be supplied", ErrInvalidControlFlow)
	}
	if err := supplied.Validate(); err != nil {
		return Failure{}, nil, err
	}
	return cloneFailureValue(*supplied), nil, nil
}

// NewFailureValue creates the typed, private/run-retained error exposed at
// steps.<id>.error and fan-out items[].error. Exact json.Number use is retained
// for the attempt number. Failure is already a persistence-boundary record;
// callers must not pass raw causes or unredacted secret material in it.
func NewFailureValue(origin NodeInvocationID, attempt *AttemptID, status NodeStatus, timeout TimeoutKind, failure Failure) (values.Value, error) {
	if err := origin.Validate(); err != nil || !hardFailure(status) {
		return values.Value{}, fmt.Errorf("%w: failure origin is invalid", ErrInvalidControlFlow)
	}
	if err := failure.Validate(); err != nil {
		return values.Value{}, err
	}
	if timeout != "" && (!timeout.Valid() || status != NodeTimedOut) {
		return values.Value{}, fmt.Errorf("%w: failure timeout metadata is invalid", ErrInvalidControlFlow)
	}
	if detail, exists := failure.Details["timeout_kind"]; exists {
		detailKind := TimeoutKind(detail)
		if status != NodeTimedOut || timeout == "" || !detailKind.Valid() || detailKind != timeout {
			return values.Value{}, fmt.Errorf("%w: failure timeout detail differs from timeout metadata", ErrInvalidControlFlow)
		}
	}
	if attempt != nil {
		if err := attempt.Validate(); err != nil || attempt.Invocation != origin {
			return values.Value{}, fmt.Errorf("%w: failure attempt does not match origin", ErrInvalidControlFlow)
		}
	}
	details := make(map[string]any, len(failure.Details))
	keys := make([]string, 0, len(failure.Details))
	for key := range failure.Details {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		details[key] = failure.Details[key]
	}
	payload := map[string]any{
		"code": failure.Code, "message": failure.Message, "retryable": failure.Retryable,
		"details": details, "status": string(status), "node_id": origin.NodeID,
	}
	if timeout != "" {
		payload["timeout_kind"] = string(timeout)
	}
	if attempt != nil {
		payload["attempt"] = json.Number(strconv.Itoa(attempt.Number))
	}
	return values.NewInline(payload, values.Metadata{
		Producer:  values.Producer{Kind: "workflow-error", Reference: controlIdentity(origin), Output: "error"},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
}

// NewRunFailureValue creates the typed originating error retained by an
// unsuccessful terminal intent that is not attributable to one node attempt,
// such as an explicit run cancellation.
func NewRunFailureValue(runID RunID, status RunStatus, failure Failure) (values.Value, error) {
	if err := validateOpaqueID("run failure run id", string(runID)); err != nil || !status.Terminal() || status == RunSucceeded {
		return values.Value{}, fmt.Errorf("%w: run failure origin is invalid", ErrInvalidControlFlow)
	}
	if err := failure.Validate(); err != nil {
		return values.Value{}, err
	}
	details := make(map[string]any, len(failure.Details))
	keys := make([]string, 0, len(failure.Details))
	for key := range failure.Details {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		details[key] = failure.Details[key]
	}
	return values.NewInline(map[string]any{
		"code": failure.Code, "message": failure.Message, "retryable": failure.Retryable,
		"details": details, "status": string(status), "run_id": string(runID),
	}, values.Metadata{
		Producer:  values.Producer{Kind: "workflow-run-error", Reference: string(runID), Output: "error"},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
}

// ValidateControlErrorValues validates the exact typed error envelope accepted
// by ControlFlowStore. It is exported so extraction hosts and persistence
// adapters reject corrupt or caller-crafted transport records consistently.
func ValidateControlErrorValues(input values.ValueSet) error {
	if len(input) != 1 {
		return fmt.Errorf("%w: control-flow error values must contain exactly error", ErrInvalidControlFlow)
	}
	errorValue, ok := input["error"]
	if !ok || errorValue.Type != values.TypeObject || errorValue.Redaction != values.RedactionPrivate || errorValue.Retention != values.RetentionRun || errorValue.MediaType != "application/json" {
		return fmt.Errorf("%w: control-flow error must be an inline JSON object with private/run classification", ErrInvalidControlFlow)
	}
	if err := values.ValidatePersistableSet(input); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidControlFlow, err)
	}
	payload, ok := errorValue.Inline.(map[string]any)
	if !ok {
		return fmt.Errorf("%w: control-flow error payload must be an object", ErrInvalidControlFlow)
	}
	code, codeOK := payload["code"].(string)
	message, messageOK := payload["message"].(string)
	retryable, retryableOK := payload["retryable"].(bool)
	details, detailsOK := payload["details"].(map[string]any)
	status, statusOK := payload["status"].(string)
	if !codeOK || !messageOK || !retryableOK || !detailsOK || !statusOK {
		return fmt.Errorf("%w: control-flow error payload omits required typed fields", ErrInvalidControlFlow)
	}
	if err := (Failure{Code: code, Message: message, Retryable: retryable, Details: controlErrorDetails(details)}).Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidControlFlow, err)
	}
	for key, value := range details {
		if _, ok := value.(string); !ok {
			return fmt.Errorf("%w: control-flow error detail %q must be a string", ErrInvalidControlFlow, key)
		}
	}
	nodeField, hasNodeField := payload["node_id"]
	runField, hasRunField := payload["run_id"]
	if hasNodeField == hasRunField {
		return fmt.Errorf("%w: control-flow error must identify exactly one node or run", ErrInvalidControlFlow)
	}
	nodeID, hasNode := nodeField.(string)
	runID, hasRun := runField.(string)
	if hasNodeField != hasNode || hasRunField != hasRun {
		return fmt.Errorf("%w: control-flow error identity must be a string", ErrInvalidControlFlow)
	}
	if errorValue.Producer.Output != "error" {
		return fmt.Errorf("%w: control-flow error producer output must be error", ErrInvalidControlFlow)
	}
	if hasNode {
		allowed := map[string]struct{}{"attempt": {}, "code": {}, "details": {}, "message": {}, "node_id": {}, "retryable": {}, "status": {}, "timeout_kind": {}}
		for key := range payload {
			if _, ok := allowed[key]; !ok {
				return fmt.Errorf("%w: node control-flow error field %q is not supported", ErrInvalidControlFlow, key)
			}
		}
		nodeStatus := NodeStatus(status)
		if err := graph.ValidateID(nodeID); err != nil || !hardFailure(nodeStatus) || errorValue.Producer.Kind != "workflow-error" {
			return fmt.Errorf("%w: node control-flow error identity is invalid", ErrInvalidControlFlow)
		}
		if attempt, exists := payload["attempt"]; exists {
			number, ok := attempt.(json.Number)
			parsed, err := strconv.ParseInt(string(number), 10, 64)
			if !ok || err != nil || parsed < 1 {
				return fmt.Errorf("%w: control-flow error attempt must be a positive exact integer", ErrInvalidControlFlow)
			}
		}
		timeoutText := ""
		if timeout, exists := payload["timeout_kind"]; exists {
			kind, ok := timeout.(string)
			if !ok || !TimeoutKind(kind).Valid() || nodeStatus != NodeTimedOut {
				return fmt.Errorf("%w: control-flow timeout metadata is invalid", ErrInvalidControlFlow)
			}
			timeoutText = kind
		}
		if detail, exists := details["timeout_kind"]; exists {
			detailText, ok := detail.(string)
			if !ok || nodeStatus != NodeTimedOut || timeoutText == "" || !TimeoutKind(detailText).Valid() || detailText != timeoutText {
				return fmt.Errorf("%w: control-flow timeout detail differs from timeout metadata", ErrInvalidControlFlow)
			}
		}
		return nil
	}
	allowed := map[string]struct{}{"code": {}, "details": {}, "message": {}, "retryable": {}, "run_id": {}, "status": {}}
	for key := range payload {
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("%w: run control-flow error field %q is not supported", ErrInvalidControlFlow, key)
		}
	}
	runStatus := RunStatus(status)
	if err := validateOpaqueID("control-flow error run id", runID); err != nil || !runStatus.Terminal() || runStatus == RunSucceeded || errorValue.Producer.Kind != "workflow-run-error" || errorValue.Producer.Reference != runID {
		return fmt.Errorf("%w: run control-flow error identity is invalid", ErrInvalidControlFlow)
	}
	if _, exists := payload["attempt"]; exists {
		return fmt.Errorf("%w: run control-flow error cannot carry an attempt", ErrInvalidControlFlow)
	}
	if _, exists := payload["timeout_kind"]; exists {
		return fmt.Errorf("%w: run control-flow error cannot carry node timeout metadata", ErrInvalidControlFlow)
	}
	return nil
}

// ValidateNodeControlErrorValues binds a structurally valid typed error to the
// exact durable node fact that owns it. Stores call this after loading the
// source so a caller cannot attach another node's otherwise-valid error.
func ValidateNodeControlErrorValues(input values.ValueSet, origin NodeInvocationID, attempt *AttemptID, status NodeStatus, expectedFailure *Failure) error {
	if err := ValidateControlErrorValues(input); err != nil {
		return err
	}
	if err := origin.Validate(); err != nil || !hardFailure(status) {
		return fmt.Errorf("%w: expected node error origin is invalid", ErrInvalidControlFlow)
	}
	errorValue := input["error"]
	payload, _ := errorValue.Inline.(map[string]any)
	nodeID, _ := payload["node_id"].(string)
	persistedStatus, _ := payload["status"].(string)
	if nodeID != origin.NodeID || NodeStatus(persistedStatus) != status || errorValue.Producer.Reference != controlIdentity(origin) {
		return fmt.Errorf("%w: node control-flow error does not match its durable source", ErrInvalidControlFlow)
	}
	if expectedFailure != nil {
		persistedFailure, err := controlFailureFromPayload(payload)
		if err != nil || !equalFailure(persistedFailure, *expectedFailure) {
			return fmt.Errorf("%w: node control-flow error differs from durable failure", ErrInvalidControlFlow)
		}
	}
	persistedAttempt, hasAttempt := payload["attempt"]
	if attempt == nil {
		if hasAttempt {
			return fmt.Errorf("%w: pre-attempt node error cannot identify an attempt", ErrInvalidControlFlow)
		}
		return nil
	}
	if attempt.Invocation != origin {
		return fmt.Errorf("%w: expected attempt belongs to another node", ErrInvalidControlFlow)
	}
	number, ok := persistedAttempt.(json.Number)
	parsed, err := strconv.Atoi(string(number))
	if !hasAttempt || !ok || err != nil || parsed != attempt.Number {
		return fmt.Errorf("%w: node control-flow error attempt does not match its durable source", ErrInvalidControlFlow)
	}
	return nil
}

// ValidateRunControlErrorValues binds a typed run error to the exact terminal
// outcome requested by a durable terminal intent.
func ValidateRunControlErrorValues(input values.ValueSet, runID RunID, status RunStatus) error {
	if err := ValidateControlErrorValues(input); err != nil {
		return err
	}
	if err := validateOpaqueID("run error binding", string(runID)); err != nil || !status.Terminal() || status == RunSucceeded {
		return fmt.Errorf("%w: expected run error origin is invalid", ErrInvalidControlFlow)
	}
	errorValue := input["error"]
	payload, _ := errorValue.Inline.(map[string]any)
	persistedRun, _ := payload["run_id"].(string)
	persistedStatus, _ := payload["status"].(string)
	if persistedRun != string(runID) || RunStatus(persistedStatus) != status || errorValue.Producer.Reference != string(runID) {
		return fmt.Errorf("%w: run control-flow error does not match its durable terminal intent", ErrInvalidControlFlow)
	}
	return nil
}

// ControlErrorOrigin identifies the durable node/attempt behind a terminal
// error. A run-level error leaves both pointers nil.
type ControlErrorOrigin struct {
	Invocation *NodeInvocationID
	Attempt    *AttemptID
}

// ValidateTerminalControlErrorValues binds an unsuccessful terminal intent's
// typed error to its run and intended status. The retained error may be a
// run-level fact (for example explicit cancellation) or the exact originating
// node failure selected during terminal accounting.
func ValidateTerminalControlErrorValues(input values.ValueSet, runID RunID, status RunStatus, reason *Failure) (ControlErrorOrigin, error) {
	if err := ValidateControlErrorValues(input); err != nil {
		return ControlErrorOrigin{}, err
	}
	if err := validateOpaqueID("terminal error run", string(runID)); err != nil || !status.Terminal() || status == RunSucceeded {
		return ControlErrorOrigin{}, fmt.Errorf("%w: expected terminal error origin is invalid", ErrInvalidControlFlow)
	}
	errorValue := input["error"]
	payload, _ := errorValue.Inline.(map[string]any)
	persistedStatus, _ := payload["status"].(string)
	if RunStatus(persistedStatus) != status {
		return ControlErrorOrigin{}, fmt.Errorf("%w: terminal error status differs from intended status", ErrInvalidControlFlow)
	}
	if reason != nil {
		persistedFailure, err := controlFailureFromPayload(payload)
		if err != nil || !equalFailure(persistedFailure, *reason) {
			return ControlErrorOrigin{}, fmt.Errorf("%w: terminal error differs from immutable reason", ErrInvalidControlFlow)
		}
	}
	if _, isRun := payload["run_id"]; isRun {
		return ControlErrorOrigin{}, ValidateRunControlErrorValues(input, runID, status)
	}
	origin, err := parseControlIdentity(errorValue.Producer.Reference)
	nodeID, _ := payload["node_id"].(string)
	if err != nil || origin.RunID != runID || origin.NodeID != nodeID {
		return ControlErrorOrigin{}, fmt.Errorf("%w: node terminal error belongs to another origin", ErrInvalidControlFlow)
	}
	result := ControlErrorOrigin{Invocation: &origin}
	if encodedAttempt, exists := payload["attempt"]; exists {
		number, ok := encodedAttempt.(json.Number)
		parsed, parseErr := strconv.Atoi(string(number))
		if !ok || parseErr != nil || parsed < 1 {
			return ControlErrorOrigin{}, fmt.Errorf("%w: terminal error attempt is invalid", ErrInvalidControlFlow)
		}
		attempt := AttemptID{Invocation: origin, Number: parsed}
		result.Attempt = &attempt
	}
	return result, nil
}

func controlFailureFromPayload(payload map[string]any) (Failure, error) {
	code, codeOK := payload["code"].(string)
	message, messageOK := payload["message"].(string)
	retryable, retryableOK := payload["retryable"].(bool)
	details, detailsOK := payload["details"].(map[string]any)
	if !codeOK || !messageOK || !retryableOK || !detailsOK {
		return Failure{}, errors.New("typed failure payload is incomplete")
	}
	failure := Failure{Code: code, Message: message, Retryable: retryable, Details: controlErrorDetails(details)}
	return failure, failure.Validate()
}

func controlErrorDetails(input map[string]any) map[string]string {
	result := make(map[string]string, len(input))
	for key, value := range input {
		if text, ok := value.(string); ok {
			result[key] = text
		}
	}
	return result
}

func catchMatches(rule graph.CatchRule, failure Failure, status NodeStatus, timeout TimeoutKind) bool {
	if len(rule.Errors) == 0 {
		return true
	}
	for _, selector := range rule.Errors {
		if selector == graph.CatchAllErrors || selector == failure.Code || selector == string(status) || timeout != "" && selector == string(timeout) {
			return true
		}
	}
	return false
}

func routeInvocations(runID RunID, targets []string) ([]NodeInvocationID, error) {
	result := make([]NodeInvocationID, len(targets))
	for index, target := range targets {
		if err := graph.ValidateID(target); err != nil {
			return nil, fmt.Errorf("%w: route target[%d]: %w", ErrInvalidControlFlow, index, err)
		}
		result[index] = NodeInvocationID{RunID: runID, NodeID: target}
	}
	return result, nil
}

func controlIdentity(id NodeInvocationID) string {
	return strconv.Itoa(len(id.RunID)) + ":" + string(id.RunID) + strconv.Itoa(len(id.NodeID)) + ":" + id.NodeID + strconv.Itoa(len(id.Iteration)) + ":" + id.Iteration
}

func parseControlIdentity(encoded string) (NodeInvocationID, error) {
	parts := make([]string, 0, 3)
	remaining := encoded
	for range 3 {
		separator := strings.IndexByte(remaining, ':')
		if separator < 1 {
			return NodeInvocationID{}, errors.New("control identity length prefix is malformed")
		}
		length, err := strconv.Atoi(remaining[:separator])
		if err != nil || length < 0 || len(remaining[separator+1:]) < length {
			return NodeInvocationID{}, errors.New("control identity component is malformed")
		}
		remaining = remaining[separator+1:]
		parts = append(parts, remaining[:length])
		remaining = remaining[length:]
	}
	identity := NodeInvocationID{RunID: RunID(parts[0]), NodeID: parts[1], Iteration: parts[2]}
	if remaining != "" {
		return NodeInvocationID{}, errors.New("control identity has trailing data")
	}
	return identity, identity.Validate()
}

func intPointer(value int) *int                     { return &value }
func valuePointer(value values.Value) *values.Value { cloned := value; return &cloned }

func cloneFailureValue(value Failure) Failure {
	value.Details = cloneStringMapRuntime(value.Details)
	return value
}

func equalFailure(left, right Failure) bool {
	if left.Code != right.Code || left.Message != right.Message || left.Retryable != right.Retryable || len(left.Details) != len(right.Details) {
		return false
	}
	for key, value := range left.Details {
		if right.Details[key] != value {
			return false
		}
	}
	return true
}

func cloneStringMapRuntime(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func cloneStepContexts(input map[string]values.StepContext) map[string]values.StepContext {
	if input == nil {
		return make(map[string]values.StepContext)
	}
	result := make(map[string]values.StepContext, len(input))
	for key, step := range input {
		result[key] = step
	}
	return result
}
