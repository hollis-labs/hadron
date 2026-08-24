package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	EventFanOutExpanded  = "fanout.expanded"
	EventFanOutCompleted = "fanout.completed"
)

var (
	ErrInvalidFanOut    = errors.New("invalid workflow fan-out")
	ErrFanOutIncomplete = errors.New("workflow fan-out is incomplete")
	ErrFanOutLimit      = errors.New("workflow fan-out concurrency limit reached")
)

// FanOutStatus is the aggregate lifecycle stored independently from worker
// leases and individual item invocations.
type FanOutStatus string

const (
	FanOutActive    FanOutStatus = "active"
	FanOutSucceeded FanOutStatus = "succeeded"
	FanOutFailed    FanOutStatus = "failed"
	FanOutCanceled  FanOutStatus = "canceled"
)

func (s FanOutStatus) Valid() bool {
	switch s {
	case FanOutActive, FanOutSucceeded, FanOutFailed, FanOutCanceled:
		return true
	default:
		return false
	}
}

// FanOutItemBinding is the immutable relationship between an evaluated item,
// its stable zero-based index, typed local input set, and child invocation.
type FanOutItemBinding struct {
	Index      int                `json:"index"`
	Iteration  string             `json:"iteration"`
	Invocation NodeInvocationID   `json:"invocation"`
	Inputs     values.ValueSetRef `json:"inputs"`
}

func (b FanOutItemBinding) Validate(parent NodeInvocationID) error {
	if b.Index < 0 {
		return fmt.Errorf("fan-out item index must not be negative")
	}
	if b.Iteration != FanOutIteration(b.Index) {
		return fmt.Errorf("fan-out item %d has noncanonical iteration %q", b.Index, b.Iteration)
	}
	if err := b.Invocation.Validate(); err != nil {
		return err
	}
	if b.Invocation.RunID != parent.RunID || b.Invocation.NodeID != parent.NodeID || b.Invocation.Iteration != b.Iteration {
		return fmt.Errorf("fan-out item invocation must be an expansion of its parent")
	}
	return b.Inputs.Validate()
}

// FanOutSnapshot is one durable runtime expansion. MaxConcurrency counts
// started nonterminal child invocations, including retry delays, generic waits,
// and pending external operations; it is deliberately independent of leases.
type FanOutSnapshot struct {
	Parent         NodeInvocationID              `json:"parent"`
	ItemName       string                        `json:"item_name"`
	IndexName      string                        `json:"index_name"`
	MaxConcurrency int                           `json:"max_concurrency,omitempty"`
	FailFast       bool                          `json:"fail_fast,omitempty"`
	Tolerate       *graph.ToleratedFailurePolicy `json:"tolerate,omitempty"`
	Status         FanOutStatus                  `json:"status"`
	Items          []FanOutItemBinding           `json:"items"`
	Outputs        *values.ValueSetRef           `json:"outputs,omitempty"`
	Failure        *Failure                      `json:"failure,omitempty"`
	Generation     uint64                        `json:"generation"`
	CreatedAt      time.Time                     `json:"created_at"`
	UpdatedAt      time.Time                     `json:"updated_at"`
}

func (s FanOutSnapshot) Validate() error {
	if err := s.Parent.Validate(); err != nil {
		return err
	}
	if s.Parent.Iteration != "" {
		return fmt.Errorf("fan-out aggregate parent must have empty iteration")
	}
	if err := validateRequiredText("fan-out item name", s.ItemName); err != nil {
		return err
	}
	if err := validateRequiredText("fan-out index name", s.IndexName); err != nil {
		return err
	}
	if s.ItemName == s.IndexName {
		return fmt.Errorf("fan-out item and index names must differ")
	}
	if s.MaxConcurrency < 0 {
		return fmt.Errorf("fan-out max_concurrency must not be negative")
	}
	if err := ValidateFanOutTolerance(s.Tolerate); err != nil {
		return err
	}
	if !s.Status.Valid() {
		return fmt.Errorf("unsupported fan-out status %q", s.Status)
	}
	for index, item := range s.Items {
		if item.Index != index {
			return fmt.Errorf("fan-out item indexes must be contiguous and ordered")
		}
		if err := item.Validate(s.Parent); err != nil {
			return err
		}
	}
	if err := validateOptionalValueSetRef(s.Outputs); err != nil {
		return err
	}
	if s.Failure != nil {
		if err := s.Failure.Validate(); err != nil {
			return err
		}
	}
	switch s.Status {
	case FanOutActive:
		if s.Outputs != nil || s.Failure != nil {
			return fmt.Errorf("active fan-out must not contain terminal outcome")
		}
	case FanOutSucceeded:
		if s.Outputs == nil || s.Failure != nil {
			return fmt.Errorf("succeeded fan-out requires only aggregate outputs")
		}
	case FanOutFailed:
		if s.Outputs == nil || s.Failure == nil {
			return fmt.Errorf("unsuccessful fan-out requires outputs and failure")
		}
	case FanOutCanceled:
		if s.Failure == nil {
			return fmt.Errorf("canceled fan-out requires failure")
		}
	}
	return validateSnapshotTimes(s.Generation, s.CreatedAt, s.UpdatedAt)
}

// FanOutItemResult is the lossless recovery projection for one item. Outputs
// is the immutable durable reference and OutputValues is its defensively
// loaded typed ValueSet. Parent aggregate values intentionally contain only
// the reference because ValueSet has no nested typed-envelope container.
type FanOutItemResult struct {
	Index        int                 `json:"index"`
	Iteration    string              `json:"iteration"`
	Invocation   NodeInvocationID    `json:"invocation"`
	Status       NodeStatus          `json:"status"`
	Outputs      *values.ValueSetRef `json:"outputs,omitempty"`
	OutputValues values.ValueSet     `json:"output_values,omitempty"`
	Failure      *Failure            `json:"failure,omitempty"`
}

// FanOutIteration produces a stable identity whose lexical order equals index
// order for every supported nonnegative int value.
func FanOutIteration(index int) string {
	return fmt.Sprintf("item-%020d", index)
}

// ValidateFanOutTolerance requires a single count or percentage tolerance.
func ValidateFanOutTolerance(policy *graph.ToleratedFailurePolicy) error {
	if policy == nil {
		return nil
	}
	if policy.Count < 0 || math.IsNaN(policy.Percentage) || math.IsInf(policy.Percentage, 0) || policy.Percentage < 0 || policy.Percentage > 100 {
		return fmt.Errorf("%w: tolerance must be a nonnegative count or percentage up to 100", ErrInvalidFanOut)
	}
	if policy.Count != 0 && policy.Percentage != 0 {
		return fmt.Errorf("%w: count and percentage tolerance are mutually exclusive", ErrInvalidFanOut)
	}
	return nil
}

// FanOutFailuresTolerated applies count or percentage policy inclusively. Nil
// policy and an explicit zero tolerance both reject any hard item failure.
func FanOutFailuresTolerated(policy *graph.ToleratedFailurePolicy, failures, total int) (bool, error) {
	if err := ValidateFanOutTolerance(policy); err != nil {
		return false, err
	}
	if failures < 0 || total < 0 || failures > total {
		return false, fmt.Errorf("%w: invalid failure counts", ErrInvalidFanOut)
	}
	if policy == nil || policy.Percentage == 0 {
		limit := 0
		if policy != nil {
			limit = policy.Count
		}
		return failures <= limit, nil
	}
	// Compare by multiplication to avoid binary rounding at the observed ratio.
	return float64(failures)*100 <= policy.Percentage*float64(total), nil
}

type ExpandFanOutRequest struct {
	FanOut                   FanOutSnapshot
	ExpectedParentGeneration uint64
	Priority                 int
	At                       time.Time
}

func (r ExpandFanOutRequest) Validate() error {
	if r.FanOut.Generation != 0 || r.FanOut.Status != FanOutActive {
		return fmt.Errorf("new fan-out must be active with zero generation")
	}
	if r.ExpectedParentGeneration == 0 || r.At.IsZero() {
		return fmt.Errorf("fan-out expansion requires parent generation and timestamp")
	}
	candidate := cloneFanOut(r.FanOut)
	candidate.Generation = 1
	candidate.CreatedAt, candidate.UpdatedAt = r.At, r.At
	return candidate.Validate()
}

type ExpandFanOutResult struct {
	FanOut   FanOutSnapshot
	Parent   NodeInvocationSnapshot
	Children []NodeInvocationSnapshot
	Event    Event
}

type CompleteFanOutRequest struct {
	Parent                   NodeInvocationID
	ExpectedParentGeneration uint64
	ExpectedFanOutGeneration uint64
	ExpectedChildGenerations map[NodeInvocationID]uint64
	Status                   FanOutStatus
	Outputs                  values.ValueSetRef
	Failure                  *Failure
	At                       time.Time
}

func (r CompleteFanOutRequest) Validate() error {
	if err := r.Parent.Validate(); err != nil {
		return err
	}
	if r.ExpectedParentGeneration == 0 || r.ExpectedFanOutGeneration == 0 || r.At.IsZero() {
		return fmt.Errorf("fan-out completion requires generations and timestamp")
	}
	if r.Status != FanOutSucceeded && r.Status != FanOutFailed && r.Status != FanOutCanceled {
		return fmt.Errorf("fan-out completion requires terminal aggregate status")
	}
	if err := r.Outputs.Validate(); err != nil {
		return err
	}
	if r.Status == FanOutSucceeded && r.Failure != nil {
		return fmt.Errorf("succeeded fan-out must not contain failure")
	}
	if r.Status != FanOutSucceeded {
		if r.Failure == nil {
			return fmt.Errorf("unsuccessful fan-out requires failure")
		}
		if err := r.Failure.Validate(); err != nil {
			return err
		}
	}
	for child, generation := range r.ExpectedChildGenerations {
		if err := child.Validate(); err != nil || generation == 0 {
			return fmt.Errorf("fan-out completion has invalid child generation")
		}
	}
	return nil
}

type CompleteFanOutResult struct {
	FanOut FanOutSnapshot
	Parent NodeInvocationSnapshot
	Event  Event
}

// FanOutStore persists expansion identity and performs atomic aggregate
// creation/completion. Implementations also enforce MaxConcurrency when a
// child's first attempt starts.
type FanOutStore interface {
	LoadFanOut(context.Context, NodeInvocationID) (FanOutSnapshot, error)
	LoadFanOutItemResults(context.Context, NodeInvocationID) ([]FanOutItemResult, error)
	ExpandFanOut(context.Context, ExpandFanOutRequest) (ExpandFanOutResult, error)
	CompleteFanOut(context.Context, CompleteFanOutRequest) (CompleteFanOutResult, error)
}

// FanOutExpressionEvaluator is the narrow typed expression seam.
type FanOutExpressionEvaluator interface {
	EvaluateRaw(graph.Expression, values.ExpressionContext, values.ExpressionOptions) (any, error)
}

type FanOutCoordinator struct {
	Store     StateStore
	Evaluator FanOutExpressionEvaluator
	Retention RetentionHook
}

// FanOutExpandCommand evaluates one static for_each declaration and persists
// the immutable runtime expansion. Saved item input sets may remain unreferenced
// if the final atomic expansion loses CAS; they never create partial children.
type FanOutExpandCommand struct {
	Parent                   NodeInvocationID
	ExpectedParentGeneration uint64
	Spec                     graph.ForEachSpec
	ExpressionContext        values.ExpressionContext
	ExpressionOptions        values.ExpressionOptions
	Priority                 int
	At                       time.Time
}

func (c FanOutCoordinator) Expand(ctx context.Context, command FanOutExpandCommand) (ExpandFanOutResult, error) {
	if ctx == nil || nilStateStore(c.Store) {
		return ExpandFanOutResult{}, fmt.Errorf("%w: fan-out requires context and state store", ErrInvalidFanOut)
	}
	if err := command.Parent.Validate(); err != nil || command.Parent.Iteration != "" || command.ExpectedParentGeneration == 0 || command.At.IsZero() {
		return ExpandFanOutResult{}, fmt.Errorf("%w: invalid expansion identity, generation, or timestamp", ErrInvalidFanOut)
	}
	if err := ValidateFanOutTolerance(command.Spec.Tolerate); err != nil {
		return ExpandFanOutResult{}, err
	}
	evaluator := c.Evaluator
	if evaluator == nil {
		evaluator = values.NewExpressionEngine()
	}
	raw, err := evaluator.EvaluateRaw(command.Spec.Items, command.ExpressionContext, command.ExpressionOptions)
	if err != nil {
		return ExpandFanOutResult{}, err
	}
	items, ok := raw.([]any)
	if !ok {
		return ExpandFanOutResult{}, fmt.Errorf("%w: for_each.items must evaluate to an array, got %T", ErrInvalidFanOut, raw)
	}
	itemName, indexName := command.Spec.ItemName, command.Spec.IndexName
	if itemName == "" {
		itemName = "item"
	}
	if indexName == "" {
		indexName = "index"
	}
	bindings := make([]FanOutItemBinding, len(items))
	for index, item := range items {
		iteration := FanOutIteration(index)
		producer := values.Producer{Kind: "fanout-item", Reference: fanOutIdentity(command.Parent)}
		metadata := values.Metadata{Producer: producer, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun}
		itemValue, valueErr := values.NewInline(item, metadata)
		if valueErr != nil {
			return ExpandFanOutResult{}, fmt.Errorf("%w: item %d: %w", ErrInvalidFanOut, index, valueErr)
		}
		indexValue, valueErr := values.NewInline(json.Number(strconv.Itoa(index)), metadata)
		if valueErr != nil {
			return ExpandFanOutResult{}, fmt.Errorf("%w: index %d: %w", ErrInvalidFanOut, index, valueErr)
		}
		invocation := NodeInvocationID{RunID: command.Parent.RunID, NodeID: command.Parent.NodeID, Iteration: iteration}
		set := values.ValueSet{itemName: itemValue, indexName: indexValue}
		ref, saveErr := SaveValuesWithRetention(ctx, c.Store, c.Retention, SaveValuesRequest{
			Owner: ValueOwner{Kind: "fanout-item-inputs", RunID: command.Parent.RunID, Invocation: &invocation}, Values: set,
		})
		if saveErr != nil {
			return ExpandFanOutResult{}, saveErr
		}
		bindings[index] = FanOutItemBinding{Index: index, Iteration: iteration, Invocation: invocation, Inputs: ref}
	}
	snapshot := FanOutSnapshot{
		Parent: command.Parent, ItemName: itemName, IndexName: indexName,
		MaxConcurrency: command.Spec.MaxConcurrency, FailFast: command.Spec.FailFast, Tolerate: cloneFanOutTolerance(command.Spec.Tolerate),
		Status: FanOutActive, Items: bindings,
	}
	return c.Store.ExpandFanOut(ctx, ExpandFanOutRequest{
		FanOut: snapshot, ExpectedParentGeneration: command.ExpectedParentGeneration,
		Priority: command.Priority, At: command.At,
	})
}

// ReconcileFailFast fences admission and cancels fan-out items that have not
// started after failures exceed the declared tolerance. The transition uses
// ordinary node CAS and is therefore safe to replay and race with a worker
// claim; already-started items are never interrupted by this policy.
func (c FanOutCoordinator) ReconcileFailFast(ctx context.Context, parent NodeInvocationID, at time.Time) ([]NodeInvocationSnapshot, error) {
	if ctx == nil || nilStateStore(c.Store) || at.IsZero() {
		return nil, fmt.Errorf("%w: fail-fast reconciliation requires context, store, and timestamp", ErrInvalidFanOut)
	}
	fanOut, err := c.Store.LoadFanOut(ctx, parent)
	if err != nil {
		return nil, err
	}
	if !fanOut.FailFast || fanOut.Status != FanOutActive {
		return nil, nil
	}
	children := make([]NodeInvocationSnapshot, 0, len(fanOut.Items))
	failures := 0
	for _, item := range fanOut.Items {
		child, loadErr := c.Store.LoadNodeInvocation(ctx, item.Invocation)
		if loadErr != nil {
			return nil, loadErr
		}
		children = append(children, child)
		if hardFailure(child.Status) && child.LatestAttempt > 0 {
			failures++
		}
	}
	tolerated, err := FanOutFailuresTolerated(fanOut.Tolerate, failures, len(children))
	if err != nil || tolerated {
		return nil, err
	}
	canceled := make([]NodeInvocationSnapshot, 0)
	for _, child := range children {
		if child.LatestAttempt != 0 || child.Status.Terminal() {
			continue
		}
		result, transitionErr := c.Store.TransitionNode(ctx, NodeTransitionRequest{
			InvocationID: child.ID, ExpectedGeneration: child.Generation, To: NodeCanceled, At: at,
		})
		if transitionErr != nil {
			if errors.Is(transitionErr, ErrCASMismatch) {
				continue
			}
			return canceled, transitionErr
		}
		canceled = append(canceled, result.Snapshot)
	}
	return canceled, nil
}

// FanOutFailFastAdmissionAllowed reports whether a first attempt may be
// admitted under the immutable fan-out snapshot. Stores call it while holding
// their claim transaction/lock so no post-failure first attempt slips through.
func FanOutFailFastAdmissionAllowed(fanOut FanOutSnapshot, children []NodeInvocationSnapshot) (bool, error) {
	if !fanOut.FailFast || fanOut.Status != FanOutActive {
		return true, nil
	}
	failures := 0
	for _, child := range children {
		if hardFailure(child.Status) && child.LatestAttempt > 0 {
			failures++
		}
	}
	tolerated, err := FanOutFailuresTolerated(fanOut.Tolerate, failures, len(children))
	return tolerated, err
}

// Collect loads every terminal child in stable index order, publishes an
// aggregate items value containing typed output references and structured
// failures, then atomically finalizes the aggregate parent.
func (c FanOutCoordinator) Collect(ctx context.Context, parent NodeInvocationID, at time.Time) (CompleteFanOutResult, values.ValueSet, []FanOutItemResult, error) {
	if ctx == nil || nilStateStore(c.Store) || at.IsZero() {
		return CompleteFanOutResult{}, nil, nil, fmt.Errorf("%w: collect requires context, store, and timestamp", ErrInvalidFanOut)
	}
	fanOut, err := c.Store.LoadFanOut(ctx, parent)
	if err != nil {
		return CompleteFanOutResult{}, nil, nil, err
	}
	parentNode, err := c.Store.LoadNodeInvocation(ctx, parent)
	if err != nil {
		return CompleteFanOutResult{}, nil, nil, err
	}
	results, err := c.Store.LoadFanOutItemResults(ctx, parent)
	if err != nil {
		return CompleteFanOutResult{}, nil, nil, err
	}
	if len(results) != len(fanOut.Items) {
		return CompleteFanOutResult{}, nil, nil, fmt.Errorf("%w: item result count does not match expansion", ErrInvalidFanOut)
	}
	generations := make(map[NodeInvocationID]uint64, len(fanOut.Items))
	failures := 0
	projections := make([]any, len(fanOut.Items))
	for index, binding := range fanOut.Items {
		node, loadErr := c.Store.LoadNodeInvocation(ctx, binding.Invocation)
		if loadErr != nil {
			return CompleteFanOutResult{}, nil, nil, loadErr
		}
		if !node.Status.Terminal() {
			return CompleteFanOutResult{}, nil, nil, fmt.Errorf("%w: item %d is %s", ErrFanOutIncomplete, index, node.Status)
		}
		item := results[index]
		if item.Index != index || item.Iteration != binding.Iteration || item.Invocation != binding.Invocation || item.Status != node.Status {
			return CompleteFanOutResult{}, nil, nil, fmt.Errorf("%w: item result %d does not match durable invocation", ErrInvalidFanOut, index)
		}
		if hardFailure(item.Status) {
			failures++
			if item.Failure == nil {
				item.Failure = &Failure{Code: "fanout_item_" + string(item.Status), Message: "fan-out item ended " + string(item.Status)}
			}
		}
		results[index] = item
		generations[node.ID] = node.Generation
		projection := map[string]any{"index": json.Number(strconv.Itoa(index)), "iteration": binding.Iteration, "status": string(node.Status), "outputs_ref": nil}
		if item.Outputs != nil {
			projection["outputs_ref"] = map[string]any{"id": item.Outputs.ID, "digest": item.Outputs.Digest}
		}
		if item.Failure != nil {
			projection["error"] = fanOutFailureProjection(*item.Failure)
		}
		projections[index] = projection
	}
	tolerated, err := FanOutFailuresTolerated(fanOut.Tolerate, failures, len(results))
	if err != nil {
		return CompleteFanOutResult{}, nil, nil, err
	}
	metadata := values.Metadata{
		Producer:  values.Producer{Kind: "fanout-aggregate", Reference: fanOutIdentity(parent), Output: "items"},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	}
	itemsValue, err := values.NewInline(projections, metadata)
	if err != nil {
		return CompleteFanOutResult{}, nil, nil, err
	}
	aggregateValues := values.ValueSet{"items": itemsValue}
	outputRef, err := SaveValuesWithRetention(ctx, c.Store, c.Retention, SaveValuesRequest{
		Owner: ValueOwner{Kind: "fanout-aggregate-outputs", RunID: parent.RunID, Invocation: &parent}, Values: aggregateValues,
	})
	if err != nil {
		return CompleteFanOutResult{}, nil, nil, err
	}
	status := FanOutSucceeded
	var failure *Failure
	if !tolerated {
		status = FanOutFailed
		failure = &Failure{
			Code: "fanout_failure_tolerance_exceeded", Message: "fan-out item failures exceeded configured tolerance",
			Details: map[string]string{"failures": strconv.Itoa(failures), "total": strconv.Itoa(len(results))},
		}
	}
	completed, err := c.Store.CompleteFanOut(ctx, CompleteFanOutRequest{
		Parent: parent, ExpectedParentGeneration: parentNode.Generation, ExpectedFanOutGeneration: fanOut.Generation,
		ExpectedChildGenerations: generations, Status: status, Outputs: outputRef, Failure: failure, At: at,
	})
	if err != nil {
		return CompleteFanOutResult{}, nil, nil, err
	}
	return completed, cloneValueSet(aggregateValues), cloneFanOutItemResults(results), nil
}

func cloneFanOut(snapshot FanOutSnapshot) FanOutSnapshot {
	snapshot.Tolerate = cloneFanOutTolerance(snapshot.Tolerate)
	snapshot.Items = append([]FanOutItemBinding(nil), snapshot.Items...)
	snapshot.Outputs = cloneValueSetRef(snapshot.Outputs)
	snapshot.Failure = cloneFanOutFailure(snapshot.Failure)
	return snapshot
}

func cloneFanOutTolerance(policy *graph.ToleratedFailurePolicy) *graph.ToleratedFailurePolicy {
	if policy == nil {
		return nil
	}
	copyPolicy := *policy
	return &copyPolicy
}

func cloneFanOutItemResults(items []FanOutItemResult) []FanOutItemResult {
	result := make([]FanOutItemResult, len(items))
	for index, item := range items {
		item.Outputs = cloneValueSetRef(item.Outputs)
		item.OutputValues = cloneValueSet(item.OutputValues)
		item.Failure = cloneFanOutFailure(item.Failure)
		result[index] = item
	}
	return result
}

func fanOutFailureProjection(failure Failure) map[string]any {
	projection := map[string]any{"code": failure.Code, "message": failure.Message, "retryable": failure.Retryable}
	if len(failure.Details) != 0 {
		details := make(map[string]any, len(failure.Details))
		for key, value := range failure.Details {
			details[key] = value
		}
		projection["details"] = details
	}
	return projection
}

func cloneFanOutFailure(failure *Failure) *Failure {
	if failure == nil {
		return nil
	}
	copyFailure := *failure
	if failure.Details != nil {
		copyFailure.Details = make(map[string]string, len(failure.Details))
		for key, value := range failure.Details {
			copyFailure.Details[key] = value
		}
	}
	return &copyFailure
}

func fanOutIdentity(parent NodeInvocationID) string {
	return string(parent.RunID) + "/" + parent.NodeID
}
