package appworkflow

import (
	"context"
	"errors"
	"fmt"

	calladapter "github.com/hollis-labs/go-workflow/adapters/call"
	"github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/stepkind"
	"github.com/hollis-labs/go-workflow/values"
)

// ErrCallContextUnavailable reports that the parent-run expression context for
// a call@v1 invocation could not be reconstructed from durable state.
var ErrCallContextUnavailable = errors.New("call expression context is unavailable")

// CallExpressionContextProvider implements calladapter.ContextProvider from
// durable state only. call@v1 evaluates the child definition's input defaults
// and resolver/import bindings in the parent run's context before it selects
// a mode, so this provider must reproduce exactly the expression scope the
// dispatcher would give the calling node:
//
//   - the parent run's pinned plan is loaded through Plans (never re-resolved);
//   - runtime.BuildExpressionContext rebuilds run inputs and step outputs;
//   - a fan-out iteration receives its durable item and index roots, matching
//     runtime recovery's idempotency-key evaluation;
//   - the compiler visibility plan scopes the context to the node's direct
//     explicit-plus-inferred producers and sets ExpressionOptions.VisibleSteps
//     to that allowlist. Host policy options (AllowEnv) stay at the host
//     default (false), which is what Hadron passes to recovery and dispatch.
type CallExpressionContextProvider struct {
	State   runtime.StateStore
	Control runtime.ControlFlowStore
	Plans   runtime.RecoveryPlanSource
}

var _ calladapter.ContextProvider = CallExpressionContextProvider{}

// NewCallExpressionContextProvider validates every durable collaborator.
func NewCallExpressionContextProvider(state runtime.StateStore, control runtime.ControlFlowStore, plans runtime.RecoveryPlanSource) (CallExpressionContextProvider, error) {
	if nilInterface(state) || nilInterface(control) || nilInterface(plans) {
		return CallExpressionContextProvider{}, fmt.Errorf("%w: state, control-flow store, and plan source are required", ErrCallContextUnavailable)
	}
	return CallExpressionContextProvider{State: state, Control: control, Plans: plans}, nil
}

// ExpressionContext implements calladapter.ContextProvider.
func (p CallExpressionContextProvider) ExpressionContext(ctx context.Context, invocation stepkind.Invocation) (values.ExpressionContext, values.ExpressionOptions, error) {
	if ctx == nil {
		return values.ExpressionContext{}, values.ExpressionOptions{}, fmt.Errorf("%w: context is required", ErrCallContextUnavailable)
	}
	if nilInterface(p.State) || nilInterface(p.Control) || nilInterface(p.Plans) {
		return values.ExpressionContext{}, values.ExpressionOptions{}, fmt.Errorf("%w: provider is not initialized", ErrCallContextUnavailable)
	}
	runID := runtime.RunID(invocation.Identity.RunID)
	nodeID := invocation.Identity.NodeID
	if runID == "" || nodeID == "" {
		return values.ExpressionContext{}, values.ExpressionOptions{}, fmt.Errorf("%w: invocation run and node identity are required", ErrCallContextUnavailable)
	}
	run, err := p.State.LoadRun(ctx, runID)
	if err != nil {
		return values.ExpressionContext{}, values.ExpressionOptions{}, err
	}
	plan, err := p.Plans.LoadRecoveryPlan(ctx, run)
	if err != nil {
		return values.ExpressionContext{}, values.ExpressionOptions{}, err
	}
	available, err := runtime.BuildExpressionContext(ctx, p.State, p.Control, plan.Plan.Graph, runID)
	if err != nil {
		return values.ExpressionContext{}, values.ExpressionOptions{}, err
	}
	if iteration := invocation.Identity.Iteration; iteration != "" {
		item, index, itemErr := p.fanOutItem(ctx, runtime.NodeInvocationID{RunID: runID, NodeID: nodeID, Iteration: iteration})
		if itemErr != nil {
			return values.ExpressionContext{}, values.ExpressionOptions{}, itemErr
		}
		available.Item, available.Index = &item, &index
	}
	scoped, options, err := plan.Visibility.ScopeNodeContext(nodeID, available, values.ExpressionOptions{})
	if err != nil {
		return values.ExpressionContext{}, values.ExpressionOptions{}, fmt.Errorf("%w: compiler visibility: %w", ErrCallContextUnavailable, err)
	}
	return scoped, options, nil
}

func (p CallExpressionContextProvider) fanOutItem(ctx context.Context, id runtime.NodeInvocationID) (values.Value, int, error) {
	fanOut, err := p.State.LoadFanOut(ctx, runtime.NodeInvocationID{RunID: id.RunID, NodeID: id.NodeID})
	if err != nil {
		return values.Value{}, 0, err
	}
	for _, binding := range fanOut.Items {
		if binding.Invocation != id {
			continue
		}
		set, loadErr := p.State.LoadValues(ctx, binding.Inputs)
		if loadErr != nil {
			return values.Value{}, 0, loadErr
		}
		item, exists := set[fanOut.ItemName]
		if !exists {
			return values.Value{}, 0, fmt.Errorf("%w: fan-out item value is missing", ErrCallContextUnavailable)
		}
		return item, binding.Index, nil
	}
	return values.Value{}, 0, fmt.Errorf("%w: fan-out iteration is absent from durable expansion", ErrCallContextUnavailable)
}
