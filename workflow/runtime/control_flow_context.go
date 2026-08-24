package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

// BuildExpressionContext reconstructs typed step outputs and errors entirely
// from durable state. It is safe to call after restart and never unwraps an
// ArtifactRef or SecretRef into the returned context.
func BuildExpressionContext(ctx context.Context, store StateStore, control ControlFlowStore, workflow graph.Graph, runID RunID) (values.ExpressionContext, error) {
	if ctx == nil || nilStateStore(store) {
		return values.ExpressionContext{}, fmt.Errorf("%w: context and state store are required", ErrInvalidControlFlow)
	}
	result := values.ExpressionContext{Steps: make(map[string]values.StepContext, len(workflow.Nodes))}
	run, err := store.LoadRun(ctx, runID)
	if err != nil {
		return values.ExpressionContext{}, err
	}
	if run.Inputs != nil {
		result.Inputs, err = store.LoadValues(ctx, *run.Inputs)
		if err != nil {
			return values.ExpressionContext{}, err
		}
	}
	for _, definition := range workflow.Nodes {
		id := NodeInvocationID{RunID: runID, NodeID: definition.ID}
		node, err := store.LoadNodeInvocation(ctx, id)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return values.ExpressionContext{}, err
		}
		step, err := durableStepContext(ctx, store, control, node)
		if err != nil {
			return values.ExpressionContext{}, err
		}
		if definition.ForEach != nil {
			items, loadErr := store.LoadFanOutItemResults(ctx, id)
			if loadErr != nil && !errors.Is(loadErr, ErrNotFound) {
				return values.ExpressionContext{}, loadErr
			}
			step.Items = make([]values.StepContext, len(items))
			for index, item := range items {
				itemStep := values.StepContext{Status: string(item.Status), Outputs: cloneValueSet(item.OutputValues)}
				if item.Failure != nil {
					itemNode, nodeErr := store.LoadNodeInvocation(ctx, item.Invocation)
					if nodeErr != nil {
						return values.ExpressionContext{}, nodeErr
					}
					var attemptID *AttemptID
					if itemNode.LatestAttempt > 0 {
						id := AttemptID{Invocation: item.Invocation, Number: itemNode.LatestAttempt}
						attemptID = &id
					}
					timeout, timeoutErr := durableFailureTimeout(item.Status, *item.Failure)
					if timeoutErr != nil {
						return values.ExpressionContext{}, timeoutErr
					}
					typed, valueErr := NewFailureValue(item.Invocation, attemptID, item.Status, timeout, *item.Failure)
					if valueErr != nil {
						return values.ExpressionContext{}, valueErr
					}
					itemStep.Error = valuePointer(typed)
				}
				step.Items[index] = itemStep
			}
		}
		result.Steps[definition.ID] = step
	}
	return result, nil
}

func durableStepContext(ctx context.Context, store StateStore, control ControlFlowStore, node NodeInvocationSnapshot) (values.StepContext, error) {
	step := values.StepContext{Status: string(node.Status)}
	if node.Outputs != nil {
		outputs, err := store.LoadValues(ctx, *node.Outputs)
		if err != nil {
			return values.StepContext{}, err
		}
		step.Outputs = outputs
	}
	if !nilControlFlowStore(control) {
		decision, err := control.LoadControlDecision(ctx, ControlDecisionID{Source: node.ID, Kind: ControlCatch})
		if err == nil && decision.Error != nil {
			set, loadErr := store.LoadValues(ctx, *decision.Error)
			if loadErr != nil {
				return values.StepContext{}, loadErr
			}
			errorValue, exists := set["error"]
			if !exists {
				return values.StepContext{}, fmt.Errorf("%w: control error value set has no error", ErrInvalidControlFlow)
			}
			step.Error = valuePointer(errorValue)
			return step, nil
		}
		if err != nil && !errors.Is(err, ErrNotFound) {
			return values.StepContext{}, err
		}
	}
	if hardFailure(node.Status) && node.LatestAttempt > 0 {
		attemptID := AttemptID{Invocation: node.ID, Number: node.LatestAttempt}
		attempt, err := store.LoadAttempt(ctx, attemptID)
		if err != nil {
			return values.StepContext{}, err
		}
		if attempt.Failure != nil {
			timeout, timeoutErr := durableFailureTimeout(node.Status, *attempt.Failure)
			if timeoutErr != nil {
				return values.StepContext{}, timeoutErr
			}
			typed, valueErr := NewFailureValue(node.ID, &attemptID, node.Status, timeout, *attempt.Failure)
			if valueErr != nil {
				return values.StepContext{}, valueErr
			}
			step.Error = valuePointer(typed)
		}
	}
	if hardFailure(node.Status) && node.LatestAttempt == 0 && !nilControlFlowStore(control) {
		intent, err := control.LoadTerminalIntent(ctx, node.ID.RunID)
		if err == nil && intent.Error != nil {
			set, loadErr := store.LoadValues(ctx, *intent.Error)
			if loadErr != nil {
				return values.StepContext{}, loadErr
			}
			origin, validationErr := ValidateTerminalControlErrorValues(set, intent.RunID, intent.IntendedStatus, intent.Reason)
			if validationErr != nil {
				return values.StepContext{}, validationErr
			}
			if origin.Invocation != nil && *origin.Invocation == node.ID && origin.Attempt == nil {
				errorValue, exists := set["error"]
				if !exists {
					return values.StepContext{}, fmt.Errorf("%w: terminal intent error value set has no error", ErrInvalidControlFlow)
				}
				step.Error = valuePointer(errorValue)
			}
		} else if err != nil && !errors.Is(err, ErrNotFound) {
			return values.StepContext{}, err
		}
	}
	return step, nil
}

// CatchBinding loads the exact typed error selected by a catch decision. The
// returned set is suitable for ExpressionContext.Locals and is defensively
// copied by StateStore.
func CatchBinding(ctx context.Context, store StateStore, control ControlFlowStore, id ControlDecisionID) (string, values.ValueSet, error) {
	if ctx == nil || nilStateStore(store) || nilControlFlowStore(control) {
		return "", nil, fmt.Errorf("%w: context and state stores are required", ErrInvalidControlFlow)
	}
	if id.Kind != ControlCatch {
		return "", nil, fmt.Errorf("%w: catch binding requires catch decision", ErrInvalidControlFlow)
	}
	decision, err := control.LoadControlDecision(ctx, id)
	if err != nil {
		return "", nil, err
	}
	if decision.Outcome != ControlSelected || decision.BindAs == "" || decision.Error == nil {
		return "", nil, nil
	}
	set, err := store.LoadValues(ctx, *decision.Error)
	if err != nil {
		return "", nil, err
	}
	errorValue, ok := set["error"]
	if !ok {
		return "", nil, fmt.Errorf("%w: catch decision error set is malformed", ErrInvalidControlFlow)
	}
	return decision.BindAs, values.ValueSet{decision.BindAs: errorValue}, nil
}

type ProgressControlNodeRequest struct {
	Graph             graph.Graph
	InvocationID      NodeInvocationID
	Dependencies      []DependencyRef
	ExpressionContext values.ExpressionContext
	ExpressionOptions values.ExpressionOptions
	At                time.Time
}

type ControlNodePreview struct {
	Target       graph.Node
	Dependencies []DependencyRef
	Context      values.ExpressionContext
	RouteOwned   bool
	Selected     bool
	OwnerIDs     []NodeInvocationID
}

// PreviewControlNode reconstructs the authoritative route decision set and
// catch locals without changing lifecycle state. Drivers use it to determine
// eligibility before persisting node-local inputs.
func (c *ControlFlowCoordinator) PreviewControlNode(ctx context.Context, request ProgressControlNodeRequest) (ControlNodePreview, error) {
	if err := c.validate(ctx); err != nil {
		return ControlNodePreview{}, err
	}
	var preview ControlNodePreview
	for _, node := range request.Graph.Nodes {
		if node.ID == request.InvocationID.NodeID {
			preview.Target = node
			break
		}
	}
	if preview.Target.ID == "" {
		return ControlNodePreview{}, fmt.Errorf("%w: target node is absent from graph", ErrInvalidControlFlow)
	}
	type owner struct {
		source graph.Node
		kind   ControlDecisionKind
	}
	var routes []owner
	for _, source := range request.Graph.Nodes {
		for _, rule := range source.Catch {
			for _, id := range rule.Targets {
				if id == preview.Target.ID {
					routes = append(routes, owner{source, ControlCatch})
				}
			}
		}
		if source.Switch != nil {
			for _, arm := range source.Switch.Arms {
				for _, id := range arm.Targets {
					if id == preview.Target.ID {
						routes = append(routes, owner{source, ControlSwitch})
					}
				}
			}
			for _, id := range source.Switch.Default {
				if id == preview.Target.ID {
					routes = append(routes, owner{source, ControlSwitch})
				}
			}
		}
	}
	preview.Context = request.ExpressionContext
	for _, dependency := range request.Dependencies {
		if dependency.InvocationID.RunID != request.InvocationID.RunID {
			return ControlNodePreview{}, fmt.Errorf("%w: route dependency belongs to another run", ErrInvalidControlFlow)
		}
		preview.Dependencies = mergeControlDependency(preview.Dependencies, dependency)
	}
	if len(routes) == 0 {
		preview.Selected = true
		return preview, nil
	}
	preview.RouteOwned = true
	sort.Slice(routes, func(i, j int) bool {
		if routes[i].source.ID != routes[j].source.ID {
			return routes[i].source.ID < routes[j].source.ID
		}
		return routes[i].kind < routes[j].kind
	})
	unique := routes[:0]
	for _, route := range routes {
		if len(unique) == 0 || unique[len(unique)-1].source.ID != route.source.ID || unique[len(unique)-1].kind != route.kind {
			unique = append(unique, route)
		}
	}
	for _, route := range unique {
		sourceID := NodeInvocationID{RunID: request.InvocationID.RunID, NodeID: route.source.ID}
		preview.OwnerIDs = appendUniqueInvocation(preview.OwnerIDs, sourceID)
		decision, err := c.Control.LoadControlDecision(ctx, ControlDecisionID{Source: sourceID, Kind: route.kind})
		if errors.Is(err, ErrNotFound) {
			source, loadErr := c.Store.LoadNodeInvocation(ctx, sourceID)
			if loadErr != nil {
				return ControlNodePreview{}, loadErr
			}
			if !source.Status.Terminal() || controlRouteApplicable(route.kind, source.Status) {
				return ControlNodePreview{}, ErrControlFlowPending
			}
			continue
		}
		if err != nil {
			return ControlNodePreview{}, err
		}
		selected := false
		for _, target := range decision.Targets {
			if target == request.InvocationID {
				selected = true
				break
			}
		}
		if !selected {
			continue
		}
		preview.Selected = true
		preview.Dependencies = mergeControlDependency(preview.Dependencies, DependencyRef{InvocationID: sourceID, FailureHandled: route.kind == ControlCatch})
		if route.kind == ControlCatch && decision.BindAs != "" {
			name, local, bindErr := CatchBinding(ctx, c.Store, c.Control, decision.ID)
			if bindErr != nil {
				return ControlNodePreview{}, bindErr
			}
			if _, exists := preview.Context.Locals[name]; exists {
				return ControlNodePreview{}, fmt.Errorf("%w: catch binding %q shadows an existing local", ErrControlFlowConflict, name)
			}
			preview.Context.Locals = cloneValueSet(preview.Context.Locals)
			if preview.Context.Locals == nil {
				preview.Context.Locals = make(values.ValueSet)
			}
			for key, value := range local {
				preview.Context.Locals[key] = value
			}
		}
	}
	return preview, nil
}

// ProgressControlNode applies a persisted switch/catch selection before normal
// readiness. An unselected route is durably skipped; a selected catch source is
// success-equivalent only for its handler and retains its typed error binding.
func (c *ControlFlowCoordinator) ProgressControlNode(ctx context.Context, request ProgressControlNodeRequest) (ProgressNodeResult, error) {
	preview, err := c.PreviewControlNode(ctx, request)
	if err != nil {
		return ProgressNodeResult{}, err
	}
	if preview.RouteOwned && !preview.Selected {
		snapshot, loadErr := c.Store.LoadNodeInvocation(ctx, request.InvocationID)
		if loadErr != nil {
			return ProgressNodeResult{}, loadErr
		}
		reason := &BlockedReason{Code: "control_route_not_selected", Message: "no owning control-flow route selected this node", Dependencies: preview.OwnerIDs, Details: map[string]string{"owners": fmt.Sprint(len(preview.OwnerIDs))}}
		return NewProgressionCoordinator(c.Store, c.Evaluator).skipNode(ctx, snapshot, reason, false, false, request.At)
	}
	return NewProgressionCoordinator(c.Store, c.Evaluator).ProgressNode(ctx, ProgressNodeRequest{InvocationID: request.InvocationID, Dependencies: preview.Dependencies, Rule: preview.Target.ReadyWhen, Predicate: preview.Target.If, ExpressionContext: preview.Context, ExpressionOptions: request.ExpressionOptions, At: request.At})
}

func mergeControlDependency(dependencies []DependencyRef, incoming DependencyRef) []DependencyRef {
	for index := range dependencies {
		if dependencies[index].InvocationID == incoming.InvocationID {
			dependencies[index].FailureHandled = dependencies[index].FailureHandled || incoming.FailureHandled
			return dependencies
		}
	}
	return append(dependencies, incoming)
}

func appendUniqueInvocation(invocations []NodeInvocationID, incoming NodeInvocationID) []NodeInvocationID {
	for _, existing := range invocations {
		if existing == incoming {
			return invocations
		}
	}
	return append(invocations, incoming)
}

// PlanFinalizerScopes derives deterministic inner-to-outer durable scopes from
// one immutable graph. An empty declared scope means the whole workflow.
func PlanFinalizerScopes(workflow graph.Graph, runID RunID) ([]FinalizerScope, error) {
	if err := validateOpaqueID("finalizer plan run id", string(runID)); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidControlFlow, err)
	}
	type planned struct {
		node     graph.Node
		ordinary map[string]struct{}
		members  map[string]struct{}
	}
	definitions := make(map[string]graph.Node, len(workflow.Nodes))
	var finalizers []planned
	for _, node := range workflow.Nodes {
		if err := graph.ValidateID(node.ID); err != nil {
			return nil, fmt.Errorf("%w: finalizer plan node: %w", ErrInvalidControlFlow, err)
		}
		if _, duplicate := definitions[node.ID]; duplicate {
			return nil, fmt.Errorf("%w: finalizer plan repeats node %q", ErrInvalidControlFlow, node.ID)
		}
		definitions[node.ID] = node
	}
	for _, node := range workflow.Nodes {
		if node.Finally != nil {
			scope := make(map[string]struct{}, len(node.Finally.Scope))
			if len(node.Finally.Scope) == 0 {
				for _, member := range workflow.Nodes {
					if member.Finally == nil {
						scope[member.ID] = struct{}{}
					}
				}
			} else {
				for _, member := range node.Finally.Scope {
					if _, exists := definitions[member]; !exists {
						return nil, fmt.Errorf("%w: finally %q scopes unknown node %q", ErrInvalidControlFlow, node.ID, member)
					}
					scope[member] = struct{}{}
				}
			}
			finalizers = append(finalizers, planned{node: node, ordinary: scope, members: cloneStringSet(scope)})
		}
	}
	for outerIndex := range finalizers {
		outerGlobal := len(finalizers[outerIndex].node.Finally.Scope) == 0
		for innerIndex := range finalizers {
			if outerIndex == innerIndex {
				continue
			}
			innerGlobal := len(finalizers[innerIndex].node.Finally.Scope) == 0
			if outerGlobal && !innerGlobal || strictSubset(finalizers[innerIndex].ordinary, finalizers[outerIndex].ordinary) {
				finalizers[outerIndex].members[finalizers[innerIndex].node.ID] = struct{}{}
			}
		}
	}
	depthMemo := make(map[string]int, len(finalizers))
	visiting := make(map[string]bool, len(finalizers))
	byID := make(map[string]int, len(finalizers))
	for index := range finalizers {
		byID[finalizers[index].node.ID] = index
	}
	var depth func(string) (int, error)
	depth = func(id string) (int, error) {
		if value, ok := depthMemo[id]; ok {
			return value, nil
		}
		if visiting[id] {
			return 0, fmt.Errorf("%w: finally scope cycle", ErrInvalidControlFlow)
		}
		visiting[id] = true
		value := 0
		for member := range finalizers[byID[id]].members {
			if _, isFinalizer := byID[member]; !isFinalizer {
				continue
			}
			inner, err := depth(member)
			if err != nil {
				return 0, err
			}
			if inner+1 > value {
				value = inner + 1
			}
		}
		visiting[id] = false
		depthMemo[id] = value
		return value, nil
	}
	result := make([]FinalizerScope, len(finalizers))
	for index, item := range finalizers {
		order, err := depth(item.node.ID)
		if err != nil {
			return nil, err
		}
		members := make([]string, 0, len(item.members))
		for member := range item.members {
			members = append(members, member)
		}
		sort.Strings(members)
		scope := make([]NodeInvocationID, len(members))
		for memberIndex, member := range members {
			scope[memberIndex] = NodeInvocationID{RunID: runID, NodeID: member}
		}
		result[index] = FinalizerScope{Invocation: NodeInvocationID{RunID: runID, NodeID: item.node.ID}, Scope: scope, Order: order}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Order != result[j].Order {
			return result[i].Order < result[j].Order
		}
		return result[i].Invocation.NodeID < result[j].Invocation.NodeID
	})
	for _, scope := range result {
		if err := scope.Validate(runID); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func controlRouteApplicable(kind ControlDecisionKind, status NodeStatus) bool {
	switch kind {
	case ControlSwitch:
		return status == NodeSucceeded
	case ControlCatch:
		return hardFailure(status)
	default:
		return false
	}
}

func cloneStringSet(input map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{}, len(input))
	for key := range input {
		result[key] = struct{}{}
	}
	return result
}
func strictSubset(inner, outer map[string]struct{}) bool {
	if len(inner) >= len(outer) {
		return false
	}
	for member := range inner {
		if _, ok := outer[member]; !ok {
			return false
		}
	}
	return true
}

// ProgressFinally makes one finalizer eligible only after its durable scope is
// terminal and all remote cancellation intents are resolved.
func (c *ControlFlowCoordinator) ProgressFinally(ctx context.Context, workflow graph.Graph, invocation NodeInvocationID, expressionContext values.ExpressionContext, expressionOptions values.ExpressionOptions, at time.Time) (ProgressNodeResult, error) {
	preview, err := c.PreviewFinally(ctx, workflow, invocation, expressionContext)
	if err != nil {
		return ProgressNodeResult{}, err
	}
	return NewProgressionCoordinator(c.Store, c.Evaluator).ProgressNode(ctx, ProgressNodeRequest{
		InvocationID: invocation, Dependencies: preview.Dependencies, Rule: graph.ReadyAllDone,
		Predicate: preview.Definition.If, ExpressionContext: preview.Context, ExpressionOptions: expressionOptions, At: at,
	})
}

// FinalizerPreview is the read-only, durable eligibility/context projection
// shared by input binding and finalizer progression. It prevents recovery from
// evaluating or persisting cleanup inputs before the exact scope is eligible.
type FinalizerPreview struct {
	Definition   graph.Node
	Context      values.ExpressionContext
	Dependencies []DependencyRef
}

// PreviewFinally validates terminal-intent ownership, nested ordering, and
// cancellation reconciliation without mutating state.
func (c *ControlFlowCoordinator) PreviewFinally(ctx context.Context, workflow graph.Graph, invocation NodeInvocationID, expressionContext values.ExpressionContext) (FinalizerPreview, error) {
	if err := c.validate(ctx); err != nil {
		return FinalizerPreview{}, err
	}
	intent, err := c.Control.LoadTerminalIntent(ctx, invocation.RunID)
	if err != nil {
		return FinalizerPreview{}, err
	}
	if intent.Status != TerminalIntentPending {
		return FinalizerPreview{}, fmt.Errorf("%w: terminal intent is complete", ErrControlFlowConflict)
	}
	var selected *FinalizerScope
	for index := range intent.Finalizers {
		if intent.Finalizers[index].Invocation == invocation {
			selected = &intent.Finalizers[index]
			break
		}
	}
	if selected == nil {
		return FinalizerPreview{}, fmt.Errorf("%w: invocation is not a declared finalizer", ErrInvalidControlFlow)
	}
	var definition *graph.Node
	for index := range workflow.Nodes {
		if workflow.Nodes[index].ID == invocation.NodeID {
			definition = &workflow.Nodes[index]
			break
		}
	}
	if definition == nil {
		return FinalizerPreview{}, fmt.Errorf("%w: declared finalizer is absent from supplied graph", ErrControlFlowConflict)
	}
	if definition.Finally == nil {
		return FinalizerPreview{}, fmt.Errorf("%w: declared finalizer is ordinary in supplied graph", ErrControlFlowConflict)
	}
	for _, prerequisite := range intent.Finalizers {
		if prerequisite.Order >= selected.Order || !containsInvocation(selected.Scope, prerequisite.Invocation) {
			continue
		}
		node, loadErr := c.Store.LoadNodeInvocation(ctx, prerequisite.Invocation)
		if loadErr != nil {
			return FinalizerPreview{}, loadErr
		}
		if !node.Status.Terminal() {
			return FinalizerPreview{}, ErrControlFlowPending
		}
	}
	pending, recoverErr := c.Store.RecoverCancellationIntents(ctx, CancellationIntentQuery{RunID: invocation.RunID})
	if recoverErr != nil {
		return FinalizerPreview{}, recoverErr
	}
	for _, item := range pending {
		if item.Status == CancellationPending {
			return FinalizerPreview{}, ErrControlFlowPending
		}
	}
	durableContext, err := BuildExpressionContext(ctx, c.Store, c.Control, workflow, invocation.RunID)
	if err != nil {
		return FinalizerPreview{}, err
	}
	if expressionContext.Inputs == nil {
		expressionContext.Inputs = durableContext.Inputs
	}
	if expressionContext.Steps == nil {
		expressionContext.Steps = durableContext.Steps
	}
	expressionContext.Run = cloneAnyMap(expressionContext.Run)
	expressionContext.Run["status"] = string(intent.IntendedStatus)
	expressionContext.Run["finally"] = map[string]any{"order": selected.Order, "node_id": invocation.NodeID}
	if intent.Error != nil {
		set, loadErr := c.Store.LoadValues(ctx, *intent.Error)
		if loadErr != nil {
			return FinalizerPreview{}, loadErr
		}
		typed, exists := set["error"]
		if !exists || typed.Type != values.TypeObject {
			return FinalizerPreview{}, fmt.Errorf("%w: terminal intent error values are malformed", ErrInvalidControlFlow)
		}
		// Store validation guarantees a private/run-retained inline error. It
		// remains typed at rest and is unwrapped only into the private evaluator
		// copy, matching steps.<id>.error behavior.
		expressionContext.Run["error"] = typed.Inline
	}
	dependencies := make([]DependencyRef, 0, len(selected.Scope)+len(definition.Needs))
	for _, member := range selected.Scope {
		dependencies = mergeControlDependency(dependencies, DependencyRef{InvocationID: member})
	}
	for _, need := range definition.Needs {
		if err := graph.ValidateID(need.Node); err != nil {
			return FinalizerPreview{}, fmt.Errorf("%w: finalizer need: %w", ErrInvalidControlFlow, err)
		}
		dependencies = mergeControlDependency(dependencies, DependencyRef{InvocationID: NodeInvocationID{RunID: invocation.RunID, NodeID: need.Node}})
	}
	for _, edge := range workflow.Edges {
		if edge.To != invocation.NodeID {
			continue
		}
		if err := graph.ValidateID(edge.From); err != nil {
			return FinalizerPreview{}, fmt.Errorf("%w: finalizer edge: %w", ErrInvalidControlFlow, err)
		}
		dependencies = mergeControlDependency(dependencies, DependencyRef{InvocationID: NodeInvocationID{RunID: invocation.RunID, NodeID: edge.From}})
	}
	return FinalizerPreview{Definition: *definition, Context: expressionContext, Dependencies: dependencies}, nil
}

func containsInvocation(input []NodeInvocationID, candidate NodeInvocationID) bool {
	for _, item := range input {
		if item == candidate {
			return true
		}
	}
	return false
}

func cloneAnyMap(input map[string]any) map[string]any {
	result := make(map[string]any, len(input)+2)
	for key, value := range input {
		result[key] = value
	}
	return result
}

// RequestRunCancellationWithFinalizers plans cleanup and atomically retains a
// typed run-level cancellation error with the terminal intent and ordinary-work
// fence. The store assigns one immutable error reference even under contention.
func (c *ControlFlowCoordinator) RequestRunCancellationWithFinalizers(ctx context.Context, workflow graph.Graph, request RequestRunCancellationRequest) (RequestRunCancellationWithFinalizersResult, error) {
	return c.RequestRunCancellationTree(ctx, workflow, request, nil)
}

// RequestRunCancellationTree plans cleanup for the root and every locally
// owned ParentCloseCancel descendant, then asks the store to validate and apply
// the complete tree atomically. Descendants may explicitly have no finalizers;
// zero-finalizer entries keep reachability validation inside the same atomic
// operation, closing the child-start versus cancellation planning race.
func (c *ControlFlowCoordinator) RequestRunCancellationTree(ctx context.Context, workflow graph.Graph, request RequestRunCancellationRequest, descendants []CancellationDescendantGraph) (RequestRunCancellationWithFinalizersResult, error) {
	if err := c.validate(ctx); err != nil {
		return RequestRunCancellationWithFinalizersResult{}, err
	}
	rootRun, loadErr := c.Store.LoadRun(ctx, request.RunID)
	if loadErr != nil {
		return RequestRunCancellationWithFinalizersResult{}, loadErr
	}
	if err := validateCancellationGraphBinding(rootRun, workflow); err != nil {
		return RequestRunCancellationWithFinalizersResult{}, err
	}
	scopes, planErr := PlanFinalizerScopes(workflow, request.RunID)
	if planErr != nil {
		return RequestRunCancellationWithFinalizersResult{}, planErr
	}
	rootValues := values.ValueSet{}
	if len(scopes) != 0 {
		typed, valueErr := NewRunFailureValue(request.RunID, RunCanceled, request.Reason)
		if valueErr != nil {
			return RequestRunCancellationWithFinalizersResult{}, valueErr
		}
		rootValues = values.ValueSet{"error": typed}
	}
	planned := append([]CancellationDescendantGraph(nil), descendants...)
	sort.Slice(planned, func(i, j int) bool { return planned[i].Run.ID < planned[j].Run.ID })
	descendantPlans := make([]CancellationDescendantPlan, 0, len(planned))
	var previous RunID
	for index, descendant := range planned {
		if descendant.Run.ID == request.RunID || descendant.Run.ID == "" || index > 0 && descendant.Run.ID == previous {
			return RequestRunCancellationWithFinalizersResult{}, fmt.Errorf("%w: cancellation descendants must be unique and exclude the root", ErrInvalidControlFlow)
		}
		if err := descendant.Run.Validate(); err != nil {
			return RequestRunCancellationWithFinalizersResult{}, fmt.Errorf("%w: cancellation descendant run: %w", ErrInvalidControlFlow, err)
		}
		if err := validateCancellationGraphBinding(descendant.Run, descendant.Graph); err != nil {
			return RequestRunCancellationWithFinalizersResult{}, err
		}
		childScopes, planErr := PlanFinalizerScopes(descendant.Graph, descendant.Run.ID)
		if planErr != nil {
			return RequestRunCancellationWithFinalizersResult{}, planErr
		}
		plan := CancellationDescendantPlan{
			RunID: descendant.Run.ID, ExpectedRunGeneration: descendant.Run.Generation,
			IdempotencyKey: cancellationTreeKey(request.IdempotencyKey, descendant.Run.ID), Finalizers: childScopes, ErrorValues: values.ValueSet{},
		}
		if len(childScopes) != 0 {
			typed, valueErr := NewRunFailureValue(descendant.Run.ID, RunCanceled, request.Reason)
			if valueErr != nil {
				return RequestRunCancellationWithFinalizersResult{}, valueErr
			}
			plan.ErrorValues = values.ValueSet{"error": typed}
		}
		descendantPlans = append(descendantPlans, plan)
		previous = descendant.Run.ID
	}
	return c.Control.RequestRunCancellationWithFinalizers(context.WithoutCancel(ctx), RequestRunCancellationWithFinalizersRequest{
		Cancellation: request, Finalizers: scopes, ErrorValues: rootValues, Descendants: descendantPlans,
	})
}

func validateCancellationGraphBinding(run RunSnapshot, workflow graph.Graph) error {
	if workflow.ID != run.Plan.ID || workflow.Version != run.Plan.Version {
		return fmt.Errorf("%w: cancellation graph %q@%q does not match run %q plan %q@%q", ErrInvalidControlFlow, workflow.ID, workflow.Version, run.ID, run.Plan.ID, run.Plan.Version)
	}
	return nil
}

func cancellationTreeKey(root string, runID RunID) string {
	return "cancel-tree:" + values.SHA256Digest([]byte(root + "\x00" + string(runID)))[7:]
}

// ReconcileRunCompletion records an intended outcome before cleanup and closes
// it after every finalizer is terminal. Cleanup failure always wins and yields
// RunFailed while retaining the original intent as typed context.
func (c *ControlFlowCoordinator) ReconcileRunCompletion(ctx context.Context, workflow graph.Graph, runID RunID, idempotencyKey string, at time.Time) (RunSnapshot, *TerminalIntentSnapshot, error) {
	if err := c.validate(ctx); err != nil {
		return RunSnapshot{}, nil, err
	}
	run, loadErr := c.Store.LoadRun(ctx, runID)
	if loadErr != nil {
		return RunSnapshot{}, nil, loadErr
	}
	scopes, planErr := PlanFinalizerScopes(workflow, runID)
	if planErr != nil {
		return RunSnapshot{}, nil, planErr
	}
	intent, intentErr := c.Control.LoadTerminalIntent(ctx, runID)
	if errors.Is(intentErr, ErrNotFound) {
		status, reason, origin, accountErr := c.accountOrdinaryNodes(ctx, workflow, runID)
		if accountErr != nil {
			return RunSnapshot{}, nil, accountErr
		}
		if len(scopes) == 0 {
			transition, transitionErr := c.Store.TransitionRun(context.WithoutCancel(ctx), RunTransitionRequest{RunID: runID, ExpectedGeneration: run.Generation, To: status, At: at})
			return transition.Snapshot, nil, transitionErr
		}
		var errorValues values.ValueSet
		if reason != nil && origin != nil {
			timeout, timeoutErr := durableFailureTimeout(origin.node.Status, *reason)
			if timeoutErr != nil {
				return RunSnapshot{}, nil, timeoutErr
			}
			typed, valueErr := NewFailureValue(origin.node.ID, origin.attempt, origin.node.Status, timeout, *reason)
			if valueErr != nil {
				return RunSnapshot{}, nil, valueErr
			}
			errorValues = values.ValueSet{"error": typed}
		}
		begin, beginErr := c.Control.BeginTerminalIntent(context.WithoutCancel(ctx), BeginTerminalIntentRequest{
			RunID: runID, ExpectedRunGeneration: run.Generation, IntendedStatus: status, Reason: reason,
			ErrorValues: errorValues, IdempotencyKey: idempotencyKey, Finalizers: scopes, At: at,
		})
		if beginErr != nil {
			return RunSnapshot{}, nil, beginErr
		}
		intent, run = begin.Intent, begin.Run
	} else if intentErr != nil {
		return RunSnapshot{}, nil, intentErr
	}
	if intent.Status == TerminalIntentCompleted {
		// The run and intent are independent reads. A concurrent recovery worker
		// can complete both between them, leaving the first run snapshot stale.
		// Reload the run after observing a completed intent before judging the
		// durable pair incoherent.
		currentRun, loadErr := c.Store.LoadRun(ctx, runID)
		if loadErr != nil {
			return RunSnapshot{}, nil, loadErr
		}
		if validationErr := validateCompletedIntentRun(currentRun, intent); validationErr != nil {
			return RunSnapshot{}, nil, validationErr
		}
		return currentRun, &intent, nil
	}
	for _, finalizer := range intent.Finalizers {
		node, loadErr := c.Store.LoadNodeInvocation(ctx, finalizer.Invocation)
		if loadErr != nil {
			return RunSnapshot{}, nil, loadErr
		}
		if !node.Status.Terminal() {
			return run, &intent, ErrControlFlowPending
		}
	}
	completed, err := c.Control.CompleteTerminalIntent(context.WithoutCancel(ctx), CompleteTerminalIntentRequest{
		RunID: runID, ExpectedRunGeneration: run.Generation, ExpectedIntentGeneration: intent.Generation, At: at,
	})
	if err != nil {
		// Another recovery worker may have completed the same immutable intent
		// after our reads. Converge on that durable fact; every other failure is
		// returned unchanged.
		currentIntent, intentLoadErr := c.Control.LoadTerminalIntent(ctx, runID)
		currentRun, runLoadErr := c.Store.LoadRun(ctx, runID)
		if intentLoadErr == nil && runLoadErr == nil && currentIntent.Status == TerminalIntentCompleted {
			if validationErr := validateCompletedIntentRun(currentRun, currentIntent); validationErr == nil {
				return currentRun, &currentIntent, nil
			}
		}
		return RunSnapshot{}, nil, err
	}
	return completed.Run, &completed.Intent, nil
}

func validateCompletedIntentRun(run RunSnapshot, intent TerminalIntentSnapshot) error {
	if intent.RunID != run.ID || !run.Status.Terminal() || run.Status != intent.IntendedStatus && run.Status != RunFailed {
		return fmt.Errorf("%w: completed terminal intent and run status are incoherent", ErrControlFlowConflict)
	}
	return nil
}

type failureOrigin struct {
	node    NodeInvocationSnapshot
	attempt *AttemptID
}

func (c *ControlFlowCoordinator) accountOrdinaryNodes(ctx context.Context, workflow graph.Graph, runID RunID) (RunStatus, *Failure, *failureOrigin, error) {
	status := RunSucceeded
	var reason *Failure
	var origin *failureOrigin
	priority := map[NodeStatus]int{NodeCanceled: 1, NodeFailed: 2, NodeTimedOut: 3, NodeCrashed: 4}
	selectedPriority := 0
	for _, definition := range workflow.Nodes {
		if definition.Finally != nil {
			continue
		}
		node, err := c.Store.LoadNodeInvocation(ctx, NodeInvocationID{RunID: runID, NodeID: definition.ID})
		if err != nil {
			return "", nil, nil, err
		}
		if !node.Status.Terminal() {
			return "", nil, nil, ErrControlFlowPending
		}
		if definition.Switch != nil && node.Status == NodeSucceeded {
			if _, decisionErr := c.Control.LoadControlDecision(ctx, ControlDecisionID{Source: node.ID, Kind: ControlSwitch}); errors.Is(decisionErr, ErrNotFound) {
				return "", nil, nil, ErrControlFlowPending
			} else if decisionErr != nil {
				return "", nil, nil, decisionErr
			}
		}
		if !hardFailure(node.Status) {
			continue
		}
		handled := false
		if len(definition.Catch) != 0 {
			decision, decisionErr := c.Control.LoadControlDecision(ctx, ControlDecisionID{Source: node.ID, Kind: ControlCatch})
			if decisionErr == nil {
				handled = decision.Outcome == ControlSelected || decision.Outcome == ControlContinued
			}
			if errors.Is(decisionErr, ErrNotFound) {
				return "", nil, nil, ErrControlFlowPending
			}
			if decisionErr != nil {
				return "", nil, nil, decisionErr
			}
		}
		if handled {
			continue
		}
		if priority[node.Status] > selectedPriority {
			selectedPriority = priority[node.Status]
			switch node.Status {
			case NodeCanceled:
				status = RunCanceled
			case NodeFailed:
				status = RunFailed
			case NodeTimedOut:
				status = RunTimedOut
			case NodeCrashed:
				status = RunCrashed
			default:
				return "", nil, nil, fmt.Errorf("%w: hard failure status %q has no run outcome", ErrInvalidControlFlow, node.Status)
			}
			failure := Failure{Code: "node_" + string(node.Status), Message: "node " + node.ID.NodeID + " ended " + string(node.Status), Details: map[string]string{"node_id": node.ID.NodeID}}
			selectedOrigin := failureOrigin{node: node}
			if node.LatestAttempt > 0 {
				attemptID := AttemptID{Invocation: node.ID, Number: node.LatestAttempt}
				attempt, loadErr := c.Store.LoadAttempt(ctx, attemptID)
				if loadErr != nil {
					return "", nil, nil, loadErr
				}
				if attempt.Failure != nil {
					failure = cloneFailureValue(*attempt.Failure)
				}
				selectedOrigin.attempt = &attemptID
			}
			reason = &failure
			origin = &selectedOrigin
		}
	}
	return status, reason, origin, nil
}
