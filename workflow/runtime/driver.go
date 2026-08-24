package runtime

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const EventNodeInputsBound = "node.inputs_bound"

// BindNodeInputsRequest atomically persists one evaluated typed input set and
// attaches its immutable reference to a still-unclaimable invocation. At and
// ExpectedGeneration fence the first application; exact replay is identified
// by IdempotencyKey and the complete typed value-set digest.
type BindNodeInputsRequest struct {
	InvocationID       NodeInvocationID `json:"invocation_id"`
	ExpectedGeneration uint64           `json:"expected_generation"`
	IdempotencyKey     string           `json:"idempotency_key"`
	Values             values.ValueSet  `json:"values"`
	MemoKeyDigest      string           `json:"memo_key_digest,omitempty"`
	At                 time.Time        `json:"at"`
}

func (r BindNodeInputsRequest) Validate() error {
	if err := r.InvocationID.Validate(); err != nil {
		return err
	}
	if r.ExpectedGeneration == 0 || r.At.IsZero() {
		return fmt.Errorf("input binding requires expected generation and timestamp")
	}
	if err := validateRequiredText("input binding idempotency key", r.IdempotencyKey); err != nil {
		return err
	}
	if r.MemoKeyDigest != "" {
		if err := values.ValidateDigest(r.MemoKeyDigest); err != nil {
			return fmt.Errorf("memo key digest: %w", err)
		}
	}
	return values.ValidatePersistableSet(r.Values)
}

type BindNodeInputsResult struct {
	Outcome IdempotencyOutcome     `json:"outcome"`
	Node    NodeInvocationSnapshot `json:"node"`
	Inputs  values.ValueSetRef     `json:"inputs"`
	Event   Event                  `json:"event"`
}

// NodeInputStore is the atomic persistence seam shared by ordinary execution
// drivers and crash recovery. It deliberately does not widen StateStore.
type NodeInputStore interface {
	BindNodeInputs(context.Context, BindNodeInputsRequest) (BindNodeInputsResult, error)
}

// NodeDriver binds one node's exact plan inputs before applying existing
// readiness/control-flow semantics. A node cannot become claimable until the
// binding transaction has committed.
type NodeDriver struct {
	Store     StateStore
	Inputs    NodeInputStore
	Control   ControlFlowStore
	Registry  stepkind.Registry
	Evaluator PredicateEvaluator
}

type DriveNodeRequest struct {
	Run               RunSnapshot
	Plan              RecoveryPlan
	InvocationID      NodeInvocationID
	Node              graph.Node
	ExpressionContext values.ExpressionContext
	ExpressionOptions values.ExpressionOptions
	At                time.Time
}

type DriveNodeResult struct {
	Binding    *BindNodeInputsResult `json:"binding,omitempty"`
	Progressed ProgressNodeResult    `json:"progressed"`
}

func (d *NodeDriver) Drive(ctx context.Context, request DriveNodeRequest) (DriveNodeResult, error) {
	if ctx == nil || d == nil || nilStateStore(d.Store) || nilNodeInputStore(d.Inputs) || nilStepKindRegistry(d.Registry) {
		return DriveNodeResult{}, fmt.Errorf("%w: context, stores, and registry are required", ErrInvalidRecovery)
	}
	if err := request.Run.Validate(); err != nil {
		return DriveNodeResult{}, fmt.Errorf("%w: invalid run: %w", ErrInvalidRecovery, err)
	}
	if request.Run.ID == "" || request.At.IsZero() {
		return DriveNodeResult{}, fmt.Errorf("%w: run identity and progression timestamp are required", ErrInvalidRecovery)
	}
	if request.Node.ID == "" || request.Node.Kind == "" || request.Node.KindVersion == "" {
		return DriveNodeResult{}, fmt.Errorf("%w: exact node identity and kind version are required", ErrInvalidRecovery)
	}
	id := request.InvocationID
	if err := id.Validate(); err != nil {
		return DriveNodeResult{}, fmt.Errorf("%w: invalid invocation: %w", ErrInvalidRecovery, err)
	}
	if id.RunID != request.Run.ID || id.NodeID != request.Node.ID {
		return DriveNodeResult{}, fmt.Errorf("%w: invocation does not match run and node", ErrInvalidRecovery)
	}
	if err := request.Plan.Validate(); err != nil {
		return DriveNodeResult{}, fmt.Errorf("%w: invalid recovery plan: %w", ErrInvalidRecovery, err)
	}
	if request.Plan.Ref != request.Run.Plan {
		return DriveNodeResult{}, fmt.Errorf("%w: exact recovery plan does not match run", ErrInvalidRecovery)
	}
	scoped, scopedOptions, err := request.Plan.Visibility.ScopeNodeContext(request.Node.ID, request.ExpressionContext, request.ExpressionOptions)
	if err != nil {
		return DriveNodeResult{}, fmt.Errorf("%w: compiler visibility: %w", ErrInvalidRecovery, err)
	}
	node, err := d.Store.LoadNodeInvocation(ctx, id)
	if err != nil {
		return DriveNodeResult{}, err
	}
	result := DriveNodeResult{}
	dependencies := graphDependencies(request.Plan.Plan.Graph, request.Run.ID, request.Node.ID)
	progressContext := scoped
	progressDependencies := dependencies
	routeOwned, routeSelected := false, true
	if graphRouteTarget(request.Plan.Plan.Graph, request.Node.ID) {
		preview, previewErr := NewControlFlowCoordinator(d.Store, d.Control, d.Evaluator).PreviewControlNode(ctx, ProgressControlNodeRequest{
			Graph: request.Plan.Plan.Graph, InvocationID: id, Dependencies: dependencies,
			ExpressionContext: scoped, ExpressionOptions: scopedOptions, At: request.At,
		})
		if previewErr != nil {
			return result, previewErr
		}
		progressContext, progressDependencies = preview.Context, preview.Dependencies
		routeOwned, routeSelected = preview.RouteOwned, preview.Selected
	}
	progression := NewProgressionCoordinator(d.Store, d.Evaluator)
	if routeOwned && !routeSelected {
		progressed, progressErr := NewControlFlowCoordinator(d.Store, d.Control, d.Evaluator).ProgressControlNode(ctx, ProgressControlNodeRequest{Graph: request.Plan.Plan.Graph, InvocationID: id, Dependencies: dependencies, ExpressionContext: scoped, ExpressionOptions: scopedOptions, At: request.At})
		result.Progressed = progressed
		return result, progressErr
	}
	states, stateErr := progression.loadDependencyStates(ctx, id, progressDependencies)
	if stateErr != nil {
		return result, stateErr
	}
	readiness, readinessErr := EvaluateReadiness(request.Node.ReadyWhen, states)
	if readinessErr != nil {
		return result, readinessErr
	}
	if readiness.Disposition != ReadinessReady {
		if graphRouteTarget(request.Plan.Plan.Graph, request.Node.ID) {
			progressed, progressErr := NewControlFlowCoordinator(d.Store, d.Control, d.Evaluator).ProgressControlNode(ctx, ProgressControlNodeRequest{Graph: request.Plan.Plan.Graph, InvocationID: id, Dependencies: dependencies, ExpressionContext: scoped, ExpressionOptions: scopedOptions, At: request.At})
			result.Progressed = progressed
			return result, progressErr
		}
		progressed, progressErr := progression.ProgressNode(ctx, ProgressNodeRequest{InvocationID: id, Dependencies: dependencies, Rule: request.Node.ReadyWhen, Predicate: request.Node.If, ExpressionContext: scoped, ExpressionOptions: scopedOptions, At: request.At})
		result.Progressed = progressed
		return result, progressErr
	}
	if node.Inputs == nil {
		if node.Status != NodePending && node.Status != NodeBlocked {
			return result, fmt.Errorf("%w: unbound node %v is already claimable or terminal", ErrRecoveryConflict, id)
		}
		_, spec, resolveErr := stepkind.Resolve(d.Registry, request.Node.Kind, request.Node.KindVersion)
		if resolveErr != nil {
			return result, resolveErr
		}
		bound, bindErr := bindNodeInputValues(request.Node, progressContext, scopedOptions, id)
		if bindErr != nil {
			return result, bindErr
		}
		if schemaErr := values.ValidateValueSetSchema(spec.InputSchema, bound); schemaErr != nil {
			return result, schemaErr
		}
		memoKeyDigest, memoErr := evaluateMemoKey(request.Node.Memoization, progressContext, scopedOptions)
		if memoErr != nil {
			return result, memoErr
		}
		binding, persistErr := d.Inputs.BindNodeInputs(context.WithoutCancel(ctx), BindNodeInputsRequest{
			InvocationID: id, ExpectedGeneration: node.Generation,
			IdempotencyKey: nodeInputBindingKey(id), Values: bound,
			MemoKeyDigest: memoKeyDigest, At: maxRecoveryTime(request.At, node.UpdatedAt),
		})
		if persistErr != nil {
			return result, persistErr
		}
		result.Binding = &binding
		node = binding.Node
	} else {
		if request.Node.Memoization != nil && node.MemoKeyDigest == "" {
			return result, fmt.Errorf("%w: memoized node is missing its durable key digest", ErrRecoveryConflict)
		}
		inputs, loadErr := d.Store.LoadValues(ctx, *node.Inputs)
		if loadErr != nil {
			return result, loadErr
		}
		_, spec, resolveErr := stepkind.Resolve(d.Registry, request.Node.Kind, request.Node.KindVersion)
		if resolveErr != nil {
			return result, resolveErr
		}
		if schemaErr := values.ValidateValueSetSchema(spec.InputSchema, inputs); schemaErr != nil {
			return result, schemaErr
		}
	}

	if graphRouteTarget(request.Plan.Plan.Graph, request.Node.ID) {
		coordinator := NewControlFlowCoordinator(d.Store, d.Control, d.Evaluator)
		progressed, progressErr := coordinator.ProgressControlNode(ctx, ProgressControlNodeRequest{
			Graph: request.Plan.Plan.Graph, InvocationID: id, Dependencies: dependencies,
			ExpressionContext: scoped, ExpressionOptions: scopedOptions,
			At: maxRecoveryTime(request.At, node.UpdatedAt),
		})
		result.Progressed = progressed
		return result, progressErr
	}
	progressed, progressErr := progression.ProgressNode(ctx, ProgressNodeRequest{
		InvocationID: id, Dependencies: dependencies, Rule: request.Node.ReadyWhen,
		Predicate: request.Node.If, ExpressionContext: scoped,
		ExpressionOptions: scopedOptions, At: maxRecoveryTime(request.At, node.UpdatedAt),
	})
	result.Progressed = progressed
	return result, progressErr
}

// DriveFinally binds one eligible finalizer with the same compiler visibility
// and atomic input seam used for ordinary work. Scope/cancellation ordering is
// previewed before expressions run, so an ineligible cleanup never publishes
// input values or becomes claimable.
func (d *NodeDriver) DriveFinally(ctx context.Context, request DriveNodeRequest) (DriveNodeResult, error) {
	if ctx == nil || d == nil || nilStateStore(d.Store) || nilNodeInputStore(d.Inputs) || nilControlFlowStore(d.Control) || nilStepKindRegistry(d.Registry) {
		return DriveNodeResult{}, fmt.Errorf("%w: context, stores, and registry are required", ErrInvalidRecovery)
	}
	if err := request.Run.Validate(); err != nil {
		return DriveNodeResult{}, fmt.Errorf("%w: invalid finalizer run: %w", ErrInvalidRecovery, err)
	}
	if request.At.IsZero() || request.InvocationID.RunID != request.Run.ID || request.InvocationID.NodeID != request.Node.ID || request.Node.Finally == nil {
		return DriveNodeResult{}, fmt.Errorf("%w: invalid finalizer drive request", ErrInvalidRecovery)
	}
	if err := request.Plan.Validate(); err != nil {
		return DriveNodeResult{}, fmt.Errorf("%w: invalid recovery plan: %w", ErrInvalidRecovery, err)
	}
	if request.Plan.Ref != request.Run.Plan {
		return DriveNodeResult{}, fmt.Errorf("%w: exact recovery plan does not match run", ErrInvalidRecovery)
	}
	coordinator := NewControlFlowCoordinator(d.Store, d.Control, d.Evaluator)
	preview, err := coordinator.PreviewFinally(ctx, request.Plan.Plan.Graph, request.InvocationID, request.ExpressionContext)
	if err != nil {
		return DriveNodeResult{}, err
	}
	scoped, options, err := request.Plan.Visibility.ScopeNodeContext(request.Node.ID, preview.Context, request.ExpressionOptions)
	if err != nil {
		return DriveNodeResult{}, fmt.Errorf("%w: compiler visibility: %w", ErrInvalidRecovery, err)
	}
	progression := NewProgressionCoordinator(d.Store, d.Evaluator)
	states, err := progression.loadDependencyStates(ctx, request.InvocationID, preview.Dependencies)
	if err != nil {
		return DriveNodeResult{}, err
	}
	readiness, err := EvaluateReadiness(graph.ReadyAllDone, states)
	if err != nil {
		return DriveNodeResult{}, err
	}
	if readiness.Disposition != ReadinessReady {
		return DriveNodeResult{}, ErrControlFlowPending
	}
	node, err := d.Store.LoadNodeInvocation(ctx, request.InvocationID)
	if err != nil {
		return DriveNodeResult{}, err
	}
	result := DriveNodeResult{}
	if node.Inputs == nil {
		if node.Status != NodePending && node.Status != NodeBlocked {
			return result, fmt.Errorf("%w: unbound finalizer %v is already claimable or terminal", ErrRecoveryConflict, request.InvocationID)
		}
		_, spec, resolveErr := stepkind.Resolve(d.Registry, request.Node.Kind, request.Node.KindVersion)
		if resolveErr != nil {
			return result, resolveErr
		}
		bound, bindErr := bindNodeInputValues(request.Node, scoped, options, request.InvocationID)
		if bindErr != nil {
			return result, bindErr
		}
		if schemaErr := values.ValidateValueSetSchema(spec.InputSchema, bound); schemaErr != nil {
			return result, schemaErr
		}
		memoKeyDigest, memoErr := evaluateMemoKey(request.Node.Memoization, scoped, options)
		if memoErr != nil {
			return result, memoErr
		}
		binding, persistErr := d.Inputs.BindNodeInputs(context.WithoutCancel(ctx), BindNodeInputsRequest{InvocationID: request.InvocationID, ExpectedGeneration: node.Generation, IdempotencyKey: nodeInputBindingKey(request.InvocationID), Values: bound, MemoKeyDigest: memoKeyDigest, At: maxRecoveryTime(request.At, node.UpdatedAt)})
		if persistErr != nil {
			return result, persistErr
		}
		result.Binding = &binding
		node = binding.Node
	} else {
		if request.Node.Memoization != nil && node.MemoKeyDigest == "" {
			return result, fmt.Errorf("%w: memoized finalizer is missing its durable key digest", ErrRecoveryConflict)
		}
		inputs, loadErr := d.Store.LoadValues(ctx, *node.Inputs)
		if loadErr != nil {
			return result, loadErr
		}
		_, spec, resolveErr := stepkind.Resolve(d.Registry, request.Node.Kind, request.Node.KindVersion)
		if resolveErr != nil {
			return result, resolveErr
		}
		if schemaErr := values.ValidateValueSetSchema(spec.InputSchema, inputs); schemaErr != nil {
			return result, schemaErr
		}
	}
	progressed, err := coordinator.ProgressFinally(ctx, request.Plan.Plan.Graph, request.InvocationID, scoped, options, maxRecoveryTime(request.At, node.UpdatedAt))
	result.Progressed = progressed
	return result, err
}

func bindNodeInputValues(node graph.Node, expression values.ExpressionContext, options values.ExpressionOptions, id NodeInvocationID) (values.ValueSet, error) {
	engine := values.NewExpressionEngine()
	names := make([]string, 0, len(node.InputBindings))
	for name := range node.InputBindings {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make(values.ValueSet, len(names))
	reference := controlIdentity(id)
	for _, name := range names {
		value, evaluateErr := engine.EvaluateBinding(node.InputBindings[name], expression, options, bindingMetadata("node_input", reference, name))
		if evaluateErr != nil {
			return nil, evaluateErr
		}
		result[name] = value
	}
	return result, values.ValidatePersistableSet(result)
}

func nodeInputBindingKey(id NodeInvocationID) string {
	return "node-inputs:" + controlIdentity(id)
}

func evaluateMemoKey(spec *graph.MemoizationSpec, context values.ExpressionContext, options values.ExpressionOptions) (string, error) {
	if spec == nil {
		return "", nil
	}
	result, err := values.NewExpressionEngine().EvaluateRaw(spec.Key, context, options)
	if err != nil {
		return "", fmt.Errorf("%w: evaluate memoization key: %w", ErrInvalidRecovery, err)
	}
	digest, err := values.DigestInline(result)
	if err != nil {
		return "", fmt.Errorf("%w: digest memoization key: %w", ErrInvalidRecovery, err)
	}
	return digest, nil
}

func nilNodeInputStore(store NodeInputStore) bool { return nilReflect(store) }

// SemanticallyEqualNodeInputRequest compares the immutable binding intent;
// generation and timestamp are fencing/application facts and are ignored on
// replay after the first transaction advanced the node.
func SemanticallyEqualNodeInputRequest(left, right BindNodeInputsRequest) bool {
	if left.InvocationID != right.InvocationID || left.IdempotencyKey != right.IdempotencyKey || left.MemoKeyDigest != right.MemoKeyDigest {
		return false
	}
	leftDigest, leftErr := values.DigestValueSet(left.Values)
	rightDigest, rightErr := values.DigestValueSet(right.Values)
	return leftErr == nil && rightErr == nil && leftDigest == rightDigest
}
