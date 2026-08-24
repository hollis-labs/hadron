package appworkflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"time"

	calladapter "github.com/hollis-labs/hadron/workflow/adapters/call"
	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

var ErrInvalidChildRunMaterializer = errors.New("invalid child-run materializer")

// ChildRunMaterializerOptions supplies the state and clock used to materialize
// an already atomically created, pinned call.mode:run child.
type ChildRunMaterializerOptions struct {
	State runtime.StateStore
	Clock Clock
}

// PinnedChildRunMaterializer turns only the graph carried by a durable
// ChildRunRequest into runtime node state. It never resolves the requested
// definition again, so movable aliases cannot change a recovered child.
type PinnedChildRunMaterializer struct {
	state runtime.StateStore
	clock Clock
}

// NewPinnedChildRunMaterializer constructs the W05-T03 child recovery seam.
func NewPinnedChildRunMaterializer(options ChildRunMaterializerOptions) (*PinnedChildRunMaterializer, error) {
	if nilInterface(options.State) {
		return nil, fmt.Errorf("%w: state store is required", ErrInvalidChildRunMaterializer)
	}
	clock := options.Clock
	if nilInterface(clock) {
		clock = ClockFunc(func() time.Time { return time.Now().UTC() })
	}
	return &PinnedChildRunMaterializer{state: options.State, clock: clock}, nil
}

// MaterializeChildRun idempotently creates child node invocations, advances a
// pending child to running, and evaluates only graph roots. The child Run,
// typed inputs, link, and creation event were already committed atomically by
// the host call store; this operation never recreates them.
func (m *PinnedChildRunMaterializer) MaterializeChildRun(ctx context.Context, input calladapter.ChildRunRequest) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalidChildRunMaterializer)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if m == nil || nilInterface(m.state) || nilInterface(m.clock) {
		return fmt.Errorf("%w: materializer is not initialized", ErrInvalidChildRunMaterializer)
	}
	request, cloneErr := cloneChildRunRequest(input)
	if cloneErr != nil {
		return errors.Join(ErrInvalidChildRunMaterializer, fmt.Errorf("request is not JSON-compatible: %w", cloneErr))
	}
	if validationErr := validatePinnedChildRequest(request); validationErr != nil {
		return errors.Join(ErrInvalidChildRunMaterializer, validationErr)
	}

	run, loadErr := m.loadPinnedChild(ctx, request)
	if loadErr != nil {
		return loadErr
	}
	if run.Status.Terminal() {
		return nil
	}
	fenced, materializeErr := m.materializeChildNodes(ctx, request, run)
	if materializeErr != nil {
		return materializeErr
	}
	if fenced {
		return nil
	}
	run, loadErr = m.advanceChildRun(ctx, request, run)
	if loadErr != nil {
		return loadErr
	}
	if !run.Status.Active() {
		return nil
	}
	return m.readyChildRoots(ctx, request)
}

func (m *PinnedChildRunMaterializer) loadPinnedChild(ctx context.Context, request calladapter.ChildRunRequest) (runtime.RunSnapshot, error) {
	run, err := m.state.LoadRun(ctx, request.ChildRunID)
	if err != nil {
		return runtime.RunSnapshot{}, err
	}
	if run.Plan != request.Plan || run.Inputs == nil {
		return runtime.RunSnapshot{}, fmt.Errorf("%w: durable child run differs from pinned request", runtime.ErrInvalidRecord)
	}
	inputs, err := m.state.LoadValues(ctx, *run.Inputs)
	if err != nil {
		return runtime.RunSnapshot{}, err
	}
	if !equalValueSets(inputs, request.Inputs) {
		return runtime.RunSnapshot{}, fmt.Errorf("%w: durable child inputs differ from pinned request", runtime.ErrInvalidRecord)
	}
	return run, nil
}

func (m *PinnedChildRunMaterializer) materializeChildNodes(ctx context.Context, request calladapter.ChildRunRequest, run runtime.RunSnapshot) (bool, error) {
	nodes := append([]graph.Node(nil), request.Definition.Graph.Nodes...)
	sort.Slice(nodes, func(left, right int) bool { return nodes[left].ID < nodes[right].ID })
	for _, node := range nodes {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if currentRun, err := m.state.LoadRun(ctx, request.ChildRunID); err != nil {
			return false, err
		} else if !currentRun.Status.Active() {
			return true, nil
		}
		invocationID := runtime.NodeInvocationID{RunID: request.ChildRunID, NodeID: node.ID}
		var bound values.ValueSet
		if !hasDependencies(request.Definition.Graph, node.ID) && len(node.InputBindings) != 0 {
			boundInputs, err := bindNodeInputs(node, request.Inputs, request.ChildRunID)
			if err != nil {
				return false, fmt.Errorf("bind child root node %s: %w", node.ID, err)
			}
			bound = boundInputs
		}
		current, loadErr := m.state.LoadNodeInvocation(ctx, invocationID)
		if loadErr == nil {
			if err := m.validateMaterializedNode(ctx, current, run.CreatedAt, bound); err != nil {
				return false, err
			}
			continue
		}
		if !errors.Is(loadErr, runtime.ErrNotFound) {
			return false, loadErr
		}
		var inputRef *values.ValueSetRef
		if bound != nil {
			// StateStore has no atomic value-set-plus-node operation. A crash or
			// lost response after SaveValues may leave an immutable, run-owned
			// value set that no node references, but it cannot poison recovery:
			// retry saves the same validated values and CreateNodeInvocation binds
			// one digest-checked reference. Any orphan remains run-owned and
			// eligible for the store's run-retention cleanup.
			ref, err := m.state.SaveValues(ctx, runtime.SaveValuesRequest{
				Owner:  runtime.ValueOwner{Kind: "node-inputs", RunID: request.ChildRunID, Invocation: &invocationID},
				Values: bound,
			})
			if err != nil {
				return false, err
			}
			inputRef = &ref
		}
		snapshot := runtime.NodeInvocationSnapshot{
			ID: invocationID, Status: runtime.NodePending, Inputs: inputRef,
			CreatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt,
		}
		created, createErr := m.state.CreateNodeInvocation(ctx, runtime.CreateNodeInvocationRequest{Snapshot: snapshot})
		if createErr == nil {
			if err := m.validateMaterializedNode(ctx, created, run.CreatedAt, bound); err != nil {
				return false, err
			}
			continue
		}
		if errors.Is(createErr, runtime.ErrInvalidRecord) {
			fenced, fenceErr := m.nodeCreationFenced(ctx, request.ChildRunID)
			if fenceErr != nil {
				return false, fenceErr
			}
			if fenced {
				return true, nil
			}
		}
		if !errors.Is(createErr, runtime.ErrAlreadyExists) {
			return false, createErr
		}
		current, loadErr = m.state.LoadNodeInvocation(ctx, invocationID)
		if loadErr != nil {
			return false, loadErr
		}
		if err := m.validateMaterializedNode(ctx, current, run.CreatedAt, bound); err != nil {
			return false, err
		}
	}
	return false, nil
}

func (m *PinnedChildRunMaterializer) nodeCreationFenced(ctx context.Context, runID runtime.RunID) (bool, error) {
	run, err := m.state.LoadRun(ctx, runID)
	if err != nil {
		return false, err
	}
	if !run.Status.Active() {
		return true, nil
	}
	control, ok := m.state.(runtime.ControlFlowStore)
	if !ok || nilInterface(control) {
		return false, nil
	}
	intent, err := control.LoadTerminalIntent(ctx, runID)
	if errors.Is(err, runtime.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// A completed intent must already have made the run terminal; observing it
	// beside an active run is durable corruption and must not be hidden as a
	// benign materialization fence.
	return intent.Status == runtime.TerminalIntentPending, nil
}

func (m *PinnedChildRunMaterializer) validateMaterializedNode(ctx context.Context, node runtime.NodeInvocationSnapshot, createdAt time.Time, expectedInputs values.ValueSet) error {
	if !node.CreatedAt.Equal(createdAt) {
		return fmt.Errorf("%w: durable child node creation time differs from child run", runtime.ErrInvalidRecord)
	}
	if expectedInputs == nil {
		if node.Inputs != nil {
			return fmt.Errorf("%w: dependency child node unexpectedly has bound inputs", runtime.ErrInvalidRecord)
		}
		return nil
	}
	if node.Inputs == nil {
		return fmt.Errorf("%w: root child node is missing bound inputs", runtime.ErrInvalidRecord)
	}
	stored, err := m.state.LoadValues(ctx, *node.Inputs)
	if err != nil {
		return err
	}
	if !equalValueSets(stored, expectedInputs) {
		return fmt.Errorf("%w: durable child node inputs differ from pinned request", runtime.ErrInvalidRecord)
	}
	return nil
}

func (m *PinnedChildRunMaterializer) advanceChildRun(ctx context.Context, request calladapter.ChildRunRequest, run runtime.RunSnapshot) (runtime.RunSnapshot, error) {
	for attempts := 0; attempts < 8; attempts++ {
		if run.Status != runtime.RunPending {
			return run, nil
		}
		at := maxTime(m.clock.Now(), run.UpdatedAt)
		transition, err := m.state.TransitionRun(ctx, runtime.RunTransitionRequest{
			RunID: request.ChildRunID, ExpectedGeneration: run.Generation,
			To: runtime.RunRunning, At: at,
		})
		if err == nil {
			return transition.Snapshot, nil
		}
		if !errors.Is(err, runtime.ErrCASMismatch) {
			return runtime.RunSnapshot{}, err
		}
		run, err = m.loadPinnedChild(ctx, request)
		if err != nil {
			return runtime.RunSnapshot{}, err
		}
	}
	return runtime.RunSnapshot{}, fmt.Errorf("%w: child run transition did not converge", runtime.ErrCASMismatch)
}

func (m *PinnedChildRunMaterializer) readyChildRoots(ctx context.Context, request calladapter.ChildRunRequest) error {
	coordinator := runtime.NewProgressionCoordinator(m.state, nil)
	nodes := append([]graph.Node(nil), request.Definition.Graph.Nodes...)
	sort.Slice(nodes, func(left, right int) bool { return nodes[left].ID < nodes[right].ID })
	for _, node := range nodes {
		if hasDependencies(request.Definition.Graph, node.ID) {
			continue
		}
		currentRun, err := m.state.LoadRun(ctx, request.ChildRunID)
		if err != nil {
			return err
		}
		if !currentRun.Status.Active() {
			return nil
		}
		id := runtime.NodeInvocationID{RunID: request.ChildRunID, NodeID: node.ID}
		current, err := m.state.LoadNodeInvocation(ctx, id)
		if err != nil {
			return err
		}
		if current.Status != runtime.NodePending && current.Status != runtime.NodeBlocked {
			continue
		}
		_, err = coordinator.ProgressNode(ctx, runtime.ProgressNodeRequest{
			InvocationID: id, Rule: node.ReadyWhen, Predicate: node.If,
			ExpressionContext: values.ExpressionContext{Inputs: request.Inputs},
			At:                maxTime(m.clock.Now(), current.UpdatedAt),
		})
		if err == nil {
			continue
		}
		if errors.Is(err, runtime.ErrCASMismatch) {
			replayed, loadErr := m.state.LoadNodeInvocation(ctx, id)
			if loadErr == nil && replayed.Status != runtime.NodePending && replayed.Status != runtime.NodeBlocked {
				continue
			}
		}
		return err
	}
	return nil
}

func validatePinnedChildRequest(request calladapter.ChildRunRequest) error {
	if err := request.Parent.Validate(); err != nil {
		return err
	}
	if request.ChildRunID == "" || request.IdempotencyKey == "" || runtime.RunID(request.Parent.RunID) == request.ChildRunID {
		return errors.New("child request requires distinct parent/child and idempotency identities")
	}
	if err := request.Plan.Validate(); err != nil {
		return err
	}
	if request.Plan.SchemaVersion != compile.ExecutionPlanSchemaVersion {
		return errors.New("child plan schema version is unsupported")
	}
	if err := values.ValidatePersistableSet(request.Inputs); err != nil {
		return err
	}
	if !request.ParentClose.Valid() {
		return errors.New("child parent-close policy is invalid")
	}
	if err := request.Definition.Graph.ValidateEnums(); err != nil {
		return err
	}
	definition := request.Definition.Definition
	resolvedGraph := request.Definition.Graph
	if definition.ID != resolvedGraph.ID || definition.Version != resolvedGraph.Version ||
		definition.Digest != resolvedGraph.Digest || definition.Provenance == nil ||
		!reflect.DeepEqual(*definition.Provenance, resolvedGraph.Provenance) {
		return errors.New("child definition and graph identities differ")
	}
	if request.Plan.ID != resolvedGraph.ID || request.Plan.Version != resolvedGraph.Version || request.Plan.Digest != resolvedGraph.Digest {
		return errors.New("child plan and graph identities differ")
	}
	inputDigest, err := values.DigestValueSet(request.Inputs)
	if err != nil {
		return err
	}
	resolution := calladapter.ResolutionRecord{
		Key: "child-materializer-validation", Invocation: request.Parent,
		Requested: definition, Resolved: definition, InputDigest: inputDigest, Lineage: request.Lineage,
	}
	if err := resolution.Validate(); err != nil {
		return err
	}
	if !reflect.DeepEqual(request.Lineage[len(request.Lineage)-1], definition) {
		return errors.New("child lineage final definition differs from pinned graph identity")
	}
	return nil
}

func cloneChildRunRequest(input calladapter.ChildRunRequest) (calladapter.ChildRunRequest, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return calladapter.ChildRunRequest{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var output calladapter.ChildRunRequest
	if err := decoder.Decode(&output); err != nil {
		return calladapter.ChildRunRequest{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return calladapter.ChildRunRequest{}, errors.New("child request contains trailing JSON")
	}
	return output, nil
}

func equalValueSets(left, right values.ValueSet) bool {
	leftDigest, leftErr := values.DigestValueSet(left)
	rightDigest, rightErr := values.DigestValueSet(right)
	return leftErr == nil && rightErr == nil && leftDigest == rightDigest
}

var _ ChildRunMaterializer = (*PinnedChildRunMaterializer)(nil)
