package offline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/runtimetest"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
)

const DefaultMaxExecutionSteps = 10000

type ExecuteOptions struct {
	Registry       stepkind.Registry
	RegistryBinder RuntimeRegistryBinder
	Verifiers      verification.Registry
	Inputs         map[string]any
	Now            func() time.Time
	MaxSteps       int
	// PollInterval bounds durable external-operation observation cadence.
	// Zero selects 100ms; tests may inject a shorter positive cadence.
	PollInterval time.Duration
	// RunID and IdempotencyKey let an ordinary host use the embedded engine
	// while preserving its caller-visible identity. Generated/offline callers
	// may omit both and receive deterministic content-derived defaults.
	RunID          runtime.RunID
	IdempotencyKey string
}

// RuntimeRegistryBinder adapts an exact immutable build registry to an
// execution store without teaching workflow core about concrete adapters.
// The returned registry must advertise exactly the manifest StepKinds.
type RuntimeRegistryBinder interface {
	BindRuntimeRegistry(context.Context, stepkind.Registry, ExecutionStore, Manifest) (stepkind.Registry, error)
}

// RuntimeRegistryBinderFunc adapts a runtime-registry binding function.
type RuntimeRegistryBinderFunc func(context.Context, stepkind.Registry, ExecutionStore, Manifest) (stepkind.Registry, error)

// BindRuntimeRegistry implements RuntimeRegistryBinder.
func (f RuntimeRegistryBinderFunc) BindRuntimeRegistry(ctx context.Context, registry stepkind.Registry, store ExecutionStore, manifest Manifest) (stepkind.Registry, error) {
	return f(ctx, registry, store, manifest)
}

type ExecutionResult struct {
	Run     runtime.RunSnapshot `json:"run"`
	Outputs values.ValueSet     `json:"outputs,omitempty"`
}

// ExecutionStore is the existing runtime contract subset needed by the
// embedded host loop. SQLite-backed daemon hosts and the in-memory offline
// implementation can therefore be parity-tested without alternate semantics.
type ExecutionStore interface {
	runtime.StateStore
	runtime.RecoveryStore
	runtime.NodeInputStore
	runtime.ControlFlowStore
	runtime.RunPolicyStore
	runtime.FanOutStore
}

type RunFailureError struct {
	Run     runtime.RunSnapshot
	Failure *runtime.Failure
}

func (e *RunFailureError) Error() string {
	if e != nil && e.Failure != nil {
		return fmt.Sprintf("offline workflow failed: %s: %s", e.Failure.Code, e.Failure.Message)
	}
	return "offline workflow failed"
}

// Execute runs an exact manifest through the ordinary durable runtime over a
// fresh concurrency-safe in-memory store. It never interprets graph edges or
// expressions independently of runtime coordinators.
func Execute(ctx context.Context, manifest Manifest, options ExecuteOptions) (ExecutionResult, error) {
	return ExecuteWithStore(ctx, manifest, options, runtimetest.NewStore())
}

// ExecuteWithStore runs the same embedded host loop against an injected
// implementation of the ordinary runtime storage contracts. It is useful for
// daemon/offline parity checks; generated artifacts use Execute's private
// in-memory instance.
func ExecuteWithStore(ctx context.Context, manifest Manifest, options ExecuteOptions, store ExecutionStore) (ExecutionResult, error) {
	if ctx == nil || nilRegistry(options.Registry) {
		return ExecutionResult{}, fmt.Errorf("%w: context and registry are required", ErrInvalidBuild)
	}
	if nilExecutionStore(store) {
		return ExecutionResult{}, fmt.Errorf("%w: execution store is required", ErrInvalidBuild)
	}
	if err := validateExecutableManifest(manifest, options.Registry); err != nil {
		return ExecutionResult{}, err
	}
	runtimeRegistry := options.Registry
	if options.RegistryBinder != nil {
		if nilRuntimeRegistryBinder(options.RegistryBinder) {
			return ExecutionResult{}, fmt.Errorf("%w: runtime registry binder is typed nil", ErrInvalidBuild)
		}
		var err error
		runtimeRegistry, err = options.RegistryBinder.BindRuntimeRegistry(ctx, options.Registry, store, manifest)
		if err != nil {
			return ExecutionResult{}, err
		}
		if nilRegistry(runtimeRegistry) {
			return ExecutionResult{}, fmt.Errorf("%w: runtime registry binder returned nil", ErrInvalidBuild)
		}
		if err := validateExecutableManifest(manifest, runtimeRegistry); err != nil {
			return ExecutionResult{}, fmt.Errorf("%w: runtime registry binder changed the executable catalog: %w", ErrInvalidBuild, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return ExecutionResult{}, err
	}
	maxSteps := options.MaxSteps
	if maxSteps == 0 {
		maxSteps = DefaultMaxExecutionSteps
	}
	if maxSteps < 1 || maxSteps > 1_000_000 {
		return ExecutionResult{}, fmt.Errorf("%w: max execution steps is out of bounds", ErrInvalidBuild)
	}
	pollInterval := options.PollInterval
	if pollInterval == 0 {
		pollInterval = 100 * time.Millisecond
	}
	if pollInterval < time.Millisecond || pollInterval > time.Hour {
		return ExecutionResult{}, fmt.Errorf("%w: external poll interval is out of bounds", ErrInvalidBuild)
	}
	now := options.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	clock := now().UTC()
	if clock.IsZero() {
		return ExecutionResult{}, fmt.Errorf("%w: execution clock returned zero", ErrInvalidBuild)
	}
	tick := func() time.Time { clock = clock.Add(time.Nanosecond); return clock }

	inputDigest, err := digestNativeInputs(options.Inputs)
	if err != nil {
		return ExecutionResult{}, err
	}
	runID := options.RunID
	if runID == "" {
		runID = runtime.RunID("offline-" + manifest.BuildDigest[len("sha256:"):len("sha256:")+16] + "-" + inputDigest[len("sha256:"):len("sha256:")+8])
	}
	startKey := options.IdempotencyKey
	if startKey == "" {
		startKey = "offline:start:" + manifest.BuildDigest + ":" + inputDigest
	}
	bound, err := runtime.BindRun(ctx, store, runtime.BindRunRequest{ID: runID, Plan: &manifest.Plan, Inputs: options.Inputs, CreatedAt: tick()})
	if err != nil {
		return ExecutionResult{}, err
	}
	if len(bound.Diagnostics) != 0 {
		return ExecutionResult{}, &DiagnosticError{Diagnostics: bound.Diagnostics}
	}
	if bound.Run == nil {
		return ExecutionResult{}, fmt.Errorf("%w: input binding returned no run", ErrInvalidBuild)
	}
	started, _, err := runtime.StartBoundRun(ctx, store, *bound.Run, startKey)
	if err != nil {
		return ExecutionResult{}, err
	}
	for _, node := range sortedNodes(manifest.Plan.Graph.Nodes) {
		_, err = store.CreateNodeInvocation(ctx, runtime.CreateNodeInvocationRequest{Snapshot: runtime.NodeInvocationSnapshot{
			ID: runtime.NodeInvocationID{RunID: runID, NodeID: node.ID}, Status: runtime.NodePending, CreatedAt: tick(), UpdatedAt: clock,
		}})
		if err != nil {
			return ExecutionResult{}, err
		}
	}
	running, err := store.TransitionRun(ctx, runtime.RunTransitionRequest{RunID: runID, ExpectedGeneration: started.Generation, To: runtime.RunRunning, At: tick()})
	if err != nil {
		return ExecutionResult{}, err
	}
	recoveryPlan := runtime.RecoveryPlan{
		Ref: running.Snapshot.Plan, Plan: manifest.Plan, Visibility: manifest.Visibility,
	}
	if planErr := recoveryPlan.Validate(); planErr != nil {
		return ExecutionResult{}, fmt.Errorf("%w: invalid embedded recovery plan: %w", ErrInvalidBuild, planErr)
	}
	coordinator := &runtime.RecoveryCoordinator{
		Store: store, Recovery: store, Inputs: store, Control: store,
		Plans: fixedPlanSource{plan: recoveryPlan}, Registry: runtimeRegistry, Policies: store,
	}
	dispatcher, err := runtime.NewStepDispatcher(runtime.DispatcherOptions{Store: store, Registry: runtimeRegistry, Now: tick, Verifiers: options.Verifiers, RetryCoordinator: &runtime.RetryCoordinator{Store: store}})
	if err != nil {
		return ExecutionResult{}, err
	}
	external, err := runtime.NewExternalOperationCoordinator(runtime.ExternalOperationOptions{Store: store, Registry: runtimeRegistry, Now: tick, Verifiers: options.Verifiers})
	if err != nil {
		return ExecutionResult{}, err
	}
	queue := runtime.NewReadyQueueCoordinator(store, nil)
	claimSequence := 0
	for step := 0; step < maxSteps; step++ {
		if err := ctx.Err(); err != nil {
			return ExecutionResult{}, err
		}
		_, recoverErr := coordinator.Recover(ctx, runtime.RecoveryRequest{Now: tick()})
		if recoverErr != nil && !concurrentProgress(recoverErr) {
			return ExecutionResult{}, recoverErr
		}
		if _, fanOutErr := advanceFanOut(ctx, store, recoveryPlan, runID, tick()); fanOutErr != nil && !errors.Is(fanOutErr, runtime.ErrFanOutIncomplete) && !concurrentProgress(fanOutErr) {
			return ExecutionResult{}, fanOutErr
		}
		current, err := store.LoadRun(ctx, runID)
		if err != nil {
			return ExecutionResult{}, err
		}
		if current.Status.Terminal() {
			return collectResult(ctx, store, current)
		}
		operations, operationErr := external.Recover(ctx, runtime.ExternalOperationQuery{RunID: runID, Limit: 1})
		if operationErr != nil {
			return ExecutionResult{}, operationErr
		}
		if len(operations) != 0 {
			timer := time.NewTimer(pollInterval)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return ExecutionResult{}, ctx.Err()
			case <-timer.C:
			}
			operationResult, reconcileErr := external.Reconcile(ctx, operations[0].Attempt)
			if reconcileErr != nil && operationResult.Operation.Generation <= operations[0].Generation {
				return ExecutionResult{}, reconcileErr
			}
			continue
		}

		claimSequence++
		claimAt := tick()
		claim, ok, claimErr := queue.ClaimNext(ctx, runtime.ReadyClaimRequest{
			RunID: runID, Owner: "offline-worker", Token: fmt.Sprintf("claim-%d", claimSequence),
			IdempotencyKey: fmt.Sprintf("offline:claim:%d", claimSequence), Now: claimAt, LeaseUntil: claimAt.Add(time.Minute),
		})
		if claimErr != nil {
			return ExecutionResult{}, claimErr
		}
		if ok {
			node, found := graphNode(manifest.Plan.Graph, claim.Candidate.InvocationID.NodeID)
			if !found {
				return ExecutionResult{}, fmt.Errorf("%w: claimed node is absent from embedded plan", ErrInvalidBuild)
			}
			if node.ForEach != nil && claim.Candidate.InvocationID.Iteration == "" {
				return ExecutionResult{}, fmt.Errorf("%w: fan-out aggregate became directly claimable", ErrInvalidBuild)
			}
			result, dispatchErr := dispatcher.Dispatch(ctx, runtime.DispatchRequest{Claim: claim, Node: node, IdempotencyKey: "offline:invoke:" + string(runID) + ":" + node.ID})
			if result.Wait != nil {
				return ExecutionResult{}, fmt.Errorf("%w: node %q attempted daemon-dependent suspension", ErrInvalidBuild, node.ID)
			}
			if dispatchErr != nil && result.Attempt.Generation == 0 {
				return ExecutionResult{}, dispatchErr
			}
			continue
		}

		activations, activationErr := store.RecoverRetryActivations(ctx, runtime.RetryActivationQuery{RunID: runID, DueBefore: clock.Add(100 * 365 * 24 * time.Hour)})
		if activationErr != nil {
			return ExecutionResult{}, activationErr
		}
		if len(activations) != 0 {
			sort.Slice(activations, func(i, j int) bool {
				if activations[i].FireAt.Equal(activations[j].FireAt) {
					return activations[i].ID < activations[j].ID
				}
				return activations[i].FireAt.Before(activations[j].FireAt)
			})
			if activations[0].FireAt.After(clock) {
				clock = activations[0].FireAt
			}
			continue
		}
		completionAt := tick()
		completed, _, completionErr := runtime.NewControlFlowCoordinator(store, store, nil).ReconcileRunCompletion(ctx, manifest.Plan.Graph, runID, "offline-complete:"+string(runID), completionAt)
		if errors.Is(completionErr, runtime.ErrControlFlowPending) {
			continue
		}
		if errors.Is(completionErr, runtime.ErrRunOutputsPending) {
			expression, expressionErr := runtime.BuildExpressionContext(ctx, store, store, manifest.Plan.Graph, runID)
			if expressionErr != nil {
				return ExecutionResult{}, expressionErr
			}
			finalized, finalizeErr := runtime.FinalizeRunOutputs(ctx, store, runtime.FinalizeRunRequest{BoundRun: *bound.Run, Run: completed, Plan: &manifest.Plan, Context: expression, Control: store, At: tick()})
			if finalizeErr != nil {
				return ExecutionResult{}, finalizeErr
			}
			if len(finalized.Diagnostics) != 0 {
				return ExecutionResult{}, &DiagnosticError{Diagnostics: finalized.Diagnostics}
			}
			continue
		}
		if completionErr != nil && !concurrentProgress(completionErr) {
			return ExecutionResult{}, completionErr
		}
		if completed.Status.Terminal() {
			continue
		}
		invocations, _ := store.ListRunInvocations(ctx, runID)
		return ExecutionResult{}, fmt.Errorf("%w: durable nodes=%v", ErrExecutionStalled, summarizeInvocations(invocations))
	}
	return ExecutionResult{}, fmt.Errorf("%w: exceeded %d steps", ErrExecutionStalled, maxSteps)
}

func advanceFanOut(ctx context.Context, store ExecutionStore, plan runtime.RecoveryPlan, runID runtime.RunID, at time.Time) (bool, error) {
	expression, err := runtime.BuildExpressionContext(ctx, store, store, plan.Plan.Graph, runID)
	if err != nil {
		return false, err
	}
	coordinator := runtime.FanOutCoordinator{Store: store}
	for _, node := range sortedNodes(plan.Plan.Graph.Nodes) {
		if node.ForEach == nil {
			continue
		}
		id := runtime.NodeInvocationID{RunID: runID, NodeID: node.ID}
		snapshot, err := store.LoadNodeInvocation(ctx, id)
		if err != nil {
			return false, err
		}
		if snapshot.Status == runtime.NodeReady {
			scoped, options, scopeErr := plan.Visibility.ScopeNodeContext(node.ID, expression, values.ExpressionOptions{})
			if scopeErr != nil {
				return false, scopeErr
			}
			_, err = coordinator.Expand(ctx, runtime.FanOutExpandCommand{Parent: id, ExpectedParentGeneration: snapshot.Generation, Spec: *node.ForEach, ExpressionContext: scoped, ExpressionOptions: options, Priority: snapshot.Priority, At: at})
			return err == nil, err
		}
		if snapshot.Status != runtime.NodeWaiting {
			continue
		}
		fanOut, err := store.LoadFanOut(ctx, id)
		if errors.Is(err, runtime.ErrNotFound) {
			continue
		}
		if err != nil {
			return false, err
		}
		terminal := true
		for _, item := range fanOut.Items {
			child, loadErr := store.LoadNodeInvocation(ctx, item.Invocation)
			if loadErr != nil {
				return false, loadErr
			}
			terminal = terminal && child.Status.Terminal()
		}
		if terminal {
			_, _, _, err = coordinator.Collect(ctx, id, at)
			return err == nil, err
		}
	}
	return false, nil
}

type fixedPlanSource struct{ plan runtime.RecoveryPlan }

func (s fixedPlanSource) LoadRecoveryPlan(context.Context, runtime.RunSnapshot) (runtime.RecoveryPlan, error) {
	return s.plan, nil
}

type DiagnosticError struct{ Diagnostics []diagnostic.Diagnostic }

func (e *DiagnosticError) Error() string {
	if e != nil && len(e.Diagnostics) != 0 {
		return e.Diagnostics[0].Message
	}
	return "offline workflow input validation failed"
}

func validateExecutableManifest(manifest Manifest, registry stepkind.Registry) error {
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	parsed, err := ParseManifest(encoded)
	if err != nil {
		return err
	}
	if parsed.BuildDigest != manifest.BuildDigest {
		return fmt.Errorf("%w: manifest changed while validating", ErrInvalidBuild)
	}
	actualCatalog := registry.List()
	if !reflect.DeepEqual(actualCatalog, manifest.StepKinds) {
		return fmt.Errorf("%w: runtime registry differs from the exact embedded catalog", ErrInvalidBuild)
	}
	for _, expected := range manifest.StepKinds {
		_, actual, resolveErr := stepkind.Resolve(registry, expected.Name, expected.Version)
		if resolveErr != nil {
			return resolveErr
		}
		if !reflect.DeepEqual(actual, expected) {
			return fmt.Errorf("%w: runtime spec for %s@%s differs from embedded metadata", ErrInvalidBuild, expected.Name, expected.Version)
		}
	}
	for _, resolved := range manifest.Bindings {
		kind, ok := registry.Lookup(resolved.Binding.Kind, resolved.Binding.Version)
		if !ok {
			return fmt.Errorf("%w: bound executable is absent", ErrInvalidBuild)
		}
		remote, ok := kind.(*remoteKind)
		if !ok {
			return fmt.Errorf("%w: binding is not backed by the closed remote executor", ErrInvalidBuild)
		}
		if _, ok := remote.bindings[resolved.Binding.NodeID]; !ok {
			return fmt.Errorf("%w: runtime binding coverage differs", ErrInvalidBuild)
		}
		if !reflect.DeepEqual(remote.profiles[resolved.Binding.NodeID], resolved.ExecutionProfile) {
			return fmt.Errorf("%w: runtime execution profile differs", ErrInvalidBuild)
		}
	}
	return nil
}

func digestNativeInputs(input map[string]any) (string, error) {
	cloned, err := cloneJSON(input)
	if err != nil {
		return "", fmt.Errorf("%w: inputs are not canonical JSON: %w", ErrInvalidBuild, err)
	}
	encoded, err := json.Marshal(cloned)
	if err != nil {
		return "", err
	}
	return values.SHA256Digest(encoded), nil
}

func collectResult(ctx context.Context, store ExecutionStore, run runtime.RunSnapshot) (ExecutionResult, error) {
	result := ExecutionResult{Run: run, Outputs: values.ValueSet{}}
	if run.Status == runtime.RunSucceeded {
		if run.Outputs != nil {
			outputs, err := store.LoadValues(ctx, *run.Outputs)
			if err != nil {
				return ExecutionResult{}, err
			}
			result.Outputs = outputs
		}
		return result, nil
	}
	var selected *runtime.Failure
	invocations, err := store.ListRunInvocations(ctx, run.ID)
	if err != nil {
		return ExecutionResult{}, err
	}
	for _, invocation := range invocations {
		if invocation.LatestAttempt == 0 {
			continue
		}
		attempt, loadErr := store.LoadAttempt(ctx, runtime.AttemptID{Invocation: invocation.ID, Number: invocation.LatestAttempt})
		if loadErr == nil && attempt.Failure != nil {
			failure := *attempt.Failure
			selected = &failure
			break
		}
	}
	return result, &RunFailureError{Run: run, Failure: selected}
}

func nilExecutionStore(store ExecutionStore) bool {
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

func nilRuntimeRegistryBinder(binder RuntimeRegistryBinder) bool {
	if binder == nil {
		return true
	}
	value := reflect.ValueOf(binder)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func sortedNodes(input []graph.Node) []graph.Node {
	result := append([]graph.Node(nil), input...)
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func concurrentProgress(err error) bool {
	return errors.Is(err, runtime.ErrCASMismatch) || errors.Is(err, runtime.ErrTransitionConflict) || errors.Is(err, runtime.ErrAttemptConflict) || errors.Is(err, runtime.ErrAlreadyExists) || errors.Is(err, runtime.ErrControlFlowPending)
}

func summarizeInvocations(input []runtime.NodeInvocationSnapshot) []string {
	result := make([]string, len(input))
	for index, invocation := range input {
		result[index] = fmt.Sprintf("%s[%s]=%s", invocation.ID.NodeID, invocation.ID.Iteration, invocation.Status)
	}
	return result
}

var _ compile.StepKindLookup = (stepkind.Registry)(nil)
