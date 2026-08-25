package appworkflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
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

const contractRunnerStepLimit = 10000

type canonicalContractRunner struct {
	dependencies compile.DependencyOptions
	verifiers    verification.Registry
}

func newCanonicalContractRunner(dependencies compile.DependencyOptions, verifiers verification.Registry) ContractRunner {
	extractors := make(map[string]compile.VerificationExpressionExtractor, len(dependencies.VerificationExtractors))
	for name, extractor := range dependencies.VerificationExtractors {
		extractors[name] = extractor
	}
	return &canonicalContractRunner{dependencies: compile.DependencyOptions{VerificationExtractors: extractors}, verifiers: verifiers}
}

func (r *canonicalContractRunner) Execute(ctx context.Context, plan *compile.ExecutionPlan, kinds stepkind.Registry, suite WorkflowContractSuite, repetitions int) (ContractTestReport, error) {
	if ctx == nil || plan == nil || nilInterface(kinds) || repetitions < 1 {
		return ContractTestReport{}, fmt.Errorf("%w: canonical runner requires context, plan, registry, and repetitions", ErrInvalidContractService)
	}
	if err := ctx.Err(); err != nil {
		return ContractTestReport{}, err
	}
	if planUsesVerification(plan.Graph) && nilInterface(r.verifiers) {
		return ContractTestReport{}, fmt.Errorf("%w: verified plan requires the exact frozen verifier catalog", ErrInvalidContractService)
	}
	inferred := compile.InferValueDependencies(plan, r.dependencies)
	if inferred.Plan == nil || len(inferred.Diagnostics) != 0 || inferred.Plan.Digest != plan.Digest {
		return ContractTestReport{}, fmt.Errorf("%w: canonical runner could not reproduce the compiled dependency plan", ErrInvalidContractService)
	}
	recoveryPlan := runtime.RecoveryPlan{
		Ref:  runtime.PlanRef{ID: plan.ID, Version: plan.Graph.Version, Digest: plan.Digest, SchemaVersion: plan.SchemaVersion},
		Plan: *plan, Visibility: inferred.Visibility,
	}
	if err := recoveryPlan.Validate(); err != nil {
		return ContractTestReport{}, fmt.Errorf("%w: invalid recovery plan: %w", ErrInvalidContractService, err)
	}

	report := ContractTestReport{Passed: true, Cases: make([]ContractCaseResult, len(suite.Cases))}
	for caseIndex, contractCase := range suite.Cases {
		var stable contractObservation
		for repetition := 0; repetition < repetitions; repetition++ {
			observed, err := executeContractRepetition(ctx, plan, recoveryPlan, kinds, r.verifiers, contractCase)
			if err != nil {
				return ContractTestReport{}, fmt.Errorf("execute contract case %s repetition %d: %w", contractCase.Name, repetition+1, err)
			}
			if repetition == 0 {
				stable = observed
				continue
			}
			if !reflectContractJSONEqual(stable, observed) {
				report.Cases[caseIndex] = ContractCaseResult{Name: contractCase.Name, Passed: false, Effects: graph.EffectSet{}, Message: "isolated repetitions produced different durable observations"}
				report.Passed = false
				stable = contractObservation{}
				break
			}
		}
		if report.Cases[caseIndex].Name != "" {
			continue
		}
		result := contractResult(contractCase, stable)
		report.Cases[caseIndex] = result
		if !result.Passed {
			report.Passed = false
		}
	}
	return report, nil
}

type contractObservation struct {
	Outputs values.ValueSet        `json:"outputs,omitempty"`
	Failure *ContractExpectedError `json:"failure,omitempty"`
	Effects graph.EffectSet        `json:"effects"`
	Calls   []ContractToolCall     `json:"calls,omitempty"`
}

type contractExecution struct {
	plan      *compile.ExecutionPlan
	recovery  runtime.RecoveryPlan
	store     *runtimetest.Store
	registry  *stepkind.MemoryRegistry
	verifiers verification.Registry
	mocks     *contractMockCatalog
	bound     runtime.BoundRun
	runID     runtime.RunID
	clock     time.Time
	claim     int
	policyRun map[runtime.NodeInvocationID]struct{}
}

func executeContractRepetition(ctx context.Context, plan *compile.ExecutionPlan, recovery runtime.RecoveryPlan, kinds stepkind.Registry, verifiers verification.Registry, contractCase WorkflowContractCase) (contractObservation, error) {
	registry, mocks, err := contractMockRegistry(kinds, plan.Graph, contractCase)
	if err != nil {
		return contractObservation{}, err
	}
	runID := runtime.RunID("contract-" + values.SHA256Digest([]byte(plan.Digest + "\x00" + contractCase.Name))[7:31])
	base := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	store := runtimetest.NewStore()
	caller := make(map[string]any, len(contractCase.Inputs))
	for name, value := range contractCase.Inputs {
		caller[name] = value
	}
	bound, err := runtime.BindRun(ctx, store, runtime.BindRunRequest{ID: runID, Plan: plan, Inputs: caller, CreatedAt: base})
	if err != nil {
		return contractObservation{}, err
	}
	if len(bound.Diagnostics) != 0 || bound.Run == nil {
		return diagnosticObservation(bound.Diagnostics), nil
	}
	started, _, err := runtime.StartBoundRun(ctx, store, *bound.Run, "contract-start:"+string(runID))
	if err != nil {
		return contractObservation{}, err
	}
	nodes := append([]graph.Node(nil), plan.Graph.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	for _, node := range nodes {
		id := runtime.NodeInvocationID{RunID: runID, NodeID: node.ID}
		if _, createErr := store.CreateNodeInvocation(ctx, runtime.CreateNodeInvocationRequest{Snapshot: runtime.NodeInvocationSnapshot{ID: id, Status: runtime.NodePending, CreatedAt: base, UpdatedAt: base}}); createErr != nil {
			return contractObservation{}, createErr
		}
	}
	transitioned, err := store.TransitionRun(ctx, runtime.RunTransitionRequest{RunID: runID, ExpectedGeneration: started.Generation, To: runtime.RunRunning, At: base.Add(time.Nanosecond)})
	if err != nil {
		return contractObservation{}, err
	}
	execution := contractExecution{
		plan: plan, recovery: recovery, store: store, registry: registry, verifiers: verifiers, mocks: mocks,
		bound: *bound.Run, runID: runID, clock: transitioned.Snapshot.UpdatedAt,
		policyRun: make(map[runtime.NodeInvocationID]struct{}),
	}
	return execution.run(ctx)
}

func (e *contractExecution) run(ctx context.Context) (contractObservation, error) {
	for pass := 0; pass < contractRunnerStepLimit; pass++ {
		if err := ctx.Err(); err != nil {
			return contractObservation{}, err
		}
		run, err := e.store.LoadRun(ctx, e.runID)
		if err != nil {
			return contractObservation{}, err
		}
		if run.Status.Terminal() {
			observed, observeErr := e.observe(ctx, run)
			var diagnosticErr *contractDiagnosticError
			if errors.As(observeErr, &diagnosticErr) {
				observed = diagnosticObservation(diagnosticErr.diagnostics)
				observed.Effects = e.mocks.observedEffects()
				observed.Calls = e.mocks.observedCalls()
				return observed, nil
			}
			return observed, observeErr
		}
		progressed := false
		changed, applyErr := e.applyControl(ctx, run)
		if applyErr != nil {
			return contractObservation{}, applyErr
		}
		progressed = progressed || changed
		changed, driveErr := e.driveNodes(ctx, run)
		if driveErr != nil {
			return contractObservation{}, driveErr
		}
		progressed = progressed || changed
		changed, fanOutErr := e.expandAndCollectFanOut(ctx)
		if fanOutErr != nil {
			return contractObservation{}, fanOutErr
		}
		progressed = progressed || changed
		changed, retryErr := e.activateRetries(ctx)
		if retryErr != nil {
			return contractObservation{}, retryErr
		}
		progressed = progressed || changed
		changed, dispatchErr := e.dispatchOne(ctx)
		if dispatchErr != nil {
			return contractObservation{}, dispatchErr
		}
		if changed {
			continue
		}
		completed, err := e.completeRun(ctx)
		var diagnosticErr *contractDiagnosticError
		if errors.As(err, &diagnosticErr) {
			observed := diagnosticObservation(diagnosticErr.diagnostics)
			observed.Effects = e.mocks.observedEffects()
			observed.Calls = e.mocks.observedCalls()
			return observed, nil
		}
		if err != nil {
			return contractObservation{}, err
		}
		if completed {
			continue
		}
		if !progressed {
			return contractObservation{}, fmt.Errorf("%w: canonical runtime made no progress", ErrContractTestFailed)
		}
	}
	return contractObservation{}, fmt.Errorf("%w: canonical runtime exceeded %d steps", ErrContractTestFailed, contractRunnerStepLimit)
}

func (e *contractExecution) applyControl(ctx context.Context, run runtime.RunSnapshot) (bool, error) {
	changed := false
	coordinator := runtime.NewControlFlowCoordinator(e.store, e.store, nil)
	expression, err := runtime.BuildExpressionContext(ctx, e.store, e.store, e.plan.Graph, e.runID)
	if err != nil {
		return false, err
	}
	for _, node := range sortedGraphNodes(e.plan.Graph.Nodes) {
		if node.Finally != nil {
			continue
		}
		id := runtime.NodeInvocationID{RunID: e.runID, NodeID: node.ID}
		snapshot, err := e.store.LoadNodeInvocation(ctx, id)
		if err != nil {
			return false, err
		}
		if node.Switch != nil && snapshot.Status == runtime.NodeSucceeded {
			if _, err := e.store.LoadControlDecision(ctx, runtime.ControlDecisionID{Source: id, Kind: runtime.ControlSwitch}); errors.Is(err, runtime.ErrNotFound) {
				if _, decisionErr := coordinator.DecideSwitch(ctx, runtime.DecideSwitchRequest{Source: id, Node: node, ExpressionContext: expression, At: e.tick()}); decisionErr != nil {
					return false, decisionErr
				}
				changed = true
			} else if err != nil {
				return false, err
			}
		}
		if len(node.Catch) != 0 && hardContractFailure(snapshot.Status) && snapshot.LatestAttempt > 0 {
			if _, err := e.store.LoadControlDecision(ctx, runtime.ControlDecisionID{Source: id, Kind: runtime.ControlCatch}); errors.Is(err, runtime.ErrNotFound) {
				if _, decisionErr := coordinator.DecideCatch(ctx, runtime.DecideCatchRequest{Source: id, Node: node, ExpressionContext: expression, At: e.tick()}); decisionErr != nil {
					return false, decisionErr
				}
				changed = true
			} else if err != nil {
				return false, err
			}
		}
		if hardContractFailure(snapshot.Status) {
			if _, done := e.policyRun[id]; done {
				continue
			}
			result, err := runtime.NewRunPolicyCoordinator(e.store, e.store, e.store).HandleRunFailure(ctx, runtime.HandleRunFailureRequest{
				Workflow: e.plan.Graph, Source: id, IdempotencyKey: "contract-policy:" + node.ID, At: e.tick(),
			})
			if errors.Is(err, runtime.ErrControlFlowPending) {
				continue
			}
			if err != nil {
				return false, err
			}
			e.policyRun[id] = struct{}{}
			changed = changed || result.Disposition == runtime.RunFailureFailFast
		}
	}
	_ = run
	return changed, nil
}

func (e *contractExecution) driveNodes(ctx context.Context, run runtime.RunSnapshot) (bool, error) {
	expression, err := runtime.BuildExpressionContext(ctx, e.store, e.store, e.plan.Graph, e.runID)
	if err != nil {
		return false, err
	}
	driver := runtime.NodeDriver{Store: e.store, Inputs: e.store, Control: e.store, Registry: e.registry}
	changed := false
	for _, node := range sortedGraphNodes(e.plan.Graph.Nodes) {
		node, err = exactContractNode(e.registry, node)
		if err != nil {
			return false, err
		}
		id := runtime.NodeInvocationID{RunID: e.runID, NodeID: node.ID}
		before, err := e.store.LoadNodeInvocation(ctx, id)
		if err != nil {
			return false, err
		}
		if before.Status != runtime.NodePending && before.Status != runtime.NodeBlocked {
			continue
		}
		var result runtime.DriveNodeResult
		if node.Finally == nil {
			result, err = driver.Drive(ctx, runtime.DriveNodeRequest{Run: run, Plan: e.recovery, InvocationID: id, Node: node, ExpressionContext: expression, At: e.tick()})
		} else {
			if _, loadErr := e.store.LoadTerminalIntent(ctx, e.runID); errors.Is(loadErr, runtime.ErrNotFound) {
				continue
			} else if loadErr != nil {
				return false, loadErr
			}
			result, err = driver.DriveFinally(ctx, runtime.DriveNodeRequest{Run: run, Plan: e.recovery, InvocationID: id, Node: node, ExpressionContext: expression, At: e.tick()})
		}
		if errors.Is(err, runtime.ErrControlFlowPending) {
			continue
		}
		if err != nil {
			return false, err
		}
		if result.Binding != nil || result.Progressed.Snapshot.Generation != before.Generation || result.Progressed.Snapshot.Status != before.Status {
			changed = true
		}
	}
	return changed, nil
}

func (e *contractExecution) expandAndCollectFanOut(ctx context.Context) (bool, error) {
	changed := false
	coordinator := runtime.FanOutCoordinator{Store: e.store}
	expression, err := runtime.BuildExpressionContext(ctx, e.store, e.store, e.plan.Graph, e.runID)
	if err != nil {
		return false, err
	}
	for _, node := range sortedGraphNodes(e.plan.Graph.Nodes) {
		if node.ForEach == nil {
			continue
		}
		id := runtime.NodeInvocationID{RunID: e.runID, NodeID: node.ID}
		snapshot, err := e.store.LoadNodeInvocation(ctx, id)
		if err != nil {
			return false, err
		}
		if snapshot.Status == runtime.NodeReady {
			scoped, options, scopeErr := e.recovery.Visibility.ScopeNodeContext(node.ID, expression, values.ExpressionOptions{})
			if scopeErr != nil {
				return false, scopeErr
			}
			if _, expandErr := coordinator.Expand(ctx, runtime.FanOutExpandCommand{Parent: id, ExpectedParentGeneration: snapshot.Generation, Spec: *node.ForEach, InputBindings: node.InputBindings, ExpressionContext: scoped, ExpressionOptions: options, Priority: snapshot.Priority, At: e.tick()}); expandErr != nil {
				return false, expandErr
			}
			changed = true
			continue
		}
		if snapshot.Status != runtime.NodeWaiting {
			continue
		}
		fanOut, err := e.store.LoadFanOut(ctx, id)
		if errors.Is(err, runtime.ErrNotFound) {
			continue
		}
		if err != nil {
			return false, err
		}
		terminal := true
		for _, item := range fanOut.Items {
			child, err := e.store.LoadNodeInvocation(ctx, item.Invocation)
			if err != nil {
				return false, err
			}
			terminal = terminal && child.Status.Terminal()
		}
		if terminal {
			if _, _, _, err := coordinator.Collect(ctx, id, e.tick()); err != nil {
				return false, err
			}
			changed = true
		}
	}
	return changed, nil
}

func (e *contractExecution) activateRetries(ctx context.Context) (bool, error) {
	activations, err := e.store.RecoverRetryActivations(ctx, runtime.RetryActivationQuery{RunID: e.runID, DueBefore: e.clock.Add(100 * 365 * 24 * time.Hour)})
	if err != nil {
		return false, err
	}
	if len(activations) == 0 {
		return false, nil
	}
	sort.Slice(activations, func(i, j int) bool {
		if activations[i].FireAt.Equal(activations[j].FireAt) {
			return activations[i].ID < activations[j].ID
		}
		return activations[i].FireAt.Before(activations[j].FireAt)
	})
	for _, activation := range activations {
		node, err := e.store.LoadNodeInvocation(ctx, activation.Attempt.Invocation)
		if err != nil {
			return false, err
		}
		at := activation.FireAt
		if !at.After(e.clock) {
			at = e.tick()
		} else {
			e.clock = at
		}
		if _, err := e.store.ActivateNodeRetry(ctx, runtime.ActivateNodeRetryRequest{ActivationID: activation.ID, ExpectedActivationGeneration: activation.Generation, ExpectedNodeGeneration: node.Generation, IdempotencyKey: "contract-activate:" + activation.ID, Now: at}); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (e *contractExecution) dispatchOne(ctx context.Context) (bool, error) {
	e.claim++
	at := e.tick()
	claim, ok, err := runtime.NewReadyQueueCoordinator(e.store, nil).ClaimNext(ctx, runtime.ReadyClaimRequest{
		RunID: e.runID, Owner: "contract-worker", Token: fmt.Sprintf("claim-%d", e.claim),
		IdempotencyKey: fmt.Sprintf("contract-claim-%d", e.claim), Now: at, LeaseUntil: at.Add(time.Minute),
	})
	if err != nil || !ok {
		return false, err
	}
	node, ok := graphNodeByID(e.plan.Graph, claim.Candidate.InvocationID.NodeID)
	if !ok {
		return false, fmt.Errorf("%w: claimed node is absent from plan", ErrInvalidContractService)
	}
	node, err = exactContractNode(e.registry, node)
	if err != nil {
		return false, err
	}
	// Fan-out aggregate nodes are expanded before claim and must never execute.
	if node.ForEach != nil && claim.Candidate.InvocationID.Iteration == "" {
		return false, fmt.Errorf("%w: fan-out aggregate became directly claimable", ErrInvalidContractService)
	}
	dispatcher, err := runtime.NewStepDispatcher(runtime.DispatcherOptions{
		Store: e.store, Registry: e.registry, Now: func() time.Time { return e.tick() },
		RetryCoordinator: &runtime.RetryCoordinator{Store: e.store}, Verifiers: e.verifiers,
	})
	if err != nil {
		return false, err
	}
	result, dispatchErr := dispatcher.Dispatch(ctx, runtime.DispatchRequest{Claim: claim, Node: node, IdempotencyKey: "contract-invoke:" + string(e.runID) + ":" + node.ID})
	if dispatchErr != nil && result.Node.Generation == 0 {
		return false, dispatchErr
	}
	return true, nil
}

func (e *contractExecution) completeRun(ctx context.Context) (bool, error) {
	ordinaryTerminal, allSucceeded, err := e.ordinaryOutcome(ctx)
	if err != nil || !ordinaryTerminal {
		return false, err
	}
	finalizers := false
	for _, node := range e.plan.Graph.Nodes {
		finalizers = finalizers || node.Finally != nil
	}
	coordinator := runtime.NewControlFlowCoordinator(e.store, e.store, nil)
	run, err := e.store.LoadRun(ctx, e.runID)
	if err != nil {
		return false, err
	}
	if finalizers || !allSucceeded || len(e.plan.Graph.Outputs) == 0 {
		reconciled, _, reconcileErr := coordinator.ReconcileRunCompletion(ctx, e.plan.Graph, e.runID, "contract-complete:"+string(e.runID), e.tick())
		if errors.Is(reconcileErr, runtime.ErrControlFlowPending) {
			return true, nil
		}
		if !errors.Is(reconcileErr, runtime.ErrRunOutputsPending) {
			return reconcileErr == nil, reconcileErr
		}
		run = reconciled
	}
	expression, err := runtime.BuildExpressionContext(ctx, e.store, e.store, e.plan.Graph, e.runID)
	if err != nil {
		return false, err
	}
	finalized, err := runtime.FinalizeRunOutputs(ctx, e.store, runtime.FinalizeRunRequest{BoundRun: e.bound, Run: run, Plan: e.plan, Context: expression, Control: e.store, At: e.tick()})
	if err != nil {
		return false, err
	}
	if len(finalized.Diagnostics) != 0 {
		return false, &contractDiagnosticError{diagnostics: finalized.Diagnostics}
	}
	return true, nil
}

func (e *contractExecution) ordinaryOutcome(ctx context.Context) (bool, bool, error) {
	allSucceeded := true
	for _, node := range e.plan.Graph.Nodes {
		if node.Finally != nil {
			continue
		}
		snapshot, err := e.store.LoadNodeInvocation(ctx, runtime.NodeInvocationID{RunID: e.runID, NodeID: node.ID})
		if err != nil {
			return false, false, err
		}
		if !snapshot.Status.Terminal() {
			return false, false, nil
		}
		if hardContractFailure(snapshot.Status) {
			handled := false
			if len(node.Catch) != 0 {
				decision, err := e.store.LoadControlDecision(ctx, runtime.ControlDecisionID{Source: snapshot.ID, Kind: runtime.ControlCatch})
				if err != nil {
					return false, false, err
				}
				handled = decision.Outcome == runtime.ControlSelected || decision.Outcome == runtime.ControlContinued
			}
			allSucceeded = allSucceeded && handled
		}
	}
	return true, allSucceeded, nil
}

func (e *contractExecution) observe(ctx context.Context, run runtime.RunSnapshot) (contractObservation, error) {
	observation := contractObservation{Effects: e.mocks.observedEffects(), Calls: e.mocks.observedCalls()}
	if run.Status == runtime.RunSucceeded {
		if len(e.plan.Graph.Outputs) == 0 {
			observation.Outputs = values.ValueSet{}
			return observation, nil
		}
		expression, err := runtime.BuildExpressionContext(ctx, e.store, e.store, e.plan.Graph, e.runID)
		if err != nil {
			return contractObservation{}, err
		}
		finalized, err := runtime.FinalizeRunOutputs(ctx, e.store, runtime.FinalizeRunRequest{
			BoundRun: e.bound, Run: run, Plan: e.plan, Context: expression, At: e.tick(),
		})
		if err != nil {
			return contractObservation{}, err
		}
		if len(finalized.Diagnostics) != 0 {
			return contractObservation{}, &contractDiagnosticError{diagnostics: finalized.Diagnostics}
		}
		observation.Outputs = finalized.Outputs
		return observation, nil
	}
	failure, err := e.durableFailure(ctx)
	if err != nil {
		return contractObservation{}, err
	}
	observation.Failure = failure
	return observation, nil
}

func (e *contractExecution) durableFailure(ctx context.Context) (*ContractExpectedError, error) {
	var selected *runtime.Failure
	priority := map[runtime.NodeStatus]int{runtime.NodeCanceled: 1, runtime.NodeFailed: 2, runtime.NodeTimedOut: 3, runtime.NodeCrashed: 4}
	selectedPriority := 0
	invocations, err := e.store.ListRunInvocations(ctx, e.runID)
	if err != nil {
		return nil, err
	}
	for _, invocation := range invocations {
		if !hardContractFailure(invocation.Status) || priority[invocation.Status] < selectedPriority || invocation.LatestAttempt == 0 {
			continue
		}
		attempt, err := e.store.LoadAttempt(ctx, runtime.AttemptID{Invocation: invocation.ID, Number: invocation.LatestAttempt})
		if err != nil {
			return nil, err
		}
		if attempt.Failure != nil {
			failureCopy := *attempt.Failure
			selected, selectedPriority = &failureCopy, priority[invocation.Status]
		}
	}
	if selected == nil {
		return &ContractExpectedError{Code: "workflow_" + string((runtime.NodeFailed)), Message: "workflow contract run failed"}, nil
	}
	return &ContractExpectedError{Code: selected.Code, Message: selected.Message}, nil
}

func (e *contractExecution) tick() time.Time {
	e.clock = e.clock.Add(time.Nanosecond)
	return e.clock
}

type contractMockCatalog struct {
	mu      sync.Mutex
	mocks   map[string]ContractExecutorMock
	nodes   map[string]graph.EffectSet
	effects map[graph.Effect]struct{}
	calls   []ContractToolCall
}

type contractMockKind struct {
	spec    stepkind.StepKindSpec
	catalog *contractMockCatalog
}

type contractPreparedMockKind struct{ *contractMockKind }

func contractMockRegistry(kinds stepkind.Registry, workflow graph.Graph, contractCase WorkflowContractCase) (*stepkind.MemoryRegistry, *contractMockCatalog, error) {
	catalog := &contractMockCatalog{mocks: make(map[string]ContractExecutorMock, len(contractCase.Mocks)), nodes: make(map[string]graph.EffectSet, len(workflow.Nodes)), effects: make(map[graph.Effect]struct{})}
	for _, mock := range contractCase.Mocks {
		catalog.mocks[mock.NodeID] = mock
	}
	registry := stepkind.NewRegistry()
	registered := make(map[string]struct{})
	for _, node := range workflow.Nodes {
		_, spec, err := stepkind.Resolve(kinds, node.Kind, node.KindVersion)
		if err != nil {
			return nil, nil, err
		}
		catalog.nodes[node.ID] = sortedEffects(append(append(graph.EffectSet(nil), spec.Effects...), node.Effects...))
		key := spec.Name + "\x00" + spec.Version
		if _, exists := registered[key]; exists {
			continue
		}
		base := &contractMockKind{spec: spec, catalog: catalog}
		var controlled stepkind.StepKind = base
		if spec.Lifecycle.Prepare {
			controlled = &contractPreparedMockKind{contractMockKind: base}
		}
		if err := registry.Register(controlled); err != nil {
			return nil, nil, err
		}
		registered[key] = struct{}{}
	}
	return registry, catalog, nil
}

func (k *contractMockKind) Spec() stepkind.StepKindSpec { return k.spec }

func (k *contractMockKind) ValidateConfig(context.Context, graph.Config) []diagnostic.Diagnostic {
	return nil
}

// Prepare preserves a registered kind's declared lifecycle contract while
// keeping qualification execution fully controlled by the literal mock.
func (k *contractPreparedMockKind) Prepare(_ context.Context, invocation stepkind.Invocation) (stepkind.PreparedInvocation, error) {
	return stepkind.PreparedInvocation{Invocation: invocation}, nil
}

func (k *contractMockKind) Execute(ctx context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	if err := ctx.Err(); err != nil {
		return stepkind.StepResult{}, err
	}
	invocation := prepared.Invocation
	k.catalog.mu.Lock()
	defer k.catalog.mu.Unlock()
	mock, ok := k.catalog.mocks[invocation.Identity.NodeID]
	if !ok || mock.Kind != k.spec.Name || mock.KindVersion != k.spec.Version {
		return stepkind.StepResult{}, contractMockFailure("contract_mock_missing", "controlled executor mock is unavailable")
	}
	if !reflectContractJSONEqual(invocation.Config, mock.ExpectedConfig) {
		return stepkind.StepResult{}, contractMockFailure("contract_mock_config", "bound executor config differs from the contract")
	}
	result, ok := contractMockResult(mock.Results, invocation.Identity.Iteration, invocation.Identity.Attempt)
	if !ok {
		return stepkind.StepResult{}, contractMockFailure("contract_mock_result", "controlled result is unavailable for the invocation identity")
	}
	expectedInputs := mock.ExpectedInputs
	if result.ExpectedInputs != nil {
		expectedInputs = *result.ExpectedInputs
	}
	if !reflectContractJSONEqual(invocation.Inputs, expectedInputs) {
		return stepkind.StepResult{}, contractMockFailure("contract_mock_inputs", "bound executor inputs differ from the contract")
	}
	for _, effect := range k.catalog.nodes[invocation.Identity.NodeID] {
		k.catalog.effects[effect] = struct{}{}
	}
	for _, call := range result.Calls {
		cloned, err := cloneContractCall(call)
		if err != nil {
			return stepkind.StepResult{}, contractMockFailure("contract_mock_call", "literal call evidence is not JSON-compatible")
		}
		if invocation.Activity != nil {
			if err := invocation.Activity.RecordToolCall(ctx, verification.ToolCall{Server: call.Kind, Tool: call.Name, Outcome: call.Outcome}); err != nil {
				return stepkind.StepResult{}, contractMockFailure("contract_mock_activity", "literal call evidence could not be recorded")
			}
		}
		k.catalog.calls = append(k.catalog.calls, cloned)
	}
	if result.Failure != nil {
		failureCopy := *result.Failure
		failureCopy.Details = cloneContractStrings(result.Failure.Details)
		return stepkind.StepResult{}, &failureCopy
	}
	outputs, err := cloneContractValueSet(result.Outputs)
	if err != nil {
		return stepkind.StepResult{}, contractMockFailure("contract_mock_output", "controlled output is not JSON-compatible")
	}
	return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: outputs}, nil
}

func contractMockResult(results []ContractMockResult, iteration string, attempt int) (ContractMockResult, bool) {
	for _, result := range results {
		if result.Iteration == iteration && result.Attempt == attempt {
			return result, true
		}
	}
	return ContractMockResult{}, false
}

func (c *contractMockCatalog) observedEffects() graph.EffectSet {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make(graph.EffectSet, 0, len(c.effects))
	for effect := range c.effects {
		result = append(result, effect)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func (c *contractMockCatalog) observedCalls() []ContractToolCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.calls) == 0 {
		return nil
	}
	result := make([]ContractToolCall, len(c.calls))
	for index, call := range c.calls {
		result[index], _ = cloneContractCall(call)
	}
	return result
}

func contractResult(expected WorkflowContractCase, observed contractObservation) ContractCaseResult {
	result := ContractCaseResult{Name: expected.Name, Effects: observed.Effects, Calls: observed.Calls}
	if !reflectContractJSONEqual(observed.Effects, expected.ExpectedEffects) {
		result.Message = "observed effects differ from the contract"
		return result
	}
	if !reflectContractJSONEqual(observed.Calls, expected.ExpectedCalls) {
		result.Message = "observed literal calls differ from the contract"
		return result
	}
	if expected.ExpectedError != nil {
		result.Failure = observed.Failure
		if observed.Failure == nil || observed.Failure.Code != expected.ExpectedError.Code || expected.ExpectedError.Message != "" && observed.Failure.Message != expected.ExpectedError.Message {
			result.Message = "observed terminal error differs from the contract"
			return result
		}
		result.Passed = true
		return result
	}
	if observed.Failure != nil {
		result.Failure = observed.Failure
		result.Message = "workflow failed while outputs were expected"
		return result
	}
	digest, err := values.DigestValueSet(observed.Outputs)
	if err != nil {
		result.Message = "workflow outputs could not be digested"
		return result
	}
	result.OutputDigest = digest
	if !reflectContractJSONEqual(observed.Outputs, expected.ExpectedOutputs) {
		result.Message = "workflow outputs differ from the contract"
		return result
	}
	result.Passed = true
	return result
}

func diagnosticObservation(findings []diagnostic.Diagnostic) contractObservation {
	if len(findings) == 0 {
		return contractObservation{Failure: &ContractExpectedError{Code: "contract_binding_failed", Message: "workflow input binding failed"}}
	}
	return contractObservation{Failure: &ContractExpectedError{Code: string(findings[0].Code), Message: findings[0].Message}}
}

type contractDiagnosticError struct{ diagnostics []diagnostic.Diagnostic }

func (e *contractDiagnosticError) Error() string { return "workflow output finalization failed" }

func contractMockFailure(code, message string) *stepkind.ExecutionError {
	return &stepkind.ExecutionError{Code: code, Message: message, Classification: stepkind.RetryPermanent}
}

func sortedGraphNodes(input []graph.Node) []graph.Node {
	result := append([]graph.Node(nil), input...)
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func graphNodeByID(workflow graph.Graph, nodeID string) (graph.Node, bool) {
	for _, node := range workflow.Nodes {
		if node.ID == nodeID {
			return node, true
		}
	}
	return graph.Node{}, false
}

func exactContractNode(registry stepkind.Registry, node graph.Node) (graph.Node, error) {
	_, spec, err := stepkind.Resolve(registry, node.Kind, node.KindVersion)
	if err != nil {
		return graph.Node{}, err
	}
	node.Kind, node.KindVersion = spec.Name, spec.Version
	return node, nil
}

func hardContractFailure(status runtime.NodeStatus) bool {
	switch status {
	case runtime.NodeFailed, runtime.NodeTimedOut, runtime.NodeCanceled, runtime.NodeCrashed:
		return true
	default:
		return false
	}
}

func planUsesVerification(workflow graph.Graph) bool {
	for _, node := range workflow.Nodes {
		if node.Verification != nil {
			return true
		}
	}
	return false
}

func cloneContractCall(input ContractToolCall) (ContractToolCall, error) {
	encoded, err := canonicalJSON(input)
	if err != nil {
		return ContractToolCall{}, err
	}
	var result ContractToolCall
	if err := decodeContractJSON(encoded, &result); err != nil {
		return ContractToolCall{}, err
	}
	return result, nil
}

func cloneContractValueSet(input values.ValueSet) (values.ValueSet, error) {
	encoded, err := canonicalJSON(input)
	if err != nil {
		return nil, err
	}
	var result values.ValueSet
	if err := decodeContractJSON(encoded, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func cloneContractStrings(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func decodeContractJSON(encoded []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("contract JSON has trailing data")
	}
	return nil
}
