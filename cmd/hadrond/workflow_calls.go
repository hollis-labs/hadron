package main

import (
	"context"
	"errors"
	"sync/atomic"

	agentadapter "github.com/hollis-labs/go-workflow/adapters/agent"
	calladapter "github.com/hollis-labs/go-workflow/adapters/call"
	workflowcompile "github.com/hollis-labs/go-workflow/compile"
	"github.com/hollis-labs/go-workflow/diagnostic"
	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/stepkind"
	"github.com/hollis-labs/go-workflow/values"
	"github.com/hollis-labs/hadron/internal/appworkflow"
)

const (
	// capabilityWorkflowCall is call@v1's RequiredCapabilities entry. The call
	// adapter does not export it as a constant.
	capabilityWorkflowCall = "workflow.call"
	// The agent_session@v1 RequiredCapabilities entries. agentadapter does not
	// export them. They are granted to the local target now so enabling the
	// session host later only lifts the agent_session gate below.
	capabilityAgentSessionLaunch  = "agent.session.launch"
	capabilityAgentSessionObserve = "agent.session.observe"
	capabilityAgentSessionCancel  = "agent.session.cancel"

	// agentSessionGateMessage is the stable refusal for any plan that would
	// run agent_session@v1, including the child graph agent_launch expands to.
	agentSessionGateMessage = "agent_session@v1 is not enabled in this hadrond yet: agent launch needs a durable session host (CW-20260930-0227)"
	// inlineCallRefusalMessage is the stable refusal for call@v1 mode inline.
	inlineCallRefusalMessage = "inline calls are not supported by hadrond: use call mode run"

	codeInlineCallUnsupported  = "call_inline_unsupported"
	codeAgentSessionNotEnabled = "agent_session_not_enabled"
)

// productionCallResolver breaks the construction cycle between call@v1 and
// the definition resolver: the resolver freezes its compile kind registry
// (which must contain call@v1) at construction, while call@v1 resolves child
// definitions through that same resolver. It is bound exactly once, before
// the runtime starts dispatching.
type productionCallResolver struct {
	resolver atomic.Pointer[appworkflow.DefinitionResolver]
}

var errCallResolverUnbound = errors.New("workflow call definition resolver is not bound")

func (r *productionCallResolver) bind(resolver *appworkflow.DefinitionResolver) {
	r.resolver.Store(resolver)
}

func (r *productionCallResolver) ResolveDefinition(ctx context.Context, ref graph.DefinitionRef) (workflowcompile.ResolvedDefinition, error) {
	resolver := r.resolver.Load()
	if resolver == nil {
		return workflowcompile.ResolvedDefinition{}, errCallResolverUnbound
	}
	return resolver.ResolveDefinition(ctx, ref)
}

// workflowCallContext defers to the durable call expression context provider,
// which needs the pinned plan source and therefore the definition resolver.
type workflowCallContext struct {
	provider atomic.Pointer[appworkflow.CallExpressionContextProvider]
}

var errCallContextUnbound = errors.New("workflow call expression context is not bound")

func (c *workflowCallContext) bind(provider appworkflow.CallExpressionContextProvider) {
	c.provider.Store(&provider)
}

func (c *workflowCallContext) ExpressionContext(ctx context.Context, invocation stepkind.Invocation) (values.ExpressionContext, values.ExpressionOptions, error) {
	provider := c.provider.Load()
	if provider == nil {
		return values.ExpressionContext{}, values.ExpressionOptions{}, errCallContextUnbound
	}
	return provider.ExpressionContext(ctx, invocation)
}

// productionChildRuns routes call@v1 child-run creation through Host, which
// applies the start policy gate to every call-started child before creating
// it. Host is constructed after call@v1 (it receives the kind), so the
// executor is bound once Host exists.
type productionChildRuns struct {
	host atomic.Pointer[appworkflow.Host]
}

var errChildRunsUnbound = errors.New("workflow child-run executor is not bound")

func (r *productionChildRuns) bind(host *appworkflow.Host) {
	r.host.Store(host)
}

func (r *productionChildRuns) StartChildRun(ctx context.Context, request calladapter.ChildRunRequest) (calladapter.ChildRunResult, error) {
	host := r.host.Load()
	if host == nil {
		return calladapter.ChildRunResult{}, errChildRunsUnbound
	}
	return host.StartChildRun(ctx, request)
}

// refusingInlineCalls satisfies call@v1's required InlineExecutor. hadrond
// does not host inline child graphs; authored inline calls are refused at
// definition validation by productionCallPolicy, so this is the fail-closed
// backstop for inline call nodes reached any other way (for example inside a
// child definition resolved at run time).
type refusingInlineCalls struct{}

func (refusingInlineCalls) ExecuteInline(context.Context, calladapter.InlineRequest) (calladapter.InlineResult, error) {
	return calladapter.InlineResult{}, &stepkind.ExecutionError{
		Code: codeInlineCallUnsupported, Message: inlineCallRefusalMessage, Classification: stepkind.RetryPermanent,
	}
}

// productionCallPolicy refuses authored call@v1 mode inline while the root
// definition is validated, before any run exists.
var productionCallPolicy = workflowcompile.PolicyHookFunc(func(_ context.Context, input workflowcompile.NodeValidation) []diagnostic.Diagnostic {
	if input.Node.Kind != calladapter.KindName || input.Node.Call == nil || input.Node.Call.Mode != graph.CallInline {
		return nil
	}
	return []diagnostic.Diagnostic{{
		Severity: diagnostic.SeverityError, Code: workflowcompile.CodePolicyViolation,
		Message: "call node " + input.Node.ID + ": " + inlineCallRefusalMessage,
		Remediation: &diagnostic.Remediation{
			Message: "Set call.mode to run; the child becomes a separately identified run and a wait_for child_run node can collect its outputs.",
		},
	}}
})

// gatedAgentLaunchExpander keeps the agent_launch source extension's stable
// name but refuses any expansion whose generated child definitions contain
// agent_session@v1. Bundled child graphs are not kind-validated when the root
// plan compiles, so the refusal must happen here to fail before a run starts.
// When a Tether session host is composed, newProductionWorkflowRuntime
// registers tetherAgentLaunchExpander (SourceExpander plus the typed-input
// refusal) instead.
type gatedAgentLaunchExpander struct {
	inner workflowcompile.NodeExpander
}

func newGatedAgentLaunchExpander() gatedAgentLaunchExpander {
	return gatedAgentLaunchExpander{inner: agentadapter.SourceExpander{}}
}

func (e gatedAgentLaunchExpander) Name() string { return e.inner.Name() }

func (e gatedAgentLaunchExpander) ExpandNode(request workflowcompile.NodeExpansionRequest) (workflowcompile.NodeExpansion, bool, []diagnostic.Diagnostic) {
	expansion, handled, findings := e.inner.ExpandNode(request)
	if !handled || len(findings) != 0 {
		return expansion, handled, findings
	}
	for _, definition := range expansion.Definitions {
		for _, node := range definition.Graph.Nodes {
			if node.Kind == agentadapter.KindName {
				return workflowcompile.NodeExpansion{}, true, []diagnostic.Diagnostic{agentSessionGateDiagnostic(request.Node.ID, request.Node.Source)}
			}
		}
	}
	return expansion, handled, findings
}

// gatedAgentSessionKind occupies agent_session@v1 in the compile-only kind
// registry so a directly authored agent_session node fails validation with
// the gate message instead of a generic unknown-kind finding. It is never
// registered with Host, so the runtime cannot dispatch it. It embeds the real
// adapter (bound to a session host with no callbacks, which refuses every
// call) so the registry sees the authentic immutable spec and the
// Observer/Heartbeater/Canceler hooks that spec advertises.
type gatedAgentSessionKind struct {
	*agentadapter.Kind
}

func newGatedAgentSessionKind() (gatedAgentSessionKind, error) {
	kind, err := agentadapter.New(agentadapter.Options{Host: agentadapter.SessionHostFuncs{}})
	if err != nil {
		return gatedAgentSessionKind{}, err
	}
	return gatedAgentSessionKind{Kind: kind}, nil
}

func (gatedAgentSessionKind) ValidateConfig(context.Context, graph.Config) []diagnostic.Diagnostic {
	return []diagnostic.Diagnostic{{
		Severity: diagnostic.SeverityError, Code: stepkind.CodeInvalidConfig, Message: agentSessionGateMessage,
		Remediation: &diagnostic.Remediation{Message: agentSessionGateRemediation},
	}}
}

func (gatedAgentSessionKind) Execute(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	return stepkind.StepResult{}, &stepkind.ExecutionError{
		Code: codeAgentSessionNotEnabled, Message: agentSessionGateMessage, Classification: stepkind.RetryPermanent,
	}
}

const agentSessionGateRemediation = "Remove the agent_launch or agent_session node, or run it on a host with a durable agent session host."

func agentSessionGateDiagnostic(nodeID string, source *graph.SourceRef) diagnostic.Diagnostic {
	finding := diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError, Code: workflowcompile.CodeNodeExpansion,
		Message:     "agent_launch node " + nodeID + ": " + agentSessionGateMessage,
		Remediation: &diagnostic.Remediation{Message: agentSessionGateRemediation},
	}
	if source != nil {
		cloned := *source
		finding.Source = &cloned
	}
	return finding
}

// loadCallLineage returns the authoritative definition lineage for call@v1
// nodes dispatched in runID. The runtime dispatcher refuses a call node
// without it, and call@v1 extends it with the resolved child to enforce the
// cycle and depth policy. A root run's lineage is its own pinned definition
// (identified by semantic graph digest, as the definition resolver reports
// call children); a child run reuses the lineage persisted with its durable
// child-run request, which already ends with the child definition; a replay
// inherits its source run's lineage.
func (w *workflowWorkers) loadCallLineage(ctx context.Context, runID workflowruntime.RunID, seen map[workflowruntime.RunID]struct{}, depth int) ([]graph.DefinitionRef, error) {
	if depth > 64 {
		return nil, errors.New("workflow call lineage exceeds 64")
	}
	if _, duplicate := seen[runID]; duplicate {
		return nil, errors.New("workflow call lineage contains a cycle")
	}
	seen[runID] = struct{}{}
	defer delete(seen, runID)
	start, err := w.roots.LoadStart(ctx, runID)
	if err == nil {
		root, rootErr := rootCallDefinition(start.Record.Plan)
		if rootErr != nil {
			return nil, rootErr
		}
		return []graph.DefinitionRef{root}, nil
	}
	if !errors.Is(err, workflowruntime.ErrNotFound) {
		return nil, err
	}
	child, childErr := w.children.LoadChildRunRequest(ctx, runID)
	if childErr == nil {
		if len(child.Lineage) == 0 {
			return nil, errors.New("durable child-run request has no call lineage")
		}
		return append([]graph.DefinitionRef(nil), child.Lineage...), nil
	}
	if !errors.Is(childErr, workflowruntime.ErrNotFound) {
		return nil, childErr
	}
	replay, replayErr := w.replays.LoadReplayProvenance(ctx, runID)
	if replayErr != nil {
		return nil, replayErr
	}
	return w.loadCallLineage(ctx, replay.SourceRunID, seen, depth+1)
}

func rootCallDefinition(plan workflowcompile.ExecutionPlan) (graph.DefinitionRef, error) {
	digest := plan.Graph.Digest
	if digest == "" {
		computed, err := workflowcompile.GraphDigest(plan.Graph)
		if err != nil {
			return graph.DefinitionRef{}, err
		}
		digest = computed
	}
	definition := plan.Definition
	definition.Digest = digest
	// Provenance describes the selected source bytes; lineage identity is
	// the semantic graph digest alone.
	definition.Provenance = nil
	return definition, nil
}
