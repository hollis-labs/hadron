package compile

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	// CodeVerificationExpressionExtraction identifies a structured verifier
	// expression-extractor failure that omitted its own valid diagnostic code.
	CodeVerificationExpressionExtraction diagnostic.Code = "HADR-SOURCE-028"

	// CodeUnknownValueProducer identifies a static step reference whose producer
	// does not exist in the graph.
	CodeUnknownValueProducer diagnostic.Code = "HADR-REF-006"
	// CodeUnavailableValueReference identifies a value root that is unavailable
	// at the consumer's evaluation phase.
	CodeUnavailableValueReference diagnostic.Code = "HADR-REF-007"
)

// ValueConsumerKind identifies the evaluation scope that consumes a reference.
type ValueConsumerKind string

const (
	// ValueConsumerNode is an executable node invocation.
	ValueConsumerNode ValueConsumerKind = "node"
	// ValueConsumerWorkflowOutput is a workflow output evaluated after graph work.
	ValueConsumerWorkflowOutput ValueConsumerKind = "workflow_output"
	// ValueConsumerActivation is a pre-run activation declaration.
	ValueConsumerActivation ValueConsumerKind = "activation"
)

// DeferredDependencyReason explains why a dependency must be resolved or
// checked at runtime instead of becoming a statically complete data edge.
type DeferredDependencyReason string

const (
	// DeferredDynamicStep is a computed steps[...] lookup or root-only steps map
	// access. It may observe only producers already visible through explicit or
	// separately inferred edges.
	DeferredDynamicStep DeferredDependencyReason = "dynamic_step"
	// DeferredFanOutItem is the invocation-local item root.
	DeferredFanOutItem DeferredDependencyReason = "fan_out_item"
	// DeferredFanOutIndex is the invocation-local index root.
	DeferredFanOutIndex DeferredDependencyReason = "fan_out_index"
	// DeferredOptionalProducer is a producer that may not materialize because it
	// is conditional, branch-selected, or fan-out shaped.
	DeferredOptionalProducer DeferredDependencyReason = "optional_producer"
	// DeferredOpaqueVerification records verifier config that core cannot inspect
	// without an extractor registered for that verifier kind.
	DeferredOpaqueVerification DeferredDependencyReason = "opaque_verification"
)

// ValueConsumer identifies one graph location that consumes expressions.
// Surface is a stable semantic path such as "with.payload" or
// "verify.checks[0].expression[0]".
type ValueConsumer struct {
	Kind    ValueConsumerKind `json:"kind"`
	ID      string            `json:"id"`
	Surface string            `json:"surface"`
	Source  *graph.SourceRef  `json:"source,omitempty"`
}

// DeferredDependency retains a dependency whose producer or invocation-local
// value cannot be fully selected at compile time. Reference is nil only for
// opaque verification config that had no registered extractor.
type DeferredDependency struct {
	Consumer   ValueConsumer            `json:"consumer"`
	Expression *graph.Expression        `json:"expression,omitempty"`
	Reference  *values.Reference        `json:"reference,omitempty"`
	ProducerID string                   `json:"producer_id,omitempty"`
	Reason     DeferredDependencyReason `json:"reason"`
}

// ValueScope is the direct value visibility of one evaluation scope.
// Producers never includes transitive or merely completed graph nodes.
type ValueScope struct {
	Producers []string `json:"producers"`
	FanOut    bool     `json:"fan_out,omitempty"`
}

// ValueVisibilityPlan is the binder artifact produced beside an inferred
// execution plan. Nodes contains every normalized node ID, including nodes
// with an empty producer set.
type ValueVisibilityPlan struct {
	Nodes           map[string]ValueScope `json:"nodes"`
	WorkflowOutputs ValueScope            `json:"workflow_outputs"`
	Activations     map[string]ValueScope `json:"activations,omitempty"`
}

// ScopeNodeContext filters an available runtime context to a node's direct
// explicit-plus-inferred producers. base is copied so host policy such as
// AllowEnv is preserved; VisibleSteps is replaced by the compiler allowlist.
// Fan-out item and index roots are retained only for fan-out node invocations.
func (p ValueVisibilityPlan) ScopeNodeContext(
	nodeID string,
	available values.ExpressionContext,
	base values.ExpressionOptions,
) (values.ExpressionContext, values.ExpressionOptions, error) {
	identity := graph.NormalizeID(nodeID)
	scope, ok := p.Nodes[identity]
	if !ok {
		return values.ExpressionContext{}, base, fmt.Errorf("value visibility scope for node %q is not present", nodeID)
	}
	scoped, options := scopeExpressionContext(scope, available, base, scope.FanOut)
	return scoped, options, nil
}

// VerificationExpressionExtractor gives one registered verifier kind sole
// authority to expose expression-bearing fields from its opaque config.
// Diagnostics should be structured; omitted sources are assigned to the
// verification check carrier by InferValueDependencies.
type VerificationExpressionExtractor interface {
	ExtractVerificationExpressions(check graph.VerificationCheck) ([]graph.Expression, []diagnostic.Diagnostic)
}

// VerificationExpressionExtractorFunc adapts a function to an extractor.
type VerificationExpressionExtractorFunc func(graph.VerificationCheck) ([]graph.Expression, []diagnostic.Diagnostic)

// ExtractVerificationExpressions implements VerificationExpressionExtractor.
func (f VerificationExpressionExtractorFunc) ExtractVerificationExpressions(check graph.VerificationCheck) ([]graph.Expression, []diagnostic.Diagnostic) {
	return f(check)
}

// DependencyOptions supplies expression ownership for opaque verifier config.
// Map keys are exact VerificationCheck.Kind values. Unknown kinds remain
// deferred and are never inspected lexically by core.
type DependencyOptions struct {
	VerificationExtractors map[string]VerificationExpressionExtractor
}

// DependencyResult is the outcome of dependency inference. Plan is nil when
// any error diagnostic exists. Visibility and Deferred remain available for
// deterministic diagnostics, tooling, and conformance inspection.
type DependencyResult struct {
	Plan        *ExecutionPlan          `json:"plan,omitempty"`
	Visibility  ValueVisibilityPlan     `json:"visibility"`
	Deferred    []DeferredDependency    `json:"deferred,omitempty"`
	Diagnostics []diagnostic.Diagnostic `json:"diagnostics,omitempty"`
}

// InferValueDependencies parses expression references without evaluating
// them, adds deterministic data edges to a cloned plan, and derives direct
// invocation visibility. It then re-runs only topology and node-shape
// validation; registered-kind, policy, and definition validation remain the
// caller's separate W01 validation phase.
func InferValueDependencies(plan *ExecutionPlan, options DependencyOptions) DependencyResult {
	if plan == nil {
		return DependencyResult{Diagnostics: []diagnostic.Diagnostic{{
			Severity: diagnostic.SeverityError,
			Code:     CodeInvalidValidationInput,
			Message:  "execution plan is required for value dependency inference",
			Remediation: &diagnostic.Remediation{
				Message: "Compile a workflow source successfully before inferring value dependencies.",
			},
		}}}
	}

	cloned, err := cloneExecutionPlan(*plan)
	if err != nil {
		return DependencyResult{Diagnostics: []diagnostic.Diagnostic{{
			Severity: diagnostic.SeverityError,
			Code:     CodeInvalidWorkflowShape,
			Message:  "execution plan cannot be cloned for value dependency inference",
			Source:   cloneSource(graphSource(plan.Graph)),
			Remediation: &diagnostic.Remediation{
				Message: "Keep graph config, metadata, schemas, and literal bindings JSON-compatible.",
			},
		}}}
	}

	analyzer := newDependencyAnalyzer(&cloned, options)
	analyzer.walk()
	analyzer.addInferredEdges()
	analyzer.diagnostics = append(analyzer.diagnostics, validateValueDependencyStructure(cloned.Graph)...)
	sortDiagnostics(analyzer.diagnostics)
	analyzer.sortDeferred()
	visibility := analyzer.visibilityPlan()

	result := DependencyResult{
		Visibility:  visibility,
		Deferred:    append([]DeferredDependency(nil), analyzer.deferred...),
		Diagnostics: append([]diagnostic.Diagnostic(nil), analyzer.diagnostics...),
	}
	if hasErrorDiagnostics(result.Diagnostics) {
		return result
	}

	graphDigest, err := digestGraph(cloned.Graph)
	if err != nil {
		result.Diagnostics = append(result.Diagnostics, dependencyInternalDiagnostic(cloned.Graph, "compute inferred graph digest", err))
		return result
	}
	cloned.Graph.Digest = graphDigest
	planDigest, err := digestPlan(cloned)
	if err != nil {
		result.Diagnostics = append(result.Diagnostics, dependencyInternalDiagnostic(cloned.Graph, "compute inferred plan digest", err))
		return result
	}
	cloned.Digest = planDigest
	result.Plan = &cloned
	return result
}

type mutableValueScope struct {
	producers map[string]struct{}
	fanOut    bool
}

type inferredEdge struct {
	from    string
	to      string
	source  *graph.SourceRef
	surface string
}

type expressionUse struct {
	consumer          ValueConsumer
	expression        graph.Expression
	scope             *mutableValueScope
	nodeID            string
	allowSteps        bool
	allowLocals       bool
	allowOutputs      bool
	allowCompensation bool
	addEdge           bool
}

type dependencyAnalyzer struct {
	plan                 *ExecutionPlan
	options              DependencyOptions
	nodes                map[string]graph.Node
	optional             map[string]bool
	nodeScopes           map[string]*mutableValueScope
	outputScope          *mutableValueScope
	activations          map[string]*mutableValueScope
	compensationHandlers map[string]struct{}
	inferred             []inferredEdge
	deferred             []DeferredDependency
	diagnostics          []diagnostic.Diagnostic
}

func newDependencyAnalyzer(plan *ExecutionPlan, options DependencyOptions) *dependencyAnalyzer {
	a := &dependencyAnalyzer{
		plan:                 plan,
		options:              options,
		nodes:                make(map[string]graph.Node, len(plan.Graph.Nodes)),
		optional:             make(map[string]bool, len(plan.Graph.Nodes)),
		nodeScopes:           make(map[string]*mutableValueScope, len(plan.Graph.Nodes)),
		outputScope:          newMutableValueScope(false),
		activations:          make(map[string]*mutableValueScope, len(plan.Graph.Activations)),
		compensationHandlers: make(map[string]struct{}),
	}
	for _, node := range plan.Graph.Nodes {
		identity := graph.NormalizeID(node.ID)
		a.nodes[identity] = node
		a.nodeScopes[identity] = newMutableValueScope(node.ForEach != nil)
		if node.If != nil || node.ForEach != nil {
			a.optional[identity] = true
		}
	}
	for _, node := range plan.Graph.Nodes {
		if node.Compensation != nil {
			a.compensationHandlers[graph.NormalizeID(node.Compensation.Handler)] = struct{}{}
		}
	}
	for _, node := range plan.Graph.Nodes {
		if node.Switch != nil {
			for _, arm := range node.Switch.Arms {
				for _, target := range arm.Targets {
					a.optional[graph.NormalizeID(target)] = true
				}
			}
			for _, target := range node.Switch.Default {
				a.optional[graph.NormalizeID(target)] = true
			}
		}
		for _, catch := range node.Catch {
			for _, target := range catch.Targets {
				a.optional[graph.NormalizeID(target)] = true
			}
		}
	}
	for _, activation := range plan.Graph.Activations {
		a.activations[graph.NormalizeID(activation.ID)] = newMutableValueScope(false)
	}
	a.addExplicitVisibility()
	return a
}

func newMutableValueScope(fanOut bool) *mutableValueScope {
	return &mutableValueScope{producers: make(map[string]struct{}), fanOut: fanOut}
}

func (a *dependencyAnalyzer) addExplicitVisibility() {
	for _, node := range a.plan.Graph.Nodes {
		to := graph.NormalizeID(node.ID)
		scope := a.nodeScopes[to]
		for _, need := range node.Needs {
			from := graph.NormalizeID(need.Node)
			if _, exists := a.nodes[from]; exists {
				scope.producers[from] = struct{}{}
			}
		}
	}
	for _, edge := range a.plan.Graph.Edges {
		from, to := graph.NormalizeID(edge.From), graph.NormalizeID(edge.To)
		if _, exists := a.nodes[from]; !exists {
			continue
		}
		if scope, exists := a.nodeScopes[to]; exists {
			scope.producers[from] = struct{}{}
		}
	}
}

func (a *dependencyAnalyzer) walk() {
	for _, node := range a.plan.Graph.Nodes {
		a.walkNode(node)
	}
	for _, output := range a.plan.Graph.Outputs {
		if output.Value == nil {
			continue
		}
		a.walkBinding(*output.Value, ValueConsumer{
			Kind: ValueConsumerWorkflowOutput, ID: output.Name,
			Surface: "workflow.outputs." + output.Name,
			Source:  firstSource(output.Value.Source, output.Source, graphSource(a.plan.Graph)),
		}, a.outputScope, "", true, false, false, false, false)
	}
	for _, activation := range a.plan.Graph.Activations {
		a.walkActivation(activation)
	}
}

func (a *dependencyAnalyzer) walkNode(node graph.Node) {
	identity := graph.NormalizeID(node.ID)
	scope := a.nodeScopes[identity]
	fallback := a.sourceForNode(node)

	bindingNames := sortedBindingNames(node.InputBindings)
	_, compensationHandler := a.compensationHandlers[identity]
	for _, name := range bindingNames {
		binding := node.InputBindings[name]
		a.walkBinding(binding, ValueConsumer{
			Kind: ValueConsumerNode, ID: identity, Surface: "with." + name,
			Source: firstSource(binding.Source, fallback),
		}, scope, identity, !compensationHandler, node.ForEach != nil, false, compensationHandler, !compensationHandler)
	}
	if node.If != nil {
		a.walkExpression(expressionUseForNode(node, identity, "if", *node.If, scope, fallback, true))
	}
	if node.ForEach != nil {
		use := expressionUseForNode(node, identity, "for_each.items", node.ForEach.Items, scope, fallback, false)
		use.allowLocals = false
		a.walkExpression(use)
	}
	if node.Switch != nil {
		for i, arm := range node.Switch.Arms {
			surface := fmt.Sprintf("switch.arms[%d].when", i)
			a.walkExpression(expressionUseForNode(node, identity, surface, arm.When, scope, firstSource(arm.Source, fallback), true))
		}
	}
	for i, catch := range node.Catch {
		if catch.When == nil {
			continue
		}
		surface := fmt.Sprintf("catch[%d].when", i)
		a.walkExpression(expressionUseForNode(node, identity, surface, *catch.When, scope, firstSource(catch.Source, fallback), true))
	}
	if node.Kind == "transform" {
		a.walkTransformConfig(node, identity, scope, fallback)
	}
	for _, output := range node.Outputs {
		if output.Value == nil {
			continue
		}
		a.walkBinding(*output.Value, ValueConsumer{
			Kind: ValueConsumerNode, ID: identity, Surface: "outputs." + output.Name,
			Source: firstSource(output.Value.Source, output.Source, fallback),
		}, scope, identity, false, node.ForEach != nil, true, false, false)
	}
	if node.Idempotency != nil && node.Idempotency.Key != nil {
		a.walkExpression(expressionUseForNode(node, identity, "idempotency.key", *node.Idempotency.Key, scope, fallback, true))
	}
	if node.Memoization != nil {
		a.walkExpression(expressionUseForNode(node, identity, "memoize.key", node.Memoization.Key, scope, fallback, true))
	}
	a.walkVerification(node, identity, "verify", node.Verification, scope, fallback)
	if node.Service != nil {
		a.walkVerification(node, identity, "service.ready_check", node.Service.ReadyCheck, scope, fallback)
	}
}

func expressionUseForNode(node graph.Node, identity, surface string, expression graph.Expression, scope *mutableValueScope, fallback *graph.SourceRef, allowLocals bool) expressionUse {
	if expression.Source == nil {
		expression.Source = cloneSource(fallback)
	}
	return expressionUse{
		consumer: ValueConsumer{
			Kind: ValueConsumerNode, ID: identity, Surface: surface,
			Source: cloneSource(expression.Source),
		},
		expression: expression, scope: scope, nodeID: identity,
		allowSteps: true, allowLocals: node.ForEach != nil && allowLocals, addEdge: true,
	}
}

func (a *dependencyAnalyzer) walkBinding(binding graph.Binding, consumer ValueConsumer, scope *mutableValueScope, nodeID string, allowSteps, allowLocals, allowOutputs, allowCompensation, addEdge bool) {
	source := firstSource(binding.Source, consumer.Source)
	consumer.Source = cloneSource(source)
	switch binding.Kind {
	case graph.BindingLiteral:
		// Literal bindings contain no expression references.
	case graph.BindingExpression:
		if binding.Expression == nil {
			return
		}
		expression := *binding.Expression
		if expression.Source == nil {
			expression.Source = cloneSource(source)
		}
		consumer.Source = cloneSource(expression.Source)
		a.walkExpression(expressionUse{
			consumer: consumer, expression: expression, scope: scope, nodeID: nodeID,
			allowSteps: allowSteps, allowLocals: allowLocals, allowOutputs: allowOutputs, allowCompensation: allowCompensation, addEdge: addEdge,
		})
	case graph.BindingInterpolation:
		expression := graph.Expression{Text: binding.Interpolation, Source: cloneSource(source)}
		references, err := values.ParseInterpolationReferences(binding.Interpolation, source)
		use := expressionUse{
			consumer: consumer, expression: expression, scope: scope, nodeID: nodeID,
			allowSteps: allowSteps, allowLocals: allowLocals, allowOutputs: allowOutputs, allowCompensation: allowCompensation, addEdge: addEdge,
		}
		if err != nil {
			a.addExpressionError(use, err)
			return
		}
		a.walkReferences(use, references)
	}
}

func (a *dependencyAnalyzer) walkExpression(use expressionUse) {
	if use.expression.Source == nil {
		use.expression.Source = cloneSource(use.consumer.Source)
	}
	use.consumer.Source = cloneSource(use.expression.Source)
	references, err := values.ParseReferences(use.expression)
	if err != nil {
		a.addExpressionError(use, err)
		return
	}
	a.walkReferences(use, references)
}

func (a *dependencyAnalyzer) walkReferences(use expressionUse, references []values.Reference) {
	for _, reference := range references {
		switch reference.Root {
		case "steps":
			a.walkStepReference(use, reference)
		case "item", "index":
			a.walkLocalReference(use, reference)
		case "outputs":
			if !use.allowOutputs {
				a.addUnavailableReference(use, reference, "raw adapter outputs exist only while projecting a node's declared outputs",
					"Use outputs only in a node output value binding; use steps.<node>.outputs from downstream or workflow outputs.")
			}
		case "compensation":
			if !use.allowCompensation {
				a.addUnavailableReference(use, reference, "compensation evidence exists only while binding a dormant compensation handler",
					"Use compensation only in a dormant handler's with bindings.")
			}
		}
	}
}

func (a *dependencyAnalyzer) walkStepReference(use expressionUse, reference values.Reference) {
	if !use.allowSteps {
		a.addUnavailableReference(use, reference, "activation expressions are evaluated before step results exist",
			"Use workflow inputs or runtime roots in activation bindings, and move step-result logic into a node.")
		return
	}
	if reference.Dynamic {
		a.addDeferred(use, &reference, "", DeferredDynamicStep)
		return
	}
	if len(reference.Path) == 0 {
		a.addDeferred(use, &reference, "", DeferredDynamicStep)
		return
	}

	producerID := reference.Path[0]
	producer, exists := a.nodes[producerID]
	if !exists {
		a.diagnostics = append(a.diagnostics, diagnostic.Diagnostic{
			Severity: diagnostic.SeverityError,
			Code:     CodeUnknownValueProducer,
			Message:  fmt.Sprintf("%s %q references unknown step producer %q in %s", use.consumer.Kind, use.consumer.ID, producerID, use.consumer.Surface),
			Source:   cloneSource(use.consumer.Source),
			Remediation: &diagnostic.Remediation{
				Message: fmt.Sprintf("Correct the reference to an existing normalized node ID or declare producer %q.", producerID),
			},
		})
		return
	}
	producerID = graph.NormalizeID(producer.ID)
	if _, dormant := a.compensationHandlers[producerID]; dormant {
		a.diagnostics = append(a.diagnostics, diagnostic.Diagnostic{
			Severity: diagnostic.SeverityError,
			Code:     CodeInvalidCompensation,
			Message:  fmt.Sprintf("%s %q cannot reference dormant compensation handler %q in %s", use.consumer.Kind, use.consumer.ID, producerID, use.consumer.Surface),
			Source:   cloneSource(use.consumer.Source),
			Remediation: &diagnostic.Remediation{
				Message: "Keep compensation handlers graph-dormant and consume their results only through bounded compensation inspection.",
			},
		})
		return
	}
	use.scope.producers[producerID] = struct{}{}
	if use.addEdge {
		a.inferred = append(a.inferred, inferredEdge{
			from: producerID, to: use.nodeID, source: cloneSource(use.consumer.Source), surface: use.consumer.Surface,
		})
	}
	if a.optional[producerID] {
		a.addDeferred(use, &reference, producerID, DeferredOptionalProducer)
	}
}

func (a *dependencyAnalyzer) walkLocalReference(use expressionUse, reference values.Reference) {
	if !use.allowLocals {
		phase := "this expression is evaluated outside a fan-out item invocation"
		if use.consumer.Surface == "for_each.items" {
			phase = "for_each.items is evaluated before item and index exist"
		}
		a.addUnavailableReference(use, reference, phase,
			"Reference workflow inputs or upstream step outputs when selecting fan-out items; use item and index only inside each expanded invocation.")
		return
	}
	reason := DeferredFanOutItem
	if reference.Root == "index" {
		reason = DeferredFanOutIndex
	}
	a.addDeferred(use, &reference, "", reason)
}

func (a *dependencyAnalyzer) addDeferred(use expressionUse, reference *values.Reference, producerID string, reason DeferredDependencyReason) {
	var clonedReference *values.Reference
	if reference != nil {
		copyOfReference := *reference
		copyOfReference.Path = append([]string(nil), reference.Path...)
		clonedReference = &copyOfReference
	}
	expression := use.expression
	expression.Source = cloneSource(expression.Source)
	a.deferred = append(a.deferred, DeferredDependency{
		Consumer:   cloneConsumer(use.consumer),
		Expression: &expression,
		Reference:  clonedReference,
		ProducerID: producerID,
		Reason:     reason,
	})
}

func (a *dependencyAnalyzer) addUnavailableReference(use expressionUse, reference values.Reference, reason, remediation string) {
	a.diagnostics = append(a.diagnostics, diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError,
		Code:     CodeUnavailableValueReference,
		Message: fmt.Sprintf("%s %q cannot use %s in %s: %s",
			use.consumer.Kind, use.consumer.ID, formatReference(reference), use.consumer.Surface, reason),
		Source: cloneSource(use.consumer.Source),
		Remediation: &diagnostic.Remediation{
			Message: remediation,
		},
	})
}

func (a *dependencyAnalyzer) addExpressionError(use expressionUse, err error) {
	var expressionErr *values.ExpressionError
	if errors.As(err, &expressionErr) {
		finding := expressionErr.Diagnostic
		finding.Message = fmt.Sprintf("%s %q has an invalid dependency expression in %s: %s",
			use.consumer.Kind, use.consumer.ID, use.consumer.Surface, finding.Message)
		if finding.Source == nil {
			finding.Source = cloneSource(use.consumer.Source)
		}
		if finding.Remediation == nil {
			finding.Remediation = &diagnostic.Remediation{Message: "Correct the expression syntax before inferring value dependencies."}
		}
		a.diagnostics = append(a.diagnostics, finding)
		return
	}
	a.diagnostics = append(a.diagnostics, diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError,
		Code:     values.CodeExpressionSyntax,
		Message:  fmt.Sprintf("%s %q has an invalid dependency expression in %s", use.consumer.Kind, use.consumer.ID, use.consumer.Surface),
		Source:   cloneSource(use.consumer.Source),
		Remediation: &diagnostic.Remediation{
			Message: "Correct the expression syntax before inferring value dependencies.",
		},
	})
}

func (a *dependencyAnalyzer) walkTransformConfig(node graph.Node, identity string, scope *mutableValueScope, fallback *graph.SourceRef) {
	walkConfigStrings(node.Config, nil, func(path []string, text string) {
		surface := "config." + strings.Join(path, ".")
		source := sourceWithPath(fallback, append([]string{"config"}, path...)...)
		a.walkExpression(expressionUseForNode(
			node, identity, surface, graph.Expression{Text: text, Source: source}, scope, fallback, true,
		))
	})
}

func (a *dependencyAnalyzer) walkVerification(node graph.Node, identity, surface string, spec *graph.VerificationSpec, scope *mutableValueScope, fallback *graph.SourceRef) {
	if spec == nil {
		return
	}
	for i, check := range spec.Checks {
		checkSurface := fmt.Sprintf("%s.checks[%d]", surface, i)
		checkSource := firstSource(check.Source, fallback)
		extractor, ok := a.options.VerificationExtractors[check.Kind]
		if !ok || isNilInterface(extractor) {
			if len(check.Config) != 0 {
				a.deferred = append(a.deferred, DeferredDependency{
					Consumer: cloneConsumer(ValueConsumer{
						Kind: ValueConsumerNode, ID: identity, Surface: checkSurface + ".config",
						Source: cloneSource(checkSource),
					}),
					Reason: DeferredOpaqueVerification,
				})
			}
			continue
		}

		expressions, findings := extractor.ExtractVerificationExpressions(check)
		for _, finding := range findings {
			a.diagnostics = append(a.diagnostics, normalizeFinding(
				finding,
				checkSource,
				CodeVerificationExpressionExtraction,
				fmt.Sprintf("verifier %q could not expose dependency expressions", check.Kind),
				"Correct the verifier config using the registered verifier schema.",
			))
		}
		for j, expression := range expressions {
			if expression.Source == nil {
				expression.Source = cloneSource(checkSource)
			}
			expressionSurface := fmt.Sprintf("%s.expression[%d]", checkSurface, j)
			a.walkExpression(expressionUseForNode(node, identity, expressionSurface, expression, scope, checkSource, true))
		}
	}
}

func (a *dependencyAnalyzer) walkActivation(activation graph.ActivationDeclaration) {
	identity := graph.NormalizeID(activation.ID)
	scope := a.activations[identity]
	if scope == nil {
		scope = newMutableValueScope(false)
		a.activations[identity] = scope
	}
	names := sortedBindingNames(activation.Inputs)
	for _, name := range names {
		binding := activation.Inputs[name]
		a.walkBinding(binding, ValueConsumer{
			Kind: ValueConsumerActivation, ID: identity, Surface: "inputs." + name,
			Source: firstSource(binding.Source, activation.Source, graphSource(a.plan.Graph)),
		}, scope, "", false, false, false, false, false)
	}
	if activation.Policy.DeduplicationKey != nil {
		expression := *activation.Policy.DeduplicationKey
		if expression.Source == nil {
			expression.Source = firstSource(activation.Source, graphSource(a.plan.Graph))
		}
		a.walkExpression(expressionUse{
			consumer: ValueConsumer{
				Kind: ValueConsumerActivation, ID: identity, Surface: "policy.deduplication_key",
				Source: cloneSource(expression.Source),
			},
			expression: expression, scope: scope, allowSteps: false,
		})
	}
}

func (a *dependencyAnalyzer) addInferredEdges() {
	sort.SliceStable(a.inferred, func(i, j int) bool {
		left, right := a.inferred[i], a.inferred[j]
		if left.from != right.from {
			return left.from < right.from
		}
		if left.to != right.to {
			return left.to < right.to
		}
		leftSource, rightSource := sourceSortKey(left.source), sourceSortKey(right.source)
		if leftSource != rightSource {
			return leftSource < rightSource
		}
		return left.surface < right.surface
	})

	seenData := make(map[string]struct{})
	for _, edge := range a.plan.Graph.Edges {
		if edge.Kind == graph.EdgeData {
			seenData[graph.NormalizeID(edge.From)+"\x00"+graph.NormalizeID(edge.To)] = struct{}{}
		}
	}
	for _, candidate := range a.inferred {
		key := candidate.from + "\x00" + candidate.to
		if _, exists := seenData[key]; exists {
			continue
		}
		seenData[key] = struct{}{}
		edge := graph.Edge{
			From: candidate.from, To: candidate.to, Kind: graph.EdgeData,
			Source: cloneSource(candidate.source),
		}
		a.plan.Graph.Edges = append(a.plan.Graph.Edges, edge)
		a.recordEdgeSource(candidate.from, candidate.to, candidate.source)
	}
}

func (a *dependencyAnalyzer) recordEdgeSource(from, to string, source *graph.SourceRef) {
	if source == nil {
		return
	}
	key := EdgeSourceKey(from, to, graph.EdgeData)
	if a.plan.SourceMap.Edges == nil {
		a.plan.SourceMap.Edges = make(map[string]graph.SourceRef)
	}
	if a.plan.Graph.SourceMap.Edges == nil {
		a.plan.Graph.SourceMap.Edges = make(map[string]graph.SourceRef)
	}
	a.plan.SourceMap.Edges[key] = *cloneSource(source)
	a.plan.Graph.SourceMap.Edges[key] = *cloneSource(source)
}

func (a *dependencyAnalyzer) visibilityPlan() ValueVisibilityPlan {
	visibility := ValueVisibilityPlan{
		Nodes:       make(map[string]ValueScope, len(a.nodeScopes)),
		Activations: make(map[string]ValueScope, len(a.activations)),
	}
	for identity, scope := range a.nodeScopes {
		visibility.Nodes[identity] = freezeScope(scope)
	}
	visibility.WorkflowOutputs = freezeScope(a.outputScope)
	for identity, scope := range a.activations {
		visibility.Activations[identity] = freezeScope(scope)
	}
	return visibility
}

func (a *dependencyAnalyzer) sortDeferred() {
	sort.SliceStable(a.deferred, func(i, j int) bool {
		return deferredSortKey(a.deferred[i]) < deferredSortKey(a.deferred[j])
	})
}

func (a *dependencyAnalyzer) sourceForNode(node graph.Node) *graph.SourceRef {
	if node.Source != nil {
		return node.Source
	}
	if source, ok := a.plan.Graph.SourceMap.Nodes[node.ID]; ok {
		return &source
	}
	if source, ok := a.plan.SourceMap.Nodes[node.ID]; ok {
		return &source
	}
	return graphSource(a.plan.Graph)
}

func validateValueDependencyStructure(value graph.Graph) []diagnostic.Diagnostic {
	v := validator{graph: value}
	v.validateStructure()
	for _, node := range value.Nodes {
		v.validateNodeShape(node)
	}
	sortDiagnostics(v.diagnostics)
	return append([]diagnostic.Diagnostic(nil), v.diagnostics...)
}

func cloneExecutionPlan(plan ExecutionPlan) (ExecutionPlan, error) {
	encoded, err := json.Marshal(plan)
	if err != nil {
		return ExecutionPlan{}, err
	}
	var cloned ExecutionPlan
	if err := decodeCanonicalJSON(encoded, &cloned); err != nil {
		return ExecutionPlan{}, err
	}
	return cloned, nil
}

func dependencyInternalDiagnostic(value graph.Graph, operation string, err error) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError,
		Code:     CodeInvalidWorkflowShape,
		Message:  operation + ": " + err.Error(),
		Source:   cloneSource(graphSource(value)),
		Remediation: &diagnostic.Remediation{
			Message: "Keep graph config, metadata, schemas, and literal bindings JSON-compatible.",
		},
	}
}

func hasErrorDiagnostics(findings []diagnostic.Diagnostic) bool {
	for _, finding := range findings {
		if finding.Severity == diagnostic.SeverityError {
			return true
		}
	}
	return false
}

func sortedBindingNames(bindings map[string]graph.Binding) []string {
	names := make([]string, 0, len(bindings))
	for name := range bindings {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func walkConfigStrings(value any, path []string, visit func([]string, string)) {
	switch value := value.(type) {
	case string:
		visit(append([]string(nil), path...), value)
	case map[string]any:
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			walkConfigStrings(value[key], appendPathSegment(path, key), visit)
		}
	case graph.Config:
		walkConfigStrings(map[string]any(value), path, visit)
	case []any:
		for i, item := range value {
			walkConfigStrings(item, appendPathSegment(path, strconv.Itoa(i)), visit)
		}
	}
}

func appendPathSegment(path []string, segment string) []string {
	result := make([]string, len(path)+1)
	copy(result, path)
	result[len(path)] = segment
	return result
}

func firstSource(sources ...*graph.SourceRef) *graph.SourceRef {
	for _, source := range sources {
		if source != nil {
			return cloneSource(source)
		}
	}
	return nil
}

func sourceWithPath(source *graph.SourceRef, path ...string) *graph.SourceRef {
	cloned := cloneSource(source)
	if cloned == nil {
		return nil
	}
	cloned.Path = append(cloned.Path, path...)
	return cloned
}

func cloneConsumer(consumer ValueConsumer) ValueConsumer {
	consumer.Source = cloneSource(consumer.Source)
	return consumer
}

func freezeScope(scope *mutableValueScope) ValueScope {
	if scope == nil {
		return ValueScope{Producers: []string{}}
	}
	producers := make([]string, 0, len(scope.producers))
	for producer := range scope.producers {
		producers = append(producers, producer)
	}
	sort.Strings(producers)
	return ValueScope{Producers: producers, FanOut: scope.fanOut}
}

func scopeExpressionContext(scope ValueScope, available values.ExpressionContext, base values.ExpressionOptions, keepLocals bool) (values.ExpressionContext, values.ExpressionOptions) {
	visible := append([]string(nil), scope.Producers...)
	sort.Strings(visible)
	steps := make(map[string]values.StepContext, len(visible))
	for _, producer := range visible {
		if step, ok := available.Steps[producer]; ok {
			steps[producer] = step
		}
	}
	scoped := available
	scoped.Steps = steps
	if !keepLocals {
		scoped.Item = nil
		scoped.Index = nil
	}
	base.VisibleSteps = visible
	return scoped, base
}

func formatReference(reference values.Reference) string {
	if reference.Dynamic {
		return reference.Root + "[...]"
	}
	if len(reference.Path) == 0 {
		return reference.Root
	}
	return reference.Root + "." + strings.Join(reference.Path, ".")
}

func sourceSortKey(source *graph.SourceRef) string {
	if source == nil {
		return "\xff"
	}
	return source.Locator + "\x00" +
		fmt.Sprintf("%010d:%010d:%010d:%010d", source.StartLine, source.StartColumn, source.EndLine, source.EndColumn) + "\x00" +
		strings.Join(source.Path, "\x00")
}

func deferredSortKey(deferred DeferredDependency) string {
	reference := ""
	if deferred.Reference != nil {
		reference = formatReference(*deferred.Reference)
	}
	expression := ""
	if deferred.Expression != nil {
		expression = deferred.Expression.Text
	}
	return sourceSortKey(deferred.Consumer.Source) + "\x00" +
		string(deferred.Consumer.Kind) + "\x00" + deferred.Consumer.ID + "\x00" + deferred.Consumer.Surface + "\x00" +
		string(deferred.Reason) + "\x00" + deferred.ProducerID + "\x00" + reference + "\x00" + expression
}
