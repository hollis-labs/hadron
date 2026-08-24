package compile

import (
	"fmt"
	"sort"
	"strings"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
)

type structuralArc struct {
	from   string
	to     string
	source *graph.SourceRef
}

func (v *validator) validateStructure() {
	known := make(map[string]int, len(v.graph.Nodes))
	for i, node := range v.graph.Nodes {
		identity := graph.NormalizeID(node.ID)
		if first, duplicate := known[identity]; duplicate {
			firstNode := v.graph.Nodes[first]
			v.add(
				CodeDuplicateNodeID,
				v.nodeSource(node),
				fmt.Sprintf("node identity %q collides with %q after normalization to %q", node.ID, firstNode.ID, identity),
				"Give every node a unique identity after lower-case kebab normalization.",
				relatedSource("first declaration of the normalized node identity", v.nodeSource(firstNode))...,
			)
			continue
		}
		known[identity] = i
	}
	finalizers := make(map[string]struct{})
	for _, node := range v.graph.Nodes {
		if node.Finally != nil {
			finalizers[graph.NormalizeID(node.ID)] = struct{}{}
		}
		if node.Service != nil {
			if node.Service.TeardownOf == "" {
				if len(node.Service.TeardownNodes) != 1 {
					v.add(CodeInvalidFinally, v.nodeSource(node), fmt.Sprintf("service node %q requires exactly one generated teardown", node.ID), "Compile service source through the canonical service lowering.")
				}
				for _, teardownID := range node.Service.TeardownNodes {
					index, exists := known[graph.NormalizeID(teardownID)]
					if !exists {
						v.add(CodeInvalidFinally, v.nodeSource(node), fmt.Sprintf("service node %q references unknown teardown %q", node.ID, teardownID), "Keep the generated service teardown node in the immutable graph.")
						continue
					}
					teardown := v.graph.Nodes[index]
					if teardown.Finally == nil || teardown.Service == nil || graph.NormalizeID(teardown.Service.TeardownOf) != graph.NormalizeID(node.ID) {
						v.add(CodeInvalidFinally, v.nodeSource(teardown), fmt.Sprintf("service teardown %q does not exactly own %q", teardown.ID, node.ID), "Use the canonical generated service teardown node.")
					}
				}
			} else if node.Finally == nil || len(node.Service.TeardownNodes) != 0 {
				v.add(CodeInvalidFinally, v.nodeSource(node), fmt.Sprintf("service teardown %q must be an ordinary finalizer", node.ID), "Use the canonical generated service teardown shape.")
			}
		}
	}

	declaredNeeds := make(map[string]struct{})
	var arcs []structuralArc
	seenArcs := make(map[string]struct{})
	addArc := func(rawFrom, rawTo string, source *graph.SourceRef, dependencyMessage string) {
		from, to := graph.NormalizeID(rawFrom), graph.NormalizeID(rawTo)
		if _, exists := known[from]; !exists {
			v.add(
				CodeUnknownDependency,
				source,
				dependencyMessage,
				fmt.Sprintf("Declare node %q or update the dependency to an existing node ID.", rawFrom),
			)
			return
		}
		if _, exists := known[to]; !exists {
			v.add(
				CodeUnknownDependency,
				source,
				fmt.Sprintf("edge targets unknown node %q", rawTo),
				fmt.Sprintf("Declare node %q or update the edge target to an existing node ID.", rawTo),
			)
			return
		}
		key := from + "\x00" + to
		if _, duplicate := seenArcs[key]; duplicate {
			return
		}
		seenArcs[key] = struct{}{}
		arcs = append(arcs, structuralArc{from: from, to: to, source: cloneSource(source)})
	}

	for _, node := range v.graph.Nodes {
		to := graph.NormalizeID(node.ID)
		for _, need := range node.Needs {
			from := graph.NormalizeID(need.Node)
			declaredNeeds[from+"\x00"+to] = struct{}{}
			source := need.Source
			if source == nil {
				source = v.nodeSource(node)
			}
			addArc(need.Node, node.ID, source, fmt.Sprintf("node %q depends on unknown node %q", node.ID, need.Node))
		}
	}
	for _, edge := range v.graph.Edges {
		key := graph.NormalizeID(edge.From) + "\x00" + graph.NormalizeID(edge.To)
		if _, represented := declaredNeeds[key]; represented {
			continue
		}
		source := edge.Source
		if source == nil {
			source = graphSource(v.graph)
		}
		addArc(edge.From, edge.To, source, fmt.Sprintf("edge depends on unknown node %q", edge.From))
	}
	for _, node := range v.graph.Nodes {
		for _, rule := range node.Catch {
			for _, target := range rule.Targets {
				source := firstSource(rule.Source, v.nodeSource(node))
				if _, cleanup := finalizers[graph.NormalizeID(target)]; cleanup {
					v.add(CodeInvalidFinally, source, fmt.Sprintf("catch on node %q targets finalizer %q", node.ID, target), "Route failures to an ordinary handler; finalizers execute only through terminal cleanup progression.")
				}
				addArc(node.ID, target, source, fmt.Sprintf("catch on node %q targets unknown node %q", node.ID, target))
			}
		}
		if node.Switch != nil {
			for _, arm := range node.Switch.Arms {
				for _, target := range arm.Targets {
					source := firstSource(arm.Source, v.nodeSource(node))
					if _, cleanup := finalizers[graph.NormalizeID(target)]; cleanup {
						v.add(CodeInvalidFinally, source, fmt.Sprintf("switch on node %q targets finalizer %q", node.ID, target), "Route branches to an ordinary node; finalizers execute only through terminal cleanup progression.")
					}
					addArc(node.ID, target, source, fmt.Sprintf("switch on node %q targets unknown node %q", node.ID, target))
				}
			}
			for _, target := range node.Switch.Default {
				if _, cleanup := finalizers[graph.NormalizeID(target)]; cleanup {
					v.add(CodeInvalidFinally, v.nodeSource(node), fmt.Sprintf("switch on node %q defaults to finalizer %q", node.ID, target), "Route defaults to an ordinary node; finalizers execute only through terminal cleanup progression.")
				}
				addArc(node.ID, target, v.nodeSource(node), fmt.Sprintf("switch on node %q defaults to unknown node %q", node.ID, target))
			}
		}
		if node.Finally != nil {
			scope := node.Finally.Scope
			if len(scope) == 0 {
				for _, member := range v.graph.Nodes {
					if member.Finally == nil {
						scope = append(scope, member.ID)
					}
				}
			}
			for _, member := range scope {
				addArc(member, node.ID, v.nodeSource(node), fmt.Sprintf("finally node %q scopes unknown node %q", node.ID, member))
			}
		}
	}
	type finalizerScope struct {
		node   graph.Node
		global bool
		set    map[string]struct{}
	}
	finalizerScopes := make([]finalizerScope, 0)
	for _, node := range v.graph.Nodes {
		if node.Finally == nil {
			continue
		}
		item := finalizerScope{node: node, global: len(node.Finally.Scope) == 0, set: make(map[string]struct{})}
		if item.global {
			for _, member := range v.graph.Nodes {
				if member.Finally == nil {
					item.set[graph.NormalizeID(member.ID)] = struct{}{}
				}
			}
		} else {
			for _, member := range node.Finally.Scope {
				item.set[graph.NormalizeID(member)] = struct{}{}
			}
		}
		finalizerScopes = append(finalizerScopes, item)
	}
	for outerIndex, outer := range finalizerScopes {
		for innerIndex, inner := range finalizerScopes {
			if outerIndex == innerIndex {
				continue
			}
			if outer.global && !inner.global || strictControlSubset(inner.set, outer.set) {
				addArc(inner.node.ID, outer.node.ID, v.nodeSource(outer.node), fmt.Sprintf("finally node %q contains nested cleanup %q", outer.node.ID, inner.node.ID))
			}
		}
	}
	v.validateCycles(known, arcs)
}

func strictControlSubset(inner, outer map[string]struct{}) bool {
	if len(inner) >= len(outer) {
		return false
	}
	for member := range inner {
		if _, exists := outer[member]; !exists {
			return false
		}
	}
	return true
}

func (v *validator) validateCycles(known map[string]int, arcs []structuralArc) {
	adjacency := make(map[string][]structuralArc, len(known))
	for _, arc := range arcs {
		adjacency[arc.from] = append(adjacency[arc.from], arc)
	}
	for node := range adjacency {
		sort.SliceStable(adjacency[node], func(i, j int) bool {
			return adjacency[node][i].to < adjacency[node][j].to
		})
	}
	identities := make([]string, 0, len(known))
	for identity := range known {
		identities = append(identities, identity)
	}
	sort.Strings(identities)

	state := make(map[string]uint8, len(known))
	stack := make([]string, 0, len(known))
	stackIndex := make(map[string]int, len(known))
	var visit func(string)
	visit = func(node string) {
		state[node] = 1
		stackIndex[node] = len(stack)
		stack = append(stack, node)
		for _, arc := range adjacency[node] {
			switch state[arc.to] {
			case 0:
				visit(arc.to)
			case 1:
				start := stackIndex[arc.to]
				cycle := append(append([]string(nil), stack[start:]...), arc.to)
				firstNode := v.graph.Nodes[known[arc.to]]
				v.add(
					CodeGraphCycle,
					arc.source,
					"dependency cycle: "+strings.Join(cycle, " -> "),
					"Remove or redirect one need, explicit edge, or inferred expression reference so the graph is acyclic.",
					relatedSource("cycle returns to this node", v.nodeSource(firstNode))...,
				)
			}
		}
		stack = stack[:len(stack)-1]
		delete(stackIndex, node)
		state[node] = 2
	}
	for _, identity := range identities {
		if state[identity] == 0 {
			visit(identity)
		}
	}
}

func (v *validator) validateNodes() {
	for _, node := range v.graph.Nodes {
		v.validateNodeShape(node)
		kind, spec, reason := v.kinds.resolve(node.Kind, node.KindVersion)
		if kind == nil {
			message := fmt.Sprintf("step kind %q version %q is not registered", node.Kind, node.KindVersion)
			remediation := fmt.Sprintf("Register step kind %q at the requested version or update the node kind.", node.Kind)
			if reason != "" {
				message = reason
				remediation = fmt.Sprintf("Set kind_version to one registered version for %q.", node.Kind)
			}
			v.add(CodeUnknownStepKind, v.nodeSource(node), message, remediation)
		} else {
			v.validateKindConfig(node, kind, spec)
			if spec.Lifecycle.Service != (node.Service != nil) {
				v.add(CodeInvalidStepConfig, v.nodeSource(node), fmt.Sprintf("node %q service lifecycle marker differs from registered kind metadata", node.ID), "Use service kinds only through canonical service lowering and do not mark ordinary kinds as services.")
			}
		}
		v.validatePolicies(node, spec)
		v.diagnostics = append(v.diagnostics, verification.ValidateSpec(v.ctx, v.verifiers, node.Verification)...)
	}
}

func (v *validator) validateNodeShape(node graph.Node) {
	source := v.nodeSource(node)
	if node.Kind == "call" && node.Call == nil {
		v.add(
			CodeInvalidCallShape,
			source,
			fmt.Sprintf("call node %q is missing its call declaration", node.ID),
			"Declare definition, mode, and optional parent-close policy under call.",
		)
	}
	if node.Kind != "call" && node.Call != nil {
		v.add(
			CodeInvalidCallShape,
			source,
			fmt.Sprintf("non-call node %q carries a call declaration", node.ID),
			"Remove the call declaration or change the node kind to call.",
		)
	}
	if node.ReadyWhen != "" && !node.ReadyWhen.Valid() {
		v.add(
			CodeUnsupportedReadinessRule,
			source,
			fmt.Sprintf("node %q uses unsupported readiness rule %q", node.ID, node.ReadyWhen),
			"Use one of all_success, all_done, one_failed, all_failed, none_failed, or always.",
		)
	}
	if node.Call != nil && !node.Call.Mode.Valid() {
		v.add(
			CodeInvalidCallMode,
			source,
			fmt.Sprintf("call node %q uses unsupported mode %q", node.ID, node.Call.Mode),
			"Set call.mode to inline or run.",
		)
	}
	if node.Call != nil && node.Call.Mode.Valid() {
		hasStatic := !zeroDefinitionRef(node.Call.Definition)
		hasDynamic := strings.TrimSpace(node.Call.DefinitionInput) != ""
		if hasStatic == hasDynamic {
			v.add(CodeInvalidCallShape, source, fmt.Sprintf("call node %q must declare exactly one definition source", node.ID), "Set either call.definition or call.definition_input.")
		}
		if hasDynamic {
			if err := graph.ValidateID(node.Call.DefinitionInput); err != nil {
				v.add(CodeInvalidCallShape, source, fmt.Sprintf("call node %q has invalid definition input %q", node.ID, node.Call.DefinitionInput), "Use a normalized input name.")
			} else if _, exists := node.InputBindings[node.Call.DefinitionInput]; !exists {
				v.add(CodeInvalidCallShape, source, fmt.Sprintf("call node %q does not bind definition input %q", node.ID, node.Call.DefinitionInput), "Bind the exact generated DefinitionRef through node.with.")
			}
		}
	}
	if node.ForEach != nil {
		v.validateForEach(node, source)
	}
	v.validateCatch(node, source)
	v.validateSwitch(node, source)
	v.validateFinally(node, source)
}

func zeroDefinitionRef(ref graph.DefinitionRef) bool {
	return ref.Authority == "" && ref.Kind == "" && ref.ID == "" && ref.Locator == "" && ref.Version == "" && ref.Digest == "" && ref.Provenance == nil
}

func (v *validator) validateCatch(node graph.Node, fallback *graph.SourceRef) {
	seenContinue := false
	unconditional := -1
	seenBindings := make(map[string]int, len(node.Catch))
	for index, rule := range node.Catch {
		source := firstSource(rule.Source, fallback)
		if unconditional >= 0 {
			v.add(CodeInvalidCatch, source, fmt.Sprintf("node %q catch rule %d is unreachable after unconditional rule %d", node.ID, index, unconditional), "Move narrowed catch rules before the unconditional catch-all route.")
		}
		if rule.ContinueOnError() {
			if seenContinue || index != len(node.Catch)-1 {
				v.add(CodeInvalidCatch, source, fmt.Sprintf("node %q continue_on_error route must be unique and last", node.ID), "Keep one continue_on_error policy after all explicit catch routes.")
			}
			seenContinue = true
			continue
		}
		if len(rule.Targets) == 0 {
			v.add(CodeInvalidCatch, source, fmt.Sprintf("node %q catch rule %d has no targets", node.ID, index), "Declare at least one handler target, or use continue_on_error explicitly.")
		}
		if rule.When != nil && strings.TrimSpace(rule.When.Text) == "" {
			v.add(CodeInvalidCatch, source, fmt.Sprintf("node %q catch rule %d has an empty predicate", node.ID, index), "Provide a non-empty catch.when expression or remove the predicate.")
		}
		if rule.BindAs != "" {
			if err := values.ValidateExpressionLocalName(rule.BindAs); err != nil {
				v.add(CodeInvalidCatch, source, fmt.Sprintf("node %q catch binding %q is invalid", node.ID, rule.BindAs), "Use a lower-snake expression identifier that does not shadow a standard root.")
			}
			if prior, duplicate := seenBindings[rule.BindAs]; duplicate {
				v.add(CodeInvalidCatch, source, fmt.Sprintf("node %q catch binding %q repeats rule %d", node.ID, rule.BindAs, prior), "Use a distinct lexical binding for each catch route.")
			} else {
				seenBindings[rule.BindAs] = index
			}
		}
		seenErrors := make(map[string]struct{}, len(rule.Errors))
		matchesAll := len(rule.Errors) == 0
		for _, code := range rule.Errors {
			if strings.TrimSpace(code) == "" || code != strings.TrimSpace(code) {
				v.add(CodeInvalidCatch, source, fmt.Sprintf("node %q catch rule %d has an invalid error selector", node.ID, index), "Use non-empty error codes without surrounding whitespace, or * for every error.")
				continue
			}
			if _, duplicate := seenErrors[code]; duplicate {
				v.add(CodeInvalidCatch, source, fmt.Sprintf("node %q catch rule %d repeats error selector %q", node.ID, index, code), "Remove duplicate error selectors from the catch rule.")
			}
			if code == graph.CatchAllErrors {
				matchesAll = true
				if len(rule.Errors) != 1 {
					v.add(CodeInvalidCatch, source, fmt.Sprintf("node %q catch rule %d combines * with narrower selectors", node.ID, index), "Use * alone for an unconditional selector, or remove it and keep the named selectors.")
				}
			}
			seenErrors[code] = struct{}{}
		}
		v.validateControlTargets(CodeInvalidCatch, node.ID, "catch", rule.Targets, source)
		if matchesAll && rule.When == nil && unconditional < 0 {
			unconditional = index
		}
	}
}

func (v *validator) validateSwitch(node graph.Node, fallback *graph.SourceRef) {
	if node.Switch == nil {
		return
	}
	if len(node.Switch.Arms) == 0 {
		v.add(CodeInvalidSwitch, fallback, fmt.Sprintf("node %q switch has no arms", node.ID), "Declare at least one ordered switch arm.")
	}
	for index, arm := range node.Switch.Arms {
		source := firstSource(arm.Source, fallback)
		if strings.TrimSpace(arm.When.Text) == "" {
			v.add(CodeInvalidSwitch, source, fmt.Sprintf("node %q switch arm %d has an empty predicate", node.ID, index), "Provide a non-empty boolean switch predicate.")
		}
		if len(arm.Targets) == 0 {
			v.add(CodeInvalidSwitch, source, fmt.Sprintf("node %q switch arm %d has no targets", node.ID, index), "Declare at least one target for every switch arm.")
		}
		v.validateControlTargets(CodeInvalidSwitch, node.ID, "switch arm", arm.Targets, source)
	}
	v.validateControlTargets(CodeInvalidSwitch, node.ID, "switch default", node.Switch.Default, fallback)
}

func (v *validator) validateFinally(node graph.Node, fallback *graph.SourceRef) {
	if node.Finally == nil {
		return
	}
	if node.ReadyWhen != "" && node.ReadyWhen != graph.ReadyAllDone {
		v.add(CodeInvalidFinally, fallback, fmt.Sprintf("finally node %q uses readiness %q", node.ID, node.ReadyWhen), "Use all_done readiness; cleanup must observe every scoped terminal outcome.")
	}
	if node.ForEach != nil || node.Switch != nil {
		v.add(CodeInvalidFinally, fallback, fmt.Sprintf("finally node %q has fan-out or switch control semantics", node.ID), "Keep one cleanup invocation per declared scope; put fan-out or branch selection in ordinary handler nodes.")
	}
	if len(node.Catch) != 0 {
		v.add(CodeInvalidFinally, fallback, fmt.Sprintf("finally node %q declares catch or continue_on_error semantics", node.ID), "Cleanup failure determines the run outcome; move catch handling to ordinary nodes before finalization.")
	}
	v.validateControlTargets(CodeInvalidFinally, node.ID, "finally scope", node.Finally.Scope, fallback)
}

func (v *validator) validateControlTargets(code diagnostic.Code, owner, surface string, targets []string, source *graph.SourceRef) {
	seen := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		normalized := graph.NormalizeID(target)
		if target == owner {
			v.add(code, source, fmt.Sprintf("node %q %s contains itself", owner, surface), "Remove the self-reference from the control-flow declaration.")
		}
		if _, duplicate := seen[normalized]; duplicate {
			v.add(code, source, fmt.Sprintf("node %q %s repeats target %q", owner, surface, target), "List each control-flow target once.")
		}
		seen[normalized] = struct{}{}
	}
}

func (v *validator) validateForEach(node graph.Node, fallback *graph.SourceRef) {
	forEach := node.ForEach
	source := forEach.Items.Source
	if source == nil {
		source = fallback
	}
	add := func(reason, remediation string) {
		v.add(CodeInvalidForEach, source, fmt.Sprintf("node %q has invalid for_each: %s", node.ID, reason), remediation)
	}
	if strings.TrimSpace(forEach.Items.Text) == "" {
		add("items expression is empty", "Provide a non-empty for_each.items expression; expression evaluation occurs later.")
	}
	if forEach.MaxConcurrency < 0 {
		add("max_concurrency is negative", "Use zero for unbounded fan-out or a positive concurrency limit.")
	}
	if forEach.ItemName != "" {
		if err := graph.ValidateID(forEach.ItemName); err != nil {
			add(fmt.Sprintf("item_name %q is not a normalized ID", forEach.ItemName), "Use a lower-case kebab-form item_name.")
		}
	}
	if forEach.IndexName != "" {
		if err := graph.ValidateID(forEach.IndexName); err != nil {
			add(fmt.Sprintf("index_name %q is not a normalized ID", forEach.IndexName), "Use a lower-case kebab-form index_name.")
		}
	}
	if forEach.ItemName != "" && forEach.ItemName == forEach.IndexName {
		add("item_name and index_name are identical", "Use distinct item and index binding names.")
	}
	if forEach.Tolerate != nil {
		if forEach.Tolerate.Count < 0 {
			add("tolerated failure count is negative", "Use a non-negative tolerated failure count.")
		}
		if forEach.Tolerate.Percentage < 0 || forEach.Tolerate.Percentage > 100 {
			add("tolerated failure percentage is outside 0 through 100", "Use a tolerated failure percentage from 0 through 100.")
		}
		if forEach.Tolerate.Count > 0 && forEach.Tolerate.Percentage > 0 {
			add("tolerated failure count and percentage are both set", "Choose either a tolerated failure count or percentage.")
		}
	}
}

func (v *validator) validatePolicies(node graph.Node, spec *stepkind.StepKindSpec) {
	for _, hook := range v.options.PolicyHooks {
		if isNilInterface(hook) {
			continue
		}
		findings := hook.ValidateNode(v.ctx, NodeValidation{GraphID: v.graph.ID, Node: node, Kind: spec})
		for _, finding := range findings {
			v.diagnostics = append(v.diagnostics, normalizeFinding(
				finding,
				v.nodeSource(node),
				CodePolicyViolation,
				fmt.Sprintf("node %q violates validation policy", node.ID),
				"Update the node effects, retry, or idempotency declaration to satisfy policy.",
			))
		}
	}
}

func sourceAtDefinition(ref graph.DefinitionRef) *graph.SourceRef {
	locator := strings.TrimSpace(ref.Locator)
	if ref.Provenance != nil && strings.TrimSpace(ref.Provenance.Locator) != "" {
		locator = strings.TrimSpace(ref.Provenance.Locator)
	}
	if locator == "" {
		return nil
	}
	return &graph.SourceRef{Format: graph.SourceWorkflow, Locator: locator}
}

func definitionRelated(ref graph.DefinitionRef) []diagnostic.RelatedReference {
	source := sourceAtDefinition(ref)
	if source == nil {
		return nil
	}
	return relatedSource("referenced definition "+quotedDefinition(ref), source)
}
