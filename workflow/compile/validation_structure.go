package compile

import (
	"fmt"
	"sort"
	"strings"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
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
	v.validateCycles(known, arcs)
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
					"explicit dependency cycle: "+strings.Join(cycle, " -> "),
					"Remove or redirect one explicit need or edge so the graph is acyclic.",
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
		}
		v.validatePolicies(node, spec)
	}
}

func (v *validator) validateNodeShape(node graph.Node) {
	source := v.nodeSource(node)
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
	if node.ForEach != nil {
		v.validateForEach(node, source)
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
