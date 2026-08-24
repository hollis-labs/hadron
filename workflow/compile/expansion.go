package compile

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
)

// CompileOptions supplies extraction-safe, deterministic source-sugar
// expanders. Expanders are selected by Name rather than caller order.
type CompileOptions struct {
	NodeExpanders []NodeExpander
}

// NodeExpansionRequest gives an expander a defensive copy of the complete
// lowered graph and the candidate node. The graph is read-only input; returned
// replacements must carry every semantic field the expander accepts.
type NodeExpansionRequest struct {
	Graph graph.Graph `json:"graph"`
	Node  graph.Node  `json:"node"`
}

// NodeExpansion replaces one authored node with ordinary graph nodes and
// optional immutable child definitions. EntryNodeID receives inbound edges;
// ExitNodeID retains downstream dependency and expression identity.
type NodeExpansion struct {
	EntryNodeID string               `json:"entry_node_id"`
	ExitNodeID  string               `json:"exit_node_id"`
	Nodes       []graph.Node         `json:"nodes"`
	Edges       []graph.Edge         `json:"edges,omitempty"`
	Definitions []ResolvedDefinition `json:"definitions,omitempty"`
}

// NodeExpander is a pure compiler extension. ExpandNode returns handled=false
// when the node is outside its vocabulary. A handled expansion with any
// diagnostic is rejected; implementations must not perform I/O or consult
// mutable registries.
type NodeExpander interface {
	Name() string
	ExpandNode(NodeExpansionRequest) (expansion NodeExpansion, handled bool, diagnostics []diagnostic.Diagnostic)
}

type expansionResult struct {
	Graph       graph.Graph
	Definitions []ResolvedDefinition
}

type selectedExpansion struct {
	original  graph.Node
	expansion NodeExpansion
}

type namedNodeExpander struct {
	name     string
	expander NodeExpander
}

func expandGraph(input graph.Graph, options CompileOptions) (expansionResult, []diagnostic.Diagnostic) {
	expanders, findings := normalizedExpanders(options.NodeExpanders, graphSource(input))
	if len(findings) != 0 || len(expanders) == 0 {
		return expansionResult{Graph: input}, findings
	}
	graphCopy := input

	selections := make(map[string]selectedExpansion)
	for _, node := range graphCopy.Nodes {
		var handled []struct {
			name      string
			expansion NodeExpansion
		}
		for _, registered := range expanders {
			request, cloneErr := cloneExpansionRequest(NodeExpansionRequest{Graph: graphCopy, Node: node})
			if cloneErr != nil {
				findings = append(findings, expansionDiagnostic(node.Source, "node could not be copied for deterministic expansion"))
				break
			}
			expansion, accepted, reported := registered.expander.ExpandNode(request)
			for _, finding := range reported {
				findings = append(findings, normalizeExpansionDiagnostic(finding, node.Source))
			}
			if !accepted {
				continue
			}
			cloned, expansionErr := cloneNodeExpansion(expansion)
			if expansionErr != nil {
				findings = append(findings, expansionDiagnostic(node.Source, fmt.Sprintf("node expander %q returned non-JSON-compatible material", registered.name)))
				continue
			}
			handled = append(handled, struct {
				name      string
				expansion NodeExpansion
			}{name: registered.name, expansion: cloned})
		}
		if len(handled) > 1 {
			names := make([]string, len(handled))
			for index := range handled {
				names[index] = handled[index].name
			}
			findings = append(findings, expansionDiagnostic(node.Source, fmt.Sprintf("node %q is claimed by multiple expanders: %s", node.ID, strings.Join(names, ", "))))
			continue
		}
		if len(handled) == 1 {
			selections[node.ID] = selectedExpansion{original: node, expansion: handled[0].expansion}
		}
	}
	if len(findings) != 0 {
		sortDiagnostics(findings)
		return expansionResult{}, findings
	}

	result, applyFindings := applyExpansions(graphCopy, selections)
	findings = append(findings, applyFindings...)
	if len(findings) != 0 {
		sortDiagnostics(findings)
		return expansionResult{}, findings
	}
	return result, nil
}

func normalizedExpanders(input []NodeExpander, source *graph.SourceRef) ([]namedNodeExpander, []diagnostic.Diagnostic) {
	result := make([]namedNodeExpander, 0, len(input))
	var findings []diagnostic.Diagnostic
	for _, expander := range input {
		if isNilInterface(expander) {
			findings = append(findings, expansionDiagnostic(source, "node expander must not be nil"))
			continue
		}
		name := expander.Name()
		if name == "" || name != strings.TrimSpace(name) {
			findings = append(findings, expansionDiagnostic(source, "node expander name must be stable non-empty text"))
		}
		result = append(result, namedNodeExpander{name: name, expander: expander})
	}
	if len(findings) != 0 {
		sortDiagnostics(findings)
		return nil, findings
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].name < result[j].name })
	for index := 1; index < len(result); index++ {
		if result[index-1].name == result[index].name {
			findings = append(findings, expansionDiagnostic(source, fmt.Sprintf("node expander name %q is registered more than once", result[index].name)))
		}
	}
	return result, findings
}

func applyExpansions(input graph.Graph, selections map[string]selectedExpansion) (expansionResult, []diagnostic.Diagnostic) {
	if len(selections) == 0 {
		return expansionResult{Graph: input}, nil
	}
	var findings []diagnostic.Diagnostic
	finalNodes := make([]graph.Node, 0, len(input.Nodes)+len(selections))
	owners := make(map[string]string)
	for _, node := range input.Nodes {
		selection, expanded := selections[node.ID]
		if !expanded {
			finalNodes = append(finalNodes, node)
			owners[node.ID] = node.ID
			continue
		}
		if issues := validateExpansion(selection); len(issues) != 0 {
			findings = append(findings, issues...)
			continue
		}
		fallbackSource := selection.original.Source
		if fallbackSource == nil {
			if mapped, ok := input.SourceMap.Nodes[selection.original.ID]; ok {
				fallbackSource = &mapped
			}
		}
		for _, replacement := range selection.expansion.Nodes {
			if replacement.Source == nil {
				replacement.Source = cloneSource(fallbackSource)
			}
			if prior, collision := owners[replacement.ID]; collision {
				findings = append(findings, expansionDiagnostic(replacement.Source, fmt.Sprintf("expanded node %q collides with node owned by %q", replacement.ID, prior)))
				continue
			}
			owners[replacement.ID] = selection.original.ID
			finalNodes = append(finalNodes, replacement)
		}
	}
	// A replacement may collide with an original that occurs later.
	seen := make(map[string]string)
	for _, node := range finalNodes {
		if prior, duplicate := seen[node.ID]; duplicate {
			findings = append(findings, expansionDiagnostic(node.Source, fmt.Sprintf("expanded node %q collides with node owned by %q", node.ID, prior)))
			continue
		}
		seen[node.ID] = node.ID
	}
	if len(findings) != 0 {
		return expansionResult{}, findings
	}

	oldEdgeSources := input.SourceMap.Edges
	finalEdges := make([]graph.Edge, 0, len(input.Edges)+len(selections))
	finalEdgeSources := make(map[string]graph.SourceRef)
	for _, edge := range input.Edges {
		originalKey := EdgeSourceKey(edge.From, edge.To, edge.Kind)
		if selection, ok := selections[edge.From]; ok {
			edge.From = selection.expansion.ExitNodeID
		}
		if selection, ok := selections[edge.To]; ok {
			edge.To = selection.expansion.EntryNodeID
		}
		finalEdges = append(finalEdges, edge)
		if source, ok := oldEdgeSources[originalKey]; ok {
			finalEdgeSources[EdgeSourceKey(edge.From, edge.To, edge.Kind)] = source
		} else if edge.Source != nil {
			finalEdgeSources[EdgeSourceKey(edge.From, edge.To, edge.Kind)] = *cloneSource(edge.Source)
		}
	}
	var definitions []ResolvedDefinition
	for _, node := range input.Nodes {
		selection, ok := selections[node.ID]
		if !ok {
			continue
		}
		for _, edge := range selection.expansion.Edges {
			if edge.Source == nil {
				edge.Source = cloneSource(selection.original.Source)
				if edge.Source == nil {
					if mapped, exists := input.SourceMap.Nodes[selection.original.ID]; exists {
						edge.Source = cloneSource(&mapped)
					}
				}
			}
			finalEdges = append(finalEdges, edge)
			if edge.Source != nil {
				finalEdgeSources[EdgeSourceKey(edge.From, edge.To, edge.Kind)] = *cloneSource(edge.Source)
			}
		}
		definitions = append(definitions, selection.expansion.Definitions...)
	}

	input.Nodes = finalNodes
	input.Edges = finalEdges
	input.SourceMap.Nodes = make(map[string]graph.SourceRef, len(finalNodes))
	for _, node := range finalNodes {
		if node.Source != nil {
			input.SourceMap.Nodes[node.ID] = *cloneSource(node.Source)
		}
	}
	input.SourceMap.Edges = finalEdgeSources
	normalizedDefinitions, definitionFindings := normalizeBundledDefinitions(definitions, graphSource(input))
	if len(definitionFindings) != 0 {
		return expansionResult{}, definitionFindings
	}
	return expansionResult{Graph: input, Definitions: normalizedDefinitions}, nil
}

func validateExpansion(selection selectedExpansion) []diagnostic.Diagnostic {
	expansion := selection.expansion
	if len(expansion.Nodes) == 0 || expansion.EntryNodeID == "" || expansion.ExitNodeID == "" {
		return []diagnostic.Diagnostic{expansionDiagnostic(selection.original.Source, fmt.Sprintf("node %q expansion requires nodes plus entry and exit identities", selection.original.ID))}
	}
	known := make(map[string]struct{}, len(expansion.Nodes))
	var findings []diagnostic.Diagnostic
	for _, node := range expansion.Nodes {
		if err := graph.ValidateID(node.ID); err != nil {
			findings = append(findings, expansionDiagnostic(node.Source, fmt.Sprintf("expanded node identity %q is invalid", node.ID)))
			continue
		}
		if _, duplicate := known[node.ID]; duplicate {
			findings = append(findings, expansionDiagnostic(node.Source, fmt.Sprintf("expanded node identity %q is duplicated", node.ID)))
		}
		known[node.ID] = struct{}{}
	}
	for label, identity := range map[string]string{"entry": expansion.EntryNodeID, "exit": expansion.ExitNodeID} {
		if _, exists := known[identity]; !exists {
			findings = append(findings, expansionDiagnostic(selection.original.Source, fmt.Sprintf("expanded %s node %q is not present in replacements", label, identity)))
		}
	}
	for _, edge := range expansion.Edges {
		if _, exists := known[edge.From]; !exists {
			findings = append(findings, expansionDiagnostic(edge.Source, fmt.Sprintf("expanded edge starts at unknown replacement %q", edge.From)))
		}
		if _, exists := known[edge.To]; !exists {
			findings = append(findings, expansionDiagnostic(edge.Source, fmt.Sprintf("expanded edge ends at unknown replacement %q", edge.To)))
		}
	}
	return findings
}

func expansionDiagnostic(source *graph.SourceRef, message string) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError,
		Code:     CodeNodeExpansion,
		Message:  message,
		Source:   cloneSource(source),
		Remediation: &diagnostic.Remediation{
			Message: "Use a supported source-sugar shape or expand the declaration into ordinary graph nodes explicitly.",
		},
	}
}

func normalizeExpansionDiagnostic(input diagnostic.Diagnostic, fallback *graph.SourceRef) diagnostic.Diagnostic {
	if input.Severity == "" {
		input.Severity = diagnostic.SeverityError
	}
	if input.Code == "" {
		input.Code = CodeNodeExpansion
	}
	if input.Source == nil {
		input.Source = cloneSource(fallback)
	} else {
		input.Source = cloneSource(input.Source)
	}
	if input.Remediation == nil {
		input.Remediation = &diagnostic.Remediation{Message: "Use a supported source-sugar shape or expand it into ordinary graph nodes."}
	} else {
		remediation := *input.Remediation
		input.Remediation = &remediation
	}
	return input
}

func cloneExpansionRequest(input NodeExpansionRequest) (NodeExpansionRequest, error) {
	var output NodeExpansionRequest
	return output, cloneExpansionJSON(input, &output)
}

func cloneNodeExpansion(input NodeExpansion) (NodeExpansion, error) {
	var output NodeExpansion
	return output, cloneExpansionJSON(input, &output)
}

func cloneExpansionJSON(input, output any) error {
	encoded, err := json.Marshal(input)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	return requireJSONEnd(decoder)
}
