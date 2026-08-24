package compile

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/hollis-labs/hadron/workflow/graph"
	"gopkg.in/yaml.v3"
)

const (
	maxMatrixDimensions = 16
	maxMatrixValues     = 256
	maxMatrixRows       = 4096
	maxSequentialGroups = 256
	maxSequentialNodes  = 4096
)

// Matrix source rules are deliberately closed and deterministic. Dimension
// names are sorted before cartesian expansion; values retain source order.
// Exclude entries are partial exact matches removed from the cartesian rows.
// Include entries are complete rows appended after exclusion. Duplicate final
// rows are rejected instead of being silently coalesced.
func (l *lowerer) lowerMatrix(node *yaml.Node, path []string) *graph.ForEachSpec {
	fields := l.mapping(node, path, "dimensions", "include", "exclude", "fail_fast", "max_parallel")
	dimensionsField, ok := fields["dimensions"]
	if !ok {
		l.invalidShape(node, path, "matrix.dimensions is required")
		return &graph.ForEachSpec{}
	}
	if dimensionsField.value.Kind != yaml.MappingNode || len(dimensionsField.value.Content) == 0 {
		l.invalidShape(dimensionsField.value, dimensionsField.path, "matrix.dimensions must be a non-empty mapping")
		return &graph.ForEachSpec{}
	}
	if len(dimensionsField.value.Content)/2 > maxMatrixDimensions {
		l.invalidShape(dimensionsField.value, dimensionsField.path, fmt.Sprintf("matrix.dimensions exceeds %d dimensions", maxMatrixDimensions))
		return &graph.ForEachSpec{}
	}

	dimensions := make(map[string][]any, len(dimensionsField.value.Content)/2)
	dimensionNames := make([]string, 0, len(dimensionsField.value.Content)/2)
	for index := 0; index+1 < len(dimensionsField.value.Content); index += 2 {
		key, value := dimensionsField.value.Content[index], dimensionsField.value.Content[index+1]
		keyPath := appendPath(dimensionsField.path, key.Value)
		name := l.normalizeID(key, keyPath)
		items := l.sequence(value, keyPath)
		if len(items) == 0 || len(items) > maxMatrixValues {
			l.invalidShape(value, keyPath, fmt.Sprintf("matrix dimension must contain between 1 and %d values", maxMatrixValues))
			continue
		}
		if _, duplicate := dimensions[name]; duplicate {
			l.invalidShape(key, keyPath, "matrix contains a duplicate normalized dimension")
			continue
		}
		values := make([]any, len(items))
		seen := make(map[string]struct{}, len(items))
		for itemIndex, item := range items {
			itemPath := appendPath(keyPath, strconv.Itoa(itemIndex))
			values[itemIndex] = l.jsonValue(item, itemPath)
			identity, err := canonicalMatrixJSON(values[itemIndex])
			if err != nil {
				l.invalidShape(item, itemPath, "matrix value must be JSON-compatible")
				continue
			}
			if _, exists := seen[identity]; exists {
				l.invalidShape(item, itemPath, "matrix dimension contains a duplicate value")
			}
			seen[identity] = struct{}{}
		}
		dimensions[name] = values
		dimensionNames = append(dimensionNames, name)
	}
	sort.Strings(dimensionNames)
	rows := []map[string]any{{}}
	for _, name := range dimensionNames {
		if len(rows) > maxMatrixRows/len(dimensions[name]) {
			l.invalidShape(dimensionsField.value, dimensionsField.path, fmt.Sprintf("matrix cartesian product exceeds %d rows", maxMatrixRows))
			rows = nil
			break
		}
		next := make([]map[string]any, 0, len(rows)*len(dimensions[name]))
		for _, row := range rows {
			for _, value := range dimensions[name] {
				candidate := cloneMatrixRow(row)
				candidate[name] = value
				next = append(next, candidate)
			}
		}
		rows = next
	}

	excludes := l.lowerMatrixRows(fields["exclude"], dimensionNames, false)
	if len(excludes) != 0 {
		kept := rows[:0]
		for _, row := range rows {
			excluded := false
			for _, pattern := range excludes {
				if matrixRowMatches(row, pattern) {
					excluded = true
					break
				}
			}
			if !excluded {
				kept = append(kept, row)
			}
		}
		rows = kept
	}
	includes := l.lowerMatrixRows(fields["include"], dimensionNames, true)
	rows = append(rows, includes...)
	if len(rows) == 0 {
		l.invalidShape(node, path, "matrix expansion must contain at least one row")
	}
	if len(rows) > maxMatrixRows {
		l.invalidShape(node, path, fmt.Sprintf("matrix expansion exceeds %d rows", maxMatrixRows))
		rows = rows[:maxMatrixRows]
	}
	seenRows := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		identity, err := canonicalMatrixJSON(row)
		if err != nil {
			continue
		}
		if _, duplicate := seenRows[identity]; duplicate {
			l.invalidShape(node, path, "matrix expansion contains a duplicate row")
		}
		seenRows[identity] = struct{}{}
	}
	encoded, err := json.Marshal(rows)
	if err != nil {
		l.invalidShape(node, path, "matrix rows cannot be encoded deterministically")
	}
	ref := l.location(node, path)
	spec := &graph.ForEachSpec{
		Items: graph.Expression{Text: string(encoded), Source: &ref}, ItemName: "matrix", IndexName: "matrix-index",
	}
	if field, exists := fields["fail_fast"]; exists {
		spec.FailFast = l.boolean(field.value, field.path)
	}
	if field, exists := fields["max_parallel"]; exists {
		spec.MaxConcurrency = l.integer(field.value, field.path)
	}
	return spec
}

func (l *lowerer) lowerMatrixRows(field sourceField, dimensions []string, complete bool) []map[string]any {
	if field.value == nil {
		return nil
	}
	items := l.sequence(field.value, field.path)
	if len(items) > maxMatrixRows {
		l.invalidShape(field.value, field.path, fmt.Sprintf("matrix row list exceeds %d entries", maxMatrixRows))
		items = items[:maxMatrixRows]
	}
	known := make(map[string]struct{}, len(dimensions))
	for _, dimension := range dimensions {
		known[dimension] = struct{}{}
	}
	rows := make([]map[string]any, 0, len(items))
	for index, item := range items {
		itemPath := appendPath(field.path, strconv.Itoa(index))
		if item.Kind != yaml.MappingNode || len(item.Content) == 0 {
			l.invalidShape(item, itemPath, "matrix row must be a non-empty mapping")
			continue
		}
		row := make(map[string]any, len(item.Content)/2)
		seenNames := make(map[string]struct{}, len(item.Content)/2)
		for pair := 0; pair+1 < len(item.Content); pair += 2 {
			key, value := item.Content[pair], item.Content[pair+1]
			keyPath := appendPath(itemPath, key.Value)
			name := l.normalizeID(key, keyPath)
			if _, ok := known[name]; !ok {
				l.invalidShape(key, keyPath, "matrix row references an unknown dimension")
				continue
			}
			if _, duplicate := seenNames[name]; duplicate {
				l.invalidShape(key, keyPath, "matrix row contains a duplicate normalized dimension")
				continue
			}
			seenNames[name] = struct{}{}
			row[name] = l.jsonValue(value, keyPath)
		}
		if complete && len(row) != len(dimensions) {
			l.invalidShape(item, itemPath, "matrix include row must declare every dimension")
		}
		rows = append(rows, row)
	}
	return rows
}

func canonicalMatrixJSON(value any) (string, error) {
	encoded, err := json.Marshal(value)
	return string(encoded), err
}

func cloneMatrixRow(row map[string]any) map[string]any {
	cloned := make(map[string]any, len(row)+1)
	for key, value := range row {
		cloned[key] = value
	}
	return cloned
}

func matrixRowMatches(row, pattern map[string]any) bool {
	for key, value := range pattern {
		left, leftErr := canonicalMatrixJSON(row[key])
		right, rightErr := canonicalMatrixJSON(value)
		if leftErr != nil || rightErr != nil || left != right {
			return false
		}
	}
	return true
}

func (l *lowerer) lowerJoin(node *yaml.Node, path []string, target string) ([]graph.Need, []graph.Edge) {
	items := l.sequence(node, path)
	if len(items) == 0 {
		l.invalidShape(node, path, "join must contain at least one dependency")
	}
	needs := make([]graph.Need, 0, len(items))
	edges := make([]graph.Edge, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for index, item := range items {
		itemPath := appendPath(path, strconv.Itoa(index))
		dependency := l.normalizeID(item, itemPath)
		if _, duplicate := seen[dependency]; duplicate {
			l.invalidShape(item, itemPath, "join contains a duplicate dependency")
		}
		seen[dependency] = struct{}{}
		ref := l.location(item, itemPath)
		need := graph.Need{Node: dependency, Kind: graph.EdgeControl, Source: &ref}
		needs = append(needs, need)
		edges = append(edges, graph.Edge{From: dependency, To: target, Kind: graph.EdgeControl, Source: &ref})
	}
	return needs, edges
}

func (l *lowerer) lowerSequentialGroups(node *yaml.Node, path []string, nodes []graph.Node) []graph.Edge {
	items := l.sequence(node, path)
	if len(items) == 0 {
		l.invalidShape(node, path, "sequential_groups must contain at least one group")
	}
	if len(items) > maxSequentialGroups {
		l.invalidShape(node, path, fmt.Sprintf("sequential_groups exceeds %d groups", maxSequentialGroups))
		items = items[:maxSequentialGroups]
	}
	known := make(map[string]struct{}, len(nodes))
	for _, graphNode := range nodes {
		known[graphNode.ID] = struct{}{}
	}
	owners := make(map[string]string)
	groupNames := make(map[string]struct{})
	var edges []graph.Edge
	for index, item := range items {
		itemPath := appendPath(path, strconv.Itoa(index))
		fields := l.mapping(item, itemPath, "name", "nodes")
		nameField, hasName := fields["name"]
		nodesField, hasNodes := fields["nodes"]
		if !hasName || !hasNodes {
			l.invalidShape(item, itemPath, "sequential group requires name and nodes")
			continue
		}
		name := l.normalizeID(nameField.value, nameField.path)
		if _, duplicate := groupNames[name]; duplicate {
			l.invalidShape(nameField.value, nameField.path, "sequential group name is duplicated")
		}
		groupNames[name] = struct{}{}
		members := l.sequence(nodesField.value, nodesField.path)
		if len(members) == 0 || len(members) > maxSequentialNodes {
			l.invalidShape(nodesField.value, nodesField.path, fmt.Sprintf("sequential group must contain between 1 and %d nodes", maxSequentialNodes))
			continue
		}
		ids := make([]string, 0, len(members))
		for memberIndex, member := range members {
			memberPath := appendPath(nodesField.path, strconv.Itoa(memberIndex))
			id := l.normalizeID(member, memberPath)
			if _, exists := known[id]; !exists {
				l.invalidShape(member, memberPath, "sequential group references an unknown node")
			}
			if owner, exists := owners[id]; exists {
				l.invalidShape(member, memberPath, fmt.Sprintf("node already belongs to sequential group %q", owner))
			} else {
				owners[id] = name
			}
			if len(ids) != 0 && ids[len(ids)-1] == id {
				l.invalidShape(member, memberPath, "sequential group contains a duplicate adjacent node")
			}
			ids = append(ids, id)
			if memberIndex != 0 {
				ref := l.location(member, memberPath)
				edges = append(edges, graph.Edge{From: ids[memberIndex-1], To: id, Kind: graph.EdgeControl, Source: &ref})
			}
		}
	}
	return edges
}

func (l *lowerer) validateAdvancedDependencies(nodes []graph.Node, edges []graph.Edge) {
	known := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		known[node.ID] = struct{}{}
	}
	seen := make(map[string]struct{}, len(edges))
	adjacency := make(map[string][]string, len(nodes))
	for _, edge := range edges {
		ref := edge.Source
		if _, exists := known[edge.From]; !exists {
			l.addDiagnostic(CodeInvalidWorkflowShape, nil, sourcePath(ref), "advanced dependency references an unknown source node", "Reference an existing step ID.")
		}
		if _, exists := known[edge.To]; !exists {
			l.addDiagnostic(CodeInvalidWorkflowShape, nil, sourcePath(ref), "advanced dependency references an unknown target node", "Reference an existing step ID.")
		}
		if edge.From == edge.To {
			l.addDiagnostic(CodeInvalidWorkflowShape, nil, sourcePath(ref), "advanced dependency cannot reference its own node", "Remove the self dependency.")
		}
		key := EdgeSourceKey(edge.From, edge.To, edge.Kind)
		if _, duplicate := seen[key]; duplicate {
			l.addDiagnostic(CodeInvalidWorkflowShape, nil, sourcePath(ref), "advanced dependency duplicates an existing edge", "Declare each dependency once.")
		}
		seen[key] = struct{}{}
		adjacency[edge.From] = append(adjacency[edge.From], edge.To)
	}
	state := make(map[string]uint8, len(nodes))
	var visit func(string) bool
	visit = func(id string) bool {
		if state[id] == 1 {
			return true
		}
		if state[id] == 2 {
			return false
		}
		state[id] = 1
		for _, next := range adjacency[id] {
			if visit(next) {
				return true
			}
		}
		state[id] = 2
		return false
	}
	ids := make([]string, 0, len(known))
	for id := range known {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if visit(id) {
			l.invalidShape(nil, nil, "advanced dependency lowering introduces a graph cycle")
			return
		}
	}
}

func sourcePath(ref *graph.SourceRef) []string {
	if ref == nil {
		return nil
	}
	return append([]string(nil), ref.Path...)
}
