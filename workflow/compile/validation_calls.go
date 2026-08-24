package compile

import (
	"fmt"
	"strings"

	"github.com/hollis-labs/hadron/workflow/graph"
)

type callTraversal struct {
	validator *validator
	resolver  DefinitionResolver
	maxDepth  int
	resolved  map[string]ResolvedDefinition
}

func (v *validator) validateCalls() {
	if isNilInterface(v.options.Definitions) {
		return
	}
	maxDepth := v.options.MaxCallDepth
	if maxDepth <= 0 {
		maxDepth = DefaultMaxCallDepth
	}
	rootKey := definitionKey(v.root, v.graph)
	traversal := callTraversal{
		validator: v,
		resolver:  v.options.Definitions,
		maxDepth:  maxDepth,
		resolved:  make(map[string]ResolvedDefinition),
	}
	traversal.walk(v.graph, 0, map[string]*graph.SourceRef{rootKey: cloneSource(graphSource(v.graph))})
}

func (t *callTraversal) walk(value graph.Graph, depth int, active map[string]*graph.SourceRef) {
	for _, node := range value.Nodes {
		if node.Call == nil {
			continue
		}
		callSource := node.Source
		if callSource == nil {
			if source, ok := value.SourceMap.Nodes[node.ID]; ok {
				callSource = &source
			} else {
				callSource = graphSource(value)
			}
		}
		requestedKey := definitionKey(node.Call.Definition, graph.Graph{})
		resolved, ok := t.resolved[requestedKey]
		if !ok {
			var err error
			resolved, err = t.resolver.ResolveDefinition(t.validator.ctx, node.Call.Definition)
			if err != nil {
				t.validator.add(
					CodeDefinitionResolution,
					callSource,
					fmt.Sprintf("call node %q could not resolve definition %s: %v", node.ID, quotedDefinition(node.Call.Definition), err),
					"Make the referenced immutable definition available or defer resolution through host policy.",
					definitionRelated(node.Call.Definition)...,
				)
				continue
			}
			t.resolved[requestedKey] = resolved
		}
		resolvedKey := definitionKey(resolved.Definition, resolved.Graph)
		if ancestorSource, cycle := active[resolvedKey]; cycle {
			t.validator.add(
				CodeCallCycle,
				callSource,
				fmt.Sprintf("call node %q creates a definition cycle at %s", node.ID, quotedDefinition(resolved.Definition)),
				"Remove or redirect the recursive call so no immutable definition repeats in one call path.",
				relatedSource("definition is already active in this call path", ancestorSource)...,
			)
			continue
		}
		nextDepth := depth + 1
		if nextDepth > t.maxDepth {
			t.validator.add(
				CodeCallDepthExceeded,
				callSource,
				fmt.Sprintf("call node %q exceeds maximum definition depth %d at %s", node.ID, t.maxDepth, quotedDefinition(resolved.Definition)),
				"Reduce nested calls or raise the explicit validation depth limit after policy review.",
				definitionRelated(resolved.Definition)...,
			)
			continue
		}
		childActive := make(map[string]*graph.SourceRef, len(active)+1)
		for identity, source := range active {
			childActive[identity] = source
		}
		childActive[resolvedKey] = cloneSource(callSource)
		t.walk(resolved.Graph, nextDepth, childActive)
	}
}

func definitionKey(ref graph.DefinitionRef, fallback graph.Graph) string {
	if digest := strings.TrimSpace(ref.Digest); digest != "" {
		return "digest:" + digest
	}
	if digest := strings.TrimSpace(fallback.Digest); digest != "" {
		return "digest:" + digest
	}
	authority := strings.TrimSpace(ref.Authority)
	kind := strings.TrimSpace(ref.Kind)
	id := graph.NormalizeID(ref.ID)
	locator := strings.TrimSpace(ref.Locator)
	version := strings.TrimSpace(ref.Version)
	if authority == "" && kind == "" && id == "" && locator == "" && version == "" {
		id = graph.NormalizeID(fallback.ID)
		version = strings.TrimSpace(fallback.Version)
		locator = strings.TrimSpace(fallback.Provenance.Locator)
	}
	return fmt.Sprintf("definition:%q:%q:%q:%q:%q", authority, kind, id, locator, version)
}
