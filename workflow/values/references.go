package values

import (
	"fmt"
	"sort"
	"strings"

	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/parser"
	"github.com/hollis-labs/hadron/workflow/graph"
)

var standardExpressionRoots = map[string]struct{}{
	"inputs":           {},
	"outputs":          {},
	"compensation":     {},
	"steps":            {},
	"item":             {},
	"index":            {},
	"run":              {},
	"run_scope":        {},
	"execution_target": {},
	"env":              {},
}

// ParseReferences parses raw expression source and reports structural access
// to the standard roots. It performs no graph-edge inference and is intended
// to remain reusable by later compiler visibility analysis.
func ParseReferences(expression graph.Expression) ([]Reference, error) {
	text := strings.TrimSpace(expression.Text)
	if text == "" {
		return nil, expressionError(CodeExpressionSyntax, "expression must not be empty", expression.Source, nil)
	}
	if strings.Contains(text, "{{") {
		return nil, expressionError(
			CodeExpressionSyntax,
			"raw expressions must not contain interpolation markers",
			expression.Source,
			nil,
		)
	}

	tree, err := parser.Parse(text)
	if err != nil {
		return nil, expressionError(CodeExpressionSyntax, "expression syntax is invalid", expression.Source, err)
	}
	collector := referenceCollector{references: make(map[string]Reference)}
	ast.Walk(&tree.Node, &collector)

	references := make([]Reference, 0, len(collector.references))
	for _, reference := range collector.references {
		references = append(references, reference)
	}
	return stableReferences(references), nil
}

// exactReference returns a reference only when the complete expression is one
// static member chain such as inputs.payload or steps.fetch.outputs.body.
// Computed members and expressions that merely contain a reference are not
// passthrough bindings.
func exactReference(expression graph.Expression) (Reference, bool, error) {
	text := strings.TrimSpace(expression.Text)
	if text == "" || strings.Contains(text, "{{") {
		return Reference{}, false, nil
	}
	tree, err := parser.Parse(text)
	if err != nil {
		return Reference{}, false, err
	}
	reference, ok := referenceFromNode(tree.Node)
	if !ok || reference.Dynamic {
		return Reference{}, false, nil
	}
	reference.Path = append([]string(nil), reference.Path...)
	return reference, true, nil
}

// ParseInterpolationReferences reports structural references from every raw
// expression segment in an interpolation template. Literal segments are not
// parsed and no expression is evaluated.
func ParseInterpolationReferences(template string, source *graph.SourceRef) ([]Reference, error) {
	segments, err := parseInterpolation(template)
	if err != nil {
		return nil, expressionError(CodeInterpolation, "string interpolation is malformed", source, err)
	}

	var references []Reference
	for _, segment := range segments {
		if !segment.expression {
			continue
		}
		parsed, err := ParseReferences(graph.Expression{Text: segment.text, Source: cloneSourceRef(source)})
		if err != nil {
			return nil, err
		}
		references = append(references, parsed...)
	}
	return stableReferences(references), nil
}

func stableReferences(references []Reference) []Reference {
	unique := make(map[string]Reference, len(references))
	for _, reference := range references {
		reference.Path = append([]string(nil), reference.Path...)
		key := fmt.Sprintf("%s\x00%s\x00%t", reference.Root, strings.Join(reference.Path, "\x00"), reference.Dynamic)
		unique[key] = reference
	}
	references = references[:0]
	for _, reference := range unique {
		references = append(references, reference)
	}
	sort.Slice(references, func(i, j int) bool {
		if references[i].Root != references[j].Root {
			return references[i].Root < references[j].Root
		}
		left, right := strings.Join(references[i].Path, "\x00"), strings.Join(references[j].Path, "\x00")
		if left != right {
			return left < right
		}
		return !references[i].Dynamic && references[j].Dynamic
	})
	return removeReferencePrefixes(references)
}

type referenceCollector struct {
	references map[string]Reference
}

func (c *referenceCollector) Visit(node *ast.Node) {
	reference, ok := referenceFromNode(*node)
	if !ok {
		return
	}
	key := fmt.Sprintf("%s\x00%s\x00%t", reference.Root, strings.Join(reference.Path, "\x00"), reference.Dynamic)
	c.references[key] = reference
}

func referenceFromNode(node ast.Node) (Reference, bool) {
	switch node := node.(type) {
	case *ast.IdentifierNode:
		if _, ok := standardExpressionRoots[node.Value]; !ok {
			return Reference{}, false
		}
		return Reference{Root: node.Value}, true
	case *ast.ChainNode:
		return referenceFromNode(node.Node)
	case *ast.MemberNode:
		reference, ok := referenceFromNode(node.Node)
		if !ok {
			return Reference{}, false
		}
		if property, static := node.Property.(*ast.StringNode); static {
			reference.Path = append(reference.Path, property.Value)
		} else {
			reference.Dynamic = true
		}
		return reference, true
	default:
		return Reference{}, false
	}
}

func removeReferencePrefixes(references []Reference) []Reference {
	filtered := make([]Reference, 0, len(references))
	for i, candidate := range references {
		prefix := false
		for j, other := range references {
			if i == j || candidate.Root != other.Root {
				continue
			}
			// AST walks encounter the root identifier separately from a computed
			// member such as steps[run.step_id].status. The root is structural
			// scaffolding in that case, not an independent whole-map reference.
			if len(candidate.Path) == 0 && other.Dynamic {
				prefix = true
				break
			}
			if candidate.Dynamic != other.Dynamic || len(candidate.Path) >= len(other.Path) {
				continue
			}
			if pathPrefix(candidate.Path, other.Path) {
				prefix = true
				break
			}
		}
		if !prefix {
			filtered = append(filtered, candidate)
		}
	}
	return filtered
}

func pathPrefix(prefix, path []string) bool {
	for index := range prefix {
		if prefix[index] != path[index] {
			return false
		}
	}
	return true
}
