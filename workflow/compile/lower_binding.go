package compile

import (
	"strings"

	"github.com/hollis-labs/hadron/workflow/graph"
	"gopkg.in/yaml.v3"
)

func (l *lowerer) lowerBinding(node *yaml.Node, path []string) graph.Binding {
	bindingRef := l.location(node, path)
	if node.Kind == yaml.MappingNode && hasAnyMappingKey(node, "literal", "expression", "interpolation") {
		fields := l.mapping(node, path, "literal", "expression", "interpolation")
		present := 0
		for _, name := range []string{"literal", "expression", "interpolation"} {
			if _, ok := fields[name]; ok {
				present++
			}
		}
		if present != 1 {
			l.invalidBinding(node, path, "explicit binding must contain exactly one binding mode")
			return graph.Binding{Source: &bindingRef}
		}
		if field, ok := fields["literal"]; ok {
			return graph.Binding{Kind: graph.BindingLiteral, Literal: l.jsonValue(field.value, field.path), Source: &bindingRef}
		}
		if field, ok := fields["expression"]; ok {
			expressionRef := l.location(field.value, field.path)
			return graph.Binding{
				Kind: graph.BindingExpression,
				Expression: &graph.Expression{
					Text:   l.string(field.value, field.path),
					Source: &expressionRef,
				},
				Source: &bindingRef,
			}
		}
		field := fields["interpolation"]
		return graph.Binding{Kind: graph.BindingInterpolation, Interpolation: l.literalString(field.value, field.path), Source: &bindingRef}
	}

	if node.Kind == yaml.ScalarNode && node.Tag == "!!str" {
		value := node.Value
		if strings.Contains(value, "{{") || strings.Contains(value, "}}") {
			if !strings.Contains(value, "{{") || !strings.Contains(value, "}}") {
				l.invalidBinding(node, path, "interpolation shorthand must contain both {{ and }}")
			}
			return graph.Binding{Kind: graph.BindingInterpolation, Interpolation: value, Source: &bindingRef}
		}
		return graph.Binding{
			Kind:       graph.BindingExpression,
			Expression: &graph.Expression{Text: strings.TrimSpace(value), Source: &bindingRef},
			Source:     &bindingRef,
		}
	}
	return graph.Binding{Kind: graph.BindingLiteral, Literal: l.jsonValue(node, path), Source: &bindingRef}
}

func (l *lowerer) lowerBindings(node *yaml.Node, path []string) map[string]graph.Binding {
	if node.Kind != yaml.MappingNode {
		l.invalidShape(node, path, "expected a binding mapping")
		return nil
	}
	bindings := make(map[string]graph.Binding, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		key, value := node.Content[i], node.Content[i+1]
		bindingPath := appendPath(path, key.Value)
		if strings.TrimSpace(key.Value) == "" {
			l.invalidBinding(key, bindingPath, "binding name must not be empty")
			continue
		}
		bindings[key.Value] = l.lowerBinding(value, bindingPath)
	}
	return bindings
}

func (l *lowerer) expression(node *yaml.Node, path []string) graph.Expression {
	ref := l.location(node, path)
	return graph.Expression{Text: l.string(node, path), Source: &ref}
}
