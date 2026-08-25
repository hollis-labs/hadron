package compile

import (
	"sort"
	"strconv"

	"github.com/hollis-labs/hadron/workflow/graph"
	"gopkg.in/yaml.v3"
)

const reactorDurabilityExtensionVersion = "reactor/v1"

func (l *lowerer) lowerDurability(node *yaml.Node, path []string) *graph.DurabilitySpec {
	if node != nil && node.Kind == yaml.ScalarNode {
		return &graph.DurabilitySpec{Mode: graph.DurabilityMode(l.string(node, path))}
	}
	fields := l.mapping(node, path, "mode", "continue_as_new")
	mode := graph.DurabilitySteps
	if field, ok := fields["mode"]; ok {
		mode = graph.DurabilityMode(l.string(field.value, field.path))
	}
	spec := &graph.DurabilitySpec{Mode: mode}
	if field, ok := fields["continue_as_new"]; ok {
		config := l.lowerContinueAsNew(field.value, field.path)
		ref := l.location(field.value, field.path)
		spec.Extension = graph.Extension{Version: reactorDurabilityExtensionVersion, Config: config, Source: &ref}
	}
	return spec
}

func (l *lowerer) lowerContinueAsNew(node *yaml.Node, path []string) graph.Config {
	fields := l.mapping(node, path, "max_events", "carry")
	config := graph.Config{}
	if field, ok := fields["max_events"]; ok {
		config["max_events"] = l.integer(field.value, field.path)
	} else {
		l.invalidShape(node, path, "continue_as_new.max_events is required")
	}
	if field, ok := fields["carry"]; ok {
		items := l.sequence(field.value, field.path)
		carry := make([]string, 0, len(items))
		seen := make(map[string]struct{}, len(items))
		for index, item := range items {
			name := l.normalizeID(item, appendPath(field.path, strconv.Itoa(index)))
			if _, duplicate := seen[name]; duplicate {
				l.invalidShape(item, appendPath(field.path, strconv.Itoa(index)), "continue_as_new carry names must be unique")
				continue
			}
			seen[name] = struct{}{}
			carry = append(carry, name)
		}
		sort.Strings(carry)
		config["carry"] = carry
	}
	return config
}
