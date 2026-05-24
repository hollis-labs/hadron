package mcpadapter

import (
	"context"
	"path/filepath"
	"sort"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
)

func (a *Adapter) CompletePromptArgument(_ context.Context, promptName string, argument mcp.CompleteArgument, _ mcp.CompleteContext) (*mcp.Completion, error) {
	switch promptName {
	case "hadron_pick_blueprint":
		if argument.Name == "tag" {
			values, err := a.completeBlueprintTags(argument.Value)
			if err != nil {
				return nil, err
			}
			return &mcp.Completion{Values: values, Total: len(values)}, nil
		}
	}
	return &mcp.Completion{Values: []string{}}, nil
}

func (a *Adapter) CompleteResourceArgument(_ context.Context, uri string, argument mcp.CompleteArgument, _ mcp.CompleteContext) (*mcp.Completion, error) {
	if uri == "hadron://blueprints/{blueprint_ref}/input-schema" && argument.Name == "blueprint_ref" {
		values, err := a.completeBlueprintRefs(argument.Value)
		if err != nil {
			return nil, err
		}
		return &mcp.Completion{Values: values, Total: len(values)}, nil
	}
	return &mcp.Completion{Values: []string{}}, nil
}

func (a *Adapter) completeBlueprintTags(prefix string) ([]string, error) {
	entries, err := a.loadBlueprintCatalog()
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	values := []string{}
	for _, entry := range entries {
		for _, tag := range entry.Tags {
			tag = strings.TrimSpace(tag)
			if tag == "" || !matchesCompletionPrefix(tag, prefix) {
				continue
			}
			if _, ok := seen[tag]; ok {
				continue
			}
			seen[tag] = struct{}{}
			values = append(values, tag)
		}
	}
	sort.Strings(values)
	if len(values) > 100 {
		values = values[:100]
	}
	return values, nil
}

func (a *Adapter) completeBlueprintRefs(prefix string) ([]string, error) {
	entries, err := a.loadBlueprintCatalog()
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	values := []string{}
	for _, entry := range entries {
		candidates := []string{entry.Slug, entry.Name, strings.TrimSuffix(filepath.Base(entry.Path), filepath.Ext(entry.Path))}
		for _, candidate := range candidates {
			candidate = strings.TrimSpace(candidate)
			if candidate == "" || !matchesCompletionPrefix(candidate, prefix) {
				continue
			}
			if _, ok := seen[candidate]; ok {
				continue
			}
			seen[candidate] = struct{}{}
			values = append(values, candidate)
		}
	}
	sort.Strings(values)
	if len(values) > 100 {
		values = values[:100]
	}
	return values, nil
}

func matchesCompletionPrefix(value, prefix string) bool {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return true
	}
	return strings.HasPrefix(strings.ToLower(value), strings.ToLower(prefix))
}
