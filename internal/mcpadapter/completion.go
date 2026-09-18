package mcpadapter

import (
	"context"
	"path/filepath"
	"sort"
	"strings"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// handleCompletion is installed as go-mcp's single CompletionHandler
// (gomcp.WithCompletionHandler). It replaces mark3labs' two-interface
// PromptCompletionProvider/ResourceCompletionProvider pattern: the official
// SDK serves completion/complete for both prompts and resource templates
// through one handler, dispatched by req.Params.Ref.
func (a *Adapter) handleCompletion(_ context.Context, req *mcpsdk.CompleteRequest) (*mcpsdk.CompleteResult, error) {
	empty := &mcpsdk.CompleteResult{Completion: mcpsdk.CompletionResultDetails{Values: []string{}}}
	if req.Params == nil || req.Params.Ref == nil {
		return empty, nil
	}
	switch req.Params.Ref.Type {
	case "ref/prompt":
		if req.Params.Ref.Name == "hadron_pick_blueprint" && req.Params.Argument.Name == "tag" {
			values, err := a.completeBlueprintTags(req.Params.Argument.Value)
			if err != nil {
				return nil, err
			}
			return &mcpsdk.CompleteResult{Completion: mcpsdk.CompletionResultDetails{Values: values, Total: len(values)}}, nil
		}
	case "ref/resource":
		if req.Params.Ref.URI == "hadron://blueprints/{blueprint_ref}/input-schema" && req.Params.Argument.Name == "blueprint_ref" {
			values, err := a.completeBlueprintRefs(req.Params.Argument.Value)
			if err != nil {
				return nil, err
			}
			return &mcpsdk.CompleteResult{Completion: mcpsdk.CompletionResultDetails{Values: values, Total: len(values)}}, nil
		}
	}
	return empty, nil
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
