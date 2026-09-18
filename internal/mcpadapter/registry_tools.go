package mcpadapter

import (
	"context"
	"strings"

	"github.com/hollis-labs/go-mcp/budget"
	gomcp "github.com/hollis-labs/go-mcp/server"
	"github.com/hollis-labs/hadron/internal/registry"
)

func registerRegistryTools(s *gomcp.Server, reg *registry.Registry) {
	if reg == nil {
		return
	}

	registerTool(s, "hadron_registry_index", "Index a directory of blueprint files into the local registry.",
		gomcp.ObjectSchema(map[string]any{
			"dir": strProp("Directory to scan (default: blueprint directory)"),
		}), func(ctx context.Context, args map[string]any) (any, error) {
			dir := strings.TrimSpace(argString(args, "dir", "."))
			if dir == "" {
				dir = "."
			}
			result, err := reg.Index(dir)
			if err != nil {
				return nil, budget.NewToolError("internal_error", err.Error())
			}
			return map[string]any{
				"indexed":   result.Indexed,
				"updated":   result.Updated,
				"unchanged": result.Unchanged,
			}, nil
		})

	registerTool(s, "hadron_registry_search", "Search the blueprint registry by keyword (matches name, slug, description, tags).",
		gomcp.ObjectSchema(map[string]any{
			"query": strProp("Search query"),
			"limit": numProp("Max items to return (default 10, max 25)"),
		}, "query"), func(ctx context.Context, args map[string]any) (any, error) {
			query := strings.TrimSpace(argString(args, "query", ""))
			if query == "" {
				return nil, budget.NewToolError("validation_error", "query is required").WithField("query")
			}
			limit := budget.ExtractLimit(args, budget.DefaultLimit)
			entries, err := reg.Search(query)
			if err != nil {
				return nil, budget.NewToolError("internal_error", err.Error())
			}
			items := make([]map[string]any, 0, len(entries))
			for _, e := range entries {
				items = append(items, map[string]any{
					"name":        e.Name,
					"slug":        e.Slug,
					"title":       e.Title,
					"description": e.Description,
					"tags":        e.Tags,
					"file_path":   e.FilePath,
					"hash":        e.VersionHash,
					"indexed_at":  e.IndexedAt,
				})
			}
			return budget.Apply(items, budget.Config{Limit: limit},
				"%d matches found. Narrow the query or use hadron_registry_show with a specific name for full details."), nil
		})

	registerTool(s, "hadron_registry_show", "Show full details for a blueprint in the registry by name or slug.",
		gomcp.ObjectSchema(map[string]any{
			"name": strProp("Blueprint name or slug"),
		}, "name"), func(ctx context.Context, args map[string]any) (any, error) {
			name := strings.TrimSpace(argString(args, "name", ""))
			if name == "" {
				return nil, budget.NewToolError("validation_error", "name is required").WithField("name")
			}
			entry, err := reg.Show(name)
			if err != nil {
				return nil, budget.NewToolError("not_found", err.Error()).WithField("name").
					WithHelpTool("hadron_registry_search").WithNextStep("call hadron_registry_search to find a valid name")
			}
			return map[string]any{
				"name":        entry.Name,
				"slug":        entry.Slug,
				"title":       entry.Title,
				"description": entry.Description,
				"author":      entry.Author,
				"tags":        entry.Tags,
				"file_path":   entry.FilePath,
				"hash":        entry.VersionHash,
				"inputs_json": entry.InputsJSON,
				"indexed_at":  entry.IndexedAt,
			}, nil
		})

	registerTool(s, "hadron_registry_list", "List blueprints in the local registry. Use hadron_registry_search for keyword lookups on a large registry.",
		gomcp.ObjectSchema(map[string]any{
			"limit": numProp("Max items to return (default 10, max 25)"),
		}), func(ctx context.Context, args map[string]any) (any, error) {
			limit := budget.ExtractLimit(args, budget.DefaultLimit)
			entries, err := reg.List()
			if err != nil {
				return nil, budget.NewToolError("internal_error", err.Error())
			}
			items := make([]map[string]any, 0, len(entries))
			for _, e := range entries {
				items = append(items, map[string]any{
					"name":       e.Name,
					"file_path":  e.FilePath,
					"hash":       e.VersionHash,
					"indexed_at": e.IndexedAt,
				})
			}
			return budget.Apply(items, budget.Config{Limit: limit},
				"%d blueprints registered. Use hadron_registry_search to narrow, or hadron_registry_show for full details on one entry."), nil
		})
}
