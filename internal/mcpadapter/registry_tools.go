package mcpadapter

import (
	"context"
	"strings"

	"github.com/hollis-labs/hadron/internal/registry"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func registerRegistryTools(s *server.MCPServer, reg *registry.Registry) {
	if reg == nil {
		return
	}

	s.AddTool(mcp.NewTool("hadron_registry_index",
		mcp.WithDescription("Index a directory of blueprint files into the local registry."),
		mcp.WithString("dir", mcp.Description("Directory to scan (default: blueprint directory)")),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		dir := strings.TrimSpace(req.GetString("dir", "."))
		if dir == "" {
			dir = "."
		}
		result, err := reg.Index(dir)
		if err != nil {
			return toolError("internal_error", err.Error()), nil
		}
		return toolJSON(map[string]any{
			"indexed":   result.Indexed,
			"updated":   result.Updated,
			"unchanged": result.Unchanged,
		}), nil
	})

	s.AddTool(mcp.NewTool("hadron_registry_search",
		mcp.WithDescription("Search the blueprint registry by keyword (matches name, slug, description, tags)."),
		mcp.WithString("query", mcp.Required(), mcp.Description("Search query")),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		query := strings.TrimSpace(req.GetString("query", ""))
		if query == "" {
			return toolError("validation_error", "query is required"), nil
		}
		entries, err := reg.Search(query)
		if err != nil {
			return toolError("internal_error", err.Error()), nil
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
		return toolJSON(map[string]any{"items": items, "count": len(items)}), nil
	})

	s.AddTool(mcp.NewTool("hadron_registry_show",
		mcp.WithDescription("Show full details for a blueprint in the registry by name or slug."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Blueprint name or slug")),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		name := strings.TrimSpace(req.GetString("name", ""))
		if name == "" {
			return toolError("validation_error", "name is required"), nil
		}
		entry, err := reg.Show(name)
		if err != nil {
			return toolError("not_found", err.Error()), nil
		}
		return toolJSON(map[string]any{
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
		}), nil
	})

	s.AddTool(mcp.NewTool("hadron_registry_list",
		mcp.WithDescription("List all blueprints in the local registry."),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		entries, err := reg.List()
		if err != nil {
			return toolError("internal_error", err.Error()), nil
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
		return toolJSON(map[string]any{"items": items, "count": len(items)}), nil
	})
}
