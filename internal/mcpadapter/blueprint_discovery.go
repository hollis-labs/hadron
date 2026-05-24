package mcpadapter

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/hollis-labs/go-mcp/budget"
	"github.com/hollis-labs/hadron/internal/agentcard"
	"github.com/hollis-labs/hadron/internal/blueprint"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

type blueprintCatalogEntry struct {
	Name        string
	Slug        string
	Title       string
	Description string
	Tags        []string
	Path        string
	InputCount  int
	Required    []string
	Score       int
	Reasons     []string
}

func (a *Adapter) registerBlueprintDiscoveryTools(s *server.MCPServer) {
	s.AddTool(mcp.NewTool("hadron_blueprint_discover",
		mcp.WithDescription("Discover blueprints from the configured blueprint directory. Use this first when you need a likely-fit workflow and do not know the exact file path yet."),
		mcp.WithString("query", mcp.Description("Optional free-text task or intent to rank likely blueprints")),
		mcp.WithString("tag", mcp.Description("Optional exact blueprint tag filter")),
		mcp.WithNumber("limit", mcp.Description("Max items to return (default 10, max 25)")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithIdempotentHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithOpenWorldHintAnnotation(false),
	), a.handleBlueprintDiscover)

	s.AddTool(mcp.NewTool("hadron_blueprint_broker",
		mcp.WithDescription("Return ranked blueprint recommendations for a task. This is the progressive-discovery companion to blueprint listing: it returns reasons and next steps, then the caller follows with hadron_blueprint_schema or hadron_blueprint_get."),
		mcp.WithString("query", mcp.Required(), mcp.Description("Task or workflow intent to match against available blueprints")),
		mcp.WithString("tag", mcp.Description("Optional exact blueprint tag filter")),
		mcp.WithNumber("limit", mcp.Description("Max items to return (default 5, max 20)")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithIdempotentHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithOpenWorldHintAnnotation(false),
	), a.handleBlueprintBroker)

	s.AddTool(mcp.NewTool("hadron_blueprint_search",
		mcp.WithDescription("Search blueprints by task keywords across name, slug, title, description, tags, and input names."),
		mcp.WithString("query", mcp.Required(), mcp.Description("Task description or keywords to search for")),
		mcp.WithString("tag", mcp.Description("Optional exact blueprint tag filter")),
		mcp.WithNumber("limit", mcp.Description("Max items to return (default 10, max 25)")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithIdempotentHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithOpenWorldHintAnnotation(false),
	), a.handleBlueprintSearch)

	s.AddTool(mcp.NewTool("hadron_blueprint_schema",
		mcp.WithDescription("Read the agent-facing input schema for a blueprint. Use after discovery to prepare inputs for hadron_run_enqueue."),
		mcp.WithString("blueprint_path", mcp.Required(), mcp.Description("Path to the blueprint file")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithIdempotentHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithOpenWorldHintAnnotation(false),
	), a.handleBlueprintSchema)
}

func (a *Adapter) handleBlueprintDiscover(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query := strings.TrimSpace(req.GetString("query", ""))
	tagFilter := strings.TrimSpace(req.GetString("tag", ""))
	limit := budget.ExtractLimit(req.GetArguments(), budget.DefaultLimit)

	entries, total, err := a.discoverBlueprints(query, tagFilter, limit, false)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(map[string]any{
		"items": entries,
		"meta": map[string]any{
			"query":                 query,
			"tag":                   tagFilter,
			"returned":              len(entries),
			"total_matches":         total,
			"blueprint_dir":         a.blueprintDir,
			"progressive_discovery": true,
			"next":                  []string{"hadron_blueprint_schema", "hadron_blueprint_get", "hadron_run_enqueue"},
		},
	}), nil
}

func (a *Adapter) handleBlueprintBroker(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query := strings.TrimSpace(req.GetString("query", ""))
	if query == "" {
		return toolError("validation_error", "query is required"), nil
	}
	tagFilter := strings.TrimSpace(req.GetString("tag", ""))
	limit := req.GetInt("limit", 5)
	if limit <= 0 {
		limit = 5
	}
	if limit > 20 {
		limit = 20
	}

	items, total, err := a.discoverBlueprints(query, tagFilter, limit, true)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(map[string]any{
		"items": items,
		"meta": map[string]any{
			"query":                 query,
			"tag":                   tagFilter,
			"returned":              len(items),
			"total_matches":         total,
			"progressive_discovery": true,
			"next":                  []string{"hadron_blueprint_schema", "hadron_blueprint_get", "hadron_run_enqueue"},
		},
	}), nil
}

func (a *Adapter) handleBlueprintSearch(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query := strings.TrimSpace(req.GetString("query", ""))
	if query == "" {
		return toolError("validation_error", "query is required"), nil
	}
	tagFilter := strings.TrimSpace(req.GetString("tag", ""))
	limit := budget.ExtractLimit(req.GetArguments(), budget.DefaultLimit)

	entries, total, err := a.discoverBlueprints(query, tagFilter, limit, true)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(map[string]any{
		"items": entries,
		"meta": map[string]any{
			"query":         query,
			"tag":           tagFilter,
			"returned":      len(entries),
			"total_matches": total,
			"next":          []string{"hadron_blueprint_schema", "hadron_blueprint_get"},
		},
	}), nil
}

func (a *Adapter) handleBlueprintSchema(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bpPath := strings.TrimSpace(req.GetString("blueprint_path", ""))
	if bpPath == "" {
		return toolError("validation_error", "blueprint_path is required"), nil
	}
	absPath, err := a.resolveBlueprintReference(bpPath)
	if err != nil {
		if os.IsNotExist(err) {
			return toolError("not_found", "blueprint file not found"), nil
		}
		if strings.Contains(err.Error(), "outside") {
			return toolError("validation_error", err.Error()), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	bp, err := blueprint.ParseFile(absPath)
	if err != nil {
		return toolError("validation_error", err.Error()), nil
	}
	skill := agentcard.SkillFromBlueprint(bp, absPath)
	return toolJSON(map[string]any{
		"path":         absPath,
		"id":           skill.ID,
		"name":         skill.Name,
		"description":  skill.Description,
		"tags":         skill.Tags,
		"input_schema": skill.InputSchema,
		"next":         []string{"hadron_run_enqueue", "hadron_blueprint_get"},
	}), nil
}

func (a *Adapter) discoverBlueprints(query, tagFilter string, limit int, requireQuery bool) ([]map[string]any, int, error) {
	entries, err := a.loadBlueprintCatalog()
	if err != nil {
		if os.IsNotExist(err) {
			return []map[string]any{}, 0, nil
		}
		return nil, 0, err
	}

	tagFilter = strings.ToLower(strings.TrimSpace(tagFilter))
	queryWords := tokeniseDiscoveryText(query)
	filtered := make([]blueprintCatalogEntry, 0, len(entries))
	for _, entry := range entries {
		if tagFilter != "" && !catalogHasTag(entry.Tags, tagFilter) {
			continue
		}
		score, reasons := scoreBlueprintEntry(entry, queryWords)
		if requireQuery && score == 0 {
			continue
		}
		entry.Score = score
		entry.Reasons = reasons
		filtered = append(filtered, entry)
	}

	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].Score != filtered[j].Score {
			return filtered[i].Score > filtered[j].Score
		}
		if filtered[i].Title != filtered[j].Title {
			return filtered[i].Title < filtered[j].Title
		}
		return filtered[i].Path < filtered[j].Path
	})

	total := len(filtered)
	if limit <= 0 {
		limit = budget.DefaultLimit
	}
	if limit > budget.MaxLimit {
		limit = budget.MaxLimit
	}
	if len(filtered) > limit {
		filtered = filtered[:limit]
	}

	out := make([]map[string]any, 0, len(filtered))
	for _, entry := range filtered {
		item := map[string]any{
			"name":            entry.Name,
			"slug":            entry.Slug,
			"title":           entry.Title,
			"description":     entry.Description,
			"tags":            entry.Tags,
			"blueprint_path":  entry.Path,
			"input_count":     entry.InputCount,
			"required_inputs": entry.Required,
			"next":            "hadron_blueprint_schema",
		}
		if entry.Score > 0 {
			item["score"] = entry.Score
		}
		if len(entry.Reasons) > 0 {
			item["reasons"] = entry.Reasons
		}
		out = append(out, item)
	}
	return out, total, nil
}

func (a *Adapter) loadBlueprintCatalog() ([]blueprintCatalogEntry, error) {
	dir := a.blueprintDir
	entries := []blueprintCatalogEntry{}
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if path == dir {
				return walkErr
			}
			return nil //nolint:nilerr
		}
		if d.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(d.Name()))
		if ext != ".yaml" && ext != ".yml" {
			return nil
		}
		bp, err := blueprint.ParseFile(path)
		if err != nil {
			return nil //nolint:nilerr
		}
		title := bp.Spec.Title
		if title == "" {
			title = bp.Spec.Name
		}
		if title == "" {
			title = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
		}
		name := bp.Spec.Name
		if name == "" {
			name = title
		}
		required := make([]string, 0, len(bp.Inputs))
		for _, input := range bp.Inputs {
			if input.Required {
				required = append(required, input.Name)
			}
		}
		entries = append(entries, blueprintCatalogEntry{
			Name:        name,
			Slug:        bp.Spec.Slug,
			Title:       title,
			Description: bp.Spec.Description,
			Tags:        bp.Spec.Tags,
			Path:        path,
			InputCount:  len(bp.Inputs),
			Required:    required,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func scoreBlueprintEntry(entry blueprintCatalogEntry, queryWords map[string]struct{}) (int, []string) {
	if len(queryWords) == 0 {
		return 0, nil
	}
	words := tokeniseDiscoveryText(strings.Join([]string{
		entry.Name,
		entry.Slug,
		entry.Title,
		entry.Description,
		strings.Join(entry.Tags, " "),
		strings.Join(entry.Required, " "),
	}, " "))

	score := 0
	reasons := []string{}
	for word := range queryWords {
		if _, ok := words[word]; !ok {
			continue
		}
		score++
		switch {
		case containsFold(entry.Name, word) || containsFold(entry.Title, word):
			reasons = append(reasons, "matched title/name: "+word)
		case containsTagFold(entry.Tags, word):
			reasons = append(reasons, "matched tag: "+word)
		default:
			reasons = append(reasons, "matched description/input: "+word)
		}
	}
	return score, reasons
}

func tokeniseDiscoveryText(s string) map[string]struct{} {
	words := map[string]struct{}{}
	isAlnum := func(r rune) bool { return 'a' <= r && r <= 'z' || '0' <= r && r <= '9' }
	for _, word := range strings.FieldsFunc(strings.ToLower(s), func(r rune) bool { return !isAlnum(r) }) {
		if len(word) > 1 {
			words[word] = struct{}{}
		}
	}
	return words
}

func catalogHasTag(tags []string, target string) bool {
	for _, tag := range tags {
		if strings.EqualFold(tag, target) {
			return true
		}
	}
	return false
}

func containsTagFold(tags []string, target string) bool {
	for _, tag := range tags {
		if containsFold(tag, target) {
			return true
		}
	}
	return false
}

func containsFold(s, sub string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(sub))
}

func (a *Adapter) resolveBlueprintPath(bpPath string) (string, error) {
	absPath, err := filepath.Abs(bpPath)
	if err != nil {
		return "", err
	}
	if _, statErr := os.Stat(absPath); statErr != nil {
		return "", statErr
	}

	absDir, err := filepath.Abs(a.blueprintDir)
	if err != nil {
		return "", err
	}

	resolvedDir, err := filepath.EvalSymlinks(absDir)
	if err != nil {
		return "", err
	}
	resolvedPath, err := filepath.EvalSymlinks(absPath)
	if err != nil {
		return "", err
	}

	relPath, err := filepath.Rel(resolvedDir, resolvedPath)
	if err != nil {
		return "", err
	}
	if relPath == ".." || strings.HasPrefix(relPath, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path is outside the blueprints directory")
	}
	return resolvedPath, nil
}

func (a *Adapter) resolveBlueprintReference(ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", os.ErrNotExist
	}
	if strings.Contains(ref, string(filepath.Separator)) || strings.HasSuffix(ref, ".yaml") || strings.HasSuffix(ref, ".yml") {
		return a.resolveBlueprintPath(ref)
	}
	if a.registry != nil {
		if path, err := a.registry.Resolve(ref); err == nil {
			return a.resolveBlueprintPath(path)
		}
	}
	entries, err := a.loadBlueprintCatalog()
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		base := strings.TrimSuffix(filepath.Base(entry.Path), filepath.Ext(entry.Path))
		if strings.EqualFold(entry.Slug, ref) || strings.EqualFold(entry.Name, ref) || strings.EqualFold(entry.Title, ref) || strings.EqualFold(base, ref) {
			return a.resolveBlueprintPath(entry.Path)
		}
	}
	return "", os.ErrNotExist
}
