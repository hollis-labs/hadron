package mcpadapter

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hollis-labs/go-mcp/budget"
	"github.com/hollis-labs/hadron/internal/persistence"
)

func (a *Adapter) handleTriggersList(ctx context.Context, _ map[string]any) (any, error) {
	items, err := a.store.ListTriggers(ctx)
	if err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	out := make([]map[string]any, 0, len(items))
	for _, t := range items {
		out = append(out, map[string]any{
			"id":             t.ID,
			"type":           t.Type,
			"name":           t.Name,
			"path":           t.Path,
			"blueprint_path": t.BlueprintPath,
			"workspace_id":   t.WorkspaceID,
			"enabled":        t.Enabled,
			"one_shot":       t.OneShot,
			"fired_count":    t.FiredCount,
			"created_at":     t.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	return map[string]any{"items": out, "count": len(out)}, nil
}

func (a *Adapter) handleTriggerCreate(ctx context.Context, args map[string]any) (any, error) {
	if err := a.checkScope(ScopeTriggerWrite); err != nil {
		return nil, err
	}
	name := strings.TrimSpace(argString(args, "name", ""))
	if name == "" {
		return nil, budget.NewToolError("validation_error", "name is required").WithField("name")
	}
	path := strings.TrimSpace(argString(args, "path", ""))
	if path == "" {
		return nil, budget.NewToolError("validation_error", "path is required").WithField("path")
	}
	blueprintPath := strings.TrimSpace(argString(args, "blueprint_path", ""))
	if blueprintPath == "" {
		return nil, budget.NewToolError("validation_error", "blueprint_path is required").WithField("blueprint_path")
	}
	workspaceID := workspaceDefault(argString(args, "workspace_id", "default"))

	triggerID := fmt.Sprintf("mcp-trig-%s-%04d", time.Now().UTC().Format("20060102-150405"), atomic.AddUint64(&runSeq, 1))
	rec := persistence.TriggerRecord{
		ID:            triggerID,
		Type:          "webhook",
		Name:          name,
		Path:          path,
		BlueprintPath: blueprintPath,
		WorkspaceID:   workspaceID,
		Enabled:       true,
		OneShot:       argBool(args, "one_shot", false),
	}
	if secret := strings.TrimSpace(argString(args, "secret", "")); secret != "" {
		rec.SecretHash = sql.NullString{String: secret, Valid: true}
	}
	if ei := strings.TrimSpace(argString(args, "extract_inputs", "")); ei != "" {
		rec.ExtractInputs = sql.NullString{String: ei, Valid: true}
	}
	ttlMinutes := argFloat(args, "ttl_minutes", 0)
	if ttlMinutes > 0 {
		expires := time.Now().UTC().Add(time.Duration(ttlMinutes) * time.Minute)
		rec.TTLExpiresAt = sql.NullString{String: expires.Format(time.RFC3339), Valid: true}
	}

	if err := a.store.CreateTrigger(ctx, rec); err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	return map[string]any{
		"trigger_id":     triggerID,
		"name":           name,
		"path":           path,
		"blueprint_path": blueprintPath,
		"webhook_url":    "/hooks/" + path,
	}, nil
}

func (a *Adapter) handleTriggerWatch(ctx context.Context, args map[string]any) (any, error) {
	if err := a.checkScope(ScopeTriggerWrite); err != nil {
		return nil, err
	}

	trigType := strings.TrimSpace(argString(args, "type", ""))
	if trigType != "webhook" && trigType != "file_watch" {
		return nil, budget.NewToolError("validation_error", "type must be 'webhook' or 'file_watch'").WithField("type")
	}
	blueprintPath := strings.TrimSpace(argString(args, "blueprint_path", ""))
	if blueprintPath == "" {
		return nil, budget.NewToolError("validation_error", "blueprint_path is required").WithField("blueprint_path")
	}
	configJSON := strings.TrimSpace(argString(args, "config", ""))
	if configJSON == "" {
		return nil, budget.NewToolError("validation_error", "config is required").WithField("config")
	}
	ttlMinutes := argFloat(args, "ttl_minutes", 0)
	if ttlMinutes <= 0 {
		return nil, budget.NewToolError("validation_error", "ttl_minutes is required and must be > 0").WithField("ttl_minutes")
	}
	if ttlMinutes > 1440 {
		return nil, budget.NewToolError("validation_error", "ttl_minutes max is 1440 (24 hours)").WithField("ttl_minutes")
	}
	oneShot := argBool(args, "one_shot", true)
	workspaceID := workspaceDefault(argString(args, "workspace_id", "default"))

	// Parse config
	var cfg map[string]any
	if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
		return nil, budget.NewToolError("validation_error", "config must be valid JSON: "+err.Error()).WithField("config")
	}

	triggerID := fmt.Sprintf("mcp-trig-%s-%04d", time.Now().UTC().Format("20060102-150405"), atomic.AddUint64(&runSeq, 1))
	expires := time.Now().UTC().Add(time.Duration(ttlMinutes) * time.Minute)

	rec := persistence.TriggerRecord{
		ID:            triggerID,
		Type:          trigType,
		BlueprintPath: blueprintPath,
		WorkspaceID:   workspaceID,
		Enabled:       true,
		OneShot:       oneShot,
		TTLExpiresAt:  sql.NullString{String: expires.Format(time.RFC3339), Valid: true},
		CreatedBy:     a.sessionID,
	}

	// Extract type-specific fields from config
	name, _ := cfg["name"].(string)
	if name == "" {
		name = trigType + "-watch"
	}
	rec.Name = name

	switch trigType {
	case "webhook":
		path, _ := cfg["path"].(string)
		if path == "" {
			return nil, budget.NewToolError("validation_error", "config.path is required for webhook triggers").WithField("config")
		}
		rec.Path = path
	case "file_watch":
		// paths can be a JSON array or a single string
		switch v := cfg["paths"].(type) {
		case []any:
			pathsJSON, _ := json.Marshal(v)
			rec.Path = string(pathsJSON)
		case string:
			rec.Path = v
		default:
			return nil, budget.NewToolError("validation_error", "config.paths is required for file_watch triggers").WithField("config")
		}

		// Optional debounce
		if d, ok := cfg["debounce"].(float64); ok && d > 0 {
			rec.DebounceSeconds = int(d)
		}

		// Store events filter in extract_inputs
		if events, ok := cfg["events"].(string); ok && events != "" {
			ei, _ := json.Marshal(map[string]string{"events": events})
			rec.ExtractInputs = sql.NullString{String: string(ei), Valid: true}
		}
	}

	if err := a.store.CreateTrigger(ctx, rec); err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}

	result := map[string]any{
		"trigger_id":     triggerID,
		"type":           trigType,
		"name":           rec.Name,
		"blueprint_path": blueprintPath,
		"one_shot":       oneShot,
		"ttl_expires_at": expires.Format(time.RFC3339),
	}
	if trigType == "webhook" {
		result["webhook_url"] = "/hooks/" + rec.Path
	}
	return result, nil
}

func (a *Adapter) handleTriggerListMine(ctx context.Context, _ map[string]any) (any, error) {
	items, err := a.store.ListTriggersByCreatedBy(ctx, a.sessionID)
	if err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	out := make([]map[string]any, 0, len(items))
	for _, t := range items {
		out = append(out, map[string]any{
			"id":             t.ID,
			"type":           t.Type,
			"name":           t.Name,
			"path":           t.Path,
			"blueprint_path": t.BlueprintPath,
			"workspace_id":   t.WorkspaceID,
			"enabled":        t.Enabled,
			"one_shot":       t.OneShot,
			"fired_count":    t.FiredCount,
			"ttl_expires_at": nullString(t.TTLExpiresAt),
			"created_at":     t.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	return map[string]any{"items": out, "count": len(out), "session_id": a.sessionID}, nil
}

func (a *Adapter) handleTriggerDelete(ctx context.Context, args map[string]any) (any, error) {
	if err := a.checkScope(ScopeTriggerWrite); err != nil {
		return nil, err
	}
	triggerID := strings.TrimSpace(argString(args, "trigger_id", ""))
	if triggerID == "" {
		return nil, budget.NewToolError("validation_error", "trigger_id is required").WithField("trigger_id")
	}
	if err := a.store.DeleteTrigger(ctx, triggerID); err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	return map[string]any{"trigger_id": triggerID, "deleted": true}, nil
}
