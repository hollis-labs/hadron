package mcpadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/mark3labs/mcp-go/mcp"
)

func (a *Adapter) handleHumanGateGet(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	gateID := strings.TrimSpace(req.GetString("gate_id", ""))
	if gateID == "" {
		return toolError("validation_error", "gate_id is required"), nil
	}
	rec, err := a.store.GetHumanGate(ctx, gateID)
	if err != nil {
		if isNotFound(err) {
			return toolError("not_found", "human gate not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	if ws := strings.TrimSpace(req.GetString("workspace_id", "")); ws != "" && rec.WorkspaceID != ws {
		return toolError("not_found", "human gate not found in workspace"), nil
	}
	options, err := parseHumanGateOptions(rec.OptionsJSON)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(humanGateEnvelope(rec, options)), nil
}

func (a *Adapter) handleHumanGateSubmit(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopeHumanGateWrite); deny != nil {
		return deny, nil
	}
	gateID := strings.TrimSpace(req.GetString("gate_id", ""))
	if gateID == "" {
		return toolError("validation_error", "gate_id is required"), nil
	}
	decision := strings.TrimSpace(req.GetString("decision", ""))
	if decision == "" {
		return toolError("validation_error", "decision is required"), nil
	}
	rec, err := a.store.GetHumanGate(ctx, gateID)
	if err != nil {
		if isNotFound(err) {
			return toolError("not_found", "human gate not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	if ws := strings.TrimSpace(req.GetString("workspace_id", "")); ws != "" && rec.WorkspaceID != ws {
		return toolError("not_found", "human gate not found in workspace"), nil
	}
	if rec.Status != "waiting" {
		return toolError("conflict", "human gate is not waiting"), nil
	}
	options, err := parseHumanGateOptions(rec.OptionsJSON)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	if !humanGateDecisionAllowed(decision, options) {
		return toolError("validation_error", "decision is not an allowed option"), nil
	}
	if submitErr := a.store.SubmitHumanGateDecision(ctx, gateID, decision, time.Now().UTC()); submitErr != nil {
		if isNotFound(submitErr) {
			return toolError("conflict", "human gate is not waiting or was not found"), nil
		}
		return toolError("internal_error", submitErr.Error()), nil
	}
	rec, err = a.store.GetHumanGate(ctx, gateID)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(humanGateEnvelope(rec, options)), nil
}

type humanGateOption struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

func parseHumanGateOptions(raw string) ([]humanGateOption, error) {
	if strings.TrimSpace(raw) == "" {
		return []humanGateOption{}, nil
	}
	var options []humanGateOption
	if err := json.Unmarshal([]byte(raw), &options); err != nil {
		return nil, fmt.Errorf("parse human gate options_json: %w", err)
	}
	return options, nil
}

func humanGateDecisionAllowed(decision string, options []humanGateOption) bool {
	for _, option := range options {
		if option.ID == decision {
			return true
		}
	}
	return false
}

func humanGateEnvelope(rec persistence.HumanGateRecord, options []humanGateOption) map[string]any {
	return map[string]any{
		"id":           rec.ID,
		"workspace_id": rec.WorkspaceID,
		"run_id":       rec.RunID,
		"step_name":    rec.StepName,
		"prompt":       rec.Prompt,
		"options":      options,
		"options_json": rec.OptionsJSON,
		"status":       rec.Status,
		"decision":     nullString(rec.Decision),
		"created_at":   rec.CreatedAt.UTC().Format(time.RFC3339),
		"updated_at":   rec.UpdatedAt.UTC().Format(time.RFC3339),
		"expires_at":   nullString(rec.ExpiresAt),
	}
}
