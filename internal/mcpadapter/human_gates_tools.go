package mcpadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hollis-labs/go-mcp/budget"
	"github.com/hollis-labs/hadron/internal/persistence"
)

func (a *Adapter) handleHumanGateGet(ctx context.Context, args map[string]any) (any, error) {
	gateID := strings.TrimSpace(argString(args, "gate_id", ""))
	if gateID == "" {
		return nil, budget.NewToolError("validation_error", "gate_id is required").WithField("gate_id")
	}
	rec, err := a.store.GetHumanGate(ctx, gateID)
	if err != nil {
		if isNotFound(err) {
			return nil, budget.NewToolError("not_found", "human gate not found").WithField("gate_id")
		}
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	if ws := strings.TrimSpace(argString(args, "workspace_id", "")); ws != "" && rec.WorkspaceID != ws {
		return nil, budget.NewToolError("not_found", "human gate not found in workspace").WithField("gate_id")
	}
	options, err := parseHumanGateOptions(rec.OptionsJSON)
	if err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	return humanGateEnvelope(rec, options), nil
}

func (a *Adapter) handleHumanGateSubmit(ctx context.Context, args map[string]any) (any, error) {
	if err := a.checkScope(ScopeHumanGateWrite); err != nil {
		return nil, err
	}
	gateID := strings.TrimSpace(argString(args, "gate_id", ""))
	if gateID == "" {
		return nil, budget.NewToolError("validation_error", "gate_id is required").WithField("gate_id")
	}
	decision := strings.TrimSpace(argString(args, "decision", ""))
	if decision == "" {
		return nil, budget.NewToolError("validation_error", "decision is required").WithField("decision")
	}
	rec, err := a.store.GetHumanGate(ctx, gateID)
	if err != nil {
		if isNotFound(err) {
			return nil, budget.NewToolError("not_found", "human gate not found").WithField("gate_id")
		}
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	if ws := strings.TrimSpace(argString(args, "workspace_id", "")); ws != "" && rec.WorkspaceID != ws {
		return nil, budget.NewToolError("not_found", "human gate not found in workspace").WithField("gate_id")
	}
	if rec.Status != "waiting" {
		return nil, budget.NewToolError("conflict", "human gate is not waiting").WithField("gate_id")
	}
	options, err := parseHumanGateOptions(rec.OptionsJSON)
	if err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	if !humanGateDecisionAllowed(decision, options) {
		return nil, budget.NewToolError("validation_error", "decision is not an allowed option").WithField("decision")
	}
	if submitErr := a.store.SubmitHumanGateDecision(ctx, gateID, decision, time.Now().UTC()); submitErr != nil {
		if isNotFound(submitErr) {
			return nil, budget.NewToolError("conflict", "human gate is not waiting or was not found").WithField("gate_id")
		}
		return nil, budget.NewToolError("internal_error", submitErr.Error())
	}
	rec, err = a.store.GetHumanGate(ctx, gateID)
	if err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	return humanGateEnvelope(rec, options), nil
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
