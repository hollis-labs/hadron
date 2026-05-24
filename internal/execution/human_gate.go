package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/internal/blueprint"
	"github.com/hollis-labs/hadron/internal/persistence"
)

func (r *runExecution) executeHumanGateStep(ctx context.Context, section string, step blueprint.Step) error {
	if step.HumanGate == nil {
		return fmt.Errorf("step %q has no human_gate", step.Name)
	}
	gate := step.HumanGate
	gateID := fmt.Sprintf("gate-%s-%s-%d", r.runID, safeGateIDPart(step.Name), time.Now().UTC().UnixNano())
	timeout := time.Duration(gate.TimeoutSeconds) * time.Second
	pollInterval := 1 * time.Second
	if gate.PollIntervalSeconds > 0 {
		pollInterval = time.Duration(gate.PollIntervalSeconds) * time.Second
	}
	optionsJSONBytes, _ := json.Marshal(gate.Options)
	expiresAt := time.Now().UTC().Add(timeout)
	if err := r.manager.store.CreateHumanGate(ctx, persistence.HumanGateRecord{
		ID:          gateID,
		WorkspaceID: r.workspaceID,
		RunID:       r.runID,
		StepName:    step.Name,
		Prompt:      gate.Prompt,
		OptionsJSON: string(optionsJSONBytes),
		Status:      "waiting",
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
		ExpiresAt:   sql.NullString{String: expiresAt.Format(time.RFC3339), Valid: true},
	}); err != nil {
		return err
	}

	startPayload, _ := json.Marshal(map[string]any{
		"gate_id":         gateID,
		"prompt":          gate.Prompt,
		"options":         gate.Options,
		"timeout_seconds": gate.TimeoutSeconds,
	})
	r.emit(section, step.Name, "human_gate_waiting", string(startPayload))
	r.emit(section, step.Name, "log", fmt.Sprintf("::set-output gate_id=%s", gateID))

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rec, err := r.manager.store.GetHumanGate(ctx, gateID)
		if err != nil {
			r.emit(section, step.Name, "human_gate_error", err.Error())
			return fmt.Errorf("human_gate: %w", err)
		}
		if rec.Status == "decided" && rec.Decision.Valid {
			if !validHumanGateDecision(gate.Options, rec.Decision.String) {
				err := fmt.Errorf("human_gate decision %q is not an allowed option", rec.Decision.String)
				r.emit(section, step.Name, "human_gate_error", err.Error())
				return err
			}
			payload, _ := json.Marshal(map[string]any{
				"gate_id":  gateID,
				"decision": rec.Decision.String,
			})
			r.emit(section, step.Name, "human_gate_decision", string(payload))
			r.emit(section, step.Name, "log", fmt.Sprintf("::set-output decision=%s", sanitizeSetOutputValue(rec.Decision.String)))
			return nil
		}

		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			r.emit(section, step.Name, "human_gate_error", ctx.Err().Error())
			return ctx.Err()
		case <-timer.C:
		}
	}
	_ = r.manager.store.ExpireHumanGate(context.Background(), gateID, time.Now().UTC())
	err := fmt.Errorf("human_gate timed out after %s", timeout)
	r.emit(section, step.Name, "human_gate_timeout", err.Error())
	return err
}

func validHumanGateDecision(options []blueprint.HumanGateOption, decision string) bool {
	for _, opt := range options {
		if opt.ID == decision {
			return true
		}
	}
	return false
}

func safeGateIDPart(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "step"
	}
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "step"
	}
	return b.String()
}
