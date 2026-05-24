package rundiagnostics

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/internal/persistence"
)

type OperationDiagnostic struct {
	Sequence       int    `json:"sequence"`
	Kind           string `json:"kind"`
	StepName       string `json:"step_name"`
	Status         string `json:"status"`
	StartedAt      string `json:"started_at,omitempty"`
	FinishedAt     string `json:"finished_at,omitempty"`
	ErrorMessage   string `json:"error_message,omitempty"`
	Truncated      bool   `json:"truncated,omitempty"`
	ResultJSON     string `json:"result_json,omitempty"`
	Server         string `json:"server,omitempty"`
	Tool           string `json:"tool,omitempty"`
	Transport      string `json:"transport,omitempty"`
	RetryCount     int    `json:"retry_count,omitempty"`
	AttemptCount   int    `json:"attempt_count,omitempty"`
	ReusedClient   bool   `json:"reused_client,omitempty"`
	HealthProbe    bool   `json:"health_probe,omitempty"`
	Reconnected    bool   `json:"reconnected,omitempty"`
	Method         string `json:"method,omitempty"`
	URL            string `json:"url,omitempty"`
	StatusCode     int    `json:"status_code,omitempty"`
	DurationMS     int64  `json:"duration_ms,omitempty"`
	Substrate      string `json:"substrate,omitempty"`
	To             string `json:"to,omitempty"`
	CorrelationID  string `json:"correlation_id,omitempty"`
	TimeoutMS      int64  `json:"timeout_ms,omitempty"`
	PollCount      int    `json:"poll_count,omitempty"`
	MessageID      string `json:"message_id,omitempty"`
	LogicalAgentID string `json:"logical_agent_id,omitempty"`
	LaunchID       string `json:"launch_id,omitempty"`
	GateID         string `json:"gate_id,omitempty"`
	Decision       string `json:"decision,omitempty"`
	Prompt         string `json:"prompt,omitempty"`
}

func SummarizeOperations(events []persistence.RunEventRecord) []OperationDiagnostic {
	ordered := append([]persistence.RunEventRecord(nil), events...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].CreatedAt.Equal(ordered[j].CreatedAt) {
			return ordered[i].ID < ordered[j].ID
		}
		return ordered[i].CreatedAt.Before(ordered[j].CreatedAt)
	})

	diags := make([]*OperationDiagnostic, 0)
	seq := 0

	for _, ev := range ordered {
		stepName := nullString(ev.StepName)
		switch ev.EventType {
		case "mcp_call_start":
			seq++
			server, tool := splitStartMessage(nullString(ev.Message))
			diags = append(diags, &OperationDiagnostic{
				Sequence:  seq,
				Kind:      "mcp_call",
				StepName:  stepName,
				Status:    "running",
				StartedAt: ev.CreatedAt.UTC().Format(time.RFC3339Nano),
				Server:    server,
				Tool:      tool,
			})
			continue
		case "http_call_start":
			seq++
			method, rawURL := splitHTTPCallStart(nullString(ev.Message))
			diags = append(diags, &OperationDiagnostic{
				Sequence:  seq,
				Kind:      "http_call",
				StepName:  stepName,
				Status:    "running",
				StartedAt: ev.CreatedAt.UTC().Format(time.RFC3339Nano),
				Method:    method,
				URL:       rawURL,
			})
			continue
		case "message_wait_start":
			seq++
			diag := &OperationDiagnostic{
				Sequence:  seq,
				Kind:      "message_wait",
				StepName:  stepName,
				Status:    "running",
				StartedAt: ev.CreatedAt.UTC().Format(time.RFC3339Nano),
			}
			applyMessageWaitStartPayload(diag, nullString(ev.Message))
			diags = append(diags, diag)
			continue
		case "agent_launch_start":
			seq++
			diag := &OperationDiagnostic{
				Sequence:  seq,
				Kind:      "agent_launch",
				StepName:  stepName,
				Status:    "running",
				StartedAt: ev.CreatedAt.UTC().Format(time.RFC3339Nano),
			}
			applyAgentLaunchStartPayload(diag, nullString(ev.Message))
			diags = append(diags, diag)
			continue
		case "human_gate_waiting":
			seq++
			diag := &OperationDiagnostic{
				Sequence:  seq,
				Kind:      "human_gate",
				StepName:  stepName,
				Status:    "running",
				StartedAt: ev.CreatedAt.UTC().Format(time.RFC3339Nano),
			}
			applyHumanGateWaitingPayload(diag, nullString(ev.Message))
			diags = append(diags, diag)
			continue
		}

		switch ev.EventType {
		case "mcp_call_transport", "mcp_call_retry", "mcp_call_reconnect", "mcp_call_result", "mcp_call_error":
			diag := latestOpenOperation(diags, stepName, "mcp_call")
			if diag == nil {
				continue
			}
			switch ev.EventType {
			case "mcp_call_transport":
				applyTransportPayload(diag, nullString(ev.Message))
			case "mcp_call_retry":
				applyRetryPayload(diag, nullString(ev.Message))
			case "mcp_call_reconnect":
				diag.Reconnected = true
				applyTransportPayload(diag, nullString(ev.Message))
			case "mcp_call_result":
				applyMCPResultPayload(diag, nullString(ev.Message))
				finishOperation(diag, "success", "", ev.CreatedAt)
			case "mcp_call_error":
				finishOperation(diag, "error", nullString(ev.Message), ev.CreatedAt)
			}
		case "http_call_response", "http_call_error":
			diag := latestOpenOperation(diags, stepName, "http_call")
			if diag == nil {
				continue
			}
			if ev.EventType == "http_call_response" {
				applyHTTPCallResultPayload(diag, nullString(ev.Message))
				finishOperation(diag, "success", "", ev.CreatedAt)
				continue
			}
			finishOperation(diag, "error", nullString(ev.Message), ev.CreatedAt)
		case "message_wait_poll", "message_wait_reply", "message_wait_timeout", "message_wait_error":
			diag := latestOpenOperation(diags, stepName, "message_wait")
			if diag == nil {
				continue
			}
			switch ev.EventType {
			case "message_wait_poll":
				diag.PollCount++
			case "message_wait_reply":
				applyMessageWaitReplyPayload(diag, nullString(ev.Message))
				finishOperation(diag, "success", "", ev.CreatedAt)
			case "message_wait_timeout":
				finishOperation(diag, "timeout", nullString(ev.Message), ev.CreatedAt)
			case "message_wait_error":
				finishOperation(diag, "error", nullString(ev.Message), ev.CreatedAt)
			}
		case "agent_launch_result", "agent_launch_error":
			diag := latestOpenOperation(diags, stepName, "agent_launch")
			if diag == nil {
				continue
			}
			if ev.EventType == "agent_launch_result" {
				applyAgentLaunchResultPayload(diag, nullString(ev.Message))
				finishOperation(diag, "success", "", ev.CreatedAt)
				continue
			}
			finishOperation(diag, "error", nullString(ev.Message), ev.CreatedAt)
		case "human_gate_decision", "human_gate_timeout", "human_gate_error":
			diag := latestOpenOperation(diags, stepName, "human_gate")
			if diag == nil {
				continue
			}
			switch ev.EventType {
			case "human_gate_decision":
				applyHumanGateDecisionPayload(diag, nullString(ev.Message))
				finishOperation(diag, "success", "", ev.CreatedAt)
			case "human_gate_timeout":
				finishOperation(diag, "timeout", nullString(ev.Message), ev.CreatedAt)
			case "human_gate_error":
				finishOperation(diag, "error", nullString(ev.Message), ev.CreatedAt)
			}
		}
	}

	out := make([]OperationDiagnostic, 0, len(diags))
	for _, diag := range diags {
		if diag.Status == "running" {
			diag.Status = "unknown"
		}
		out = append(out, *diag)
	}
	return out
}

func FilterAndPageOperations(items []OperationDiagnostic, kind string, limit int, cursor string) ([]OperationDiagnostic, string, int, error) {
	filtered := make([]OperationDiagnostic, 0, len(items))
	kind = strings.TrimSpace(kind)
	for _, item := range items {
		if kind != "" && item.Kind != kind {
			continue
		}
		filtered = append(filtered, item)
	}
	totalCount := len(filtered)

	cursorSeq := 0
	if strings.TrimSpace(cursor) != "" {
		if _, err := fmt.Sscanf(cursor, "%d", &cursorSeq); err != nil {
			return nil, "", 0, fmt.Errorf("invalid cursor")
		}
	}

	start := 0
	if cursorSeq > 0 {
		for start < len(filtered) && filtered[start].Sequence <= cursorSeq {
			start++
		}
	}
	if start >= len(filtered) {
		return []OperationDiagnostic{}, "", totalCount, nil
	}

	if limit <= 0 {
		limit = totalCount
	}
	end := start + limit
	nextCursor := ""
	if end < len(filtered) {
		nextCursor = fmt.Sprintf("%d", filtered[end-1].Sequence)
	} else {
		end = len(filtered)
	}
	page := append([]OperationDiagnostic(nil), filtered[start:end]...)
	return page, nextCursor, totalCount, nil
}

func latestOpenOperation(diags []*OperationDiagnostic, stepName, kind string) *OperationDiagnostic {
	for i := len(diags) - 1; i >= 0; i-- {
		diag := diags[i]
		if diag.StepName != stepName || diag.Kind != kind {
			continue
		}
		switch diag.Status {
		case "success", "error", "timeout":
			continue
		}
		return diag
	}
	return nil
}

func finishOperation(diag *OperationDiagnostic, status, errMsg string, ts time.Time) {
	diag.Status = status
	diag.ErrorMessage = errMsg
	diag.FinishedAt = ts.UTC().Format(time.RFC3339Nano)
}

func splitHTTPCallStart(raw string) (string, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ""
	}
	parts := strings.SplitN(raw, " ", 2)
	if len(parts) != 2 {
		return raw, ""
	}
	return parts[0], parts[1]
}

func applyMCPResultPayload(diag *OperationDiagnostic, raw string) {
	payload := parsePayload(raw)
	diag.Server = stringField(payload, "server", diag.Server)
	diag.Tool = stringField(payload, "tool", diag.Tool)
	diag.Transport = stringField(payload, "transport", diag.Transport)
	diag.ResultJSON = stringField(payload, "result_json", diag.ResultJSON)
	diag.Truncated = boolField(payload, "truncated", diag.Truncated)
	diag.ReusedClient = boolField(payload, "reused_client", diag.ReusedClient)
	diag.HealthProbe = boolField(payload, "health_probe", diag.HealthProbe)
	diag.Reconnected = boolField(payload, "reconnected", diag.Reconnected)
	diag.RetryCount = intField(payload, "retry_count", diag.RetryCount)
	diag.AttemptCount = intField(payload, "attempt_count", diag.AttemptCount)
}

func applyHTTPCallResultPayload(diag *OperationDiagnostic, raw string) {
	payload := parsePayload(raw)
	diag.StatusCode = intField(payload, "status_code", diag.StatusCode)
	diag.DurationMS = int64Field(payload, "duration_ms", diag.DurationMS)
	diag.ResultJSON = stringField(payload, "body_json", diag.ResultJSON)
	if diag.ResultJSON == "" {
		diag.ResultJSON = stringField(payload, "body", diag.ResultJSON)
	}
	diag.Truncated = boolField(payload, "truncated", diag.Truncated)
}

func applyMessageWaitStartPayload(diag *OperationDiagnostic, raw string) {
	payload := parsePayload(raw)
	diag.Substrate = stringField(payload, "substrate", diag.Substrate)
	diag.To = stringField(payload, "to", diag.To)
	diag.CorrelationID = stringField(payload, "correlation_id", diag.CorrelationID)
	diag.TimeoutMS = int64Field(payload, "timeout_ms", diag.TimeoutMS)
}

func applyMessageWaitReplyPayload(diag *OperationDiagnostic, raw string) {
	payload := parsePayload(raw)
	diag.MessageID = stringField(payload, "message_id", diag.MessageID)
	diag.ResultJSON = stringField(payload, "body_json", diag.ResultJSON)
	if diag.ResultJSON == "" {
		diag.ResultJSON = stringField(payload, "body", diag.ResultJSON)
	}
}

func applyAgentLaunchStartPayload(diag *OperationDiagnostic, raw string) {
	payload := parsePayload(raw)
	diag.Substrate = stringField(payload, "substrate", diag.Substrate)
	diag.LaunchID = stringField(payload, "launch_id", diag.LaunchID)
	diag.LogicalAgentID = stringField(payload, "logical_agent_id", diag.LogicalAgentID)
}

func applyAgentLaunchResultPayload(diag *OperationDiagnostic, raw string) {
	payload := parsePayload(raw)
	diag.Substrate = stringField(payload, "substrate", diag.Substrate)
	diag.LaunchID = stringField(payload, "launch_id", diag.LaunchID)
	diag.ResultJSON = stringField(payload, "result_json", diag.ResultJSON)
	diag.Truncated = boolField(payload, "truncated", diag.Truncated)
}

func applyHumanGateWaitingPayload(diag *OperationDiagnostic, raw string) {
	payload := parsePayload(raw)
	diag.GateID = stringField(payload, "gate_id", diag.GateID)
	diag.Prompt = stringField(payload, "prompt", diag.Prompt)
	if timeoutSeconds := intField(payload, "timeout_seconds", 0); timeoutSeconds > 0 {
		diag.TimeoutMS = int64(timeoutSeconds) * 1000
	}
}

func applyHumanGateDecisionPayload(diag *OperationDiagnostic, raw string) {
	payload := parsePayload(raw)
	diag.GateID = stringField(payload, "gate_id", diag.GateID)
	diag.Decision = stringField(payload, "decision", diag.Decision)
}

func int64Field(payload map[string]any, key string, fallback int64) int64 {
	if payload == nil {
		return fallback
	}
	switch v := payload[key].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	case json.Number:
		n, err := v.Int64()
		if err == nil {
			return n
		}
	}
	return fallback
}
