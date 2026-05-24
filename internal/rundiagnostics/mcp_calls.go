package rundiagnostics

import (
	"database/sql"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/hollis-labs/hadron/internal/persistence"
)

type MCPCallDiagnostic struct {
	Sequence     int    `json:"sequence"`
	StepName     string `json:"step_name"`
	Server       string `json:"server,omitempty"`
	Tool         string `json:"tool,omitempty"`
	Transport    string `json:"transport,omitempty"`
	Status       string `json:"status"`
	RetryCount   int    `json:"retry_count"`
	AttemptCount int    `json:"attempt_count"`
	ReusedClient bool   `json:"reused_client,omitempty"`
	HealthProbe  bool   `json:"health_probe,omitempty"`
	Reconnected  bool   `json:"reconnected,omitempty"`
	Truncated    bool   `json:"truncated,omitempty"`
	ResultJSON   string `json:"result_json,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
	StartedAt    string `json:"started_at,omitempty"`
	FinishedAt   string `json:"finished_at,omitempty"`
}

func SummarizeMCPCalls(events []persistence.RunEventRecord) []MCPCallDiagnostic {
	ops := SummarizeOperations(events)
	out := make([]MCPCallDiagnostic, 0, len(ops))
	for _, op := range ops {
		if op.Kind != "mcp_call" {
			continue
		}
		out = append(out, MCPCallDiagnostic{
			Sequence:     op.Sequence,
			StepName:     op.StepName,
			Server:       op.Server,
			Tool:         op.Tool,
			Transport:    op.Transport,
			Status:       op.Status,
			RetryCount:   op.RetryCount,
			AttemptCount: op.AttemptCount,
			ReusedClient: op.ReusedClient,
			HealthProbe:  op.HealthProbe,
			Reconnected:  op.Reconnected,
			Truncated:    op.Truncated,
			ResultJSON:   op.ResultJSON,
			ErrorMessage: op.ErrorMessage,
			StartedAt:    op.StartedAt,
			FinishedAt:   op.FinishedAt,
		})
	}
	return out
}

func applyTransportPayload(diag *OperationDiagnostic, raw string) {
	payload := parsePayload(raw)
	diag.Server = stringField(payload, "server", diag.Server)
	diag.Tool = stringField(payload, "tool", diag.Tool)
	diag.Transport = stringField(payload, "transport", diag.Transport)
	diag.ReusedClient = boolField(payload, "reused_client", diag.ReusedClient)
	diag.HealthProbe = boolField(payload, "health_probe", diag.HealthProbe)
}

func applyRetryPayload(diag *OperationDiagnostic, raw string) {
	payload := parsePayload(raw)
	diag.Server = stringField(payload, "server", diag.Server)
	diag.Tool = stringField(payload, "tool", diag.Tool)
	diag.Transport = stringField(payload, "transport", diag.Transport)
	diag.RetryCount = intField(payload, "retry_count", diag.RetryCount)
	diag.AttemptCount = intField(payload, "attempt_count", diag.AttemptCount)
}

func parsePayload(raw string) map[string]any {
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil
	}
	return payload
}

func splitStartMessage(raw string) (string, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ""
	}
	parts := strings.SplitN(raw, ".", 2)
	if len(parts) != 2 {
		return raw, ""
	}
	return parts[0], parts[1]
}

func stringField(payload map[string]any, key, fallback string) string {
	if payload == nil {
		return fallback
	}
	if v, ok := payload[key].(string); ok && strings.TrimSpace(v) != "" {
		return v
	}
	return fallback
}

func boolField(payload map[string]any, key string, fallback bool) bool {
	if payload == nil {
		return fallback
	}
	if v, ok := payload[key].(bool); ok {
		return v
	}
	return fallback
}

func intField(payload map[string]any, key string, fallback int) int {
	if payload == nil {
		return fallback
	}
	switch v := payload[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case string:
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return fallback
}

func nullString(s sql.NullString) string {
	if s.Valid {
		return s.String
	}
	return ""
}
