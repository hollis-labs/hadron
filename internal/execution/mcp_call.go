package execution

import (
	"context"
	"encoding/json"
	"fmt"

	feotel "github.com/hollis-labs/go-otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/hollis-labs/hadron/internal/blueprint"
)

const maxMCPCallResultBytes = 65536

func (r *runExecution) executeMCPCallStep(ctx context.Context, section string, step blueprint.Step) error {
	if step.MCPCall == nil {
		return fmt.Errorf("step %q has no mcp_call", step.Name)
	}
	ctx, span := feotel.StartSpan(ctx, "hadron.step.mcp_call")
	span.SetAttributes(
		attribute.String("hadron.section", section),
		attribute.String("hadron.step.name", step.Name),
		attribute.String("hadron.run.id", r.runID),
		attribute.String("hadron.workspace.id", r.workspaceID),
		attribute.String("hadron.mcp.server", step.MCPCall.Server),
		attribute.String("hadron.mcp.tool", step.MCPCall.Tool),
	)
	defer span.End()
	if r.manager.mcpCaller == nil {
		err := fmt.Errorf("mcp_call caller is not configured")
		span.RecordError(err)
		r.emit(section, step.Name, "mcp_call_error", err.Error())
		return err
	}

	call := step.MCPCall
	r.emit(section, step.Name, "mcp_call_start", fmt.Sprintf("%s.%s", call.Server, call.Tool))

	result, err := r.manager.mcpCaller.CallTool(ctx, call.Server, call.Tool, call.Arguments)
	if err != nil {
		span.RecordError(err)
		r.emit(section, step.Name, "mcp_call_error", err.Error())
		return fmt.Errorf("mcp_call: %w", err)
	}

	actualResult := result
	var metadata *MCPCallMetadata
	if wrapped, ok := result.(MCPToolResult); ok {
		actualResult = wrapped.Result
		metadata = &wrapped.Metadata
	}

	if metadata != nil {
		span.SetAttributes(
			attribute.String("hadron.mcp.transport", metadata.Transport),
			attribute.Bool("hadron.mcp.reused_client", metadata.ReusedClient),
			attribute.Bool("hadron.mcp.health_probe", metadata.HealthProbe),
			attribute.Bool("hadron.mcp.reconnected", metadata.Reconnected),
			attribute.Int("hadron.mcp.retry_count", metadata.RetryCount),
			attribute.Int("hadron.mcp.attempt_count", metadata.AttemptCount),
		)
		transportPayload := map[string]any{
			"server":        metadata.Server,
			"tool":          call.Tool,
			"transport":     metadata.Transport,
			"reused_client": metadata.ReusedClient,
			"health_probe":  metadata.HealthProbe,
		}
		transportJSON, _ := json.Marshal(transportPayload)
		r.emit(section, step.Name, "mcp_call_transport", string(transportJSON))

		if metadata.RetryCount > 0 {
			retryJSON, _ := json.Marshal(map[string]any{
				"server":        metadata.Server,
				"tool":          call.Tool,
				"transport":     metadata.Transport,
				"retry_count":   metadata.RetryCount,
				"attempt_count": metadata.AttemptCount,
			})
			r.emit(section, step.Name, "mcp_call_retry", string(retryJSON))
		}
		if metadata.Reconnected {
			reconnectJSON, _ := json.Marshal(map[string]any{
				"server":       metadata.Server,
				"tool":         call.Tool,
				"transport":    metadata.Transport,
				"health_probe": metadata.HealthProbe,
			})
			r.emit(section, step.Name, "mcp_call_reconnect", string(reconnectJSON))
		}
	}

	resultJSONBytes, err := json.Marshal(actualResult)
	if err != nil {
		span.RecordError(err)
		r.emit(section, step.Name, "mcp_call_error", err.Error())
		return fmt.Errorf("mcp_call marshal result: %w", err)
	}
	resultJSON, truncated := truncateJSONBytes(resultJSONBytes, maxMCPCallResultBytes)

	eventPayload := map[string]any{
		"server":      call.Server,
		"tool":        call.Tool,
		"result_json": resultJSON,
		"truncated":   truncated,
	}
	if metadata != nil {
		eventPayload["transport"] = metadata.Transport
		eventPayload["reused_client"] = metadata.ReusedClient
		eventPayload["health_probe"] = metadata.HealthProbe
		eventPayload["reconnected"] = metadata.Reconnected
		eventPayload["retry_count"] = metadata.RetryCount
		eventPayload["attempt_count"] = metadata.AttemptCount
	}
	eventJSON, _ := json.Marshal(eventPayload)
	r.emit(section, step.Name, "mcp_call_result", string(eventJSON))
	r.emit(section, step.Name, "log", fmt.Sprintf("::set-output result_json=%s", resultJSON))
	return nil
}

func truncateJSONBytes(b []byte, limit int) (string, bool) {
	if len(b) <= limit {
		return string(b), false
	}
	return string(b[:limit]), true
}
