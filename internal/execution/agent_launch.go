package execution

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hollis-labs/hadron/internal/blueprint"
)

const maxAgentLaunchResultBytes = 65536

func (r *runExecution) executeAgentLaunchStep(ctx context.Context, blueprintPath, section string, step blueprint.Step) error {
	if step.AgentLaunch == nil {
		return fmt.Errorf("step %q has no agent_launch", step.Name)
	}
	if r.manager.agents == nil {
		err := fmt.Errorf("agent_launch launcher is not configured")
		r.emit(section, step.Name, "agent_launch_error", err.Error())
		return err
	}

	req := agentLaunchRequestFromBlueprint(step.AgentLaunch, blueprintPath, step.Dir)
	startPayload, _ := json.Marshal(map[string]any{
		"substrate":        req.Substrate,
		"launch_id":        req.LaunchID,
		"logical_agent_id": req.LogicalAgentID,
		"injection":        req.Injection,
		"metadata":         req.Metadata,
	})
	r.emit(section, step.Name, "agent_launch_start", string(startPayload))

	stepCtx := ctx
	if timeout := stepTimeoutSeconds(step, r.manager.settings); timeout > 0 {
		var cancel context.CancelFunc
		stepCtx, cancel = context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
		defer cancel()
	}

	result, err := r.manager.agents.LaunchAgent(stepCtx, req)
	if err != nil {
		r.emit(section, step.Name, "agent_launch_error", err.Error())
		return fmt.Errorf("agent_launch: %w", err)
	}

	outputs := map[string]any{
		"session_id":  result.SessionID,
		"mailbox":     result.Mailbox,
		"mailbox_urn": result.Mailbox,
	}
	for k, v := range result.Handles {
		outputs[k] = v
	}
	outputJSONBytes, err := json.Marshal(outputs)
	if err != nil {
		r.emit(section, step.Name, "agent_launch_error", err.Error())
		return fmt.Errorf("agent_launch marshal result: %w", err)
	}
	outputJSON, truncated := truncateJSONBytes(outputJSONBytes, maxAgentLaunchResultBytes)

	eventPayload := map[string]any{
		"substrate":   req.Substrate,
		"launch_id":   req.LaunchID,
		"result_json": outputJSON,
		"truncated":   truncated,
	}
	eventJSON, _ := json.Marshal(eventPayload)
	r.emit(section, step.Name, "agent_launch_result", string(eventJSON))
	r.emit(section, step.Name, "log", fmt.Sprintf("::set-output session_id=%s", sanitizeSetOutputValue(result.SessionID)))
	r.emit(section, step.Name, "log", fmt.Sprintf("::set-output mailbox=%s", sanitizeSetOutputValue(result.Mailbox)))
	r.emit(section, step.Name, "log", fmt.Sprintf("::set-output mailbox_urn=%s", sanitizeSetOutputValue(result.Mailbox)))
	r.emit(section, step.Name, "log", fmt.Sprintf("::set-output result_json=%s", outputJSON))
	for k, v := range result.Handles {
		r.emit(section, step.Name, "log", fmt.Sprintf("::set-output %s=%s", k, sanitizeSetOutputValue(outputValueString(v))))
	}
	return nil
}

func agentLaunchRequestFromBlueprint(in *blueprint.AgentLaunch, blueprintPath, stepDir string) AgentLaunchRequest {
	files := make([]AgentNativeFile, 0, len(in.Injection.NativeFiles))
	for _, file := range in.Injection.NativeFiles {
		files = append(files, AgentNativeFile{
			RelPath: file.RelPath,
			Source:  file.Source,
		})
	}
	return AgentLaunchRequest{
		Substrate:      in.Substrate,
		LaunchID:       in.LaunchID,
		LogicalAgentID: in.LogicalAgentID,
		PromptAppend:   in.PromptAppend,
		BlueprintPath:  blueprintPath,
		StepDir:        stepDir,
		Injection: AgentInjection{
			NativeFiles: files,
		},
		Metadata: in.Metadata,
	}
}

func stepTimeoutSeconds(step blueprint.Step, settings SettingsValidator) int {
	timeout := step.TimeoutSeconds
	if timeout == 0 && settings != nil {
		timeout = settings.GetDefaultTimeout()
	}
	return timeout
}

func outputValueString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case fmt.Stringer:
		return t.String()
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(b)
	}
}
