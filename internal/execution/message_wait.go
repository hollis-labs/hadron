package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	feotel "github.com/hollis-labs/go-otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/hollis-labs/hadron/internal/blueprint"
)

const maxMessageWaitBodyBytes = 65536

func (r *runExecution) executeMessageWaitStep(ctx context.Context, section string, step blueprint.Step) error {
	if step.MessageWait == nil {
		return fmt.Errorf("step %q has no message_wait", step.Name)
	}
	ctx, span := feotel.StartSpan(ctx, "hadron.step.message_wait")
	span.SetAttributes(
		attribute.String("hadron.section", section),
		attribute.String("hadron.step.name", step.Name),
		attribute.String("hadron.run.id", r.runID),
		attribute.String("hadron.workspace.id", r.workspaceID),
		attribute.String("hadron.message.substrate", step.MessageWait.Substrate),
		attribute.String("hadron.message.to", step.MessageWait.To),
		attribute.String("hadron.message.correlation_id", step.MessageWait.CorrelationID),
		attribute.Int("hadron.message.timeout_seconds", step.MessageWait.TimeoutSeconds),
	)
	defer span.End()
	if r.manager.messages == nil {
		err := fmt.Errorf("message_wait source is not configured")
		span.RecordError(err)
		r.emit(section, step.Name, "message_wait_error", err.Error())
		return err
	}

	wait := step.MessageWait
	timeout := time.Duration(wait.TimeoutSeconds) * time.Second
	pollInterval := 1 * time.Second
	if wait.PollIntervalSeconds > 0 {
		pollInterval = time.Duration(wait.PollIntervalSeconds) * time.Second
	}
	query := MessageQuery{
		Substrate:     wait.Substrate,
		To:            wait.To,
		CorrelationID: wait.CorrelationID,
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	startPayload, _ := json.Marshal(map[string]any{
		"substrate":      query.Substrate,
		"to":             query.To,
		"correlation_id": query.CorrelationID,
		"timeout_ms":     timeout.Milliseconds(),
	})
	r.emit(section, step.Name, "message_wait_start", string(startPayload))

	for {
		msg, err := r.manager.messages.PollMessage(waitCtx, query)
		if err != nil {
			span.RecordError(err)
			r.emit(section, step.Name, "message_wait_error", err.Error())
			return fmt.Errorf("message_wait: %w", err)
		}
		if msg != nil {
			span.SetAttributes(attribute.String("hadron.message.id", msg.ID))
			r.emitMessageWaitReply(section, step.Name, msg)
			return nil
		}

		r.emit(section, step.Name, "message_wait_poll", "no matching message")
		timer := time.NewTimer(pollInterval)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
				err := fmt.Errorf("message_wait timed out after %s", timeout)
				span.RecordError(err)
				r.emit(section, step.Name, "message_wait_timeout", err.Error())
				return err
			}
			span.RecordError(waitCtx.Err())
			r.emit(section, step.Name, "message_wait_error", waitCtx.Err().Error())
			return waitCtx.Err()
		case <-timer.C:
		}
	}
}

func (r *runExecution) emitMessageWaitReply(section, stepName string, msg *Message) {
	body := truncateStringBytes(msg.Body, maxMessageWaitBodyBytes)
	bodyJSON := ""
	if msg.BodyJSON != nil {
		b, err := json.Marshal(msg.BodyJSON)
		if err == nil {
			bodyJSON = truncateStringBytes(string(b), maxMessageWaitBodyBytes)
		}
	} else {
		bodyJSON = compactJSON(body)
	}

	payload := map[string]any{
		"message_id": msg.ID,
		"body":       body,
		"body_json":  bodyJSON,
	}
	eventJSON, _ := json.Marshal(payload)
	r.emit(section, stepName, "message_wait_reply", string(eventJSON))
	r.emit(section, stepName, "log", fmt.Sprintf("::set-output message_id=%s", sanitizeSetOutputValue(msg.ID)))
	r.emit(section, stepName, "log", fmt.Sprintf("::set-output body=%s", sanitizeSetOutputValue(body)))
	if bodyJSON != "" {
		r.emit(section, stepName, "log", fmt.Sprintf("::set-output body_json=%s", bodyJSON))
	}
}

func truncateStringBytes(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return s[:limit]
}
