package messagesubstrate_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/hadron/internal/messagesubstrate"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
	workflowwait "github.com/hollis-labs/go-workflow/wait"
)

func TestWaitBridgeBuildsCanonicalTypedResumeCommand(t *testing.T) {
	resumer := &recordingResumer{result: workflowruntime.ResumeWaitResult{Outcome: workflowruntime.ResumeApplied}}
	wake := testMessageWake()
	result, err := (messagesubstrate.WaitBridge{Resumer: resumer}).ResumeMessage(t.Context(), wake)
	if err != nil || result.Outcome != workflowruntime.ResumeApplied || len(resumer.commands) != 1 {
		t.Fatalf("ResumeMessage = %#v, %v, commands=%d", result, err, len(resumer.commands))
	}
	command := resumer.commands[0]
	payload, ok := command.Payload.Inline.(map[string]any)
	number, numberOK := payload["count"].(json.Number)
	if !ok || !numberOK || number.String() != "9007199254740993" || command.Payload.Redaction != values.RedactionPrivate || command.Payload.Retention != values.RetentionRun ||
		command.Payload.Producer.Kind != "message" || command.Payload.Producer.Reference != wake.Envelope.ID || command.WakeSource != workflowwait.WakeMessage {
		t.Fatalf("resume command payload = %#v", command)
	}
	if command.Responder.Reference != wake.Envelope.From.URN() || command.Responder.Attributes["to"] != wake.Envelope.To.URN() ||
		command.Responder.Attributes["message_id"] != wake.Envelope.ID || command.IdempotencyKey == wake.Envelope.ID || !strings.HasPrefix(command.IdempotencyKey, "message-wake:") {
		t.Fatalf("resume provenance = %#v", command)
	}

	other := wake
	other.Substrate = "remote"
	if _, err := (messagesubstrate.WaitBridge{Resumer: resumer}).ResumeMessage(t.Context(), other); err != nil {
		t.Fatal(err)
	}
	if resumer.commands[0].IdempotencyKey == resumer.commands[1].IdempotencyKey {
		t.Fatalf("substrate-scoped idempotency collided: %q", resumer.commands[0].IdempotencyKey)
	}
}

func TestWaitBridgeRejectsMalformedDeliveryWithoutPayloadLeak(t *testing.T) {
	base := testMessageWake()
	tests := []struct {
		name   string
		mutate func(*messagesubstrate.MessageWake)
	}{
		{"sender", func(w *messagesubstrate.MessageWake) { w.Envelope.From.Kind = "unknown" }},
		{"destination", func(w *messagesubstrate.MessageWake) { w.Envelope.To = messaging.Address{} }},
		{"correlation", func(w *messagesubstrate.MessageWake) { w.Correlation = "different" }},
		{"content-type", func(w *messagesubstrate.MessageWake) { w.Envelope.ContentType = "text/plain" }},
		{"json", func(w *messagesubstrate.MessageWake) { w.Envelope.Payload = json.RawMessage(`{"secret-payload":`) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wake := base
			test.mutate(&wake)
			_, err := (messagesubstrate.WaitBridge{Resumer: &recordingResumer{}}).ResumeMessage(t.Context(), wake)
			if err == nil || strings.Contains(err.Error(), "secret-payload") || strings.Contains(err.Error(), string(wake.Envelope.Payload)) {
				t.Fatalf("unsafe error = %v", err)
			}
		})
	}
}

func TestWaitBridgePropagatesDuplicateAndContextSafely(t *testing.T) {
	duplicate := workflowruntime.ResumeWaitResult{Outcome: workflowruntime.ResumeReplayed}
	resumer := &recordingResumer{result: duplicate}
	result, err := (messagesubstrate.WaitBridge{Resumer: resumer}).ResumeMessage(t.Context(), testMessageWake())
	if err != nil || result.Outcome != workflowruntime.ResumeReplayed {
		t.Fatalf("duplicate propagation = %#v, %v", result, err)
	}

	secretCause := errors.New("backend accidentally included secret-payload")
	resumer.err = secretCause
	result, err = (messagesubstrate.WaitBridge{Resumer: resumer}).ResumeMessage(t.Context(), testMessageWake())
	if !errors.Is(err, secretCause) || strings.Contains(err.Error(), "secret-payload") {
		t.Fatalf("safe wrapped resume error = %#v, %v", result, err)
	}

	var nilResumer *recordingResumer
	if _, err := (messagesubstrate.WaitBridge{Resumer: nilResumer}).ResumeMessage(t.Context(), testMessageWake()); err == nil {
		t.Fatal("typed-nil resumer accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (messagesubstrate.WaitBridge{Resumer: &recordingResumer{}}).ResumeMessage(canceled, testMessageWake()); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled context = %v", err)
	}
	late, cancelLate := context.WithCancel(context.Background())
	resumer = &recordingResumer{cancel: cancelLate}
	if _, err := (messagesubstrate.WaitBridge{Resumer: resumer}).ResumeMessage(late, testMessageWake()); !errors.Is(err, context.Canceled) {
		t.Fatalf("late canceled context = %v", err)
	}
}

type recordingResumer struct {
	commands []workflowruntime.ResumeCommand
	result   workflowruntime.ResumeWaitResult
	err      error
	cancel   context.CancelFunc
}

func (r *recordingResumer) Resume(_ context.Context, command workflowruntime.ResumeCommand) (workflowruntime.ResumeWaitResult, error) {
	r.commands = append(r.commands, command)
	if r.cancel != nil {
		r.cancel()
	}
	return r.result, r.err
}

func testMessageWake() messagesubstrate.MessageWake {
	return messagesubstrate.MessageWake{
		WaitID: "wait-message", Substrate: "local", Correlation: "thread-1", ReceivedAt: time.Date(2026, 8, 24, 17, 0, 0, 0, time.UTC),
		Envelope: messaging.Envelope{
			ID: "message-1", Kind: messaging.MsgKindResponse,
			From:     messaging.Address{Kind: messaging.KindUser, Authority: "project", ID: "alice"},
			To:       messaging.Address{Kind: messaging.KindWorkflow, Authority: "project", ID: "run-1"},
			ThreadID: "thread-1", ContentType: "application/json", Payload: json.RawMessage(`{"count":9007199254740993}`),
		},
	}
}
