package messagesubstrate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/go-messaging"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
	workflowwait "github.com/hollis-labs/go-workflow/wait"
)

// WaitResumer is the narrow generic resume surface required by message
// delivery. *runtime.WaitCoordinator satisfies it; the bridge does not own
// wait state, duplicate handling, authorization, or timeout decisions.
type WaitResumer interface {
	Resume(context.Context, workflowruntime.ResumeCommand) (workflowruntime.ResumeWaitResult, error)
}

// MessageWake binds an already-delivered envelope to one expected generic
// wait. It deliberately contains no resume token: message identity is checked
// by the runtime's injected responder authorizer.
type MessageWake struct {
	WaitID      workflowruntime.WaitID
	Substrate   string
	Correlation string
	Envelope    messaging.Envelope
	ReceivedAt  time.Time
}

// WaitBridge converts arrival-driven message delivery into the canonical
// generic resume command. It performs no polling and persists no event or
// workflow value itself.
type WaitBridge struct{ Resumer WaitResumer }

// ResumeMessage validates and resumes one message-backed wait. Raw payload is
// never copied into errors, responder metadata, or idempotency metadata.
func (b WaitBridge) ResumeMessage(ctx context.Context, wake MessageWake) (workflowruntime.ResumeWaitResult, error) {
	if ctx == nil {
		return workflowruntime.ResumeWaitResult{}, safeBridgeError("context is required", nil)
	}
	if err := ctx.Err(); err != nil {
		return workflowruntime.ResumeWaitResult{}, err
	}
	if nilInterface(b.Resumer) {
		return workflowruntime.ResumeWaitResult{}, safeBridgeError("resume coordinator is unavailable", nil)
	}
	if err := validateWake(wake); err != nil {
		return workflowruntime.ResumeWaitResult{}, safeBridgeError("message wake is invalid", err)
	}
	payload, err := decodePayload(wake.Envelope.Payload)
	if err != nil {
		return workflowruntime.ResumeWaitResult{}, safeBridgeError("message payload is invalid JSON", err)
	}
	value, err := values.NewInline(payload, values.Metadata{
		Producer:  values.Producer{Kind: "message", Reference: wake.Envelope.ID, Output: "payload"},
		MediaType: messageMediaType(wake.Envelope.ContentType), Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		return workflowruntime.ResumeWaitResult{}, safeBridgeError("message payload cannot become a workflow value", err)
	}
	responder := workflowwait.Responder{
		Kind: "message_sender", Reference: wake.Envelope.From.URN(),
		Attributes: map[string]string{"message_id": wake.Envelope.ID, "substrate": wake.Substrate, "to": wake.Envelope.To.URN()},
	}
	result, err := b.Resumer.Resume(ctx, workflowruntime.ResumeCommand{
		WaitID: wake.WaitID, Correlation: wake.Correlation, WakeSource: workflowwait.WakeMessage,
		Responder: responder, Payload: value, IdempotencyKey: messageIdempotencyKey(wake.Substrate, wake.Envelope.ID), ReceivedAt: wake.ReceivedAt.UTC(),
	})
	if err != nil {
		return result, safeBridgeError("message resume failed", err)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return result, contextErr
	}
	return result, nil
}

func validateWake(wake MessageWake) error {
	if err := (workflowruntime.WaitRef{ID: wake.WaitID}).Validate(); err != nil {
		return err
	}
	if !stableText(wake.Substrate, 256) || !stableText(wake.Correlation, 4096) || !stableText(wake.Envelope.ID, 4096) {
		return errors.New("substrate, correlation, and message id are required stable text")
	}
	if wake.ReceivedAt.IsZero() {
		return errors.New("received_at is required")
	}
	from := wake.Envelope.From.URN()
	parsed, err := messaging.ParseURN(from)
	if err != nil || parsed != wake.Envelope.From || parsed.URN() != from {
		return errors.New("sender address is not canonical")
	}
	to := wake.Envelope.To.URN()
	parsed, err = messaging.ParseURN(to)
	if err != nil || parsed != wake.Envelope.To || parsed.URN() != to {
		return errors.New("destination address is not canonical")
	}
	envelopeCorrelation := firstNonEmpty(wake.Envelope.Metadata["correlation_id"], wake.Envelope.ThreadID, wake.Envelope.InReplyTo)
	if envelopeCorrelation == "" || envelopeCorrelation != wake.Correlation {
		return errors.New("message correlation does not match wake")
	}
	if wake.Envelope.ContentType != "" && wake.Envelope.ContentType != "application/json" {
		return errors.New("message wait supports application/json payloads only")
	}
	return nil
}

func messageIdempotencyKey(substrate, messageID string) string {
	encoded, _ := json.Marshal([]string{substrate, messageID})
	digest := values.SHA256Digest(encoded)
	return "message-wake:" + strings.TrimPrefix(digest, "sha256:")
}

func decodePayload(raw json.RawMessage) (any, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("multiple JSON values")
	}
	return value, nil
}

func messageMediaType(contentType string) string {
	if contentType == "" {
		return "application/json"
	}
	return contentType
}

func safeBridgeError(message string, cause error) error {
	return &messageWakeError{message: message, cause: cause}
}

type messageWakeError struct {
	message string
	cause   error
}

func (e *messageWakeError) Error() string { return "message wait bridge: " + e.message }
func (e *messageWakeError) Unwrap() error { return e.cause }

func stableText(value string, maximum int) bool {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) || len(value) > maximum {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
