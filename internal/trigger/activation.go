package trigger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
)

// ActivationEvent is the credential-free handoff from an authenticated
// webhook, file watcher, or external trigger adapter. Transport headers,
// bearer credentials, and raw callback tokens are intentionally absent.
type ActivationEvent struct {
	RegistrationID string
	IdempotencyKey string
	OccurredAt     time.Time
	ReceivedAt     time.Time
	Payload        map[string]any
	LogicalRunID   string
	SourceRef      string
}

type ActivationManager struct{ Service appworkflow.ActivationService }

// LoadRegistration exposes the exact graph-native activation definition to an
// authenticated transport before it accepts a credential-free event. An
// opaque registration ID is never treated as authority.
func (m ActivationManager) LoadRegistration(ctx context.Context, id string) (hoststate.ActivationRegistration, error) {
	if ctx == nil || m.Service.Store == nil {
		return hoststate.ActivationRegistration{}, appworkflow.ErrHostNotReady
	}
	return m.Service.Store.LoadActivation(ctx, id)
}

func (m ActivationManager) Fire(ctx context.Context, event ActivationEvent) (appworkflow.ActivationStartResult, error) {
	if ctx == nil {
		return appworkflow.ActivationStartResult{}, errors.New("trigger activation requires context")
	}
	payload, err := cloneActivationPayload(event.Payload)
	if err != nil {
		return appworkflow.ActivationStartResult{}, err
	}
	return m.Service.ActivateExternal(ctx, appworkflow.ExternalActivationRequest{
		RegistrationID: event.RegistrationID, IdempotencyKey: event.IdempotencyKey,
		OccurredAt: event.OccurredAt.UTC(), ReceivedAt: event.ReceivedAt.UTC(), Payload: payload,
		LogicalRunID: event.LogicalRunID, SourceRef: event.SourceRef,
	})
}

func cloneActivationPayload(input map[string]any) (map[string]any, error) {
	if input == nil {
		return map[string]any{}, nil
	}
	encoded, err := json.Marshal(input)
	if err != nil || len(encoded) > hoststate.MaximumActivationPayloadBytes {
		return nil, fmt.Errorf("trigger activation payload is invalid or too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var result map[string]any
	if err := decoder.Decode(&result); err != nil || result == nil {
		return nil, errors.New("trigger activation payload is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("trigger activation payload has trailing content")
	}
	return result, nil
}
