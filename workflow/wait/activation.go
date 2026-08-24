package wait

import (
	"context"
	"fmt"
	"time"

	"github.com/hollis-labs/hadron/workflow/values"
)

type ActivationID string

// Activation is the core's application-neutral timed activation request.
type Activation struct {
	ID         ActivationID        `json:"id"`
	Kind       string              `json:"kind"`
	RunID      string              `json:"run_id"`
	NodeID     string              `json:"node_id"`
	Iteration  string              `json:"iteration,omitempty"`
	WaitID     string              `json:"wait_id,omitempty"`
	FireAt     time.Time           `json:"fire_at"`
	DedupKey   string              `json:"dedup_key"`
	PayloadRef *values.ValueSetRef `json:"payload_ref,omitempty"`
}

func (a Activation) Validate() error {
	fields := []struct{ name, value string }{
		{"activation id", string(a.ID)},
		{"activation kind", a.Kind},
		{"activation run id", a.RunID},
		{"activation node id", a.NodeID},
		{"activation dedup key", a.DedupKey},
	}
	for _, field := range fields {
		if err := requiredText(field.name, field.value); err != nil {
			return err
		}
	}
	if err := optionalText("activation iteration", a.Iteration); err != nil {
		return err
	}
	if err := optionalText("activation wait id", a.WaitID); err != nil {
		return err
	}
	if a.FireAt.IsZero() {
		return fmt.Errorf("activation fire_at is required")
	}
	if a.PayloadRef != nil {
		return a.PayloadRef.Validate()
	}
	return nil
}

// ActivationScheduler is implemented by a host scheduler adapter. Schedule
// and Cancel must be idempotent by activation identity.
type ActivationScheduler interface {
	Schedule(context.Context, Activation) error
	Cancel(context.Context, ActivationID) error
}

// Materializer lets an application expose a durable wait as a one-shot
// endpoint or job with a TTL. Calls must be idempotent by wait identity.
type Materializer interface {
	Materialize(context.Context, Materialization) error
	Resolve(context.Context, Materialization) error
}

// Materialization contains no raw resume token. Opaque is host-owned but must
// not be logged by core.
type Materialization struct {
	WaitID    string    `json:"wait_id"`
	Kind      Kind      `json:"kind"`
	ResumeURL string    `json:"resume_url,omitempty"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
	Opaque    string    `json:"-"`
}
