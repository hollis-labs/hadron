package service

import (
	"context"
	"errors"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
)

const (
	KindName    = "service"
	KindVersion = "v1"
)

var (
	ErrInvalidOptions = errors.New("invalid service adapter options")
	ErrInvalidConfig  = errors.New("invalid service configuration")
)

// StartRequest is the immutable, idempotent provider request. Config is an
// adapter-owned JSON object; hosts must defensively copy it and persist only
// its digest plus non-secret provider references.
type StartRequest struct {
	Identity       stepkind.InvocationIdentity `json:"identity"`
	Provider       string                      `json:"provider"`
	Config         graph.Config                `json:"config"`
	IdempotencyKey string                      `json:"idempotency_key"`
}

// Host is the provider binding. Start and RequestStop must be exactly
// idempotent for their keys; Observe must not retain caller-owned aliases.
type Host interface {
	Start(context.Context, StartRequest) (stepkind.ExternalOperationRef, error)
	Observe(context.Context, stepkind.ExternalOperationRef) (stepkind.ServiceObservation, error)
	RequestStop(context.Context, stepkind.ExternalOperationRef, string) error
}

type Options struct{ Host Host }
