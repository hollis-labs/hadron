// Package tetherhost implements go-workflow's agent SessionHost on top of the
// Tether daemon (muxd). Each agent_session@v1 step becomes one keyed Tether
// session; the agent returns its result as a nonce-bearing reply on the step's
// correlation thread, which Hadron reads back when it observes the session.
//
// The package talks to muxd only through the narrow Client interface below.
// The production adapter over go-tether-client is a separate, later change;
// tests use the in-process fake in package tethertest.
package tetherhost

import (
	"context"
	"encoding/json"
	"errors"
)

// Client is the part of the Tether daemon API the session host needs.
// Implementations classify failures with the package sentinels so the host
// never inspects transport errors.
type Client interface {
	// CreateSession creates a session, or replays the one already created
	// under IdempotencyKey (Replayed=true). A different request under the
	// same key fails ErrIdempotencyConflict.
	CreateSession(ctx context.Context, req CreateRequest) (Session, error)
	// LaunchSession starts a created session. It is idempotent for keyed
	// sessions: an already launched session returns Replayed=true.
	LaunchSession(ctx context.Context, id string) (Session, error)
	// GetSession reads the session lifecycle state.
	GetSession(ctx context.Context, id string) (SessionState, error)
	// SessionHealth reports whether the session's process is alive.
	SessionHealth(ctx context.Context, id string) (Health, error)
	// StopSession stops a session. Stopping a session that is not running is
	// not an error.
	StopSession(ctx context.Context, id string) error
	// ThreadReplies lists a message thread oldest first, read-only.
	ThreadReplies(ctx context.Context, threadID string) ([]Reply, error)
}

// CreateRequest is one keyed Tether session creation. Tether's idempotency
// digest covers every field, so a retry must rebuild it byte-for-byte.
type CreateRequest struct {
	Launch         string
	PromptAppend   string
	IdempotencyKey string
}

// Session identifies a created or launched session.
type Session struct {
	ID       string
	Replayed bool
}

// Tether session lifecycle states.
const (
	StateCreated   = "created"
	StateReady     = "ready"
	StateLaunching = "launching"
	StateRunning   = "running"
	StateCompleted = "completed"
	StateFailed    = "failed"
	StateKilled    = "killed"
)

// SessionState is a session's lifecycle state. ExitCode is set for exited
// sessions; -1 marks a session muxd's restart sweep failed because its
// process was lost.
type SessionState struct {
	ID       string
	State    string
	ExitCode *int
}

// Health is a session liveness probe.
type Health struct {
	Alive bool
}

// Reply is one message on a thread. From is asserted by the sender and is
// not authenticated by Tether (same-host trust).
type Reply struct {
	MessageID string
	Kind      string
	From      string
	To        string
	Payload   json.RawMessage
}

var (
	// ErrUnavailable means muxd could not be reached. It is always retryable.
	ErrUnavailable = errors.New("tether daemon unavailable")
	// ErrIdempotencyConflict is Tether's 409 idempotency_conflict: the key
	// was already used for a different request.
	ErrIdempotencyConflict = errors.New("tether idempotency conflict")
	// ErrNotFound means the session or thread does not exist.
	ErrNotFound = errors.New("tether resource not found")
)
