package tetherhost

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	messaging "github.com/hollis-labs/go-messaging"
	tether "github.com/hollis-labs/go-tether-client"
)

// TetherClient adapts go-tether-client to Client. Constructing it never
// dials muxd: every call connects on demand, so a daemon that is down at
// hadrond startup surfaces later as ErrUnavailable, not a startup failure.
type TetherClient struct {
	client *tether.Client
	store  messaging.Store
}

var _ Client = (*TetherClient)(nil)

// NewTetherClient builds the adapter for endpoint: a socket path (absolute,
// or "~/"-relative), or any address go-tether-client accepts ("unix:/path",
// "tcp:host:port", "http(s)://host"). Empty means the default muxd socket.
// Thread reads assert ResultMailbox as the caller (?as=), which scopes them
// to the result replies addressed to Hadron.
func NewTetherClient(endpoint string, options ...tether.Option) (*TetherClient, error) {
	address, err := listenAddress(endpoint)
	if err != nil {
		return nil, err
	}
	options = append(options, tether.WithSelfURN(ResultMailbox))
	client, err := tether.New(address, options...)
	if err != nil {
		return nil, fmt.Errorf("tether client for %q: %w", endpoint, err)
	}
	return &TetherClient{client: client, store: client.AsStore()}, nil
}

// listenAddress turns a settings endpoint into a go-tether-client address.
func listenAddress(endpoint string) (string, error) {
	endpoint = strings.TrimSpace(endpoint)
	switch {
	case endpoint == "":
		return tether.DefaultListenAddr, nil
	case strings.HasPrefix(endpoint, "unix:"), strings.HasPrefix(endpoint, "tcp:"),
		strings.HasPrefix(endpoint, "http://"), strings.HasPrefix(endpoint, "https://"):
		return endpoint, nil
	case endpoint == "~" || strings.HasPrefix(endpoint, "~/"):
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("expand tether endpoint %q: %w", endpoint, err)
		}
		return "unix:" + filepath.Join(home, strings.TrimPrefix(endpoint, "~")), nil
	case filepath.IsAbs(endpoint):
		return "unix:" + endpoint, nil
	default:
		return "", fmt.Errorf("tether endpoint %q must be an absolute or ~/ socket path, unix:, tcp:, or http(s) address", endpoint)
	}
}

// CreateSession creates or replays a keyed session (201 fresh, 200 replay).
func (c *TetherClient) CreateSession(ctx context.Context, request CreateRequest) (Session, error) {
	response, err := c.client.CreateSessionWithInput(ctx, tether.LaunchRequest{
		Launch: request.Launch, PromptAppend: request.PromptAppend, IdempotencyKey: request.IdempotencyKey,
	})
	if err != nil {
		return Session{}, mapTetherError(ctx, err)
	}
	return Session{ID: response.ID, Replayed: response.Replayed}, nil
}

// LaunchSession launches a created session; a keyed session already past
// created answers replayed=true.
func (c *TetherClient) LaunchSession(ctx context.Context, id string) (Session, error) {
	response, err := c.client.LaunchSession(ctx, id)
	if err != nil {
		return Session{}, mapTetherError(ctx, err)
	}
	return Session{ID: response.ID, Replayed: response.Replayed}, nil
}

// GetSession reads the session row.
func (c *TetherClient) GetSession(ctx context.Context, id string) (SessionState, error) {
	session, err := c.client.GetSession(ctx, id)
	if err != nil {
		return SessionState{}, mapTetherError(ctx, err)
	}
	state := SessionState{ID: session.ID, State: session.State}
	if session.ExitCode != nil {
		exitCode := *session.ExitCode
		state.ExitCode = &exitCode
	}
	return state, nil
}

// SessionHealth probes the live runtime.
func (c *TetherClient) SessionHealth(ctx context.Context, id string) (Health, error) {
	health, err := c.client.SessionHealth(ctx, id)
	if err != nil {
		return Health{}, mapTetherError(ctx, err)
	}
	return Health{Alive: health.Alive}, nil
}

// StopSession stops a running session. muxd answers 404 for a session that
// is not running (or unknown); both mean there is nothing left to stop, so
// the call succeeds.
func (c *TetherClient) StopSession(ctx context.Context, id string) error {
	err := mapTetherError(ctx, c.client.StopSession(ctx, id))
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

// ThreadReplies reads a thread (GET /messages/thread/{id}?as=<ResultMailbox>),
// read-only with no delivery side effects.
func (c *TetherClient) ThreadReplies(ctx context.Context, threadID string) ([]Reply, error) {
	envelopes, err := c.store.Thread(ctx, threadID, messaging.Filter{})
	if err != nil {
		return nil, mapTetherError(ctx, err)
	}
	replies := make([]Reply, len(envelopes))
	for index, envelope := range envelopes {
		replies[index] = Reply{
			MessageID: envelope.ID, Kind: string(envelope.Kind),
			From: envelope.From.URN(), To: envelope.To.URN(), Payload: envelope.Payload,
		}
	}
	return replies, nil
}

// mapTetherError classifies go-tether-client errors with the package
// sentinels, keeping the original error in the chain:
//   - ErrDaemonUnreachable (connection refused, missing socket) and an HTTP
//     client timeout while the caller's context is still live → ErrUnavailable
//   - *APIError code idempotency_conflict → ErrIdempotencyConflict
//   - *APIError 404 or code not_found, or messaging.ErrNotFound → ErrNotFound
//
// Everything else passes through unchanged.
func mapTetherError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, tether.ErrDaemonUnreachable) {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	var netErr net.Error
	if ctx.Err() == nil && errors.As(err, &netErr) && netErr.Timeout() {
		// The client's own request timeout: muxd is up but not answering.
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	var apiErr *tether.APIError
	if errors.As(err, &apiErr) {
		switch {
		case apiErr.Code == tether.CodeIdempotencyConflict:
			return fmt.Errorf("%w: %w", ErrIdempotencyConflict, err)
		case apiErr.StatusCode == http.StatusNotFound || apiErr.Code == "not_found":
			return fmt.Errorf("%w: %w", ErrNotFound, err)
		}
	}
	if errors.Is(err, messaging.ErrNotFound) {
		return fmt.Errorf("%w: %w", ErrNotFound, err)
	}
	return err
}
