// Package tethertest provides an in-process fake of the Tether daemon for
// tetherhost and hadrond tests. It models keyed create/launch replay, the
// session state machine, muxd's restart sweep, daemon unavailability and
// thread replies. It is safe for concurrent use.
package tethertest

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"

	"github.com/hollis-labs/hadron/internal/tetherhost"
)

type session struct {
	id       string
	request  tetherhost.CreateRequest
	state    string
	exitCode *int
}

// Fake is an in-memory tetherhost.Client.
type Fake struct {
	mu          sync.Mutex
	next        int
	sessions    map[string]*session
	byKey       map[string]string
	threads     map[string][]tetherhost.Reply
	unavailable bool
	loseCreate  int

	creates  int
	launches int
	stops    int
}

var _ tetherhost.Client = (*Fake)(nil)

// New returns an empty, available fake daemon.
func New() *Fake {
	return &Fake{sessions: map[string]*session{}, byKey: map[string]string{}, threads: map[string][]tetherhost.Reply{}}
}

// SetUnavailable makes every call fail tetherhost.ErrUnavailable while true.
func (f *Fake) SetUnavailable(unavailable bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unavailable = unavailable
}

// LoseCreateResponses makes the next n CreateSession calls apply (or replay)
// the create and then report ErrUnavailable, modeling a response lost in
// transit.
func (f *Fake) LoseCreateResponses(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loseCreate = n
}

// CreateSession creates or replays a keyed session.
func (f *Fake) CreateSession(_ context.Context, request tetherhost.CreateRequest) (tetherhost.Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable {
		return tetherhost.Session{}, tetherhost.ErrUnavailable
	}
	var result tetherhost.Session
	if id, ok := f.byKey[request.IdempotencyKey]; ok && request.IdempotencyKey != "" {
		if f.sessions[id].request != request {
			return tetherhost.Session{}, tetherhost.ErrIdempotencyConflict
		}
		result = tetherhost.Session{ID: id, Replayed: true}
	} else {
		f.next++
		f.creates++
		id := "sess-" + strconv.Itoa(f.next)
		f.sessions[id] = &session{id: id, request: request, state: tetherhost.StateCreated}
		if request.IdempotencyKey != "" {
			f.byKey[request.IdempotencyKey] = id
		}
		result = tetherhost.Session{ID: id}
	}
	if f.loseCreate > 0 {
		f.loseCreate--
		return tetherhost.Session{}, tetherhost.ErrUnavailable
	}
	return result, nil
}

// LaunchSession moves a created or ready session to running; launching an
// already launched session is a replay.
func (f *Fake) LaunchSession(_ context.Context, id string) (tetherhost.Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable {
		return tetherhost.Session{}, tetherhost.ErrUnavailable
	}
	current, ok := f.sessions[id]
	if !ok {
		return tetherhost.Session{}, tetherhost.ErrNotFound
	}
	if current.state != tetherhost.StateCreated && current.state != tetherhost.StateReady {
		return tetherhost.Session{ID: id, Replayed: true}, nil
	}
	f.launches++
	current.state = tetherhost.StateRunning
	return tetherhost.Session{ID: id}, nil
}

// GetSession reads a session's state.
func (f *Fake) GetSession(_ context.Context, id string) (tetherhost.SessionState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable {
		return tetherhost.SessionState{}, tetherhost.ErrUnavailable
	}
	current, ok := f.sessions[id]
	if !ok {
		return tetherhost.SessionState{}, tetherhost.ErrNotFound
	}
	state := tetherhost.SessionState{ID: id, State: current.state}
	if current.exitCode != nil {
		code := *current.exitCode
		state.ExitCode = &code
	}
	return state, nil
}

// SessionHealth reports a running or launching session alive.
func (f *Fake) SessionHealth(_ context.Context, id string) (tetherhost.Health, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable {
		return tetherhost.Health{}, tetherhost.ErrUnavailable
	}
	current, ok := f.sessions[id]
	if !ok {
		return tetherhost.Health{}, tetherhost.ErrNotFound
	}
	return tetherhost.Health{Alive: current.state == tetherhost.StateRunning || current.state == tetherhost.StateLaunching}, nil
}

// StopSession kills a live session; stopping an exited session is a no-op.
func (f *Fake) StopSession(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable {
		return tetherhost.ErrUnavailable
	}
	current, ok := f.sessions[id]
	if !ok {
		return tetherhost.ErrNotFound
	}
	f.stops++
	switch current.state {
	case tetherhost.StateCompleted, tetherhost.StateFailed, tetherhost.StateKilled:
	default:
		current.state = tetherhost.StateKilled
	}
	return nil
}

// ThreadReplies lists a thread oldest first.
func (f *Fake) ThreadReplies(_ context.Context, threadID string) ([]tetherhost.Reply, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable {
		return nil, tetherhost.ErrUnavailable
	}
	replies := f.threads[threadID]
	result := make([]tetherhost.Reply, len(replies))
	for index, reply := range replies {
		reply.Payload = append(json.RawMessage(nil), reply.Payload...)
		result[index] = reply
	}
	return result, nil
}

// PostReply appends a reply to a thread, as mux_message_send would.
func (f *Fake) PostReply(threadID string, reply tetherhost.Reply) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if reply.MessageID == "" {
		reply.MessageID = "msg-" + strconv.Itoa(len(f.threads[threadID])+1)
	}
	reply.Payload = append(json.RawMessage(nil), reply.Payload...)
	f.threads[threadID] = append(f.threads[threadID], reply)
}

// Complete exits a session successfully.
func (f *Fake) Complete(id string) { f.exit(id, tetherhost.StateCompleted, 0) }

// Fail exits a session with a failure exit code.
func (f *Fake) Fail(id string, exitCode int) { f.exit(id, tetherhost.StateFailed, exitCode) }

// Kill marks a session stopped, as a human `mux session stop` would.
func (f *Fake) Kill(id string) { f.exit(id, tetherhost.StateKilled, 137) }

func (f *Fake) exit(id, state string, exitCode int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if current, ok := f.sessions[id]; ok {
		current.state = state
		current.exitCode = &exitCode
	}
}

// RestartSweep models muxd restarting: every launching or running session is
// failed with exit code -1.
func (f *Fake) RestartSweep() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, current := range f.sessions {
		if current.state == tetherhost.StateRunning || current.state == tetherhost.StateLaunching {
			lost := -1
			current.state = tetherhost.StateFailed
			current.exitCode = &lost
		}
	}
}

// Request returns the create request a session was made from.
func (f *Fake) Request(id string) (tetherhost.CreateRequest, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	current, ok := f.sessions[id]
	if !ok {
		return tetherhost.CreateRequest{}, false
	}
	return current.request, true
}

// State returns a session's current state, or "" when it does not exist.
func (f *Fake) State(id string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if current, ok := f.sessions[id]; ok {
		return current.state
	}
	return ""
}

// SessionIDs returns every session id in creation order.
func (f *Fake) SessionIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]string, 0, f.next)
	for index := 1; index <= f.next; index++ {
		ids = append(ids, "sess-"+strconv.Itoa(index))
	}
	return ids
}

// Creates counts sessions actually created (replays excluded).
func (f *Fake) Creates() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.creates
}

// Launches counts sessions actually launched (replays excluded).
func (f *Fake) Launches() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.launches
}

// Stops counts StopSession calls on existing sessions.
func (f *Fake) Stops() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stops
}
