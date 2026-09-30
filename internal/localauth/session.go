package localauth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

const (
	// CodeTTL is how long a login code can be redeemed.
	CodeTTL = 60 * time.Second
	// SessionTTL is how long a browser session lasts.
	SessionTTL = 12 * time.Hour
	// SessionCookie names the browser session cookie.
	SessionCookie = "hadron_session"
)

// ErrRateLimited is returned when codes are issued or redeemed too fast.
var ErrRateLimited = errors.New("too many login attempts; wait a minute and try again")

// Sessions holds single-use login codes and the browser sessions they open.
// Only digests of codes and session ids are kept. It lives in memory: a
// daemon restart ends every session, and `hadron ui` opens a new one.
type Sessions struct {
	now func() time.Time

	mu       sync.Mutex
	codes    map[[sha256.Size]byte]time.Time
	sessions map[[sha256.Size]byte]time.Time
	issued   window
	redeemed window
}

// NewSessions returns an empty store. now may be nil.
func NewSessions(now func() time.Time) *Sessions {
	if now == nil {
		now = time.Now
	}
	return &Sessions{
		now:      now,
		codes:    map[[sha256.Size]byte]time.Time{},
		sessions: map[[sha256.Size]byte]time.Time{},
		issued:   window{limit: 10, span: time.Minute},
		redeemed: window{limit: 20, span: time.Minute},
	}
}

func secret() (string, [sha256.Size]byte, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", [sha256.Size]byte{}, err
	}
	value := hex.EncodeToString(raw)
	return value, sha256.Sum256([]byte(value)), nil
}

// IssueCode returns a fresh login code valid for CodeTTL.
func (s *Sessions) IssueCode() (string, time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if !s.issued.allow(now) {
		return "", time.Time{}, ErrRateLimited
	}
	s.sweep(now)
	code, digest, err := secret()
	if err != nil {
		return "", time.Time{}, err
	}
	expires := now.Add(CodeTTL)
	s.codes[digest] = expires
	return code, expires, nil
}

// Redeem spends a login code and opens a session, returning its id. A code
// works once: of any number of concurrent redemptions exactly one succeeds.
func (s *Sessions) Redeem(code string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if !s.redeemed.allow(now) {
		return "", ErrRateLimited
	}
	digest := sha256.Sum256([]byte(code))
	expires, ok := s.codes[digest]
	delete(s.codes, digest)
	if !ok || !now.Before(expires) {
		return "", errors.New("sign-in code is invalid, expired or already used")
	}
	id, idDigest, err := secret()
	if err != nil {
		return "", err
	}
	s.sessions[idDigest] = now.Add(SessionTTL)
	return id, nil
}

// Valid reports whether a session id is live.
func (s *Sessions) Valid(id string) bool {
	if id == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	expires, ok := s.sessions[sha256.Sum256([]byte(id))]
	return ok && s.now().Before(expires)
}

// End closes one session.
func (s *Sessions) End(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, sha256.Sum256([]byte(id)))
}

// EndAll closes every session and discards outstanding codes (token
// rotation).
func (s *Sessions) EndAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.codes = map[[sha256.Size]byte]time.Time{}
	s.sessions = map[[sha256.Size]byte]time.Time{}
}

func (s *Sessions) sweep(now time.Time) {
	for key, expires := range s.codes {
		if !now.Before(expires) {
			delete(s.codes, key)
		}
	}
	for key, expires := range s.sessions {
		if !now.Before(expires) {
			delete(s.sessions, key)
		}
	}
}

// window is a fixed-limit sliding window counter.
type window struct {
	limit int
	span  time.Duration
	hits  []time.Time
}

func (w *window) allow(now time.Time) bool {
	kept := w.hits[:0]
	for _, hit := range w.hits {
		if now.Sub(hit) < w.span {
			kept = append(kept, hit)
		}
	}
	w.hits = kept
	if len(w.hits) >= w.limit {
		return false
	}
	w.hits = append(w.hits, now)
	return true
}
