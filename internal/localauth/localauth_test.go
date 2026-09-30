package localauth

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEnsureTokenCreatesPrivateFileOnce(t *testing.T) {
	path := TokenPath(filepath.Join(t.TempDir(), "data"))
	first, err := EnsureToken(path)
	if err != nil || !ValidToken(first) {
		t.Fatalf("EnsureToken = %q, %v", first, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("token mode = %v, %v", info.Mode().Perm(), err)
	}
	again, err := EnsureToken(path)
	if err != nil || again != first {
		t.Fatalf("second EnsureToken = %q, %v; want the same token", again, err)
	}
}

func TestReadTokenFailsClosed(t *testing.T) {
	dir := t.TempDir()
	path := TokenPath(dir)
	if _, err := EnsureToken(path); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []os.FileMode{0o640, 0o604, 0o660} {
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadToken(path); err == nil {
			t.Errorf("mode %o accepted", mode)
		}
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.token")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadToken(link); err == nil {
		t.Error("symlink accepted")
	}
	bad := filepath.Join(dir, "bad.token")
	if err := os.WriteFile(bad, []byte("not-a-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadToken(bad); err == nil {
		t.Error("malformed token accepted")
	}
}

func TestVerifierFollowsRotationAndEndsSessions(t *testing.T) {
	path := TokenPath(t.TempDir())
	old, err := EnsureToken(path)
	if err != nil {
		t.Fatal(err)
	}
	var changed atomic.Int32
	v := NewVerifier(path, nil, func() { changed.Add(1) })
	if !v.Verify(old) || v.Verify(old[:63]+"0") || v.Verify("") {
		t.Fatal("verifier accepted a wrong token or rejected the right one")
	}
	rotated, err := RotateToken(path)
	if err != nil || rotated == old {
		t.Fatalf("RotateToken = %q, %v", rotated, err)
	}
	if info, statErr := os.Stat(path); statErr != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("rotated token mode = %v, %v", info.Mode().Perm(), statErr)
	}
	if v.Verify(old) {
		t.Fatal("old token still verifies after rotation")
	}
	if !v.Verify(rotated) {
		t.Fatal("new token does not verify")
	}
	if changed.Load() != 1 {
		t.Fatalf("onChange ran %d times, want 1", changed.Load())
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if v.Verify(rotated) {
		t.Fatal("a loosened token file still verifies")
	}
}

func TestLoginCodeIsSingleUseUnderConcurrency(t *testing.T) {
	s := NewSessions(nil)
	code, _, err := s.IssueCode()
	if err != nil {
		t.Fatal(err)
	}
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if id, err := s.Redeem(code); err == nil && s.Valid(id) {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d concurrent redemptions succeeded, want exactly 1", wins.Load())
	}
}

func TestLoginCodeExpiresAndSessionsEnd(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	s := NewSessions(func() time.Time { return now })
	code, expires, err := s.IssueCode()
	if err != nil || !expires.Equal(now.Add(CodeTTL)) {
		t.Fatalf("IssueCode = %v, %v", expires, err)
	}
	now = now.Add(CodeTTL)
	if _, redeemErr := s.Redeem(code); redeemErr == nil {
		t.Fatal("expired code redeemed")
	}
	code, _, _ = s.IssueCode()
	id, err := s.Redeem(code)
	if err != nil || !s.Valid(id) {
		t.Fatalf("fresh code = %q, %v", id, err)
	}
	if s.Valid("forged") || s.Valid("") {
		t.Fatal("unknown session id accepted")
	}
	now = now.Add(SessionTTL)
	if s.Valid(id) {
		t.Fatal("session outlived its TTL")
	}
	now = now.Add(-SessionTTL)
	s.EndAll()
	if s.Valid(id) {
		t.Fatal("EndAll left a session open")
	}
}

func TestLoginRateLimits(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	s := NewSessions(func() time.Time { return now })
	for i := 0; i < 10; i++ {
		if _, _, err := s.IssueCode(); err != nil {
			t.Fatalf("issue %d: %v", i, err)
		}
	}
	if _, _, err := s.IssueCode(); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("11th code in a minute = %v, want ErrRateLimited", err)
	}
	for i := 0; i < 20; i++ {
		_, _ = s.Redeem("guess")
	}
	if _, err := s.Redeem("guess"); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("21st redemption in a minute = %v, want ErrRateLimited", err)
	}
	now = now.Add(time.Minute)
	if _, _, err := s.IssueCode(); err != nil {
		t.Fatalf("after the window: %v", err)
	}
}

func TestScrubEnvRemovesToken(t *testing.T) {
	got := ScrubEnv([]string{"PATH=/bin", EnvToken + "=secret", "HADRON_TOKEN_FILE=/x", "HOME=/h"})
	want := []string{"PATH=/bin", "HADRON_TOKEN_FILE=/x", "HOME=/h"}
	if len(got) != len(want) {
		t.Fatalf("ScrubEnv = %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ScrubEnv = %v", got)
		}
	}
}
