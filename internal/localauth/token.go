// Package localauth is how hadrond authenticates the operator on this
// machine.
//
// Loopback is not a credential: every process running as the operator's
// uid can reach 127.0.0.1, including every agent Hadron, Tether or Torque
// launches. The operator instead holds a random token in a private file,
// <DataDir>/operator.token, which hadrond creates on first start. The CLI
// sends it as a Bearer token. The browser UI never sees it: a caller holding
// the token asks for a short-lived single-use login code, and redeeming that
// code at /auth/login sets an HttpOnly session cookie.
//
// The boundary is who can read the token file. A process running as the
// operator's uid that can read it (an unsandboxed agent included) can act as
// the operator; closing that needs agent sandboxing (Torque CW-20260930-0237).
package localauth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

// TokenFileName is the operator credential inside Hadron's data dir.
const TokenFileName = "operator.token"

// EnvToken names the environment variable the CLI also accepts. The token
// file is preferred: an exported variable leaks into every process started
// from that shell, agents included. Launchers scrub it (ScrubEnv).
const EnvToken = "HADRON_TOKEN"

const tokenBytes = 32

// TokenPath is the operator token file for a data dir.
func TokenPath(dataDir string) string { return filepath.Join(dataDir, TokenFileName) }

func newToken() (string, error) {
	raw := make([]byte, tokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}

// ValidToken reports whether s has the token shape (64 lower-case hex).
func ValidToken(s string) bool {
	if len(s) != 2*tokenBytes {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// ReadToken reads the operator token, refusing a file that is not a private
// regular file owned by the current effective uid.
func ReadToken(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if privateErr := checkPrivate(path, info); privateErr != nil {
		return "", privateErr
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0) // #nosec G304 -- operator-owned data-dir path, checked above and after open.
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return "", fmt.Errorf("operator token %s changed while opening", path)
	}
	if privateErr := checkPrivate(path, opened); privateErr != nil {
		return "", privateErr
	}
	data, err := io.ReadAll(io.LimitReader(file, 4*tokenBytes))
	if err != nil {
		return "", err
	}
	token := strings.TrimSuffix(string(data), "\n")
	if !ValidToken(token) {
		return "", fmt.Errorf("operator token %s is malformed", path)
	}
	return token, nil
}

func checkPrivate(path string, info fs.FileInfo) error {
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("operator token %s is not a regular file", path)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("operator token %s is readable or writable by group or others (mode %o); run chmod 600", path, info.Mode().Perm())
	}
	if owner, ok := fileOwner(info); !ok || owner != os.Geteuid() {
		return fmt.Errorf("operator token %s is not owned by uid %d", path, os.Geteuid())
	}
	return nil
}

// EnsureToken returns the operator token, creating the file (0600, never
// following a symlink) when it does not exist yet.
func EnsureToken(path string) (string, error) {
	token, err := ReadToken(path)
	if err == nil || !errors.Is(err, fs.ErrNotExist) {
		return token, err
	}
	if mkdirErr := os.MkdirAll(filepath.Dir(path), 0o700); mkdirErr != nil {
		return "", fmt.Errorf("create operator token dir: %w", mkdirErr)
	}
	candidate, err := newToken()
	if err != nil {
		return "", err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600) // #nosec G304 -- operator-owned data-dir path.
	if errors.Is(err, fs.ErrExist) {
		return ReadToken(path) // another process created it first
	}
	if err != nil {
		return "", fmt.Errorf("create operator token: %w", err)
	}
	_, writeErr := file.WriteString(candidate + "\n")
	syncErr := file.Sync()
	closeErr := file.Close()
	if joined := errors.Join(writeErr, syncErr, closeErr); joined != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("write operator token: %w", joined)
	}
	return ReadToken(path)
}

// RotateToken replaces the operator token atomically and returns the new
// one. hadrond notices the change, accepts only the new token and ends every
// browser session.
func RotateToken(path string) (string, error) {
	if _, err := ReadToken(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	token, err := newToken()
	if err != nil {
		return "", err
	}
	if mkdirErr := os.MkdirAll(filepath.Dir(path), 0o700); mkdirErr != nil {
		return "", fmt.Errorf("create operator token dir: %w", mkdirErr)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+TokenFileName+".*")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	chmodErr := tmp.Chmod(0o600)
	_, writeErr := tmp.WriteString(token + "\n")
	syncErr := tmp.Sync()
	closeErr := tmp.Close()
	if joined := errors.Join(chmodErr, writeErr, syncErr, closeErr); joined != nil {
		return "", fmt.Errorf("write operator token: %w", joined)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return "", fmt.Errorf("install operator token: %w", err)
	}
	return token, nil
}

// Verifier checks presented tokens against the operator token file,
// re-reading it when it changes. It keeps only the token's digest. A file
// that fails any check verifies nothing (fail closed) until fixed.
type Verifier struct {
	path     string
	logger   *slog.Logger
	onChange func()

	mu     sync.Mutex
	stamp  stamp
	loaded bool
	digest [sha256.Size]byte
	valid  bool
}

// stamp is what makes the verifier re-read the file: content (mtime,
// size, inode) and trust (mode, owner) changes both count, so a chmod that
// loosens the file takes effect at once.
type stamp struct {
	exists  bool
	modTime int64
	size    int64
	ino     uint64
	mode    os.FileMode
	uid     uint32
}

// NewVerifier watches path. onChange, when set, runs after the token changes
// (hadrond ends browser sessions then).
func NewVerifier(path string, logger *slog.Logger, onChange func()) *Verifier {
	if logger == nil {
		logger = slog.Default()
	}
	return &Verifier{path: path, logger: logger, onChange: onChange}
}

// Path is the token file the verifier reads.
func (v *Verifier) Path() string { return v.path }

// Verify reports whether token is the current operator token.
func (v *Verifier) Verify(token string) bool {
	if !ValidToken(token) {
		return false
	}
	v.mu.Lock()
	v.refresh()
	valid, want := v.valid, v.digest
	v.mu.Unlock()
	got := sha256.Sum256([]byte(token))
	return valid && subtle.ConstantTimeCompare(got[:], want[:]) == 1
}

func (v *Verifier) refresh() {
	current := statStamp(v.path)
	if v.loaded && current == v.stamp {
		return
	}
	previous, hadPrevious := v.digest, v.valid
	v.loaded, v.stamp = true, current
	token, err := ReadToken(v.path)
	if err != nil {
		v.valid = false
		v.digest = [sha256.Size]byte{}
		v.logger.Error("operator token refused; no operator request will authenticate until it is fixed", "path", v.path, "error", err.Error())
	} else {
		v.digest, v.valid = sha256.Sum256([]byte(token)), true
	}
	if hadPrevious && (!v.valid || v.digest != previous) {
		v.logger.Warn("operator token changed; browser sessions ended", "path", v.path)
		if v.onChange != nil {
			v.onChange()
		}
	}
}

func statStamp(path string) stamp {
	info, err := os.Lstat(path)
	if err != nil {
		return stamp{}
	}
	s := stamp{exists: true, modTime: info.ModTime().UnixNano(), size: info.Size(), mode: info.Mode()}
	if sys, ok := info.Sys().(*syscall.Stat_t); ok {
		s.ino = uint64(sys.Ino) //nolint:unconvert // Ino's width differs by platform
		s.uid = sys.Uid
	}
	return s
}

// ScrubEnv removes the operator token variable from an environment list, so
// a process Hadron launches never inherits the operator's credential.
func ScrubEnv(env []string) []string {
	out := env[:0:0]
	for _, entry := range env {
		if strings.HasPrefix(entry, EnvToken+"=") {
			continue
		}
		out = append(out, entry)
	}
	return out
}
