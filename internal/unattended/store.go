package unattended

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// maxFileBytes bounds what the daemon will read.
const maxFileBytes = 1 << 20

// Snapshot is the allow-list as last loaded.
type Snapshot struct {
	Entries []Entry
	// ModTime is the file's mtime at load; zero when the file is absent.
	ModTime time.Time
}

// ReadChecked reads the allow-list the way the daemon trusts it. A missing
// file is an empty list. The file must be a regular file (not a symlink),
// owned by uid, and not group- or world-writable; anything else is refused
// so a loosened file is never honored.
func ReadChecked(path string, uid int) (File, time.Time, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return File{Version: FileVersion}, time.Time{}, nil
	}
	if err != nil {
		return File{}, time.Time{}, fmt.Errorf("stat allow-list: %w", err)
	}
	if !info.Mode().IsRegular() {
		return File{}, time.Time{}, fmt.Errorf("allow-list %s is not a regular file", path)
	}
	if info.Mode().Perm()&0o022 != 0 {
		return File{}, time.Time{}, fmt.Errorf("allow-list %s is group- or world-writable (mode %o)", path, info.Mode().Perm())
	}
	if owner, ok := fileOwner(info); !ok || owner != uid {
		return File{}, time.Time{}, fmt.Errorf("allow-list %s is not owned by uid %d", path, uid)
	}
	if info.Size() > maxFileBytes {
		return File{}, time.Time{}, fmt.Errorf("allow-list %s exceeds %d bytes", path, maxFileBytes)
	}
	data, err := os.ReadFile(path) //nolint:gosec // path is the operator's data-dir allow-list, checked above
	if err != nil {
		return File{}, time.Time{}, fmt.Errorf("read allow-list: %w", err)
	}
	file, err := Decode(data)
	if err != nil {
		return File{}, time.Time{}, err
	}
	return file, info.ModTime(), nil
}

// WriteAtomic replaces the allow-list with mode 0600 via rename, so a reader
// never sees a partial file.
func WriteAtomic(path string, file File) error {
	data, err := Encode(file)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if mkdirErr := os.MkdirAll(dir, 0o700); mkdirErr != nil {
		return fmt.Errorf("create allow-list dir: %w", mkdirErr)
	}
	tmp, err := os.CreateTemp(dir, "."+FileName+".*")
	if err != nil {
		return fmt.Errorf("create allow-list temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }() // no-op once renamed into place
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod allow-list temp file: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write allow-list: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync allow-list: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close allow-list: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("install allow-list: %w", err)
	}
	return nil
}

// Store serves the daemon's view of the allow-list, reloading when the
// file's mtime or size changes. A file that fails any check is treated as
// empty (fail closed) until it is fixed, and the reason is logged.
type Store struct {
	path   string
	uid    int
	logger *slog.Logger
	now    func() time.Time

	mu      sync.Mutex
	loaded  bool
	stamp   fileStamp
	current Snapshot
	// live is the set of ids that matched at the last load, to log expiry.
	live map[string]bool
}

type fileStamp struct {
	exists  bool
	modTime time.Time
	size    int64
}

// NewStore watches path for uid. logger and now may be nil.
func NewStore(path string, uid int, logger *slog.Logger, now func() time.Time) *Store {
	if logger == nil {
		logger = slog.Default()
	}
	if now == nil {
		now = time.Now
	}
	return &Store{path: path, uid: uid, logger: logger, now: now}
}

// Path is the file the store reads.
func (s *Store) Path() string { return s.path }

// Snapshot returns the current allow-list, reloading if the file changed.
func (s *Store) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	stamp := s.statStamp()
	if !s.loaded || stamp != s.stamp {
		s.reload(stamp)
	}
	s.logExpiries()
	return Snapshot{Entries: append([]Entry(nil), s.current.Entries...), ModTime: s.current.ModTime}
}

func (s *Store) statStamp() fileStamp {
	info, err := os.Lstat(s.path)
	if err != nil {
		return fileStamp{}
	}
	return fileStamp{exists: true, modTime: info.ModTime(), size: info.Size()}
}

func (s *Store) reload(stamp fileStamp) {
	previous := s.current.Entries
	file, modTime, err := ReadChecked(s.path, s.uid)
	s.loaded, s.stamp = true, stamp
	if err != nil {
		s.logger.Error("unattended allow-list refused; no workflow may start unattended until it is fixed",
			"path", s.path, "error", err.Error())
		s.current = Snapshot{}
		s.logDiff(previous, nil, time.Time{})
		return
	}
	s.current = Snapshot{Entries: file.Entries, ModTime: modTime}
	s.logDiff(previous, file.Entries, modTime)
}

// logDiff records what a reload changed, since edits bypass every API.
func (s *Store) logDiff(before, after []Entry, modTime time.Time) {
	old := indexEntries(before)
	next := indexEntries(after)
	now := s.now()
	for _, id := range sortedIDs(next) {
		entry := next[id]
		prior, existed := old[id]
		switch {
		case !existed:
			s.logger.Warn("unattended allow-list entry added", entryAttrs(entry, modTime, now)...)
		case !sameEntry(prior, entry):
			s.logger.Warn("unattended allow-list entry changed", entryAttrs(entry, modTime, now)...)
		}
	}
	for _, id := range sortedIDs(old) {
		if _, kept := next[id]; !kept {
			s.logger.Warn("unattended allow-list entry removed", entryAttrs(old[id], modTime, now)...)
		}
	}
	s.live = map[string]bool{}
	for id, entry := range next {
		s.live[id] = !entry.Expired(now)
	}
}

// logExpiries reports entries that expired since they were last seen live.
func (s *Store) logExpiries() {
	now := s.now()
	for _, entry := range s.current.Entries {
		if s.live[entry.ID] && entry.Expired(now) {
			s.live[entry.ID] = false
			s.logger.Warn("unattended allow-list entry expired", entryAttrs(entry, s.current.ModTime, now)...)
		}
	}
}

func entryAttrs(entry Entry, modTime, now time.Time) []any {
	attrs := []any{
		"entry", entry.ID, "plan_id", entry.PlanID, "digest", entry.Digest,
		"added_by", entry.AddedBy, "added_at", entry.AddedAt.UTC().Format(time.RFC3339),
		"reason", entry.Reason, "expired", entry.Expired(now),
	}
	if !modTime.IsZero() {
		attrs = append(attrs, "file_mtime", modTime.UTC().Format(time.RFC3339Nano))
	}
	if entry.ExpiresAt != nil {
		attrs = append(attrs, "expires_at", entry.ExpiresAt.UTC().Format(time.RFC3339))
	}
	return attrs
}

func indexEntries(entries []Entry) map[string]Entry {
	out := make(map[string]Entry, len(entries))
	for _, entry := range entries {
		out[entry.ID] = entry
	}
	return out
}

func sortedIDs(entries map[string]Entry) []string {
	ids := make([]string, 0, len(entries))
	for id := range entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func sameEntry(a, b Entry) bool {
	if (a.ExpiresAt == nil) != (b.ExpiresAt == nil) {
		return false
	}
	if a.ExpiresAt != nil && !a.ExpiresAt.Equal(*b.ExpiresAt) {
		return false
	}
	a.ExpiresAt, b.ExpiresAt = nil, nil
	return a.ID == b.ID && a.PlanID == b.PlanID && a.Digest == b.Digest && a.Scope == b.Scope &&
		a.Reason == b.Reason && a.AddedBy == b.AddedBy && a.AddedAt.Equal(b.AddedAt)
}
