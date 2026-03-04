// Package telemetry provides structured JSONL logging for blueprint runs.
//
// Each run produces a file at {logsDir}/{runID}.jsonl where every line is
// a JSON-encoded LogEntry.  When Enabled is false all writes are no-ops so
// callers can unconditionally call Write without checking the flag.
package telemetry

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// LogEntry is one line in a run's JSONL telemetry file.
type LogEntry struct {
	TS      time.Time `json:"ts"`
	Level   string    `json:"level"`
	Event   string    `json:"event"`
	RunID   string    `json:"run_id,omitempty"`
	Section string    `json:"section,omitempty"`
	Step    string    `json:"step,omitempty"`
	Msg     string    `json:"msg,omitempty"`
}

// Logger writes structured JSONL telemetry.
type Logger struct {
	logsDir string
	enabled bool
}

// New creates a Logger. If enabled is false, Write is a no-op.
func New(logsDir string, enabled bool) *Logger {
	return &Logger{logsDir: logsDir, enabled: enabled}
}

// Write appends entry as a JSON line to {logsDir}/{runID}.jsonl.
// Safe to call with a nil Logger or when enabled=false.
func (l *Logger) Write(entry LogEntry) {
	if l == nil || !l.enabled || l.logsDir == "" || entry.RunID == "" {
		return
	}
	if entry.TS.IsZero() {
		entry.TS = time.Now().UTC()
	}
	if entry.Level == "" {
		entry.Level = "info"
	}
	if err := os.MkdirAll(l.logsDir, 0o755); err != nil {
		return
	}
	path := filepath.Join(l.logsDir, entry.RunID+".jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	_ = enc.Encode(entry)
}

// PurgeOlderThan removes JSONL files with a modification time older than d.
func (l *Logger) PurgeOlderThan(d time.Duration) {
	if l == nil || l.logsDir == "" {
		return
	}
	cutoff := time.Now().Add(-d)
	entries, err := os.ReadDir(l.logsDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			_ = os.Remove(filepath.Join(l.logsDir, e.Name()))
		}
	}
}
