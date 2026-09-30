// Package unattended is the operator allow-list that lets named workflows
// start without a human confirmation.
//
// Hadron's production start policy asks for confirmation whenever a
// workflow's effects advise it (mutate, destructive, or unresolved call
// nodes). A start with nobody present to confirm (a schedule, a reactor, the
// failure handler) is refused. An entry here, pinned to one plan id and one
// exact graph digest, turns that Confirm into an Allow and names itself in the
// persisted policy decision. Call-started child runs are evaluated against
// the list on their own plan and digest: an entry for a root never covers
// its children, which resolve at call time and are not pinned by the root's
// digest.
//
// The list lives in a file the daemon only reads. There is no HTTP, MCP or
// A2A write path: `hadron workflow unattended` edits the file directly. The
// trust boundary is filesystem ownership. The daemon refuses a file that is
// group- or world-writable or owned by another uid. Same-uid processes can
// still write it, and every agent Hadron, Tether or Torque launches runs as
// the operator's uid, so an agent could allow-list its own workflow (its own
// edited digest included). The digest pin does not stop that; sandboxing
// agent launches away from Hadron's data dir does. See docs/workflows.md.
package unattended

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/hollis-labs/go-workflow/graph"
	"github.com/hollis-labs/go-workflow/values"
)

// FileName is the allow-list file inside Hadron's data dir.
const FileName = "workflow-unattended.json"

// FileVersion is the only schema version this build reads or writes.
const FileVersion = 1

// File is the on-disk allow-list.
type File struct {
	Version int     `json:"version"`
	Entries []Entry `json:"entries"`
}

// Entry lets one exact plan start unattended.
type Entry struct {
	// ID names the entry in decisions, logs and `revoke`.
	ID string `json:"id"`
	// PlanID and Digest pin one exact workflow. Digest is the compiled
	// graph digest (`hadron workflow validate` prints it as "graph"), which
	// is the same whether the workflow starts at top level or as a
	// call-started child. Editing the workflow changes it, so the edited
	// workflow needs a new entry.
	PlanID string `json:"plan_id"`
	Digest string `json:"digest"`
	// Scope optionally narrows which starts the entry covers.
	Scope Scope `json:"scope,omitempty"`
	// ExpiresAt, when set, ends the entry. Expired entries stay in the file
	// (and in `list`) but never match.
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	Reason    string     `json:"reason"`
	AddedBy   string     `json:"added_by"`
	AddedAt   time.Time  `json:"added_at"`
}

// Scope narrows an entry. Empty fields match anything.
type Scope struct {
	// ActivationID limits the entry to starts from one schedule or trigger
	// registration.
	ActivationID string `json:"activation_id,omitempty"`
	// Principal limits the entry to starts bound to one principal.
	Principal string `json:"principal,omitempty"`
}

// Expired reports whether the entry has ended at now.
func (e Entry) Expired(now time.Time) bool {
	return e.ExpiresAt != nil && !now.Before(*e.ExpiresAt)
}

// Validate checks one entry's shape.
func (e Entry) Validate() error {
	if strings.TrimSpace(e.ID) == "" || strings.TrimSpace(e.ID) != e.ID {
		return errors.New("entry id is required and must not carry surrounding space")
	}
	if err := graph.ValidateID(e.PlanID); err != nil {
		return fmt.Errorf("entry %s: plan id: %w", e.ID, err)
	}
	if err := values.ValidateDigest(e.Digest); err != nil {
		return fmt.Errorf("entry %s: digest: %w", e.ID, err)
	}
	if strings.TrimSpace(e.Reason) == "" {
		return fmt.Errorf("entry %s: reason is required", e.ID)
	}
	if strings.TrimSpace(e.AddedBy) == "" {
		return fmt.Errorf("entry %s: added_by is required", e.ID)
	}
	if e.AddedAt.IsZero() {
		return fmt.Errorf("entry %s: added_at is required", e.ID)
	}
	if e.ExpiresAt != nil && !e.ExpiresAt.After(e.AddedAt) {
		return fmt.Errorf("entry %s: expires_at must be after added_at", e.ID)
	}
	return nil
}

// Validate checks the whole file, including unique entry ids.
func (f File) Validate() error {
	if f.Version != FileVersion {
		return fmt.Errorf("unsupported allow-list version %d (want %d)", f.Version, FileVersion)
	}
	seen := map[string]struct{}{}
	for _, entry := range f.Entries {
		if err := entry.Validate(); err != nil {
			return err
		}
		if _, dup := seen[entry.ID]; dup {
			return fmt.Errorf("duplicate entry id %s", entry.ID)
		}
		seen[entry.ID] = struct{}{}
	}
	return nil
}

// Decode parses and validates a file body. Unknown fields are refused so a
// typo cannot silently widen or drop a scope.
func Decode(data []byte) (File, error) {
	var file File
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&file); err != nil {
		return File{}, fmt.Errorf("decode allow-list: %w", err)
	}
	if decoder.More() {
		return File{}, errors.New("decode allow-list: trailing data")
	}
	if err := file.Validate(); err != nil {
		return File{}, err
	}
	return file, nil
}

// Encode renders a file with stable entry order.
func Encode(file File) ([]byte, error) {
	if err := file.Validate(); err != nil {
		return nil, err
	}
	entries := append([]Entry(nil), file.Entries...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })
	file.Entries = entries
	if file.Entries == nil {
		file.Entries = []Entry{}
	}
	out, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(out, '\n'), nil
}

// StartFacts is what an entry is matched against. Digest is the graph
// digest (PolicyFacts.GraphDigest).
type StartFacts struct {
	PlanID       string
	Digest       string
	ActivationID string
	Principal    string
}

// Match returns the first live entry (by id order) covering the start.
func Match(entries []Entry, facts StartFacts, now time.Time) (Entry, bool) {
	ordered := append([]Entry(nil), entries...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	for _, entry := range ordered {
		if entry.Expired(now) || entry.PlanID != facts.PlanID || entry.Digest != facts.Digest {
			continue
		}
		if entry.Scope.ActivationID != "" && entry.Scope.ActivationID != facts.ActivationID {
			continue
		}
		if entry.Scope.Principal != "" && entry.Scope.Principal != facts.Principal {
			continue
		}
		return entry, true
	}
	return Entry{}, false
}
