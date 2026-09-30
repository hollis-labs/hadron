package unattended

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
)

const (
	digestA = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	digestB = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

var added = time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)

func entry(id, plan, digest string) Entry {
	return Entry{ID: id, PlanID: plan, Digest: digest, Reason: "because", AddedBy: "local:op", AddedAt: added}
}

func TestDecodeRefusesUnknownFieldsAndBadEntries(t *testing.T) {
	good := `{"version":1,"entries":[{"id":"e1","plan_id":"nightly","digest":"` + digestA + `","reason":"r","added_by":"local:op","added_at":"2026-09-30T12:00:00Z"}]}`
	if _, err := Decode([]byte(good)); err != nil {
		t.Fatalf("good file: %v", err)
	}
	for name, body := range map[string]string{
		"unknown field":   strings.Replace(good, `"reason"`, `"scopes":{},"reason"`, 1),
		"wrong version":   strings.Replace(good, `"version":1`, `"version":2`, 1),
		"bad digest":      strings.Replace(good, digestA, "sha256:nope", 1),
		"missing reason":  strings.Replace(good, `"reason":"r"`, `"reason":""`, 1),
		"missing added":   strings.Replace(good, `,"added_at":"2026-09-30T12:00:00Z"`, "", 1),
		"trailing data":   good + `{}`,
		"duplicate id":    strings.Replace(good, `}]}`, `},{"id":"e1","plan_id":"nightly","digest":"`+digestA+`","reason":"r","added_by":"local:op","added_at":"2026-09-30T12:00:00Z"}]}`, 1),
		"expiry <= added": strings.Replace(good, `"reason":"r"`, `"expires_at":"2026-09-30T11:00:00Z","reason":"r"`, 1),
	} {
		if _, err := Decode([]byte(body)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestMatchPinsDigestAndHonorsScopeAndExpiry(t *testing.T) {
	now := added.Add(time.Hour)
	past := added.Add(30 * time.Minute)
	scoped := entry("b-scoped", "nightly", digestA)
	scoped.Scope = Scope{ActivationID: "act-1", Principal: "service:cron"}
	expired := entry("a-expired", "nightly", digestA)
	expired.ExpiresAt = &past
	entries := []Entry{expired, scoped, entry("c-other", "other", digestA)}

	cases := []struct {
		name  string
		facts StartFacts
		want  string
	}{
		{"scope match", StartFacts{PlanID: "nightly", Digest: digestA, ActivationID: "act-1", Principal: "service:cron"}, "b-scoped"},
		{"wrong activation", StartFacts{PlanID: "nightly", Digest: digestA, ActivationID: "act-2", Principal: "service:cron"}, ""},
		{"wrong principal", StartFacts{PlanID: "nightly", Digest: digestA, ActivationID: "act-1", Principal: "user:x"}, ""},
		{"edited digest", StartFacts{PlanID: "nightly", Digest: digestB, ActivationID: "act-1", Principal: "service:cron"}, ""},
		{"unscoped entry", StartFacts{PlanID: "other", Digest: digestA}, "c-other"},
	}
	for _, tc := range cases {
		got, ok := Match(entries, tc.facts, now)
		if (tc.want == "") == ok || got.ID != tc.want {
			t.Errorf("%s: got %q (%v), want %q", tc.name, got.ID, ok, tc.want)
		}
	}
	if _, ok := Match([]Entry{expired}, StartFacts{PlanID: "nightly", Digest: digestA}, now); ok {
		t.Error("expired entry matched")
	}
}

func writeFile(t *testing.T, dir string, entries ...Entry) string {
	t.Helper()
	path := filepath.Join(dir, FileName)
	if err := WriteAtomic(path, File{Version: FileVersion, Entries: entries}); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestWriteAtomicIsPrivateAndReadable(t *testing.T) {
	path := writeFile(t, t.TempDir(), entry("e1", "nightly", digestA))
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %v, %v; want 0600", info.Mode().Perm(), err)
	}
	file, _, err := ReadChecked(path, os.Getuid())
	if err != nil || len(file.Entries) != 1 {
		t.Fatalf("ReadChecked = %#v, %v", file, err)
	}
}

func TestReadCheckedFailsClosed(t *testing.T) {
	dir := t.TempDir()
	if file, _, err := ReadChecked(filepath.Join(dir, "absent.json"), os.Getuid()); err != nil || len(file.Entries) != 0 {
		t.Fatalf("missing file = %#v, %v; want empty list", file, err)
	}
	path := writeFile(t, dir, entry("e1", "nightly", digestA))
	for _, mode := range []os.FileMode{0o620, 0o602, 0o666} {
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		if _, _, err := ReadChecked(path, os.Getuid()); err == nil {
			t.Errorf("mode %o accepted", mode)
		}
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadChecked(path, os.Getuid()); err != nil {
		t.Errorf("0644 refused: %v", err)
	}
	if _, _, err := ReadChecked(path, os.Getuid()+1); err == nil {
		t.Error("file owned by another uid accepted")
	}
	link := filepath.Join(dir, "link.json")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadChecked(link, os.Getuid()); err == nil {
		t.Error("symlink accepted")
	}
}

func newTestStore(t *testing.T, path string, now *time.Time) (*Store, *bytes.Buffer) {
	t.Helper()
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return NewStore(path, os.Getuid(), logger, func() time.Time { return *now }), &logs
}

// touch moves the file's mtime so the store sees a change even when the
// rewrite lands inside the filesystem's timestamp resolution.
func touch(t *testing.T, path string, at time.Time) {
	t.Helper()
	if err := os.Chtimes(path, at, at); err != nil {
		t.Fatal(err)
	}
}

func TestStoreReloadsAndLogsEveryChange(t *testing.T) {
	dir := t.TempDir()
	now := added.Add(time.Minute)
	path := writeFile(t, dir, entry("e1", "nightly", digestA))
	touch(t, path, added.Add(10*time.Second))
	store, logs := newTestStore(t, path, &now)

	if got := store.Snapshot(); len(got.Entries) != 1 || !got.ModTime.Equal(added.Add(10*time.Second)) {
		t.Fatalf("first snapshot = %#v", got)
	}
	if !strings.Contains(logs.String(), "entry added") || !strings.Contains(logs.String(), "entry=e1") ||
		!strings.Contains(logs.String(), "added_by=local:op") || !strings.Contains(logs.String(), "file_mtime=") ||
		!strings.Contains(logs.String(), "added_at=2026-09-30T12:00:00Z") {
		t.Fatalf("add not logged with provenance:\n%s", logs.String())
	}

	logs.Reset()
	store.Snapshot()
	if logs.Len() != 0 {
		t.Fatalf("unchanged file logged:\n%s", logs.String())
	}

	changed := entry("e1", "nightly", digestA)
	changed.Reason = "new reason"
	writeFile(t, dir, changed, entry("e2", "other", digestB))
	touch(t, path, added.Add(20*time.Second))
	store.Snapshot()
	if !strings.Contains(logs.String(), "entry changed") || !strings.Contains(logs.String(), "entry=e2") {
		t.Fatalf("change/add not logged:\n%s", logs.String())
	}

	logs.Reset()
	writeFile(t, dir, entry("e2", "other", digestB))
	touch(t, path, added.Add(30*time.Second))
	store.Snapshot()
	if !strings.Contains(logs.String(), "entry removed") || !strings.Contains(logs.String(), "entry=e1") {
		t.Fatalf("removal not logged:\n%s", logs.String())
	}
}

func TestStoreLogsExpiryOnce(t *testing.T) {
	dir := t.TempDir()
	now := added.Add(time.Minute)
	ends := added.Add(time.Hour)
	e := entry("e1", "nightly", digestA)
	e.ExpiresAt = &ends
	path := writeFile(t, dir, e)
	store, logs := newTestStore(t, path, &now)
	store.Snapshot()
	logs.Reset()
	now = ends.Add(time.Second)
	store.Snapshot()
	store.Snapshot()
	if strings.Count(logs.String(), "entry expired") != 1 {
		t.Fatalf("expiry logged %d times:\n%s", strings.Count(logs.String(), "entry expired"), logs.String())
	}
}

func TestStoreFailsClosedOnLoosenedFile(t *testing.T) {
	dir := t.TempDir()
	now := added.Add(time.Minute)
	path := writeFile(t, dir, entry("e1", "nightly", digestA))
	store, logs := newTestStore(t, path, &now)
	if len(store.Snapshot().Entries) != 1 {
		t.Fatal("expected one entry")
	}
	if err := os.Chmod(path, 0o666); err != nil {
		t.Fatal(err)
	}
	touch(t, path, added.Add(time.Minute))
	if got := store.Snapshot(); len(got.Entries) != 0 {
		t.Fatalf("world-writable file honored: %#v", got)
	}
	if !strings.Contains(logs.String(), "allow-list refused") || !strings.Contains(logs.String(), "entry removed") {
		t.Fatalf("refusal not logged:\n%s", logs.String())
	}
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	touch(t, path, added.Add(2*time.Minute))
	if got := store.Snapshot(); len(got.Entries) != 0 {
		t.Fatalf("malformed file honored: %#v", got)
	}
}

func testFacts(plan, digest string) hoststate.PolicyFacts {
	facts := hoststate.PolicyFacts{ActivationID: "act-1"}
	facts.Plan.ID, facts.Plan.Digest = plan, digest
	facts.Identity.Principal = "service:cron"
	return facts
}

func TestApplyOnlyWaivesConfirm(t *testing.T) {
	dir := t.TempDir()
	now := added.Add(time.Minute)
	path := writeFile(t, dir, entry("e1", "nightly", digestA))
	touch(t, path, added.Add(5*time.Second))
	store, _ := newTestStore(t, path, &now)
	facts := testFacts("nightly", digestA)

	confirm := hoststate.PolicyDecision{ID: "d1", Outcome: hoststate.PolicyConfirm, Reason: "advised", Attributes: map[string]string{"k": "v"}}
	got := Apply(store, facts, confirm, now)
	if got.Outcome != hoststate.PolicyAllow || got.ID != "d1" || got.Attributes["k"] != "v" ||
		got.Attributes[AttrEntry] != "e1" || got.Attributes[AttrAddedAt] != added.Format(time.RFC3339Nano) ||
		got.Attributes[AttrFileMTime] != added.Add(5*time.Second).Format(time.RFC3339Nano) ||
		got.Attributes[AttrAddedBy] != "local:op" || got.Attributes[AttrReason] != "because" {
		t.Fatalf("Apply(confirm) = %#v", got)
	}
	if err := (hoststate.PolicyDecision{ID: "d1", RunID: "r", Operation: "start", Outcome: got.Outcome, Reason: got.Reason, Attributes: got.Attributes, DecidedAt: now}).Validate(); err != nil {
		t.Fatalf("waived decision invalid: %v", err)
	}
	if confirm.Attributes[AttrEntry] != "" {
		t.Fatal("Apply mutated the input decision's attributes")
	}
	for _, outcome := range []hoststate.PolicyOutcome{hoststate.PolicyAllow, hoststate.PolicyDeny} {
		in := hoststate.PolicyDecision{Outcome: outcome, Reason: "x"}
		if out := Apply(store, facts, in, now); out.Outcome != outcome || out.Attributes[AttrEntry] != "" {
			t.Errorf("Apply(%s) = %#v, want unchanged", outcome, out)
		}
	}
	if out := Apply(store, testFacts("nightly", digestB), confirm, now); out.Outcome != hoststate.PolicyConfirm {
		t.Errorf("unlisted digest = %#v, want confirm", out)
	}
	if out := Apply(nil, facts, confirm, now); out.Outcome != hoststate.PolicyConfirm {
		t.Errorf("nil store = %#v, want confirm", out)
	}
}
