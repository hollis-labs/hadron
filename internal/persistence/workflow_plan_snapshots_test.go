package persistence

import (
	"bytes"
	"errors"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestWorkflowPlanSnapshotsKeepExactLocatorVariantsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshots.db")
	store, _ := openWorkflowStateTest(t, path)
	host, hostErr := NewWorkflowHostStore(store)
	if hostErr != nil {
		t.Fatal(hostErr)
	}
	first := exactSnapshotStart(t, workflowHostStartFixture(t, "variant"), "https://example.test/first.workflow.yaml", values.RetentionRun)
	second := cloneStartForRun(first, "variant-second")
	second = exactSnapshotStart(t, second, "https://example.test/second.workflow.yaml", values.RetentionRun)
	if first.Plan.Digest != second.Plan.Digest || first.Snapshot.Digest == second.Snapshot.Digest {
		t.Fatalf("variant identities = plan %q/%q snapshot %q/%q", first.Plan.Digest, second.Plan.Digest, first.Snapshot.Digest, second.Snapshot.Digest)
	}
	for _, record := range []hoststate.StartRecord{first, second} {
		if _, outcome, err := host.RecordStart(t.Context(), record); err != nil || outcome != workflowruntime.IdempotencyApplied {
			t.Fatalf("RecordStart(%s) = %q, %v", record.Run.ID, outcome, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	reopenedHost, _ := NewWorkflowHostStore(reopened)
	for _, want := range []hoststate.StartRecord{first, second} {
		loaded, err := reopenedHost.LoadStart(t.Context(), want.Run.ID)
		if err != nil || loaded.Record.Snapshot == nil || loaded.Record.Snapshot.Digest != want.Snapshot.Digest || loaded.Record.Snapshot.Source.Definition.Locator != want.Snapshot.Source.Definition.Locator {
			t.Fatalf("LoadStart(%s) = %#v, %v", want.Run.ID, loaded.Record.Snapshot, err)
		}
		loaded.Record.Snapshot.Source.Content[0] = 'X'
		again, err := reopenedHost.LoadStart(t.Context(), want.Run.ID)
		if err != nil || again.Record.Snapshot.Source.Content[0] == 'X' {
			t.Fatalf("snapshot source was not defensive: %#v, %v", again.Record.Snapshot, err)
		}
	}
	var snapshots, links int
	if err := reopened.DB().QueryRow(`SELECT COUNT(1) FROM workflow_plan_snapshots`).Scan(&snapshots); err != nil {
		t.Fatal(err)
	}
	if err := reopened.DB().QueryRow(`SELECT COUNT(1) FROM workflow_host_start_plan_snapshots`).Scan(&links); err != nil {
		t.Fatal(err)
	}
	if snapshots != 2 || links != 2 {
		t.Fatalf("snapshot/link counts = %d/%d", snapshots, links)
	}
}

func TestWorkflowPlanSnapshotCorruptionAndCollisionFailClosed(t *testing.T) {
	store, _ := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "corrupt.db"))
	host, _ := NewWorkflowHostStore(store)
	record := exactSnapshotStart(t, workflowHostStartFixture(t, "corrupt"), "corrupt.workflow.yaml", values.RetentionRun)
	if _, _, err := host.RecordStart(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`DROP TRIGGER workflow_plan_snapshots_reject_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`UPDATE workflow_plan_snapshots SET source_snapshot_json = '{}' WHERE snapshot_digest = ?`, record.Snapshot.Digest); err != nil {
		t.Fatal(err)
	}
	if _, err := host.LoadStart(t.Context(), record.Run.ID); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("corrupt snapshot load = %v", err)
	}
	err := host.state.write(t.Context(), "collision test", func(query workflowSQL) error {
		return ensureWorkflowPlanSnapshot(t.Context(), query, *record.Snapshot, workflowTime(record.RecordedAt))
	})
	if !errors.Is(err, workflowruntime.ErrAlreadyExists) {
		t.Fatalf("snapshot collision = %v", err)
	}
}

func TestWorkflowPlanSnapshotCleanupIsBoundedDeterministicAndReferenceSafe(t *testing.T) {
	store, _ := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "cleanup.db"))
	host, _ := NewWorkflowHostStore(store)
	protected := exactSnapshotStart(t, workflowHostStartFixture(t, "protected"), "protected.workflow.yaml", values.RetentionRun)
	if _, _, err := host.RecordStart(t.Context(), protected); err != nil {
		t.Fatal(err)
	}
	first := exactSnapshotStart(t, workflowHostStartFixture(t, "orphan-a"), "orphan-a.workflow.yaml", values.RetentionRun)
	second := exactSnapshotStart(t, workflowHostStartFixture(t, "orphan-b"), "orphan-b.workflow.yaml", values.RetentionRun)
	project := exactSnapshotStart(t, workflowHostStartFixture(t, "project"), "project.workflow.yaml", values.RetentionProject)
	for _, record := range []hoststate.StartRecord{first, second, project} {
		err := host.state.write(t.Context(), "seed unreferenced plan snapshot", func(query workflowSQL) error {
			return ensureWorkflowPlanSnapshot(t.Context(), query, *record.Snapshot, workflowTime(record.RecordedAt))
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	want := []string{first.Snapshot.Digest, second.Snapshot.Digest}
	sort.Strings(want)
	firstDeleted, err := host.CollectUnreferencedPlanSnapshots(t.Context(), 1)
	if err != nil || !reflect.DeepEqual(firstDeleted, want[:1]) {
		t.Fatalf("first cleanup = %#v, %v want %#v", firstDeleted, err, want[:1])
	}
	secondDeleted, err := host.CollectUnreferencedPlanSnapshots(t.Context(), 1)
	if err != nil || !reflect.DeepEqual(secondDeleted, want[1:]) {
		t.Fatalf("second cleanup = %#v, %v want %#v", secondDeleted, err, want[1:])
	}
	for _, digest := range []string{protected.Snapshot.Digest, project.Snapshot.Digest} {
		if _, err := host.LoadPlanSnapshot(t.Context(), digest); err != nil {
			t.Fatalf("protected snapshot %q was collected: %v", digest, err)
		}
	}
	if _, err := host.CollectUnreferencedPlanSnapshots(t.Context(), 0); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("unbounded cleanup = %v", err)
	}
}

func exactSnapshotStart(t *testing.T, record hoststate.StartRecord, locator string, retention values.RetentionClass) hoststate.StartRecord {
	t.Helper()
	content := []byte("workflow source for " + record.Plan.ID)
	sourceDigest := values.SHA256Digest(content)
	provenance := graph.Provenance{Authority: "project", Origin: "workflow-file", Locator: locator, Revision: record.Plan.Graph.Version, Digest: sourceDigest}
	plan := record.Plan
	plan.Definition.Authority = provenance.Authority
	plan.Definition.Locator = locator
	plan.Definition.Digest = sourceDigest
	plan.Definition.Provenance = &provenance
	plan.Provenance = provenance
	plan.SourceDigests = []workflowcompile.SourceDigest{{Format: graph.SourceWorkflow, Digest: sourceDigest}}
	plan.Digest, _ = workflowcompile.PlanDigest(plan)
	record.Plan = plan
	record.Run.Plan = workflowruntime.PlanRef{ID: plan.ID, Version: plan.Graph.Version, Digest: plan.Digest, SchemaVersion: plan.SchemaVersion}
	record.Run.Provenance = provenance
	record.Facts.Plan = record.Run.Plan
	record.Requested = graph.DefinitionRef{Kind: "file", ID: plan.ID, Locator: locator, Version: plan.Graph.Version}
	sealed, err := hoststate.SealPlanSnapshot(hoststate.PlanSnapshot{
		SchemaVersion: hoststate.PlanSnapshotSchemaVersion, Plan: plan, SourceMap: plan.SourceMap,
		Compile: hoststate.UnavailableCompileDescriptor("test provider compile metadata unavailable"),
		Source: &hoststate.SourceSnapshot{
			SchemaVersion: hoststate.SourceSnapshotSchemaVersion, Definition: plan.Definition,
			Format: graph.SourceWorkflow, SourceSchemaID: "workflow.source", SourceSchemaVersion: "1",
			TrustClass: "project", Digest: sourceDigest, Content: content,
			Redaction: values.RedactionPrivate, Retention: retention,
		},
	})
	if err != nil || sealed.Validate() != nil {
		t.Fatalf("SealPlanSnapshot = %#v, %v", sealed, err)
	}
	record.Snapshot = &sealed
	return record
}

func cloneStartForRun(input hoststate.StartRecord, suffix string) hoststate.StartRecord {
	output := input
	output.Run.ID = workflowruntime.RunID("run-" + suffix)
	output.Facts.RunID = output.Run.ID
	output.Decision.ID = "decision-" + suffix
	output.Decision.RunID = output.Run.ID
	output.StartKey = "host-start-" + suffix
	output.RequestDigest = values.SHA256Digest([]byte("request-" + suffix))
	output.CallerInputHash = values.SHA256Digest([]byte("inputs-" + suffix))
	return output
}

func TestExactSnapshotFixtureRetainsRawSourceOnlyInternally(t *testing.T) {
	record := exactSnapshotStart(t, workflowHostStartFixture(t, "raw"), "raw.workflow.yaml", values.RetentionRun)
	encoded, err := encodeWorkflowJSON(record)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains([]byte(encoded), record.Snapshot.Source.Content) {
		t.Fatalf("start JSON contains raw source: %s", encoded)
	}
}
