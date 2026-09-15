package persistence

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
)

func TestOpen_AppliesMigrations(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hadron.db")

	store, openErr := Open(dbPath)
	if openErr != nil {
		t.Fatalf("open store: %v", openErr)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()

	tables := []string{"runs", "schedules", "pipeline_runs", "pipeline_stage_runs", "settings", "schema_migrations", "run_events", "workspaces", "human_gates", "messages"}
	for _, tbl := range tables {
		var name string
		err := store.DB().QueryRowContext(ctx,
			`SELECT name FROM sqlite_master WHERE type='table' AND name = ?`, tbl,
		).Scan(&name)
		if err != nil {
			t.Fatalf("table %s not found: %v", tbl, err)
		}
	}
	if tableExists(t, store.DB(), "queue_entries") {
		t.Fatalf("queue_entries should be removed by the forward migration")
	}
}

func TestOpen_DropsObsoleteQueueEntriesAndPreservesWorkflowState(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "hadron-upgrade.db")
	base := workflowTestTime()
	seedLegacyDatabaseThroughMigration(t, dbPath, 29)

	legacy, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open legacy db: %v", err)
	}
	legacy.SetMaxOpenConns(1)
	legacy.SetMaxIdleConns(1)
	if _, err = legacy.ExecContext(ctx, `
INSERT INTO runs (id, blueprint_path, status, input_json, created_at)
VALUES ('legacy-run', './legacy.yaml', 'queued', '{}', ?);
INSERT INTO queue_entries (run_id, state, available_at)
VALUES ('legacy-run', 'ready', ?);
`, base.Format(time.RFC3339), base.Format(time.RFC3339)); err != nil {
		_ = legacy.Close()
		t.Fatalf("seed obsolete queue entries: %v", err)
	}
	legacyStore := &Store{db: legacy}
	legacyState, err := NewWorkflowStateStore(legacyStore)
	if err != nil {
		_ = legacy.Close()
		t.Fatalf("legacy workflow state store: %v", err)
	}
	run := createWorkflowTestRun(t, legacyState, "upgrade-workflow-run", base)
	node := createWorkflowTestNode(t, legacyState, run.ID, "resource-node", base)
	ready, err := legacyState.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: node.ID, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: base,
	})
	if err != nil {
		_ = legacy.Close()
		t.Fatalf("ready seed node: %v", err)
	}
	requirements := workflowSchedulerRequirements(t, run.ID, workflowruntime.SchedulerLimits{
		Workers: 1,
		Named:   map[string]int{"upgrade-resource": 1},
	}, workflowruntime.SchedulerDemand{Concurrency: []graph.ConcurrencyClaim{{Resource: "upgrade-resource"}}})
	admitted, err := legacyState.AdmitNode(ctx, workflowSchedulerAdmission(ready.Snapshot.ID, "upgrade", requirements, base.Add(time.Second), base.Add(time.Minute)))
	if err != nil || !admitted.Claim.Acquired {
		_ = legacy.Close()
		t.Fatalf("seed scheduler state = %#v, %v", admitted, err)
	}
	if err = legacy.Close(); err != nil {
		t.Fatalf("close legacy db: %v", err)
	}

	upgraded, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open upgraded db: %v", err)
	}
	assertQueueEntriesRemovedAndWorkflowStatePreserved(t, upgraded, run.ID, node.ID, base.Add(2*time.Second))
	if err = upgraded.Close(); err != nil {
		t.Fatalf("close upgraded db: %v", err)
	}

	reopened, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen upgraded db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	assertQueueEntriesRemovedAndWorkflowStatePreserved(t, reopened, run.ID, node.ID, base.Add(3*time.Second))
}

func seedLegacyDatabaseThroughMigration(t *testing.T, path string, maxVersion int) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open legacy seed db: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx := context.Background()
	if _, err = db.ExecContext(ctx, `
		PRAGMA journal_mode=WAL;
		PRAGMA synchronous=NORMAL;
		PRAGMA busy_timeout=5000;
	`); err != nil {
		t.Fatalf("set legacy pragmas: %v", err)
	}
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		t.Fatalf("read migrations: %v", err)
	}
	type migration struct {
		version int
		name    string
	}
	var migrations []migration
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		version, err := parseMigrationVersion(entry.Name())
		if err != nil {
			t.Fatalf("parse migration %s: %v", entry.Name(), err)
		}
		if version <= maxVersion {
			migrations = append(migrations, migration{version: version, name: entry.Name()})
		}
	}
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].version < migrations[j].version
	})
	for _, migration := range migrations {
		content, err := migrationsFS.ReadFile(filepath.Join("migrations", migration.name))
		if err != nil {
			t.Fatalf("read migration %s: %v", migration.name, err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin migration %s: %v", migration.name, err)
		}
		if _, err := tx.ExecContext(ctx, string(content)); err != nil {
			_ = tx.Rollback()
			t.Fatalf("exec migration %s: %v", migration.name, err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)`,
			migration.version,
			time.Now().UTC().Format(time.RFC3339),
		); err != nil {
			_ = tx.Rollback()
			t.Fatalf("record migration %s: %v", migration.name, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit migration %s: %v", migration.name, err)
		}
	}
}

func assertQueueEntriesRemovedAndWorkflowStatePreserved(t *testing.T, store *Store, runID workflowruntime.RunID, nodeID workflowruntime.NodeInvocationID, now time.Time) {
	t.Helper()
	if tableExists(t, store.DB(), "queue_entries") {
		t.Fatalf("queue_entries should be absent after migration 30; obsolete rows are intentionally removed")
	}
	var applied int
	if err := store.DB().QueryRow(`SELECT COUNT(1) FROM schema_migrations WHERE version = 30`).Scan(&applied); err != nil {
		t.Fatalf("read migration 30 marker: %v", err)
	}
	if applied != 1 {
		t.Fatalf("migration 30 marker count = %d, want 1", applied)
	}
	state, err := NewWorkflowStateStore(store)
	if err != nil {
		t.Fatalf("workflow state store after upgrade: %v", err)
	}
	run, err := state.LoadRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("load preserved workflow run: %v", err)
	}
	if run.ID != runID {
		t.Fatalf("preserved run ID = %s, want %s", run.ID, runID)
	}
	node, err := state.LoadNodeInvocation(context.Background(), nodeID)
	if err != nil {
		t.Fatalf("load preserved workflow node: %v", err)
	}
	if node.ID != nodeID || node.Lease == nil {
		t.Fatalf("preserved node = %#v, want leased %s", node, nodeID)
	}
	resources, err := state.InspectSchedulerResources(context.Background(), workflowruntime.SchedulerResourceQuery{Now: now})
	if err != nil {
		t.Fatalf("inspect preserved scheduler resources: %v", err)
	}
	if len(resources.Holders) == 0 {
		t.Fatalf("preserved scheduler holders = %#v, want holders for %s", resources.Holders, nodeID)
	}
	for _, holder := range resources.Holders {
		if holder.Invocation != nodeID {
			t.Fatalf("preserved scheduler holders = %#v, want only %s", resources.Holders, nodeID)
		}
	}
}

func tableExists(t *testing.T, db *sql.DB, table string) bool {
	t.Helper()
	var name string
	err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name = ?`, table).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if err != nil {
		t.Fatalf("query table %s: %v", table, err)
	}
	return true
}

func TestWorkspaceMigrationAndScopedQueries(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hadron.db")
	store, openErr := Open(dbPath)
	if openErr != nil {
		t.Fatalf("open store: %v", openErr)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	defaultWS, workspaceErr := store.GetWorkspace(ctx, "default")
	if workspaceErr != nil {
		t.Fatalf("default workspace should exist: %v", workspaceErr)
	}
	if defaultWS.Name == "" {
		t.Fatalf("default workspace should have a name")
	}

	tables := []string{"runs", "schedules", "pipeline_runs", "pipeline_stage_runs"}
	for _, table := range tables {
		if !hasColumn(t, store, table, "workspace_id") {
			t.Fatalf("expected workspace_id column in %s", table)
		}
	}

	if createErr := store.CreateWorkspace(ctx, "team-a", "Team A"); createErr != nil {
		t.Fatalf("create workspace: %v", createErr)
	}
	workspaces, listErr := store.ListWorkspaces(ctx)
	if listErr != nil {
		t.Fatalf("list workspaces: %v", listErr)
	}
	if len(workspaces) < 2 {
		t.Fatalf("expected at least default + team-a workspaces")
	}

	if createErr := store.CreateRun(ctx, RunRecord{
		ID:            "run-team-a-1",
		WorkspaceID:   "team-a",
		BlueprintPath: "./bp.yaml",
		Status:        "queued",
		CreatedAt:     now,
	}); createErr != nil {
		t.Fatalf("create run: %v", createErr)
	}
	runs, listErr := store.ListRunsByWorkspace(ctx, "team-a", 10)
	if listErr != nil {
		t.Fatalf("list runs by workspace: %v", listErr)
	}
	if len(runs) != 1 || runs[0].WorkspaceID != "team-a" {
		t.Fatalf("unexpected runs by workspace result: %+v", runs)
	}

	if createErr := store.CreateSchedule(ctx, ScheduleRecord{
		ID:            "sch-team-a-1",
		WorkspaceID:   "team-a",
		Name:          "sched-a",
		BlueprintPath: "./bp.yaml",
		CronExpr:      "* * * * *",
		Enabled:       true,
		CreatedAt:     now,
		UpdatedAt:     now,
	}); createErr != nil {
		t.Fatalf("create schedule: %v", createErr)
	}
	schedules, listErr := store.ListSchedulesByWorkspace(ctx, "team-a")
	if listErr != nil {
		t.Fatalf("list schedules by workspace: %v", listErr)
	}
	if len(schedules) != 1 || schedules[0].WorkspaceID != "team-a" {
		t.Fatalf("unexpected schedules by workspace result: %+v", schedules)
	}

	if createErr := store.CreatePipelineRun(ctx, PipelineRunRecord{
		ID:           "pl-team-a-1",
		WorkspaceID:  "team-a",
		PipelinePath: "./pl.yaml",
		Status:       "queued",
		CreatedAt:    now,
	}); createErr != nil {
		t.Fatalf("create pipeline run: %v", createErr)
	}
	pipelines, listErr := store.ListPipelineRunsByWorkspace(ctx, "team-a", 10)
	if listErr != nil {
		t.Fatalf("list pipelines by workspace: %v", listErr)
	}
	if len(pipelines) != 1 || pipelines[0].WorkspaceID != "team-a" {
		t.Fatalf("unexpected pipelines by workspace result: %+v", pipelines)
	}
}

func hasColumn(t *testing.T, store *Store, table, column string) bool {
	t.Helper()
	rows, err := store.DB().Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		t.Fatalf("pragma table_info(%s): %v", table, err)
	}
	defer closeRows(rows)
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			t.Fatalf("scan table info %s: %v", table, err)
		}
		if name == column {
			return true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("table info rows err %s: %v", table, err)
	}
	return false
}

func TestRunAndScheduleCRUD(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hadron.db")
	store, openErr := Open(dbPath)
	if openErr != nil {
		t.Fatalf("open store: %v", openErr)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	run := RunRecord{
		ID:            "run-001",
		BlueprintPath: "./blueprints/setup.yaml",
		Status:        "queued",
		InputJSON:     `{"project":"demo"}`,
		CreatedAt:     now,
	}
	if createErr := store.CreateRun(ctx, run); createErr != nil {
		t.Fatalf("create run: %v", createErr)
	}

	fetchedRun, getErr := store.GetRun(ctx, run.ID)
	if getErr != nil {
		t.Fatalf("get run: %v", getErr)
	}
	if fetchedRun.ID != run.ID || fetchedRun.Status != "queued" {
		t.Fatalf("unexpected run fetched: %+v", fetchedRun)
	}

	errMsg := "step failed"
	if updateErr := store.UpdateRunStatus(ctx, run.ID, "failed", &errMsg); updateErr != nil {
		t.Fatalf("update run status: %v", updateErr)
	}

	runs, listErr := store.ListRuns(ctx, 10)
	if listErr != nil {
		t.Fatalf("list runs: %v", listErr)
	}
	if len(runs) != 1 {
		t.Fatalf("expected 1 run, got %d", len(runs))
	}
	if runs[0].Status != "failed" {
		t.Fatalf("expected failed run status, got %s", runs[0].Status)
	}
	if !runs[0].ErrorMessage.Valid || runs[0].ErrorMessage.String != errMsg {
		t.Fatalf("expected error message %q, got %+v", errMsg, runs[0].ErrorMessage)
	}

	sched := ScheduleRecord{
		ID:            "sch-001",
		Name:          "Nightly setup",
		BlueprintPath: "./blueprints/setup.yaml",
		CronExpr:      "0 2 * * *",
		Enabled:       true,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if createErr := store.CreateSchedule(ctx, sched); createErr != nil {
		t.Fatalf("create schedule: %v", createErr)
	}

	if updateErr := store.UpdateScheduleEnabled(ctx, sched.ID, false); updateErr != nil {
		t.Fatalf("update schedule enabled: %v", updateErr)
	}

	schedules, listErr := store.ListSchedules(ctx)
	if listErr != nil {
		t.Fatalf("list schedules: %v", listErr)
	}
	if len(schedules) != 1 {
		t.Fatalf("expected 1 schedule, got %d", len(schedules))
	}
	if schedules[0].Enabled {
		t.Fatalf("expected disabled schedule, got enabled")
	}
}

func TestOpen_RequiresPath(t *testing.T) {
	_, err := Open("")
	if err == nil {
		t.Fatalf("expected error for empty path")
	}
}

func TestUpdateRunStatus_LeavesNullErrorWhenNil(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hadron.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	now := time.Now().UTC()

	run := RunRecord{ID: "run-002", BlueprintPath: "./bp.yaml", Status: "queued", CreatedAt: now}
	if createErr := store.CreateRun(ctx, run); createErr != nil {
		t.Fatalf("create run: %v", createErr)
	}
	if updateErr := store.UpdateRunStatus(ctx, run.ID, "running", nil); updateErr != nil {
		t.Fatalf("update run status: %v", updateErr)
	}

	var msg sql.NullString
	if scanErr := store.DB().QueryRowContext(ctx, `SELECT error_message FROM runs WHERE id = ?`, run.ID).Scan(&msg); scanErr != nil {
		t.Fatalf("read error_message: %v", scanErr)
	}
	if msg.Valid {
		t.Fatalf("expected null error_message, got %+v", msg)
	}
}

func TestRunEventsCRUD(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hadron.db")
	store, openErr := Open(dbPath)
	if openErr != nil {
		t.Fatalf("open store: %v", openErr)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	run := RunRecord{ID: "run-evt-1", BlueprintPath: "./bp.yaml", Status: "queued", CreatedAt: now}
	if createErr := store.CreateRun(ctx, run); createErr != nil {
		t.Fatalf("create run: %v", createErr)
	}

	if appendErr := store.AppendRunEvent(ctx, RunEventRecord{
		RunID:     run.ID,
		EventType: "queued",
		Message:   sql.NullString{String: "run queued", Valid: true},
		CreatedAt: now,
	}); appendErr != nil {
		t.Fatalf("append run event: %v", appendErr)
	}

	events, listErr := store.ListRunEvents(ctx, run.ID, 10)
	if listErr != nil {
		t.Fatalf("list run events: %v", listErr)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].EventType != "queued" {
		t.Fatalf("expected queued event, got %s", events[0].EventType)
	}
	if !events[0].Message.Valid || events[0].Message.String != "run queued" {
		t.Fatalf("unexpected event message: %+v", events[0].Message)
	}
}

func TestPipelineRunsAndStagesCRUD(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hadron.db")
	store, openErr := Open(dbPath)
	if openErr != nil {
		t.Fatalf("open store: %v", openErr)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	if createErr := store.CreatePipelineRun(ctx, PipelineRunRecord{
		ID:           "pl-001",
		PipelinePath: "./testdata/pipelines/simple-success.yaml",
		Status:       "queued",
		CreatedAt:    now,
	}); createErr != nil {
		t.Fatalf("create pipeline run: %v", createErr)
	}
	if startErr := store.SetPipelineRunStarted(ctx, "pl-001", now); startErr != nil {
		t.Fatalf("set pipeline started: %v", startErr)
	}

	if addErr := store.AddPipelineStageRun(ctx, PipelineStageRunRecord{
		PipelineRunID: "pl-001",
		StageIndex:    0,
		StageName:     "first",
		RunID:         "run-001",
		Status:        "running",
		CreatedAt:     now,
		UpdatedAt:     now,
	}); addErr != nil {
		t.Fatalf("add stage run: %v", addErr)
	}
	if updateErr := store.UpdatePipelineStageRunStatus(ctx, "pl-001", 0, "success"); updateErr != nil {
		t.Fatalf("update stage status: %v", updateErr)
	}
	if finishErr := store.SetPipelineRunFinished(ctx, "pl-001", "success", now, nil); finishErr != nil {
		t.Fatalf("set pipeline finished: %v", finishErr)
	}

	rec, getErr := store.GetPipelineRun(ctx, "pl-001")
	if getErr != nil {
		t.Fatalf("get pipeline run: %v", getErr)
	}
	if rec.Status != "success" {
		t.Fatalf("expected success status, got %s", rec.Status)
	}

	list, listErr := store.ListPipelineRuns(ctx, 10)
	if listErr != nil {
		t.Fatalf("list pipeline runs: %v", listErr)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 pipeline run, got %d", len(list))
	}

	stages, err := store.ListPipelineStageRuns(ctx, "pl-001")
	if err != nil {
		t.Fatalf("list stage runs: %v", err)
	}
	if len(stages) != 1 {
		t.Fatalf("expected 1 stage run, got %d", len(stages))
	}
	if stages[0].Status != "success" {
		t.Fatalf("expected stage success, got %s", stages[0].Status)
	}
}

func TestUpdateScheduleEnabledAndNext_PreservesNextRunWhenNil(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hadron.db")
	store, openErr := Open(dbPath)
	if openErr != nil {
		t.Fatalf("open store: %v", openErr)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	nextRun := now.Add(time.Hour).Format(time.RFC3339)

	sched := ScheduleRecord{
		ID:            "sch-next",
		Name:          "Hourly",
		BlueprintPath: "./bp.yaml",
		CronExpr:      "0 * * * *",
		Enabled:       true,
		CreatedAt:     now,
		UpdatedAt:     now,
		NextRunAt:     sql.NullString{String: nextRun, Valid: true},
	}
	if err := store.CreateSchedule(ctx, sched); err != nil {
		t.Fatalf("create schedule: %v", err)
	}

	// A plain disable then re-enable (nextRun nil) must leave next_run_at
	// intact, or the re-enabled schedule would never be dispatched.
	if err := store.UpdateScheduleEnabledAndNext(ctx, sched.ID, false, nil); err != nil {
		t.Fatalf("disable: %v", err)
	}
	// The nil-nextRun branch must persist enabled=false while keeping
	// next_run_at — verify it now, before the schedule is re-enabled, so a
	// regression that dropped the disable update cannot hide behind the
	// schedule's initial enabled state.
	disabled, err := store.GetSchedule(ctx, sched.ID)
	if err != nil {
		t.Fatalf("get schedule after disable: %v", err)
	}
	if disabled.Enabled {
		t.Fatalf("expected schedule to be disabled after the disable call")
	}
	if !disabled.NextRunAt.Valid || disabled.NextRunAt.String != nextRun {
		t.Fatalf("expected next_run_at preserved through disable as %q, got %+v", nextRun, disabled.NextRunAt)
	}

	if updateErr := store.UpdateScheduleEnabledAndNext(ctx, sched.ID, true, nil); updateErr != nil {
		t.Fatalf("re-enable: %v", updateErr)
	}
	got, err := store.GetSchedule(ctx, sched.ID)
	if err != nil {
		t.Fatalf("get schedule: %v", err)
	}
	if !got.Enabled {
		t.Fatalf("expected schedule to be re-enabled")
	}
	if !got.NextRunAt.Valid || got.NextRunAt.String != nextRun {
		t.Fatalf("expected next_run_at preserved as %q, got %+v", nextRun, got.NextRunAt)
	}

	// A non-nil nextRun still updates the column.
	newNext := now.Add(2 * time.Hour)
	if updateErr := store.UpdateScheduleEnabledAndNext(ctx, sched.ID, true, &newNext); updateErr != nil {
		t.Fatalf("update with explicit next: %v", updateErr)
	}
	got, err = store.GetSchedule(ctx, sched.ID)
	if err != nil {
		t.Fatalf("get schedule: %v", err)
	}
	if want := newNext.Format(time.RFC3339); !got.NextRunAt.Valid || got.NextRunAt.String != want {
		t.Fatalf("expected next_run_at updated to %q, got %+v", want, got.NextRunAt)
	}
}

func TestHumanGateLifecycle(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hadron.db")
	store, openErr := Open(dbPath)
	if openErr != nil {
		t.Fatalf("open store: %v", openErr)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	if err := store.CreateHumanGate(ctx, HumanGateRecord{
		ID:          "gate-1",
		WorkspaceID: "default",
		RunID:       "run-1",
		StepName:    "approve",
		Prompt:      "Approve?",
		OptionsJSON: `[{"id":"approve","label":"Approve"}]`,
		Status:      "waiting",
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("create human gate: %v", err)
	}
	if err := store.SubmitHumanGateDecision(ctx, "gate-1", "approve", now.Add(time.Second)); err != nil {
		t.Fatalf("submit decision: %v", err)
	}
	gate, err := store.GetHumanGate(ctx, "gate-1")
	if err != nil {
		t.Fatalf("get human gate: %v", err)
	}
	if gate.Status != "decided" || !gate.Decision.Valid || gate.Decision.String != "approve" {
		t.Fatalf("unexpected gate after decision: %+v", gate)
	}
	if err := store.SubmitHumanGateDecision(ctx, "gate-1", "deny", now.Add(2*time.Second)); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected second submit to fail with sql.ErrNoRows, got %v", err)
	}
}
