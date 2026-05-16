package persistence

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func TestOpen_AppliesMigrations(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hadron.db")

	store, openErr := Open(dbPath)
	if openErr != nil {
		t.Fatalf("open store: %v", openErr)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()

	tables := []string{"runs", "schedules", "queue_entries", "pipeline_runs", "pipeline_stage_runs", "settings", "schema_migrations", "run_events", "workspaces"}
	for _, tbl := range tables {
		var name string
		err := store.DB().QueryRowContext(ctx,
			`SELECT name FROM sqlite_master WHERE type='table' AND name = ?`, tbl,
		).Scan(&name)
		if err != nil {
			t.Fatalf("table %s not found: %v", tbl, err)
		}
	}
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

	if err := store.UpdateScheduleEnabledAndNext(ctx, sched.ID, true, nil); err != nil {
		t.Fatalf("re-enable: %v", err)
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
	if err := store.UpdateScheduleEnabledAndNext(ctx, sched.ID, true, &newNext); err != nil {
		t.Fatalf("update with explicit next: %v", err)
	}
	got, err = store.GetSchedule(ctx, sched.ID)
	if err != nil {
		t.Fatalf("get schedule: %v", err)
	}
	if want := newNext.Format(time.RFC3339); !got.NextRunAt.Valid || got.NextRunAt.String != want {
		t.Fatalf("expected next_run_at updated to %q, got %+v", want, got.NextRunAt)
	}
}
