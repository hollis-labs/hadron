package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	feotel "github.com/hollis-labs/go-otel"
	gosched "github.com/hollis-labs/go-scheduler"
	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/persistence"
	"go.opentelemetry.io/otel/attribute"
)

// scheduleJobType tags every job the scheduler dispatches. Hadron schedules
// always run a blueprint; the type string exists so a future multi-job-type
// consumer of go-scheduler stays forward-compatible.
const scheduleJobType = "hadron.blueprint.run"

// schedulePayload is the JSON-encoded job.Payload carried from a schedule
// record through the engine to runnerAdapter.Enqueue. It holds the
// Hadron-specific fields the neutral go-scheduler types deliberately omit.
type schedulePayload struct {
	WorkspaceID   string `json:"workspace_id"`
	BlueprintPath string `json:"blueprint_path"`
}

// storeAdapter implements go-scheduler's Store over Hadron's persistence
// store. ClaimAndUpdateScheduleRun, SetScheduleNextRun, and DisableSchedule
// already match the go-scheduler signatures, so they are promoted directly
// from the embedded *persistence.Store; only ListDueSchedules needs a record
// conversion.
type storeAdapter struct {
	*persistence.Store
}

// ListDueSchedules loads Hadron schedule records and maps each into the
// neutral go-scheduler Schedule. Records with an unparseable next-run are
// returned with a zero NextRun, which the engine skips.
func (a storeAdapter) ListDueSchedules(ctx context.Context, now time.Time, limit int) ([]gosched.Schedule, error) {
	ctx, span := feotel.StartSpan(ctx, "hadron.scheduler.list_due")
	span.SetAttributes(
		attribute.String("hollis.component", "scheduler"),
		attribute.String("hadron.scheduler.now", now.UTC().Format(time.RFC3339Nano)),
		attribute.Int("hadron.scheduler.limit", limit),
	)
	defer span.End()
	recs, err := a.Store.ListDueSchedules(ctx, now, limit)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.SetAttributes(attribute.Int("hadron.scheduler.due_record_count", len(recs)))
	out := make([]gosched.Schedule, 0, len(recs))
	for _, rec := range recs {
		sched, convErr := toSchedule(rec)
		if convErr != nil {
			// A record we cannot encode cannot be dispatched; skip it rather
			// than aborting the whole tick.
			continue
		}
		out = append(out, sched)
	}
	span.SetAttributes(attribute.Int("hadron.scheduler.dispatchable_count", len(out)))
	return out, nil
}

// toSchedule converts a Hadron schedule record into a neutral go-scheduler
// Schedule, packing the Hadron-specific fields into the opaque job payload.
func toSchedule(rec persistence.ScheduleRecord) (gosched.Schedule, error) {
	payload, err := json.Marshal(schedulePayload{
		WorkspaceID:   rec.WorkspaceID,
		BlueprintPath: rec.BlueprintPath,
	})
	if err != nil {
		return gosched.Schedule{}, fmt.Errorf("encode schedule payload: %w", err)
	}
	return gosched.Schedule{
		ID:       rec.ID,
		CronExpr: rec.CronExpr,
		LastRun:  parseNullTime(rec.LastRunAt),
		NextRun:  parseNullTime(rec.NextRunAt),
		Enabled:  rec.Enabled,
		JobType:  scheduleJobType,
		Payload:  payload,
	}, nil
}

// parseNullTime decodes an RFC3339 nullable timestamp, returning the zero
// time for a NULL or unparseable value.
func parseNullTime(ns sql.NullString) time.Time {
	if !ns.Valid {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, ns.String)
	if err != nil {
		return time.Time{}
	}
	return t
}

// runEnqueuer is the one capability runnerAdapter needs from the execution
// manager: submitting a blueprint run. Narrowing *execution.Manager to this
// interface keeps the adapter's decode-and-dispatch logic unit-testable
// without standing up a live Manager.
type runEnqueuer interface {
	Enqueue(ctx context.Context, req execution.Request) error
}

// runnerAdapter implements go-scheduler's Runner over Hadron's execution
// manager. It decodes the opaque job payload back into an execution.Request.
type runnerAdapter struct {
	mgr runEnqueuer
}

// Enqueue dispatches a fired schedule's job through the execution manager.
// A duplicate-run race is reported back to the engine as ErrDuplicateJob so
// the schedule is requeued without counting a worker error.
func (r runnerAdapter) Enqueue(ctx context.Context, job gosched.Job) error {
	ctx, span := feotel.StartSpan(ctx, "hadron.scheduler.enqueue")
	span.SetAttributes(
		attribute.String("hollis.component", "scheduler"),
		attribute.String("hadron.schedule.id", job.ScheduleID),
		attribute.String("hadron.run.id", job.RunID),
		attribute.String("hadron.job.type", job.JobType),
	)
	defer span.End()
	var payload schedulePayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		span.RecordError(err)
		return fmt.Errorf("decode schedule payload: %w", err)
	}
	span.SetAttributes(
		attribute.String("hadron.workspace.id", payload.WorkspaceID),
		attribute.String("hadron.blueprint.path", payload.BlueprintPath),
	)
	err := r.mgr.Enqueue(ctx, execution.Request{
		WorkspaceID:   payload.WorkspaceID,
		RunID:         job.RunID,
		BlueprintPath: payload.BlueprintPath,
		Inputs:        map[string]any{},
	})
	if err != nil && isDuplicateRun(err) {
		span.RecordError(err)
		span.SetAttributes(attribute.Bool("hadron.scheduler.duplicate_run", true))
		return fmt.Errorf("enqueue scheduled run %s: %w", job.RunID, gosched.ErrDuplicateJob)
	}
	if err != nil {
		span.RecordError(err)
	}
	return err
}

// isDuplicateRun reports whether an enqueue error is a benign already-running
// race — a unique-constraint violation or a missing-row signal — rather than
// a genuine fault. This mirrors the check the in-tree engine performed before
// the extraction.
func isDuplicateRun(err error) bool {
	return errors.Is(err, sql.ErrNoRows) ||
		strings.Contains(strings.ToLower(err.Error()), "unique")
}
