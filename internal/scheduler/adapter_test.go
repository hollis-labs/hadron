package scheduler

import (
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/persistence"
)

func TestToSchedule(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	rec := persistence.ScheduleRecord{
		ID:            "sch-1",
		WorkspaceID:   "alpha",
		BlueprintPath: "./bp.yaml",
		CronExpr:      "* * * * *",
		Enabled:       true,
		LastRunAt:     sql.NullString{String: now.Format(time.RFC3339), Valid: true},
		NextRunAt:     sql.NullString{String: now.Add(time.Minute).Format(time.RFC3339), Valid: true},
	}

	sched, err := toSchedule(rec)
	if err != nil {
		t.Fatalf("toSchedule failed: %v", err)
	}
	if sched.ID != "sch-1" || sched.CronExpr != "* * * * *" || !sched.Enabled {
		t.Fatalf("unexpected schedule scalars: %+v", sched)
	}
	if sched.JobType != scheduleJobType {
		t.Fatalf("expected JobType %q, got %q", scheduleJobType, sched.JobType)
	}
	if !sched.LastRun.Equal(now) {
		t.Fatalf("expected LastRun %s, got %s", now, sched.LastRun)
	}
	if !sched.NextRun.Equal(now.Add(time.Minute)) {
		t.Fatalf("expected NextRun %s, got %s", now.Add(time.Minute), sched.NextRun)
	}

	var p schedulePayload
	if err := json.Unmarshal(sched.Payload, &p); err != nil {
		t.Fatalf("payload not valid JSON: %v", err)
	}
	if p.WorkspaceID != "alpha" || p.BlueprintPath != "./bp.yaml" {
		t.Fatalf("payload did not round-trip Hadron fields: %+v", p)
	}
}

func TestParseNullTime(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	if got := parseNullTime(sql.NullString{String: now.Format(time.RFC3339), Valid: true}); !got.Equal(now) {
		t.Fatalf("expected %s, got %s", now, got)
	}
	if got := parseNullTime(sql.NullString{Valid: false}); !got.IsZero() {
		t.Fatalf("expected zero time for NULL, got %s", got)
	}
	if got := parseNullTime(sql.NullString{String: "not-a-time", Valid: true}); !got.IsZero() {
		t.Fatalf("expected zero time for unparseable value, got %s", got)
	}
}

func TestIsDuplicateRun(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"no rows", sql.ErrNoRows, true},
		{"unique violation", errors.New("UNIQUE constraint failed: runs.id"), true},
		{"wrapped no rows", errors.Join(errors.New("enqueue"), sql.ErrNoRows), true},
		{"unrelated", errors.New("disk full"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isDuplicateRun(tc.err); got != tc.want {
				t.Fatalf("isDuplicateRun(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
