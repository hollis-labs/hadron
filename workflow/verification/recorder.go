package verification

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ActivityRecorder is a concurrency-safe attempt-local literal evidence sink.
// Runtimes issue one recorder and freeze it immediately after executor return.
// Trusted adapters call only the typed recording methods at actual activity
// boundaries; workflow/model data must never be translated into these calls.
type ActivityRecorder struct {
	mu         sync.Mutex
	activities []Activity
	frozen     bool
}

// NewActivityRecorder creates an empty recorder. Runtime dispatchers, not
// workflow authors or executor output parsers, own recorder issuance.
func NewActivityRecorder() *ActivityRecorder { return &ActivityRecorder{} }

func (r *ActivityRecorder) RecordToolCall(ctx context.Context, call ToolCall) error {
	return r.record(ctx, Activity{Kind: ActivityToolCall, ToolCall: &call})
}

func (r *ActivityRecorder) RecordTest(ctx context.Context, run TestRun) error {
	return r.record(ctx, Activity{Kind: ActivityTest, Test: &run})
}

func (r *ActivityRecorder) RecordLint(ctx context.Context, run LintRun) error {
	return r.record(ctx, Activity{Kind: ActivityLint, Lint: &run})
}

func (r *ActivityRecorder) record(ctx context.Context, activity Activity) error {
	if ctx == nil {
		return errors.New("activity context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil {
		return errors.New("activity recorder is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return ErrRecorderFrozen
	}
	activity.Sequence = uint64(len(r.activities) + 1)
	if err := activity.Validate(); err != nil {
		return fmt.Errorf("invalid literal activity: %w", err)
	}
	cloned, err := CloneActivities([]Activity{activity})
	if err != nil {
		return err
	}
	r.activities = append(r.activities, cloned[0])
	return nil
}

// Freeze atomically closes recording and returns a defensive snapshot. Exact
// replay calls return the same immutable snapshot.
func (r *ActivityRecorder) Freeze() ([]Activity, error) {
	if r == nil {
		return nil, errors.New("activity recorder is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.frozen = true
	return CloneActivities(r.activities)
}
