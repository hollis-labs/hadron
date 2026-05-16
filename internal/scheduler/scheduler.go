// Package scheduler wires Hadron's persistence and execution layers to the
// shared github.com/hollis-labs/go-scheduler cron engine.
//
// The cron engine itself lives in go-scheduler. This package is a thin
// adapter: New constructs the engine over Hadron-specific Store and Runner
// implementations (see adapter.go), and the file re-exports the handful of
// engine symbols Hadron callers already depend on so existing import sites
// need no change.
package scheduler

import (
	"time"

	gosched "github.com/hollis-labs/go-scheduler"
	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/persistence"
)

// Engine is the cron schedule engine. Aliased to the go-scheduler type so
// callers keep using scheduler.Engine.
type Engine = gosched.Engine

// Status is a point-in-time snapshot of engine activity. Aliased to the
// go-scheduler type so callers keep using scheduler.Status.
type Status = gosched.Status

// New builds a scheduler engine wired to Hadron's persistence store and
// execution manager. The returned engine dispatches due blueprint schedules
// through the manager's run queue.
func New(store *persistence.Store, mgr *execution.Manager) *Engine {
	return gosched.New(storeAdapter{Store: store}, runnerAdapter{mgr: mgr})
}

// ValidateCron reports whether expr is a valid standard cron expression.
func ValidateCron(expr string) error {
	return gosched.ValidateCron(expr)
}

// NextRun returns the next activation time for expr at or after from.
func NextRun(expr string, from time.Time) (time.Time, error) {
	return gosched.NextRun(expr, from)
}
