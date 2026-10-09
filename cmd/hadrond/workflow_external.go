package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	workflowruntime "github.com/hollis-labs/libs/workflow/runtime"
)

const (
	// workflowExternalReconcileInterval matches the Host recovery interval.
	workflowExternalReconcileInterval = 2 * time.Second
	workflowExternalReconcileBatch    = workflowRecoveryBatch
)

// externalOperationDriver is the part of runtime.ExternalOperationCoordinator
// the reconcile loop needs. Keeping it narrow lets tests drive the loop.
type externalOperationDriver interface {
	Recover(context.Context, workflowruntime.ExternalOperationQuery) ([]workflowruntime.ExternalOperationSnapshot, error)
	Reconcile(context.Context, workflowruntime.AttemptID) (workflowruntime.ExternalOperationResult, error)
}

// workflowExternalReconciler drives go-workflow's ExternalOperationCoordinator.
// The runtime persists suspended external operations but ships no poll loop
// for them (the offline engine inlines its own), so a daemon host must call
// Reconcile for every pending operation until it closes.
//
// Each pass lists up to batch pending operations in store order (oldest
// update first) and reconciles each once. Correctness lives in the store:
// the coordinator fences competing observers with durable refs and CAS
// generations, and transient heartbeat/observe failures leave an operation
// pending for the next pass. Stop cancels the loop context; an in-flight
// adapter call sees that cancellation while the coordinator's durable writes
// run without it.
type workflowExternalReconciler struct {
	driver   externalOperationDriver
	interval time.Duration
	batch    int

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func newWorkflowExternalReconciler(driver externalOperationDriver, interval time.Duration, batch int) (*workflowExternalReconciler, error) {
	if driver == nil {
		return nil, errors.New("workflow external reconciler requires an external operation coordinator")
	}
	if interval <= 0 || batch <= 0 {
		return nil, errors.New("workflow external reconciler requires a positive interval and batch limit")
	}
	return &workflowExternalReconciler{driver: driver, interval: interval, batch: batch}, nil
}

func (r *workflowExternalReconciler) Start() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	r.cancel, r.done = cancel, done
	go func() {
		defer close(done)
		r.loop(ctx)
	}()
}

func (r *workflowExternalReconciler) Stop(ctx context.Context) error {
	if ctx == nil {
		return errors.New("workflow external reconciler stop requires context")
	}
	r.mu.Lock()
	cancel, done := r.cancel, r.done
	r.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		r.mu.Lock()
		if r.done == done {
			r.cancel, r.done = nil, nil
		}
		r.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *workflowExternalReconciler) loop(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		r.pass(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// pass reconciles at most one batch and reports how many operations it
// attempted. Errors are logged, never fatal: a failed pass is retried on the
// next tick, and terminal adapter outcomes are recorded durably by the
// coordinator itself.
func (r *workflowExternalReconciler) pass(ctx context.Context) int {
	if ctx.Err() != nil {
		return 0
	}
	operations, err := r.driver.Recover(ctx, workflowruntime.ExternalOperationQuery{Limit: r.batch})
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("workflow external operation recovery failed", "error", err)
		}
		return 0
	}
	attempted := 0
	for _, operation := range operations {
		if ctx.Err() != nil {
			break
		}
		attempted++
		if _, reconcileErr := r.driver.Reconcile(ctx, operation.Attempt); reconcileErr != nil && ctx.Err() == nil {
			slog.Debug("workflow external operation reconcile did not close the operation",
				"run_id", string(operation.Attempt.Invocation.RunID), "node_id", operation.Attempt.Invocation.NodeID,
				"attempt", operation.Attempt.Number, "error", reconcileErr)
		}
	}
	return attempted
}
