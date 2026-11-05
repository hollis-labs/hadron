package appworkflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
)

type streamInspector struct {
	graphInspectorFunc
	events func(context.Context, rundiagnostics.EventsQuery) (rundiagnostics.EventPage, error)
}

func (p streamInspector) Events(ctx context.Context, q rundiagnostics.EventsQuery) (rundiagnostics.EventPage, error) {
	return p.events(ctx, q)
}

func TestEventStreamCapSharedAcrossSSELeaseAndLongPoll(t *testing.T) {
	var rejected atomic.Int64
	inspector := streamInspector{events: func(context.Context, rundiagnostics.EventsQuery) (rundiagnostics.EventPage, error) {
		return rundiagnostics.EventPage{}, nil
	}}
	operator, ctx, run := newEventsOperator(t, inspector, func(appworkflow.RunOperationAuthorization) error { return nil }, func(o *appworkflow.WorkflowOperatorOptions) {
		o.EventStreamLimit = 1
		o.EventStreamRejected = func(context.Context) { rejected.Add(1) }
	})
	request := appworkflow.WorkflowRunReadRequest{RunID: run, Identity: appworkflow.IdentityRequest{SourceAuthority: "test"}, WaitMS: 1}
	streamCtx, release, err := operator.BeginWorkflowEventStream(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	// An SSE connection holds its reservation even between page reads, and its
	// own reads do not consume a second slot.
	for range 2 {
		if _, err := operator.FetchWorkflowEvents(streamCtx, request); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := operator.FetchWorkflowEvents(ctx, request); !errors.Is(err, appworkflow.ErrEventStreamLimit) {
		t.Fatalf("long poll at cap: %v", err)
	}
	if _, _, err := operator.BeginWorkflowEventStream(ctx, request); !errors.Is(err, appworkflow.ErrEventStreamLimit) {
		t.Fatalf("second SSE at cap: %v", err)
	}
	other := authenticatedContext(t.Context(), "user:other")
	if _, err := operator.FetchWorkflowEvents(other, request); err != nil {
		t.Fatalf("independent principal: %v", err)
	}
	if rejected.Load() != 2 {
		t.Fatalf("rejection metric calls = %d, want 2", rejected.Load())
	}
	release()
	release()
	if _, err := operator.FetchWorkflowEvents(ctx, request); err != nil {
		t.Fatalf("slot not released: %v", err)
	}
	if got := appworkflow.SafeWorkflowOperationError(appworkflow.ErrEventStreamLimit, nil).Code; got != appworkflow.WorkflowErrorCodeEventStreamLimit {
		t.Fatalf("safe code=%q", got)
	}
}

func TestEventStreamLongPollReleasesOnCancellation(t *testing.T) {
	entered := make(chan struct{}, 1)
	inspector := streamInspector{events: func(ctx context.Context, q rundiagnostics.EventsQuery) (rundiagnostics.EventPage, error) {
		if q.Wait == 0 {
			return rundiagnostics.EventPage{}, nil
		}
		entered <- struct{}{}
		<-ctx.Done()
		return rundiagnostics.EventPage{}, ctx.Err()
	}}
	operator, ctx, run := newEventsOperator(t, inspector, func(appworkflow.RunOperationAuthorization) error { return nil }, func(o *appworkflow.WorkflowOperatorOptions) { o.EventStreamLimit = 1 })
	request := appworkflow.WorkflowRunReadRequest{RunID: run, Identity: appworkflow.IdentityRequest{SourceAuthority: "test"}, WaitMS: 1000}
	pollCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := operator.FetchWorkflowEvents(pollCtx, request); done <- err }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("poll did not start")
	}
	if _, _, err := operator.BeginWorkflowEventStream(ctx, request); !errors.Is(err, appworkflow.ErrEventStreamLimit) {
		t.Fatalf("SSE while poll active: %v", err)
	}
	// Ordinary non-waiting event reads are not followers and remain available.
	immediate := request
	immediate.WaitMS = 0
	if _, err := operator.FetchWorkflowEvents(ctx, immediate); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("poll error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("poll did not cancel")
	}
	_, release, err := operator.BeginWorkflowEventStream(ctx, request)
	if err != nil {
		t.Fatalf("canceled poll retained its slot: %v", err)
	}
	release()
}

func TestEventStreamDefaultLimit(t *testing.T) {
	operator, ctx, run := newEventsOperator(t, &pagedInspector{}, func(appworkflow.RunOperationAuthorization) error { return nil })
	request := appworkflow.WorkflowRunReadRequest{RunID: run, Identity: appworkflow.IdentityRequest{SourceAuthority: "test"}}
	for range 16 {
		_, release, err := operator.BeginWorkflowEventStream(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
	}
	if _, _, err := operator.BeginWorkflowEventStream(ctx, request); !errors.Is(err, appworkflow.ErrEventStreamLimit) {
		t.Fatalf("17th follower: %v", err)
	}
}

func TestEventStreamCapConcurrentAcquisitions(t *testing.T) {
	operator, ctx, run := newEventsOperator(t, &pagedInspector{}, func(appworkflow.RunOperationAuthorization) error { return nil }, func(o *appworkflow.WorkflowOperatorOptions) { o.EventStreamLimit = 4 })
	request := appworkflow.WorkflowRunReadRequest{RunID: run, Identity: appworkflow.IdentityRequest{SourceAuthority: "test"}}
	type result struct {
		release func()
		err     error
	}
	results := make(chan result, 32)
	start := make(chan struct{})
	for range 32 {
		go func() {
			<-start
			_, release, err := operator.BeginWorkflowEventStream(ctx, request)
			results <- result{release, err}
		}()
	}
	close(start)
	accepted := 0
	for range 32 {
		select {
		case r := <-results:
			if r.err == nil {
				accepted++
				defer r.release()
			} else if !errors.Is(r.err, appworkflow.ErrEventStreamLimit) {
				t.Errorf("acquire: %v", r.err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("acquisition did not return")
		}
	}
	if accepted != 4 {
		t.Fatalf("accepted %d concurrent streams, want 4", accepted)
	}
}
