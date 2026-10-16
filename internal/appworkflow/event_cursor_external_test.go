package appworkflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
	workflowruntime "github.com/hollis-labs/libs/workflow/engine/runtime"
)

// pagedInspector serves a fixed event list the way rundiagnostics.Service does:
// the next Limit events after a sequence, truncated while more follow.
type pagedInspector struct {
	graphInspectorFunc
	total int
	calls int
}

func (p *pagedInspector) Events(_ context.Context, query rundiagnostics.EventsQuery) (rundiagnostics.EventPage, error) {
	p.calls++
	var page rundiagnostics.EventPage
	for sequence := query.After + 1; sequence <= uint64(p.total); sequence++ {
		if len(page.Events) == query.Limit {
			page.Truncated = true
			break
		}
		page.Events = append(page.Events, workflowruntime.RenderedEvent{Sequence: sequence, RunID: query.RunID, Type: "e"})
	}
	return page, nil
}

func newEventsOperator(t *testing.T, inspector appworkflow.GraphRunInspector, authorize func(appworkflow.RunOperationAuthorization) error) (*appworkflow.WorkflowOperator, context.Context, workflowruntime.RunID) {
	t.Helper()
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	ctx := authenticatedContext(t.Context(), "user:owner")
	start := fixture.startRequest("cursor-run", "cursor-start", "ignored")
	if _, err := fixture.host.StartRun(ctx, start); err != nil {
		t.Fatal(err)
	}
	operator, err := appworkflow.NewWorkflowOperator(appworkflow.WorkflowOperatorOptions{
		Host: fixture.host, Diagnostics: inspector,
		Replay: replayRunnerFunc(func(context.Context, workflowruntime.ReplayRequest) (workflowruntime.BeginReplayResult, error) {
			return workflowruntime.BeginReplayResult{}, nil
		}),
		RunAccess: appworkflow.RunOperationAuthorizerFunc(func(_ context.Context, request appworkflow.RunOperationAuthorization) error {
			return authorize(request)
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return operator, ctx, start.RunID
}

func TestFetchWorkflowEventsResumesFromNextCursorWithoutLossOrRepeat(t *testing.T) {
	inspector := &pagedInspector{total: 23}
	operator, ctx, runID := newEventsOperator(t, inspector, func(appworkflow.RunOperationAuthorization) error { return nil })
	request := appworkflow.WorkflowRunReadRequest{RunID: runID, Identity: appworkflow.IdentityRequest{SourceAuthority: "test"}, EventLimit: 10}

	var got []uint64
	for range 5 {
		result, err := operator.FetchWorkflowEvents(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range result.Events {
			got = append(got, event.Sequence)
		}
		request.After = result.NextCursor
		if !result.Truncated {
			break
		}
	}
	if len(got) != 23 {
		t.Fatalf("read %d events, want 23: %v", len(got), got)
	}
	for i, sequence := range got {
		if sequence != uint64(i+1) {
			t.Fatalf("lost or repeated an event at %d: %v", i, got)
		}
	}
	// Caught up: nothing new, and the cursor stays where it was.
	caughtUp, err := operator.FetchWorkflowEvents(ctx, request)
	if err != nil || len(caughtUp.Events) != 0 || caughtUp.NextCursor != request.After {
		t.Fatalf("caught-up read = %+v, %v (cursor was %q)", caughtUp, err, request.After)
	}
}

func TestFetchWorkflowEventsRejectsUnusableCursors(t *testing.T) {
	inspector := &pagedInspector{total: 5}
	var authorized int
	operator, ctx, runID := newEventsOperator(t, inspector, func(appworkflow.RunOperationAuthorization) error { authorized++; return nil })
	request := appworkflow.WorkflowRunReadRequest{RunID: runID, Identity: appworkflow.IdentityRequest{SourceAuthority: "test"}}
	for name, cursor := range map[string]string{
		"not base64":      "%%%",
		"not json":        "bm90LWpzb24",
		"another run":     appworkflow.EncodeEventCursor("some-other-run", 3),
		"unknown version": "eyJ2Ijo5LCJydW4iOiJjdXJzb3ItcnVuIiwic2VxIjoxfQ",
	} {
		request.After = cursor
		if _, err := operator.FetchWorkflowEvents(ctx, request); !errors.Is(err, appworkflow.ErrWorkflowInvalidRequest) {
			t.Errorf("%s: err = %v, want ErrWorkflowInvalidRequest", name, err)
		}
	}
	if inspector.calls != 0 {
		t.Fatalf("a bad cursor reached the event store %d times", inspector.calls)
	}
	// The cursor is read only after the caller is authorized.
	if authorized == 0 {
		t.Fatal("a bad cursor was rejected before authorization")
	}
}

func TestFetchWorkflowEventsAuthorizesBeforeReadingACursor(t *testing.T) {
	inspector := &pagedInspector{total: 5}
	operator, ctx, runID := newEventsOperator(t, inspector, func(appworkflow.RunOperationAuthorization) error { return appworkflow.ErrPolicyDenied })
	request := appworkflow.WorkflowRunReadRequest{RunID: runID, Identity: appworkflow.IdentityRequest{SourceAuthority: "test"}, After: "garbage"}
	if _, err := operator.FetchWorkflowEvents(ctx, request); !errors.Is(err, appworkflow.ErrPolicyDenied) {
		t.Fatalf("err = %v, want ErrPolicyDenied (cursor details must not be revealed to an unauthorized caller)", err)
	}
	if inspector.calls != 0 {
		t.Fatal("an unauthorized read reached the event store")
	}
}

func TestFetchWorkflowEventsCursorNeedsAnInspectorThatPages(t *testing.T) {
	inspector := graphInspectorFunc(func(_ context.Context, query rundiagnostics.Query) (rundiagnostics.Result, error) {
		return rundiagnostics.Result{Events: []workflowruntime.RenderedEvent{{Sequence: 7, RunID: query.RunID, Type: "e"}}}, nil
	})
	operator, ctx, runID := newEventsOperator(t, inspector, func(appworkflow.RunOperationAuthorization) error { return nil })
	request := appworkflow.WorkflowRunReadRequest{RunID: runID, Identity: appworkflow.IdentityRequest{SourceAuthority: "test"}}
	first, err := operator.FetchWorkflowEvents(ctx, request)
	if err != nil || first.NextCursor != appworkflow.EncodeEventCursor(runID, 7) {
		t.Fatalf("first page = %+v, %v", first, err)
	}
	request.After = first.NextCursor
	if _, err := operator.FetchWorkflowEvents(ctx, request); !errors.Is(err, appworkflow.ErrWorkflowInvalidRequest) {
		t.Fatalf("cursor with a non-paging inspector: %v", err)
	}
}
