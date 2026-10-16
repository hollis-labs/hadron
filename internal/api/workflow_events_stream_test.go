package api_test

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/api"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	workflowruntime "github.com/hollis-labs/libs/workflow/engine/runtime"
)

func streamEvent(run string, sequence uint64) workflowruntime.RenderedEvent {
	return workflowruntime.RenderedEvent{Sequence: sequence, RunID: workflowruntime.RunID(run), Type: "adapter.observation"}
}

func openStream(t *testing.T, serverURL, path string, headers map[string]string) (*http.Response, *bufio.Scanner) {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, serverURL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer allowed")
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })
	return response, bufio.NewScanner(response.Body)
}

// readFrames returns the stream's frames (blank-line separated) until it closes.
func readFrames(t *testing.T, scanner *bufio.Scanner) []string {
	t.Helper()
	var frames []string
	var current []string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if len(current) > 0 {
				frames = append(frames, strings.Join(current, "\n"))
				current = nil
			}
			continue
		}
		current = append(current, line)
	}
	if len(current) > 0 {
		frames = append(frames, strings.Join(current, "\n"))
	}
	return frames
}

func TestWorkflowEventStreamSendsLabelledEventsHeartbeatsAndEnds(t *testing.T) {
	defer api.SetWorkflowStreamTuning(time.Millisecond, time.Hour)()
	var mu sync.Mutex
	var requests []appworkflow.WorkflowRunReadRequest
	pages := []appworkflow.WorkflowEventListResult{
		{RunID: "stream-run", Events: []workflowruntime.RenderedEvent{streamEvent("stream-run", 1), streamEvent("stream-run", 2)}, NextCursor: appworkflow.EncodeEventCursor("stream-run", 2, "")},
		{RunID: "stream-run", NextCursor: appworkflow.EncodeEventCursor("stream-run", 2, "")},
		{RunID: "stream-run", Events: []workflowruntime.RenderedEvent{streamEvent("stream-run", 3)}, NextCursor: appworkflow.EncodeEventCursor("stream-run", 3, "")},
		{RunID: "stream-run", NextCursor: appworkflow.EncodeEventCursor("stream-run", 3, ""), RunTerminal: true},
	}
	service := &workflowHTTPService{events: func(_ context.Context, request appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowEventListResult, error) {
		mu.Lock()
		defer mu.Unlock()
		requests = append(requests, request)
		return pages[len(requests)-1], nil
	}}
	server := workflowHTTPTestServer(t, service, allowWorkflowHTTP)
	defer server.Close()

	response, scanner := openStream(t, server.URL, "/v1/workflows/runs/stream-run/events/stream?wait_ms=20", nil)
	if response.StatusCode != http.StatusOK || !strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("status=%d content-type=%q", response.StatusCode, response.Header.Get("Content-Type"))
	}
	frames := readFrames(t, scanner)
	if len(frames) != 5 {
		t.Fatalf("frames = %q", frames)
	}
	for i, wantSeq := range []uint64{1, 2} {
		wantID := "id: " + appworkflow.EncodeEventCursor("stream-run", wantSeq, "")
		if !strings.HasPrefix(frames[i], wantID+"\nevent: workflow_event\ndata: ") {
			t.Fatalf("frame %d = %q, want id %s", i, frames[i], wantID)
		}
	}
	if frames[2] != ": heartbeat" || !strings.HasPrefix(frames[3], "id: "+appworkflow.EncodeEventCursor("stream-run", 3, "")+"\nevent: workflow_event") || !strings.HasPrefix(frames[4], "event: end") {
		t.Fatalf("frames = %q", frames)
	}
	// Each read resumes from the cursor the last one returned.
	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 4 || requests[0].After != "" || requests[1].After != pages[0].NextCursor || requests[3].After != pages[2].NextCursor {
		t.Fatalf("requests = %+v", requests)
	}
	if requests[0].WaitMS != 20 || requests[0].EventLimit != 100 || requests[0].Identity.SourceAuthority == "" {
		t.Fatalf("first request = %+v", requests[0])
	}
}

func TestWorkflowEventStreamResumesFromLastEventIDOverTheQueryCursor(t *testing.T) {
	var got appworkflow.WorkflowRunReadRequest
	service := &workflowHTTPService{events: func(_ context.Context, request appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowEventListResult, error) {
		got = request
		return appworkflow.WorkflowEventListResult{RunID: request.RunID, RunTerminal: true}, nil
	}}
	server := workflowHTTPTestServer(t, service, allowWorkflowHTTP)
	defer server.Close()
	cursor := appworkflow.EncodeEventCursor("resume-run", 41, "")
	_, scanner := openStream(t, server.URL, "/v1/workflows/runs/resume-run/events/stream?after=ignored&wait_ms=0", map[string]string{"Last-Event-ID": cursor})
	readFrames(t, scanner)
	if got.After != cursor {
		t.Fatalf("after = %q, want the Last-Event-ID", got.After)
	}
}

func TestWorkflowEventStreamReportsAGapAndStops(t *testing.T) {
	service := &workflowHTTPService{events: func(_ context.Context, request appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowEventListResult, error) {
		return appworkflow.WorkflowEventListResult{RunID: request.RunID, NextCursor: "tail", Gap: &appworkflow.WorkflowEventGap{Reason: "cursor_ahead", EarliestAvailable: 1, HighWater: 9}}, nil
	}}
	server := workflowHTTPTestServer(t, service, allowWorkflowHTTP)
	defer server.Close()
	_, scanner := openStream(t, server.URL, "/v1/workflows/runs/gap-run/events/stream", nil)
	frames := readFrames(t, scanner)
	if len(frames) != 1 || !strings.HasPrefix(frames[0], "id: tail\nevent: gap\ndata: {") || !strings.Contains(frames[0], `"reason":"cursor_ahead"`) || !strings.Contains(frames[0], `"high_water":9`) {
		t.Fatalf("frames = %q", frames)
	}
}

func TestWorkflowEventStreamFirstReadFailuresAreOrdinaryHTTPErrors(t *testing.T) {
	service := &workflowHTTPService{events: func(_ context.Context, request appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowEventListResult, error) {
		if request.After == "bad" {
			return appworkflow.WorkflowEventListResult{}, appworkflow.ErrWorkflowInvalidRequest
		}
		return appworkflow.WorkflowEventListResult{}, nil
	}}
	server := workflowHTTPTestServer(t, service, allowWorkflowHTTP)
	defer server.Close()

	for name, test := range map[string]struct {
		path    string
		headers map[string]string
		want    int
	}{
		"bad cursor":        {"/v1/workflows/runs/r/events/stream?after=bad", nil, http.StatusBadRequest},
		"bad wait":          {"/v1/workflows/runs/r/events/stream?wait_ms=999999", nil, http.StatusBadRequest},
		"hidden run":        {"/v1/workflows/runs/hidden-run/events/stream", nil, http.StatusNotFound},
		"unauthenticated":   {"/v1/workflows/runs/r/events/stream", map[string]string{"Authorization": "Bearer nope"}, http.StatusUnauthorized},
		"empty run segment": {"/v1/workflows/runs/%20/events/stream", nil, http.StatusNotFound},
	} {
		response, _ := openStream(t, server.URL, test.path, test.headers)
		if response.StatusCode != test.want {
			t.Errorf("%s: status = %d, want %d", name, response.StatusCode, test.want)
		}
		if strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
			t.Errorf("%s: an error opened an event stream", name)
		}
	}

	post, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/v1/workflows/runs/r/events/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	post.Header.Set("Authorization", "Bearer allowed")
	response, err := http.DefaultClient.Do(post)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d", response.StatusCode)
	}
}

func TestWorkflowEventStreamStopsReadingWhenTheClientLeaves(t *testing.T) {
	defer api.SetWorkflowStreamTuning(time.Millisecond, time.Hour)()
	var mu sync.Mutex
	calls := 0
	service := &workflowHTTPService{events: func(ctx context.Context, request appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowEventListResult, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		select {
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
		}
		return appworkflow.WorkflowEventListResult{RunID: request.RunID, NextCursor: request.After}, nil
	}}
	server := workflowHTTPTestServer(t, service, allowWorkflowHTTP)
	defer server.Close()

	ctx, cancel := context.WithCancel(t.Context())
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/v1/workflows/runs/leave-run/events/stream?wait_ms=20", nil)
	request.Header.Set("Authorization", "Bearer allowed")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	cancel()
	_ = response.Body.Close()
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	settled := calls
	mu.Unlock()
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if settled == 0 || calls != settled {
		t.Fatalf("reads continued after the client left: %d then %d", settled, calls)
	}
}

// wait_ms=0 means "the default", and a tiny wait is raised to the floor, so an
// idle stream cannot become a loop that hammers the event store.
func TestWorkflowEventStreamCannotBeMadeToSpin(t *testing.T) {
	for _, query := range []string{"wait_ms=0", "wait_ms=1", ""} {
		var mu sync.Mutex
		var waits []int
		var reads int
		service := &workflowHTTPService{events: func(ctx context.Context, request appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowEventListResult, error) {
			mu.Lock()
			reads++
			waits = append(waits, request.WaitMS)
			mu.Unlock()
			// Behave like a real idle read: hold for the wait the stream asked for.
			select {
			case <-time.After(time.Duration(request.WaitMS) * time.Millisecond):
			case <-ctx.Done():
			}
			return appworkflow.WorkflowEventListResult{RunID: request.RunID, NextCursor: request.After}, nil
		}}
		server := workflowHTTPTestServer(t, service, allowWorkflowHTTP)
		ctx, cancel := context.WithTimeout(t.Context(), 2500*time.Millisecond)
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/v1/workflows/runs/idle-run/events/stream?"+query, nil)
		request.Header.Set("Authorization", "Bearer allowed")
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			_, _ = bufio.NewReader(response.Body).WriteTo(discard{})
			_ = response.Body.Close()
		}
		cancel()
		server.Close()
		mu.Lock()
		if reads > 4 {
			t.Errorf("%q: %d reads in 2.5s of an idle stream, want at most 4", query, reads)
		}
		for _, wait := range waits {
			if wait < 1000 {
				t.Errorf("%q: a read asked for a %dms wait, below the 1s floor", query, wait)
			}
		}
		mu.Unlock()
	}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

func TestWorkflowEventStreamEndsAtItsMaximumLifetimeAndCanBeResumed(t *testing.T) {
	defer api.SetWorkflowStreamTuning(time.Millisecond, 120*time.Millisecond)()
	service := &workflowHTTPService{events: func(_ context.Context, request appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowEventListResult, error) {
		time.Sleep(30 * time.Millisecond)
		return appworkflow.WorkflowEventListResult{RunID: request.RunID, NextCursor: request.After}, nil
	}}
	server := workflowHTTPTestServer(t, service, allowWorkflowHTTP)
	defer server.Close()
	_, scanner := openStream(t, server.URL, "/v1/workflows/runs/long-run/events/stream?wait_ms=30", nil)
	frames := readFrames(t, scanner)
	if len(frames) == 0 || frames[len(frames)-1] != "event: end\ndata: {\"reason\":\"reconnect\"}" {
		t.Fatalf("frames = %q", frames)
	}
}

func TestWorkflowEventStreamLimitReturns429BeforeSSEHeaders(t *testing.T) {
	service := &workflowHTTPService{beginStream: func(ctx context.Context, _ appworkflow.WorkflowRunReadRequest) (context.Context, func(), error) {
		return ctx, nil, appworkflow.ErrEventStreamLimit
	}}
	server := workflowHTTPTestServer(t, service, allowWorkflowHTTP)
	defer server.Close()
	response, _ := openStream(t, server.URL, "/v1/workflows/runs/stream-run/events/stream", nil)
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusTooManyRequests || !strings.Contains(string(body), appworkflow.WorkflowErrorCodeEventStreamLimit) || strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("status=%d content-type=%q body=%s", response.StatusCode, response.Header.Get("Content-Type"), body)
	}
}

func TestWorkflowEventStreamReleasesReservationOnReturn(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "terminal"
		if fail {
			name = "read error"
		}
		t.Run(name, func(t *testing.T) {
			released := make(chan struct{})
			service := &workflowHTTPService{
				beginStream: func(ctx context.Context, _ appworkflow.WorkflowRunReadRequest) (context.Context, func(), error) {
					return ctx, func() { close(released) }, nil
				},
				events: func(_ context.Context, request appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowEventListResult, error) {
					if fail {
						return appworkflow.WorkflowEventListResult{}, appworkflow.ErrWorkflowInvalidRequest
					}
					return appworkflow.WorkflowEventListResult{RunID: request.RunID, RunTerminal: true}, nil
				},
			}
			server := workflowHTTPTestServer(t, service, allowWorkflowHTTP)
			defer server.Close()
			response, scanner := openStream(t, server.URL, "/v1/workflows/runs/stream-run/events/stream", nil)
			if fail {
				if response.StatusCode != http.StatusBadRequest {
					t.Fatalf("status=%d", response.StatusCode)
				}
			} else {
				readFrames(t, scanner)
			}
			select {
			case <-released:
			case <-time.After(10 * time.Second):
				t.Fatal("stream reservation not released")
			}
		})
	}
}
