package api

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/libs/workflow/engine/values"
)

const (
	// workflowStreamWait is how long one read holds the stream open before it
	// sends a heartbeat comment.
	workflowStreamWait = 15 * time.Second
	// workflowStreamPageLimit is how many events one read returns.
	workflowStreamPageLimit = 100
)

// Stream tuning. They are variables only so tests can shorten them.
var (
	// workflowStreamMinWait is the shortest wait one read of a stream may ask
	// for. A shorter (but nonzero) ?wait_ms is raised to it, and zero means the
	// default, so an idle stream can never turn into a read loop that spins.
	workflowStreamMinWait = time.Second
	// workflowStreamMaxLifetime bounds one connection. A stream that has lived
	// this long ends with an end event whose reason is "reconnect"; the client
	// resumes with Last-Event-ID. It stops a never-ending run from pinning a
	// goroutine and polling the database for ever.
	workflowStreamMaxLifetime = 20 * time.Minute
)

// handleWorkflowEventStreamRoute serves GET /v1/workflows/runs/{id}/events/stream,
// a Server-Sent Events view of the same resumable feed the POST events read
// pages through.
//
// Each event is sent as
//
//	id: <cursor after this event>
//	event: workflow_event
//	data: <rendered event JSON>
//
// so a client that reconnects with Last-Event-ID (or ?after=) resumes after
// the last event it saw, without loss or repeats. A comment line is sent every
// time a wait ends with nothing new (a heartbeat). A "gap" event means the
// position cannot be resumed (the data says why and where the log ends) and
// ends the stream; an "end" event means the run finished and every event has
// been sent (reason "run_finished"), or the connection reached its maximum
// lifetime (reason "reconnect": resume with Last-Event-ID). The stream never
// reveals private values: it uses the default display policy.
//
// Writes are not error-checked: the loop ends when the request context is
// canceled, which net/http does when the client disconnects.
func (s *Server) handleWorkflowEventStreamRoute(w http.ResponseWriter, r *http.Request, escapedRunID string) {
	if r.Method != http.MethodGet {
		writeWorkflowOperationError(w, http.StatusMethodNotAllowed, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeInvalidRequest})
		return
	}
	if s.deps.WorkflowReads == nil {
		writeWorkflowOperationError(w, http.StatusServiceUnavailable, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeUnavailable})
		return
	}
	decoded, err := url.PathUnescape(escapedRunID)
	if err != nil || strings.TrimSpace(decoded) == "" {
		writeWorkflowOperationError(w, http.StatusNotFound, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeNotFound})
		return
	}
	runID := appworkflow.RunID(decoded)
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeWorkflowOperationError(w, http.StatusInternalServerError, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeInternal})
		return
	}
	after := r.URL.Query().Get("after")
	if header := strings.TrimSpace(r.Header.Get("Last-Event-ID")); header != "" {
		after = header
	}
	wait := workflowStreamWait
	if raw := r.URL.Query().Get("wait_ms"); raw != "" {
		ms, parseErr := strconv.Atoi(raw)
		if parseErr != nil || ms < 0 || ms > appworkflow.MaxWorkflowEventWaitMS {
			writeWorkflowOperationError(w, http.StatusBadRequest, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeInvalidRequest})
			return
		}
		if ms > 0 {
			wait = max(time.Duration(ms)*time.Millisecond, workflowStreamMinWait)
		}
	}
	var display values.DisplayPolicy
	ctx, ok := s.authenticateWorkflow(w, r, appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessEvents, RunID: runID, Display: &display}, false)
	if !ok {
		return
	}
	request := appworkflow.WorkflowRunReadRequest{
		RunID: runID, Identity: workflowHTTPIdentity(appworkflow.IdentityRequest{}),
		EventLimit: workflowStreamPageLimit, After: after, WaitMS: int(wait / time.Millisecond),
	}

	streams, ok := s.deps.WorkflowReads.(appworkflow.WorkflowEventStreamOperations)
	if !ok {
		s.writeWorkflowFailure(w, appworkflow.ErrHostNotReady, nil, false)
		return
	}
	ctx, release, err := streams.BeginWorkflowEventStream(ctx, request)
	if err != nil {
		s.writeWorkflowFailure(w, err, nil, false)
		return
	}
	defer release()

	// The first read happens before any header is written, so a bad cursor, a
	// missing run or a denial gets a normal HTTP error.
	result, err := s.deps.WorkflowReads.FetchWorkflowEvents(ctx, request)
	if err != nil {
		s.writeWorkflowFailure(w, err, nil, false)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	started := time.Now()
	for {
		if done := writeWorkflowStreamPage(w, result); done {
			flusher.Flush()
			return
		}
		if time.Since(started) >= workflowStreamMaxLifetime {
			writeWorkflowSSE(w, "", "end", map[string]string{"reason": "reconnect"})
			flusher.Flush()
			return
		}
		flusher.Flush()
		request.After = result.NextCursor
		select {
		case <-ctx.Done():
			return
		default:
		}
		result, err = s.deps.WorkflowReads.FetchWorkflowEvents(ctx, request)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Headers are gone: report the safe code in-band and stop.
			code := appworkflow.SafeWorkflowOperationError(err, nil).Code
			writeWorkflowSSE(w, "", "error", map[string]string{"code": code})
			flusher.Flush()
			return
		}
	}
}

// writeWorkflowStreamPage writes one read's frames and reports whether the
// stream is over (a gap, or the run finished and everything was sent).
func writeWorkflowStreamPage(w http.ResponseWriter, result appworkflow.WorkflowEventListResult) bool {
	if result.Gap != nil {
		writeWorkflowSSE(w, result.NextCursor, "gap", result.Gap)
		return true
	}
	for _, event := range result.Events {
		writeWorkflowSSE(w, result.CursorAfter(event.Sequence), "workflow_event", event)
	}
	if len(result.Events) == 0 {
		if result.RunTerminal && !result.Truncated {
			writeWorkflowSSE(w, "", "end", map[string]string{"reason": "run_finished"})
			return true
		}
		_, _ = w.Write([]byte(": heartbeat\n\n"))
	}
	return false
}

func writeWorkflowSSE(w http.ResponseWriter, id, event string, data any) {
	encoded, err := json.Marshal(data)
	if err != nil {
		encoded = []byte(`{}`)
	}
	var frame strings.Builder
	if id != "" {
		frame.WriteString("id: " + id + "\n")
	}
	frame.WriteString("event: " + event + "\ndata: " + string(encoded) + "\n\n")
	_, _ = w.Write([]byte(frame.String()))
}
