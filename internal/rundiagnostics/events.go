package rundiagnostics

import (
	"context"
	"fmt"
	"strings"

	workflowruntime "github.com/hollis-labs/libs/workflow/engine/runtime"
	"github.com/hollis-labs/libs/workflow/engine/values"
)

// EventsQuery pages a run's events. After is the sequence of the last event
// the caller already holds (zero starts from the beginning); the page holds
// the next Limit events in ascending sequence order.
type EventsQuery struct {
	RunID   workflowruntime.RunID
	Display values.DisplayPolicy
	After   uint64
	Limit   int
}

// EventPage is one page of rendered events. Truncated reports that more
// events follow the last one returned.
type EventPage struct {
	Events    []workflowruntime.RenderedEvent
	Truncated bool
}

// Events returns one page of the run's events without projecting the rest of
// the run, so a caller can follow a long run by passing the last sequence it
// received as After. A zero Limit selects the default.
func (s Service) Events(ctx context.Context, request EventsQuery) (EventPage, error) {
	if strings.TrimSpace(string(request.RunID)) == "" {
		return EventPage{}, fmt.Errorf("%w: run_id is required", ErrInvalidGraphQuery)
	}
	if err := request.Display.Validate(); err != nil {
		return EventPage{}, fmt.Errorf("%w: %w", ErrInvalidGraphQuery, err)
	}
	if request.Limit < 0 || request.Limit > maximumEventLimit {
		return EventPage{}, fmt.Errorf("%w: event limit must be between 0 and %d", ErrInvalidGraphQuery, maximumEventLimit)
	}
	if request.Limit == 0 {
		request.Limit = defaultEventLimit
	}
	if ctx == nil || typedNil(s.State) {
		return EventPage{}, fmt.Errorf("%w: context and state reader are required", ErrInvalidGraphQuery)
	}
	if err := ctx.Err(); err != nil {
		return EventPage{}, err
	}
	run, err := s.State.LoadRun(ctx, request.RunID)
	if err != nil {
		return EventPage{}, err
	}
	if validateErr := run.Validate(); validateErr != nil {
		return EventPage{}, corrupt("run", validateErr)
	}
	events, truncated, err := s.loadEventsAfter(ctx, request.RunID, request.After, request.Limit)
	if err != nil {
		return EventPage{}, err
	}
	page := EventPage{Truncated: truncated}
	for _, event := range events {
		rendered, renderErr := workflowruntime.RenderEvent(event, request.Display)
		if renderErr != nil {
			return EventPage{}, corrupt("event", renderErr)
		}
		page.Events = append(page.Events, rendered)
	}
	return page, nil
}
