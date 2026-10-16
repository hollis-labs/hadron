package rundiagnostics

import (
	"context"
	"fmt"
	"strings"
	"time"

	workflowruntime "github.com/hollis-labs/libs/workflow/engine/runtime"
	"github.com/hollis-labs/libs/workflow/engine/values"
)

// EventFeed is implemented by state readers that can wake a waiting reader and
// describe the event log's position and identity. Without it Events returns
// immediately, whatever Wait asks for, and reports no gaps.
type EventFeed interface {
	// EventsChanged returns a channel closed after the next committed write.
	EventsChanged() <-chan struct{}
	// EventHighWater is the last event sequence allocated for the run (zero
	// when none).
	EventHighWater(context.Context, workflowruntime.RunID) (uint64, error)
	// StoreGeneration names the database's event log.
	StoreGeneration(context.Context) (string, error)
}

// Gap reasons.
const (
	// GapCursorAhead: the caller's position is beyond the log's high water, as
	// after a restore from an older copy. The caller must decide to continue
	// from the high water or start over.
	GapCursorAhead = "cursor_ahead"
	// GapGenerationChanged: the cursor was issued by a different event log.
	GapGenerationChanged = "generation_changed"
)

// maxWait bounds one long poll.
const maxWait = 30 * time.Second

// pollInterval is how often a waiting read re-checks the database when no
// in-process write wakes it (a writer in another process cannot).
const pollInterval = 2 * time.Second

// EventsQuery pages a run's events. After is the sequence of the last event
// the caller already holds (zero starts from the beginning); the page holds
// the next Limit events in ascending sequence order. When no event follows and
// Wait is positive the read waits up to Wait (at most 30s) for one.
type EventsQuery struct {
	RunID   workflowruntime.RunID
	Display values.DisplayPolicy
	After   uint64
	Limit   int
	Wait    time.Duration
	// Generation, when set, is the event log identity the caller's position
	// was issued under; a different log yields a gap, not events.
	Generation string
}

// EventGap tells a caller its position cannot be resumed as is. EarliestAvailable
// is the first sequence still readable and HighWater the last that exists.
type EventGap struct {
	Reason            string
	EarliestAvailable uint64
	HighWater         uint64
}

// EventPage is one page of rendered events. Truncated reports that more
// events follow the last one returned. Terminal reports that the run had
// already finished before this read began, so an empty, untruncated page means
// nothing more will ever arrive.
type EventPage struct {
	Events     []workflowruntime.RenderedEvent
	Truncated  bool
	Terminal   bool
	Gap        *EventGap
	Generation string
	HighWater  uint64
}

// Events returns one page of the run's events without projecting the rest of
// the run, so a caller can follow a run by passing the last sequence it
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
	if request.Wait < 0 || request.Wait > maxWait {
		return EventPage{}, fmt.Errorf("%w: wait must be between 0 and %s", ErrInvalidGraphQuery, maxWait)
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
	feed, _ := s.State.(EventFeed)
	var page EventPage
	if feed != nil {
		generation, err := feed.StoreGeneration(ctx)
		if err != nil {
			return EventPage{}, err
		}
		page.Generation = generation
		if request.Generation != "" && request.Generation != generation {
			return s.gapPage(ctx, feed, request, page, GapGenerationChanged)
		}
	}

	deadline := time.Now().Add(request.Wait)
	for {
		// Take the wake channel before reading so a write between the read and
		// the wait cannot be missed.
		var changed <-chan struct{}
		if feed != nil {
			changed = feed.EventsChanged()
		}
		// Read the run's state before its events: a run seen as finished here
		// has all its events already, so an empty read afterwards really is the end.
		run, err := s.State.LoadRun(ctx, request.RunID)
		if err != nil {
			return EventPage{}, err
		}
		if validateErr := run.Validate(); validateErr != nil {
			return EventPage{}, corrupt("run", validateErr)
		}
		page.Terminal = run.Status.Terminal()
		events, truncated, err := s.loadEventsAfter(ctx, request.RunID, request.After, request.Limit)
		if err != nil {
			return EventPage{}, err
		}
		if len(events) > 0 {
			page.Truncated = truncated
			for _, event := range events {
				rendered, renderErr := workflowruntime.RenderEvent(event, request.Display)
				if renderErr != nil {
					return EventPage{}, corrupt("event", renderErr)
				}
				page.Events = append(page.Events, rendered)
			}
			if feed != nil {
				page.HighWater, err = feed.EventHighWater(ctx, request.RunID)
				if err != nil {
					return EventPage{}, err
				}
			}
			return page, nil
		}
		if feed != nil {
			page.HighWater, err = feed.EventHighWater(ctx, request.RunID)
			if err != nil {
				return EventPage{}, err
			}
			if request.After > page.HighWater {
				return s.gapPage(ctx, feed, request, page, GapCursorAhead)
			}
		}
		remaining := time.Until(deadline)
		if feed == nil || page.Terminal || remaining <= 0 {
			return page, nil
		}
		timer := time.NewTimer(min(remaining, pollInterval))
		select {
		case <-changed:
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return EventPage{}, ctx.Err()
		}
		timer.Stop()
	}
}

// gapPage returns the page for a position that cannot be resumed: no events and
// the gap, with the log's current high water.
func (s Service) gapPage(ctx context.Context, feed EventFeed, request EventsQuery, page EventPage, reason string) (EventPage, error) {
	run, err := s.State.LoadRun(ctx, request.RunID)
	if err != nil {
		return EventPage{}, err
	}
	highWater, err := feed.EventHighWater(ctx, request.RunID)
	if err != nil {
		return EventPage{}, err
	}
	page.Terminal = run.Status.Terminal()
	page.HighWater = highWater
	earliest := uint64(1)
	if highWater == 0 {
		earliest = 0
	}
	page.Gap = &EventGap{Reason: reason, EarliestAvailable: earliest, HighWater: highWater}
	return page, nil
}
