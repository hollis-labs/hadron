package appworkflow

import (
	"context"
	"errors"
	"sync"
)

const DefaultEventStreamLimit = 16
const WorkflowErrorCodeEventStreamLimit = "event_stream_limit_exceeded"

var ErrEventStreamLimit = errors.New("concurrent workflow event stream limit exceeded")

// WorkflowEventStreamOperations reserves a slot for an entire streaming
// response. The returned context is used for its reads; release must be deferred.
type WorkflowEventStreamOperations interface {
	BeginWorkflowEventStream(context.Context, WorkflowRunReadRequest) (context.Context, func(), error)
}

type eventStreamLimiter struct {
	mu       sync.Mutex
	limit    int
	active   map[string]int
	rejected func(context.Context)
}

type eventStreamLease struct {
	limiter   *eventStreamLimiter
	principal string
	active    bool // guarded by limiter.mu
}
type eventStreamLeaseKey struct{}

func (l *eventStreamLimiter) acquire(ctx context.Context, principal string) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return ctx, nil, err
	}
	l.mu.Lock()
	if lease, ok := ctx.Value(eventStreamLeaseKey{}).(*eventStreamLease); ok && lease.limiter == l && lease.principal == principal && lease.active {
		l.mu.Unlock()
		return ctx, func() {}, nil
	}
	if l.active[principal] >= l.limit {
		l.mu.Unlock()
		if l.rejected != nil {
			l.rejected(ctx)
		}
		return ctx, nil, ErrEventStreamLimit
	}
	l.active[principal]++
	lease := &eventStreamLease{limiter: l, principal: principal, active: true}
	l.mu.Unlock()
	release := func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if !lease.active {
			return
		}
		lease.active = false
		l.active[principal]--
		if l.active[principal] == 0 {
			delete(l.active, principal)
		}
	}
	return context.WithValue(ctx, eventStreamLeaseKey{}, lease), release, nil
}

// BeginWorkflowEventStream authorizes before reserving a principal's shared
// SSE/long-poll slot. Run IDs, transport names and client identity hints do not
// partition the cap. FetchWorkflowEvents still authorizes every page.
func (s *WorkflowOperator) BeginWorkflowEventStream(ctx context.Context, request WorkflowRunReadRequest) (context.Context, func(), error) {
	if err := request.Display.Validate(); err != nil {
		return ctx, nil, err
	}
	if err := s.authorizeRun(ctx, RunOperationAuthorization{Operation: RunOperationEvents, RunID: request.RunID, Display: &request.Display}, request.Identity); err != nil {
		return ctx, nil, err
	}
	identity, err := s.host.bindIdentity(ctx, request.Identity)
	if err != nil {
		return ctx, nil, err
	}
	return s.eventStreams.acquire(ctx, identity.Principal)
}
