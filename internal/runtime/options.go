package runtime

import (
	"context"
	"errors"
	"sync"

	libsched "github.com/hollis-labs/libs/core/util/scheduler"
)

type runtimeOptions struct {
	fires  *Fires
	source sourceEngine
}

// Option configures New.
type Option func(*runtimeOptions)

// WithFires binds the activation service as the runner behind the framework
// scheduler module (see RegisterScheduler).
func WithFires(f *Fires) Option { return func(o *runtimeOptions) { o.fires = f } }

// WithSourceEngine binds the framework scheduler module's engine so the runtime
// starts and stops it with the recovered Host.
func WithSourceEngine(e sourceEngine) Option { return func(o *runtimeOptions) { o.source = e } }

// ErrNotReady is returned for a fire that arrives before the runtime is built.
var ErrNotReady = errors.New("workflow runtime is not ready")

// Fires is the scheduler module's runner, bound to the activation service once
// the runtime exists. The module starts before the runtime, so it is created
// first and bound later.
type Fires struct {
	mu sync.Mutex
	r  libsched.Runner
}

// Enqueue dispatches one fired schedule through the activation service.
func (f *Fires) Enqueue(ctx context.Context, job libsched.Job) error {
	f.mu.Lock()
	r := f.r
	f.mu.Unlock()
	if r == nil {
		return ErrNotReady
	}
	return r.Enqueue(ctx, job)
}

func (f *Fires) bind(r libsched.Runner) { f.mu.Lock(); f.r = r; f.mu.Unlock() }
