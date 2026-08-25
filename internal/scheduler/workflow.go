package scheduler

import (
	"errors"
	"reflect"

	gosched "github.com/hollis-labs/go-scheduler"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
)

// NewWorkflow constructs the graph-native activation scheduler. Unlike New,
// this path never imports or creates a legacy execution.Request: every fire is
// dispatched by ActivationService through Host.StartRun.
func NewWorkflow(store hoststate.ActivationStore, host *appworkflow.Host, options ...gosched.Option) (*Engine, error) {
	return NewWorkflowWithService(appworkflow.ActivationService{Host: host, Store: store}, options...)
}

// NewWorkflowWithService installs one fully composed activation service as the
// scheduler runner. Production uses this seam so schedule and external ingress
// share the same current-registry fence and Host.
func NewWorkflowWithService(service appworkflow.ActivationService, options ...gosched.Option) (*Engine, error) {
	if service.Host == nil || nilActivationStore(service.Store) {
		return nil, errors.New("workflow activation scheduler requires host and durable store")
	}
	options = append(options, gosched.WithObserver(appworkflow.ActivationObserver{Store: service.Store}))
	return gosched.New(service.Store, service, options...), nil
}

func nilActivationStore(store hoststate.ActivationStore) bool {
	if store == nil {
		return true
	}
	value := reflect.ValueOf(store)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
