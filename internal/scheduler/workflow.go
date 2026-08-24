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
	if host == nil || nilActivationStore(store) {
		return nil, errors.New("workflow activation scheduler requires host and durable store")
	}
	service := appworkflow.ActivationService{Host: host, Store: store}
	options = append(options, gosched.WithObserver(appworkflow.ActivationObserver{Store: store}))
	return gosched.New(store, service, options...), nil
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
