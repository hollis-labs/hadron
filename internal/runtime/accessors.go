package runtime

import "github.com/hollis-labs/hadron/internal/appworkflow"

// Host returns the workflow host.
func (r *Runtime) Host() *appworkflow.Host { return r.host }

// Operations returns the operator service (runs, events, replay, diagnostics).
func (r *Runtime) Operations() *appworkflow.WorkflowOperator { return r.operations }

// Exposure returns the exposure service.
func (r *Runtime) Exposure() *appworkflow.WorkflowExposureService { return r.exposure }

// Lifecycle returns the workflow lifecycle service (authoring, registration, exposure).
func (r *Runtime) Lifecycle() *appworkflow.WorkflowLifecycleService { return r.lifecycle }
