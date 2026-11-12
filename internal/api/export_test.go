package api

import "time"

// SetWorkflowStreamTuning overrides the event stream's minimum wait and
// maximum lifetime for a test and returns a function that restores them.
func SetWorkflowStreamTuning(minWait, maxLifetime time.Duration) func() {
	previousWait, previousLifetime := workflowStreamMinWait, workflowStreamMaxLifetime
	workflowStreamMinWait, workflowStreamMaxLifetime = minWait, maxLifetime
	return func() { workflowStreamMinWait, workflowStreamMaxLifetime = previousWait, previousLifetime }
}
