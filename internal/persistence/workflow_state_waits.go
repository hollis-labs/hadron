package persistence

import (
	"context"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
)

// LoadWait implements runtime.StateStore.
func (s *WorkflowStateStore) LoadWait(ctx context.Context, id workflowruntime.WaitID) (workflowruntime.WaitSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.WaitSnapshot{}, err
	}
	return loadWorkflowWait(ctx, s.db, id)
}
