package rundiagnostics

import (
	"context"
	"errors"

	"github.com/hollis-labs/hadron/internal/persistence"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
)

// PersistenceActivationAttemptSource adapts migration-0023 activation journal
// reads to the transport-neutral diagnostics contract. The underlying query
// returns only credential-free facts and validates its joins before mapping.
type PersistenceActivationAttemptSource struct {
	Store *persistence.WorkflowActivationStore
}

var _ ActivationAttemptSource = PersistenceActivationAttemptSource{}

func (s PersistenceActivationAttemptSource) ListRunActivationAttempts(ctx context.Context, runID workflowruntime.RunID, limit int) ([]ActivationFireAttempt, bool, error) {
	if s.Store == nil {
		return nil, false, errors.New("workflow activation diagnostic store is required")
	}
	records, truncated, err := s.Store.ListWorkflowActivationAttemptsForDiagnostics(ctx, runID, limit)
	if err != nil {
		return nil, false, err
	}
	result := make([]ActivationFireAttempt, 0, len(records))
	for _, record := range records {
		result = append(result, ActivationFireAttempt{
			FireID: record.FireID, ActivationID: record.RegistrationID, RunID: record.PhysicalRunID,
			ScheduledAt: record.ScheduledAt, FiredAt: record.ClaimedAt, Attempt: record.Attempt,
			Status: record.Outcome, FailureCode: record.ReasonCode, Source: string(record.SourceKind),
		})
	}
	return result, truncated, nil
}
