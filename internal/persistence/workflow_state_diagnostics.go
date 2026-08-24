package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	gosched "github.com/hollis-labs/go-scheduler"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
)

// WorkflowActivationAttemptDiagnosticRecord is the bounded, credential-free
// activation journal projection used by Hadron run diagnostics. It preserves
// the scheduler's stable fire identity and per-attempt observed time without
// exposing a claim lease or activation payload.
type WorkflowActivationAttemptDiagnosticRecord struct {
	FireID         string
	RegistrationID string
	PhysicalRunID  workflowruntime.RunID
	SourceKind     hoststate.ActivationSourceKind
	ScheduledAt    time.Time
	ClaimedAt      time.Time
	Attempt        int
	Outcome        string
	ReasonCode     string
}

// ListRunInvocationsForDiagnostics is a bounded read-only projection used by
// Hadron diagnostics. It deliberately does not widen the runtime StateStore.
func (s *WorkflowStateStore) ListRunInvocationsForDiagnostics(ctx context.Context, runID workflowruntime.RunID, limit int) ([]workflowruntime.NodeInvocationSnapshot, bool, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, false, err
	}
	if limit < 1 {
		return nil, false, workflowInvalid(errors.New("diagnostic node limit must be positive"))
	}
	if _, err := loadWorkflowRun(ctx, s.db, runID); err != nil {
		return nil, false, err
	}
	rows, err := s.db.QueryContext(ctx, workflowNodeSelect+` WHERE n.run_id = ? ORDER BY n.node_id, n.iteration LIMIT ?`, runID, limit+1)
	if err != nil {
		return nil, false, fmt.Errorf("list bounded workflow run invocations: %w", err)
	}
	defer closeRows(rows)
	result := make([]workflowruntime.NodeInvocationSnapshot, 0, limit+1)
	for rows.Next() {
		node, scanErr := scanWorkflowNode(rows)
		if scanErr != nil {
			return nil, false, scanErr
		}
		result = append(result, node)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("list bounded workflow run invocations: %w", err)
	}
	truncated := len(result) > limit
	if truncated {
		result = result[:limit]
	}
	return result, truncated, nil
}

// ListAttemptsForDiagnostics reads one deterministic prefix and reports
// truncation instead of materializing an unbounded attempt history.
func (s *WorkflowStateStore) ListAttemptsForDiagnostics(ctx context.Context, id workflowruntime.NodeInvocationID, limit int) ([]workflowruntime.AttemptSnapshot, bool, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, false, err
	}
	if err := id.Validate(); err != nil {
		return nil, false, workflowInvalid(err)
	}
	if limit < 1 {
		return nil, false, workflowInvalid(errors.New("diagnostic attempt limit must be positive"))
	}
	rows, err := s.db.QueryContext(ctx, workflowAttemptSelect+`
WHERE run_id = ? AND node_id = ? AND iteration = ?
ORDER BY attempt_number ASC LIMIT ?`, id.RunID, id.NodeID, id.Iteration, limit+1)
	if err != nil {
		return nil, false, fmt.Errorf("list bounded workflow attempts: %w", err)
	}
	defer closeRows(rows)
	result := make([]workflowruntime.AttemptSnapshot, 0, limit+1)
	for rows.Next() {
		attempt, scanErr := scanWorkflowAttempt(rows)
		if scanErr != nil {
			return nil, false, scanErr
		}
		result = append(result, attempt)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("list bounded workflow attempts: %w", err)
	}
	truncated := len(result) > limit
	if truncated {
		result = result[:limit]
	}
	return result, truncated, nil
}

// ListWorkflowActivationAttemptsForDiagnostics joins a physical workflow run
// to every durable scheduler attempt for its activation fire. The method
// validates the denormalized dispatch columns, canonical dispatch JSON, fire,
// attempt, and result relationships before returning any row.
func (s *WorkflowActivationStore) ListWorkflowActivationAttemptsForDiagnostics(ctx context.Context, runID workflowruntime.RunID, limit int) ([]WorkflowActivationAttemptDiagnosticRecord, bool, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, false, err
	}
	if hoststate.ValidatePublicText(string(runID), 256, true) != nil {
		return nil, false, activationInvalid("diagnostic activation run identity is invalid")
	}
	if limit < 1 || limit > 5000 {
		return nil, false, activationInvalid("diagnostic activation attempt limit must be between 1 and 5000")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT
d.fire_id, d.registration_id, d.attempt, d.status, d.logical_run_id,
COALESCE(d.physical_run_id, ''), COALESCE(d.host_start_key, ''), d.scheduled_at,
d.observed_at, COALESCE(d.reason_code, ''), d.generation, d.dispatch_json,
f.registration_id, f.scheduled_at, f.fired_at, f.attempt, f.status,
COALESCE(f.last_error_code, ''), a.attempt, a.claimed_at, a.claim_expires_at,
r.outcome, r.reason_code, r.completed_at, reg.source_kind, reg.registration_json
FROM workflow_activation_dispatches d
JOIN workflow_activation_fires f ON f.fire_id = d.fire_id
JOIN workflow_activation_registrations reg ON reg.registration_id = d.registration_id
JOIN workflow_activation_attempts a ON a.fire_id = d.fire_id
LEFT JOIN workflow_activation_attempt_results r ON r.fire_id = a.fire_id AND r.attempt = a.attempt
WHERE d.physical_run_id = ?
ORDER BY f.scheduled_at, d.fire_id, a.attempt
LIMIT ?`, runID, limit+1)
	if err != nil {
		return nil, false, fmt.Errorf("list bounded workflow activation attempts: %w", err)
	}
	defer closeRows(rows)
	records := make([]WorkflowActivationAttemptDiagnosticRecord, 0, limit+1)
	for rows.Next() {
		record, scanErr := scanWorkflowActivationAttemptDiagnostic(rows, runID)
		if scanErr != nil {
			return nil, false, scanErr
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("list bounded workflow activation attempts: %w", err)
	}
	truncated := len(records) > limit
	if truncated {
		records = records[:limit]
	}
	return records, truncated, nil
}

type activationAttemptDiagnosticColumns struct {
	dispatchJSON, registrationJSON                     []byte
	dispatchFireID, dispatchRegistrationID             string
	dispatchAttempt                                    int
	dispatchStatus, dispatchLogicalRunID               string
	dispatchPhysicalRunID, dispatchHostStartKey        string
	dispatchScheduledAt, dispatchObservedAt            string
	dispatchReasonCode                                 string
	dispatchGeneration                                 int64
	fireRegistrationID, fireScheduledAt                string
	fireFiredAt                                        sql.NullString
	fireAttempt                                        int
	fireStatus, fireLastError                          string
	attempt                                            int
	attemptClaimedAt, attemptClaimExpiresAt            string
	resultOutcome, resultReasonCode, resultCompletedAt sql.NullString
}

func scanWorkflowActivationAttemptDiagnostic(rows *sql.Rows, runID workflowruntime.RunID) (WorkflowActivationAttemptDiagnosticRecord, error) {
	var columns activationAttemptDiagnosticColumns
	var sourceKind string
	err := rows.Scan(&columns.dispatchFireID, &columns.dispatchRegistrationID, &columns.dispatchAttempt,
		&columns.dispatchStatus, &columns.dispatchLogicalRunID, &columns.dispatchPhysicalRunID,
		&columns.dispatchHostStartKey, &columns.dispatchScheduledAt, &columns.dispatchObservedAt,
		&columns.dispatchReasonCode, &columns.dispatchGeneration, &columns.dispatchJSON,
		&columns.fireRegistrationID, &columns.fireScheduledAt, &columns.fireFiredAt,
		&columns.fireAttempt, &columns.fireStatus, &columns.fireLastError, &columns.attempt,
		&columns.attemptClaimedAt, &columns.attemptClaimExpiresAt, &columns.resultOutcome,
		&columns.resultReasonCode, &columns.resultCompletedAt, &sourceKind, &columns.registrationJSON)
	if err != nil {
		return WorkflowActivationAttemptDiagnosticRecord{}, fmt.Errorf("scan workflow activation attempt diagnostic: %w", err)
	}
	return validateWorkflowActivationAttemptDiagnostic(columns, hoststate.ActivationSourceKind(sourceKind), runID)
}

func validateWorkflowActivationAttemptDiagnostic(columns activationAttemptDiagnosticColumns, sourceKind hoststate.ActivationSourceKind, runID workflowruntime.RunID) (WorkflowActivationAttemptDiagnosticRecord, error) {
	invalid := func(message string) (WorkflowActivationAttemptDiagnosticRecord, error) {
		return WorkflowActivationAttemptDiagnosticRecord{}, activationInvalid("stored activation diagnostic is corrupt: " + message)
	}
	var dispatch hoststate.ActivationDispatch
	if err := decodeActivationJSON(columns.dispatchJSON, &dispatch); err != nil || dispatch.Validate() != nil {
		return invalid("dispatch JSON is invalid")
	}
	var registration hoststate.ActivationRegistration
	if err := decodeActivationJSON(columns.registrationJSON, &registration); err != nil || registration.Validate() != nil ||
		registration.ID != columns.dispatchRegistrationID || registration.Source.Kind != sourceKind {
		return invalid("registration JSON or source kind is invalid")
	}
	dispatchScheduledAt, err := parseWorkflowTime("activation diagnostic dispatch scheduled_at", columns.dispatchScheduledAt)
	if err != nil {
		return WorkflowActivationAttemptDiagnosticRecord{}, err
	}
	dispatchObservedAt, err := parseWorkflowTime("activation diagnostic dispatch observed_at", columns.dispatchObservedAt)
	if err != nil {
		return WorkflowActivationAttemptDiagnosticRecord{}, err
	}
	if columns.dispatchGeneration <= 0 {
		return invalid("dispatch generation is invalid")
	}
	storedDispatch := hoststate.ActivationDispatch{
		FireID: columns.dispatchFireID, RegistrationID: columns.dispatchRegistrationID,
		Attempt: columns.dispatchAttempt, Status: hoststate.ActivationDispatchStatus(columns.dispatchStatus),
		LogicalRunID: columns.dispatchLogicalRunID, PhysicalRunID: workflowruntime.RunID(columns.dispatchPhysicalRunID),
		HostStartKey: columns.dispatchHostStartKey, ScheduledAt: dispatchScheduledAt,
		ObservedAt: dispatchObservedAt, ReasonCode: columns.dispatchReasonCode,
		Generation: uint64(columns.dispatchGeneration),
	}
	if storedDispatch.Validate() != nil || dispatch != storedDispatch || dispatch.PhysicalRunID != runID {
		return invalid("dispatch columns diverge from its canonical snapshot")
	}
	fireScheduledAt, err := parseWorkflowTime("activation diagnostic fire scheduled_at", columns.fireScheduledAt)
	if err != nil {
		return WorkflowActivationAttemptDiagnosticRecord{}, err
	}
	fireFiredAt, err := parseOptionalWorkflowTime("activation diagnostic fire fired_at", columns.fireFiredAt)
	if err != nil {
		return WorkflowActivationAttemptDiagnosticRecord{}, err
	}
	claimedAt, err := parseWorkflowTime("activation diagnostic attempt claimed_at", columns.attemptClaimedAt)
	if err != nil {
		return WorkflowActivationAttemptDiagnosticRecord{}, err
	}
	claimExpiresAt, err := parseWorkflowTime("activation diagnostic attempt claim_expires_at", columns.attemptClaimExpiresAt)
	if err != nil {
		return WorkflowActivationAttemptDiagnosticRecord{}, err
	}
	if columns.fireRegistrationID != dispatch.RegistrationID || !fireScheduledAt.Equal(dispatch.ScheduledAt) ||
		columns.fireAttempt < 1 || columns.attempt < 1 || columns.attempt > columns.fireAttempt ||
		dispatch.Attempt < 1 || dispatch.Attempt > columns.fireAttempt || claimedAt.Before(fireScheduledAt) ||
		!claimExpiresAt.After(claimedAt) || !sourceKind.Valid() || !validActivationFireStatus(gosched.FireStatus(columns.fireStatus)) ||
		hoststate.ValidatePublicText(columns.fireLastError, 128, false) != nil {
		return invalid("fire, attempt, dispatch, or registration relationship is invalid")
	}
	if columns.attempt == columns.fireAttempt && !fireFiredAt.Equal(claimedAt) {
		return invalid("current fire observation differs from its attempt")
	}
	outcome, reasonCode := string(gosched.FireClaimed), ""
	if columns.resultOutcome.Valid || columns.resultReasonCode.Valid || columns.resultCompletedAt.Valid {
		if !columns.resultOutcome.Valid || !columns.resultCompletedAt.Valid || !validActivationAttemptOutcome(columns.resultOutcome.String) {
			return invalid("attempt result is incomplete or unsupported")
		}
		completedAt, parseErr := parseWorkflowTime("activation diagnostic attempt completed_at", columns.resultCompletedAt.String)
		if parseErr != nil {
			return WorkflowActivationAttemptDiagnosticRecord{}, parseErr
		}
		if completedAt.Before(claimedAt) || hoststate.ValidatePublicText(columns.resultReasonCode.String, 128, false) != nil {
			return invalid("attempt result time or reason is invalid")
		}
		outcome, reasonCode = columns.resultOutcome.String, columns.resultReasonCode.String
		if columns.attempt == columns.fireAttempt && !activationResultMatchesFire(outcome, gosched.FireStatus(columns.fireStatus)) {
			return invalid("current attempt result differs from fire status")
		}
		if columns.attempt == columns.fireAttempt && columns.fireLastError != reasonCode {
			return invalid("current attempt reason differs from fire error code")
		}
	} else if columns.attempt != columns.fireAttempt || gosched.FireStatus(columns.fireStatus) != gosched.FireClaimed {
		return invalid("unfinished result does not describe the current claimed attempt")
	}
	return WorkflowActivationAttemptDiagnosticRecord{
		FireID: dispatch.FireID, RegistrationID: dispatch.RegistrationID, PhysicalRunID: runID,
		SourceKind: sourceKind, ScheduledAt: fireScheduledAt.UTC(), ClaimedAt: claimedAt.UTC(),
		Attempt: columns.attempt, Outcome: outcome, ReasonCode: reasonCode,
	}, nil
}

func validActivationFireStatus(status gosched.FireStatus) bool {
	switch status {
	case gosched.FirePending, gosched.FireClaimed, gosched.FireRetrying, gosched.FireSucceeded, gosched.FireSkipped, gosched.FireExhausted:
		return true
	default:
		return false
	}
}

func validActivationAttemptOutcome(outcome string) bool {
	switch outcome {
	case string(gosched.FireRetrying), string(gosched.FireSucceeded), string(gosched.FireSkipped), string(gosched.FireExhausted), "abandoned":
		return true
	default:
		return false
	}
}

func activationResultMatchesFire(outcome string, status gosched.FireStatus) bool {
	return outcome == string(status) || (outcome == "abandoned" && status == gosched.FireRetrying)
}
