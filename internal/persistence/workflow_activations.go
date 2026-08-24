package persistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	gosched "github.com/hollis-labs/go-scheduler"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

const workflowActivationClaimLease = 2 * time.Minute

type WorkflowActivationStore struct {
	db    *sql.DB
	state *WorkflowStateStore
}

var _ hoststate.ActivationStore = (*WorkflowActivationStore)(nil)

func NewWorkflowActivationStore(store *Store) (*WorkflowActivationStore, error) {
	state, err := NewWorkflowStateStore(store)
	if err != nil {
		return nil, err
	}
	return &WorkflowActivationStore{db: store.db, state: state}, nil
}

func (s *WorkflowActivationStore) RegisterActivation(ctx context.Context, input hoststate.ActivationRegistration) (hoststate.ActivationRegistration, workflowruntime.IdempotencyOutcome, error) {
	registration, err := input.Clone()
	if err != nil || registration.Validate() != nil {
		return hoststate.ActivationRegistration{}, "", activationInvalid("registration is malformed")
	}
	registration.CreatedAt = registration.CreatedAt.UTC()
	registration.UpdatedAt = registration.UpdatedAt.UTC()
	registration.ExpiresAt = registration.ExpiresAt.UTC()
	encoded, err := encodeActivationJSON(registration)
	if err != nil {
		return hoststate.ActivationRegistration{}, "", activationInvalid("registration is not JSON-compatible")
	}
	scopeKey, err := workflowActivationScopeKey(registration.RunScope)
	if err != nil {
		return hoststate.ActivationRegistration{}, "", activationInvalid("registration scope is invalid")
	}
	outcome := workflowruntime.IdempotencyApplied
	err = s.state.write(ctx, "register workflow activation", func(query workflowSQL) error {
		prior, loadErr := loadWorkflowActivation(ctx, query, registration.ID)
		if loadErr == nil {
			priorJSON, _ := encodeActivationJSON(prior)
			if !bytes.Equal(priorJSON, encoded) {
				return &workflowruntime.IdempotencyConflictError{Operation: "register workflow activation", Key: registration.ID}
			}
			registration, outcome = prior, workflowruntime.IdempotencyReplayed
			return nil
		}
		if !errors.Is(loadErr, workflowruntime.ErrNotFound) {
			return loadErr
		}
		if _, execErr := query.ExecContext(ctx, `INSERT INTO workflow_activation_registrations(
registration_id, version, source_kind, scope_key, enabled, expires_at, generation, created_at, updated_at, registration_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, registration.ID, registration.Version, registration.Source.Kind, scopeKey,
			boolInteger(registration.Enabled), workflowOptionalTime(registration.ExpiresAt), registration.Generation,
			workflowTime(registration.CreatedAt), workflowTime(registration.UpdatedAt), encoded); execErr != nil {
			return fmt.Errorf("insert workflow activation registration: %w", execErr)
		}
		return insertWorkflowActivationSchedule(ctx, query, registration)
	})
	if err != nil {
		return hoststate.ActivationRegistration{}, "", err
	}
	cloned, cloneErr := registration.Clone()
	return cloned, outcome, cloneErr
}

func (s *WorkflowActivationStore) LoadActivation(ctx context.Context, id string) (hoststate.ActivationRegistration, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return hoststate.ActivationRegistration{}, err
	}
	return loadWorkflowActivation(ctx, s.db, id)
}

func loadWorkflowActivation(ctx context.Context, query workflowSQL, id string) (hoststate.ActivationRegistration, error) {
	var encoded []byte
	var version, sourceKind, scopeKey, created, updated string
	var enabled int
	var expires sql.NullString
	var generation int64
	err := query.QueryRowContext(ctx, `SELECT version, source_kind, scope_key, enabled, expires_at, generation, created_at, updated_at, registration_json
FROM workflow_activation_registrations WHERE registration_id = ?`, id).Scan(&version, &sourceKind, &scopeKey, &enabled, &expires, &generation, &created, &updated, &encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return hoststate.ActivationRegistration{}, fmt.Errorf("%w: workflow activation %q", workflowruntime.ErrNotFound, id)
	}
	if err != nil {
		return hoststate.ActivationRegistration{}, err
	}
	var registration hoststate.ActivationRegistration
	if decodeErr := decodeActivationJSON(encoded, &registration); decodeErr != nil {
		return registration, activationInvalid("stored activation registration JSON is corrupt")
	}
	if generation <= 0 {
		return registration, activationInvalid("stored activation registration generation is invalid")
	}
	parsedCreated, err := parseWorkflowTime("activation created_at", created)
	if err != nil {
		return registration, err
	}
	parsedUpdated, err := parseWorkflowTime("activation updated_at", updated)
	if err != nil {
		return registration, err
	}
	parsedExpiry, err := parseOptionalWorkflowTime("activation expires_at", expires)
	if err != nil {
		return registration, err
	}
	actualScope, digestErr := workflowActivationScopeKey(registration.RunScope)
	if digestErr != nil || registration.ID != id || registration.Version != version || string(registration.Source.Kind) != sourceKind ||
		actualScope != scopeKey || registration.Enabled != (enabled != 0) || !registration.ExpiresAt.Equal(parsedExpiry) ||
		registration.Generation != uint64(generation) || !registration.CreatedAt.Equal(parsedCreated) || !registration.UpdatedAt.Equal(parsedUpdated) {
		return registration, activationInvalid("stored activation registration columns diverge from snapshot")
	}
	if err := registration.Validate(); err != nil {
		return registration, activationInvalid("stored activation registration is invalid")
	}
	return registration.Clone()
}

func insertWorkflowActivationSchedule(ctx context.Context, query workflowSQL, registration hoststate.ActivationRegistration) error {
	if registration.Source.Kind != hoststate.ActivationSourceSchedule && registration.Source.Kind != hoststate.ActivationSourceTimer {
		return nil
	}
	var cron string
	var next time.Time
	if registration.Source.Kind == hoststate.ActivationSourceSchedule {
		cron, _ = registration.Source.Config["cron"].(string)
		var err error
		next, err = gosched.NextRun(cron, registration.CreatedAt)
		if err != nil {
			return activationInvalid("activation cron expression is invalid")
		}
	} else {
		fireAt, ok := registration.Source.Config["fire_at"].(string)
		if !ok {
			return activationInvalid("timer activation requires fire_at")
		}
		parsed, err := time.Parse(time.RFC3339Nano, fireAt)
		if err != nil || parsed.Location() != time.UTC || !parsed.After(registration.CreatedAt) {
			return activationInvalid("timer activation fire_at is invalid")
		}
		next = parsed
	}
	retry := toSchedulerRetry(registration.Policy.Retry)
	retryJSON, _ := json.Marshal(retry)
	payloadJSON, _ := json.Marshal(struct {
		Payload map[string]any `json:"payload"`
	}{Payload: map[string]any{}})
	_, err := query.ExecContext(ctx, `INSERT INTO workflow_activation_schedules(
registration_id, cron_expr, last_run_at, next_run_at, enabled, generation, retry_json, payload_json
) VALUES (?, ?, NULL, ?, ?, 1, ?, ?)`, registration.ID, cron, workflowTime(next), boolInteger(registration.Enabled), retryJSON, payloadJSON)
	if err != nil {
		return fmt.Errorf("insert workflow activation schedule: %w", err)
	}
	return nil
}

func (s *WorkflowActivationStore) ListDueSchedules(ctx context.Context, now time.Time, limit int) ([]gosched.Schedule, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, activationInvalid("schedule query limit must be positive")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT s.registration_id, s.cron_expr, s.last_run_at, s.next_run_at,
s.enabled, s.retry_json, s.payload_json
FROM workflow_activation_schedules s
JOIN workflow_activation_registrations r ON r.registration_id = s.registration_id
WHERE s.enabled = 1 AND r.enabled = 1 AND (r.expires_at IS NULL OR r.expires_at > ?) AND s.next_run_at <= ?
ORDER BY s.next_run_at, s.registration_id LIMIT ?`, workflowTime(now.UTC()), workflowTime(now.UTC()), limit)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	result := make([]gosched.Schedule, 0)
	for rows.Next() {
		var schedule gosched.Schedule
		var last sql.NullString
		var next string
		var enabled int
		var retryJSON, payload []byte
		if scanErr := rows.Scan(&schedule.ID, &schedule.CronExpr, &last, &next, &enabled, &retryJSON, &payload); scanErr != nil {
			return nil, scanErr
		}
		schedule.LastRun, err = parseOptionalWorkflowTime("activation last run", last)
		if err != nil {
			return nil, err
		}
		schedule.NextRun, err = parseWorkflowTime("activation next run", next)
		if err != nil {
			return nil, err
		}
		if err := decodeActivationJSON(retryJSON, &schedule.Retry); err != nil || schedule.Retry.Validate() != nil {
			return nil, activationInvalid("stored activation retry policy is invalid")
		}
		schedule.Enabled, schedule.JobType, schedule.Payload = enabled != 0, "hadron.workflow.activation", append([]byte(nil), payload...)
		result = append(result, schedule)
	}
	return result, rows.Err()
}

func (s *WorkflowActivationStore) CreateFire(ctx context.Context, creation gosched.FireCreation) (bool, error) {
	created := false
	err := s.state.write(ctx, "create workflow activation fire", func(query workflowSQL) error {
		var next string
		var enabled int
		var generation int64
		if err := query.QueryRowContext(ctx, `SELECT next_run_at, enabled, generation FROM workflow_activation_schedules WHERE registration_id = ?`, creation.ScheduleID).Scan(&next, &enabled, &generation); errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: workflow activation schedule", workflowruntime.ErrNotFound)
		} else if err != nil {
			return err
		}
		parsedNext, err := parseWorkflowTime("activation expected next", next)
		if err != nil {
			return err
		}
		if enabled == 0 || !parsedNext.Equal(creation.ExpectedNext) {
			return nil
		}
		if generation <= 0 {
			return activationInvalid("stored activation schedule generation is invalid")
		}
		if creation.Fire.ID != gosched.DeriveFireID(creation.ScheduleID, creation.ExpectedNext) || creation.Fire.ScheduleID != creation.ScheduleID ||
			creation.Fire.Status != gosched.FirePending || creation.Fire.Attempt != 0 || !creation.Fire.ScheduledAt.Equal(creation.ExpectedNext) {
			return activationInvalid("scheduler fire creation is incoherent")
		}
		if _, loadErr := loadWorkflowActivationFire(ctx, query, creation.Fire.ID); loadErr == nil {
			return nil
		} else if !errors.Is(loadErr, workflowruntime.ErrNotFound) {
			return loadErr
		}
		if insertErr := insertWorkflowActivationFire(ctx, query, creation.Fire); insertErr != nil {
			return insertErr
		}
		result, err := query.ExecContext(ctx, `UPDATE workflow_activation_schedules SET last_run_at = ?, next_run_at = ?, generation = generation + 1
WHERE registration_id = ? AND generation = ? AND next_run_at = ?`, workflowTime(creation.ExpectedNext), workflowTime(creation.NextRun), creation.ScheduleID, generation, workflowTime(parsedNext))
		if err != nil {
			return err
		}
		count, _ := result.RowsAffected()
		created = count == 1
		if !created {
			return workflowCAS("activation schedule", uint64(generation), uint64(generation+1))
		}
		return appendWorkflowActivationEvent(ctx, query, creation.ScheduleID, creation.Fire.ID, 0, "fire_created", "", creation.Fire.ScheduledAt)
	})
	if errors.Is(err, workflowruntime.ErrCASMismatch) {
		return false, nil
	}
	return created, err
}

func (s *WorkflowActivationStore) ListDueFires(ctx context.Context, now time.Time, limit int) ([]gosched.Fire, error) {
	if limit <= 0 {
		return nil, activationInvalid("fire query limit must be positive")
	}
	now = now.UTC()
	var result []gosched.Fire
	err := s.state.write(ctx, "recover and list workflow activation fires", func(query workflowSQL) error {
		rows, rowsErr := query.QueryContext(ctx, `SELECT fire_id FROM workflow_activation_fires
WHERE status = ? AND claim_expires_at <= ? ORDER BY claim_expires_at, fire_id`, gosched.FireClaimed, workflowTime(now))
		if rowsErr != nil {
			return rowsErr
		}
		var stale []string
		for rows.Next() {
			var id string
			if scanErr := rows.Scan(&id); scanErr != nil {
				closeRows(rows)
				return scanErr
			}
			stale = append(stale, id)
		}
		closeRows(rows)
		for _, id := range stale {
			fire, loadErr := loadWorkflowActivationFire(ctx, query, id)
			if loadErr != nil {
				return loadErr
			}
			updateResult, updateErr := query.ExecContext(ctx, `UPDATE workflow_activation_fires SET status = ?, next_attempt_at = ?,
claim_expires_at = NULL, last_error_code = ?, generation = generation + 1
WHERE fire_id = ? AND status = ? AND attempt = ? AND claim_expires_at <= ?`, gosched.FireRetrying, workflowTime(now), "claim_expired", id, gosched.FireClaimed, fire.Attempt, workflowTime(now))
			if updateErr != nil {
				return updateErr
			}
			count, _ := updateResult.RowsAffected()
			if count != 1 {
				continue
			}
			if _, err := query.ExecContext(ctx, `INSERT INTO workflow_activation_attempt_results(fire_id, attempt, outcome, reason_code, completed_at) VALUES (?, ?, ?, ?, ?)`, id, fire.Attempt, "abandoned", "claim_expired", workflowTime(now)); err != nil {
				return err
			}
			if err := appendWorkflowActivationEvent(ctx, query, fire.ScheduleID, id, fire.Attempt, "claim_expired", "claim_expired", now); err != nil {
				return err
			}
		}
		rows, rowsErr = query.QueryContext(ctx, `SELECT fire_id FROM workflow_activation_fires
WHERE status IN (?, ?) AND next_attempt_at <= ? ORDER BY next_attempt_at, fire_id LIMIT ?`, gosched.FirePending, gosched.FireRetrying, workflowTime(now), limit)
		if rowsErr != nil {
			return rowsErr
		}
		defer closeRows(rows)
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			fire, err := loadWorkflowActivationFire(ctx, query, id)
			if err != nil {
				return err
			}
			result = append(result, fire)
		}
		return rows.Err()
	})
	return result, err
}

func (s *WorkflowActivationStore) ClaimFire(ctx context.Context, claim gosched.FireClaim) (gosched.Fire, bool, error) {
	var claimed gosched.Fire
	won := false
	err := s.state.write(ctx, "claim workflow activation fire", func(query workflowSQL) error {
		fire, err := loadWorkflowActivationFire(ctx, query, claim.FireID)
		if err != nil {
			return err
		}
		if fire.Status != claim.ExpectedStatus || fire.Attempt != claim.ExpectedAttempt || claim.ClaimedAt.Before(fire.ScheduledAt) {
			return nil
		}
		claimed = fire
		claimed.Attempt++
		claimed.Status, claimed.FiredAt, claimed.NextAttemptAt = gosched.FireClaimed, claim.ClaimedAt.UTC(), time.Time{}
		expires := claim.ClaimedAt.UTC().Add(workflowActivationClaimLease)
		result, err := query.ExecContext(ctx, `UPDATE workflow_activation_fires SET fired_at = ?, attempt = ?, status = ?,
next_attempt_at = NULL, claim_expires_at = ?, generation = generation + 1
WHERE fire_id = ? AND status = ? AND attempt = ?`, workflowTime(claimed.FiredAt), claimed.Attempt, claimed.Status,
			workflowTime(expires), fire.ID, fire.Status, fire.Attempt)
		if err != nil {
			return err
		}
		count, _ := result.RowsAffected()
		if count != 1 {
			return nil
		}
		if _, err := query.ExecContext(ctx, `INSERT INTO workflow_activation_attempts(fire_id, attempt, claimed_at, claim_expires_at) VALUES (?, ?, ?, ?)`, fire.ID, claimed.Attempt, workflowTime(claimed.FiredAt), workflowTime(expires)); err != nil {
			return err
		}
		won = true
		return appendWorkflowActivationEvent(ctx, query, fire.ScheduleID, fire.ID, claimed.Attempt, "claim", "", claimed.FiredAt)
	})
	return claimed, won, err
}

func (s *WorkflowActivationStore) TransitionFire(ctx context.Context, transition gosched.FireTransition) (bool, error) {
	applied := false
	err := s.state.write(ctx, "transition workflow activation fire", func(query workflowSQL) error {
		fire, err := loadWorkflowActivationFire(ctx, query, transition.FireID)
		if err != nil {
			return err
		}
		if fire.Status != transition.From || fire.Attempt != transition.Attempt {
			return nil
		}
		var claimExpires string
		claimErr := query.QueryRowContext(ctx, `SELECT claim_expires_at FROM workflow_activation_fires
WHERE fire_id = ? AND status = ? AND attempt = ?`, fire.ID, gosched.FireClaimed, fire.Attempt).Scan(&claimExpires)
		if errors.Is(claimErr, sql.ErrNoRows) {
			return nil
		} else if claimErr != nil {
			return claimErr
		}
		expiresAt, err := parseWorkflowTime("activation claim expiry", claimExpires)
		if err != nil {
			return err
		}
		if !transition.At.Before(expiresAt) {
			return nil
		}
		switch transition.To {
		case gosched.FireRetrying, gosched.FireSucceeded, gosched.FireSkipped, gosched.FireExhausted:
		default:
			return activationInvalid("fire transition target is unsupported")
		}
		if transition.At.Before(fire.FiredAt) || (transition.To == gosched.FireRetrying && transition.NextAttemptAt.Before(transition.At)) {
			return activationInvalid("fire transition timestamps regress")
		}
		next := time.Time{}
		if transition.To == gosched.FireRetrying {
			next = transition.NextAttemptAt.UTC()
		}
		reason := safeActivationReason(transition)
		result, err := query.ExecContext(ctx, `UPDATE workflow_activation_fires SET status = ?, next_attempt_at = ?,
claim_expires_at = NULL, last_error_code = ?, generation = generation + 1
WHERE fire_id = ? AND status = ? AND attempt = ?`, transition.To, workflowOptionalTime(next), reason, fire.ID, fire.Status, fire.Attempt)
		if err != nil {
			return err
		}
		count, _ := result.RowsAffected()
		if count != 1 {
			return nil
		}
		if _, err := query.ExecContext(ctx, `INSERT INTO workflow_activation_attempt_results(fire_id, attempt, outcome, reason_code, completed_at) VALUES (?, ?, ?, ?, ?)`, fire.ID, fire.Attempt, transition.To, reason, workflowTime(transition.At.UTC())); err != nil {
			return err
		}
		if transition.To == gosched.FireExhausted {
			dispatch, dispatchErr := loadWorkflowActivationDispatch(ctx, query, fire.ID)
			if dispatchErr == nil && dispatch.Status == hoststate.ActivationDispatchStarting {
				priorGeneration := dispatch.Generation
				dispatch.Status, dispatch.ReasonCode, dispatch.ObservedAt, dispatch.Generation = hoststate.ActivationDispatchExhausted, "attempts_exhausted", transition.At.UTC(), dispatch.Generation+1
				encoded, _ := encodeActivationJSON(dispatch)
				updated, err := query.ExecContext(ctx, `UPDATE workflow_activation_dispatches SET status = ?, reason_code = ?, observed_at = ?, generation = ?, dispatch_json = ?
WHERE fire_id = ? AND generation = ? AND status = ?`, dispatch.Status, dispatch.ReasonCode, workflowTime(dispatch.ObservedAt), dispatch.Generation,
					encoded, dispatch.FireID, priorGeneration, hoststate.ActivationDispatchStarting)
				if err != nil {
					return err
				}
				count, _ := updated.RowsAffected()
				if count != 1 {
					return workflowCAS("activation dispatch exhaustion", priorGeneration, priorGeneration+1)
				}
			} else if dispatchErr != nil && !errors.Is(dispatchErr, workflowruntime.ErrNotFound) {
				return dispatchErr
			}
		}
		applied = true
		return appendWorkflowActivationEvent(ctx, query, fire.ScheduleID, fire.ID, fire.Attempt, string(transition.To), reason, transition.At.UTC())
	})
	return applied, err
}

func (s *WorkflowActivationStore) DisableSchedule(ctx context.Context, id string) error {
	return s.state.write(ctx, "disable workflow activation schedule", func(query workflowSQL) error {
		if _, err := query.ExecContext(ctx, `UPDATE workflow_activation_schedules SET enabled = 0, generation = generation + 1 WHERE registration_id = ?`, id); err != nil {
			return err
		}
		return nil
	})
}

func insertWorkflowActivationFire(ctx context.Context, query workflowSQL, fire gosched.Fire) error {
	if fire.ID == "" || fire.ScheduleID == "" || fire.ScheduledAt.IsZero() || fire.Attempt < 0 || fire.Retry.Validate() != nil {
		return activationInvalid("workflow activation fire is invalid")
	}
	retryJSON, _ := json.Marshal(fire.Retry)
	_, err := query.ExecContext(ctx, `INSERT INTO workflow_activation_fires(
fire_id, registration_id, scheduled_at, fired_at, attempt, status, next_attempt_at, claim_expires_at,
last_error_code, retry_json, job_type, payload_json, generation
) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, 1)`, fire.ID, fire.ScheduleID, workflowTime(fire.ScheduledAt),
		workflowOptionalTime(fire.FiredAt), fire.Attempt, fire.Status, workflowOptionalTime(fire.NextAttemptAt),
		fire.LastError, retryJSON, fire.JobType, append([]byte(nil), fire.Payload...))
	if err != nil {
		return fmt.Errorf("insert workflow activation fire: %w", err)
	}
	return nil
}

func loadWorkflowActivationFire(ctx context.Context, query workflowSQL, id string) (gosched.Fire, error) {
	var fire gosched.Fire
	var scheduled string
	var fired, next sql.NullString
	var retryJSON []byte
	err := query.QueryRowContext(ctx, `SELECT fire_id, registration_id, scheduled_at, fired_at, attempt, status,
next_attempt_at, COALESCE(last_error_code, ''), retry_json, job_type, payload_json FROM workflow_activation_fires WHERE fire_id = ?`, id).
		Scan(&fire.ID, &fire.ScheduleID, &scheduled, &fired, &fire.Attempt, &fire.Status, &next, &fire.LastError, &retryJSON, &fire.JobType, &fire.Payload)
	if errors.Is(err, sql.ErrNoRows) {
		return fire, fmt.Errorf("%w: workflow activation fire %q", workflowruntime.ErrNotFound, id)
	}
	if err != nil {
		return fire, err
	}
	if fire.ScheduledAt, err = parseWorkflowTime("activation fire scheduled_at", scheduled); err != nil {
		return fire, err
	}
	if fire.FiredAt, err = parseOptionalWorkflowTime("activation fire fired_at", fired); err != nil {
		return fire, err
	}
	if fire.NextAttemptAt, err = parseOptionalWorkflowTime("activation fire next_attempt_at", next); err != nil {
		return fire, err
	}
	if err := decodeActivationJSON(retryJSON, &fire.Retry); err != nil || fire.Retry.Validate() != nil {
		return fire, activationInvalid("stored activation fire retry policy is invalid")
	}
	return fire, nil
}

func (s *WorkflowActivationStore) RecordActivationEvent(ctx context.Context, input hoststate.ActivationEvent) (gosched.Fire, workflowruntime.IdempotencyOutcome, error) {
	event, err := input.Clone()
	if err != nil || event.Validate() != nil {
		return gosched.Fire{}, "", activationInvalid("external activation event is invalid")
	}
	encoded, _ := encodeActivationJSON(event)
	digest := values.SHA256Digest(encoded)
	fireID := externalActivationFireID(event.RegistrationID, event.IdempotencyKey)
	var recorded gosched.Fire
	outcome := workflowruntime.IdempotencyApplied
	err = s.state.write(ctx, "record external workflow activation", func(query workflowSQL) error {
		registration, loadErr := loadWorkflowActivation(ctx, query, event.RegistrationID)
		if loadErr != nil {
			return loadErr
		}
		if registration.Source.Kind != hoststate.ActivationSourceWebhook && registration.Source.Kind != hoststate.ActivationSourceFile &&
			registration.Source.Kind != hoststate.ActivationSourceExternal {
			return activationInvalid("registration does not accept external activation events")
		}
		var priorDigest, priorFire string
		loadErr = query.QueryRowContext(ctx, `SELECT event_digest, fire_id FROM workflow_activation_external_events WHERE registration_id = ? AND idempotency_key = ?`, event.RegistrationID, event.IdempotencyKey).Scan(&priorDigest, &priorFire)
		if loadErr == nil {
			if priorDigest != digest {
				return &workflowruntime.IdempotencyConflictError{Operation: "record external activation", Key: event.IdempotencyKey}
			}
			fireID, outcome = priorFire, workflowruntime.IdempotencyReplayed
			recorded, loadErr = loadWorkflowActivationFire(ctx, query, fireID)
			return loadErr
		}
		if !errors.Is(loadErr, sql.ErrNoRows) {
			return loadErr
		}
		payload, payloadErr := activationEventJobPayload(event.Payload)
		if payloadErr != nil {
			return payloadErr
		}
		fire := gosched.Fire{ID: fireID, ScheduleID: event.RegistrationID, ScheduledAt: event.OccurredAt, Status: gosched.FirePending,
			NextAttemptAt: event.OccurredAt, Retry: toSchedulerRetry(registration.Policy.Retry), JobType: "hadron.workflow.activation.external", Payload: payload}
		if insertErr := insertWorkflowActivationFire(ctx, query, fire); insertErr != nil {
			return insertErr
		}
		recorded = fire
		if _, insertErr := query.ExecContext(ctx, `INSERT INTO workflow_activation_external_events(registration_id, idempotency_key, event_digest, fire_id, occurred_at, event_json) VALUES (?, ?, ?, ?, ?, ?)`, event.RegistrationID, event.IdempotencyKey, digest, fireID, workflowTime(event.OccurredAt), encoded); insertErr != nil {
			return insertErr
		}
		return appendWorkflowActivationEvent(ctx, query, event.RegistrationID, fireID, 0, "external_received", "", event.OccurredAt)
	})
	return recorded, outcome, err
}

func activationEventJobPayload(payload values.ValueSet) ([]byte, error) {
	result := make(map[string]any, len(payload))
	for name, value := range payload {
		if value.Type == values.TypeArtifact || value.Type == values.TypeSecretRef {
			return nil, activationInvalid("classified activation event values cannot enter inline run inputs")
		}
		result[name] = value.Inline
	}
	return json.Marshal(struct {
		Payload map[string]any `json:"payload"`
	}{Payload: result})
}

func (s *WorkflowActivationStore) PrepareActivation(ctx context.Context, request hoststate.ActivationPrepareRequest) (hoststate.ActivationPrepareResult, error) {
	var result hoststate.ActivationPrepareResult
	err := s.state.write(ctx, "prepare workflow activation", func(query workflowSQL) error {
		registration, err := loadWorkflowActivation(ctx, query, request.RegistrationID)
		if err != nil {
			return err
		}
		if request.ExpectedRegistrationGeneration != registration.Generation {
			return workflowCAS("activation registration", request.ExpectedRegistrationGeneration, registration.Generation)
		}
		if request.Attempt < 1 || request.ObservedAt.Before(request.ScheduledAt) ||
			hoststate.ValidatePublicText(request.LogicalRunID, 256, true) != nil {
			return activationInvalid("activation prepare request is invalid")
		}
		fire, err := loadWorkflowActivationFire(ctx, query, request.FireID)
		if err != nil {
			return err
		}
		if fire.ScheduleID != registration.ID || !workflowActivationSourceMatchesFire(registration.Source.Kind, fire.JobType) {
			return activationInvalid("activation fire does not match its registration source")
		}
		if !fire.ScheduledAt.Equal(request.ScheduledAt) || fire.Status != gosched.FireClaimed || fire.Attempt != request.Attempt {
			return activationInvalid("activation fire is not the exact currently claimed attempt")
		}
		if prior, loadErr := loadWorkflowActivationDispatch(ctx, query, request.FireID); loadErr == nil {
			if prior.RegistrationID != request.RegistrationID || prior.LogicalRunID != request.LogicalRunID ||
				!prior.ScheduledAt.Equal(request.ScheduledAt) {
				return &workflowruntime.IdempotencyConflictError{Operation: "prepare workflow activation", Key: request.FireID}
			}
			result = hoststate.ActivationPrepareResult{Registration: registration, Dispatch: prior, Outcome: workflowruntime.IdempotencyReplayed}
			result.ReplaceRuns, err = workflowActivationReplacementRuns(ctx, query, registration, request.FireID, request.LogicalRunID)
			return err
		} else if !errors.Is(loadErr, workflowruntime.ErrNotFound) {
			return loadErr
		}
		status, reason := hoststate.ActivationDispatchStarting, ""
		if !registration.Enabled || (!registration.ExpiresAt.IsZero() && !request.ObservedAt.Before(registration.ExpiresAt)) {
			status, reason = hoststate.ActivationDispatchSkipped, "registration_inactive"
		} else if registration.Policy.StartingDeadline > 0 && request.ObservedAt.After(request.ScheduledAt.Add(registration.Policy.StartingDeadline)) && !registration.Policy.Catchup {
			status, reason = hoststate.ActivationDispatchSkipped, "starting_deadline_missed"
		}
		active, err := workflowActivationActiveRuns(ctx, query, registration.ID, "", request.FireID)
		if err != nil {
			return err
		}
		if status == hoststate.ActivationDispatchStarting && registration.Policy.Overlap == graph.OverlapForbid && len(active) != 0 {
			status, reason = hoststate.ActivationDispatchSkipped, "overlap_forbidden"
		}
		logicalExisting, err := workflowActivationScopeRuns(ctx, query, registration.RunScope, request.LogicalRunID, request.FireID)
		if err != nil {
			return err
		}
		if status == hoststate.ActivationDispatchStarting && registration.Policy.RunIDReuse == graph.RunIDReuseReject && len(logicalExisting) != 0 {
			status, reason = hoststate.ActivationDispatchSkipped, "logical_run_exists"
		}
		dispatch := hoststate.ActivationDispatch{FireID: request.FireID, RegistrationID: request.RegistrationID, Attempt: request.Attempt,
			Status: status, LogicalRunID: request.LogicalRunID, ScheduledAt: request.ScheduledAt.UTC(), ObservedAt: request.ObservedAt.UTC(), ReasonCode: reason, Generation: 1}
		if status == hoststate.ActivationDispatchStarting {
			dispatch.PhysicalRunID = physicalWorkflowActivationRunID(request.LogicalRunID, request.FireID)
			dispatch.HostStartKey = workflowActivationStartKey(request.RegistrationID, request.FireID)
			if registration.Policy.Overlap == graph.OverlapReplace {
				result.ReplaceRuns = append(result.ReplaceRuns, active...)
			}
			if registration.Policy.RunIDReuse == graph.RunIDReuseTerminateExisting {
				activeLogical, activeErr := workflowActivationActiveScopeRuns(ctx, query, registration.RunScope, request.LogicalRunID, request.FireID)
				if activeErr != nil {
					return activeErr
				}
				result.ReplaceRuns = append(result.ReplaceRuns, activeLogical...)
			}
			result.ReplaceRuns = sortedUniqueRunIDs(result.ReplaceRuns)
		}
		if err := dispatch.Validate(); err != nil {
			return activationInvalid("prepared activation dispatch is invalid")
		}
		encoded, _ := encodeActivationJSON(dispatch)
		if _, err := query.ExecContext(ctx, `INSERT INTO workflow_activation_dispatches(fire_id, registration_id, attempt, status,
logical_run_id, physical_run_id, host_start_key, scheduled_at, observed_at, reason_code, generation, dispatch_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, dispatch.FireID, dispatch.RegistrationID, dispatch.Attempt, dispatch.Status,
			dispatch.LogicalRunID, optionalString(string(dispatch.PhysicalRunID)), optionalString(dispatch.HostStartKey), workflowTime(dispatch.ScheduledAt),
			workflowTime(dispatch.ObservedAt), optionalString(dispatch.ReasonCode), dispatch.Generation, encoded); err != nil {
			return err
		}
		result.Registration, result.Dispatch, result.Outcome = registration, dispatch, workflowruntime.IdempotencyApplied
		return appendWorkflowActivationEvent(ctx, query, registration.ID, request.FireID, request.Attempt, "dispatch_prepared", reason, request.ObservedAt)
	})
	return result, err
}

func workflowActivationSourceMatchesFire(kind hoststate.ActivationSourceKind, jobType string) bool {
	switch jobType {
	case "hadron.workflow.activation":
		return kind == hoststate.ActivationSourceSchedule || kind == hoststate.ActivationSourceTimer
	case "hadron.workflow.activation.external":
		return kind == hoststate.ActivationSourceWebhook || kind == hoststate.ActivationSourceFile || kind == hoststate.ActivationSourceExternal
	default:
		return false
	}
}

func (s *WorkflowActivationStore) CompleteActivation(ctx context.Context, request hoststate.ActivationCompleteRequest) (hoststate.ActivationDispatch, error) {
	var result hoststate.ActivationDispatch
	err := s.state.write(ctx, "complete workflow activation", func(query workflowSQL) error {
		current, err := loadWorkflowActivationDispatch(ctx, query, request.FireID)
		if err != nil {
			return err
		}
		if current.Status == request.Status && current.Attempt == request.Attempt {
			result = current
			return nil
		}
		if request.Status != hoststate.ActivationDispatchStarted || current.Status != hoststate.ActivationDispatchStarting ||
			request.ExpectedGeneration != current.Generation || request.Attempt != current.Attempt || request.At.Before(current.ObservedAt) {
			return workflowCAS("activation dispatch", request.ExpectedGeneration, current.Generation)
		}
		next := current
		next.Status, next.ReasonCode, next.ObservedAt, next.Generation = request.Status, request.ReasonCode, request.At.UTC(), current.Generation+1
		if validationErr := next.Validate(); validationErr != nil {
			return activationInvalid("completed activation dispatch is invalid")
		}
		encoded, _ := encodeActivationJSON(next)
		updated, err := query.ExecContext(ctx, `UPDATE workflow_activation_dispatches SET status = ?, observed_at = ?, reason_code = ?, generation = ?, dispatch_json = ?
WHERE fire_id = ? AND generation = ? AND status = ?`, next.Status, workflowTime(next.ObservedAt), optionalString(next.ReasonCode), next.Generation, encoded, next.FireID, current.Generation, current.Status)
		if err != nil {
			return err
		}
		count, _ := updated.RowsAffected()
		if count != 1 {
			return workflowCAS("activation dispatch", request.ExpectedGeneration, current.Generation+1)
		}
		result = next
		return appendWorkflowActivationEvent(ctx, query, next.RegistrationID, next.FireID, next.Attempt, "dispatch_started", next.ReasonCode, next.ObservedAt)
	})
	return result, err
}

func loadWorkflowActivationDispatch(ctx context.Context, query workflowSQL, fireID string) (hoststate.ActivationDispatch, error) {
	var encoded []byte
	err := query.QueryRowContext(ctx, `SELECT dispatch_json FROM workflow_activation_dispatches WHERE fire_id = ?`, fireID).Scan(&encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return hoststate.ActivationDispatch{}, fmt.Errorf("%w: activation dispatch %q", workflowruntime.ErrNotFound, fireID)
	}
	if err != nil {
		return hoststate.ActivationDispatch{}, err
	}
	var result hoststate.ActivationDispatch
	if err := decodeActivationJSON(encoded, &result); err != nil || result.Validate() != nil || result.FireID != fireID {
		return result, activationInvalid("stored activation dispatch is corrupt")
	}
	return result, nil
}

func (s *WorkflowActivationStore) LoadActivationDispatch(ctx context.Context, fireID string) (hoststate.ActivationDispatch, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return hoststate.ActivationDispatch{}, err
	}
	return loadWorkflowActivationDispatch(ctx, s.db, fireID)
}

func workflowActivationActiveRuns(ctx context.Context, query workflowSQL, registrationID, logicalID, excludingFire string) ([]workflowruntime.RunID, error) {
	statement := `SELECT d.physical_run_id FROM workflow_activation_dispatches d
LEFT JOIN workflow_runs r ON r.run_id = d.physical_run_id
WHERE d.registration_id = ? AND d.fire_id <> ? AND d.status IN (?, ?) AND d.physical_run_id IS NOT NULL
AND (r.run_id IS NULL OR r.status IN (?, ?, ?))`
	args := []any{registrationID, excludingFire, hoststate.ActivationDispatchStarting, hoststate.ActivationDispatchStarted,
		workflowruntime.RunPending, workflowruntime.RunRunning, workflowruntime.RunWaiting}
	if logicalID != "" {
		statement += ` AND d.logical_run_id = ?`
		args = append(args, logicalID)
	}
	statement += ` ORDER BY d.physical_run_id`
	rows, err := query.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	var result []workflowruntime.RunID
	for rows.Next() {
		var id workflowruntime.RunID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	return result, rows.Err()
}

func workflowActivationScopeRuns(ctx context.Context, query workflowSQL, scope hoststate.RunScope, logicalID, excludingFire string) ([]workflowruntime.RunID, error) {
	scopeKey, _ := workflowActivationScopeKey(scope)
	rows, err := query.QueryContext(ctx, `SELECT d.physical_run_id FROM workflow_activation_dispatches d
JOIN workflow_activation_registrations a ON a.registration_id = d.registration_id
WHERE a.scope_key = ? AND d.logical_run_id = ? AND d.fire_id <> ? AND d.status IN (?, ?)
ORDER BY d.physical_run_id`, scopeKey, logicalID, excludingFire, hoststate.ActivationDispatchStarting, hoststate.ActivationDispatchStarted)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	var result []workflowruntime.RunID
	for rows.Next() {
		var id workflowruntime.RunID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	return result, rows.Err()
}

func workflowActivationReplacementRuns(ctx context.Context, query workflowSQL, registration hoststate.ActivationRegistration, fireID, logicalID string) ([]workflowruntime.RunID, error) {
	var result []workflowruntime.RunID
	if registration.Policy.Overlap == graph.OverlapReplace {
		active, err := workflowActivationActiveRuns(ctx, query, registration.ID, "", fireID)
		if err != nil {
			return nil, err
		}
		result = append(result, active...)
	}
	if registration.Policy.RunIDReuse == graph.RunIDReuseTerminateExisting {
		logical, err := workflowActivationActiveScopeRuns(ctx, query, registration.RunScope, logicalID, fireID)
		if err != nil {
			return nil, err
		}
		result = append(result, logical...)
	}
	return sortedUniqueRunIDs(result), nil
}

func workflowActivationActiveScopeRuns(ctx context.Context, query workflowSQL, scope hoststate.RunScope, logicalID, excludingFire string) ([]workflowruntime.RunID, error) {
	all, err := workflowActivationScopeRuns(ctx, query, scope, logicalID, excludingFire)
	if err != nil {
		return nil, err
	}
	result := make([]workflowruntime.RunID, 0, len(all))
	for _, runID := range all {
		var status workflowruntime.RunStatus
		if err := query.QueryRowContext(ctx, `SELECT status FROM workflow_runs WHERE run_id = ?`, runID).Scan(&status); errors.Is(err, sql.ErrNoRows) {
			// A prepared dispatch whose Host StartRun has not committed is active.
			result = append(result, runID)
			continue
		} else if err != nil {
			return nil, err
		}
		if status.Active() {
			result = append(result, runID)
		}
	}
	return result, nil
}

func (s *WorkflowActivationStore) CreateCallback(ctx context.Context, input hoststate.CallbackRegistration) (hoststate.CallbackRegistration, workflowruntime.IdempotencyOutcome, error) {
	registration, err := input.Clone()
	if err != nil || registration.Validate() != nil {
		return hoststate.CallbackRegistration{}, "", activationInvalid("callback registration is invalid")
	}
	encoded, _ := encodeActivationJSON(registration)
	outcome := workflowruntime.IdempotencyApplied
	err = s.state.write(ctx, "create workflow callback", func(query workflowSQL) error {
		prior, loadErr := loadWorkflowCallback(ctx, query, registration.ID)
		if loadErr == nil {
			priorJSON, _ := encodeActivationJSON(prior)
			if !bytes.Equal(priorJSON, encoded) {
				return &workflowruntime.IdempotencyConflictError{Operation: "create workflow callback", Key: registration.ID}
			}
			registration, outcome = prior, workflowruntime.IdempotencyReplayed
			return nil
		}
		if !errors.Is(loadErr, workflowruntime.ErrNotFound) {
			return loadErr
		}
		if _, insertErr := query.ExecContext(ctx, `INSERT INTO workflow_callback_registrations(callback_id, wait_id, correlation,
credential_digest, expires_at, consumed_at, generation, registration_json) VALUES (?, ?, ?, ?, ?, NULL, ?, ?)`,
			registration.ID, registration.WaitID, registration.Correlation, registration.CredentialDigest,
			workflowTime(registration.ExpiresAt), registration.Generation, encoded); insertErr != nil {
			return insertErr
		}
		return appendWorkflowActivationEvent(ctx, query, "", "", 0, "callback_created", "", registration.CreatedAt)
	})
	return registration, outcome, err
}

func (s *WorkflowActivationStore) LoadCallback(ctx context.Context, id string) (hoststate.CallbackRegistration, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return hoststate.CallbackRegistration{}, err
	}
	return loadWorkflowCallback(ctx, s.db, id)
}

func loadWorkflowCallback(ctx context.Context, query workflowSQL, id string) (hoststate.CallbackRegistration, error) {
	var encoded []byte
	var waitID, correlation, credential, expires string
	var consumed sql.NullString
	var generation int64
	err := query.QueryRowContext(ctx, `SELECT wait_id, correlation, credential_digest, expires_at, consumed_at,
generation, registration_json FROM workflow_callback_registrations WHERE callback_id = ?`, id).
		Scan(&waitID, &correlation, &credential, &expires, &consumed, &generation, &encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return hoststate.CallbackRegistration{}, fmt.Errorf("%w: workflow callback %q", workflowruntime.ErrNotFound, id)
	}
	if err != nil {
		return hoststate.CallbackRegistration{}, err
	}
	var registration hoststate.CallbackRegistration
	if decodeErr := decodeActivationJSON(encoded, &registration); decodeErr != nil {
		return registration, activationInvalid("stored callback JSON is corrupt")
	}
	if generation <= 0 {
		return registration, activationInvalid("stored callback generation is invalid")
	}
	parsedExpiry, err := parseWorkflowTime("callback expires_at", expires)
	if err != nil {
		return registration, err
	}
	parsedConsumed, err := parseOptionalWorkflowTime("callback consumed_at", consumed)
	if err != nil {
		return registration, err
	}
	registration.ConsumedAt = parsedConsumed
	registration.Generation = uint64(generation)
	if string(registration.WaitID) != waitID || registration.Correlation != correlation || registration.CredentialDigest != credential ||
		!registration.ExpiresAt.Equal(parsedExpiry) || registration.ID != id || registration.Validate() != nil {
		return registration, activationInvalid("stored callback columns diverge from snapshot")
	}
	return registration.Clone()
}

func (s *WorkflowActivationStore) BeginCallback(ctx context.Context, request hoststate.CallbackBeginRequest) (hoststate.CallbackDelivery, error) {
	var delivery hoststate.CallbackDelivery
	err := s.state.write(ctx, "begin workflow callback", func(query workflowSQL) error {
		registration, err := loadWorkflowCallback(ctx, query, request.CallbackID)
		if err != nil {
			return err
		}
		if request.IdempotencyKey == "" || request.ReceivedAt.IsZero() {
			return activationInvalid("callback delivery identity is invalid")
		}
		if registration.CredentialDigest != request.CredentialDigest {
			return hoststate.ErrCallbackCredential
		}
		if !request.ReceivedAt.Before(registration.ExpiresAt) {
			return hoststate.ErrCallbackExpired
		}
		if values.ValidateDigest(request.PayloadDigest) != nil {
			return activationInvalid("callback payload digest is invalid")
		}
		requestDigest := callbackRequestDigest(request.CallbackID, request.IdempotencyKey, request.CredentialDigest, request.PayloadDigest)
		var received string
		var completed sql.NullString
		var outcome, priorDigest string
		loadErr := query.QueryRowContext(ctx, `SELECT received_at, completed_at, outcome, request_digest
FROM workflow_callback_deliveries WHERE callback_id = ? AND idempotency_key = ?`, request.CallbackID, request.IdempotencyKey).
			Scan(&received, &completed, &outcome, &priorDigest)
		if loadErr == nil {
			if priorDigest != requestDigest {
				return &workflowruntime.IdempotencyConflictError{Operation: "resume workflow callback", Key: request.IdempotencyKey}
			}
			receivedAt, err := parseWorkflowTime("callback received_at", received)
			if err != nil {
				return err
			}
			completedAt, err := parseOptionalWorkflowTime("callback completed_at", completed)
			if err != nil {
				return err
			}
			delivery = hoststate.CallbackDelivery{Registration: registration, IdempotencyKey: request.IdempotencyKey,
				ReceivedAt: receivedAt, CompletedAt: completedAt, Outcome: workflowruntime.IdempotencyReplayed}
			return nil
		}
		if !errors.Is(loadErr, sql.ErrNoRows) {
			return loadErr
		}
		if !registration.ConsumedAt.IsZero() {
			return &workflowruntime.IdempotencyConflictError{Operation: "resume consumed workflow callback", Key: request.IdempotencyKey}
		}
		if _, err := query.ExecContext(ctx, `INSERT INTO workflow_callback_deliveries(callback_id, idempotency_key,
received_at, completed_at, outcome, request_digest) VALUES (?, ?, ?, NULL, ?, ?)`, request.CallbackID,
			request.IdempotencyKey, workflowTime(request.ReceivedAt), workflowruntime.IdempotencyApplied, requestDigest); err != nil {
			return err
		}
		delivery = hoststate.CallbackDelivery{Registration: registration, IdempotencyKey: request.IdempotencyKey,
			ReceivedAt: request.ReceivedAt.UTC(), Outcome: workflowruntime.IdempotencyApplied}
		return nil
	})
	return delivery, err
}

func (s *WorkflowActivationStore) CompleteCallback(ctx context.Context, callbackID, key string, at time.Time) (hoststate.CallbackDelivery, error) {
	var delivery hoststate.CallbackDelivery
	err := s.state.write(ctx, "complete workflow callback", func(query workflowSQL) error {
		registration, err := loadWorkflowCallback(ctx, query, callbackID)
		if err != nil {
			return err
		}
		var received string
		var completed sql.NullString
		var outcome, requestDigest string
		deliveryErr := query.QueryRowContext(ctx, `SELECT received_at, completed_at, outcome, request_digest FROM workflow_callback_deliveries
WHERE callback_id = ? AND idempotency_key = ?`, callbackID, key).Scan(&received, &completed, &outcome, &requestDigest)
		if errors.Is(deliveryErr, sql.ErrNoRows) {
			return fmt.Errorf("%w: workflow callback delivery", workflowruntime.ErrNotFound)
		} else if deliveryErr != nil {
			return deliveryErr
		}
		receivedAt, err := parseWorkflowTime("callback received_at", received)
		if err != nil {
			return err
		}
		completedAt, err := parseOptionalWorkflowTime("callback completed_at", completed)
		if err != nil {
			return err
		}
		if !completedAt.IsZero() {
			delivery = hoststate.CallbackDelivery{Registration: registration, IdempotencyKey: key, ReceivedAt: receivedAt,
				CompletedAt: completedAt, Outcome: workflowruntime.IdempotencyReplayed}
			return nil
		}
		if at.Before(receivedAt) {
			return activationInvalid("callback completion time regresses")
		}
		next := registration
		next.ConsumedAt, next.Generation = at.UTC(), registration.Generation+1
		encoded, _ := encodeActivationJSON(next)
		updated, err := query.ExecContext(ctx, `UPDATE workflow_callback_registrations SET consumed_at = ?, generation = ?, registration_json = ?
WHERE callback_id = ? AND generation = ? AND consumed_at IS NULL`, workflowTime(next.ConsumedAt), next.Generation, encoded, callbackID, registration.Generation)
		if err != nil {
			return err
		}
		count, _ := updated.RowsAffected()
		if count != 1 {
			return workflowCAS("workflow callback", registration.Generation, registration.Generation+1)
		}
		if _, err := query.ExecContext(ctx, `UPDATE workflow_callback_deliveries SET completed_at = ?, outcome = ?
WHERE callback_id = ? AND idempotency_key = ? AND completed_at IS NULL`, workflowTime(at.UTC()), workflowruntime.IdempotencyApplied, callbackID, key); err != nil {
			return err
		}
		delivery = hoststate.CallbackDelivery{Registration: next, IdempotencyKey: key, ReceivedAt: receivedAt,
			CompletedAt: at.UTC(), Outcome: workflowruntime.IdempotencyApplied}
		return appendWorkflowActivationEvent(ctx, query, "", "", 0, "callback_consumed", "", at.UTC())
	})
	return delivery, err
}

func callbackRequestDigest(callbackID, key, credentialDigest, payloadDigest string) string {
	return values.SHA256Digest([]byte(callbackID + "\x00" + key + "\x00" + credentialDigest + "\x00" + payloadDigest))
}

func (s *WorkflowActivationStore) RecordActivationObserver(ctx context.Context, event gosched.ObserverEvent) error {
	if event.At.IsZero() {
		return activationInvalid("scheduler observer event time is required")
	}
	switch event.Kind {
	case gosched.ObserverClaim, gosched.ObserverFire, gosched.ObserverRetry, gosched.ObserverSkip,
		gosched.ObserverSuccess, gosched.ObserverExhaustion, gosched.ObserverDisable, gosched.ObserverEngineError:
	default:
		return activationInvalid("scheduler observer event kind is unsupported")
	}
	reason := safeObserverReason(event)
	return s.state.write(ctx, "record workflow activation observer", func(query workflowSQL) error {
		return appendWorkflowActivationEvent(ctx, query, event.Fire.ScheduleID, event.Fire.ID, event.Fire.Attempt, "observer_"+string(event.Kind), reason, event.At.UTC())
	})
}

func appendWorkflowActivationEvent(ctx context.Context, query workflowSQL, registrationID, fireID string, attempt int, kind, reason string, at time.Time) error {
	if len(kind) > 128 || len(reason) > 128 {
		return activationInvalid("activation event fields exceed bounds")
	}
	payload, _ := json.Marshal(struct {
		Kind       string    `json:"kind"`
		ReasonCode string    `json:"reason_code,omitempty"`
		OccurredAt time.Time `json:"occurred_at"`
	}{Kind: kind, ReasonCode: reason, OccurredAt: at.UTC()})
	_, err := query.ExecContext(ctx, `INSERT INTO workflow_activation_events(registration_id, fire_id, attempt, kind, reason_code, occurred_at, event_json) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		optionalString(registrationID), optionalString(fireID), nullableAttempt(attempt), kind, optionalString(reason), workflowTime(at.UTC()), payload)
	return err
}

func toSchedulerRetry(policy hoststate.ActivationRetryPolicy) gosched.RetryPolicy {
	return gosched.RetryPolicy{MaxAttempts: policy.MaxAttempts, Backoff: gosched.BackoffPolicy{
		Strategy: gosched.BackoffStrategy(policy.Strategy), InitialDelay: policy.Initial, MaxDelay: policy.Maximum,
	}}
}

func safeActivationReason(transition gosched.FireTransition) string {
	if transition.Reason != "" && hoststate.ValidatePublicText(transition.Reason, 128, false) == nil {
		return transition.Reason
	}
	if transition.Error != "" {
		return "dispatch_failed"
	}
	return ""
}

func safeObserverReason(event gosched.ObserverEvent) string {
	if event.Reason != "" && hoststate.ValidatePublicText(event.Reason, 128, false) == nil {
		return event.Reason
	}
	if event.Err != nil {
		return "operation_failed"
	}
	return ""
}

func externalActivationFireID(registrationID, key string) string {
	sum := sha256.Sum256([]byte(registrationID + "\x00" + key))
	return "fire-external-" + hex.EncodeToString(sum[:])
}

func physicalWorkflowActivationRunID(logical, fireID string) workflowruntime.RunID {
	sum := sha256.Sum256([]byte(logical + "\x00" + fireID))
	prefix := logical
	if len(prefix) > 64 {
		prefix = prefix[:64]
	}
	return workflowruntime.RunID(prefix + "-" + hex.EncodeToString(sum[:12]))
}

func workflowActivationStartKey(registrationID, fireID string) string {
	sum := sha256.Sum256([]byte(registrationID + "\x00" + fireID))
	return "activation-start-" + hex.EncodeToString(sum[:])
}

func workflowActivationScopeKey(scope hoststate.RunScope) (string, error) {
	encoded, err := encodeActivationJSON(scope)
	if err != nil {
		return "", err
	}
	return values.SHA256Digest(encoded), nil
}

func sortedUniqueRunIDs(input []workflowruntime.RunID) []workflowruntime.RunID {
	sort.Slice(input, func(i, j int) bool { return input[i] < input[j] })
	result := input[:0]
	for _, id := range input {
		if len(result) == 0 || result[len(result)-1] != id {
			result = append(result, id)
		}
	}
	return result
}

func encodeActivationJSON(input any) ([]byte, error) { return json.Marshal(input) }

func decodeActivationJSON(encoded []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("activation JSON contains trailing content")
	}
	return nil
}

func activationInvalid(message string) error {
	return fmt.Errorf("%w: %w", workflowruntime.ErrInvalidRecord, errors.New(message))
}

func boolInteger(value bool) int {
	if value {
		return 1
	}
	return 0
}

func optionalString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableAttempt(attempt int) any {
	if attempt == 0 {
		return nil
	}
	return attempt
}
