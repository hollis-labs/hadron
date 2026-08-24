package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	gosched "github.com/hollis-labs/go-scheduler"
)

const legacyClaimLease = 2 * time.Minute

func (a storeAdapter) CreateFire(ctx context.Context, creation gosched.FireCreation) (bool, error) {
	created := false
	err := a.legacyWrite(ctx, func(query *sql.Conn) error {
		var next string
		var enabled int
		if err := query.QueryRowContext(ctx, `SELECT next_run_at, enabled FROM schedules WHERE id = ?`, creation.ScheduleID).Scan(&next, &enabled); err != nil {
			return err
		}
		parsed, parseErr := time.Parse(time.RFC3339Nano, next)
		if parseErr != nil {
			return parseErr
		}
		if enabled == 0 || !parsed.Equal(creation.ExpectedNext) {
			return nil
		}
		if creation.Fire.ID != gosched.DeriveFireID(creation.ScheduleID, creation.ExpectedNext) {
			return errors.New("legacy scheduler fire identity mismatch")
		}
		retryJSON, _ := json.Marshal(creation.Fire.Retry)
		inserted, err := query.ExecContext(ctx, `INSERT OR IGNORE INTO legacy_schedule_fires(fire_id, schedule_id, scheduled_at,
fired_at, attempt, status, next_attempt_at, claim_expires_at, retry_json, job_type, payload_json, last_error_code)
VALUES (?, ?, ?, NULL, 0, ?, ?, NULL, ?, ?, ?, NULL)`, creation.Fire.ID, creation.ScheduleID,
			legacyTime(creation.ExpectedNext), gosched.FirePending, legacyTime(creation.ExpectedNext), retryJSON,
			creation.Fire.JobType, creation.Fire.Payload)
		if err != nil {
			return err
		}
		insertedCount, _ := inserted.RowsAffected()
		if insertedCount != 1 {
			return nil
		}
		updated, err := query.ExecContext(ctx, `UPDATE schedules SET last_run_at = ?, next_run_at = ?, updated_at = ?
WHERE id = ? AND next_run_at = ? AND enabled = 1`, legacyTime(creation.ExpectedNext), legacyTime(creation.NextRun),
			legacyTime(creation.ExpectedNext), creation.ScheduleID, next)
		if err != nil {
			return err
		}
		count, _ := updated.RowsAffected()
		created = count == 1
		return nil
	})
	return created, err
}

func (a storeAdapter) ListDueFires(ctx context.Context, now time.Time, limit int) ([]gosched.Fire, error) {
	var result []gosched.Fire
	err := a.legacyWrite(ctx, func(query *sql.Conn) error {
		if _, err := query.ExecContext(ctx, `UPDATE legacy_schedule_fires SET status = ?, next_attempt_at = ?,
claim_expires_at = NULL, last_error_code = ? WHERE status = ? AND claim_expires_at <= ?`, gosched.FireRetrying,
			legacyTime(now), "claim_expired", gosched.FireClaimed, legacyTime(now)); err != nil {
			return err
		}
		rows, err := query.QueryContext(ctx, `SELECT fire_id FROM legacy_schedule_fires WHERE status IN (?, ?)
AND next_attempt_at <= ? ORDER BY next_attempt_at, fire_id LIMIT ?`, gosched.FirePending, gosched.FireRetrying, legacyTime(now), limit)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			fire, err := loadLegacyFire(ctx, query, id)
			if err != nil {
				return err
			}
			result = append(result, fire)
		}
		return rows.Err()
	})
	return result, err
}

func (a storeAdapter) ClaimFire(ctx context.Context, claim gosched.FireClaim) (gosched.Fire, bool, error) {
	var claimed gosched.Fire
	won := false
	err := a.legacyWrite(ctx, func(query *sql.Conn) error {
		fire, err := loadLegacyFire(ctx, query, claim.FireID)
		if err != nil {
			return err
		}
		if fire.Status != claim.ExpectedStatus || fire.Attempt != claim.ExpectedAttempt {
			return nil
		}
		claimed = fire
		claimed.Status, claimed.Attempt, claimed.FiredAt, claimed.NextAttemptAt = gosched.FireClaimed, fire.Attempt+1, claim.ClaimedAt.UTC(), time.Time{}
		updated, err := query.ExecContext(ctx, `UPDATE legacy_schedule_fires SET status = ?, attempt = ?, fired_at = ?,
next_attempt_at = NULL, claim_expires_at = ? WHERE fire_id = ? AND status = ? AND attempt = ?`, claimed.Status,
			claimed.Attempt, legacyTime(claimed.FiredAt), legacyTime(claimed.FiredAt.Add(legacyClaimLease)), fire.ID, fire.Status, fire.Attempt)
		if err != nil {
			return err
		}
		count, _ := updated.RowsAffected()
		if count != 1 {
			return nil
		}
		if _, err := query.ExecContext(ctx, `INSERT INTO legacy_schedule_fire_attempts(fire_id, attempt, claimed_at) VALUES (?, ?, ?)`, fire.ID, claimed.Attempt, legacyTime(claimed.FiredAt)); err != nil {
			return err
		}
		won = true
		return nil
	})
	return claimed, won, err
}

func (a storeAdapter) TransitionFire(ctx context.Context, transition gosched.FireTransition) (bool, error) {
	applied := false
	err := a.legacyWrite(ctx, func(query *sql.Conn) error {
		fire, err := loadLegacyFire(ctx, query, transition.FireID)
		if err != nil {
			return err
		}
		if fire.Status != transition.From || fire.Attempt != transition.Attempt {
			return nil
		}
		var claimExpires string
		claimErr := query.QueryRowContext(ctx, `SELECT claim_expires_at FROM legacy_schedule_fires
WHERE fire_id = ? AND status = ? AND attempt = ?`, fire.ID, gosched.FireClaimed, fire.Attempt).Scan(&claimExpires)
		if errors.Is(claimErr, sql.ErrNoRows) {
			return nil
		} else if claimErr != nil {
			return claimErr
		}
		expiresAt, err := time.Parse(time.RFC3339Nano, claimExpires)
		if err != nil {
			return err
		}
		if !transition.At.Before(expiresAt) {
			return nil
		}
		reason := transition.Reason
		if transition.Error != "" {
			reason = "dispatch_failed"
		}
		updated, err := query.ExecContext(ctx, `UPDATE legacy_schedule_fires SET status = ?, next_attempt_at = ?,
claim_expires_at = NULL, last_error_code = ? WHERE fire_id = ? AND status = ? AND attempt = ?`, transition.To,
			legacyOptionalTime(transition.NextAttemptAt), reason, fire.ID, fire.Status, fire.Attempt)
		if err != nil {
			return err
		}
		count, _ := updated.RowsAffected()
		if count != 1 {
			return nil
		}
		if _, err := query.ExecContext(ctx, `UPDATE legacy_schedule_fire_attempts SET completed_at = ?, outcome = ?
WHERE fire_id = ? AND attempt = ? AND completed_at IS NULL`, legacyTime(transition.At), transition.To, fire.ID, fire.Attempt); err != nil {
			return err
		}
		applied = true
		return nil
	})
	return applied, err
}

func loadLegacyFire(ctx context.Context, query *sql.Conn, id string) (gosched.Fire, error) {
	var fire gosched.Fire
	var scheduled string
	var fired, next sql.NullString
	var retryJSON []byte
	err := query.QueryRowContext(ctx, `SELECT fire_id, schedule_id, scheduled_at, fired_at, attempt, status,
next_attempt_at, retry_json, job_type, payload_json, COALESCE(last_error_code, '') FROM legacy_schedule_fires WHERE fire_id = ?`, id).
		Scan(&fire.ID, &fire.ScheduleID, &scheduled, &fired, &fire.Attempt, &fire.Status, &next, &retryJSON, &fire.JobType, &fire.Payload, &fire.LastError)
	if err != nil {
		return fire, err
	}
	fire.ScheduledAt, err = time.Parse(time.RFC3339Nano, scheduled)
	if err != nil {
		return fire, err
	}
	if fired.Valid {
		fire.FiredAt, err = time.Parse(time.RFC3339Nano, fired.String)
		if err != nil {
			return fire, err
		}
	}
	if next.Valid {
		fire.NextAttemptAt, err = time.Parse(time.RFC3339Nano, next.String)
		if err != nil {
			return fire, err
		}
	}
	if err := json.Unmarshal(retryJSON, &fire.Retry); err != nil {
		return fire, err
	}
	return fire, nil
}

func (a storeAdapter) legacyWrite(ctx context.Context, fn func(*sql.Conn) error) error {
	conn, err := a.Store.DB().Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()
	if err := fn(conn); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return err
	}
	committed = true
	return nil
}

func legacyTime(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }

func legacyOptionalTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return legacyTime(value)
}

var _ gosched.Store = storeAdapter{}
