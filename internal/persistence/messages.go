package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

func (s *Store) CreateMessage(ctx context.Context, rec MessageRecord) error {
	if rec.Channel == "" {
		rec.Channel = ""
	}
	if rec.ThreadID == "" {
		rec.ThreadID = ""
	}
	if rec.InReplyTo == "" {
		rec.InReplyTo = ""
	}
	if rec.CorrelationID == "" {
		rec.CorrelationID = ""
	}
	if rec.PayloadJSON == "" {
		rec.PayloadJSON = "null"
	}
	if rec.ContentType == "" {
		rec.ContentType = "application/json"
	}
	if rec.MetadataJSON == "" {
		rec.MetadataJSON = "{}"
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO messages(
			id, substrate, kind, channel, from_urn, to_urn, thread_id, in_reply_to,
			correlation_id, payload_json, content_type, metadata_json, created_at,
			delivered_at, consumed_at, canceled_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		rec.ID,
		rec.Substrate,
		rec.Kind,
		rec.Channel,
		rec.FromURN,
		rec.ToURN,
		rec.ThreadID,
		rec.InReplyTo,
		rec.CorrelationID,
		rec.PayloadJSON,
		rec.ContentType,
		rec.MetadataJSON,
		rec.CreatedAt.UTC().Format(time.RFC3339Nano),
		rec.DeliveredAt,
		rec.ConsumedAt,
		rec.CanceledAt,
	)
	if err != nil {
		return fmt.Errorf("create message: %w", err)
	}
	return nil
}

func (s *Store) GetMessage(ctx context.Context, id string) (MessageRecord, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, substrate, kind, channel, from_urn, to_urn, thread_id, in_reply_to,
		       correlation_id, payload_json, content_type, metadata_json, created_at,
		       delivered_at, consumed_at, canceled_at
		FROM messages
		WHERE id = ?
	`, id)
	return scanMessageRecord(row)
}

func (s *Store) ListMessagesByRecipient(ctx context.Context, substrate, toURN, correlationID string, limit int) ([]MessageRecord, error) {
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, substrate, kind, channel, from_urn, to_urn, thread_id, in_reply_to,
		       correlation_id, payload_json, content_type, metadata_json, created_at,
		       delivered_at, consumed_at, canceled_at
		FROM messages
		WHERE substrate = ? AND to_urn = ? AND canceled_at IS NULL
		  AND (? = '' OR thread_id = ? OR in_reply_to = ? OR correlation_id = ?)
		ORDER BY created_at ASC, id ASC
		LIMIT ?
	`, substrate, toURN, correlationID, correlationID, correlationID, correlationID, limit)
	if err != nil {
		return nil, fmt.Errorf("list messages by recipient: %w", err)
	}
	defer closeRows(rows)

	var out []MessageRecord
	for rows.Next() {
		rec, err := scanMessageRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list messages by recipient rows: %w", err)
	}
	return out, nil
}

func (s *Store) ListMessagesByRecipientNonDestructive(ctx context.Context, substrate, toURN, correlationID string, limit int) ([]MessageRecord, error) {
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, substrate, kind, channel, from_urn, to_urn, thread_id, in_reply_to,
		       correlation_id, payload_json, content_type, metadata_json, created_at,
		       delivered_at, consumed_at, canceled_at
		FROM messages
		WHERE substrate = ? AND to_urn = ? AND canceled_at IS NULL
		  AND (? = '' OR thread_id = ? OR in_reply_to = ? OR correlation_id = ?)
		ORDER BY created_at ASC, id ASC
		LIMIT ?
	`, substrate, toURN, correlationID, correlationID, correlationID, correlationID, limit)
	if err != nil {
		return nil, fmt.Errorf("list messages by recipient nondestructive: %w", err)
	}
	defer closeRows(rows)

	var out []MessageRecord
	for rows.Next() {
		rec, err := scanMessageRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list messages by recipient nondestructive rows: %w", err)
	}
	return out, nil
}

func (s *Store) ListMessagesByThread(ctx context.Context, substrate, threadID string, limit int) ([]MessageRecord, error) {
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, substrate, kind, channel, from_urn, to_urn, thread_id, in_reply_to,
		       correlation_id, payload_json, content_type, metadata_json, created_at,
		       delivered_at, consumed_at, canceled_at
		FROM messages
		WHERE substrate = ? AND canceled_at IS NULL
		  AND (thread_id = ? OR correlation_id = ?)
		ORDER BY created_at ASC, id ASC
		LIMIT ?
	`, substrate, threadID, threadID, limit)
	if err != nil {
		return nil, fmt.Errorf("list messages by thread: %w", err)
	}
	defer closeRows(rows)

	var out []MessageRecord
	for rows.Next() {
		rec, err := scanMessageRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list messages by thread rows: %w", err)
	}
	return out, nil
}

func (s *Store) ClaimNextMessage(ctx context.Context, substrate, toURN, correlationID string, claimedAt time.Time) (MessageRecord, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return MessageRecord{}, fmt.Errorf("claim next message begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	row := tx.QueryRowContext(ctx, `
		SELECT id, substrate, kind, channel, from_urn, to_urn, thread_id, in_reply_to,
		       correlation_id, payload_json, content_type, metadata_json, created_at,
		       delivered_at, consumed_at, canceled_at
		FROM messages
		WHERE substrate = ? AND to_urn = ? AND delivered_at IS NULL AND consumed_at IS NULL AND canceled_at IS NULL
		  AND (? = '' OR thread_id = ? OR in_reply_to = ? OR correlation_id = ?)
		ORDER BY created_at ASC, id ASC
		LIMIT 1
	`, substrate, toURN, correlationID, correlationID, correlationID, correlationID)
	rec, err := scanMessageRecord(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return MessageRecord{}, err
		}
		return MessageRecord{}, fmt.Errorf("claim next message select: %w", err)
	}

	stamp := claimedAt.UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `
		UPDATE messages
		SET delivered_at = ?, consumed_at = ?
		WHERE id = ? AND delivered_at IS NULL AND consumed_at IS NULL
	`, stamp, stamp, rec.ID); err != nil {
		return MessageRecord{}, fmt.Errorf("claim next message update: %w", err)
	}
	rec.DeliveredAt = sql.NullString{String: stamp, Valid: true}
	rec.ConsumedAt = sql.NullString{String: stamp, Valid: true}
	if err := tx.Commit(); err != nil {
		return MessageRecord{}, fmt.Errorf("claim next message commit: %w", err)
	}
	return rec, nil
}

func (s *Store) ConsumeMessage(ctx context.Context, id string, consumedAt time.Time) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE messages
		SET consumed_at = CASE WHEN consumed_at IS NULL THEN ? ELSE consumed_at END
		WHERE id = ? AND canceled_at IS NULL
	`, consumedAt.UTC().Format(time.RFC3339Nano), id)
	if err != nil {
		return fmt.Errorf("consume message: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("consume message rows: %w", err)
	}
	if affected == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func scanMessageRecord(scanner interface {
	Scan(dest ...any) error
}) (MessageRecord, error) {
	var rec MessageRecord
	var created string
	if err := scanner.Scan(
		&rec.ID,
		&rec.Substrate,
		&rec.Kind,
		&rec.Channel,
		&rec.FromURN,
		&rec.ToURN,
		&rec.ThreadID,
		&rec.InReplyTo,
		&rec.CorrelationID,
		&rec.PayloadJSON,
		&rec.ContentType,
		&rec.MetadataJSON,
		&created,
		&rec.DeliveredAt,
		&rec.ConsumedAt,
		&rec.CanceledAt,
	); err != nil {
		return MessageRecord{}, err
	}
	createdAt, err := time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return MessageRecord{}, fmt.Errorf("parse message created_at: %w", err)
	}
	rec.CreatedAt = createdAt
	return rec, nil
}
