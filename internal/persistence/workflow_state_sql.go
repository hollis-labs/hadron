package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

const workflowValueIDPrefix = "values-"

// WorkflowStateStore is Hadron's SQLite-backed graph-native runtime adapter.
// Its tables and operations remain separate from legacy blueprint and pipeline
// persistence APIs on Store.
type WorkflowStateStore struct {
	db *sql.DB
}

var _ workflowruntime.StateStore = (*WorkflowStateStore)(nil)

// NewWorkflowStateStore wraps an open Hadron persistence store. The returned
// adapter shares the store's database lifetime and must not outlive it.
func NewWorkflowStateStore(store *Store) (*WorkflowStateStore, error) {
	if store == nil || store.db == nil {
		return nil, fmt.Errorf("workflow state store requires an open persistence store")
	}
	return &WorkflowStateStore{db: store.db}, nil
}

type workflowSQL interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type workflowScanner interface {
	Scan(...any) error
}

func (s *WorkflowStateStore) write(ctx context.Context, operation string, fn func(workflowSQL) error) error {
	if err := checkWorkflowContext(ctx); err != nil {
		return err
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("%s: acquire sqlite connection: %w", operation, err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("%s: begin sqlite transaction: %w", operation, err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.ExecContext(rollbackCtx, "ROLLBACK")
	}()

	if err := fn(conn); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("%s: commit sqlite transaction: %w", operation, err)
	}
	committed = true
	return nil
}

func checkWorkflowContext(ctx context.Context) error {
	if ctx == nil {
		return workflowInvalid(errors.New("context is required"))
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func workflowInvalid(err error) error {
	return fmt.Errorf("%w: %w", workflowruntime.ErrInvalidRecord, err)
}

func workflowCAS(resource string, expected, actual uint64) error {
	return &workflowruntime.CASMismatchError{Resource: resource, Expected: expected, Actual: actual}
}

func workflowIdempotencyConflict(operation, key string) error {
	return &workflowruntime.IdempotencyConflictError{Operation: operation, Key: key}
}

func workflowTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func workflowOptionalTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return workflowTime(value)
}

func parseWorkflowTime(field, value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, workflowInvalid(fmt.Errorf("decode %s: %w", field, err))
	}
	return parsed, nil
}

func parseOptionalWorkflowTime(field string, value sql.NullString) (time.Time, error) {
	if !value.Valid {
		return time.Time{}, nil
	}
	return parseWorkflowTime(field, value.String)
}

func workflowGeneration(field string, value int64) (uint64, error) {
	if value < 0 {
		return 0, workflowInvalid(fmt.Errorf("%s must not be negative", field))
	}
	return uint64(value), nil
}

func sqliteGeneration(field string, value uint64) (int64, error) {
	if value > math.MaxInt64 {
		return 0, workflowInvalid(fmt.Errorf("%s exceeds SQLite integer range", field))
	}
	return int64(value), nil
}

func encodeWorkflowJSON(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", workflowInvalid(fmt.Errorf("encode JSON: %w", err))
	}
	return string(encoded), nil
}

func encodeOptionalWorkflowJSON[T any](value *T) (any, error) {
	if value == nil {
		return nil, nil
	}
	return encodeWorkflowJSON(value)
}

func decodeWorkflowJSON(field, encoded string, target any) error {
	if err := json.Unmarshal([]byte(encoded), target); err != nil {
		return workflowInvalid(fmt.Errorf("decode %s: %w", field, err))
	}
	return nil
}

func decodeOptionalWorkflowJSON[T any](field string, encoded sql.NullString) (*T, error) {
	if !encoded.Valid {
		return nil, nil
	}
	var result T
	if err := decodeWorkflowJSON(field, encoded.String, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func canonicalCreateRunRequest(request workflowruntime.CreateRunRequest) (string, error) {
	request.CreatedAt = request.CreatedAt.UTC()
	return encodeWorkflowJSON(request)
}

func canonicalClaimRequest(request workflowruntime.ClaimNodeRequest) (string, error) {
	request.Now = request.Now.UTC()
	request.LeaseUntil = request.LeaseUntil.UTC()
	return encodeWorkflowJSON(request)
}

func canonicalActivationRequest(request workflowruntime.ExternalActivationRequest) (string, error) {
	request.OccurredAt = request.OccurredAt.UTC()
	return encodeWorkflowJSON(request)
}

func canonicalClaimResult(result workflowruntime.ClaimResult) (string, error) {
	if result.Lease != nil {
		lease := *result.Lease
		lease.ExpiresAt = lease.ExpiresAt.UTC()
		result.Lease = &lease
	}
	return encodeWorkflowJSON(result)
}

func workflowValueID(sequence int64) string {
	return fmt.Sprintf("%s%012d", workflowValueIDPrefix, sequence)
}

func parseWorkflowValueID(id string) (int64, error) {
	if !strings.HasPrefix(id, workflowValueIDPrefix) {
		return 0, workflowInvalid(fmt.Errorf("unsupported value-set reference id %q", id))
	}
	sequence, err := strconv.ParseInt(strings.TrimPrefix(id, workflowValueIDPrefix), 10, 64)
	if err != nil || sequence < 1 {
		return 0, workflowInvalid(fmt.Errorf("unsupported value-set reference id %q", id))
	}
	return sequence, nil
}

func equalWorkflowValueRef(left, right *values.ValueSetRef) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func equalWorkflowLease(left, right *workflowruntime.ClaimLease) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Owner == right.Owner && left.Token == right.Token &&
		left.Generation == right.Generation && left.ExpiresAt.Equal(right.ExpiresAt)
}

func equalWorkflowBlocked(left, right *workflowruntime.BlockedReason) bool {
	if left == nil || right == nil {
		return left == right
	}
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}

func isSQLiteConstraint(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "constraint failed") || strings.Contains(message, "unique constraint")
}

func expectOneWorkflowRow(result sql.Result, resource string, expected, actual uint64) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect %s CAS update: %w", resource, err)
	}
	if affected != 1 {
		return workflowCAS(resource, expected, actual)
	}
	return nil
}
