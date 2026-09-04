package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
)

const maximumExposureListRecords = 1000

// WorkflowExposureStore persists Hadron-local MCP principals and exposure
// profiles without becoming part of the extraction-ready runtime store.
type WorkflowExposureStore struct {
	state *WorkflowStateStore
	now   func() time.Time
}

func NewWorkflowExposureStore(store *Store) (*WorkflowExposureStore, error) {
	state, err := NewWorkflowStateStore(store)
	if err != nil {
		return nil, err
	}
	return &WorkflowExposureStore{state: state, now: func() time.Time { return time.Now().UTC() }}, nil
}

func (s *WorkflowExposureStore) PutExposureProfile(ctx context.Context, record hoststate.ExposureProfileRecord, expectedGeneration uint64) (hoststate.ExposureProfileSnapshot, error) {
	record = record.Clone()
	if err := record.Validate(); err != nil {
		return hoststate.ExposureProfileSnapshot{}, fmt.Errorf("%w: exposure profile: %w", hoststate.ErrInvalidRecord, err)
	}
	encoded, err := encodeWorkflowJSON(record)
	if err != nil {
		return hoststate.ExposureProfileSnapshot{}, err
	}
	var result hoststate.ExposureProfileSnapshot
	err = s.state.write(ctx, "put workflow exposure profile", func(query workflowSQL) error {
		prior, loadErr := loadExposureProfile(ctx, query, record.ID)
		now := s.now().UTC()
		if errors.Is(loadErr, workflowruntime.ErrNotFound) {
			if expectedGeneration != 0 {
				return exposureCAS("exposure profile", expectedGeneration, 0)
			}
			_, execErr := query.ExecContext(ctx, `INSERT INTO workflow_exposure_profiles(profile_id, generation, record_json, created_at, updated_at) VALUES (?, 1, ?, ?, ?)`, record.ID, encoded, workflowTime(now), workflowTime(now))
			if execErr != nil {
				if isSQLiteConstraint(execErr) {
					return exposureCAS("exposure profile", expectedGeneration, 1)
				}
				return fmt.Errorf("insert workflow exposure profile: %w", execErr)
			}
			result = hoststate.ExposureProfileSnapshot{Record: record, Generation: 1, CreatedAt: now, UpdatedAt: now}
			return nil
		}
		if loadErr != nil {
			return loadErr
		}
		if expectedGeneration != prior.Generation {
			return exposureCAS("exposure profile", expectedGeneration, prior.Generation)
		}
		generation := prior.Generation + 1
		if now.Before(prior.UpdatedAt) {
			now = prior.UpdatedAt
		}
		update, execErr := query.ExecContext(ctx, `UPDATE workflow_exposure_profiles SET generation = ?, record_json = ?, updated_at = ? WHERE profile_id = ? AND generation = ?`, generation, encoded, workflowTime(now), record.ID, prior.Generation)
		if execErr != nil {
			return fmt.Errorf("update workflow exposure profile: %w", execErr)
		}
		if execErr = expectExposureRow(update, "exposure profile", prior.Generation); execErr != nil {
			return execErr
		}
		result = hoststate.ExposureProfileSnapshot{Record: record, Generation: generation, CreatedAt: prior.CreatedAt, UpdatedAt: now}
		return nil
	})
	return result.Clone(), err
}

func (s *WorkflowExposureStore) GetExposureProfile(ctx context.Context, id string) (hoststate.ExposureProfileSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return hoststate.ExposureProfileSnapshot{}, err
	}
	return loadExposureProfile(ctx, s.state.db, id)
}

func (s *WorkflowExposureStore) ListExposureProfiles(ctx context.Context, limit int) ([]hoststate.ExposureProfileSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	limit, err := exposureListLimit(limit)
	if err != nil {
		return nil, err
	}
	rows, err := s.state.db.QueryContext(ctx, `SELECT profile_id, generation, record_json, created_at, updated_at FROM workflow_exposure_profiles ORDER BY profile_id LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("list workflow exposure profiles: %w", err)
	}
	defer closeRows(rows)
	result := make([]hoststate.ExposureProfileSnapshot, 0)
	for rows.Next() {
		snapshot, scanErr := scanExposureProfile(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list workflow exposure profiles: %w", err)
	}
	return result, nil
}

func (s *WorkflowExposureStore) DeleteExposureProfile(ctx context.Context, id string, expectedGeneration uint64) error {
	return s.state.write(ctx, "delete workflow exposure profile", func(query workflowSQL) error {
		prior, err := loadExposureProfile(ctx, query, id)
		if err != nil {
			return err
		}
		if prior.Generation != expectedGeneration {
			return exposureCAS("exposure profile", expectedGeneration, prior.Generation)
		}
		var assignments int
		if assignmentErr := query.QueryRowContext(ctx, `SELECT COUNT(1) FROM workflow_mcp_principals WHERE profile_id = ?`, id).Scan(&assignments); assignmentErr != nil {
			return fmt.Errorf("inspect exposure profile assignments: %w", assignmentErr)
		}
		if assignments != 0 {
			return fmt.Errorf("%w: exposure profile is still assigned", hoststate.ErrConflict)
		}
		result, err := query.ExecContext(ctx, `DELETE FROM workflow_exposure_profiles WHERE profile_id = ? AND generation = ?`, id, expectedGeneration)
		if err != nil {
			if isSQLiteConstraint(err) {
				return fmt.Errorf("%w: exposure profile is still assigned", hoststate.ErrConflict)
			}
			return fmt.Errorf("delete workflow exposure profile: %w", err)
		}
		return expectExposureRow(result, "exposure profile", expectedGeneration)
	})
}

func (s *WorkflowExposureStore) PutMCPPrincipal(ctx context.Context, record hoststate.MCPPrincipalRecord, expectedGeneration uint64) (hoststate.MCPPrincipalSnapshot, error) {
	record = record.Clone()
	if err := record.Validate(); err != nil {
		return hoststate.MCPPrincipalSnapshot{}, fmt.Errorf("%w: MCP principal: %w", hoststate.ErrInvalidRecord, err)
	}
	public := record.Public()
	encoded, err := encodeWorkflowJSON(public)
	if err != nil {
		return hoststate.MCPPrincipalSnapshot{}, err
	}
	var result hoststate.MCPPrincipalSnapshot
	err = s.state.write(ctx, "put workflow MCP principal", func(query workflowSQL) error {
		if record.ProfileID != "" {
			if _, profileErr := loadExposureProfile(ctx, query, record.ProfileID); profileErr != nil {
				if errors.Is(profileErr, workflowruntime.ErrNotFound) {
					return fmt.Errorf("%w: MCP principal profile does not exist", hoststate.ErrInvalidRecord)
				}
				return profileErr
			}
		}
		prior, loadErr := loadMCPPrincipal(ctx, query, "principal_id = ?", record.ID)
		now := s.now().UTC()
		if errors.Is(loadErr, workflowruntime.ErrNotFound) {
			if expectedGeneration != 0 {
				return exposureCAS("MCP principal", expectedGeneration, 0)
			}
			_, execErr := query.ExecContext(ctx, `INSERT INTO workflow_mcp_principals(principal_id, credential_digest, profile_id, generation, record_json, created_at, updated_at) VALUES (?, ?, NULLIF(?, ''), 1, ?, ?, ?)`, record.ID, record.CredentialDigest, record.ProfileID, encoded, workflowTime(now), workflowTime(now))
			if execErr != nil {
				if isSQLiteConstraint(execErr) {
					return fmt.Errorf("%w: MCP principal identity, credential, or profile conflicts", hoststate.ErrConflict)
				}
				return fmt.Errorf("insert workflow MCP principal: %w", execErr)
			}
			result = hoststate.MCPPrincipalSnapshot{Record: record, Generation: 1, CreatedAt: now, UpdatedAt: now}
			return nil
		}
		if loadErr != nil {
			return loadErr
		}
		if expectedGeneration != prior.Generation {
			return exposureCAS("MCP principal", expectedGeneration, prior.Generation)
		}
		generation := prior.Generation + 1
		if now.Before(prior.UpdatedAt) {
			now = prior.UpdatedAt
		}
		update, execErr := query.ExecContext(ctx, `UPDATE workflow_mcp_principals SET credential_digest = ?, profile_id = NULLIF(?, ''), generation = ?, record_json = ?, updated_at = ? WHERE principal_id = ? AND generation = ?`, record.CredentialDigest, record.ProfileID, generation, encoded, workflowTime(now), record.ID, prior.Generation)
		if execErr != nil {
			if isSQLiteConstraint(execErr) {
				return fmt.Errorf("%w: MCP principal credential or profile conflicts", hoststate.ErrConflict)
			}
			return fmt.Errorf("update workflow MCP principal: %w", execErr)
		}
		if execErr = expectExposureRow(update, "MCP principal", prior.Generation); execErr != nil {
			return execErr
		}
		result = hoststate.MCPPrincipalSnapshot{Record: record, Generation: generation, CreatedAt: prior.CreatedAt, UpdatedAt: now}
		return nil
	})
	return result.Clone(), err
}

func (s *WorkflowExposureStore) GetMCPPrincipal(ctx context.Context, id string) (hoststate.MCPPrincipalSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return hoststate.MCPPrincipalSnapshot{}, err
	}
	return loadMCPPrincipal(ctx, s.state.db, "principal_id = ?", id)
}

func (s *WorkflowExposureStore) ResolveMCPPrincipalDigest(ctx context.Context, digest string) (hoststate.MCPPrincipalSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return hoststate.MCPPrincipalSnapshot{}, err
	}
	result, err := loadMCPPrincipal(ctx, s.state.db, "credential_digest = ?", digest)
	if err != nil {
		return hoststate.MCPPrincipalSnapshot{}, err
	}
	if !hoststate.MatchMCPTokenDigest(result.Record.CredentialDigest, digest) {
		return hoststate.MCPPrincipalSnapshot{}, fmt.Errorf("%w: MCP principal", workflowruntime.ErrNotFound)
	}
	return result, nil
}

func (s *WorkflowExposureStore) ListMCPPrincipals(ctx context.Context, limit int) ([]hoststate.MCPPrincipalSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	limit, err := exposureListLimit(limit)
	if err != nil {
		return nil, err
	}
	rows, err := s.state.db.QueryContext(ctx, `SELECT principal_id, credential_digest, profile_id, generation, record_json, created_at, updated_at FROM workflow_mcp_principals ORDER BY principal_id LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("list workflow MCP principals: %w", err)
	}
	defer closeRows(rows)
	result := make([]hoststate.MCPPrincipalSnapshot, 0)
	for rows.Next() {
		snapshot, scanErr := scanMCPPrincipal(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, snapshot.Public())
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list workflow MCP principals: %w", err)
	}
	return result, nil
}

func (s *WorkflowExposureStore) DeleteMCPPrincipal(ctx context.Context, id string, expectedGeneration uint64) error {
	return s.state.write(ctx, "delete workflow MCP principal", func(query workflowSQL) error {
		prior, err := loadMCPPrincipal(ctx, query, "principal_id = ?", id)
		if err != nil {
			return err
		}
		if prior.Generation != expectedGeneration {
			return exposureCAS("MCP principal", expectedGeneration, prior.Generation)
		}
		result, err := query.ExecContext(ctx, `DELETE FROM workflow_mcp_principals WHERE principal_id = ? AND generation = ?`, id, expectedGeneration)
		if err != nil {
			return fmt.Errorf("delete workflow MCP principal: %w", err)
		}
		return expectExposureRow(result, "MCP principal", expectedGeneration)
	})
}

func loadExposureProfile(ctx context.Context, query workflowSQL, id string) (hoststate.ExposureProfileSnapshot, error) {
	row := query.QueryRowContext(ctx, `SELECT profile_id, generation, record_json, created_at, updated_at FROM workflow_exposure_profiles WHERE profile_id = ?`, id)
	return scanExposureProfile(row)
}

func scanExposureProfile(row workflowScanner) (hoststate.ExposureProfileSnapshot, error) {
	var id, encoded, created, updated string
	var generation int64
	if err := row.Scan(&id, &generation, &encoded, &created, &updated); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return hoststate.ExposureProfileSnapshot{}, fmt.Errorf("%w: exposure profile", workflowruntime.ErrNotFound)
		}
		return hoststate.ExposureProfileSnapshot{}, fmt.Errorf("scan workflow exposure profile: %w", err)
	}
	var record hoststate.ExposureProfileRecord
	if err := decodeWorkflowJSON("workflow exposure profile", encoded, &record); err != nil {
		return hoststate.ExposureProfileSnapshot{}, err
	}
	createdAt, err := parseWorkflowTime("exposure profile created_at", created)
	if err != nil {
		return hoststate.ExposureProfileSnapshot{}, err
	}
	updatedAt, err := parseWorkflowTime("exposure profile updated_at", updated)
	if err != nil {
		return hoststate.ExposureProfileSnapshot{}, err
	}
	if generation <= 0 {
		return hoststate.ExposureProfileSnapshot{}, workflowInvalid(errors.New("stored exposure profile is corrupt"))
	}
	snapshot := hoststate.ExposureProfileSnapshot{Record: record, Generation: uint64(generation), CreatedAt: createdAt, UpdatedAt: updatedAt}
	if record.ID != id || snapshot.Validate() != nil {
		return hoststate.ExposureProfileSnapshot{}, workflowInvalid(errors.New("stored exposure profile is corrupt"))
	}
	return snapshot.Clone(), nil
}

func loadMCPPrincipal(ctx context.Context, query workflowSQL, predicate string, argument any) (hoststate.MCPPrincipalSnapshot, error) {
	row := query.QueryRowContext(ctx, `SELECT principal_id, credential_digest, COALESCE(profile_id, ''), generation, record_json, created_at, updated_at FROM workflow_mcp_principals WHERE `+predicate, argument)
	return scanMCPPrincipal(row)
}

func scanMCPPrincipal(row workflowScanner) (hoststate.MCPPrincipalSnapshot, error) {
	var id, digest, profileID, encoded, created, updated string
	var generation int64
	if err := row.Scan(&id, &digest, &profileID, &generation, &encoded, &created, &updated); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return hoststate.MCPPrincipalSnapshot{}, fmt.Errorf("%w: MCP principal", workflowruntime.ErrNotFound)
		}
		return hoststate.MCPPrincipalSnapshot{}, fmt.Errorf("scan workflow MCP principal: %w", err)
	}
	var record hoststate.MCPPrincipalRecord
	if err := decodeWorkflowJSON("workflow MCP principal", encoded, &record); err != nil {
		return hoststate.MCPPrincipalSnapshot{}, err
	}
	record.CredentialDigest = digest
	createdAt, err := parseWorkflowTime("MCP principal created_at", created)
	if err != nil {
		return hoststate.MCPPrincipalSnapshot{}, err
	}
	updatedAt, err := parseWorkflowTime("MCP principal updated_at", updated)
	if err != nil {
		return hoststate.MCPPrincipalSnapshot{}, err
	}
	if generation <= 0 {
		return hoststate.MCPPrincipalSnapshot{}, workflowInvalid(errors.New("stored MCP principal is corrupt"))
	}
	snapshot := hoststate.MCPPrincipalSnapshot{Record: record, Generation: uint64(generation), CreatedAt: createdAt, UpdatedAt: updatedAt}
	if record.ID != id || record.ProfileID != profileID || snapshot.Validate() != nil {
		return hoststate.MCPPrincipalSnapshot{}, workflowInvalid(errors.New("stored MCP principal is corrupt"))
	}
	return snapshot.Clone(), nil
}

func exposureListLimit(limit int) (int, error) {
	if limit == 0 {
		return 100, nil
	}
	if limit < 1 || limit > maximumExposureListRecords {
		return 0, workflowInvalid(fmt.Errorf("exposure list limit must be between 1 and %d", maximumExposureListRecords))
	}
	return limit, nil
}

func exposureCAS(resource string, expected, actual uint64) error {
	return fmt.Errorf("%w: %s generation expected %d actual %d", hoststate.ErrConflict, resource, expected, actual)
}

func expectExposureRow(result sql.Result, resource string, expected uint64) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect %s CAS update: %w", resource, err)
	}
	if affected != 1 {
		return exposureCAS(resource, expected, expected+1)
	}
	return nil
}
