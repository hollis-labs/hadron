package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

const maximumPlanSnapshotCleanup = 1000

type persistedPlanMaterial struct {
	SchemaVersion string                      `json:"schema_version"`
	Source        *hoststate.SourceSnapshot   `json:"source,omitempty"`
	Compile       hoststate.CompileDescriptor `json:"compile"`
}

func ensureWorkflowPlanSnapshot(ctx context.Context, query workflowSQL, snapshot hoststate.PlanSnapshot, createdAt string) error {
	if err := snapshot.Validate(); err != nil {
		return workflowInvalid(fmt.Errorf("plan snapshot: %w", err))
	}
	if err := ensureWorkflowPlan(ctx, query, snapshot.PlanRef()); err != nil {
		return err
	}
	planJSON, err := encodeWorkflowJSON(snapshot.Plan)
	if err != nil {
		return err
	}
	sourceMapJSON, err := encodeWorkflowJSON(snapshot.SourceMap)
	if err != nil {
		return err
	}
	materialJSON, err := encodeWorkflowJSON(persistedPlanMaterial{SchemaVersion: snapshot.SchemaVersion, Source: snapshot.Source, Compile: snapshot.Compile})
	if err != nil {
		return err
	}
	if _, err := query.ExecContext(ctx, `
INSERT INTO workflow_plan_snapshots(snapshot_digest, plan_digest, schema_version, plan_snapshot_json, source_map_json, source_snapshot_json, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(snapshot_digest) DO NOTHING`, snapshot.Digest, snapshot.Plan.Digest, snapshot.SchemaVersion, planJSON, sourceMapJSON, materialJSON, createdAt); err != nil {
		return fmt.Errorf("record workflow plan snapshot: %w", err)
	}
	var storedPlanDigest, storedSchema, storedPlan, storedMap, storedMaterial string
	if err := query.QueryRowContext(ctx, `SELECT plan_digest, schema_version, plan_snapshot_json, source_map_json, source_snapshot_json FROM workflow_plan_snapshots WHERE snapshot_digest = ?`, snapshot.Digest).Scan(&storedPlanDigest, &storedSchema, &storedPlan, &storedMap, &storedMaterial); err != nil {
		return fmt.Errorf("load workflow plan snapshot collision guard: %w", err)
	}
	if storedPlanDigest != snapshot.Plan.Digest || storedSchema != snapshot.SchemaVersion || storedPlan != planJSON || storedMap != sourceMapJSON || storedMaterial != materialJSON {
		return fmt.Errorf("%w: snapshot digest %q collides with different exact material", workflowruntime.ErrAlreadyExists, snapshot.Digest)
	}
	return nil
}

func linkWorkflowPlanSnapshot(ctx context.Context, query workflowSQL, runID workflowruntime.RunID, snapshot hoststate.PlanSnapshot) error {
	if _, err := query.ExecContext(ctx, `INSERT INTO workflow_host_start_plan_snapshots(run_id, snapshot_digest, plan_digest) VALUES (?, ?, ?)`, runID, snapshot.Digest, snapshot.Plan.Digest); err != nil {
		if isSQLiteConstraint(err) {
			return fmt.Errorf("%w: workflow host start %q has a conflicting plan snapshot link", workflowruntime.ErrAlreadyExists, runID)
		}
		return fmt.Errorf("link workflow host start plan snapshot: %w", err)
	}
	return nil
}

func loadWorkflowPlanSnapshotForRun(ctx context.Context, query workflowSQL, runID workflowruntime.RunID) (hoststate.PlanSnapshot, error) {
	var snapshotDigest, planDigest string
	if err := query.QueryRowContext(ctx, `SELECT snapshot_digest, plan_digest FROM workflow_host_start_plan_snapshots WHERE run_id = ?`, runID).Scan(&snapshotDigest, &planDigest); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return hoststate.PlanSnapshot{}, fmt.Errorf("%w: workflow plan snapshot link for run %q", workflowruntime.ErrNotFound, runID)
		}
		return hoststate.PlanSnapshot{}, err
	}
	snapshot, err := loadWorkflowPlanSnapshot(ctx, query, snapshotDigest)
	if err != nil {
		return hoststate.PlanSnapshot{}, err
	}
	if snapshot.Plan.Digest != planDigest {
		return hoststate.PlanSnapshot{}, fmt.Errorf("%w: workflow plan snapshot link has mismatched plan digest", workflowruntime.ErrInvalidRecord)
	}
	return snapshot, nil
}

func loadWorkflowPlanSnapshot(ctx context.Context, query workflowSQL, snapshotDigest string) (hoststate.PlanSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return hoststate.PlanSnapshot{}, err
	}
	var storedDigest, planDigest, schemaVersion, planJSON, sourceMapJSON, materialJSON string
	if err := query.QueryRowContext(ctx, `SELECT snapshot_digest, plan_digest, schema_version, plan_snapshot_json, source_map_json, source_snapshot_json FROM workflow_plan_snapshots WHERE snapshot_digest = ?`, snapshotDigest).Scan(&storedDigest, &planDigest, &schemaVersion, &planJSON, &sourceMapJSON, &materialJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return hoststate.PlanSnapshot{}, fmt.Errorf("%w: workflow plan snapshot %q", workflowruntime.ErrNotFound, snapshotDigest)
		}
		return hoststate.PlanSnapshot{}, fmt.Errorf("load workflow plan snapshot: %w", err)
	}
	var plan compile.ExecutionPlan
	if err := decodeWorkflowJSON("workflow plan snapshot", planJSON, &plan); err != nil {
		return hoststate.PlanSnapshot{}, err
	}
	var sourceMap graph.SourceMap
	if err := decodeWorkflowJSON("workflow plan source map", sourceMapJSON, &sourceMap); err != nil {
		return hoststate.PlanSnapshot{}, err
	}
	var material persistedPlanMaterial
	if err := decodeWorkflowJSON("workflow plan source snapshot", materialJSON, &material); err != nil {
		return hoststate.PlanSnapshot{}, err
	}
	snapshot := hoststate.PlanSnapshot{SchemaVersion: material.SchemaVersion, Digest: storedDigest, Plan: plan, SourceMap: sourceMap, Source: material.Source, Compile: material.Compile}
	if schemaVersion != snapshot.SchemaVersion || planDigest != plan.Digest {
		return hoststate.PlanSnapshot{}, fmt.Errorf("%w: workflow plan snapshot envelope identity differs from indexed identity", workflowruntime.ErrInvalidRecord)
	}
	if err := snapshot.Validate(); err != nil {
		return hoststate.PlanSnapshot{}, fmt.Errorf("%w: workflow plan snapshot %q: %w", workflowruntime.ErrInvalidRecord, snapshotDigest, err)
	}
	return snapshot.Clone()
}

// LoadPlanSnapshot returns a defensive copy by exact locator-sensitive
// snapshot digest, never by the relocation-stable execution plan digest.
func (s *WorkflowHostStore) LoadPlanSnapshot(ctx context.Context, snapshotDigest string) (hoststate.PlanSnapshot, error) {
	if s == nil || s.state == nil {
		return hoststate.PlanSnapshot{}, workflowInvalid(errors.New("workflow host store is required"))
	}
	return loadWorkflowPlanSnapshot(ctx, s.state.db, snapshotDigest)
}

// CollectUnreferencedPlanSnapshots transactionally removes at most limit
// run-retained exact snapshots. New starts are protected by exact snapshot
// refs. Legacy starts without a ref conservatively protect every variant of
// their plan digest. Project-retained source is never collected here.
func (s *WorkflowHostStore) CollectUnreferencedPlanSnapshots(ctx context.Context, limit int) ([]string, error) {
	if s == nil || s.state == nil {
		return nil, workflowInvalid(errors.New("workflow host store is required"))
	}
	if limit < 1 || limit > maximumPlanSnapshotCleanup {
		return nil, workflowInvalid(fmt.Errorf("plan snapshot cleanup limit must be between 1 and %d", maximumPlanSnapshotCleanup))
	}
	deleted := make([]string, 0, limit)
	err := s.state.write(ctx, "collect workflow plan snapshots", func(query workflowSQL) error {
		type candidate struct {
			snapshotDigest string
			planDigest     string
			material       string
		}
		rows, queryErr := query.QueryContext(ctx, `
SELECT s.snapshot_digest, s.plan_digest, s.source_snapshot_json
FROM workflow_plan_snapshots s
WHERE NOT EXISTS (
    SELECT 1 FROM workflow_host_start_plan_snapshots l
    WHERE l.snapshot_digest = s.snapshot_digest
)
AND COALESCE(json_extract(s.source_snapshot_json, '$.source.retention'), 'run') = 'run'
AND NOT EXISTS (
    SELECT 1
    FROM workflow_host_starts h
    LEFT JOIN workflow_host_start_plan_snapshots l ON l.run_id = h.run_id
    WHERE l.run_id IS NULL
      AND json_extract(h.request_json, '$.plan.digest') = s.plan_digest
)
ORDER BY s.snapshot_digest
LIMIT ?`, limit)
		if queryErr != nil {
			return queryErr
		}
		candidates := make([]candidate, 0)
		for rows.Next() {
			var item candidate
			if scanErr := rows.Scan(&item.snapshotDigest, &item.planDigest, &item.material); scanErr != nil {
				_ = rows.Close()
				return scanErr
			}
			candidates = append(candidates, item)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			_ = rows.Close()
			return rowsErr
		}
		if closeErr := rows.Close(); closeErr != nil {
			return closeErr
		}

		protectedRefs := make(map[string]struct{})
		linkRows, linkQueryErr := query.QueryContext(ctx, `SELECT run_id, snapshot_digest, plan_digest FROM workflow_host_start_plan_snapshots ORDER BY run_id`)
		if linkQueryErr != nil {
			return linkQueryErr
		}
		type link struct{ snapshotDigest, planDigest string }
		links := make(map[string]link)
		for linkRows.Next() {
			var runID string
			var item link
			if scanErr := linkRows.Scan(&runID, &item.snapshotDigest, &item.planDigest); scanErr != nil {
				_ = linkRows.Close()
				return scanErr
			}
			links[runID] = item
			protectedRefs[item.snapshotDigest] = struct{}{}
		}
		if rowsErr := linkRows.Err(); rowsErr != nil {
			_ = linkRows.Close()
			return rowsErr
		}
		if closeErr := linkRows.Close(); closeErr != nil {
			return closeErr
		}

		legacyPlanDigests := make(map[string]struct{})
		startRows, startQueryErr := query.QueryContext(ctx, `SELECT run_id, request_json FROM workflow_host_starts ORDER BY run_id`)
		if startQueryErr != nil {
			return startQueryErr
		}
		for startRows.Next() {
			var runID, encoded string
			if scanErr := startRows.Scan(&runID, &encoded); scanErr != nil {
				_ = startRows.Close()
				return scanErr
			}
			var record hoststate.StartRecord
			if decodeErr := decodeWorkflowJSON("workflow host start during snapshot cleanup", encoded, &record); decodeErr != nil {
				_ = startRows.Close()
				return decodeErr
			}
			if validationErr := record.Validate(); validationErr != nil {
				_ = startRows.Close()
				return workflowInvalid(fmt.Errorf("workflow host start during snapshot cleanup: %w", validationErr))
			}
			if linked, ok := links[runID]; ok {
				if linked.planDigest != record.Plan.Digest {
					_ = startRows.Close()
					return workflowInvalid(fmt.Errorf("workflow host start %q plan snapshot link differs from record", runID))
				}
			} else {
				legacyPlanDigests[record.Plan.Digest] = struct{}{}
			}
		}
		if rowsErr := startRows.Err(); rowsErr != nil {
			_ = startRows.Close()
			return rowsErr
		}
		if closeErr := startRows.Close(); closeErr != nil {
			return closeErr
		}

		for _, item := range candidates {
			if len(deleted) == limit {
				break
			}
			if _, live := protectedRefs[item.snapshotDigest]; live {
				continue
			}
			if _, legacyLive := legacyPlanDigests[item.planDigest]; legacyLive {
				continue
			}
			var material persistedPlanMaterial
			if decodeErr := decodeWorkflowJSON("workflow plan cleanup material", item.material, &material); decodeErr != nil {
				return decodeErr
			}
			if material.Source != nil && material.Source.Retention == values.RetentionProject {
				continue
			}
			if material.Source != nil && material.Source.Retention != values.RetentionRun {
				return workflowInvalid(fmt.Errorf("workflow plan snapshot %q has unsupported cleanup retention %q", item.snapshotDigest, material.Source.Retention))
			}
			result, deleteErr := query.ExecContext(ctx, `DELETE FROM workflow_plan_snapshots WHERE snapshot_digest = ? AND NOT EXISTS (SELECT 1 FROM workflow_host_start_plan_snapshots WHERE snapshot_digest = ?)`, item.snapshotDigest, item.snapshotDigest)
			if deleteErr != nil {
				return deleteErr
			}
			count, countErr := result.RowsAffected()
			if countErr != nil {
				return countErr
			}
			if count == 1 {
				deleted = append(deleted, item.snapshotDigest)
			}
		}
		return nil
	})
	return deleted, err
}
