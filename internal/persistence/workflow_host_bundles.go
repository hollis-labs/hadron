package persistence

import (
	"context"
	"errors"
	"fmt"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
)

// FindBundledDefinitions resolves exact generated children only from complete
// immutable plans already recorded in the Hadron start journal. The resolver
// authorizes before calling this method and authorizes each returned trust
// context again; this store performs no policy decision and exposes no source
// bytes.
func (s *WorkflowHostStore) FindBundledDefinitions(ctx context.Context, requested graph.DefinitionRef) ([]hoststate.BundledDefinitionCandidate, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	if s == nil || s.state == nil || s.state.db == nil {
		return nil, errors.New("workflow host store is not initialized")
	}
	rows, err := s.state.db.QueryContext(ctx, `SELECT request_json FROM workflow_host_starts ORDER BY run_id`)
	if err != nil {
		return nil, fmt.Errorf("list durable workflow plans for bundle resolution: %w", err)
	}
	defer closeRows(rows)
	result := make([]hoststate.BundledDefinitionCandidate, 0)
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var encoded string
		if err := rows.Scan(&encoded); err != nil {
			return nil, fmt.Errorf("scan durable workflow plan for bundle resolution: %w", err)
		}
		var record hoststate.StartRecord
		if err := decodeWorkflowJSON("workflow host start bundle", encoded, &record); err != nil {
			return nil, err
		}
		if err := record.Validate(); err != nil {
			return nil, fmt.Errorf("%w: durable workflow plan for bundle resolution: %w", hoststate.ErrInvalidRecord, err)
		}
		resolver, err := workflowcompile.NewBundledDefinitionResolver(&record.Plan)
		if err != nil {
			return nil, fmt.Errorf("%w: durable workflow plan bundle: %w", hoststate.ErrInvalidRecord, err)
		}
		resolved, err := resolver.ResolveDefinition(ctx, requested)
		if errors.Is(err, workflowcompile.ErrBundledDefinitionNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("resolve durable workflow plan bundle: %w", err)
		}
		trustClass, _ := record.Plan.Provenance.Metadata["trust_class"].(string)
		result = append(result, hoststate.BundledDefinitionCandidate{
			Definition: resolved, Container: record.Run.Plan, TrustClass: trustClass,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate durable workflow plans for bundle resolution: %w", err)
	}
	return result, nil
}

var _ hoststate.BundledDefinitionSource = (*WorkflowHostStore)(nil)
