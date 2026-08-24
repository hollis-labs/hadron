package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
)

var _ workflowruntime.SchedulerResourceStore = (*WorkflowStateStore)(nil)

func (s *WorkflowStateStore) AdmitNode(ctx context.Context, request workflowruntime.AdmitNodeRequest) (workflowruntime.AdmitNodeResult, error) {
	request.Claim.Now = request.Claim.Now.UTC()
	request.Claim.LeaseUntil = request.Claim.LeaseUntil.UTC()
	request.EnqueuedAt = request.EnqueuedAt.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.AdmitNodeResult{}, workflowInvalid(err)
	}
	requestJSON, encodeErr := encodeWorkflowJSON(request)
	if encodeErr != nil {
		return workflowruntime.AdmitNodeResult{}, encodeErr
	}
	var result workflowruntime.AdmitNodeResult
	writeErr := s.write(ctx, "admit workflow scheduler node", func(query workflowSQL) error {
		priorRequest, priorResult, found, loadErr := loadWorkflowIdempotency(ctx, query, "workflow_scheduler_admission_idempotency", request.Claim.IdempotencyKey)
		if loadErr != nil {
			return loadErr
		}
		if found {
			if priorRequest != requestJSON {
				return workflowIdempotencyConflict("scheduler admission", request.Claim.IdempotencyKey)
			}
			if err := decodeWorkflowJSON("scheduler admission result", priorResult, &result); err != nil {
				return err
			}
			allowed, err := workflowControlAdmissionAllowed(ctx, query, request.Claim.InvocationID)
			if err != nil {
				return err
			}
			if !allowed {
				result = workflowruntime.AdmitNodeResult{Claim: workflowruntime.ClaimResult{Replayed: true}}
				return nil
			}
			if result.Claim.Acquired {
				node, err := loadWorkflowNode(ctx, query, request.Claim.InvocationID)
				if err != nil {
					return err
				}
				if result.Claim.Lease == nil || node.Lease == nil || !matchesWorkflowLease(node.Lease, result.Claim.Lease.Owner, result.Claim.Lease.Token, result.Claim.Lease.Generation) || !node.Lease.ExpiresAt.Equal(result.Claim.Lease.ExpiresAt) || !node.Lease.ExpiresAt.After(request.Claim.Now) {
					return workflowIdempotencyConflict("scheduler admission", request.Claim.IdempotencyKey)
				}
				if err := validateWorkflowSchedulerReplayHolders(ctx, query, request, result.Claim.Lease); err != nil {
					return err
				}
			}
			result.Claim.Replayed = true
			return nil
		}
		current, err := loadWorkflowNode(ctx, query, request.Claim.InvocationID)
		if err != nil {
			return err
		}
		if current.ClaimGeneration != request.Claim.ExpectedClaimGeneration {
			return workflowCAS("scheduler node claim", request.Claim.ExpectedClaimGeneration, current.ClaimGeneration)
		}
		if definitionErr := ensureWorkflowSchedulerDefinitions(ctx, query, request.Requirements); definitionErr != nil {
			return definitionErr
		}
		if expiryErr := expireWorkflowSchedulerHolders(ctx, query, request.Claim.Now); expiryErr != nil {
			return expiryErr
		}
		run, err := loadWorkflowRun(ctx, query, current.ID.RunID)
		if err != nil {
			return err
		}
		allowed, err := workflowControlAdmissionAllowed(ctx, query, current.ID)
		if err != nil {
			return err
		}
		if current.Status != workflowruntime.NodeReady || !run.Status.Active() || !allowed || current.Lease != nil && current.Lease.ExpiresAt.After(request.Claim.Now) {
			if waiterErr := deleteWorkflowSchedulerWaiter(ctx, query, current.ID); waiterErr != nil {
				return waiterErr
			}
			return recordWorkflowSchedulerAdmission(ctx, query, request.Claim.IdempotencyKey, requestJSON, result)
		}
		if request.Claim.Now.Before(current.UpdatedAt) {
			return workflowInvalid(errors.New("claim time must not regress node updated_at"))
		}
		blocked, err := blockedWorkflowSchedulerResources(ctx, query, current.ID, request.Requirements, request.Claim.Now)
		if err != nil {
			return err
		}
		fanOutEligible, err := workflowFanOutClaimEligible(ctx, query, current, request.Claim.Now)
		if err != nil {
			return err
		}
		if !fanOutEligible {
			blocked = append(blocked, workflowruntime.SchedulerResourceID{Kind: workflowruntime.SchedulerResourceFanOut, RunID: current.ID.RunID, NodeID: current.ID.NodeID})
		}
		sortWorkflowSchedulerResourceIDs(blocked)
		if len(blocked) != 0 {
			result.Blocked = blocked
			waiter := workflowruntime.SchedulerResourceWaiter{Invocation: current.ID, Requirements: append([]workflowruntime.SchedulerResourceRequirement(nil), request.Requirements...), Blocked: append([]workflowruntime.SchedulerResourceID(nil), blocked...), Priority: request.Priority, EnqueuedAt: request.EnqueuedAt, UpdatedAt: request.Claim.Now}
			if err := upsertWorkflowSchedulerWaiter(ctx, query, waiter); err != nil {
				return err
			}
			return recordWorkflowSchedulerAdmission(ctx, query, request.Claim.IdempotencyKey, requestJSON, result)
		}
		next := cloneWorkflowNode(current)
		next.ClaimGeneration++
		next.Lease = &workflowruntime.ClaimLease{Owner: request.Claim.Owner, Token: request.Claim.Token, Generation: next.ClaimGeneration, ExpiresAt: request.Claim.LeaseUntil}
		next.Generation++
		next.UpdatedAt = request.Claim.Now
		if err := next.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := updateWorkflowNodeCAS(ctx, query, next, current.Generation); err != nil {
			return err
		}
		for _, requirement := range request.Requirements {
			holder := workflowruntime.SchedulerResourceHolder{Resource: requirement.Resource, Invocation: next.ID, Units: requirement.Units, ClaimGeneration: next.ClaimGeneration, Owner: request.Claim.Owner, AcquiredAt: request.Claim.Now, ExpiresAt: request.Claim.LeaseUntil}
			if err := insertWorkflowSchedulerHolder(ctx, query, holder); err != nil {
				return err
			}
		}
		if err := deleteWorkflowSchedulerWaiter(ctx, query, next.ID); err != nil {
			return err
		}
		result.Claim = workflowruntime.ClaimResult{Acquired: true, Lease: cloneWorkflowLease(next.Lease)}
		return recordWorkflowSchedulerAdmission(ctx, query, request.Claim.IdempotencyKey, requestJSON, result)
	})
	if writeErr != nil {
		return workflowruntime.AdmitNodeResult{}, writeErr
	}
	return result, nil
}

func validateWorkflowSchedulerReplayHolders(ctx context.Context, query workflowSQL, request workflowruntime.AdmitNodeRequest, lease *workflowruntime.ClaimLease) error {
	for _, requirement := range request.Requirements {
		key, err := workflowSchedulerResourceKey(requirement.Resource)
		if err != nil {
			return err
		}
		var encoded string
		err = query.QueryRowContext(ctx, `SELECT snapshot_json FROM workflow_scheduler_holders WHERE resource_key = ? AND run_id = ? AND node_id = ? AND iteration = ?`, key, request.Claim.InvocationID.RunID, request.Claim.InvocationID.NodeID, request.Claim.InvocationID.Iteration).Scan(&encoded)
		if errors.Is(err, sql.ErrNoRows) {
			return workflowIdempotencyConflict("scheduler admission", request.Claim.IdempotencyKey)
		}
		if err != nil {
			return err
		}
		var holder workflowruntime.SchedulerResourceHolder
		if err := decodeWorkflowJSON("scheduler replay holder", encoded, &holder); err != nil {
			return err
		}
		if err := holder.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if holder.Resource != requirement.Resource || holder.Invocation != request.Claim.InvocationID || holder.Units != requirement.Units || holder.ClaimGeneration != lease.Generation || holder.Owner != lease.Owner || !holder.ExpiresAt.Equal(lease.ExpiresAt) {
			return workflowIdempotencyConflict("scheduler admission", request.Claim.IdempotencyKey)
		}
	}
	return nil
}

func (s *WorkflowStateStore) InspectSchedulerResources(ctx context.Context, request workflowruntime.SchedulerResourceQuery) (workflowruntime.SchedulerResourceState, error) {
	if request.RunID != "" {
		if err := (workflowruntime.NodeInvocationID{RunID: request.RunID, NodeID: "valid"}).Validate(); err != nil {
			return workflowruntime.SchedulerResourceState{}, workflowInvalid(err)
		}
	}
	if request.Now.IsZero() || request.Limit < 0 {
		return workflowruntime.SchedulerResourceState{}, workflowInvalid(errors.New("resource inspection requires now and nonnegative limit"))
	}
	request.Now = request.Now.UTC()
	state := workflowruntime.SchedulerResourceState{}
	err := s.write(ctx, "inspect workflow scheduler resources", func(query workflowSQL) error {
		if err := validateWorkflowSchedulerDefinitions(ctx, query); err != nil {
			return err
		}
		if err := expireWorkflowSchedulerHolders(ctx, query, request.Now); err != nil {
			return err
		}
		holders, err := listWorkflowSchedulerHolders(ctx, query)
		if err != nil {
			return err
		}
		for _, holder := range holders {
			if request.RunID == "" || holder.Invocation.RunID == request.RunID {
				state.Holders = append(state.Holders, holder)
			}
		}
		waiters, err := listWorkflowSchedulerWaiters(ctx, query)
		if err != nil {
			return err
		}
		for _, waiter := range waiters {
			node, loadErr := loadWorkflowNode(ctx, query, waiter.Invocation)
			if errors.Is(loadErr, workflowruntime.ErrNotFound) {
				_ = deleteWorkflowSchedulerWaiter(ctx, query, waiter.Invocation)
				continue
			}
			if loadErr != nil {
				return loadErr
			}
			run, runErr := loadWorkflowRun(ctx, query, waiter.Invocation.RunID)
			if runErr != nil {
				return runErr
			}
			allowed, admissionErr := workflowControlAdmissionAllowed(ctx, query, waiter.Invocation)
			if admissionErr != nil {
				return admissionErr
			}
			if node.Status != workflowruntime.NodeReady || !run.Status.Active() || !allowed || node.Lease != nil && node.Lease.ExpiresAt.After(request.Now) {
				if err := deleteWorkflowSchedulerWaiter(ctx, query, waiter.Invocation); err != nil {
					return err
				}
				continue
			}
			blocked, blockedErr := blockedWorkflowSchedulerResources(ctx, query, node.ID, waiter.Requirements, request.Now)
			if blockedErr != nil {
				return blockedErr
			}
			fanOutEligible, fanOutErr := workflowFanOutClaimEligible(ctx, query, node, request.Now)
			if fanOutErr != nil {
				return fanOutErr
			}
			if !fanOutEligible {
				blocked = append(blocked, workflowruntime.SchedulerResourceID{Kind: workflowruntime.SchedulerResourceFanOut, RunID: node.ID.RunID, NodeID: node.ID.NodeID})
			}
			sortWorkflowSchedulerResourceIDs(blocked)
			if len(blocked) == 0 {
				if err := deleteWorkflowSchedulerWaiter(ctx, query, waiter.Invocation); err != nil {
					return err
				}
				continue
			}
			waiter.Blocked = blocked
			if err := upsertWorkflowSchedulerWaiter(ctx, query, waiter); err != nil {
				return err
			}
			if request.RunID == "" || waiter.Invocation.RunID == request.RunID {
				state.Waiters = append(state.Waiters, waiter)
			}
		}
		return nil
	})
	if err != nil {
		return workflowruntime.SchedulerResourceState{}, err
	}
	sort.Slice(state.Holders, func(i, j int) bool {
		if state.Holders[i].Resource != state.Holders[j].Resource {
			return workflowSchedulerResourceLess(state.Holders[i].Resource, state.Holders[j].Resource)
		}
		return workflowInvocationLess(state.Holders[i].Invocation, state.Holders[j].Invocation)
	})
	sort.Slice(state.Waiters, func(i, j int) bool {
		if !state.Waiters[i].EnqueuedAt.Equal(state.Waiters[j].EnqueuedAt) {
			return state.Waiters[i].EnqueuedAt.Before(state.Waiters[j].EnqueuedAt)
		}
		if state.Waiters[i].Priority != state.Waiters[j].Priority {
			return state.Waiters[i].Priority > state.Waiters[j].Priority
		}
		return workflowInvocationLess(state.Waiters[i].Invocation, state.Waiters[j].Invocation)
	})
	if request.Limit > 0 {
		if len(state.Holders) > request.Limit {
			state.Holders = state.Holders[:request.Limit]
		}
		if len(state.Waiters) > request.Limit {
			state.Waiters = state.Waiters[:request.Limit]
		}
	}
	return state, nil
}

func validateWorkflowSchedulerDefinitions(ctx context.Context, query workflowSQL) error {
	rows, err := query.QueryContext(ctx, `SELECT resource_key, limit_value, resource_json FROM workflow_scheduler_resources`)
	if err != nil {
		return err
	}
	defer closeRows(rows)
	for rows.Next() {
		var key, encoded string
		var limit int
		if err := rows.Scan(&key, &limit, &encoded); err != nil {
			return err
		}
		var resource workflowruntime.SchedulerResourceID
		if err := decodeWorkflowJSON("scheduler resource definition", encoded, &resource); err != nil {
			return err
		}
		derived, err := workflowSchedulerResourceKey(resource)
		if err != nil {
			return err
		}
		if derived != key || limit < 1 {
			return workflowInvalid(errors.New("scheduler resource definition columns diverge from semantic snapshot"))
		}
	}
	return rows.Err()
}

func ensureWorkflowSchedulerDefinitions(ctx context.Context, query workflowSQL, requirements []workflowruntime.SchedulerResourceRequirement) error {
	for _, requirement := range requirements {
		key, err := workflowSchedulerResourceKey(requirement.Resource)
		if err != nil {
			return err
		}
		var limit int
		var resourceJSON string
		err = query.QueryRowContext(ctx, `SELECT limit_value, resource_json FROM workflow_scheduler_resources WHERE resource_key = ?`, key).Scan(&limit, &resourceJSON)
		if err == nil {
			var persisted workflowruntime.SchedulerResourceID
			if decodeErr := decodeWorkflowJSON("scheduler resource definition", resourceJSON, &persisted); decodeErr != nil {
				return decodeErr
			}
			persistedKey, keyErr := workflowSchedulerResourceKey(persisted)
			if keyErr != nil {
				return keyErr
			}
			if persistedKey != key || persisted != requirement.Resource {
				return workflowInvalid(errors.New("scheduler resource definition columns diverge from semantic snapshot"))
			}
			if limit != requirement.Limit {
				return workflowInvalid(fmt.Errorf("%w: resource limit differs from durable definition", workflowruntime.ErrInvalidSchedulerResource))
			}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		encoded, encodeErr := encodeWorkflowJSON(requirement.Resource)
		if encodeErr != nil {
			return encodeErr
		}
		if _, err := query.ExecContext(ctx, `INSERT INTO workflow_scheduler_resources(resource_key, limit_value, resource_json) VALUES (?, ?, ?)`, key, requirement.Limit, encoded); err != nil {
			return err
		}
	}
	return nil
}

func blockedWorkflowSchedulerResources(ctx context.Context, query workflowSQL, invocation workflowruntime.NodeInvocationID, requirements []workflowruntime.SchedulerResourceRequirement, now time.Time) ([]workflowruntime.SchedulerResourceID, error) {
	blocked := make([]workflowruntime.SchedulerResourceID, 0)
	for _, requirement := range requirements {
		key, err := workflowSchedulerResourceKey(requirement.Resource)
		if err != nil {
			return nil, err
		}
		rows, err := query.QueryContext(ctx, `SELECT snapshot_json FROM workflow_scheduler_holders WHERE resource_key = ?`, key)
		if err != nil {
			return nil, err
		}
		occupied := 0
		for rows.Next() {
			var encoded string
			if err := rows.Scan(&encoded); err != nil {
				closeRows(rows)
				return nil, err
			}
			var holder workflowruntime.SchedulerResourceHolder
			if err := decodeWorkflowJSON("scheduler holder", encoded, &holder); err != nil {
				closeRows(rows)
				return nil, err
			}
			if err := holder.Validate(); err != nil {
				closeRows(rows)
				return nil, workflowInvalid(err)
			}
			if holder.Invocation != invocation && holder.ExpiresAt.After(now) {
				occupied += holder.Units
			}
		}
		if err := rows.Err(); err != nil {
			closeRows(rows)
			return nil, err
		}
		closeRows(rows)
		if occupied+requirement.Units > requirement.Limit {
			blocked = append(blocked, requirement.Resource)
		}
	}
	return blocked, nil
}

func insertWorkflowSchedulerHolder(ctx context.Context, query workflowSQL, holder workflowruntime.SchedulerResourceHolder) error {
	if err := holder.Validate(); err != nil {
		return workflowInvalid(err)
	}
	key, err := workflowSchedulerResourceKey(holder.Resource)
	if err != nil {
		return err
	}
	encoded, err := encodeWorkflowJSON(holder)
	if err != nil {
		return err
	}
	_, err = query.ExecContext(ctx, `INSERT INTO workflow_scheduler_holders(resource_key, run_id, node_id, iteration, units, claim_generation, owner, acquired_at, expires_at, snapshot_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, key, holder.Invocation.RunID, holder.Invocation.NodeID, holder.Invocation.Iteration, holder.Units, holder.ClaimGeneration, holder.Owner, workflowTime(holder.AcquiredAt), workflowTime(holder.ExpiresAt), encoded)
	return err
}

func expireWorkflowSchedulerHolders(ctx context.Context, query workflowSQL, now time.Time) error {
	holders, err := listWorkflowSchedulerHolders(ctx, query)
	if err != nil {
		return err
	}
	for _, holder := range holders {
		if holder.ExpiresAt.After(now) {
			continue
		}
		key, err := workflowSchedulerResourceKey(holder.Resource)
		if err != nil {
			return err
		}
		if _, err := query.ExecContext(ctx, `DELETE FROM workflow_scheduler_holders WHERE resource_key = ? AND run_id = ? AND node_id = ? AND iteration = ?`, key, holder.Invocation.RunID, holder.Invocation.NodeID, holder.Invocation.Iteration); err != nil {
			return err
		}
	}
	return nil
}

func listWorkflowSchedulerHolders(ctx context.Context, query workflowSQL) ([]workflowruntime.SchedulerResourceHolder, error) {
	rows, err := query.QueryContext(ctx, `SELECT resource_key, run_id, node_id, iteration, units, claim_generation, owner, acquired_at, expires_at, snapshot_json FROM workflow_scheduler_holders`)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	result := make([]workflowruntime.SchedulerResourceHolder, 0)
	for rows.Next() {
		var resourceKey, runID, nodeID, iteration, owner, acquiredAt, expiresAt, encoded string
		var units int
		var claimGeneration int64
		if err := rows.Scan(&resourceKey, &runID, &nodeID, &iteration, &units, &claimGeneration, &owner, &acquiredAt, &expiresAt, &encoded); err != nil {
			return nil, err
		}
		var holder workflowruntime.SchedulerResourceHolder
		if err := decodeWorkflowJSON("scheduler holder", encoded, &holder); err != nil {
			return nil, err
		}
		parsedGeneration, generationErr := workflowGeneration("scheduler holder claim generation", claimGeneration)
		if generationErr != nil {
			return nil, generationErr
		}
		parsedAcquired, acquiredErr := parseWorkflowTime("scheduler holder acquired_at", acquiredAt)
		if acquiredErr != nil {
			return nil, acquiredErr
		}
		parsedExpiry, expiryErr := parseWorkflowTime("scheduler holder expires_at", expiresAt)
		if expiryErr != nil {
			return nil, expiryErr
		}
		derivedKey, keyErr := workflowSchedulerResourceKey(holder.Resource)
		if keyErr != nil {
			return nil, keyErr
		}
		columnsMatch := derivedKey == resourceKey && holder.Invocation == (workflowruntime.NodeInvocationID{RunID: workflowruntime.RunID(runID), NodeID: nodeID, Iteration: iteration}) &&
			holder.Units == units && holder.ClaimGeneration == parsedGeneration && holder.Owner == owner &&
			holder.AcquiredAt.Equal(parsedAcquired) && holder.ExpiresAt.Equal(parsedExpiry)
		if !columnsMatch {
			return nil, workflowInvalid(errors.New("scheduler holder columns diverge from semantic snapshot"))
		}
		if err := holder.Validate(); err != nil {
			return nil, workflowInvalid(err)
		}
		result = append(result, holder)
	}
	return result, rows.Err()
}

func upsertWorkflowSchedulerWaiter(ctx context.Context, query workflowSQL, waiter workflowruntime.SchedulerResourceWaiter) error {
	if err := waiter.Validate(); err != nil {
		return workflowInvalid(err)
	}
	encoded, err := encodeWorkflowJSON(waiter)
	if err != nil {
		return err
	}
	_, err = query.ExecContext(ctx, `INSERT INTO workflow_scheduler_waiters(run_id, node_id, iteration, priority, enqueued_at, updated_at, snapshot_json) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(run_id, node_id, iteration) DO UPDATE SET priority=excluded.priority, enqueued_at=excluded.enqueued_at, updated_at=excluded.updated_at, snapshot_json=excluded.snapshot_json`, waiter.Invocation.RunID, waiter.Invocation.NodeID, waiter.Invocation.Iteration, waiter.Priority, workflowTime(waiter.EnqueuedAt), workflowTime(waiter.UpdatedAt), encoded)
	return err
}

func deleteWorkflowSchedulerWaiter(ctx context.Context, query workflowSQL, id workflowruntime.NodeInvocationID) error {
	_, err := query.ExecContext(ctx, `DELETE FROM workflow_scheduler_waiters WHERE run_id = ? AND node_id = ? AND iteration = ?`, id.RunID, id.NodeID, id.Iteration)
	return err
}

func listWorkflowSchedulerWaiters(ctx context.Context, query workflowSQL) ([]workflowruntime.SchedulerResourceWaiter, error) {
	rows, err := query.QueryContext(ctx, `SELECT run_id, node_id, iteration, priority, enqueued_at, updated_at, snapshot_json FROM workflow_scheduler_waiters`)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	result := make([]workflowruntime.SchedulerResourceWaiter, 0)
	for rows.Next() {
		var runID, nodeID, iteration, enqueuedAt, updatedAt, encoded string
		var priority int
		if err := rows.Scan(&runID, &nodeID, &iteration, &priority, &enqueuedAt, &updatedAt, &encoded); err != nil {
			return nil, err
		}
		var waiter workflowruntime.SchedulerResourceWaiter
		if err := decodeWorkflowJSON("scheduler waiter", encoded, &waiter); err != nil {
			return nil, err
		}
		parsedEnqueued, enqueuedErr := parseWorkflowTime("scheduler waiter enqueued_at", enqueuedAt)
		if enqueuedErr != nil {
			return nil, enqueuedErr
		}
		parsedUpdated, updatedErr := parseWorkflowTime("scheduler waiter updated_at", updatedAt)
		if updatedErr != nil {
			return nil, updatedErr
		}
		columnsMatch := waiter.Invocation == (workflowruntime.NodeInvocationID{RunID: workflowruntime.RunID(runID), NodeID: nodeID, Iteration: iteration}) &&
			waiter.Priority == priority && waiter.EnqueuedAt.Equal(parsedEnqueued) && waiter.UpdatedAt.Equal(parsedUpdated)
		if !columnsMatch {
			return nil, workflowInvalid(errors.New("scheduler waiter columns diverge from semantic snapshot"))
		}
		if err := waiter.Validate(); err != nil {
			return nil, workflowInvalid(err)
		}
		result = append(result, waiter)
	}
	return result, rows.Err()
}

func recordWorkflowSchedulerAdmission(ctx context.Context, query workflowSQL, key, requestJSON string, result workflowruntime.AdmitNodeResult) error {
	resultJSON, err := encodeWorkflowJSON(result)
	if err != nil {
		return err
	}
	_, err = query.ExecContext(ctx, `INSERT INTO workflow_scheduler_admission_idempotency(idempotency_key, request_json, result_json) VALUES (?, ?, ?)`, key, requestJSON, resultJSON)
	if isSQLiteConstraint(err) {
		return workflowIdempotencyConflict("scheduler admission", key)
	}
	return err
}

func workflowSchedulerResourceKey(resource workflowruntime.SchedulerResourceID) (string, error) {
	if err := resource.Validate(); err != nil {
		return "", workflowInvalid(err)
	}
	return encodeWorkflowJSON(resource)
}

func releaseWorkflowSchedulerResources(ctx context.Context, query workflowSQL, id workflowruntime.NodeInvocationID) error {
	if _, err := query.ExecContext(ctx, `DELETE FROM workflow_scheduler_holders WHERE run_id = ? AND node_id = ? AND iteration = ?`, id.RunID, id.NodeID, id.Iteration); err != nil {
		return err
	}
	return deleteWorkflowSchedulerWaiter(ctx, query, id)
}

func syncWorkflowSchedulerHolderLease(ctx context.Context, query workflowSQL, id workflowruntime.NodeInvocationID, lease *workflowruntime.ClaimLease) error {
	if lease == nil {
		return releaseWorkflowSchedulerResources(ctx, query, id)
	}
	holders, err := listWorkflowSchedulerHolders(ctx, query)
	if err != nil {
		return err
	}
	for _, holder := range holders {
		if holder.Invocation != id {
			continue
		}
		if holder.ClaimGeneration != lease.Generation || holder.Owner != lease.Owner {
			return workflowInvalid(errors.New("scheduler holder differs from node lease"))
		}
		holder.ExpiresAt = lease.ExpiresAt
		key, err := workflowSchedulerResourceKey(holder.Resource)
		if err != nil {
			return err
		}
		encoded, err := encodeWorkflowJSON(holder)
		if err != nil {
			return err
		}
		if _, err := query.ExecContext(ctx, `UPDATE workflow_scheduler_holders SET expires_at = ?, snapshot_json = ? WHERE resource_key = ? AND run_id = ? AND node_id = ? AND iteration = ?`, workflowTime(holder.ExpiresAt), encoded, key, id.RunID, id.NodeID, id.Iteration); err != nil {
			return err
		}
	}
	return nil
}

func sortWorkflowSchedulerResourceIDs(input []workflowruntime.SchedulerResourceID) {
	sort.Slice(input, func(i, j int) bool { return workflowSchedulerResourceLess(input[i], input[j]) })
}

func workflowSchedulerResourceLess(left, right workflowruntime.SchedulerResourceID) bool {
	if left.Kind != right.Kind {
		return left.Kind < right.Kind
	}
	if left.Name != right.Name {
		return left.Name < right.Name
	}
	if left.RunID != right.RunID {
		return left.RunID < right.RunID
	}
	return left.NodeID < right.NodeID
}

func workflowInvocationLess(left, right workflowruntime.NodeInvocationID) bool {
	if left.RunID != right.RunID {
		return left.RunID < right.RunID
	}
	if left.NodeID != right.NodeID {
		return left.NodeID < right.NodeID
	}
	return left.Iteration < right.Iteration
}
