package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

var _ workflowruntime.FanOutStore = (*WorkflowStateStore)(nil)

func (s *WorkflowStateStore) LoadFanOut(ctx context.Context, parent workflowruntime.NodeInvocationID) (workflowruntime.FanOutSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.FanOutSnapshot{}, err
	}
	return loadWorkflowFanOut(ctx, s.db, parent)
}

func loadWorkflowFanOut(ctx context.Context, query workflowSQL, parent workflowruntime.NodeInvocationID) (workflowruntime.FanOutSnapshot, error) {
	var encoded string
	if err := query.QueryRowContext(ctx, `SELECT snapshot_json FROM workflow_fanouts WHERE run_id = ? AND node_id = ?`, parent.RunID, parent.NodeID).Scan(&encoded); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workflowruntime.FanOutSnapshot{}, fmt.Errorf("%w: fan-out aggregate", workflowruntime.ErrNotFound)
		}
		return workflowruntime.FanOutSnapshot{}, fmt.Errorf("load workflow fan-out: %w", err)
	}
	var snapshot workflowruntime.FanOutSnapshot
	if err := decodeWorkflowJSON("fan-out", encoded, &snapshot); err != nil {
		return workflowruntime.FanOutSnapshot{}, err
	}
	if snapshot.Parent != parent {
		return workflowruntime.FanOutSnapshot{}, workflowInvalid(errors.New("persisted fan-out identity diverges"))
	}
	if err := snapshot.Validate(); err != nil {
		return workflowruntime.FanOutSnapshot{}, workflowInvalid(err)
	}
	return snapshot, nil
}

func (s *WorkflowStateStore) LoadFanOutItemResults(ctx context.Context, parent workflowruntime.NodeInvocationID) ([]workflowruntime.FanOutItemResult, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	fanOut, err := loadWorkflowFanOut(ctx, s.db, parent)
	if err != nil {
		return nil, err
	}
	result := make([]workflowruntime.FanOutItemResult, len(fanOut.Items))
	for index, binding := range fanOut.Items {
		node, err := loadWorkflowNode(ctx, s.db, binding.Invocation)
		if err != nil {
			return nil, err
		}
		item := workflowruntime.FanOutItemResult{Index: index, Iteration: binding.Iteration, Invocation: binding.Invocation, Status: node.Status, Outputs: cloneWorkflowValueRef(node.Outputs)}
		if node.Outputs != nil {
			item.OutputValues, err = loadWorkflowValues(ctx, s.db, *node.Outputs)
			if err != nil {
				return nil, err
			}
		}
		if node.LatestAttempt > 0 && workflowFanOutHardFailure(node.Status) {
			attempt, err := loadWorkflowAttempt(ctx, s.db, workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt})
			if err != nil {
				return nil, err
			}
			item.Failure = cloneWorkflowFailure(attempt.Failure)
		}
		result[index] = item
	}
	return result, nil
}

func (s *WorkflowStateStore) ExpandFanOut(ctx context.Context, request workflowruntime.ExpandFanOutRequest) (workflowruntime.ExpandFanOutResult, error) {
	if err := request.Validate(); err != nil {
		return workflowruntime.ExpandFanOutResult{}, workflowInvalid(err)
	}
	var result workflowruntime.ExpandFanOutResult
	writeErr := s.write(ctx, "expand workflow fan-out", func(query workflowSQL) error {
		run, runErr := loadWorkflowRun(ctx, query, request.FanOut.Parent.RunID)
		if runErr != nil {
			return runErr
		}
		if !run.Status.Active() {
			return workflowInvalid(errors.New("terminal run cannot expand fan-out"))
		}
		allowed, err := workflowControlAdmissionAllowed(ctx, query, request.FanOut.Parent)
		if err != nil {
			return err
		}
		if !allowed {
			return workflowInvalid(errors.New("pending terminal intent fences fan-out expansion"))
		}
		parent, parentErr := loadWorkflowNode(ctx, query, request.FanOut.Parent)
		if parentErr != nil {
			return parentErr
		}
		if parent.Generation != request.ExpectedParentGeneration {
			return workflowCAS("fan-out parent", request.ExpectedParentGeneration, parent.Generation)
		}
		if parent.Status != workflowruntime.NodePending && parent.Status != workflowruntime.NodeBlocked && parent.Status != workflowruntime.NodeReady {
			return workflowInvalid(fmt.Errorf("fan-out parent status %q cannot expand", parent.Status))
		}
		if parent.LatestAttempt != 0 || parent.Lease != nil || parent.Wait != nil || parent.Outputs != nil {
			return workflowInvalid(errors.New("fan-out parent must not have attempt, lease, wait, or outputs"))
		}
		at := request.At.UTC()
		if at.Before(parent.UpdatedAt) {
			return workflowInvalid(errors.New("fan-out expansion time must not regress parent"))
		}
		var exists int
		if queryErr := query.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_fanouts WHERE run_id = ? AND node_id = ?`, parent.ID.RunID, parent.ID.NodeID).Scan(&exists); queryErr != nil {
			return queryErr
		}
		if exists != 0 {
			return fmt.Errorf("%w: fan-out aggregate", workflowruntime.ErrAlreadyExists)
		}
		children := make([]workflowruntime.NodeInvocationSnapshot, len(request.FanOut.Items))
		for index, binding := range request.FanOut.Items {
			if _, valuesErr := loadWorkflowValues(ctx, query, binding.Inputs); valuesErr != nil {
				return workflowInvalid(fmt.Errorf("fan-out item %d inputs do not resolve: %w", index, valuesErr))
			}
			child := workflowruntime.NodeInvocationSnapshot{
				ID: binding.Invocation, Status: workflowruntime.NodeReady, Inputs: cloneWorkflowValueRef(&binding.Inputs),
				Priority: request.Priority, Generation: 1, CreatedAt: at, UpdatedAt: at,
			}
			if validationErr := child.Validate(); validationErr != nil {
				return workflowInvalid(validationErr)
			}
			if insertErr := insertWorkflowNode(ctx, query, child); insertErr != nil {
				return insertErr
			}
			children[index] = child
		}
		fanOut := cloneWorkflowFanOut(request.FanOut)
		fanOut.Generation = 1
		fanOut.CreatedAt, fanOut.UpdatedAt = at, at
		nextParent := cloneWorkflowNode(parent)
		nextParent.Status, nextParent.Blocked = workflowruntime.NodeWaiting, nil
		nextParent.Generation++
		nextParent.UpdatedAt = at
		if validationErr := fanOut.Validate(); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		if validationErr := nextParent.Validate(); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		encoded, encodeErr := encodeWorkflowJSON(fanOut)
		if encodeErr != nil {
			return encodeErr
		}
		if _, execErr := query.ExecContext(ctx, `INSERT INTO workflow_fanouts(run_id, node_id, iteration, status, max_concurrency, fail_fast, generation, snapshot_json) VALUES (?, ?, '', ?, ?, ?, ?, ?)`, fanOut.Parent.RunID, fanOut.Parent.NodeID, fanOut.Status, fanOut.MaxConcurrency, fanOut.FailFast, fanOut.Generation, encoded); execErr != nil {
			return fmt.Errorf("insert workflow fan-out: %w", execErr)
		}
		for _, binding := range fanOut.Items {
			inputsJSON, inputsErr := encodeWorkflowJSON(binding.Inputs)
			if inputsErr != nil {
				return inputsErr
			}
			if _, execErr := query.ExecContext(ctx, `INSERT INTO workflow_fanout_items(run_id, node_id, item_index, iteration, inputs_ref_json) VALUES (?, ?, ?, ?, ?)`, fanOut.Parent.RunID, fanOut.Parent.NodeID, binding.Index, binding.Iteration, inputsJSON); execErr != nil {
				return fmt.Errorf("insert workflow fan-out item: %w", execErr)
			}
		}
		if updateErr := updateWorkflowNodeCAS(ctx, query, nextParent, parent.Generation); updateErr != nil {
			return updateErr
		}
		parentID := parent.ID
		event, eventErr := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{
			RunID: parent.ID.RunID, Invocation: &parentID, Type: workflowruntime.EventFanOutExpanded, OccurredAt: at,
			Attributes: map[string]string{"items": strconv.Itoa(len(children)), "max_concurrency": strconv.Itoa(fanOut.MaxConcurrency), "from_status": string(parent.Status), "to_status": string(nextParent.Status)},
			Redaction:  values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if eventErr != nil {
			return eventErr
		}
		result = workflowruntime.ExpandFanOutResult{FanOut: fanOut, Parent: nextParent, Children: children, Event: event}
		return nil
	})
	if writeErr != nil {
		return workflowruntime.ExpandFanOutResult{}, writeErr
	}
	return result, nil
}

func (s *WorkflowStateStore) CompleteFanOut(ctx context.Context, request workflowruntime.CompleteFanOutRequest) (workflowruntime.CompleteFanOutResult, error) {
	if err := request.Validate(); err != nil {
		return workflowruntime.CompleteFanOutResult{}, workflowInvalid(err)
	}
	var result workflowruntime.CompleteFanOutResult
	writeErr := s.write(ctx, "complete workflow fan-out", func(query workflowSQL) error {
		run, runErr := loadWorkflowRun(ctx, query, request.Parent.RunID)
		if runErr != nil {
			return runErr
		}
		if !run.Status.Active() {
			return workflowInvalid(errors.New("terminal run fences fan-out completion"))
		}
		allowed, err := workflowControlAdmissionAllowed(ctx, query, request.Parent)
		if err != nil {
			return err
		}
		if !allowed {
			return workflowInvalid(errors.New("pending terminal intent fences fan-out completion"))
		}
		parent, parentErr := loadWorkflowNode(ctx, query, request.Parent)
		if parentErr != nil {
			return parentErr
		}
		if parent.Generation != request.ExpectedParentGeneration {
			return workflowCAS("fan-out parent", request.ExpectedParentGeneration, parent.Generation)
		}
		fanOut, fanOutErr := loadWorkflowFanOut(ctx, query, request.Parent)
		if fanOutErr != nil {
			return fanOutErr
		}
		if fanOut.Generation != request.ExpectedFanOutGeneration {
			return workflowCAS("fan-out aggregate", request.ExpectedFanOutGeneration, fanOut.Generation)
		}
		if fanOut.Status != workflowruntime.FanOutActive || parent.Status != workflowruntime.NodeWaiting {
			return workflowInvalid(errors.New("fan-out completion requires active waiting aggregate"))
		}
		if len(request.ExpectedChildGenerations) != len(fanOut.Items) {
			return workflowInvalid(errors.New("fan-out completion must fence every item"))
		}
		for _, binding := range fanOut.Items {
			child, childErr := loadWorkflowNode(ctx, query, binding.Invocation)
			if childErr != nil {
				return childErr
			}
			expected, ok := request.ExpectedChildGenerations[binding.Invocation]
			if !ok || child.Generation != expected {
				return workflowCAS("fan-out child", expected, child.Generation)
			}
			if !child.Status.Terminal() {
				return fmt.Errorf("%w: child %s is %s", workflowruntime.ErrFanOutIncomplete, binding.Iteration, child.Status)
			}
		}
		if _, valuesErr := loadWorkflowValues(ctx, query, request.Outputs); valuesErr != nil {
			return workflowInvalid(fmt.Errorf("fan-out aggregate outputs do not resolve: %w", valuesErr))
		}
		at := request.At.UTC()
		if at.Before(parent.UpdatedAt) || at.Before(fanOut.UpdatedAt) {
			return workflowInvalid(errors.New("fan-out completion time must not regress"))
		}
		nextFanOut := cloneWorkflowFanOut(fanOut)
		nextFanOut.Status = request.Status
		nextFanOut.Outputs = cloneWorkflowValueRef(&request.Outputs)
		nextFanOut.Failure = cloneWorkflowFailure(request.Failure)
		nextFanOut.Generation++
		nextFanOut.UpdatedAt = at
		nextParent := cloneWorkflowNode(parent)
		switch request.Status {
		case workflowruntime.FanOutSucceeded:
			nextParent.Status = workflowruntime.NodeSucceeded
		case workflowruntime.FanOutFailed:
			nextParent.Status = workflowruntime.NodeFailed
		case workflowruntime.FanOutCanceled:
			nextParent.Status = workflowruntime.NodeCanceled
		case workflowruntime.FanOutActive:
			return workflowInvalid(errors.New("active fan-out cannot complete"))
		}
		nextParent.Outputs = cloneWorkflowValueRef(&request.Outputs)
		nextParent.Generation++
		nextParent.UpdatedAt = at
		if validationErr := nextFanOut.Validate(); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		if validationErr := nextParent.Validate(); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		encoded, encodeErr := encodeWorkflowJSON(nextFanOut)
		if encodeErr != nil {
			return encodeErr
		}
		updated, updateErr := query.ExecContext(ctx, `UPDATE workflow_fanouts SET status = ?, generation = ?, snapshot_json = ? WHERE run_id = ? AND node_id = ? AND generation = ?`, nextFanOut.Status, nextFanOut.Generation, encoded, request.Parent.RunID, request.Parent.NodeID, fanOut.Generation)
		if updateErr != nil {
			return updateErr
		}
		if rowErr := expectOneWorkflowRow(updated, "fan-out aggregate", fanOut.Generation, fanOut.Generation); rowErr != nil {
			return rowErr
		}
		if nodeUpdateErr := updateWorkflowNodeCAS(ctx, query, nextParent, parent.Generation); nodeUpdateErr != nil {
			return nodeUpdateErr
		}
		parentID := parent.ID
		event, eventErr := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{
			RunID: parent.ID.RunID, Invocation: &parentID, Type: workflowruntime.EventFanOutCompleted, OccurredAt: at,
			Attributes: map[string]string{"status": string(nextFanOut.Status), "items": strconv.Itoa(len(fanOut.Items))}, Values: &request.Outputs,
			Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if eventErr != nil {
			return eventErr
		}
		result = workflowruntime.CompleteFanOutResult{FanOut: nextFanOut, Parent: nextParent, Event: event}
		return nil
	})
	if writeErr != nil {
		return workflowruntime.CompleteFanOutResult{}, writeErr
	}
	return result, nil
}

func workflowFanOutClaimEligible(ctx context.Context, query workflowSQL, node workflowruntime.NodeInvocationSnapshot, now time.Time) (bool, error) {
	if node.ID.Iteration == "" || node.LatestAttempt > 0 {
		return true, nil
	}
	parent := workflowruntime.NodeInvocationID{RunID: node.ID.RunID, NodeID: node.ID.NodeID}
	fanOut, err := loadWorkflowFanOut(ctx, query, parent)
	if errors.Is(err, workflowruntime.ErrNotFound) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	member, occupied := false, 0
	children := make([]workflowruntime.NodeInvocationSnapshot, 0, len(fanOut.Items))
	for _, binding := range fanOut.Items {
		if binding.Invocation == node.ID {
			member = true
			continue
		}
		child, childErr := loadWorkflowNode(ctx, query, binding.Invocation)
		if childErr != nil {
			return false, childErr
		}
		children = append(children, child)
		if child.Status.Terminal() {
			continue
		}
		if child.LatestAttempt > 0 || child.Lease != nil && child.Lease.ExpiresAt.After(now) {
			occupied++
		}
	}
	allowed, err := workflowruntime.FanOutFailFastAdmissionAllowed(fanOut, children)
	if err != nil || member && !allowed {
		return false, err
	}
	if fanOut.MaxConcurrency == 0 {
		return true, nil
	}
	return !member || occupied < fanOut.MaxConcurrency, nil
}

func cloneWorkflowFanOut(snapshot workflowruntime.FanOutSnapshot) workflowruntime.FanOutSnapshot {
	if snapshot.Tolerate != nil {
		policy := *snapshot.Tolerate
		snapshot.Tolerate = &policy
	}
	snapshot.Items = append([]workflowruntime.FanOutItemBinding(nil), snapshot.Items...)
	snapshot.Outputs = cloneWorkflowValueRef(snapshot.Outputs)
	snapshot.Failure = cloneWorkflowFailure(snapshot.Failure)
	return snapshot
}

func workflowFanOutHardFailure(status workflowruntime.NodeStatus) bool {
	return status == workflowruntime.NodeFailed || status == workflowruntime.NodeTimedOut || status == workflowruntime.NodeCrashed || status == workflowruntime.NodeCanceled
}
