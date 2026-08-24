package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

var _ workflowruntime.ServiceStore = (*WorkflowStateStore)(nil)

func (s *WorkflowStateStore) LoadService(ctx context.Context, start workflowruntime.NodeInvocationID) (workflowruntime.ServiceSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.ServiceSnapshot{}, err
	}
	if err := start.Validate(); err != nil {
		return workflowruntime.ServiceSnapshot{}, workflowInvalid(err)
	}
	return loadWorkflowService(ctx, s.db, start)
}

func (s *WorkflowStateStore) PrepareServiceStart(ctx context.Context, request workflowruntime.PrepareServiceStartRequest) (workflowruntime.ServiceSnapshot, error) {
	request.At = request.At.UTC()
	if request.Service.Status != workflowruntime.ServiceLaunching || request.Service.Generation != 0 || request.At.IsZero() || request.ExpectedNodeGeneration == 0 || request.ExpectedAttemptGeneration == 0 {
		return workflowruntime.ServiceSnapshot{}, workflowInvalid(errors.New("service start intent is invalid"))
	}
	var result workflowruntime.ServiceSnapshot
	err := s.write(ctx, "prepare workflow service start", func(query workflowSQL) error {
		if prior, loadErr := loadWorkflowService(ctx, query, request.Service.Start.Invocation); loadErr == nil {
			candidate := cloneWorkflowService(request.Service)
			candidate.Generation, candidate.CreatedAt, candidate.UpdatedAt = prior.Generation, prior.CreatedAt, prior.UpdatedAt
			if prior.Status != workflowruntime.ServiceLaunching || !reflect.DeepEqual(prior, candidate) {
				return fmt.Errorf("%w: divergent service start intent", workflowruntime.ErrIdempotencyConflict)
			}
			result = prior
			return nil
		} else if !errors.Is(loadErr, workflowruntime.ErrNotFound) {
			return loadErr
		}
		node, err := loadWorkflowNode(ctx, query, request.Service.Start.Invocation)
		if err != nil {
			return err
		}
		attempt, err := loadWorkflowAttempt(ctx, query, request.Service.Start)
		if err != nil {
			return err
		}
		if node.Generation != request.ExpectedNodeGeneration || attempt.Generation != request.ExpectedAttemptGeneration {
			return workflowCAS("service start intent", request.ExpectedNodeGeneration, node.Generation)
		}
		if node.Status != workflowruntime.NodeRunning || attempt.Status != workflowruntime.NodeRunning || node.LatestAttempt != attempt.ID.Number {
			return workflowInvalid(errors.New("service start intent requires a running attempt"))
		}
		result = cloneWorkflowService(request.Service)
		result.Generation = 1
		result.CreatedAt, result.UpdatedAt = request.At, request.At
		if err := result.Validate(); err != nil {
			return workflowInvalid(err)
		}
		return insertWorkflowService(ctx, query, result)
	})
	return result, err
}

func (s *WorkflowStateStore) SuspendServiceStart(ctx context.Context, request workflowruntime.SuspendServiceStartRequest) (workflowruntime.SuspendServiceStartResult, error) {
	request.At = request.At.UTC()
	var result workflowruntime.SuspendServiceStartResult
	writeErr := s.write(ctx, "suspend workflow service start", func(query workflowSQL) error {
		prior, node, attempt, loadErr := loadWorkflowServiceAttempt(ctx, query, request.Service.Start.Invocation, request.Service.Start)
		if loadErr != nil {
			return loadErr
		}
		if prior.Status != workflowruntime.ServiceLaunching || request.Service.Status != workflowruntime.ServiceStarting || request.Service.Generation != 0 ||
			prior.Start != request.Service.Start || prior.Invocation.IdempotencyKey != request.Service.Invocation.IdempotencyKey {
			return workflowInvalid(errors.New("service suspension requires matching launch intent"))
		}
		if node.Generation != request.ExpectedNodeGeneration || attempt.Generation != request.ExpectedAttemptGeneration {
			return workflowCAS("service suspension", request.ExpectedNodeGeneration, node.Generation)
		}
		if err := validateWorkflowLifecycleClaim(node, &request.Claim, request.At); err != nil {
			return err
		}
		if node.Status != workflowruntime.NodeRunning || attempt.Status != workflowruntime.NodeRunning || request.At.Before(prior.UpdatedAt) {
			return workflowInvalid(errors.New("service suspension requires running state"))
		}
		nextService := cloneWorkflowService(request.Service)
		nextService.ReadyCheck = cloneWorkflowVerification(prior.ReadyCheck)
		nextService.Generation, nextService.CreatedAt, nextService.UpdatedAt = prior.Generation+1, prior.CreatedAt, request.At
		nextNode := cloneWorkflowNode(node)
		nextNode.Status, nextNode.Lease = workflowruntime.NodeWaiting, nil
		nextNode.Generation++
		nextNode.UpdatedAt = request.At
		if err := nextService.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := updateWorkflowServiceCAS(ctx, query, nextService, prior.Generation); err != nil {
			return err
		}
		if err := updateWorkflowNodeCAS(ctx, query, nextNode, node.Generation); err != nil {
			return err
		}
		events, err := appendWorkflowServiceEvents(ctx, query, nextService, &attempt.ID, workflowruntime.EventServiceSuspended, node.Status, nextNode.Status, request.At)
		if err != nil {
			return err
		}
		result = workflowruntime.SuspendServiceStartResult{Service: nextService, Node: nextNode, Attempt: attempt, Events: events}
		return nil
	})
	return result, writeErr
}

func (s *WorkflowStateStore) RecoverServiceStart(ctx context.Context, request workflowruntime.RecoverServiceStartRequest) (workflowruntime.SuspendServiceStartResult, error) {
	request.At = request.At.UTC()
	var result workflowruntime.SuspendServiceStartResult
	writeErr := s.write(ctx, "recover workflow service start", func(query workflowSQL) error {
		service, node, attempt, loadErr := loadWorkflowServiceAttempt(ctx, query, request.Start.Invocation, request.Start)
		if loadErr != nil {
			return loadErr
		}
		if service.Generation != request.ExpectedServiceGeneration || node.Generation != request.ExpectedNodeGeneration || attempt.Generation != request.ExpectedAttemptGeneration {
			return workflowCAS("service start recovery", request.ExpectedServiceGeneration, service.Generation)
		}
		if service.Status != workflowruntime.ServiceLaunching || node.Status != workflowruntime.NodeRunning || attempt.Status != workflowruntime.NodeRunning {
			return workflowInvalid(errors.New("service start recovery requires launching state"))
		}
		if request.At.IsZero() || request.At.Before(service.UpdatedAt) || request.At.Before(node.UpdatedAt) || request.At.Before(attempt.UpdatedAt) {
			return workflowInvalid(errors.New("service start recovery time must not regress"))
		}
		nextService, nextNode := cloneWorkflowService(service), cloneWorkflowNode(node)
		nextService.Ref, nextService.Status = cloneWorkflowExternalRef(request.Ref), workflowruntime.ServiceStarting
		nextService.Generation++
		nextService.UpdatedAt = request.At
		nextNode.Status, nextNode.Lease = workflowruntime.NodeWaiting, nil
		nextNode.Generation++
		nextNode.UpdatedAt = request.At
		if err := nextService.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := updateWorkflowServiceCAS(ctx, query, nextService, service.Generation); err != nil {
			return err
		}
		if err := updateWorkflowNodeCAS(ctx, query, nextNode, node.Generation); err != nil {
			return err
		}
		events, err := appendWorkflowServiceEvents(ctx, query, nextService, &attempt.ID, workflowruntime.EventServiceSuspended, node.Status, nextNode.Status, request.At)
		if err != nil {
			return err
		}
		result = workflowruntime.SuspendServiceStartResult{Service: nextService, Node: nextNode, Attempt: attempt, Events: events}
		return nil
	})
	return result, writeErr
}

func (s *WorkflowStateStore) ApplyServiceReady(ctx context.Context, request workflowruntime.ApplyServiceReadyRequest) (workflowruntime.ApplyServiceReadyResult, error) {
	request.At, request.ObservedAt, request.HeartbeatAt = request.At.UTC(), request.ObservedAt.UTC(), request.HeartbeatAt.UTC()
	var result workflowruntime.ApplyServiceReadyResult
	writeErr := s.write(ctx, "apply workflow service readiness", func(query workflowSQL) error {
		service, node, attempt, loadErr := loadWorkflowServiceAttempt(ctx, query, request.Start.Invocation, request.Start)
		if loadErr != nil {
			return loadErr
		}
		if service.Generation != request.ExpectedServiceGeneration || node.Generation != request.ExpectedNodeGeneration || attempt.Generation != request.ExpectedAttemptGeneration {
			return workflowCAS("service readiness", request.ExpectedServiceGeneration, service.Generation)
		}
		if service.Status != workflowruntime.ServiceStarting || node.Status != workflowruntime.NodeWaiting || attempt.Status != workflowruntime.NodeRunning || request.At.Before(service.UpdatedAt) {
			return workflowInvalid(errors.New("service readiness requires active starting state"))
		}
		if request.At.IsZero() || request.At.Before(node.UpdatedAt) || request.At.Before(attempt.UpdatedAt) || (!request.ObservedAt.IsZero() && (request.ObservedAt.Before(service.LastObservedAt) || request.ObservedAt.After(request.At))) || (!request.HeartbeatAt.IsZero() && (request.HeartbeatAt.Before(service.LastHeartbeatAt) || request.HeartbeatAt.After(request.At))) {
			return workflowInvalid(errors.New("service readiness chronology must not regress"))
		}
		if request.Ready && request.Failure != nil || !request.Ready && request.Outputs != nil {
			return workflowInvalid(errors.New("service readiness outcome is ambiguous"))
		}
		if request.Ready && request.Outputs == nil {
			return workflowInvalid(errors.New("ready service requires outputs"))
		}
		if request.Outputs != nil {
			if _, err := loadWorkflowValues(ctx, query, *request.Outputs); err != nil {
				return err
			}
		}
		nextService, nextNode, nextAttempt := cloneWorkflowService(service), cloneWorkflowNode(node), cloneWorkflowAttempt(attempt)
		nextService.Generation++
		nextService.UpdatedAt = request.At
		if !request.ObservedAt.IsZero() {
			nextService.LastObservedAt = request.ObservedAt
		}
		if request.HeartbeatAt.After(nextService.LastHeartbeatAt) {
			nextService.LastHeartbeatAt = request.HeartbeatAt
		}
		eventType := workflowruntime.EventServiceSuspended
		if request.Ready {
			nextService.Status, nextService.Outputs = workflowruntime.ServiceReady, cloneWorkflowValueRef(request.Outputs)
			nextService.ReadyAt = request.At
			if nextService.LastHeartbeatAt.IsZero() {
				nextService.LastHeartbeatAt = request.At
			}
			nextNode.Status, nextNode.Outputs, nextNode.Origin = workflowruntime.NodeSucceeded, cloneWorkflowValueRef(request.Outputs), workflowruntime.OriginExecuted
			nextAttempt.Status, nextAttempt.Outputs, nextAttempt.FinishedAt = workflowruntime.NodeSucceeded, cloneWorkflowValueRef(request.Outputs), request.At
			eventType = workflowruntime.EventServiceReady
		} else if request.Failure != nil {
			nextService.Status, nextService.Failure = workflowruntime.ServiceFailed, cloneWorkflowFailure(request.Failure)
			nextNode.Status, nextNode.Origin = workflowruntime.NodeFailed, workflowruntime.OriginExecuted
			nextAttempt.Status, nextAttempt.Failure, nextAttempt.FinishedAt = workflowruntime.NodeFailed, cloneWorkflowFailure(request.Failure), request.At
			eventType = workflowruntime.EventServiceFailed
		}
		terminal := request.Ready || request.Failure != nil
		if terminal {
			nextNode.Generation++
			nextNode.UpdatedAt = request.At
			nextAttempt.Generation++
			nextAttempt.UpdatedAt = request.At
		}
		if err := nextService.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := updateWorkflowServiceCAS(ctx, query, nextService, service.Generation); err != nil {
			return err
		}
		if terminal {
			if err := updateWorkflowNodeCAS(ctx, query, nextNode, node.Generation); err != nil {
				return err
			}
			if err := updateWorkflowAttemptCAS(ctx, query, nextAttempt, attempt.Generation); err != nil {
				return err
			}
		}
		events, err := appendWorkflowServiceEvents(ctx, query, nextService, &attempt.ID, eventType, node.Status, nextNode.Status, request.At)
		if err != nil {
			return err
		}
		result = workflowruntime.ApplyServiceReadyResult{Service: nextService, Node: nextNode, Attempt: nextAttempt, Events: events}
		return nil
	})
	return result, writeErr
}

func (s *WorkflowStateStore) ApplyServiceHeartbeat(ctx context.Context, request workflowruntime.ApplyServiceHeartbeatRequest) (workflowruntime.ServiceSnapshot, error) {
	request.At, request.ObservedAt, request.HeartbeatAt = request.At.UTC(), request.ObservedAt.UTC(), request.HeartbeatAt.UTC()
	var result workflowruntime.ServiceSnapshot
	writeErr := s.write(ctx, "apply workflow service heartbeat", func(query workflowSQL) error {
		service, loadErr := loadWorkflowService(ctx, query, request.Start)
		if loadErr != nil {
			return loadErr
		}
		if service.Generation != request.ExpectedServiceGeneration {
			return workflowCAS("service heartbeat", request.ExpectedServiceGeneration, service.Generation)
		}
		if service.Status != workflowruntime.ServiceReady || request.At.Before(service.UpdatedAt) {
			return workflowInvalid(errors.New("service heartbeat requires ready state"))
		}
		if request.At.IsZero() || (!request.ObservedAt.IsZero() && (request.ObservedAt.Before(service.LastObservedAt) || request.ObservedAt.After(request.At))) || (!request.HeartbeatAt.IsZero() && (request.HeartbeatAt.Before(service.LastHeartbeatAt) || request.HeartbeatAt.After(request.At))) {
			return workflowInvalid(errors.New("service heartbeat chronology must not regress"))
		}
		if request.Failure != nil && !request.HeartbeatAt.IsZero() {
			return workflowInvalid(errors.New("failed service heartbeat cannot report liveness"))
		}
		result = cloneWorkflowService(service)
		result.Generation++
		result.UpdatedAt = request.At
		if !request.ObservedAt.IsZero() {
			result.LastObservedAt = request.ObservedAt
		}
		if !request.HeartbeatAt.IsZero() {
			result.LastHeartbeatAt = request.HeartbeatAt
		}
		eventType := workflowruntime.EventServiceReady
		if request.Failure != nil {
			result.Status, result.Failure = workflowruntime.ServiceFailed, cloneWorkflowFailure(request.Failure)
			eventType = workflowruntime.EventServiceFailed
		}
		if err := result.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := updateWorkflowServiceCAS(ctx, query, result, service.Generation); err != nil {
			return err
		}
		_, eventErr := appendWorkflowServiceEvents(ctx, query, result, &result.Start, eventType, workflowruntime.NodeSucceeded, workflowruntime.NodeSucceeded, request.At)
		return eventErr
	})
	return result, writeErr
}

func (s *WorkflowStateStore) SuspendServiceTeardown(ctx context.Context, request workflowruntime.SuspendServiceTeardownRequest) (workflowruntime.SuspendServiceTeardownResult, error) {
	request.At = request.At.UTC()
	var result workflowruntime.SuspendServiceTeardownResult
	writeErr := s.write(ctx, "suspend workflow service teardown", func(query workflowSQL) error {
		service, serviceErr := loadWorkflowService(ctx, query, request.Start)
		if serviceErr != nil {
			return serviceErr
		}
		node, nodeErr := loadWorkflowNode(ctx, query, request.Teardown.Invocation)
		if nodeErr != nil {
			return nodeErr
		}
		attempt, attemptErr := loadWorkflowAttempt(ctx, query, request.Teardown)
		if attemptErr != nil {
			return attemptErr
		}
		if service.Generation != request.ExpectedServiceGeneration || node.Generation != request.ExpectedNodeGeneration || attempt.Generation != request.ExpectedAttemptGeneration {
			return workflowCAS("service teardown", request.ExpectedServiceGeneration, service.Generation)
		}
		if service.Status != workflowruntime.ServiceReady && service.Status != workflowruntime.ServiceStarting && service.Status != workflowruntime.ServiceFailed {
			return workflowInvalid(errors.New("service is not active for teardown"))
		}
		if node.Status != workflowruntime.NodeRunning || attempt.Status != workflowruntime.NodeRunning || request.At.Before(service.UpdatedAt) {
			return workflowInvalid(errors.New("service teardown requires running attempt"))
		}
		if request.At.IsZero() || request.At.Before(node.UpdatedAt) || request.At.Before(attempt.UpdatedAt) {
			return workflowInvalid(errors.New("service teardown time must not regress"))
		}
		if err := validateWorkflowLifecycleClaim(node, &request.Claim, request.At); err != nil {
			return err
		}
		nextService, nextNode := cloneWorkflowService(service), cloneWorkflowNode(node)
		nextService.Status, nextService.Teardown, nextService.StopRequestedAt = workflowruntime.ServiceStopping, cloneWorkflowAttemptID(&attempt.ID), request.At
		invocation := cloneWorkflowServiceInvocation(request.Invocation)
		nextService.TeardownInvocation = &invocation
		nextService.Generation++
		nextService.UpdatedAt = request.At
		nextNode.Status, nextNode.Lease = workflowruntime.NodeWaiting, nil
		nextNode.Generation++
		nextNode.UpdatedAt = request.At
		if err := nextService.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := updateWorkflowServiceCAS(ctx, query, nextService, service.Generation); err != nil {
			return err
		}
		if err := updateWorkflowNodeCAS(ctx, query, nextNode, node.Generation); err != nil {
			return err
		}
		events, err := appendWorkflowServiceEvents(ctx, query, nextService, &attempt.ID, workflowruntime.EventServiceStopping, node.Status, nextNode.Status, request.At)
		if err != nil {
			return err
		}
		result = workflowruntime.SuspendServiceTeardownResult{Service: nextService, Node: nextNode, Attempt: attempt, Events: events}
		return nil
	})
	return result, writeErr
}

func (s *WorkflowStateStore) ApplyServiceStop(ctx context.Context, request workflowruntime.ApplyServiceStopRequest) (workflowruntime.ApplyServiceStopResult, error) {
	request.At, request.ObservedAt, request.HeartbeatAt = request.At.UTC(), request.ObservedAt.UTC(), request.HeartbeatAt.UTC()
	var result workflowruntime.ApplyServiceStopResult
	writeErr := s.write(ctx, "apply workflow service stop", func(query workflowSQL) error {
		service, serviceErr := loadWorkflowService(ctx, query, request.Start)
		if serviceErr != nil {
			return serviceErr
		}
		if service.Teardown == nil {
			return workflowInvalid(errors.New("stopping service has no teardown"))
		}
		node, nodeErr := loadWorkflowNode(ctx, query, service.Teardown.Invocation)
		if nodeErr != nil {
			return nodeErr
		}
		attempt, attemptErr := loadWorkflowAttempt(ctx, query, *service.Teardown)
		if attemptErr != nil {
			return attemptErr
		}
		if service.Generation != request.ExpectedServiceGeneration || node.Generation != request.ExpectedNodeGeneration || attempt.Generation != request.ExpectedAttemptGeneration || service.Status != workflowruntime.ServiceStopping {
			return workflowCAS("service stop", request.ExpectedServiceGeneration, service.Generation)
		}
		if request.At.IsZero() || request.At.Before(service.UpdatedAt) || request.At.Before(node.UpdatedAt) || request.At.Before(attempt.UpdatedAt) || (!request.ObservedAt.IsZero() && (request.ObservedAt.Before(service.LastObservedAt) || request.ObservedAt.After(request.At))) || (!request.HeartbeatAt.IsZero() && (request.HeartbeatAt.Before(service.LastHeartbeatAt) || request.HeartbeatAt.After(request.At))) {
			return workflowInvalid(errors.New("service stop chronology must not regress"))
		}
		if request.Stopped && request.Failure != nil {
			return workflowInvalid(errors.New("service stop outcome is ambiguous"))
		}
		nextService, nextNode, nextAttempt := cloneWorkflowService(service), cloneWorkflowNode(node), cloneWorkflowAttempt(attempt)
		nextService.Generation++
		nextService.UpdatedAt = request.At
		if !request.ObservedAt.IsZero() {
			nextService.LastObservedAt = request.ObservedAt
		}
		if !request.HeartbeatAt.IsZero() {
			nextService.LastHeartbeatAt = request.HeartbeatAt
		}
		eventType := workflowruntime.EventServiceStopping
		if request.Failure != nil || request.Stopped {
			nextAttempt.FinishedAt, nextAttempt.UpdatedAt = request.At, request.At
			nextAttempt.Generation++
			nextNode.Generation++
			nextNode.UpdatedAt = request.At
			if request.Failure != nil {
				nextService.Status, nextService.Failure = workflowruntime.ServiceFailed, cloneWorkflowFailure(request.Failure)
				nextAttempt.Status, nextAttempt.Failure = workflowruntime.NodeFailed, cloneWorkflowFailure(request.Failure)
				nextNode.Status, nextNode.Origin = workflowruntime.NodeFailed, workflowruntime.OriginExecuted
				eventType = workflowruntime.EventServiceFailed
			} else {
				nextService.Status = workflowruntime.ServiceStopped
				nextAttempt.Status = workflowruntime.NodeSucceeded
				nextNode.Status, nextNode.Outputs, nextNode.Origin = workflowruntime.NodeSucceeded, nil, workflowruntime.OriginExecuted
				eventType = workflowruntime.EventServiceStopped
			}
		}
		if err := nextService.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := updateWorkflowServiceCAS(ctx, query, nextService, service.Generation); err != nil {
			return err
		}
		if request.Failure != nil || request.Stopped {
			if err := updateWorkflowNodeCAS(ctx, query, nextNode, node.Generation); err != nil {
				return err
			}
			if err := updateWorkflowAttemptCAS(ctx, query, nextAttempt, attempt.Generation); err != nil {
				return err
			}
		}
		events, err := appendWorkflowServiceEvents(ctx, query, nextService, service.Teardown, eventType, node.Status, nextNode.Status, request.At)
		if err != nil {
			return err
		}
		result = workflowruntime.ApplyServiceStopResult{Service: nextService, Node: nextNode, Attempt: nextAttempt, Events: events}
		return nil
	})
	return result, writeErr
}

func (s *WorkflowStateStore) RecoverServices(ctx context.Context, query workflowruntime.ServiceQuery) ([]workflowruntime.ServiceSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	if query.Limit < 0 {
		return nil, workflowInvalid(errors.New("service recovery limit must not be negative"))
	}
	arguments := []any{}
	statement := `SELECT snapshot_json FROM workflow_services WHERE status IN ('launching','starting','ready','stopping')`
	if query.RunID != "" {
		statement += ` AND run_id = ?`
		arguments = append(arguments, query.RunID)
	}
	statement += ` ORDER BY updated_at, run_id, node_id, iteration`
	if query.Limit > 0 {
		statement += ` LIMIT ?`
		arguments = append(arguments, query.Limit)
	}
	rows, err := s.db.QueryContext(ctx, statement, arguments...)
	if err != nil {
		return nil, fmt.Errorf("recover workflow services: %w", err)
	}
	defer func() { _ = rows.Close() }()
	result := make([]workflowruntime.ServiceSnapshot, 0)
	for rows.Next() {
		var encoded string
		if err := rows.Scan(&encoded); err != nil {
			return nil, err
		}
		var service workflowruntime.ServiceSnapshot
		if err := decodeWorkflowJSON("service", encoded, &service); err != nil {
			return nil, err
		}
		if err := service.Validate(); err != nil {
			return nil, workflowInvalid(err)
		}
		result = append(result, service)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func loadWorkflowService(ctx context.Context, query workflowSQL, start workflowruntime.NodeInvocationID) (workflowruntime.ServiceSnapshot, error) {
	var encoded string
	if err := query.QueryRowContext(ctx, `SELECT snapshot_json FROM workflow_services WHERE run_id = ? AND node_id = ? AND iteration = ?`, start.RunID, start.NodeID, start.Iteration).Scan(&encoded); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workflowruntime.ServiceSnapshot{}, fmt.Errorf("%w: service", workflowruntime.ErrNotFound)
		}
		return workflowruntime.ServiceSnapshot{}, fmt.Errorf("load workflow service: %w", err)
	}
	var service workflowruntime.ServiceSnapshot
	if err := decodeWorkflowJSON("service", encoded, &service); err != nil {
		return workflowruntime.ServiceSnapshot{}, err
	}
	if service.Start.Invocation != start {
		return workflowruntime.ServiceSnapshot{}, workflowInvalid(errors.New("persisted service identity diverges"))
	}
	if err := service.Validate(); err != nil {
		return workflowruntime.ServiceSnapshot{}, workflowInvalid(err)
	}
	return service, nil
}

func loadWorkflowServiceAttempt(ctx context.Context, query workflowSQL, start workflowruntime.NodeInvocationID, attemptID workflowruntime.AttemptID) (workflowruntime.ServiceSnapshot, workflowruntime.NodeInvocationSnapshot, workflowruntime.AttemptSnapshot, error) {
	service, err := loadWorkflowService(ctx, query, start)
	if err != nil {
		return workflowruntime.ServiceSnapshot{}, workflowruntime.NodeInvocationSnapshot{}, workflowruntime.AttemptSnapshot{}, err
	}
	node, err := loadWorkflowNode(ctx, query, start)
	if err != nil {
		return workflowruntime.ServiceSnapshot{}, workflowruntime.NodeInvocationSnapshot{}, workflowruntime.AttemptSnapshot{}, err
	}
	attempt, err := loadWorkflowAttempt(ctx, query, attemptID)
	return service, node, attempt, err
}

func insertWorkflowService(ctx context.Context, query workflowSQL, service workflowruntime.ServiceSnapshot) error {
	encoded, err := encodeWorkflowJSON(service)
	if err != nil {
		return err
	}
	_, err = query.ExecContext(ctx, `INSERT INTO workflow_services(run_id,node_id,iteration,status,generation,updated_at,snapshot_json) VALUES (?,?,?,?,?,?,?)`, service.Start.Invocation.RunID, service.Start.Invocation.NodeID, service.Start.Invocation.Iteration, service.Status, service.Generation, workflowTime(service.UpdatedAt), encoded)
	if isSQLiteConstraint(err) {
		return fmt.Errorf("%w: service", workflowruntime.ErrAlreadyExists)
	}
	return err
}

func updateWorkflowServiceCAS(ctx context.Context, query workflowSQL, service workflowruntime.ServiceSnapshot, expected uint64) error {
	encoded, err := encodeWorkflowJSON(service)
	if err != nil {
		return err
	}
	result, err := query.ExecContext(ctx, `UPDATE workflow_services SET status=?, generation=?, updated_at=?, snapshot_json=? WHERE run_id=? AND node_id=? AND iteration=? AND generation=?`, service.Status, service.Generation, workflowTime(service.UpdatedAt), encoded, service.Start.Invocation.RunID, service.Start.Invocation.NodeID, service.Start.Invocation.Iteration, expected)
	if err != nil {
		return err
	}
	return expectOneWorkflowRow(result, "service", expected, service.Generation-1)
}

func appendWorkflowServiceEvents(ctx context.Context, query workflowSQL, service workflowruntime.ServiceSnapshot, attempt *workflowruntime.AttemptID, eventType string, from, to workflowruntime.NodeStatus, at time.Time) ([]workflowruntime.Event, error) {
	invocation := service.Start.Invocation
	if attempt != nil {
		invocation = attempt.Invocation
	}
	attributes := map[string]string{
		"service_status": string(service.Status), "from_status": string(from), "to_status": string(to),
		"service_start_node": service.Start.Invocation.NodeID,
	}
	if service.Ref.Kind != "" {
		attributes["service_kind"] = service.Ref.Kind
	}
	requests := []workflowruntime.AppendEventRequest{{
		RunID: invocation.RunID, Invocation: &invocation, Attempt: cloneWorkflowAttemptID(attempt),
		Type: eventType, OccurredAt: at, Attributes: attributes,
		Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	}}
	if from != to {
		requests = append(requests, workflowruntime.AppendEventRequest{
			RunID: invocation.RunID, Invocation: &invocation, Attempt: cloneWorkflowAttemptID(attempt),
			Type: workflowruntime.EventNodeStatusChanged, OccurredAt: at,
			Attributes: map[string]string{"from_status": string(from), "to_status": string(to), "resource": "service"},
			Redaction:  values.RedactionPrivate, Retention: values.RetentionRun,
		})
	}
	events := make([]workflowruntime.Event, 0, len(requests))
	for _, request := range requests {
		event, err := appendWorkflowEvent(ctx, query, request)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func cloneWorkflowService(service workflowruntime.ServiceSnapshot) workflowruntime.ServiceSnapshot {
	encoded, err := encodeWorkflowJSON(service)
	if err != nil {
		panic("clone validated workflow service: " + err.Error())
	}
	var cloned workflowruntime.ServiceSnapshot
	if err := decodeWorkflowJSON("cloned service", encoded, &cloned); err != nil {
		panic("clone validated workflow service: " + err.Error())
	}
	return cloned
}

func cloneWorkflowExternalRef(ref stepkind.ExternalOperationRef) stepkind.ExternalOperationRef {
	ref.Metadata = cloneWorkflowStringMap(ref.Metadata)
	return ref
}

func cloneWorkflowVerification(value *graph.VerificationSpec) *graph.VerificationSpec {
	if value == nil {
		return nil
	}
	encoded, err := encodeWorkflowJSON(value)
	if err != nil {
		panic("clone workflow service verification: " + err.Error())
	}
	var cloned graph.VerificationSpec
	if err := decodeWorkflowJSON("service verification", encoded, &cloned); err != nil {
		panic("clone workflow service verification: " + err.Error())
	}
	return &cloned
}

func cloneWorkflowServiceInvocation(value stepkind.Invocation) stepkind.Invocation {
	encoded, err := encodeWorkflowJSON(value)
	if err != nil {
		panic("clone workflow service invocation: " + err.Error())
	}
	var cloned stepkind.Invocation
	if err := decodeWorkflowJSON("service invocation", encoded, &cloned); err != nil {
		panic("clone workflow service invocation: " + err.Error())
	}
	return cloned
}
