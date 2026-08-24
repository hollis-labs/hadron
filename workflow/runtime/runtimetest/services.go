package runtimetest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

var _ workflowruntime.ServiceStore = (*Store)(nil)

func (s *Store) LoadService(ctx context.Context, start workflowruntime.NodeInvocationID) (workflowruntime.ServiceSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ServiceSnapshot{}, err
	}
	if err := start.Validate(); err != nil {
		return workflowruntime.ServiceSnapshot{}, invalid(err)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	service, ok := s.services[start]
	if !ok {
		return workflowruntime.ServiceSnapshot{}, fmt.Errorf("%w: service", workflowruntime.ErrNotFound)
	}
	return cloneService(service), nil
}

func (s *Store) PrepareServiceStart(ctx context.Context, request workflowruntime.PrepareServiceStartRequest) (workflowruntime.ServiceSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ServiceSnapshot{}, err
	}
	request.At = request.At.UTC()
	if request.Service.Status != workflowruntime.ServiceLaunching || request.Service.Generation != 0 || request.At.IsZero() || request.ExpectedNodeGeneration == 0 || request.ExpectedAttemptGeneration == 0 {
		return workflowruntime.ServiceSnapshot{}, invalid(errors.New("service start intent is invalid"))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, exists := s.services[request.Service.Start.Invocation]; exists {
		candidate := cloneService(request.Service)
		candidate.Generation, candidate.CreatedAt, candidate.UpdatedAt = prior.Generation, prior.CreatedAt, prior.UpdatedAt
		if prior.Status != workflowruntime.ServiceLaunching || !reflect.DeepEqual(prior, candidate) {
			return workflowruntime.ServiceSnapshot{}, fmt.Errorf("%w: divergent service start intent", workflowruntime.ErrIdempotencyConflict)
		}
		return cloneService(prior), nil
	}
	node, nodeOK := s.nodes[request.Service.Start.Invocation]
	attempt, attemptOK := s.attempts[request.Service.Start]
	if !nodeOK || !attemptOK {
		return workflowruntime.ServiceSnapshot{}, fmt.Errorf("%w: service start lifecycle", workflowruntime.ErrNotFound)
	}
	if node.Generation != request.ExpectedNodeGeneration || attempt.Generation != request.ExpectedAttemptGeneration || node.Status != workflowruntime.NodeRunning || attempt.Status != workflowruntime.NodeRunning {
		return workflowruntime.ServiceSnapshot{}, casMismatch("service start intent", request.ExpectedNodeGeneration, node.Generation)
	}
	service := cloneService(request.Service)
	service.Generation = 1
	service.CreatedAt, service.UpdatedAt = request.At, request.At
	if err := service.Validate(); err != nil {
		return workflowruntime.ServiceSnapshot{}, invalid(err)
	}
	s.services[node.ID] = cloneService(service)
	return cloneService(service), nil
}

func (s *Store) SuspendServiceStart(ctx context.Context, request workflowruntime.SuspendServiceStartRequest) (workflowruntime.SuspendServiceStartResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.SuspendServiceStartResult{}, err
	}
	request.At = request.At.UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if request.ExpectedNodeGeneration == 0 || request.ExpectedAttemptGeneration == 0 || request.At.IsZero() || request.Service.Status != workflowruntime.ServiceStarting || request.Service.Generation != 0 {
		return workflowruntime.SuspendServiceStartResult{}, invalid(errors.New("new service suspension is invalid"))
	}
	node, ok := s.nodes[request.Service.Start.Invocation]
	if !ok {
		return workflowruntime.SuspendServiceStartResult{}, fmt.Errorf("%w: node", workflowruntime.ErrNotFound)
	}
	attempt, ok := s.attempts[request.Service.Start]
	if !ok {
		return workflowruntime.SuspendServiceStartResult{}, fmt.Errorf("%w: attempt", workflowruntime.ErrNotFound)
	}
	prior, exists := s.services[request.Service.Start.Invocation]
	if !exists || prior.Status != workflowruntime.ServiceLaunching || prior.Start != request.Service.Start || prior.Invocation.IdempotencyKey != request.Service.Invocation.IdempotencyKey {
		return workflowruntime.SuspendServiceStartResult{}, invalid(errors.New("service suspension requires matching durable launch intent"))
	}
	if node.Generation != request.ExpectedNodeGeneration || attempt.Generation != request.ExpectedAttemptGeneration {
		return workflowruntime.SuspendServiceStartResult{}, casMismatch("service suspension", request.ExpectedNodeGeneration, node.Generation)
	}
	if node.Status != workflowruntime.NodeRunning || attempt.Status != workflowruntime.NodeRunning || !attempt.FinishedAt.IsZero() || node.LatestAttempt != attempt.ID.Number {
		return workflowruntime.SuspendServiceStartResult{}, invalid(errors.New("service suspension requires matching running attempt"))
	}
	if err := validateLifecycleClaim(node, &request.Claim, request.At); err != nil {
		return workflowruntime.SuspendServiceStartResult{}, err
	}
	service := cloneService(request.Service)
	service.Generation = prior.Generation + 1
	service.CreatedAt, service.UpdatedAt = prior.CreatedAt, request.At
	nextNode := cloneNode(node)
	nextNode.Status, nextNode.Lease = workflowruntime.NodeWaiting, nil
	nextNode.Generation++
	nextNode.UpdatedAt = request.At
	if err := service.Validate(); err != nil {
		return workflowruntime.SuspendServiceStartResult{}, invalid(err)
	}
	if err := nextNode.Validate(); err != nil {
		return workflowruntime.SuspendServiceStartResult{}, invalid(err)
	}
	events, err := s.appendEventRequestsLocked(serviceEventRequests(service, &attempt.ID, workflowruntime.EventServiceSuspended, node.Status, nextNode.Status, request.At))
	if err != nil {
		return workflowruntime.SuspendServiceStartResult{}, err
	}
	s.releaseSchedulerResourcesLocked(node.ID)
	s.services[node.ID] = cloneService(service)
	s.nodes[node.ID] = cloneNode(nextNode)
	return workflowruntime.SuspendServiceStartResult{Service: cloneService(service), Node: cloneNode(nextNode), Attempt: cloneAttempt(attempt), Events: cloneEvents(events)}, nil
}

func (s *Store) RecoverServiceStart(ctx context.Context, request workflowruntime.RecoverServiceStartRequest) (workflowruntime.SuspendServiceStartResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.SuspendServiceStartResult{}, err
	}
	request.At = request.At.UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	service, node, attempt, loadErr := s.loadActiveServiceAttemptLocked(request.Start.Invocation, request.Start)
	if loadErr != nil {
		return workflowruntime.SuspendServiceStartResult{}, loadErr
	}
	if service.Generation != request.ExpectedServiceGeneration || node.Generation != request.ExpectedNodeGeneration || attempt.Generation != request.ExpectedAttemptGeneration || service.Status != workflowruntime.ServiceLaunching || node.Status != workflowruntime.NodeRunning || attempt.Status != workflowruntime.NodeRunning {
		return workflowruntime.SuspendServiceStartResult{}, casMismatch("service start recovery", request.ExpectedServiceGeneration, service.Generation)
	}
	if request.At.IsZero() || request.At.Before(service.UpdatedAt) || request.At.Before(node.UpdatedAt) || request.At.Before(attempt.UpdatedAt) {
		return workflowruntime.SuspendServiceStartResult{}, invalid(errors.New("service start recovery time must not regress"))
	}
	nextService, nextNode := cloneService(service), cloneNode(node)
	nextService.Ref, nextService.Status = request.Ref, workflowruntime.ServiceStarting
	nextService.Generation++
	nextService.UpdatedAt = request.At
	nextNode.Status, nextNode.Lease = workflowruntime.NodeWaiting, nil
	nextNode.Generation++
	nextNode.UpdatedAt = request.At
	if err := nextService.Validate(); err != nil {
		return workflowruntime.SuspendServiceStartResult{}, invalid(err)
	}
	events, err := s.appendEventRequestsLocked(serviceEventRequests(nextService, &attempt.ID, workflowruntime.EventServiceSuspended, node.Status, nextNode.Status, request.At))
	if err != nil {
		return workflowruntime.SuspendServiceStartResult{}, err
	}
	s.releaseSchedulerResourcesLocked(node.ID)
	s.services[node.ID], s.nodes[node.ID] = cloneService(nextService), cloneNode(nextNode)
	return workflowruntime.SuspendServiceStartResult{Service: cloneService(nextService), Node: cloneNode(nextNode), Attempt: cloneAttempt(attempt), Events: cloneEvents(events)}, nil
}

func (s *Store) ApplyServiceReady(ctx context.Context, request workflowruntime.ApplyServiceReadyRequest) (workflowruntime.ApplyServiceReadyResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ApplyServiceReadyResult{}, err
	}
	request.At, request.ObservedAt, request.HeartbeatAt = request.At.UTC(), request.ObservedAt.UTC(), request.HeartbeatAt.UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	service, node, attempt, loadErr := s.loadActiveServiceAttemptLocked(request.Start.Invocation, request.Start)
	if loadErr != nil {
		return workflowruntime.ApplyServiceReadyResult{}, loadErr
	}
	if service.Generation != request.ExpectedServiceGeneration || node.Generation != request.ExpectedNodeGeneration || attempt.Generation != request.ExpectedAttemptGeneration {
		return workflowruntime.ApplyServiceReadyResult{}, casMismatch("service readiness", request.ExpectedServiceGeneration, service.Generation)
	}
	if service.Status != workflowruntime.ServiceStarting || node.Status != workflowruntime.NodeWaiting || attempt.Status != workflowruntime.NodeRunning || request.At.Before(service.UpdatedAt) {
		return workflowruntime.ApplyServiceReadyResult{}, invalid(errors.New("service readiness requires active starting state"))
	}
	if request.At.IsZero() || request.At.Before(node.UpdatedAt) || request.At.Before(attempt.UpdatedAt) || (!request.ObservedAt.IsZero() && (request.ObservedAt.Before(service.LastObservedAt) || request.ObservedAt.After(request.At))) || (!request.HeartbeatAt.IsZero() && (request.HeartbeatAt.Before(service.LastHeartbeatAt) || request.HeartbeatAt.After(request.At))) {
		return workflowruntime.ApplyServiceReadyResult{}, invalid(errors.New("service readiness chronology must not regress"))
	}
	if request.Ready && request.Failure != nil || !request.Ready && request.Outputs != nil {
		return workflowruntime.ApplyServiceReadyResult{}, invalid(errors.New("service readiness outcome is ambiguous"))
	}
	nextService, nextNode, nextAttempt := cloneService(service), cloneNode(node), cloneAttempt(attempt)
	nextService.Generation++
	nextService.UpdatedAt = request.At
	if !request.ObservedAt.IsZero() {
		nextService.LastObservedAt = request.ObservedAt
	}
	if !request.HeartbeatAt.IsZero() {
		nextService.LastHeartbeatAt = request.HeartbeatAt
	}
	eventType := workflowruntime.EventServiceSuspended
	if request.Failure != nil {
		nextService.Status, nextService.Failure = workflowruntime.ServiceFailed, cloneFailure(request.Failure)
		nextAttempt.Status, nextAttempt.Failure = workflowruntime.NodeFailed, cloneFailure(request.Failure)
		nextNode.Status, nextNode.Origin = workflowruntime.NodeFailed, workflowruntime.OriginExecuted
		eventType = workflowruntime.EventServiceFailed
	} else if request.Ready {
		if request.Outputs == nil {
			return workflowruntime.ApplyServiceReadyResult{}, invalid(errors.New("ready service requires outputs"))
		}
		nextService.Status, nextService.Outputs = workflowruntime.ServiceReady, cloneValueSetRef(request.Outputs)
		nextService.ReadyAt, nextService.LastHeartbeatAt = request.At, request.At
		nextAttempt.Status, nextAttempt.Outputs = workflowruntime.NodeSucceeded, cloneValueSetRef(request.Outputs)
		nextNode.Status, nextNode.Outputs, nextNode.Origin = workflowruntime.NodeSucceeded, cloneValueSetRef(request.Outputs), workflowruntime.OriginExecuted
		eventType = workflowruntime.EventServiceReady
	}
	if nextService.Status != workflowruntime.ServiceStarting {
		nextAttempt.FinishedAt, nextAttempt.UpdatedAt = request.At, request.At
		nextAttempt.Generation++
		nextNode.Generation++
		nextNode.UpdatedAt = request.At
	}
	if err := nextService.Validate(); err != nil {
		return workflowruntime.ApplyServiceReadyResult{}, invalid(err)
	}
	events, err := s.appendEventRequestsLocked(serviceEventRequests(nextService, &attempt.ID, eventType, node.Status, nextNode.Status, request.At))
	if err != nil {
		return workflowruntime.ApplyServiceReadyResult{}, err
	}
	s.services[nextService.Start.Invocation] = cloneService(nextService)
	if nextService.Status != workflowruntime.ServiceStarting {
		s.nodes[nextNode.ID], s.attempts[nextAttempt.ID] = cloneNode(nextNode), cloneAttempt(nextAttempt)
	}
	return workflowruntime.ApplyServiceReadyResult{Service: cloneService(nextService), Node: cloneNode(nextNode), Attempt: cloneAttempt(nextAttempt), Events: cloneEvents(events)}, nil
}

func (s *Store) ApplyServiceHeartbeat(ctx context.Context, request workflowruntime.ApplyServiceHeartbeatRequest) (workflowruntime.ServiceSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ServiceSnapshot{}, err
	}
	request.At, request.ObservedAt, request.HeartbeatAt = request.At.UTC(), request.ObservedAt.UTC(), request.HeartbeatAt.UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	service, ok := s.services[request.Start]
	if !ok {
		return workflowruntime.ServiceSnapshot{}, fmt.Errorf("%w: ready service", workflowruntime.ErrNotFound)
	}
	if service.Generation != request.ExpectedServiceGeneration {
		return workflowruntime.ServiceSnapshot{}, casMismatch("service heartbeat", request.ExpectedServiceGeneration, service.Generation)
	}
	if service.Status != workflowruntime.ServiceReady || request.At.Before(service.UpdatedAt) {
		return workflowruntime.ServiceSnapshot{}, invalid(errors.New("service heartbeat requires ready state"))
	}
	if request.At.IsZero() || (!request.ObservedAt.IsZero() && (request.ObservedAt.Before(service.LastObservedAt) || request.ObservedAt.After(request.At))) || (!request.HeartbeatAt.IsZero() && (request.HeartbeatAt.Before(service.LastHeartbeatAt) || request.HeartbeatAt.After(request.At))) {
		return workflowruntime.ServiceSnapshot{}, invalid(errors.New("service heartbeat chronology must not regress"))
	}
	if request.Failure != nil && !request.HeartbeatAt.IsZero() {
		return workflowruntime.ServiceSnapshot{}, invalid(errors.New("failed service heartbeat cannot report liveness"))
	}
	next := cloneService(service)
	next.Generation++
	next.UpdatedAt = request.At
	if !request.ObservedAt.IsZero() {
		next.LastObservedAt = request.ObservedAt
	}
	if !request.HeartbeatAt.IsZero() {
		next.LastHeartbeatAt = request.HeartbeatAt
	}
	eventType := workflowruntime.EventServiceReady
	if request.Failure != nil {
		next.Status, next.Failure = workflowruntime.ServiceFailed, cloneFailure(request.Failure)
		eventType = workflowruntime.EventServiceFailed
	}
	if err := next.Validate(); err != nil {
		return workflowruntime.ServiceSnapshot{}, invalid(err)
	}
	events, err := s.appendEventRequestsLocked(serviceEventRequests(next, &next.Start, eventType, workflowruntime.NodeSucceeded, workflowruntime.NodeSucceeded, request.At))
	if err != nil {
		return workflowruntime.ServiceSnapshot{}, err
	}
	_ = events
	s.services[request.Start] = cloneService(next)
	return cloneService(next), nil
}

func (s *Store) SuspendServiceTeardown(ctx context.Context, request workflowruntime.SuspendServiceTeardownRequest) (workflowruntime.SuspendServiceTeardownResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.SuspendServiceTeardownResult{}, err
	}
	request.At = request.At.UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	service, ok := s.services[request.Start]
	if !ok {
		return workflowruntime.SuspendServiceTeardownResult{}, fmt.Errorf("%w: service", workflowruntime.ErrNotFound)
	}
	node, ok := s.nodes[request.Teardown.Invocation]
	if !ok {
		return workflowruntime.SuspendServiceTeardownResult{}, fmt.Errorf("%w: teardown node", workflowruntime.ErrNotFound)
	}
	attempt, ok := s.attempts[request.Teardown]
	if !ok {
		return workflowruntime.SuspendServiceTeardownResult{}, fmt.Errorf("%w: teardown attempt", workflowruntime.ErrNotFound)
	}
	if service.Generation != request.ExpectedServiceGeneration || node.Generation != request.ExpectedNodeGeneration || attempt.Generation != request.ExpectedAttemptGeneration {
		return workflowruntime.SuspendServiceTeardownResult{}, casMismatch("service teardown", request.ExpectedServiceGeneration, service.Generation)
	}
	if service.Status != workflowruntime.ServiceReady && service.Status != workflowruntime.ServiceStarting && service.Status != workflowruntime.ServiceFailed {
		return workflowruntime.SuspendServiceTeardownResult{}, invalid(errors.New("service is not active for teardown"))
	}
	if node.Status != workflowruntime.NodeRunning || attempt.Status != workflowruntime.NodeRunning || request.At.Before(service.UpdatedAt) {
		return workflowruntime.SuspendServiceTeardownResult{}, invalid(errors.New("service teardown requires running attempt"))
	}
	if request.At.IsZero() || request.At.Before(node.UpdatedAt) || request.At.Before(attempt.UpdatedAt) {
		return workflowruntime.SuspendServiceTeardownResult{}, invalid(errors.New("service teardown time must not regress"))
	}
	if err := validateLifecycleClaim(node, &request.Claim, request.At); err != nil {
		return workflowruntime.SuspendServiceTeardownResult{}, err
	}
	nextService, nextNode := cloneService(service), cloneNode(node)
	nextService.Status, nextService.Teardown, nextService.StopRequestedAt = workflowruntime.ServiceStopping, cloneAttemptID(&attempt.ID), request.At
	invocation := cloneServiceInvocation(request.Invocation)
	nextService.TeardownInvocation = &invocation
	nextService.Generation++
	nextService.UpdatedAt = request.At
	nextNode.Status, nextNode.Lease = workflowruntime.NodeWaiting, nil
	nextNode.Generation++
	nextNode.UpdatedAt = request.At
	if err := nextService.Validate(); err != nil {
		return workflowruntime.SuspendServiceTeardownResult{}, invalid(err)
	}
	events, err := s.appendEventRequestsLocked(serviceEventRequests(nextService, &attempt.ID, workflowruntime.EventServiceStopping, node.Status, nextNode.Status, request.At))
	if err != nil {
		return workflowruntime.SuspendServiceTeardownResult{}, err
	}
	s.releaseSchedulerResourcesLocked(node.ID)
	s.services[request.Start], s.nodes[node.ID] = cloneService(nextService), cloneNode(nextNode)
	return workflowruntime.SuspendServiceTeardownResult{Service: cloneService(nextService), Node: cloneNode(nextNode), Attempt: cloneAttempt(attempt), Events: cloneEvents(events)}, nil
}

func (s *Store) ApplyServiceStop(ctx context.Context, request workflowruntime.ApplyServiceStopRequest) (workflowruntime.ApplyServiceStopResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ApplyServiceStopResult{}, err
	}
	request.At, request.ObservedAt, request.HeartbeatAt = request.At.UTC(), request.ObservedAt.UTC(), request.HeartbeatAt.UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	service, ok := s.services[request.Start]
	if !ok || service.Teardown == nil {
		return workflowruntime.ApplyServiceStopResult{}, fmt.Errorf("%w: stopping service", workflowruntime.ErrNotFound)
	}
	node, ok := s.nodes[service.Teardown.Invocation]
	if !ok {
		return workflowruntime.ApplyServiceStopResult{}, fmt.Errorf("%w: teardown node", workflowruntime.ErrNotFound)
	}
	attempt, ok := s.attempts[*service.Teardown]
	if !ok {
		return workflowruntime.ApplyServiceStopResult{}, fmt.Errorf("%w: teardown attempt", workflowruntime.ErrNotFound)
	}
	if service.Generation != request.ExpectedServiceGeneration || node.Generation != request.ExpectedNodeGeneration || attempt.Generation != request.ExpectedAttemptGeneration || service.Status != workflowruntime.ServiceStopping {
		return workflowruntime.ApplyServiceStopResult{}, casMismatch("service stop", request.ExpectedServiceGeneration, service.Generation)
	}
	if request.At.IsZero() || request.At.Before(service.UpdatedAt) || request.At.Before(node.UpdatedAt) || request.At.Before(attempt.UpdatedAt) || (!request.ObservedAt.IsZero() && (request.ObservedAt.Before(service.LastObservedAt) || request.ObservedAt.After(request.At))) || (!request.HeartbeatAt.IsZero() && (request.HeartbeatAt.Before(service.LastHeartbeatAt) || request.HeartbeatAt.After(request.At))) {
		return workflowruntime.ApplyServiceStopResult{}, invalid(errors.New("service stop chronology must not regress"))
	}
	if request.Stopped && request.Failure != nil {
		return workflowruntime.ApplyServiceStopResult{}, invalid(errors.New("service stop outcome is ambiguous"))
	}
	nextService, nextNode, nextAttempt := cloneService(service), cloneNode(node), cloneAttempt(attempt)
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
			nextService.Status, nextService.Failure = workflowruntime.ServiceFailed, cloneFailure(request.Failure)
			nextAttempt.Status, nextAttempt.Failure = workflowruntime.NodeFailed, cloneFailure(request.Failure)
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
		return workflowruntime.ApplyServiceStopResult{}, invalid(err)
	}
	events, err := s.appendEventRequestsLocked(serviceEventRequests(nextService, service.Teardown, eventType, node.Status, nextNode.Status, request.At))
	if err != nil {
		return workflowruntime.ApplyServiceStopResult{}, err
	}
	s.services[request.Start] = cloneService(nextService)
	if request.Failure != nil || request.Stopped {
		s.nodes[nextNode.ID], s.attempts[nextAttempt.ID] = cloneNode(nextNode), cloneAttempt(nextAttempt)
	}
	return workflowruntime.ApplyServiceStopResult{Service: cloneService(nextService), Node: cloneNode(nextNode), Attempt: cloneAttempt(nextAttempt), Events: cloneEvents(events)}, nil
}

func (s *Store) RecoverServices(ctx context.Context, query workflowruntime.ServiceQuery) ([]workflowruntime.ServiceSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if query.Limit < 0 {
		return nil, invalid(errors.New("service recovery limit must not be negative"))
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]workflowruntime.ServiceSnapshot, 0)
	for _, service := range s.services {
		if (service.Status == workflowruntime.ServiceLaunching || service.Status == workflowruntime.ServiceStarting || service.Status == workflowruntime.ServiceReady || service.Status == workflowruntime.ServiceStopping) && (query.RunID == "" || service.Start.Invocation.RunID == query.RunID) {
			result = append(result, cloneService(service))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].UpdatedAt.Equal(result[j].UpdatedAt) {
			return result[i].UpdatedAt.Before(result[j].UpdatedAt)
		}
		return fmt.Sprint(result[i].Start) < fmt.Sprint(result[j].Start)
	})
	if query.Limit > 0 && len(result) > query.Limit {
		result = result[:query.Limit]
	}
	return result, nil
}

func (s *Store) loadActiveServiceAttemptLocked(start workflowruntime.NodeInvocationID, attemptID workflowruntime.AttemptID) (workflowruntime.ServiceSnapshot, workflowruntime.NodeInvocationSnapshot, workflowruntime.AttemptSnapshot, error) {
	service, ok := s.services[start]
	if !ok {
		return workflowruntime.ServiceSnapshot{}, workflowruntime.NodeInvocationSnapshot{}, workflowruntime.AttemptSnapshot{}, fmt.Errorf("%w: service", workflowruntime.ErrNotFound)
	}
	node, ok := s.nodes[start]
	if !ok {
		return workflowruntime.ServiceSnapshot{}, workflowruntime.NodeInvocationSnapshot{}, workflowruntime.AttemptSnapshot{}, fmt.Errorf("%w: node", workflowruntime.ErrNotFound)
	}
	attempt, ok := s.attempts[attemptID]
	if !ok {
		return workflowruntime.ServiceSnapshot{}, workflowruntime.NodeInvocationSnapshot{}, workflowruntime.AttemptSnapshot{}, fmt.Errorf("%w: attempt", workflowruntime.ErrNotFound)
	}
	return service, node, attempt, nil
}

func serviceEventRequests(service workflowruntime.ServiceSnapshot, attempt *workflowruntime.AttemptID, eventType string, from, to workflowruntime.NodeStatus, at time.Time) []workflowruntime.AppendEventRequest {
	invocation := service.Start.Invocation
	if attempt != nil {
		invocation = attempt.Invocation
	}
	attributes := map[string]string{"service_status": string(service.Status), "from_status": string(from), "to_status": string(to), "service_kind": service.Ref.Kind}
	attributes["service_start_node"] = service.Start.Invocation.NodeID
	return []workflowruntime.AppendEventRequest{{RunID: invocation.RunID, Invocation: &invocation, Attempt: cloneAttemptID(attempt), Type: eventType, OccurredAt: at, Attributes: attributes, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}}
}

func cloneService(service workflowruntime.ServiceSnapshot) workflowruntime.ServiceSnapshot {
	encoded, err := json.Marshal(service)
	if err != nil {
		panic("clone validated service: " + err.Error())
	}
	var cloned workflowruntime.ServiceSnapshot
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&cloned); err != nil {
		panic("clone validated service: " + err.Error())
	}
	return cloned
}

func cloneServiceInvocation(invocation stepkind.Invocation) stepkind.Invocation {
	encoded, err := json.Marshal(invocation)
	if err != nil {
		panic("clone service invocation: " + err.Error())
	}
	var cloned stepkind.Invocation
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&cloned); err != nil {
		panic("clone service invocation: " + err.Error())
	}
	return cloned
}
