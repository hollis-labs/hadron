package runtimetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

var _ workflowruntime.WaitStore = (*Store)(nil)

// SuspendNodeWait atomically materializes an open wait, changes the running
// node to waiting, releases its lease, and appends both durable facts.
func (s *Store) SuspendNodeWait(ctx context.Context, request workflowruntime.SuspendNodeWaitRequest) (workflowruntime.SuspendWaitResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.SuspendWaitResult{}, err
	}
	request.At = request.At.UTC()
	request.Wait.CreatedAt, request.Wait.UpdatedAt = request.At, request.At
	if !request.Wait.Deadline.IsZero() {
		request.Wait.Deadline = request.Wait.Deadline.UTC()
	}
	if err := request.Validate(); err != nil {
		return workflowruntime.SuspendWaitResult{}, invalid(err)
	}
	if !request.Wait.Deadline.IsZero() && request.Wait.Deadline.Before(request.At) {
		return workflowruntime.SuspendWaitResult{}, invalid(errors.New("wait deadline must not precede suspension"))
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, ok := s.suspends[request.Wait.Ref.ID]; ok {
		if equalJSON(prior.request, request) {
			result := cloneSuspendResult(prior.result)
			result.Outcome = workflowruntime.IdempotencyReplayed
			return result, nil
		}
		return workflowruntime.SuspendWaitResult{}, idempotencyConflict("suspend wait", string(request.Wait.Ref.ID))
	}
	if _, exists := s.waits[request.Wait.Ref.ID]; exists {
		return workflowruntime.SuspendWaitResult{}, fmt.Errorf("%w: wait %q", workflowruntime.ErrAlreadyExists, request.Wait.Ref.ID)
	}
	currentNode, ok := s.nodes[request.Wait.Invocation]
	if !ok {
		return workflowruntime.SuspendWaitResult{}, fmt.Errorf("%w: node invocation", workflowruntime.ErrNotFound)
	}
	if currentNode.Generation != request.ExpectedNodeGeneration {
		return workflowruntime.SuspendWaitResult{}, casMismatch("suspend wait node", request.ExpectedNodeGeneration, currentNode.Generation)
	}
	if currentNode.Status != workflowruntime.NodeRunning {
		return workflowruntime.SuspendWaitResult{}, invalid(errors.New("suspension requires a running node"))
	}
	if currentNode.Wait != nil {
		return workflowruntime.SuspendWaitResult{}, invalid(errors.New("running node already references a wait"))
	}
	if err := validateLifecycleClaim(currentNode, &request.Claim, request.At); err != nil {
		return workflowruntime.SuspendWaitResult{}, err
	}
	currentAttempt, attemptErr := s.unfinishedAttemptLocked(currentNode)
	if attemptErr != nil {
		return workflowruntime.SuspendWaitResult{}, attemptErr
	}
	if currentAttempt == nil || currentAttempt.ID.Number != currentNode.LatestAttempt {
		return workflowruntime.SuspendWaitResult{}, attemptConflict(currentNode.ID, currentNode.LatestAttempt, "suspension requires the matching unfinished attempt")
	}
	if currentAttempt.Generation != request.ExpectedAttemptGeneration {
		return workflowruntime.SuspendWaitResult{}, casMismatch("suspend wait attempt", request.ExpectedAttemptGeneration, currentAttempt.Generation)
	}
	if request.At.Before(currentNode.UpdatedAt) || request.At.Before(currentAttempt.UpdatedAt) {
		return workflowruntime.SuspendWaitResult{}, invalid(errors.New("suspend time must not regress persisted state"))
	}

	nextWait := cloneWait(request.Wait)
	nextWait.Generation = 1
	nextNode := cloneNode(currentNode)
	nextNode.Status = workflowruntime.NodeWaiting
	nextNode.Wait = &workflowruntime.WaitRef{ID: nextWait.Ref.ID}
	nextNode.Lease = nil
	nextNode.Generation++
	nextNode.UpdatedAt = request.At
	if err := nextWait.Validate(); err != nil {
		return workflowruntime.SuspendWaitResult{}, invalid(err)
	}
	if err := nextNode.Validate(); err != nil {
		return workflowruntime.SuspendWaitResult{}, invalid(err)
	}
	invocation, attemptID := nextNode.ID, currentAttempt.ID
	eventRequests := []workflowruntime.AppendEventRequest{
		{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventWaitSuspended, OccurredAt: request.At, Attributes: waitEventAttributes(nextWait, "", string(workflowruntime.WaitOpen)), Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
		{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventNodeStatusChanged, OccurredAt: request.At, Attributes: transitionAttributes("node", string(currentNode.Status), string(nextNode.Status)), Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
	}
	if err := s.validateEventRequestsLocked(eventRequests); err != nil {
		return workflowruntime.SuspendWaitResult{}, err
	}
	events, eventErr := s.appendEventRequestsLocked(eventRequests)
	if eventErr != nil {
		return workflowruntime.SuspendWaitResult{}, eventErr
	}
	s.waits[nextWait.Ref.ID] = cloneWait(nextWait)
	s.nodes[nextNode.ID] = cloneNode(nextNode)
	result := workflowruntime.SuspendWaitResult{Outcome: workflowruntime.IdempotencyApplied, Wait: cloneWait(nextWait), Node: cloneNode(nextNode), Attempt: cloneAttempt(*currentAttempt), Events: cloneEvents(events)}
	s.suspends[nextWait.Ref.ID] = suspendRecord{request: cloneSuspendRequest(request), result: cloneSuspendResult(result)}
	return result, nil
}

// ResumeNodeWait is the common atomic data-plane mutation used by every wake
// source after the host authorizer has accepted the responder.
func (s *Store) ResumeNodeWait(ctx context.Context, request workflowruntime.ResumeNodeWaitRequest) (workflowruntime.ResumeWaitResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ResumeWaitResult{}, err
	}
	request.ReceivedAt = request.ReceivedAt.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.ResumeWaitResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if request.IdempotencyKey != "" {
		if prior, ok := s.waitResumes[request.IdempotencyKey]; ok {
			if !equalAtomicResumeRequest(prior.request, request) {
				return workflowruntime.ResumeWaitResult{}, idempotencyConflict("resume node wait", request.IdempotencyKey)
			}
			result := cloneResumeResult(prior.result)
			result.Outcome = workflowruntime.ResumeReplayed
			return result, nil
		}
	}
	currentWait, ok := s.waits[request.WaitID]
	if !ok {
		return workflowruntime.ResumeWaitResult{}, fmt.Errorf("%w: wait %q", workflowruntime.ErrNotFound, request.WaitID)
	}
	tokenMatches := (currentWait.ResumeTokenDigest == "" && request.PresentedTokenDigest == "") || workflowwait.EqualTokenDigest(currentWait.ResumeTokenDigest, request.PresentedTokenDigest)
	if !tokenMatches {
		return workflowruntime.ResumeWaitResult{}, invalid(workflowruntime.ErrInvalidResumeToken)
	}
	if currentWait.Correlation != request.Correlation || currentWait.WakeSource != request.WakeSource {
		return workflowruntime.ResumeWaitResult{}, invalid(errors.New("resume does not match immutable wait correlation or wake source"))
	}
	if currentWait.Status != workflowruntime.WaitOpen {
		if currentWait.Status == workflowruntime.WaitResumed {
			if request.IdempotencyKey != "" {
				return workflowruntime.ResumeWaitResult{}, idempotencyConflict("resume node wait", request.IdempotencyKey)
			}
			result := cloneResumeResult(s.waitResumeResults[currentWait.Ref.ID])
			result.Outcome = workflowruntime.ResumeAlreadyResumed
			return result, nil
		}
		return workflowruntime.ResumeWaitResult{Wait: cloneWait(currentWait)}, &workflowruntime.WaitClosedError{WaitID: currentWait.Ref.ID, Status: currentWait.Status, ResolvedAt: currentWait.ResolvedAt}
	}
	if !currentWait.Deadline.IsZero() && !request.ReceivedAt.Before(currentWait.Deadline) {
		return workflowruntime.ResumeWaitResult{}, &workflowruntime.WaitClosedError{WaitID: currentWait.Ref.ID, Status: workflowruntime.WaitTimedOut, ResolvedAt: currentWait.Deadline}
	}
	if currentWait.Generation != request.ExpectedWaitGeneration {
		return workflowruntime.ResumeWaitResult{}, casMismatch("resume wait", request.ExpectedWaitGeneration, currentWait.Generation)
	}
	currentNode, ok := s.nodes[currentWait.Invocation]
	if !ok {
		return workflowruntime.ResumeWaitResult{}, fmt.Errorf("%w: node invocation", workflowruntime.ErrNotFound)
	}
	if currentNode.Generation != request.ExpectedNodeGeneration {
		return workflowruntime.ResumeWaitResult{}, casMismatch("resume wait node", request.ExpectedNodeGeneration, currentNode.Generation)
	}
	if currentNode.Status != workflowruntime.NodeWaiting || currentNode.Wait == nil || currentNode.Wait.ID != currentWait.Ref.ID {
		return workflowruntime.ResumeWaitResult{}, invalid(errors.New("open wait resume requires its matching waiting node"))
	}
	currentAttempt, attemptErr := s.unfinishedAttemptLocked(currentNode)
	if attemptErr != nil {
		return workflowruntime.ResumeWaitResult{}, attemptErr
	}
	if currentAttempt == nil || currentAttempt.Generation != request.ExpectedAttemptGeneration {
		actual := uint64(0)
		if currentAttempt != nil {
			actual = currentAttempt.Generation
		}
		return workflowruntime.ResumeWaitResult{}, casMismatch("resume wait attempt", request.ExpectedAttemptGeneration, actual)
	}
	if request.ReceivedAt.Before(currentWait.UpdatedAt) || request.ReceivedAt.Before(currentNode.UpdatedAt) || request.ReceivedAt.Before(currentAttempt.UpdatedAt) {
		return workflowruntime.ResumeWaitResult{}, invalid(errors.New("resume time must not regress persisted state"))
	}
	if err := values.ValidateValueSchema(currentWait.ResumeSchema.Schema, request.Payload); err != nil {
		return workflowruntime.ResumeWaitResult{}, invalid(err)
	}

	set := values.ValueSet{workflowruntime.ResumeValueName: request.Payload}
	if err := values.ValidatePersistableSet(set); err != nil {
		return workflowruntime.ResumeWaitResult{}, invalid(err)
	}
	setCopy, cloneErr := cloneValueSet(set)
	if cloneErr != nil {
		return workflowruntime.ResumeWaitResult{}, invalid(cloneErr)
	}
	nextValueID := fmt.Sprintf("values-%012d", s.nextValueSet+1)
	ref, refErr := values.NewValueSetRef(nextValueID, setCopy)
	if refErr != nil {
		return workflowruntime.ResumeWaitResult{}, invalid(refErr)
	}
	resolution := &workflowwait.Resolution{Source: request.WakeSource, Responder: cloneResponder(request.Responder), PayloadDigest: ref.Digest, IdempotencyKey: request.IdempotencyKey, ResolvedAt: request.ReceivedAt}
	nextWait := cloneWait(currentWait)
	nextWait.Status = workflowruntime.WaitResumed
	nextWait.ResumeValues = &ref
	nextWait.Resolution = resolution
	nextWait.ResolvedAt = request.ReceivedAt
	nextWait.UpdatedAt = request.ReceivedAt
	nextWait.Generation++
	nextNode := cloneNode(currentNode)
	nextNode.Status = workflowruntime.NodeReady
	nextNode.Wait = nil
	nextNode.Lease = nil
	nextNode.UpdatedAt = request.ReceivedAt
	nextNode.Generation++
	if err := nextWait.Validate(); err != nil {
		return workflowruntime.ResumeWaitResult{}, invalid(err)
	}
	if err := nextNode.Validate(); err != nil {
		return workflowruntime.ResumeWaitResult{}, invalid(err)
	}
	invocation, attemptID := nextNode.ID, currentAttempt.ID
	eventRequests := []workflowruntime.AppendEventRequest{
		{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventWaitResumed, OccurredAt: request.ReceivedAt, Attributes: waitEventAttributes(nextWait, string(workflowruntime.WaitOpen), string(workflowruntime.WaitResumed)), Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
		{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventNodeStatusChanged, OccurredAt: request.ReceivedAt, Attributes: transitionAttributes("node", string(currentNode.Status), string(nextNode.Status)), Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
	}
	if err := s.validateEventRequestsLocked(eventRequests); err != nil {
		return workflowruntime.ResumeWaitResult{}, err
	}
	events, eventErr := s.appendEventRequestsLocked(eventRequests)
	if eventErr != nil {
		return workflowruntime.ResumeWaitResult{}, eventErr
	}
	s.nextValueSet++
	s.valueSets[ref.ID] = storedValues{ref: ref, owner: workflowruntime.ValueOwner{Kind: "wait_resume", RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID}, values: setCopy}
	s.waits[nextWait.Ref.ID] = cloneWait(nextWait)
	s.nodes[nextNode.ID] = cloneNode(nextNode)
	result := workflowruntime.ResumeWaitResult{Outcome: workflowruntime.ResumeApplied, Wait: cloneWait(nextWait), Node: cloneNode(nextNode), Attempt: cloneAttempt(*currentAttempt), Values: ref, Events: cloneEvents(events)}
	s.waitResumeResults[nextWait.Ref.ID] = cloneResumeResult(result)
	if request.IdempotencyKey != "" {
		s.waitResumes[request.IdempotencyKey] = waitResumeRecord{request: cloneAtomicResumeRequest(request), result: cloneResumeResult(result)}
	}
	return result, nil
}

// RecoverOpenWaits returns storage state only; callers own scheduling policy.
func (s *Store) RecoverOpenWaits(ctx context.Context, query workflowruntime.OpenWaitQuery) ([]workflowruntime.WaitSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if query.Limit < 0 {
		return nil, invalid(errors.New("open wait recovery limit must not be negative"))
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]workflowruntime.WaitSnapshot, 0)
	for _, snapshot := range s.waits {
		if snapshot.Status == workflowruntime.WaitOpen && (query.RunID == "" || snapshot.Invocation.RunID == query.RunID) {
			result = append(result, cloneWait(snapshot))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		left, right := result[i], result[j]
		if left.Deadline.IsZero() != right.Deadline.IsZero() {
			return !left.Deadline.IsZero()
		}
		if !left.Deadline.Equal(right.Deadline) {
			return left.Deadline.Before(right.Deadline)
		}
		if !left.CreatedAt.Equal(right.CreatedAt) {
			return left.CreatedAt.Before(right.CreatedAt)
		}
		return left.Ref.ID < right.Ref.ID
	})
	return limit(result, query.Limit), nil
}

func (s *Store) validateEventRequestsLocked(requests []workflowruntime.AppendEventRequest) error {
	base := make(map[workflowruntime.RunID]int)
	for _, request := range requests {
		index := base[request.RunID]
		if index == 0 {
			index = len(s.events[request.RunID])
		}
		candidate := workflowruntime.Event{Sequence: uint64(index + 1), RunID: request.RunID, Invocation: cloneInvocationID(request.Invocation), Attempt: cloneAttemptID(request.Attempt), Type: request.Type, OccurredAt: request.OccurredAt, Attributes: cloneStringMap(request.Attributes), Values: cloneValueSetRef(request.Values), Redaction: request.Redaction, Retention: request.Retention}
		if err := candidate.Validate(); err != nil {
			return invalid(err)
		}
		base[request.RunID] = index + 1
	}
	return nil
}

func (s *Store) appendEventRequestsLocked(requests []workflowruntime.AppendEventRequest) ([]workflowruntime.Event, error) {
	events := make([]workflowruntime.Event, 0, len(requests))
	for _, request := range requests {
		event, err := s.appendEventLocked(request)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func waitEventAttributes(snapshot workflowruntime.WaitSnapshot, from, to string) map[string]string {
	attributes := map[string]string{"entity": "wait", "wait_id": string(snapshot.Ref.ID), "kind": string(snapshot.Kind), "wake_source": string(snapshot.WakeSource), "to_status": to}
	if from != "" {
		attributes["from_status"] = from
	}
	if !snapshot.Deadline.IsZero() {
		attributes["deadline"] = snapshot.Deadline.UTC().Format(time.RFC3339Nano)
	}
	if snapshot.Resolution != nil {
		attributes["responder_kind"] = snapshot.Resolution.Responder.Kind
		attributes["responder_reference"] = snapshot.Resolution.Responder.Reference
	}
	return attributes
}

func equalJSON(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}
func equalAtomicResumeRequest(left, right workflowruntime.ResumeNodeWaitRequest) bool {
	left.ExpectedWaitGeneration, right.ExpectedWaitGeneration = 0, 0
	left.ExpectedNodeGeneration, right.ExpectedNodeGeneration = 0, 0
	left.ExpectedAttemptGeneration, right.ExpectedAttemptGeneration = 0, 0
	left.ReceivedAt, right.ReceivedAt = time.Time{}, time.Time{}
	return equalJSON(left, right)
}
func cloneResponder(value workflowwait.Responder) workflowwait.Responder {
	value.Attributes = cloneStringMap(value.Attributes)
	return value
}
func cloneSuspendRequest(request workflowruntime.SuspendNodeWaitRequest) workflowruntime.SuspendNodeWaitRequest {
	request.Wait = cloneWait(request.Wait)
	return request
}
func cloneSuspendResult(result workflowruntime.SuspendWaitResult) workflowruntime.SuspendWaitResult {
	result.Wait = cloneWait(result.Wait)
	result.Node = cloneNode(result.Node)
	result.Attempt = cloneAttempt(result.Attempt)
	result.Events = cloneEvents(result.Events)
	return result
}
func cloneAtomicResumeRequest(request workflowruntime.ResumeNodeWaitRequest) workflowruntime.ResumeNodeWaitRequest {
	request.Responder = cloneResponder(request.Responder)
	set, _ := cloneValueSet(values.ValueSet{workflowruntime.ResumeValueName: request.Payload})
	request.Payload = set[workflowruntime.ResumeValueName]
	return request
}
func cloneResumeResult(result workflowruntime.ResumeWaitResult) workflowruntime.ResumeWaitResult {
	result.Wait = cloneWait(result.Wait)
	result.Node = cloneNode(result.Node)
	result.Attempt = cloneAttempt(result.Attempt)
	result.Events = cloneEvents(result.Events)
	return result
}
