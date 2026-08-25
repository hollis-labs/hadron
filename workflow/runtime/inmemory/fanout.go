package inmemory

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

func (s *Store) LoadFanOut(ctx context.Context, parent workflowruntime.NodeInvocationID) (workflowruntime.FanOutSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.FanOutSnapshot{}, err
	}
	if err := parent.Validate(); err != nil {
		return workflowruntime.FanOutSnapshot{}, invalid(err)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot, ok := s.fanOuts[parent]
	if !ok {
		return workflowruntime.FanOutSnapshot{}, fmt.Errorf("%w: fan-out aggregate", workflowruntime.ErrNotFound)
	}
	return cloneFanOut(snapshot), nil
}

func (s *Store) LoadFanOutItemResults(ctx context.Context, parent workflowruntime.NodeInvocationID) ([]workflowruntime.FanOutItemResult, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := parent.Validate(); err != nil {
		return nil, invalid(err)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	fanOut, ok := s.fanOuts[parent]
	if !ok {
		return nil, fmt.Errorf("%w: fan-out aggregate", workflowruntime.ErrNotFound)
	}
	results := make([]workflowruntime.FanOutItemResult, len(fanOut.Items))
	for index, binding := range fanOut.Items {
		node, ok := s.nodes[binding.Invocation]
		if !ok {
			return nil, fmt.Errorf("%w: fan-out child", workflowruntime.ErrNotFound)
		}
		item := workflowruntime.FanOutItemResult{
			Index: index, Iteration: binding.Iteration, Invocation: binding.Invocation,
			Status: node.Status, Outputs: cloneValueSetRef(node.Outputs),
		}
		if node.Outputs != nil {
			stored, exists := s.valueSets[node.Outputs.ID]
			if !exists || stored.ref != *node.Outputs {
				return nil, invalid(fmt.Errorf("fan-out item %d outputs do not resolve", index))
			}
			cloned, err := cloneValueSet(stored.values)
			if err != nil {
				return nil, invalid(err)
			}
			item.OutputValues = cloned
		}
		if node.LatestAttempt > 0 && hardFailureStatus(node.Status) {
			attempt, exists := s.attempts[workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt}]
			if !exists {
				return nil, fmt.Errorf("%w: fan-out item attempt", workflowruntime.ErrNotFound)
			}
			item.Failure = cloneFailure(attempt.Failure)
		}
		results[index] = item
	}
	return cloneFanOutItemResults(results), nil
}

func (s *Store) ExpandFanOut(ctx context.Context, request workflowruntime.ExpandFanOutRequest) (workflowruntime.ExpandFanOutResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ExpandFanOutResult{}, err
	}
	request.At = request.At.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.ExpandFanOutResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[request.FanOut.Parent.RunID]
	if !ok {
		return workflowruntime.ExpandFanOutResult{}, fmt.Errorf("%w: run", workflowruntime.ErrNotFound)
	}
	if !run.Status.Active() {
		return workflowruntime.ExpandFanOutResult{}, invalid(errors.New("terminal run cannot expand fan-out"))
	}
	if !s.controlAdmissionAllowedLocked(request.FanOut.Parent) {
		return workflowruntime.ExpandFanOutResult{}, invalid(errors.New("pending terminal intent fences fan-out expansion"))
	}
	parent, ok := s.nodes[request.FanOut.Parent]
	if !ok {
		return workflowruntime.ExpandFanOutResult{}, fmt.Errorf("%w: fan-out parent", workflowruntime.ErrNotFound)
	}
	if parent.Generation != request.ExpectedParentGeneration {
		return workflowruntime.ExpandFanOutResult{}, casMismatch("fan-out parent", request.ExpectedParentGeneration, parent.Generation)
	}
	if parent.Status != workflowruntime.NodePending && parent.Status != workflowruntime.NodeBlocked && parent.Status != workflowruntime.NodeReady {
		return workflowruntime.ExpandFanOutResult{}, invalid(fmt.Errorf("fan-out parent status %q cannot expand", parent.Status))
	}
	if parent.LatestAttempt != 0 || parent.Lease != nil || parent.Wait != nil || parent.Outputs != nil {
		return workflowruntime.ExpandFanOutResult{}, invalid(errors.New("fan-out parent must not have attempt, lease, wait, or outputs"))
	}
	if request.At.Before(parent.UpdatedAt) {
		return workflowruntime.ExpandFanOutResult{}, invalid(errors.New("fan-out expansion time must not regress parent"))
	}
	if _, exists := s.fanOuts[parent.ID]; exists {
		return workflowruntime.ExpandFanOutResult{}, fmt.Errorf("%w: fan-out aggregate", workflowruntime.ErrAlreadyExists)
	}
	children := make([]workflowruntime.NodeInvocationSnapshot, len(request.FanOut.Items))
	for index, item := range request.FanOut.Items {
		if _, exists := s.nodes[item.Invocation]; exists {
			return workflowruntime.ExpandFanOutResult{}, fmt.Errorf("%w: fan-out item %d invocation", workflowruntime.ErrAlreadyExists, index)
		}
		stored, ok := s.valueSets[item.Inputs.ID]
		if !ok || stored.ref != item.Inputs {
			return workflowruntime.ExpandFanOutResult{}, invalid(fmt.Errorf("fan-out item %d inputs do not resolve", index))
		}
		ref := item.Inputs
		child := workflowruntime.NodeInvocationSnapshot{
			ID: item.Invocation, Status: workflowruntime.NodeReady, Inputs: &ref,
			Priority: request.Priority, Generation: 1, CreatedAt: request.At, UpdatedAt: request.At,
		}
		if err := child.Validate(); err != nil {
			return workflowruntime.ExpandFanOutResult{}, invalid(err)
		}
		children[index] = child
	}
	nextFanOut := cloneFanOut(request.FanOut)
	nextFanOut.Generation = 1
	nextFanOut.CreatedAt, nextFanOut.UpdatedAt = request.At, request.At
	nextParent := cloneNode(parent)
	nextParent.Status = workflowruntime.NodeWaiting
	nextParent.Blocked = nil
	nextParent.Generation++
	nextParent.UpdatedAt = request.At
	if err := nextFanOut.Validate(); err != nil {
		return workflowruntime.ExpandFanOutResult{}, invalid(err)
	}
	if err := nextParent.Validate(); err != nil {
		return workflowruntime.ExpandFanOutResult{}, invalid(err)
	}
	parentID := parent.ID
	event, err := s.appendEventLocked(workflowruntime.AppendEventRequest{
		RunID: parent.ID.RunID, Invocation: &parentID, Type: workflowruntime.EventFanOutExpanded, OccurredAt: request.At,
		Attributes: map[string]string{
			"items": strconv.Itoa(len(children)), "max_concurrency": strconv.Itoa(nextFanOut.MaxConcurrency),
			"from_status": string(parent.Status), "to_status": string(nextParent.Status),
		}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		return workflowruntime.ExpandFanOutResult{}, err
	}
	s.fanOuts[parent.ID] = nextFanOut
	s.nodes[parent.ID] = nextParent
	for _, child := range children {
		s.nodes[child.ID] = child
	}
	return workflowruntime.ExpandFanOutResult{
		FanOut: cloneFanOut(nextFanOut), Parent: cloneNode(nextParent), Children: cloneNodes(children), Event: cloneEvent(event),
	}, nil
}

func (s *Store) CompleteFanOut(ctx context.Context, request workflowruntime.CompleteFanOutRequest) (workflowruntime.CompleteFanOutResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.CompleteFanOutResult{}, err
	}
	request.At = request.At.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.CompleteFanOutResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[request.Parent.RunID]
	if !ok {
		return workflowruntime.CompleteFanOutResult{}, fmt.Errorf("%w: run", workflowruntime.ErrNotFound)
	}
	if !run.Status.Active() {
		return workflowruntime.CompleteFanOutResult{}, invalid(errors.New("terminal run fences fan-out completion"))
	}
	if !s.controlAdmissionAllowedLocked(request.Parent) {
		return workflowruntime.CompleteFanOutResult{}, invalid(errors.New("pending terminal intent fences fan-out completion"))
	}
	parent, ok := s.nodes[request.Parent]
	if !ok {
		return workflowruntime.CompleteFanOutResult{}, fmt.Errorf("%w: fan-out parent", workflowruntime.ErrNotFound)
	}
	if parent.Generation != request.ExpectedParentGeneration {
		return workflowruntime.CompleteFanOutResult{}, casMismatch("fan-out parent", request.ExpectedParentGeneration, parent.Generation)
	}
	fanOut, ok := s.fanOuts[request.Parent]
	if !ok {
		return workflowruntime.CompleteFanOutResult{}, fmt.Errorf("%w: fan-out aggregate", workflowruntime.ErrNotFound)
	}
	if fanOut.Generation != request.ExpectedFanOutGeneration {
		return workflowruntime.CompleteFanOutResult{}, casMismatch("fan-out aggregate", request.ExpectedFanOutGeneration, fanOut.Generation)
	}
	if fanOut.Status != workflowruntime.FanOutActive || parent.Status != workflowruntime.NodeWaiting {
		return workflowruntime.CompleteFanOutResult{}, invalid(errors.New("fan-out completion requires active waiting aggregate"))
	}
	if len(request.ExpectedChildGenerations) != len(fanOut.Items) {
		return workflowruntime.CompleteFanOutResult{}, invalid(errors.New("fan-out completion must fence every item"))
	}
	for _, item := range fanOut.Items {
		child, exists := s.nodes[item.Invocation]
		if !exists {
			return workflowruntime.CompleteFanOutResult{}, fmt.Errorf("%w: fan-out child", workflowruntime.ErrNotFound)
		}
		expected, present := request.ExpectedChildGenerations[item.Invocation]
		if !present || child.Generation != expected {
			return workflowruntime.CompleteFanOutResult{}, casMismatch("fan-out child", expected, child.Generation)
		}
		if !child.Status.Terminal() {
			return workflowruntime.CompleteFanOutResult{}, fmt.Errorf("%w: child %s is %s", workflowruntime.ErrFanOutIncomplete, item.Iteration, child.Status)
		}
	}
	storedOutputs, ok := s.valueSets[request.Outputs.ID]
	if !ok || storedOutputs.ref != request.Outputs {
		return workflowruntime.CompleteFanOutResult{}, invalid(errors.New("fan-out aggregate outputs do not resolve"))
	}
	nextFanOut := cloneFanOut(fanOut)
	nextFanOut.Status = request.Status
	nextFanOut.Outputs = cloneValueSetRef(&request.Outputs)
	nextFanOut.Failure = cloneFailure(request.Failure)
	nextFanOut.Generation++
	nextFanOut.UpdatedAt = request.At
	nextParent := cloneNode(parent)
	switch request.Status {
	case workflowruntime.FanOutSucceeded:
		nextParent.Status = workflowruntime.NodeSucceeded
	case workflowruntime.FanOutFailed:
		nextParent.Status = workflowruntime.NodeFailed
	case workflowruntime.FanOutCanceled:
		nextParent.Status = workflowruntime.NodeCanceled
	case workflowruntime.FanOutActive:
		return workflowruntime.CompleteFanOutResult{}, invalid(errors.New("active fan-out cannot complete"))
	}
	nextParent.Outputs = cloneValueSetRef(&request.Outputs)
	nextParent.Generation++
	nextParent.UpdatedAt = request.At
	if err := nextFanOut.Validate(); err != nil {
		return workflowruntime.CompleteFanOutResult{}, invalid(err)
	}
	if err := nextParent.Validate(); err != nil {
		return workflowruntime.CompleteFanOutResult{}, invalid(err)
	}
	parentID := parent.ID
	event, err := s.appendEventLocked(workflowruntime.AppendEventRequest{
		RunID: parent.ID.RunID, Invocation: &parentID, Type: workflowruntime.EventFanOutCompleted, OccurredAt: request.At,
		Attributes: map[string]string{"status": string(request.Status), "items": strconv.Itoa(len(fanOut.Items))},
		Values:     &request.Outputs, Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		return workflowruntime.CompleteFanOutResult{}, err
	}
	s.fanOuts[parent.ID] = nextFanOut
	s.nodes[parent.ID] = nextParent
	return workflowruntime.CompleteFanOutResult{FanOut: cloneFanOut(nextFanOut), Parent: cloneNode(nextParent), Event: cloneEvent(event)}, nil
}

func (s *Store) enforceFanOutStartLocked(node workflowruntime.NodeInvocationSnapshot) error {
	if node.ID.Iteration == "" || node.LatestAttempt > 0 {
		return nil
	}
	parent := workflowruntime.NodeInvocationID{RunID: node.ID.RunID, NodeID: node.ID.NodeID}
	fanOut, ok := s.fanOuts[parent]
	if !ok {
		return nil
	}
	member := false
	active := 0
	children := make([]workflowruntime.NodeInvocationSnapshot, 0, len(fanOut.Items))
	for _, item := range fanOut.Items {
		if item.Invocation == node.ID {
			member = true
		}
		child, exists := s.nodes[item.Invocation]
		if exists {
			children = append(children, child)
		}
		if exists && item.Invocation != node.ID && child.LatestAttempt > 0 && !child.Status.Terminal() {
			active++
		}
	}
	if allowed, err := workflowruntime.FanOutFailFastAdmissionAllowed(fanOut, children); err != nil {
		return err
	} else if member && !allowed {
		return fmt.Errorf("%w: parent %s is fail-fast fenced", workflowruntime.ErrFanOutLimit, nodeIdentity(parent))
	}
	if fanOut.MaxConcurrency > 0 && member && active >= fanOut.MaxConcurrency {
		return fmt.Errorf("%w: parent %s has %d active items", workflowruntime.ErrFanOutLimit, nodeIdentity(parent), active)
	}
	return nil
}

// fanOutClaimEligibleLocked reserves a first-attempt fan-out slot at claim
// time. A live first-attempt lease counts as a reservation so two workers
// cannot both acquire the final slot before either starts its attempt.
func (s *Store) fanOutClaimEligibleLocked(node workflowruntime.NodeInvocationSnapshot, now time.Time) bool {
	if node.ID.Iteration == "" || node.LatestAttempt > 0 {
		return true
	}
	parent := workflowruntime.NodeInvocationID{RunID: node.ID.RunID, NodeID: node.ID.NodeID}
	fanOut, ok := s.fanOuts[parent]
	if !ok {
		return true
	}
	member, occupied := false, 0
	children := make([]workflowruntime.NodeInvocationSnapshot, 0, len(fanOut.Items))
	for _, item := range fanOut.Items {
		if item.Invocation == node.ID {
			member = true
			continue
		}
		child, exists := s.nodes[item.Invocation]
		if exists {
			children = append(children, child)
		}
		if !exists || child.Status.Terminal() {
			continue
		}
		if child.LatestAttempt > 0 || child.Lease != nil && child.Lease.ExpiresAt.After(now) {
			occupied++
		}
	}
	if allowed, err := workflowruntime.FanOutFailFastAdmissionAllowed(fanOut, children); err != nil || member && !allowed {
		return false
	}
	if fanOut.MaxConcurrency == 0 {
		return true
	}
	return !member || occupied < fanOut.MaxConcurrency
}

func cloneFanOut(snapshot workflowruntime.FanOutSnapshot) workflowruntime.FanOutSnapshot {
	if snapshot.Tolerate != nil {
		policy := *snapshot.Tolerate
		snapshot.Tolerate = &policy
	}
	snapshot.Items = append([]workflowruntime.FanOutItemBinding(nil), snapshot.Items...)
	snapshot.Outputs = cloneValueSetRef(snapshot.Outputs)
	snapshot.Failure = cloneFailure(snapshot.Failure)
	return snapshot
}

func cloneFanOutItemResults(items []workflowruntime.FanOutItemResult) []workflowruntime.FanOutItemResult {
	result := make([]workflowruntime.FanOutItemResult, len(items))
	for index, item := range items {
		item.Outputs = cloneValueSetRef(item.Outputs)
		if item.OutputValues != nil {
			item.OutputValues, _ = cloneValueSet(item.OutputValues)
		}
		item.Failure = cloneFailure(item.Failure)
		result[index] = item
	}
	return result
}

func hardFailureStatus(status workflowruntime.NodeStatus) bool {
	return status == workflowruntime.NodeFailed || status == workflowruntime.NodeTimedOut || status == workflowruntime.NodeCrashed || status == workflowruntime.NodeCanceled
}

func cloneNodes(nodes []workflowruntime.NodeInvocationSnapshot) []workflowruntime.NodeInvocationSnapshot {
	result := make([]workflowruntime.NodeInvocationSnapshot, len(nodes))
	for index := range nodes {
		result[index] = cloneNode(nodes[index])
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].ID.Iteration < result[j].ID.Iteration })
	return result
}
