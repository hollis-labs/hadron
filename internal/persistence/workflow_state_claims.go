package persistence

import (
	"context"
	"errors"
	"fmt"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
)

// ClaimNode implements runtime.StateStore. Only ready nodes are eligible;
// non-ready nodes and ready nodes with a live lease return an idempotently
// persisted Acquired=false result after claim-generation CAS succeeds.
func (s *WorkflowStateStore) ClaimNode(ctx context.Context, request workflowruntime.ClaimNodeRequest) (workflowruntime.ClaimResult, error) {
	if err := validateWorkflowClaim(request); err != nil {
		return workflowruntime.ClaimResult{}, workflowInvalid(err)
	}
	requestJSON, canonicalErr := canonicalClaimRequest(request)
	if canonicalErr != nil {
		return workflowruntime.ClaimResult{}, canonicalErr
	}
	var result workflowruntime.ClaimResult
	writeErr := s.write(ctx, "claim workflow node", func(query workflowSQL) error {
		priorRequest, priorResult, found, loadErr := loadWorkflowIdempotency(
			ctx, query, "workflow_claim_idempotency", request.IdempotencyKey,
		)
		if loadErr != nil {
			return loadErr
		}
		if found {
			if priorRequest != requestJSON {
				return workflowIdempotencyConflict("claim node", request.IdempotencyKey)
			}
			if decodeErr := decodeWorkflowJSON("claim result", priorResult, &result); decodeErr != nil {
				return decodeErr
			}
			if validationErr := validateWorkflowClaimResult(result); validationErr != nil {
				return workflowInvalid(validationErr)
			}
			current, err := loadWorkflowNode(ctx, query, request.InvocationID)
			if err != nil {
				return err
			}
			allowed, err := workflowControlAdmissionAllowed(ctx, query, current.ID)
			if err != nil {
				return err
			}
			if !allowed {
				result = workflowruntime.ClaimResult{Acquired: false, Replayed: true}
				return nil
			}
			result.Replayed = true
			return nil
		}

		current, err := loadWorkflowNode(ctx, query, request.InvocationID)
		if err != nil {
			return err
		}
		if current.ClaimGeneration != request.ExpectedClaimGeneration {
			return workflowCAS("node claim", request.ExpectedClaimGeneration, current.ClaimGeneration)
		}
		allowed, err := workflowControlAdmissionAllowed(ctx, query, current.ID)
		if err != nil {
			return err
		}
		if !allowed {
			result = workflowruntime.ClaimResult{Acquired: false}
			return recordWorkflowClaimIdempotency(ctx, query, request.IdempotencyKey, requestJSON, result)
		}
		now := request.Now.UTC()
		run, err := loadWorkflowRun(ctx, query, current.ID.RunID)
		if err != nil {
			return err
		}
		eligible, err := workflowFanOutClaimEligible(ctx, query, current, now)
		if err != nil {
			return err
		}
		if !run.Status.Active() || !eligible || current.Status != workflowruntime.NodeReady ||
			(current.Lease != nil && current.Lease.ExpiresAt.After(now)) {
			result = workflowruntime.ClaimResult{Acquired: false}
			return recordWorkflowClaimIdempotency(ctx, query, request.IdempotencyKey, requestJSON, result)
		}
		if now.Before(current.UpdatedAt) {
			return workflowInvalid(errors.New("claim time must not regress node updated_at"))
		}
		next := cloneWorkflowNode(current)
		next.ClaimGeneration++
		next.Lease = &workflowruntime.ClaimLease{
			Owner: request.Owner, Token: request.Token,
			Generation: next.ClaimGeneration, ExpiresAt: request.LeaseUntil.UTC(),
		}
		next.Generation++
		next.UpdatedAt = now
		if err := next.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := releaseWorkflowSchedulerResources(ctx, query, current.ID); err != nil {
			return err
		}
		if err := updateWorkflowNodeCAS(ctx, query, next, current.Generation); err != nil {
			return err
		}
		result = workflowruntime.ClaimResult{Acquired: true, Lease: cloneWorkflowLease(next.Lease)}
		return recordWorkflowClaimIdempotency(ctx, query, request.IdempotencyKey, requestJSON, result)
	})
	if writeErr != nil {
		return workflowruntime.ClaimResult{}, writeErr
	}
	return result, nil
}

func recordWorkflowClaimIdempotency(ctx context.Context, query workflowSQL, key, requestJSON string, result workflowruntime.ClaimResult) error {
	resultJSON, err := canonicalClaimResult(result)
	if err != nil {
		return err
	}
	if _, err := query.ExecContext(ctx, `
INSERT INTO workflow_claim_idempotency(idempotency_key, request_json, result_json)
VALUES (?, ?, ?)`, key, requestJSON, resultJSON); err != nil {
		if isSQLiteConstraint(err) {
			return workflowIdempotencyConflict("claim node", key)
		}
		return fmt.Errorf("record workflow claim idempotency: %w", err)
	}
	return nil
}

// RenewNodeLease implements runtime.StateStore with lease fencing, monotonic
// timestamps, extension-only expiry, and node record CAS in one transaction.
func (s *WorkflowStateStore) RenewNodeLease(ctx context.Context, request workflowruntime.RenewLeaseRequest) (workflowruntime.ClaimLease, error) {
	if err := validateWorkflowRenew(request); err != nil {
		return workflowruntime.ClaimLease{}, workflowInvalid(err)
	}
	var result workflowruntime.ClaimLease
	err := s.write(ctx, "renew workflow node lease", func(query workflowSQL) error {
		current, err := loadWorkflowNode(ctx, query, request.InvocationID)
		if err != nil {
			return err
		}
		allowed, err := workflowControlAdmissionAllowed(ctx, query, current.ID)
		if err != nil {
			return err
		}
		if !allowed {
			return workflowInvalid(errors.New("pending terminal intent fences lease renewal"))
		}
		if !matchesWorkflowLease(current.Lease, request.Owner, request.Token, request.Generation) {
			return workflowruntime.ErrClaimMismatch
		}
		now := request.Now.UTC()
		if !current.Lease.ExpiresAt.After(now) {
			return workflowruntime.ErrLeaseExpired
		}
		if now.Before(current.UpdatedAt) {
			return workflowInvalid(errors.New("renewal time must not regress node updated_at"))
		}
		leaseUntil := request.LeaseUntil.UTC()
		if !leaseUntil.After(current.Lease.ExpiresAt) {
			return workflowInvalid(errors.New("renewal must extend the current lease expiry"))
		}
		next := cloneWorkflowNode(current)
		next.Lease.ExpiresAt = leaseUntil
		next.Generation++
		next.UpdatedAt = now
		if err := next.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := updateWorkflowNodeCAS(ctx, query, next, current.Generation); err != nil {
			return err
		}
		result = *cloneWorkflowLease(next.Lease)
		return nil
	})
	if err != nil {
		return workflowruntime.ClaimLease{}, err
	}
	return result, nil
}

// ReleaseNodeClaim implements runtime.StateStore with lease fencing and a node
// record generation CAS in one transaction.
func (s *WorkflowStateStore) ReleaseNodeClaim(ctx context.Context, request workflowruntime.ReleaseClaimRequest) error {
	if err := validateWorkflowRelease(request); err != nil {
		return workflowInvalid(err)
	}
	return s.write(ctx, "release workflow node claim", func(query workflowSQL) error {
		current, err := loadWorkflowNode(ctx, query, request.InvocationID)
		if err != nil {
			return err
		}
		if !matchesWorkflowLease(current.Lease, request.Owner, request.Token, request.Generation) {
			return workflowruntime.ErrClaimMismatch
		}
		now := request.Now.UTC()
		if now.Before(current.UpdatedAt) {
			return workflowInvalid(errors.New("release time must not regress node updated_at"))
		}
		next := cloneWorkflowNode(current)
		next.Lease = nil
		next.Generation++
		next.UpdatedAt = now
		if err := next.Validate(); err != nil {
			return workflowInvalid(err)
		}
		return updateWorkflowNodeCAS(ctx, query, next, current.Generation)
	})
}

func validateWorkflowClaim(request workflowruntime.ClaimNodeRequest) error {
	if err := request.InvocationID.Validate(); err != nil {
		return err
	}
	if request.Owner == "" || request.Token == "" || request.IdempotencyKey == "" {
		return errors.New("claim requires owner, token, and idempotency key")
	}
	if request.Now.IsZero() || !request.LeaseUntil.After(request.Now) {
		return errors.New("claim lease must end after non-zero now")
	}
	return nil
}

func validateWorkflowRenew(request workflowruntime.RenewLeaseRequest) error {
	if err := request.InvocationID.Validate(); err != nil {
		return err
	}
	if request.Owner == "" || request.Token == "" || request.Generation == 0 {
		return errors.New("renewal requires owner, token, and generation")
	}
	if request.Now.IsZero() || !request.LeaseUntil.After(request.Now) {
		return errors.New("renewal lease must end after non-zero now")
	}
	return nil
}

func validateWorkflowRelease(request workflowruntime.ReleaseClaimRequest) error {
	if err := request.InvocationID.Validate(); err != nil {
		return err
	}
	if request.Now.IsZero() {
		return errors.New("release now is required")
	}
	if request.Owner == "" || request.Token == "" || request.Generation == 0 {
		return errors.New("release requires owner, token, and generation")
	}
	return nil
}

func validateWorkflowClaimResult(result workflowruntime.ClaimResult) error {
	if result.Acquired {
		if result.Lease == nil {
			return errors.New("acquired claim result requires a lease")
		}
		return result.Lease.Validate()
	}
	if result.Lease != nil {
		return errors.New("non-acquired claim result must not expose a lease")
	}
	return nil
}

func matchesWorkflowLease(lease *workflowruntime.ClaimLease, owner, token string, generation uint64) bool {
	return lease != nil && lease.Owner == owner && lease.Token == token && lease.Generation == generation
}

func cloneWorkflowLease(lease *workflowruntime.ClaimLease) *workflowruntime.ClaimLease {
	if lease == nil {
		return nil
	}
	copyLease := *lease
	return &copyLease
}
