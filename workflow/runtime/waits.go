package runtime

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

const (
	EventWaitSuspended = "wait.suspended"
	EventWaitResumed   = "wait.resumed"
	ResumeValueName    = "resume"
)

var (
	ErrInvalidResumeToken = errors.New("invalid workflow wait resume token")
	ErrWaitClosed         = errors.New("workflow wait is closed")
	ErrWaitUnresumable    = errors.New("workflow wait cannot be resumed")
	ErrWaitRecovery       = errors.New("workflow wait recovery failed")
)

// PostCommitError reports an operational adapter failure after the durable
// result was committed. Callers must inspect and retain the returned result.
type PostCommitError struct {
	Operation string
	Err       error
}

func (e *PostCommitError) Error() string {
	return fmt.Sprintf("%s after durable commit: %v", e.Operation, e.Err)
}
func (e *PostCommitError) Unwrap() error { return e.Err }

// WaitClosedError reports a response that lost to a durable terminal outcome.
type WaitClosedError struct {
	WaitID     WaitID
	Status     WaitStatus
	ResolvedAt time.Time
}

func (e *WaitClosedError) Error() string {
	return fmt.Sprintf("%s: wait %q is %s", ErrWaitClosed, e.WaitID, e.Status)
}
func (e *WaitClosedError) Unwrap() error { return ErrWaitClosed }

// SuspendNodeWaitRequest atomically creates a wait and releases its running
// worker lease while preserving the unfinished attempt for continuation.
type SuspendNodeWaitRequest struct {
	Wait                      WaitSnapshot
	ExpectedNodeGeneration    uint64
	ExpectedAttemptGeneration uint64
	Claim                     ClaimProof
	At                        time.Time
}

func (r SuspendNodeWaitRequest) Validate() error {
	if r.Wait.Generation != 0 || r.Wait.Status != WaitOpen {
		return fmt.Errorf("new wait must be open with zero generation")
	}
	if r.ExpectedNodeGeneration == 0 || r.ExpectedAttemptGeneration == 0 {
		return fmt.Errorf("suspend requires positive node and attempt generations")
	}
	if err := r.Claim.Validate(); err != nil {
		return err
	}
	if r.At.IsZero() {
		return fmt.Errorf("suspend time is required")
	}
	if !r.Wait.CreatedAt.IsZero() && !r.Wait.CreatedAt.Equal(r.At) {
		return fmt.Errorf("wait created_at must equal suspend time")
	}
	candidate := r.Wait
	candidate.Generation = 1
	candidate.CreatedAt, candidate.UpdatedAt = r.At, r.At
	if err := candidate.Validate(); err != nil {
		return err
	}
	return nil
}

type SuspendWaitResult struct {
	Outcome IdempotencyOutcome
	Wait    WaitSnapshot
	Node    NodeInvocationSnapshot
	Attempt AttemptSnapshot
	Events  []Event
}

// ResumeNodeWaitRequest is the fully validated input to an atomic store. It
// carries only the presented token digest, never the raw token.
type ResumeNodeWaitRequest struct {
	WaitID                    WaitID
	ExpectedWaitGeneration    uint64
	ExpectedNodeGeneration    uint64
	ExpectedAttemptGeneration uint64
	Correlation               string
	PresentedTokenDigest      string
	WakeSource                workflowwait.WakeSource
	Responder                 workflowwait.Responder
	Payload                   values.Value
	IdempotencyKey            string
	ReceivedAt                time.Time
}

func (r ResumeNodeWaitRequest) Validate() error {
	if err := (WaitRef{ID: r.WaitID}).Validate(); err != nil {
		return err
	}
	if r.ExpectedWaitGeneration == 0 || r.ExpectedNodeGeneration == 0 || r.ExpectedAttemptGeneration == 0 {
		return fmt.Errorf("resume requires positive wait, node, and attempt generations")
	}
	if err := validateRequiredText("resume correlation", r.Correlation); err != nil {
		return err
	}
	if !r.WakeSource.Valid() || r.ReceivedAt.IsZero() {
		return fmt.Errorf("resume requires wake source and received_at")
	}
	if r.PresentedTokenDigest != "" {
		if err := values.ValidateDigest(r.PresentedTokenDigest); err != nil {
			return ErrInvalidResumeToken
		}
	}
	if err := r.Responder.Validate(); err != nil {
		return err
	}
	if r.IdempotencyKey != "" {
		if err := validateRequiredText("resume idempotency key", r.IdempotencyKey); err != nil {
			return err
		}
	}
	return r.Payload.Validate()
}

type ResumeOutcome string

const (
	ResumeApplied        ResumeOutcome = "applied"
	ResumeReplayed       ResumeOutcome = "replayed"
	ResumeAlreadyResumed ResumeOutcome = "already_resumed"
	ResumeClosed         ResumeOutcome = "closed"
)

type ResumeWaitResult struct {
	Outcome ResumeOutcome
	Wait    WaitSnapshot
	Node    NodeInvocationSnapshot
	Attempt AttemptSnapshot
	Values  values.ValueSetRef
	Events  []Event
}

// OpenWaitQuery supports deterministic durable recovery. Limit zero means
// unlimited; results are ordered by deadline, creation time, then wait ID.
type OpenWaitQuery struct {
	RunID RunID
	Limit int
}

// WaitStore is the only production mutation surface for generic waits. The
// unsafe wait CRUD methods deliberately are not part of StateStore.
type WaitStore interface {
	LoadWait(context.Context, WaitID) (WaitSnapshot, error)
	LoadNodeInvocation(context.Context, NodeInvocationID) (NodeInvocationSnapshot, error)
	LoadAttempt(context.Context, AttemptID) (AttemptSnapshot, error)
	SuspendNodeWait(context.Context, SuspendNodeWaitRequest) (SuspendWaitResult, error)
	ResumeNodeWait(context.Context, ResumeNodeWaitRequest) (ResumeWaitResult, error)
	TimeoutWait(context.Context, TimeoutWaitRequest) (WaitTimeoutResult, error)
	RecoverOpenWaits(context.Context, OpenWaitQuery) ([]WaitSnapshot, error)
}

// ResumeCommand is the adapter-facing raw response. Token is hashed before it
// reaches persistence.
type ResumeCommand struct {
	WaitID         WaitID
	Correlation    string
	Token          string
	WakeSource     workflowwait.WakeSource
	Responder      workflowwait.Responder
	Payload        values.Value
	IdempotencyKey string
	ReceivedAt     time.Time
}

func (c ResumeCommand) Validate() error {
	if err := (WaitRef{ID: c.WaitID}).Validate(); err != nil {
		return err
	}
	if err := validateRequiredText("resume correlation", c.Correlation); err != nil {
		return err
	}
	if !c.WakeSource.Valid() {
		return fmt.Errorf("unsupported resume wake source %q", c.WakeSource)
	}
	if err := c.Responder.Validate(); err != nil {
		return err
	}
	if err := c.Payload.Validate(); err != nil {
		return err
	}
	if c.IdempotencyKey != "" {
		if err := validateRequiredText("resume idempotency key", c.IdempotencyKey); err != nil {
			return err
		}
	}
	if c.ReceivedAt.IsZero() {
		return fmt.Errorf("resume received_at is required")
	}
	return nil
}

// WaitCoordinator owns generic suspension/resume orchestration. Store state is
// authoritative; Scheduler, Materializer, and Authorizer are optional host
// adapters.
type WaitCoordinator struct {
	Store        WaitStore
	Scheduler    workflowwait.ActivationScheduler
	Materializer workflowwait.Materializer
	Authorizer   workflowwait.ResponderAuthorizer
}

// SuspendCommand keeps the one-time raw resume token outside the persistence
// request while allowing the coordinator to bind it to the stored digest and
// reject token-bearing URL paths.
type SuspendCommand struct {
	Request     SuspendNodeWaitRequest
	ResumeToken string
}

func (c WaitCoordinator) Suspend(ctx context.Context, command SuspendCommand) (SuspendWaitResult, error) {
	if ctx == nil || c.Store == nil {
		return SuspendWaitResult{}, fmt.Errorf("wait coordinator requires context and store")
	}
	request := command.Request
	request.At = request.At.UTC()
	request.Wait.CreatedAt, request.Wait.UpdatedAt = request.At, request.At
	if !request.Wait.Deadline.IsZero() {
		request.Wait.Deadline = request.Wait.Deadline.UTC()
	}
	if err := request.Validate(); err != nil {
		return SuspendWaitResult{}, err
	}
	presentedDigest := ""
	if command.ResumeToken != "" {
		tokenDigest, digestErr := workflowwait.DigestToken(command.ResumeToken)
		if digestErr != nil {
			return SuspendWaitResult{}, ErrInvalidResumeToken
		}
		presentedDigest = tokenDigest
	}
	if request.Wait.ResumeTokenDigest != "" || presentedDigest != "" {
		if !workflowwait.EqualTokenDigest(request.Wait.ResumeTokenDigest, presentedDigest) {
			return SuspendWaitResult{}, ErrInvalidResumeToken
		}
	}
	if command.ResumeToken != "" && request.Wait.ResumeURL != "" {
		parsed, parseErr := url.Parse(request.Wait.ResumeURL)
		if parseErr != nil || parsed == nil {
			return SuspendWaitResult{}, fmt.Errorf("wait resume URL is invalid")
		}
		path, unescapeErr := url.PathUnescape(parsed.EscapedPath())
		if unescapeErr != nil || strings.Contains(parsed.Host, command.ResumeToken) || strings.Contains(path, command.ResumeToken) {
			return SuspendWaitResult{}, fmt.Errorf("wait resume URL authority and path must not contain the raw token")
		}
	}
	result, storeErr := c.Store.SuspendNodeWait(ctx, request)
	if storeErr != nil {
		return result, storeErr
	}
	if materializeErr := c.materialize(ctx, result.Wait, false); materializeErr != nil {
		return result, materializeErr
	}
	return result, nil
}

func (c WaitCoordinator) Resume(ctx context.Context, command ResumeCommand) (ResumeWaitResult, error) {
	if ctx == nil || c.Store == nil {
		return ResumeWaitResult{}, fmt.Errorf("wait coordinator requires context and store")
	}
	if err := command.Validate(); err != nil {
		return ResumeWaitResult{}, err
	}
	command.ReceivedAt = command.ReceivedAt.UTC()
	current, loadErr := c.Store.LoadWait(ctx, command.WaitID)
	if loadErr != nil {
		return ResumeWaitResult{}, loadErr
	}
	digest := ""
	if command.Token != "" {
		tokenDigest, digestErr := workflowwait.DigestToken(command.Token)
		if digestErr != nil {
			return ResumeWaitResult{}, ErrInvalidResumeToken
		}
		digest = tokenDigest
	}
	if current.Authority.Kind == "legacy_unresumable" {
		return ResumeWaitResult{}, fmt.Errorf("%w: legacy wait %q has no issued resume credential", ErrWaitUnresumable, command.WaitID)
	}
	if current.ResumeTokenDigest != "" || digest != "" {
		if !workflowwait.EqualTokenDigest(current.ResumeTokenDigest, digest) {
			return ResumeWaitResult{}, ErrInvalidResumeToken
		}
	}
	if current.Correlation != command.Correlation || current.WakeSource != command.WakeSource {
		return ResumeWaitResult{}, fmt.Errorf("%w: correlation or wake source", ErrInvalidRecord)
	}
	if c.Authorizer != nil {
		if err := c.Authorizer.AuthorizeResume(ctx, workflowwait.AuthorizationRequest{Record: current.Record, Source: command.WakeSource, Responder: command.Responder}); err != nil {
			return ResumeWaitResult{}, err
		}
	}
	if err := values.ValidateValueSchema(current.ResumeSchema.Schema, command.Payload); err != nil {
		return ResumeWaitResult{}, err
	}
	node, nodeErr := c.Store.LoadNodeInvocation(ctx, current.Invocation)
	if nodeErr != nil {
		return ResumeWaitResult{}, nodeErr
	}
	attempt, attemptErr := c.Store.LoadAttempt(ctx, AttemptID{Invocation: current.Invocation, Number: node.LatestAttempt})
	if attemptErr != nil {
		return ResumeWaitResult{}, attemptErr
	}
	if current.Status == WaitOpen && !current.Deadline.IsZero() && !command.ReceivedAt.Before(current.Deadline) {
		timedOut, timeoutErr := c.Store.TimeoutWait(ctx, TimeoutWaitRequest{
			WaitID: current.Ref.ID, ExpectedWaitGeneration: current.Generation,
			ExpectedNodeGeneration: node.Generation, IdempotencyKey: timeoutActivationKey(current),
			Deadline: current.Deadline, Now: current.Deadline,
		})
		result := ResumeWaitResult{Outcome: ResumeClosed, Wait: timedOut.Wait, Node: timedOut.Node, Attempt: timedOut.Attempt, Events: timedOut.Events}
		if timeoutErr != nil {
			return result, timeoutErr
		}
		closedErr := error(&WaitClosedError{WaitID: timedOut.Wait.Ref.ID, Status: timedOut.Wait.Status, ResolvedAt: timedOut.Wait.ResolvedAt})
		if cleanupErr := c.resolveAdapters(ctx, timedOut.Wait); cleanupErr != nil {
			closedErr = errors.Join(closedErr, cleanupErr)
		}
		return result, closedErr
	}
	result, err := c.Store.ResumeNodeWait(ctx, ResumeNodeWaitRequest{
		WaitID: command.WaitID, ExpectedWaitGeneration: current.Generation, ExpectedNodeGeneration: node.Generation,
		ExpectedAttemptGeneration: attempt.Generation, Correlation: command.Correlation, PresentedTokenDigest: digest,
		WakeSource: command.WakeSource, Responder: command.Responder, Payload: command.Payload,
		IdempotencyKey: command.IdempotencyKey, ReceivedAt: command.ReceivedAt,
	})
	if err != nil {
		return result, err
	}
	if err := c.resolveAdapters(ctx, result.Wait); err != nil {
		return result, err
	}
	return result, nil
}

// Recover reconstructs open-wait scheduling solely from durable records.
func (c WaitCoordinator) Recover(ctx context.Context, query OpenWaitQuery, now time.Time) ([]WaitTimeoutResult, error) {
	if ctx == nil || c.Store == nil || now.IsZero() {
		return nil, fmt.Errorf("wait recovery requires context, store, and now")
	}
	now = now.UTC()
	waits, err := c.Store.RecoverOpenWaits(ctx, query)
	if err != nil {
		return nil, err
	}
	results := make([]WaitTimeoutResult, 0)
	for _, snapshot := range waits {
		if snapshot.Deadline.IsZero() || now.Before(snapshot.Deadline) {
			if err := c.materialize(ctx, snapshot, false); err != nil {
				return results, fmt.Errorf("%w: %w", ErrWaitRecovery, err)
			}
			continue
		}
		node, loadErr := c.Store.LoadNodeInvocation(ctx, snapshot.Invocation)
		if loadErr != nil {
			return results, loadErr
		}
		result, timeoutErr := c.Store.TimeoutWait(ctx, TimeoutWaitRequest{WaitID: snapshot.Ref.ID, ExpectedWaitGeneration: snapshot.Generation, ExpectedNodeGeneration: node.Generation, IdempotencyKey: timeoutActivationKey(snapshot), Deadline: snapshot.Deadline, Now: snapshot.Deadline})
		if timeoutErr != nil {
			return results, timeoutErr
		}
		results = append(results, result)
		if err := c.resolveAdapters(ctx, result.Wait); err != nil {
			return results, err
		}
	}
	return results, nil
}

func (c WaitCoordinator) materialize(ctx context.Context, snapshot WaitSnapshot, resolving bool) error {
	materialization := workflowwait.Materialization{WaitID: string(snapshot.Ref.ID), Kind: snapshot.Kind, ResumeURL: snapshot.ResumeURL, ExpiresAt: snapshot.Deadline}
	if resolving {
		if c.Materializer != nil {
			if err := c.Materializer.Resolve(ctx, materialization); err != nil {
				return &PostCommitError{Operation: "resolve wait materialization", Err: err}
			}
		}
		return nil
	}
	if c.Scheduler != nil && !snapshot.Deadline.IsZero() {
		activation := timeoutActivation(snapshot)
		if err := c.Scheduler.Schedule(ctx, activation); err != nil {
			return &PostCommitError{Operation: "schedule wait timeout", Err: err}
		}
	}
	if c.Materializer != nil {
		if err := c.Materializer.Materialize(ctx, materialization); err != nil {
			return &PostCommitError{Operation: "materialize wait", Err: err}
		}
	}
	return nil
}

func (c WaitCoordinator) resolveAdapters(ctx context.Context, snapshot WaitSnapshot) error {
	var operational []error
	if c.Scheduler != nil && !snapshot.Deadline.IsZero() {
		if err := c.Scheduler.Cancel(ctx, timeoutActivation(snapshot).ID); err != nil {
			operational = append(operational, &PostCommitError{Operation: "cancel wait timeout", Err: err})
		}
	}
	if c.Materializer != nil {
		materialization := workflowwait.Materialization{WaitID: string(snapshot.Ref.ID), Kind: snapshot.Kind, ResumeURL: snapshot.ResumeURL, ExpiresAt: snapshot.Deadline}
		if err := c.Materializer.Resolve(ctx, materialization); err != nil {
			operational = append(operational, &PostCommitError{Operation: "resolve wait materialization", Err: err})
		}
	}
	return errors.Join(operational...)
}

func timeoutActivation(snapshot WaitSnapshot) workflowwait.Activation {
	id := workflowwait.ActivationID(timeoutActivationKey(snapshot))
	return workflowwait.Activation{ID: id, Kind: "wait_timeout", RunID: string(snapshot.Invocation.RunID), NodeID: snapshot.Invocation.NodeID, Iteration: snapshot.Invocation.Iteration, WaitID: string(snapshot.Ref.ID), FireAt: snapshot.Deadline.UTC(), DedupKey: string(id)}
}

func timeoutActivationKey(snapshot WaitSnapshot) string {
	return "wait-timeout:" + values.SHA256Digest([]byte(snapshot.Ref.ID))[7:]
}
