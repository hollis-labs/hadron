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
	ActivationWaitWake = "wait_wake"
)

var (
	ErrInvalidResumeToken = errors.New("invalid workflow wait resume token")
	ErrWaitClosed         = errors.New("workflow wait is closed")
	ErrWaitUnresumable    = errors.New("workflow wait cannot be resumed")
	ErrWaitRecovery       = errors.New("workflow wait recovery failed")
	ErrWaitWakeNotDue     = errors.New("workflow timer wake is not due")
	ErrWaitWakePending    = errors.New("workflow timer wake precedes timeout")
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

// TimerWakeCommand is the credential-free host handoff for a scheduled timer.
// Correlation, payload, responder, semantic resolution time, and idempotency
// identity are derived exclusively from the durable wait record.
type TimerWakeCommand struct {
	WaitID  WaitID
	FiredAt time.Time
}

// Validate reports malformed timer delivery input. Whether the timer is due
// is checked against the persisted WakeAt after the record is loaded.
func (c TimerWakeCommand) Validate() error {
	if err := (WaitRef{ID: c.WaitID}).Validate(); err != nil {
		return err
	}
	if c.FiredAt.IsZero() {
		return fmt.Errorf("timer fired_at is required")
	}
	return nil
}

// WaitWakeNotDueError reports an early host timer delivery without mutating
// durable state.
type WaitWakeNotDueError struct {
	FiredAt time.Time
	WakeAt  time.Time
}

func (e *WaitWakeNotDueError) Error() string {
	return fmt.Sprintf("%s: fired_at %s precedes wake_at %s", ErrWaitWakeNotDue, e.FiredAt.Format(time.RFC3339Nano), e.WakeAt.Format(time.RFC3339Nano))
}
func (e *WaitWakeNotDueError) Unwrap() error { return ErrWaitWakeNotDue }

// OpenWaitQuery supports deterministic durable recovery. Limit zero means
// unlimited; results are ordered by earliest WakeAt/Deadline, creation time,
// then wait ID.
type OpenWaitQuery struct {
	RunID RunID
	Limit int
}

// WaitRecoveryResults contains every durable transition applied while
// recovering open waits. Future waits are only re-materialized.
type WaitRecoveryResults struct {
	Woken    []ResumeWaitResult
	TimedOut []WaitTimeoutResult
}

// WaitStore is the only production mutation surface for generic waits. The
// unsafe wait CRUD methods deliberately are not part of StateStore.
type WaitStore interface {
	LoadWait(context.Context, WaitID) (WaitSnapshot, error)
	LoadNodeInvocation(context.Context, NodeInvocationID) (NodeInvocationSnapshot, error)
	LoadAttempt(context.Context, AttemptID) (AttemptSnapshot, error)
	LoadWaitContinuation(context.Context, AttemptID) (WaitSnapshot, error)
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
	if !request.Wait.WakeAt.IsZero() {
		request.Wait.WakeAt = request.Wait.WakeAt.UTC()
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
	return c.resume(ctx, command, false)
}

func (c WaitCoordinator) resume(ctx context.Context, command ResumeCommand, coreTimer bool) (ResumeWaitResult, error) {
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
	if current.Kind == workflowwait.KindTimer && current.WakeSource == workflowwait.WakeTimer && !coreTimer {
		return ResumeWaitResult{}, fmt.Errorf("%w: successful timers must use WakeTimer", ErrInvalidRecord)
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
		if err := ctx.Err(); err != nil {
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

// WakeTimer resolves a due successful timer through the same authorized,
// atomic, idempotent resume path as every external wake. Late delivery uses
// persisted WakeAt as the semantic resolution time, so a scheduled wake that
// precedes a later Deadline wins deterministically after recovery.
func (c WaitCoordinator) WakeTimer(ctx context.Context, command TimerWakeCommand) (ResumeWaitResult, error) {
	if ctx == nil || c.Store == nil {
		return ResumeWaitResult{}, fmt.Errorf("wait coordinator requires context and store")
	}
	if err := command.Validate(); err != nil {
		return ResumeWaitResult{}, err
	}
	command.FiredAt = command.FiredAt.UTC()
	current, err := c.Store.LoadWait(ctx, command.WaitID)
	if err != nil {
		return ResumeWaitResult{}, err
	}
	if current.Kind != workflowwait.KindTimer || current.WakeSource != workflowwait.WakeTimer || current.WakeAt.IsZero() {
		return ResumeWaitResult{}, fmt.Errorf("%w: wait is not a successful timer", ErrInvalidRecord)
	}
	if command.FiredAt.Before(current.WakeAt) {
		return ResumeWaitResult{}, &WaitWakeNotDueError{FiredAt: command.FiredAt, WakeAt: current.WakeAt}
	}
	payload, err := values.NewInline(map[string]any{"woke_at": current.WakeAt.UTC().Format(time.RFC3339Nano)}, values.Metadata{
		Producer:  values.Producer{Kind: "timer", Reference: string(current.Ref.ID), Output: ResumeValueName},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		return ResumeWaitResult{}, err
	}
	return c.resume(ctx, ResumeCommand{
		WaitID: current.Ref.ID, Correlation: current.Correlation, WakeSource: workflowwait.WakeTimer,
		Responder: workflowwait.Responder{Kind: "system", Reference: "wait-timer"}, Payload: payload,
		IdempotencyKey: timerActivationKey(current), ReceivedAt: current.WakeAt.UTC(),
	}, true)
}

// Recover reconstructs open-wait scheduling solely from durable records.
func (c WaitCoordinator) Recover(ctx context.Context, query OpenWaitQuery, now time.Time) ([]WaitTimeoutResult, error) {
	result, err := c.RecoverWaits(ctx, query, now)
	return result.TimedOut, err
}

// RecoverWaits deterministically fires due successful timers, times out due
// deadlines, and re-materializes future waits. Store recovery ordering is by
// the earliest applicable WakeAt or Deadline and respects query Limit.
func (c WaitCoordinator) RecoverWaits(ctx context.Context, query OpenWaitQuery, now time.Time) (WaitRecoveryResults, error) {
	if ctx == nil || c.Store == nil || now.IsZero() {
		return WaitRecoveryResults{}, fmt.Errorf("wait recovery requires context, store, and now")
	}
	now = now.UTC()
	waits, err := c.Store.RecoverOpenWaits(ctx, query)
	if err != nil {
		return WaitRecoveryResults{}, err
	}
	results := WaitRecoveryResults{}
	for _, snapshot := range waits {
		if !snapshot.WakeAt.IsZero() && !now.Before(snapshot.WakeAt) {
			result, wakeErr := c.WakeTimer(ctx, TimerWakeCommand{WaitID: snapshot.Ref.ID, FiredAt: now})
			if result.Outcome != "" {
				results.Woken = append(results.Woken, result)
			}
			if wakeErr != nil {
				return results, wakeErr
			}
			continue
		}
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
		results.TimedOut = append(results.TimedOut, result)
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
	if c.Scheduler != nil && !snapshot.WakeAt.IsZero() {
		activation := TimerActivation(snapshot)
		if err := c.Scheduler.Schedule(ctx, activation); err != nil {
			return &PostCommitError{Operation: "schedule wait wake", Err: err}
		}
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
	if c.Scheduler != nil && !snapshot.WakeAt.IsZero() {
		if err := c.Scheduler.Cancel(ctx, TimerActivation(snapshot).ID); err != nil {
			operational = append(operational, &PostCommitError{Operation: "cancel wait wake", Err: err})
		}
	}
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

// TimerActivation converts a durable successful timer into the host scheduler
// envelope. Identity depends only on immutable wait ID plus activation kind.
func TimerActivation(snapshot WaitSnapshot) workflowwait.Activation {
	id := workflowwait.ActivationID(timerActivationKey(snapshot))
	return workflowwait.Activation{ID: id, Kind: ActivationWaitWake, RunID: string(snapshot.Invocation.RunID), NodeID: snapshot.Invocation.NodeID, Iteration: snapshot.Invocation.Iteration, WaitID: string(snapshot.Ref.ID), FireAt: snapshot.WakeAt.UTC(), DedupKey: string(id)}
}

func timerActivationKey(snapshot WaitSnapshot) string {
	return "wait-wake:" + values.SHA256Digest([]byte(snapshot.Ref.ID))[7:]
}
