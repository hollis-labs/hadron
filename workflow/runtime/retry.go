package runtime

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

const (
	EventRetryScheduled = "retry.scheduled"
	EventRetryActivated = "retry.activated"
	EventRetryCanceled  = "retry.canceled"
)

var (
	ErrInvalidRetryPolicy = errors.New("invalid workflow retry policy")
	ErrRetryNotDue        = errors.New("workflow retry activation is not due")
	ErrRetryDenied        = errors.New("workflow retry is not permitted")
)

// RetryActivationStatus is the closed durable lifecycle for one retry timer.
type RetryActivationStatus string

const (
	RetryScheduled RetryActivationStatus = "scheduled"
	RetryActivated RetryActivationStatus = "activated"
	RetryCanceled  RetryActivationStatus = "canceled"
)

// Valid reports whether s is a supported retry-activation state.
func (s RetryActivationStatus) Valid() bool {
	switch s {
	case RetryScheduled, RetryActivated, RetryCanceled:
		return true
	default:
		return false
	}
}

// RetryActivationSnapshot is the restart-durable timer produced when one
// unsuccessful attempt is closed for a later retry. Attempt names the closed
// attempt; activation creates exactly Attempt.Number+1 when the node is next
// claimed and dispatched.
type RetryActivationSnapshot struct {
	ID         string                `json:"id"`
	Attempt    AttemptID             `json:"attempt"`
	Failure    Failure               `json:"failure"`
	FireAt     time.Time             `json:"fire_at"`
	Status     RetryActivationStatus `json:"status"`
	Generation uint64                `json:"generation"`
	CreatedAt  time.Time             `json:"created_at"`
	UpdatedAt  time.Time             `json:"updated_at"`
}

// Validate checks durable retry state without deciding retry eligibility.
func (s RetryActivationSnapshot) Validate() error {
	if err := validateRequiredText("retry activation id", s.ID); err != nil {
		return err
	}
	if err := s.Attempt.Validate(); err != nil {
		return err
	}
	if err := s.Failure.Validate(); err != nil {
		return err
	}
	if !s.Status.Valid() {
		return fmt.Errorf("unsupported retry activation status %q", s.Status)
	}
	if s.FireAt.IsZero() || s.FireAt.Before(s.CreatedAt) {
		return fmt.Errorf("retry fire_at must not precede creation")
	}
	return validateSnapshotTimes(s.Generation, s.CreatedAt, s.UpdatedAt)
}

// RetryAuthorizationRequest is the narrow host-policy seam for retrying
// externally consequential effects. The host returns nil to grant the retry.
type RetryAuthorizationRequest struct {
	Node           graph.Node
	Spec           stepkind.StepKindSpec
	AttemptNumber  int
	Failure        Failure
	AttemptStatus  NodeStatus
	IdempotencyKey string
}

// RetryAuthorizer may grant an otherwise policy-gated mutate, materialize, or
// destructive retry. It performs no persistence or adapter I/O.
type RetryAuthorizer interface {
	AuthorizeRetry(context.Context, RetryAuthorizationRequest) error
}

// RetryEvaluationRequest contains all immutable facts used to decide whether
// another attempt may be scheduled.
type RetryEvaluationRequest struct {
	Node           graph.Node
	Spec           stepkind.StepKindSpec
	AttemptNumber  int
	Failure        Failure
	AttemptStatus  NodeStatus
	Timeout        TimeoutKind
	IdempotencyKey string
	FailedAt       time.Time
}

// RetryDecision is deterministic for a request and policy decision. Retry is
// false for normal policy exhaustion as well as denied unsafe retries; Reason
// is a stable searchable explanation.
type RetryDecision struct {
	Retry        bool
	Reason       string
	MatchedClass string
	Delay        time.Duration
	FireAt       time.Time
}

const (
	RetryReasonEligible           = "eligible"
	RetryReasonNoPolicy           = "no_policy"
	RetryReasonAttemptsExhausted  = "attempts_exhausted"
	RetryReasonClassNotSelected   = "class_not_selected"
	RetryReasonPermanentFailure   = "permanent_failure"
	RetryReasonCanceled           = "canceled"
	RetryReasonKindUnsupported    = "kind_unsupported"
	RetryReasonIdempotencyMissing = "idempotency_missing"
	RetryReasonEffectDenied       = "effect_denied"
)

// RetryEvaluator implements graph retry policy without retaining state.
type RetryEvaluator struct {
	Authorizer RetryAuthorizer
}

// Evaluate validates the policy and returns an effect-aware retry decision.
func (e RetryEvaluator) Evaluate(ctx context.Context, request RetryEvaluationRequest) (RetryDecision, error) {
	if ctx == nil {
		return RetryDecision{}, fmt.Errorf("%w: context is required", ErrInvalidRetryPolicy)
	}
	if request.Node.ID == "" {
		return RetryDecision{}, fmt.Errorf("%w: node id is required", ErrInvalidRetryPolicy)
	}
	if request.AttemptNumber < 1 || request.FailedAt.IsZero() {
		return RetryDecision{}, fmt.Errorf("%w: attempt number and failure time are required", ErrInvalidRetryPolicy)
	}
	if err := request.Failure.Validate(); err != nil {
		return RetryDecision{}, fmt.Errorf("%w: %w", ErrInvalidRetryPolicy, err)
	}
	if request.AttemptStatus != NodeFailed && request.AttemptStatus != NodeTimedOut && request.AttemptStatus != NodeCrashed && request.AttemptStatus != NodeCanceled {
		return RetryDecision{}, fmt.Errorf("%w: unsupported attempt outcome %q", ErrInvalidRetryPolicy, request.AttemptStatus)
	}
	if request.Timeout != "" && !request.Timeout.Valid() {
		return RetryDecision{}, fmt.Errorf("%w: unsupported timeout class %q", ErrInvalidRetryPolicy, request.Timeout)
	}
	policy := request.Node.Retry
	if policy == nil {
		return RetryDecision{Reason: RetryReasonNoPolicy}, nil
	}
	if err := ValidateRetryPolicy(*policy); err != nil {
		return RetryDecision{}, err
	}
	if request.AttemptStatus == NodeCanceled {
		return RetryDecision{Reason: RetryReasonCanceled}, nil
	}
	if request.AttemptNumber >= policy.Attempts {
		return RetryDecision{Reason: RetryReasonAttemptsExhausted}, nil
	}
	matched, classSelected := retryClassSelected(*policy, request)
	if !classSelected {
		return RetryDecision{Reason: RetryReasonClassNotSelected}, nil
	}
	if !request.Failure.Retryable && request.AttemptStatus != NodeTimedOut && len(policy.On) == 0 {
		return RetryDecision{Reason: RetryReasonPermanentFailure}, nil
	}
	if request.Spec.RetrySafety == stepkind.RetryUnsupported {
		return RetryDecision{Reason: RetryReasonKindUnsupported, MatchedClass: matched}, nil
	}
	if requireRetryIdempotency(request) != nil {
		//nolint:nilerr // A safety denial is a normal non-retry decision, not evaluator failure.
		return RetryDecision{Reason: RetryReasonIdempotencyMissing, MatchedClass: matched}, nil
	}
	if e.authorizeEffects(ctx, request) != nil {
		//nolint:nilerr // A policy denial is a normal non-retry decision; the durable reason is EffectDenied.
		return RetryDecision{Reason: RetryReasonEffectDenied, MatchedClass: matched}, nil
	}
	delay, err := CalculateBackoff(policy.Backoff, request.AttemptNumber)
	if err != nil {
		return RetryDecision{}, err
	}
	return RetryDecision{
		Retry: true, Reason: RetryReasonEligible, MatchedClass: matched,
		Delay: delay, FireAt: request.FailedAt.UTC().Add(delay),
	}, nil
}

// ValidateRetryPolicy rejects malformed attempts, class filters, and duration
// arithmetic before a failure can mutate durable state.
func ValidateRetryPolicy(policy graph.RetryPolicy) error {
	if policy.Attempts < 1 {
		return fmt.Errorf("%w: attempts must be positive", ErrInvalidRetryPolicy)
	}
	seen := make(map[string]struct{}, len(policy.On))
	for index, class := range policy.On {
		if err := validateRequiredText(fmt.Sprintf("retry on[%d]", index), class); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidRetryPolicy, err)
		}
		if class != strings.ToLower(class) {
			return fmt.Errorf("%w: retry class %q must be lowercase", ErrInvalidRetryPolicy, class)
		}
		if _, duplicate := seen[class]; duplicate {
			return fmt.Errorf("%w: duplicate retry class %q", ErrInvalidRetryPolicy, class)
		}
		seen[class] = struct{}{}
	}
	_, err := CalculateBackoff(policy.Backoff, 1)
	return err
}

// CalculateBackoff returns the delay after failedAttempt. It uses checked
// duration arithmetic and caps the result at max_delay when supplied.
func CalculateBackoff(policy graph.BackoffPolicy, failedAttempt int) (time.Duration, error) {
	if failedAttempt < 1 {
		return 0, fmt.Errorf("%w: failed attempt must be positive", ErrInvalidRetryPolicy)
	}
	strategy := policy.Strategy
	if strategy == "" {
		strategy = graph.BackoffNone
	}
	if !strategy.Valid() {
		return 0, fmt.Errorf("%w: unsupported backoff strategy %q", ErrInvalidRetryPolicy, strategy)
	}
	initial, err := parseRetryDuration("initial_delay", policy.InitialDelay)
	if err != nil {
		return 0, err
	}
	maximum, err := parseRetryDuration("max_delay", policy.MaxDelay)
	if err != nil {
		return 0, err
	}
	if strategy != graph.BackoffNone && initial <= 0 {
		return 0, fmt.Errorf("%w: %s backoff requires positive initial_delay", ErrInvalidRetryPolicy, strategy)
	}
	if maximum > 0 && initial > maximum {
		return 0, fmt.Errorf("%w: initial_delay exceeds max_delay", ErrInvalidRetryPolicy)
	}
	multiplier := policy.Multiplier
	if multiplier == 0 {
		multiplier = 2
	}
	if math.IsNaN(multiplier) || math.IsInf(multiplier, 0) || multiplier <= 0 {
		return 0, fmt.Errorf("%w: multiplier must be finite and positive", ErrInvalidRetryPolicy)
	}
	var factor float64
	switch strategy {
	case graph.BackoffNone:
		return 0, nil
	case graph.BackoffFixed:
		factor = 1
	case graph.BackoffLinear:
		factor = 1 + float64(failedAttempt-1)*multiplier
	case graph.BackoffExponential:
		factor = math.Pow(multiplier, float64(failedAttempt-1))
	}
	if math.IsInf(factor, 0) || factor > float64(math.MaxInt64)/float64(initial) {
		if maximum > 0 {
			return maximum, nil
		}
		return 0, fmt.Errorf("%w: backoff duration overflows", ErrInvalidRetryPolicy)
	}
	delay := time.Duration(float64(initial) * factor)
	if maximum > 0 && delay > maximum {
		delay = maximum
	}
	return delay, nil
}

func parseRetryDuration(name string, raw graph.Duration) (time.Duration, error) {
	if raw == "" {
		return 0, nil
	}
	duration, err := time.ParseDuration(string(raw))
	if err != nil || duration < 0 {
		return 0, fmt.Errorf("%w: invalid %s %q", ErrInvalidRetryPolicy, name, raw)
	}
	return duration, nil
}

func retryClassSelected(policy graph.RetryPolicy, request RetryEvaluationRequest) (string, bool) {
	classes := []string{request.Failure.Code}
	if request.Failure.Retryable {
		classes = append(classes, "retryable")
	}
	if request.AttemptStatus == NodeTimedOut {
		classes = append(classes, "timeout")
		if request.Timeout != "" {
			classes = append(classes, string(request.Timeout))
		}
	}
	if request.AttemptStatus == NodeCrashed {
		classes = append(classes, "crashed")
	}
	if len(policy.On) == 0 {
		if request.Failure.Retryable || request.AttemptStatus == NodeTimedOut || request.AttemptStatus == NodeCrashed {
			return classes[0], true
		}
		return "", false
	}
	selected := make(map[string]struct{}, len(policy.On))
	for _, class := range policy.On {
		selected[class] = struct{}{}
	}
	for _, class := range classes {
		if _, ok := selected[class]; ok {
			return class, true
		}
	}
	return "", false
}

func requireRetryIdempotency(request RetryEvaluationRequest) error {
	mode, err := effectiveRetryIdempotency(request.Spec.Idempotency, request.Node.Idempotency)
	if err != nil {
		return err
	}
	if mode == graph.IdempotencyKeyed && strings.TrimSpace(request.IdempotencyKey) == "" {
		return ErrRetryDenied
	}
	if request.Spec.RetrySafety == stepkind.RetryRequiresIdempotency && mode != graph.IdempotencyIntrinsic && strings.TrimSpace(request.IdempotencyKey) == "" {
		return ErrRetryDenied
	}
	return nil
}

// effectiveRetryIdempotency combines trusted executor metadata with the
// workflow declaration as a constraint. A graph may require a key or disable
// retries for an otherwise stronger executor, but it cannot upgrade the
// executor's intrinsic guarantee or turn keyed behavior into intrinsic.
func effectiveRetryIdempotency(specMode graph.IdempotencyMode, node *graph.IdempotencySpec) (graph.IdempotencyMode, error) {
	if !specMode.Valid() {
		return "", ErrRetryDenied
	}
	if node == nil {
		return specMode, nil
	}
	if !node.Mode.Valid() {
		return "", ErrRetryDenied
	}
	switch specMode {
	case graph.IdempotencyNone:
		return graph.IdempotencyNone, nil
	case graph.IdempotencyKeyed:
		if node.Mode == graph.IdempotencyNone {
			return graph.IdempotencyNone, nil
		}
		return graph.IdempotencyKeyed, nil
	case graph.IdempotencyIntrinsic:
		return node.Mode, nil
	default:
		return "", ErrRetryDenied
	}
}

func (e RetryEvaluator) authorizeEffects(ctx context.Context, request RetryEvaluationRequest) error {
	// Both declarations constrain retries. A workflow node may add stricter
	// effects, but cannot narrow trusted adapter metadata to bypass safety.
	effects := append(graph.EffectSet(nil), request.Spec.Effects...)
	effects = append(effects, request.Node.Effects...)
	effectSet := make(map[graph.Effect]struct{}, len(effects))
	unique := effects[:0]
	for _, effect := range effects {
		if _, exists := effectSet[effect]; exists {
			continue
		}
		effectSet[effect] = struct{}{}
		unique = append(unique, effect)
	}
	effects = unique
	sort.Slice(effects, func(i, j int) bool { return effects[i] < effects[j] })
	mode, err := effectiveRetryIdempotency(request.Spec.Idempotency, request.Node.Idempotency)
	if err != nil {
		return err
	}
	keyed := mode == graph.IdempotencyKeyed && strings.TrimSpace(request.IdempotencyKey) != ""
	needsGrant := false
	for _, effect := range effects {
		switch effect {
		case graph.EffectRead, graph.EffectCompute:
		case graph.EffectMaterialize:
			if mode != graph.IdempotencyIntrinsic && !keyed {
				needsGrant = true
			}
		case graph.EffectMutate:
			if !keyed {
				needsGrant = true
			}
		case graph.EffectDestructive:
			switch request.Spec.RetrySafety {
			case stepkind.RetrySafe:
			case stepkind.RetryRequiresIdempotency:
				if mode != graph.IdempotencyIntrinsic && (mode != graph.IdempotencyKeyed || !keyed) {
					return ErrRetryDenied
				}
			default:
				return ErrRetryDenied
			}
			needsGrant = true
		default:
			return ErrRetryDenied
		}
	}
	if !needsGrant {
		return nil
	}
	if e.Authorizer == nil {
		return ErrRetryDenied
	}
	return e.Authorizer.AuthorizeRetry(ctx, RetryAuthorizationRequest{
		Node: request.Node, Spec: request.Spec, AttemptNumber: request.AttemptNumber,
		Failure: request.Failure, AttemptStatus: request.AttemptStatus,
		IdempotencyKey: request.IdempotencyKey,
	})
}

// ScheduleNodeRetryRequest atomically closes the current attempt, releases
// its claim, sets the aggregate node waiting, and inserts one scheduled timer.
type ScheduleNodeRetryRequest struct {
	Activation                RetryActivationSnapshot
	ExpectedNodeGeneration    uint64
	ExpectedAttemptGeneration uint64
	Claim                     ClaimProof
	AttemptStatus             NodeStatus
	At                        time.Time
}

func (r ScheduleNodeRetryRequest) Validate() error {
	if r.Activation.Generation != 0 || r.Activation.Status != RetryScheduled {
		return fmt.Errorf("new retry activation must be scheduled with zero generation")
	}
	if r.ExpectedNodeGeneration == 0 || r.ExpectedAttemptGeneration == 0 || r.At.IsZero() {
		return fmt.Errorf("retry schedule requires generations and timestamp")
	}
	if err := r.Claim.Validate(); err != nil {
		return err
	}
	if r.AttemptStatus != NodeFailed && r.AttemptStatus != NodeTimedOut && r.AttemptStatus != NodeCrashed {
		return fmt.Errorf("retry schedule requires failed, timed_out, or crashed attempt")
	}
	candidate := r.Activation
	candidate.Status = RetryScheduled
	candidate.Generation = 1
	candidate.CreatedAt, candidate.UpdatedAt = r.At, r.At
	return candidate.Validate()
}

type ScheduleNodeRetryResult struct {
	Activation RetryActivationSnapshot
	Node       NodeInvocationSnapshot
	Attempt    AttemptSnapshot
	Events     []Event
}

// ActivateNodeRetryRequest fires a due activation exactly once. IdempotencyKey
// distinguishes same-intent replay from conflicting activation attempts.
type ActivateNodeRetryRequest struct {
	ActivationID                 string
	ExpectedActivationGeneration uint64
	ExpectedNodeGeneration       uint64
	IdempotencyKey               string
	Now                          time.Time
}

func (r ActivateNodeRetryRequest) Validate() error {
	if err := validateRequiredText("retry activation id", r.ActivationID); err != nil {
		return err
	}
	if r.ExpectedActivationGeneration == 0 || r.ExpectedNodeGeneration == 0 {
		return fmt.Errorf("retry activation requires generations")
	}
	if err := validateRequiredText("retry activation idempotency key", r.IdempotencyKey); err != nil {
		return err
	}
	if r.Now.IsZero() {
		return fmt.Errorf("retry activation now is required")
	}
	return nil
}

type ActivateNodeRetryResult struct {
	Outcome    IdempotencyOutcome
	Activation RetryActivationSnapshot
	Node       NodeInvocationSnapshot
	Event      *Event
}

type RetryActivationQuery struct {
	RunID     RunID
	DueBefore time.Time
	Limit     int
}

// RetryStore is the atomic persistence surface for retry timers.
type RetryStore interface {
	LoadRetryActivation(context.Context, string) (RetryActivationSnapshot, error)
	ScheduleNodeRetry(context.Context, ScheduleNodeRetryRequest) (ScheduleNodeRetryResult, error)
	ActivateNodeRetry(context.Context, ActivateNodeRetryRequest) (ActivateNodeRetryResult, error)
	RecoverRetryActivations(context.Context, RetryActivationQuery) ([]RetryActivationSnapshot, error)
}

// RetryCoordinator evaluates an unsuccessful attempt, atomically closes it
// into a durable retry wait, and only then notifies the optional host timer.
// A scheduler failure cannot undo the persisted activation; recovery remains
// authoritative and may schedule it again idempotently.
type RetryCoordinator struct {
	Store     RetryStore
	Evaluator RetryEvaluator
	Scheduler workflowwait.ActivationScheduler
}

// ScheduleRetryCommand binds a policy evaluation to the currently fenced
// running attempt.
type ScheduleRetryCommand struct {
	Node           graph.Node
	Spec           stepkind.StepKindSpec
	NodeSnapshot   NodeInvocationSnapshot
	Attempt        AttemptSnapshot
	Claim          ClaimProof
	Failure        Failure
	AttemptStatus  NodeStatus
	Timeout        TimeoutKind
	IdempotencyKey string
	At             time.Time
}

// Schedule returns a non-retry decision without mutating state. A true retry
// decision always accompanies a durable result, even if host scheduling then
// returns an operational error.
func (c RetryCoordinator) Schedule(ctx context.Context, command ScheduleRetryCommand) (ScheduleNodeRetryResult, RetryDecision, error) {
	if ctx == nil || c.Store == nil {
		return ScheduleNodeRetryResult{}, RetryDecision{}, fmt.Errorf("%w: retry coordinator requires context and store", ErrInvalidRetryPolicy)
	}
	if command.NodeSnapshot.ID != command.Attempt.ID.Invocation || command.Attempt.ID.Number < 1 || command.At.IsZero() {
		return ScheduleNodeRetryResult{}, RetryDecision{}, fmt.Errorf("%w: retry command identity or timestamp is invalid", ErrInvalidRetryPolicy)
	}
	decision, err := c.Evaluator.Evaluate(ctx, RetryEvaluationRequest{
		Node: command.Node, Spec: command.Spec, AttemptNumber: command.Attempt.ID.Number,
		Failure: command.Failure, AttemptStatus: command.AttemptStatus, Timeout: command.Timeout,
		IdempotencyKey: command.IdempotencyKey, FailedAt: command.At,
	})
	if err != nil || !decision.Retry {
		return ScheduleNodeRetryResult{}, decision, err
	}
	activationID, err := retryActivationID(command.Attempt.ID)
	if err != nil {
		return ScheduleNodeRetryResult{}, decision, fmt.Errorf("%w: %w", ErrInvalidRetryPolicy, err)
	}
	activation := RetryActivationSnapshot{
		ID: activationID, Attempt: command.Attempt.ID,
		Failure: command.Failure, FireAt: decision.FireAt, Status: RetryScheduled,
	}
	result, err := c.Store.ScheduleNodeRetry(context.WithoutCancel(ctx), ScheduleNodeRetryRequest{
		Activation: activation, ExpectedNodeGeneration: command.NodeSnapshot.Generation,
		ExpectedAttemptGeneration: command.Attempt.Generation, Claim: command.Claim,
		AttemptStatus: command.AttemptStatus, At: command.At,
	})
	if err != nil {
		return ScheduleNodeRetryResult{}, decision, err
	}
	if c.Scheduler != nil {
		if err := c.Scheduler.Schedule(context.WithoutCancel(ctx), RetryActivation(result.Activation)); err != nil {
			return result, decision, fmt.Errorf("schedule durable retry activation: %w", err)
		}
	}
	return result, decision, nil
}

func retryActivationID(attempt AttemptID) (string, error) {
	encoded, err := EncodeAttemptIdentity(attempt)
	if err != nil {
		return "", err
	}
	return "retry:" + encoded, nil
}

// RetryActivation converts the durable retry record to the host scheduler's
// application-neutral activation envelope.
func RetryActivation(snapshot RetryActivationSnapshot) workflowwait.Activation {
	id := workflowwait.ActivationID(snapshot.ID)
	return workflowwait.Activation{
		ID: id, Kind: "node_retry", RunID: string(snapshot.Attempt.Invocation.RunID),
		NodeID: snapshot.Attempt.Invocation.NodeID, Iteration: snapshot.Attempt.Invocation.Iteration,
		FireAt: snapshot.FireAt.UTC(), DedupKey: string(id),
	}
}
