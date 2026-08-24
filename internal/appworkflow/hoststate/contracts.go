// Package hoststate defines Hadron-owned durable host records shared by the
// appworkflow service and its SQLite adapter. It deliberately lives outside
// workflow core: these identities and policy facts are application concerns.
package hoststate

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

var (
	ErrConflict      = errors.New("workflow host journal conflict")
	ErrInvalidRecord = errors.New("invalid workflow host journal record")
)

// StartPhase is the restart-durable root materialization checkpoint.
type StartPhase string

const (
	StartRecorded          StartPhase = "recorded"
	StartRunCreated        StartPhase = "run_created"
	StartNodesMaterialized StartPhase = "nodes_materialized"
	StartRunning           StartPhase = "running"
	StartDryRunComplete    StartPhase = "dry_run_complete"
)

func (p StartPhase) Valid() bool {
	switch p {
	case StartRecorded, StartRunCreated, StartNodesMaterialized, StartRunning, StartDryRunComplete:
		return true
	default:
		return false
	}
}

func (p StartPhase) Terminal() bool { return p == StartRunning || p == StartDryRunComplete }

// IdentityBinding is Hadron's exact authenticated identity, logical scope, and
// optional compute target binding. Runtime BoundRun remains application
// neutral; this product-owned envelope is persisted beside it.
type IdentityBinding struct {
	Principal       string            `json:"principal"`
	SourceAuthority string            `json:"source_authority"`
	Trust           string            `json:"trust"`
	Grants          []string          `json:"grants,omitempty"`
	RunScope        RunScope          `json:"run_scope"`
	ExecutionTarget *ExecutionTarget  `json:"execution_target,omitempty"`
	Extension       map[string]string `json:"extension,omitempty"`
}

func (b IdentityBinding) Validate() error {
	for _, field := range []struct{ name, value string }{
		{"principal", b.Principal}, {"source_authority", b.SourceAuthority},
		{"trust", b.Trust},
	} {
		if err := ValidatePublicText(field.value, 256, true); err != nil {
			return fmt.Errorf("%s is invalid", field.name)
		}
	}
	if err := b.RunScope.Validate(); err != nil {
		return err
	}
	if b.ExecutionTarget != nil {
		if err := b.ExecutionTarget.Validate(); err != nil {
			return err
		}
	}
	if err := validateSortedUnique(b.Grants, MaximumIdentityGrants, "identity grants"); err != nil {
		return err
	}
	return ValidatePublicAttributes(b.Extension)
}

// Clone returns a defensive copy with canonical UTC target timestamps.
func (b IdentityBinding) Clone() IdentityBinding {
	b.Grants = append([]string(nil), b.Grants...)
	b.Extension = cloneStringMap(b.Extension)
	b.RunScope = b.RunScope.Clone()
	if b.ExecutionTarget != nil {
		target := b.ExecutionTarget.Clone()
		b.ExecutionTarget = &target
	}
	return b
}

// PolicyFacts is the normalized pre-execution policy input.
type PolicyFacts struct {
	Operation            string                                       `json:"operation"`
	RunID                runtime.RunID                                `json:"run_id"`
	Plan                 runtime.PlanRef                              `json:"plan"`
	Identity             IdentityBinding                              `json:"identity"`
	RunScope             RunScope                                     `json:"run_scope"`
	ExecutionTarget      *ExecutionTarget                             `json:"execution_target,omitempty"`
	Effects              graph.EffectSet                              `json:"effects"`
	RequiredCapabilities []string                                     `json:"required_capabilities,omitempty"`
	TargetRequirements   map[string]graph.ExecutionTargetRequirements `json:"target_requirements,omitempty"`
	UnresolvedCallNodes  []string                                     `json:"unresolved_call_nodes,omitempty"`
	NodeCount            int                                          `json:"node_count"`
	BlastRadius          map[string]int                               `json:"blast_radius"`
	DryRunAvailable      bool                                         `json:"dry_run_available"`
	ConfirmationAdvised  bool                                         `json:"confirmation_advised"`
}

func (f PolicyFacts) Validate() error {
	if strings.TrimSpace(f.Operation) == "" || f.NodeCount < 0 {
		return errors.New("policy facts require operation and non-negative node count")
	}
	if err := f.Plan.Validate(); err != nil {
		return err
	}
	if err := f.Identity.Validate(); err != nil {
		return err
	}
	if !reflect.DeepEqual(f.RunScope, f.Identity.RunScope) || !reflect.DeepEqual(f.ExecutionTarget, f.Identity.ExecutionTarget) {
		return errors.New("policy scope and target must match the authenticated identity binding")
	}
	if !sort.StringsAreSorted(f.RequiredCapabilities) {
		return errors.New("required capabilities must be sorted")
	}
	if !sort.StringsAreSorted(f.UnresolvedCallNodes) {
		return errors.New("unresolved call nodes must be sorted")
	}
	for key, count := range f.BlastRadius {
		if strings.TrimSpace(key) == "" || count < 0 {
			return errors.New("blast radius requires named non-negative counts")
		}
	}
	if err := ValidateExecutionTargetBinding(f.ExecutionTarget, f.RequiredCapabilities, f.TargetRequirements); err != nil {
		return err
	}
	return nil
}

type PolicyOutcome string

const (
	PolicyAllow   PolicyOutcome = "allow"
	PolicyDeny    PolicyOutcome = "deny"
	PolicyConfirm PolicyOutcome = "confirm"
)

func (o PolicyOutcome) Valid() bool { return o == PolicyAllow || o == PolicyDeny || o == PolicyConfirm }

// PolicyDecision is an append-only, non-secret operational fact.
type PolicyDecision struct {
	ID         string            `json:"id"`
	RunID      runtime.RunID     `json:"run_id"`
	Operation  string            `json:"operation"`
	Outcome    PolicyOutcome     `json:"outcome"`
	Reason     string            `json:"reason"`
	Attributes map[string]string `json:"attributes,omitempty"`
	DecidedAt  time.Time         `json:"decided_at"`
}

func (d PolicyDecision) Validate() error {
	if strings.TrimSpace(d.ID) == "" || strings.TrimSpace(string(d.RunID)) == "" ||
		strings.TrimSpace(d.Operation) == "" || !d.Outcome.Valid() ||
		strings.TrimSpace(d.Reason) == "" || d.DecidedAt.IsZero() {
		return errors.New("policy decision requires identity, operation, outcome, reason, and time")
	}
	return validateMap(d.Attributes)
}

// PolicyEvaluation preserves the facts and immutable caller intent that
// produced a decision, including denied requests that never create a Run.
type PolicyEvaluation struct {
	StartKey      string         `json:"start_key"`
	RequestDigest string         `json:"request_digest"`
	Facts         PolicyFacts    `json:"facts"`
	Decision      PolicyDecision `json:"decision"`
}

func (e PolicyEvaluation) Validate() error {
	if strings.TrimSpace(e.StartKey) == "" || strings.TrimSpace(e.RequestDigest) == "" {
		return errors.New("policy evaluation start key and request digest are required")
	}
	if err := values.ValidateDigest(e.RequestDigest); err != nil {
		return fmt.Errorf("policy request digest: %w", err)
	}
	if err := e.Facts.Validate(); err != nil {
		return err
	}
	if err := e.Decision.Validate(); err != nil {
		return err
	}
	if e.Decision.RunID != e.Facts.RunID || e.Decision.Operation != e.Facts.Operation {
		return errors.New("policy evaluation identities do not match")
	}
	return nil
}

// ActivationBinding identifies a scheduler/trigger delivery without embedding
// its payload or any raw callback credential.
type ActivationBinding struct {
	ActivationID   string    `json:"activation_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	OccurredAt     time.Time `json:"occurred_at"`
}

func (a ActivationBinding) Validate() error {
	if strings.TrimSpace(a.ActivationID) == "" || strings.TrimSpace(a.IdempotencyKey) == "" || a.OccurredAt.IsZero() {
		return errors.New("activation binding requires ids and occurred_at")
	}
	return nil
}

// StartRecord is immutable once accepted. Plan is stored as an extraction-safe
// recovery artifact so restart does not re-resolve a movable root reference.
type StartRecord struct {
	Run             runtime.BoundRun      `json:"run"`
	Plan            compile.ExecutionPlan `json:"plan"`
	Requested       graph.DefinitionRef   `json:"requested"`
	StartKey        string                `json:"start_key"`
	RequestDigest   string                `json:"request_digest"`
	CallerInputHash string                `json:"caller_input_hash"`
	Identity        IdentityBinding       `json:"identity"`
	Facts           PolicyFacts           `json:"facts"`
	Decision        PolicyDecision        `json:"decision"`
	Activation      *ActivationBinding    `json:"activation,omitempty"`
	DryRun          bool                  `json:"dry_run,omitempty"`
	RecordedAt      time.Time             `json:"recorded_at"`
}

// BundledDefinitionCandidate associates one exact serialized child definition
// with the trust class of the durable plan that contains it. Definition
// resolution authorizes both the requested tuple and this resolved trust
// context; stores must return defensive copies in deterministic order.
type BundledDefinitionCandidate struct {
	Definition compile.ResolvedDefinition `json:"definition"`
	Container  runtime.PlanRef            `json:"container"`
	TrustClass string                     `json:"trust_class"`
}

// BundledDefinitionSource finds exact child definitions inside durable plan
// snapshots. An empty result means no persisted plan contains the requested
// immutable tuple. Implementations must not perform authorization themselves.
type BundledDefinitionSource interface {
	FindBundledDefinitions(context.Context, graph.DefinitionRef) ([]BundledDefinitionCandidate, error)
}

func (r StartRecord) Validate() error {
	if err := r.Run.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(r.StartKey) == "" || strings.TrimSpace(r.RequestDigest) == "" || strings.TrimSpace(r.CallerInputHash) == "" || r.RecordedAt.IsZero() {
		return errors.New("start record requires key, request/input hashes, and recorded_at")
	}
	if err := values.ValidateDigest(r.RequestDigest); err != nil {
		return fmt.Errorf("start request digest: %w", err)
	}
	if err := values.ValidateDigest(r.CallerInputHash); err != nil {
		return fmt.Errorf("start input digest: %w", err)
	}
	if r.Run.ID != r.Facts.RunID || r.Run.Plan != r.Facts.Plan || r.Decision.RunID != r.Run.ID {
		return errors.New("start record run identities do not match")
	}
	if !reflect.DeepEqual(r.Identity, r.Facts.Identity) {
		return errors.New("start record identity differs from policy identity")
	}
	if r.Plan.Digest != r.Run.Plan.Digest || r.Plan.ID != r.Run.Plan.ID {
		return errors.New("start record plan does not match bound run")
	}
	if err := r.Identity.Validate(); err != nil {
		return err
	}
	if err := r.Facts.Validate(); err != nil {
		return err
	}
	if err := r.Decision.Validate(); err != nil {
		return err
	}
	if r.Decision.Operation != r.Facts.Operation {
		return errors.New("start record policy operation does not match facts")
	}
	if r.Activation != nil {
		return r.Activation.Validate()
	}
	return nil
}

// StartSnapshot pairs the immutable record with its mutable CAS checkpoint.
type StartSnapshot struct {
	Record     StartRecord `json:"record"`
	Phase      StartPhase  `json:"phase"`
	Generation uint64      `json:"generation"`
	UpdatedAt  time.Time   `json:"updated_at"`
}

func (s StartSnapshot) Validate() error {
	if err := s.Record.Validate(); err != nil {
		return err
	}
	if !s.Phase.Valid() || s.Generation == 0 || s.UpdatedAt.Before(s.Record.RecordedAt) {
		return errors.New("start snapshot requires valid phase, generation, and ordered time")
	}
	return nil
}

type AdvanceStartRequest struct {
	RunID              runtime.RunID
	ExpectedGeneration uint64
	From               StartPhase
	To                 StartPhase
	At                 time.Time
}

// CancellationIntent is the caller's immutable host-level command. RequestedAt
// stays zero when the caller omitted a time so exact replay is independent of
// the host clock chosen for the first attempt.
type CancellationIntent struct {
	RunID          runtime.RunID `json:"run_id"`
	IdempotencyKey string        `json:"idempotency_key"`
	Reason         string        `json:"reason"`
	RequestedAt    time.Time     `json:"requested_at,omitempty"`
}

func (i CancellationIntent) Validate() error {
	if strings.TrimSpace(string(i.RunID)) == "" || strings.TrimSpace(i.IdempotencyKey) == "" || strings.TrimSpace(i.Reason) == "" {
		return errors.New("host cancellation requires run, idempotency key, and reason")
	}
	return nil
}

// BindCancellationRequest asks the journal to bind an exact host command to a
// stable time lower bound. DefaultAt is used only on first application when
// RequestedAt is zero; the current run generation is resolved per attempt.
type BindCancellationRequest struct {
	Intent    CancellationIntent
	DefaultAt time.Time
}

// CancellationBinding is restart-safe input for cancellation preparation.
// EffectiveAt is a stable lower bound; ExpectedGeneration is deliberately not
// persisted because unrelated valid run transitions may advance it before the
// core transaction begins.
type CancellationBinding struct {
	Intent      CancellationIntent `json:"intent"`
	EffectiveAt time.Time          `json:"effective_at"`
	RecordedAt  time.Time          `json:"recorded_at"`
}

func (b CancellationBinding) Validate() error {
	if err := b.Intent.Validate(); err != nil {
		return err
	}
	if b.EffectiveAt.IsZero() || b.RecordedAt.IsZero() {
		return errors.New("host cancellation binding requires effective and recorded times")
	}
	if !b.Intent.RequestedAt.IsZero() && !b.Intent.RequestedAt.Equal(b.EffectiveAt) {
		return errors.New("host cancellation binding changed requested time")
	}
	return nil
}

// Journal is the Hadron-owned durable host surface. Implementations must use
// exact canonical replay and CAS; decisions and immutable start records are
// append-only even though the separate materialization checkpoint advances.
type Journal interface {
	RecordStart(context.Context, StartRecord) (StartSnapshot, runtime.IdempotencyOutcome, error)
	LoadStart(context.Context, runtime.RunID) (StartSnapshot, error)
	LoadStartByKey(context.Context, string) (StartSnapshot, error)
	ListIncompleteStarts(context.Context, int) ([]StartSnapshot, error)
	AdvanceStart(context.Context, AdvanceStartRequest) (StartSnapshot, error)
	RecordPolicyEvaluation(context.Context, PolicyEvaluation) (PolicyEvaluation, runtime.IdempotencyOutcome, error)
	LoadPolicyEvaluation(context.Context, string) (PolicyEvaluation, error)
	LoadPolicyEvaluationByStartKey(context.Context, string) (PolicyEvaluation, error)
	ListPolicyDecisions(context.Context, runtime.RunID) ([]PolicyDecision, error)
	ListRunNodes(context.Context, runtime.RunID) ([]runtime.NodeInvocationSnapshot, error)
	BindCancellation(context.Context, BindCancellationRequest) (CancellationBinding, runtime.IdempotencyOutcome, error)
	PrepareCancellation(context.Context, CancellationBinding) (runtime.RequestRunCancellationRequest, error)
	ListPendingCancellations(context.Context, int) ([]CancellationBinding, error)
}

func validateMap(input map[string]string) error {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if strings.TrimSpace(key) == "" || strings.TrimSpace(input[key]) == "" {
			return errors.New("metadata keys and values must be non-empty")
		}
	}
	return nil
}
