package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
)

var (
	ErrInvalidSchedulerResource = errors.New("invalid workflow scheduler resource")
)

// SchedulerResourceKind identifies one independently enforced admission
// dimension. Fan-out occupancy is persisted by FanOutStore and is reported
// through the same diagnostic vocabulary, but is not a worker lease resource.
type SchedulerResourceKind string

const (
	SchedulerResourceWorker     SchedulerResourceKind = "worker"
	SchedulerResourceRun        SchedulerResourceKind = "run"
	SchedulerResourceEffect     SchedulerResourceKind = "effect"
	SchedulerResourceCapability SchedulerResourceKind = "capability"
	SchedulerResourceKey        SchedulerResourceKind = "concurrency_key"
	SchedulerResourceFanOut     SchedulerResourceKind = "fan_out"
)

func (k SchedulerResourceKind) Valid() bool {
	switch k {
	case SchedulerResourceWorker, SchedulerResourceRun, SchedulerResourceEffect,
		SchedulerResourceCapability, SchedulerResourceKey, SchedulerResourceFanOut:
		return true
	default:
		return false
	}
}

// SchedulerResourceID is structured so opaque run identities and graph IDs
// cannot collide through delimiter concatenation. RunID is set only for run
// and fan-out resources; NodeID is set only for fan-out resources.
type SchedulerResourceID struct {
	Kind   SchedulerResourceKind `json:"kind"`
	Name   string                `json:"name,omitempty"`
	RunID  RunID                 `json:"run_id,omitempty"`
	NodeID string                `json:"node_id,omitempty"`
}

func (id SchedulerResourceID) Validate() error {
	if !id.Kind.Valid() {
		return fmt.Errorf("%w: unsupported kind %q", ErrInvalidSchedulerResource, id.Kind)
	}
	switch id.Kind {
	case SchedulerResourceWorker:
		if id.Name != "global" || id.RunID != "" || id.NodeID != "" {
			return fmt.Errorf("%w: worker resource must be global", ErrInvalidSchedulerResource)
		}
	case SchedulerResourceRun:
		if id.Name != "" || id.NodeID != "" {
			return fmt.Errorf("%w: run resource contains unrelated identity", ErrInvalidSchedulerResource)
		}
		if err := validateOpaqueID("resource run id", string(id.RunID)); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidSchedulerResource, err)
		}
	case SchedulerResourceEffect:
		if id.RunID != "" || id.NodeID != "" || !graph.Effect(id.Name).Valid() {
			return fmt.Errorf("%w: effect resource is invalid", ErrInvalidSchedulerResource)
		}
	case SchedulerResourceCapability:
		if id.RunID != "" || id.NodeID != "" {
			return fmt.Errorf("%w: capability resource contains unrelated identity", ErrInvalidSchedulerResource)
		}
		if err := validateRequiredText("resource capability", id.Name); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidSchedulerResource, err)
		}
	case SchedulerResourceKey:
		if id.RunID != "" || id.NodeID != "" {
			return fmt.Errorf("%w: concurrency-key resource contains unrelated identity", ErrInvalidSchedulerResource)
		}
		if err := graph.ValidateID(id.Name); err != nil {
			return fmt.Errorf("%w: concurrency key: %w", ErrInvalidSchedulerResource, err)
		}
	case SchedulerResourceFanOut:
		if id.Name != "" {
			return fmt.Errorf("%w: fan-out resource contains unrelated name", ErrInvalidSchedulerResource)
		}
		if err := validateOpaqueID("fan-out run id", string(id.RunID)); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidSchedulerResource, err)
		}
		if err := graph.ValidateID(id.NodeID); err != nil {
			return fmt.Errorf("%w: fan-out node id: %w", ErrInvalidSchedulerResource, err)
		}
	}
	return nil
}

// SchedulerResourceRequirement requests Units from one persisted semaphore.
// Limit is part of the durable definition: another process cannot silently
// claim the same resource using a different configured limit.
type SchedulerResourceRequirement struct {
	Resource SchedulerResourceID `json:"resource"`
	Units    int                 `json:"units"`
	Limit    int                 `json:"limit"`
}

func (r SchedulerResourceRequirement) Validate() error {
	if err := r.Resource.Validate(); err != nil {
		return err
	}
	if r.Resource.Kind == SchedulerResourceFanOut {
		return fmt.Errorf("%w: fan-out occupancy is store-derived", ErrInvalidSchedulerResource)
	}
	if r.Units < 1 || r.Limit < 1 || r.Units > r.Limit {
		return fmt.Errorf("%w: units must be positive and not exceed limit", ErrInvalidSchedulerResource)
	}
	return nil
}

// SchedulerLimits is host-owned initial admission configuration. The first
// durable admission fixes each resource's limit for that database; a later
// request with a different limit fails closed because safe live resizing needs
// a separately versioned, quiescence-aware reconfiguration contract. A zero
// optional limit disables that dimension. Workers must be positive. Named
// resources used by a node must have an explicit positive definition.
type SchedulerLimits struct {
	Workers      int                  `json:"workers"`
	PerRun       int                  `json:"per_run,omitempty"`
	Effects      map[graph.Effect]int `json:"effects,omitempty"`
	Capabilities map[string]int       `json:"capabilities,omitempty"`
	Named        map[string]int       `json:"named,omitempty"`
}

// SchedulerDemand is the trusted execution metadata for one invocation.
// Effects and capabilities must come from the registered StepKindSpec (plus
// host policy), never from an untrusted graph declaration that narrows it.
type SchedulerDemand struct {
	Effects      graph.EffectSet          `json:"effects,omitempty"`
	Capabilities []string                 `json:"capabilities,omitempty"`
	Concurrency  []graph.ConcurrencyClaim `json:"concurrency,omitempty"`
}

// BuildSchedulerRequirements validates and canonicalizes independently
// enforced worker, run, effect, capability, and named-key requirements.
func BuildSchedulerRequirements(runID RunID, limits SchedulerLimits, demand SchedulerDemand) ([]SchedulerResourceRequirement, error) {
	if err := validateOpaqueID("scheduler run id", string(runID)); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidSchedulerResource, err)
	}
	if limits.Workers < 1 || limits.PerRun < 0 {
		return nil, fmt.Errorf("%w: workers must be positive and per-run limit nonnegative", ErrInvalidSchedulerResource)
	}
	requirements := []SchedulerResourceRequirement{{Resource: SchedulerResourceID{Kind: SchedulerResourceWorker, Name: "global"}, Units: 1, Limit: limits.Workers}}
	if limits.PerRun > 0 {
		requirements = append(requirements, SchedulerResourceRequirement{Resource: SchedulerResourceID{Kind: SchedulerResourceRun, RunID: runID}, Units: 1, Limit: limits.PerRun})
	}
	for effect, limit := range limits.Effects {
		if !effect.Valid() || limit < 1 {
			return nil, fmt.Errorf("%w: invalid effect limit", ErrInvalidSchedulerResource)
		}
	}
	for capability, limit := range limits.Capabilities {
		if err := validateRequiredText("scheduler capability", capability); err != nil || limit < 1 {
			return nil, fmt.Errorf("%w: invalid capability limit", ErrInvalidSchedulerResource)
		}
	}
	for name, limit := range limits.Named {
		if err := graph.ValidateID(name); err != nil || limit < 1 {
			return nil, fmt.Errorf("%w: invalid named resource limit", ErrInvalidSchedulerResource)
		}
	}
	seenEffects := make(map[graph.Effect]struct{}, len(demand.Effects))
	for _, effect := range demand.Effects {
		if !effect.Valid() {
			return nil, fmt.Errorf("%w: invalid demanded effect %q", ErrInvalidSchedulerResource, effect)
		}
		if _, duplicate := seenEffects[effect]; duplicate {
			continue
		}
		seenEffects[effect] = struct{}{}
		if limit := limits.Effects[effect]; limit > 0 {
			requirements = append(requirements, SchedulerResourceRequirement{Resource: SchedulerResourceID{Kind: SchedulerResourceEffect, Name: string(effect)}, Units: 1, Limit: limit})
		}
	}
	seenCapabilities := make(map[string]struct{}, len(demand.Capabilities))
	for _, capability := range demand.Capabilities {
		if err := validateRequiredText("scheduler capability", capability); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidSchedulerResource, err)
		}
		if _, duplicate := seenCapabilities[capability]; duplicate {
			continue
		}
		seenCapabilities[capability] = struct{}{}
		if limit := limits.Capabilities[capability]; limit > 0 {
			requirements = append(requirements, SchedulerResourceRequirement{Resource: SchedulerResourceID{Kind: SchedulerResourceCapability, Name: capability}, Units: 1, Limit: limit})
		}
	}
	seenNamed := make(map[string]struct{}, len(demand.Concurrency))
	for _, claim := range demand.Concurrency {
		if err := graph.ValidateID(claim.Resource); err != nil {
			return nil, fmt.Errorf("%w: concurrency claim: %w", ErrInvalidSchedulerResource, err)
		}
		if _, duplicate := seenNamed[claim.Resource]; duplicate {
			return nil, fmt.Errorf("%w: duplicate concurrency claim %q", ErrInvalidSchedulerResource, claim.Resource)
		}
		seenNamed[claim.Resource] = struct{}{}
		limit, exists := limits.Named[claim.Resource]
		if !exists {
			return nil, fmt.Errorf("%w: concurrency claim %q has no configured limit", ErrInvalidSchedulerResource, claim.Resource)
		}
		units := claim.Amount
		if units == 0 {
			units = 1
		}
		requirements = append(requirements, SchedulerResourceRequirement{Resource: SchedulerResourceID{Kind: SchedulerResourceKey, Name: claim.Resource}, Units: units, Limit: limit})
	}
	sortSchedulerRequirements(requirements)
	if err := validateSchedulerRequirements(requirements); err != nil {
		return nil, err
	}
	return requirements, nil
}

// SchedulerAdmissionPolicy supplies immutable resource requirements for one
// persisted candidate. Implementations may load pinned graph/spec metadata but
// must not perform side effects.
type SchedulerAdmissionPolicy interface {
	Requirements(context.Context, ReadyCandidate) ([]SchedulerResourceRequirement, error)
}

type SchedulerAdmissionPolicyFunc func(context.Context, ReadyCandidate) ([]SchedulerResourceRequirement, error)

func (f SchedulerAdmissionPolicyFunc) Requirements(ctx context.Context, candidate ReadyCandidate) ([]SchedulerResourceRequirement, error) {
	return f(ctx, candidate)
}

// AdmitNodeRequest atomically acquires a node claim and all requirements, or
// records a diagnostic waiter without acquiring either. Claim idempotency and
// requirements are one immutable request.
type AdmitNodeRequest struct {
	Claim        ClaimNodeRequest               `json:"claim"`
	Requirements []SchedulerResourceRequirement `json:"requirements"`
	Priority     int                            `json:"priority,omitempty"`
	EnqueuedAt   time.Time                      `json:"enqueued_at"`
}

func (r AdmitNodeRequest) Validate() error { return validateAdmitNodeRequest(r) }

type AdmitNodeResult struct {
	Claim   ClaimResult           `json:"claim"`
	Blocked []SchedulerResourceID `json:"blocked,omitempty"`
}

type SchedulerResourceHolder struct {
	Resource        SchedulerResourceID `json:"resource"`
	Invocation      NodeInvocationID    `json:"invocation"`
	Units           int                 `json:"units"`
	ClaimGeneration uint64              `json:"claim_generation"`
	Owner           string              `json:"owner"`
	AcquiredAt      time.Time           `json:"acquired_at"`
	ExpiresAt       time.Time           `json:"expires_at"`
}

func (h SchedulerResourceHolder) Validate() error {
	if err := h.Resource.Validate(); err != nil {
		return err
	}
	if err := h.Invocation.Validate(); err != nil {
		return err
	}
	if h.Units < 1 || h.ClaimGeneration == 0 || h.AcquiredAt.IsZero() || !h.ExpiresAt.After(h.AcquiredAt) {
		return fmt.Errorf("%w: holder requires units, claim, owner, and ordered timestamps", ErrInvalidSchedulerResource)
	}
	if err := validateRequiredText("scheduler holder owner", h.Owner); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSchedulerResource, err)
	}
	return nil
}

type SchedulerResourceWaiter struct {
	Invocation   NodeInvocationID               `json:"invocation"`
	Requirements []SchedulerResourceRequirement `json:"requirements"`
	Blocked      []SchedulerResourceID          `json:"blocked"`
	Priority     int                            `json:"priority,omitempty"`
	EnqueuedAt   time.Time                      `json:"enqueued_at"`
	UpdatedAt    time.Time                      `json:"updated_at"`
}

func (w SchedulerResourceWaiter) Validate() error {
	if err := w.Invocation.Validate(); err != nil {
		return err
	}
	if err := validateSchedulerRequirements(w.Requirements); err != nil {
		return err
	}
	if len(w.Blocked) == 0 || w.EnqueuedAt.IsZero() || w.UpdatedAt.Before(w.EnqueuedAt) {
		return fmt.Errorf("%w: waiter requires blocked resources and ordered timestamps", ErrInvalidSchedulerResource)
	}
	var prior SchedulerResourceID
	seen := make(map[SchedulerResourceID]struct{}, len(w.Blocked))
	for index, blocked := range w.Blocked {
		if err := blocked.Validate(); err != nil {
			return err
		}
		if index > 0 && !schedulerResourceLess(prior, blocked) {
			return fmt.Errorf("%w: blocked resources must use canonical order", ErrInvalidSchedulerResource)
		}
		if _, duplicate := seen[blocked]; duplicate {
			return fmt.Errorf("%w: blocked resources must be unique", ErrInvalidSchedulerResource)
		}
		seen[blocked] = struct{}{}
		prior = blocked
	}
	return nil
}

type SchedulerResourceQuery struct {
	RunID RunID     `json:"run_id,omitempty"`
	Now   time.Time `json:"now"`
	Limit int       `json:"limit,omitempty"`
}

type SchedulerResourceState struct {
	Holders []SchedulerResourceHolder `json:"holders"`
	Waiters []SchedulerResourceWaiter `json:"waiters"`
}

// SchedulerResourceStore owns cross-process admission and diagnostics.
type SchedulerResourceStore interface {
	AdmitNode(context.Context, AdmitNodeRequest) (AdmitNodeResult, error)
	InspectSchedulerResources(context.Context, SchedulerResourceQuery) (SchedulerResourceState, error)
}

func validateAdmitNodeRequest(request AdmitNodeRequest) error {
	if err := request.Claim.InvocationID.Validate(); err != nil {
		return err
	}
	if err := validateReadyClaimRequest(ReadyClaimRequest{RunID: request.Claim.InvocationID.RunID, Owner: request.Claim.Owner, Token: request.Claim.Token, IdempotencyKey: request.Claim.IdempotencyKey, Now: request.Claim.Now, LeaseUntil: request.Claim.LeaseUntil}); err != nil {
		return err
	}
	if request.EnqueuedAt.IsZero() || request.EnqueuedAt.After(request.Claim.Now) {
		return fmt.Errorf("admission enqueued_at must be non-zero and not after now")
	}
	return validateSchedulerRequirements(request.Requirements)
}

func nilSchedulerAdmissionPolicy(policy SchedulerAdmissionPolicy) bool {
	if policy == nil {
		return true
	}
	value := reflect.ValueOf(policy)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func validateSchedulerRequirements(requirements []SchedulerResourceRequirement) error {
	if len(requirements) == 0 {
		return fmt.Errorf("%w: admission requires at least the worker resource", ErrInvalidSchedulerResource)
	}
	seen := make(map[SchedulerResourceID]struct{}, len(requirements))
	var prior SchedulerResourceID
	hasWorker := false
	for index, requirement := range requirements {
		if err := requirement.Validate(); err != nil {
			return err
		}
		if _, duplicate := seen[requirement.Resource]; duplicate {
			return fmt.Errorf("%w: duplicate requirement", ErrInvalidSchedulerResource)
		}
		if index > 0 && !schedulerResourceLess(prior, requirement.Resource) {
			return fmt.Errorf("%w: requirements must use canonical order", ErrInvalidSchedulerResource)
		}
		seen[requirement.Resource] = struct{}{}
		prior = requirement.Resource
		hasWorker = hasWorker || requirement.Resource.Kind == SchedulerResourceWorker
	}
	if !hasWorker {
		return fmt.Errorf("%w: admission must include global worker resource", ErrInvalidSchedulerResource)
	}
	return nil
}

func sortSchedulerRequirements(requirements []SchedulerResourceRequirement) {
	sort.Slice(requirements, func(i, j int) bool { return schedulerResourceLess(requirements[i].Resource, requirements[j].Resource) })
}

func schedulerResourceLess(left, right SchedulerResourceID) bool {
	if left.Kind != right.Kind {
		return left.Kind < right.Kind
	}
	if left.Name != right.Name {
		return left.Name < right.Name
	}
	if left.RunID != right.RunID {
		return left.RunID < right.RunID
	}
	return left.NodeID < right.NodeID
}

func nilSchedulerResourceStore(store SchedulerResourceStore) bool {
	if store == nil {
		return true
	}
	value := reflect.ValueOf(store)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
