package runtime

import (
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

// RunID is an opaque host-independent run identifier.
type RunID string

// WaitID is an opaque host-independent wait identifier.
type WaitID string

// NodeInvocationID identifies one node expansion inside a run. Iteration is
// empty for nodes that are not expanded by for_each.
type NodeInvocationID struct {
	RunID     RunID  `json:"run_id"`
	NodeID    string `json:"node_id"`
	Iteration string `json:"iteration,omitempty"`
}

// Validate reports malformed invocation identity.
func (id NodeInvocationID) Validate() error {
	if err := validateOpaqueID("run_id", string(id.RunID)); err != nil {
		return err
	}
	if err := graph.ValidateID(id.NodeID); err != nil {
		return fmt.Errorf("node_id: %w", err)
	}
	if !utf8.ValidString(id.Iteration) {
		return fmt.Errorf("iteration must contain valid UTF-8")
	}
	return nil
}

// AttemptID identifies one numbered execution attempt.
type AttemptID struct {
	Invocation NodeInvocationID `json:"invocation"`
	Number     int              `json:"number"`
}

// Validate reports malformed attempt identity.
func (id AttemptID) Validate() error {
	if err := id.Invocation.Validate(); err != nil {
		return err
	}
	if id.Number < 1 {
		return fmt.Errorf("attempt number must be positive")
	}
	return nil
}

// PlanRef identifies an immutable compiled execution plan without embedding it
// in mutable runtime records.
type PlanRef struct {
	ID            string `json:"id"`
	Version       string `json:"version"`
	Digest        string `json:"digest"`
	SchemaVersion string `json:"schema_version"`
}

// Validate reports malformed plan identity or digest metadata.
func (r PlanRef) Validate() error {
	if err := graph.ValidateID(r.ID); err != nil {
		return fmt.Errorf("plan id: %w", err)
	}
	if err := validateRequiredText("plan version", r.Version); err != nil {
		return err
	}
	if err := values.ValidateDigest(r.Digest); err != nil {
		return fmt.Errorf("plan digest: %w", err)
	}
	return validateRequiredText("plan schema version", r.SchemaVersion)
}

// ValueRef identifies one named value inside a persisted value set.
type ValueRef struct {
	Set  values.ValueSetRef `json:"set"`
	Name string             `json:"name"`
}

// Validate reports malformed value identity.
func (r ValueRef) Validate() error {
	if err := r.Set.Validate(); err != nil {
		return err
	}
	return validateRequiredText("value name", r.Name)
}

// BlockedReason preserves explainable readiness failure without overloading a
// pending status.
type BlockedReason struct {
	Code         string             `json:"code"`
	Message      string             `json:"message"`
	Dependencies []NodeInvocationID `json:"dependencies,omitempty"`
	Details      map[string]string  `json:"details,omitempty"`
}

// Validate reports malformed structured blocked metadata.
func (r BlockedReason) Validate() error {
	if err := validateRequiredText("blocked reason code", r.Code); err != nil {
		return err
	}
	if err := validateRequiredText("blocked reason message", r.Message); err != nil {
		return err
	}
	for i, dependency := range r.Dependencies {
		if err := dependency.Validate(); err != nil {
			return fmt.Errorf("blocked dependency[%d]: %w", i, err)
		}
	}
	return validateStringMap("blocked reason details", r.Details)
}

// Failure is a persisted application-neutral execution failure.
type Failure struct {
	Code      string            `json:"code"`
	Message   string            `json:"message"`
	Retryable bool              `json:"retryable,omitempty"`
	Details   map[string]string `json:"details,omitempty"`
}

// ExecutorMetadata identifies the application-neutral executor contract used
// by one attempt. It is immutable for the lifetime of that attempt.
type ExecutorMetadata struct {
	Kind       string            `json:"kind"`
	Version    string            `json:"version"`
	Target     string            `json:"target,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// Validate reports malformed persisted executor identity.
func (m ExecutorMetadata) Validate() error {
	if err := validateRequiredText("executor kind", m.Kind); err != nil {
		return err
	}
	if err := validateRequiredText("executor version", m.Version); err != nil {
		return err
	}
	if m.Target != "" {
		if err := validateRequiredText("executor target", m.Target); err != nil {
			return err
		}
	}
	return validateStringMap("executor attributes", m.Attributes)
}

// Validate reports malformed failure metadata.
func (f Failure) Validate() error {
	if err := validateRequiredText("failure code", f.Code); err != nil {
		return err
	}
	if err := validateRequiredText("failure message", f.Message); err != nil {
		return err
	}
	return validateStringMap("failure details", f.Details)
}

// ClaimLease is an opaque token plus monotonic generation proving current
// ownership until ExpiresAt. Tokens are never returned to contending claimants.
type ClaimLease struct {
	Owner      string    `json:"owner"`
	Token      string    `json:"token"`
	Generation uint64    `json:"generation"`
	ExpiresAt  time.Time `json:"expires_at"`
}

// Validate reports malformed lease metadata.
func (l ClaimLease) Validate() error {
	if err := validateRequiredText("lease owner", l.Owner); err != nil {
		return err
	}
	if err := validateRequiredText("lease token", l.Token); err != nil {
		return err
	}
	if l.Generation == 0 {
		return fmt.Errorf("lease generation must be positive")
	}
	if l.ExpiresAt.IsZero() {
		return fmt.Errorf("lease expiry is required")
	}
	return nil
}

// RunSnapshot is one persisted run view. Generation is a store-managed CAS
// revision, not a lifecycle state-machine version.
type RunSnapshot struct {
	ID         RunID               `json:"id"`
	Plan       PlanRef             `json:"plan"`
	Status     RunStatus           `json:"status"`
	Inputs     *values.ValueSetRef `json:"inputs,omitempty"`
	Outputs    *values.ValueSetRef `json:"outputs,omitempty"`
	Generation uint64              `json:"generation"`
	CreatedAt  time.Time           `json:"created_at"`
	UpdatedAt  time.Time           `json:"updated_at"`
}

// Validate checks record integrity without deciding whether a transition into
// Status is legal.
func (s RunSnapshot) Validate() error {
	if err := validateOpaqueID("run id", string(s.ID)); err != nil {
		return err
	}
	if err := s.Plan.Validate(); err != nil {
		return err
	}
	if !s.Status.Valid() {
		return fmt.Errorf("unsupported run status %q", s.Status)
	}
	if err := validateOptionalValueSetRef(s.Inputs); err != nil {
		return fmt.Errorf("run inputs: %w", err)
	}
	if err := validateOptionalValueSetRef(s.Outputs); err != nil {
		return fmt.Errorf("run outputs: %w", err)
	}
	return validateSnapshotTimes(s.Generation, s.CreatedAt, s.UpdatedAt)
}

// NodeInvocationSnapshot is one persisted graph-node expansion. Generation is
// the record CAS revision; ClaimGeneration changes only on successful claims.
type NodeInvocationSnapshot struct {
	ID              NodeInvocationID    `json:"id"`
	Status          NodeStatus          `json:"status"`
	Blocked         *BlockedReason      `json:"blocked,omitempty"`
	Inputs          *values.ValueSetRef `json:"inputs,omitempty"`
	Outputs         *values.ValueSetRef `json:"outputs,omitempty"`
	Wait            *WaitRef            `json:"wait,omitempty"`
	LatestAttempt   int                 `json:"latest_attempt,omitempty"`
	Priority        int                 `json:"priority,omitempty"`
	ClaimGeneration uint64              `json:"claim_generation"`
	Lease           *ClaimLease         `json:"lease,omitempty"`
	Generation      uint64              `json:"generation"`
	CreatedAt       time.Time           `json:"created_at"`
	UpdatedAt       time.Time           `json:"updated_at"`
}

// Validate checks record integrity without enforcing lifecycle transitions.
func (s NodeInvocationSnapshot) Validate() error {
	if err := s.ID.Validate(); err != nil {
		return err
	}
	if !s.Status.Valid() {
		return fmt.Errorf("unsupported node status %q", s.Status)
	}
	if err := validateBlocked(s.Status == NodeBlocked, s.Blocked); err != nil {
		return err
	}
	if s.Blocked != nil {
		for i, dependency := range s.Blocked.Dependencies {
			if dependency.RunID != s.ID.RunID {
				return fmt.Errorf("blocked dependency[%d] must belong to invocation run", i)
			}
		}
	}
	if err := validateOptionalValueSetRef(s.Inputs); err != nil {
		return fmt.Errorf("node inputs: %w", err)
	}
	if err := validateOptionalValueSetRef(s.Outputs); err != nil {
		return fmt.Errorf("node outputs: %w", err)
	}
	if s.Wait != nil {
		if err := s.Wait.Validate(); err != nil {
			return err
		}
	}
	if s.LatestAttempt < 0 {
		return fmt.Errorf("latest attempt must not be negative")
	}
	if s.Lease != nil {
		if err := s.Lease.Validate(); err != nil {
			return err
		}
		if s.Lease.Generation != s.ClaimGeneration {
			return fmt.Errorf("lease generation must equal claim generation")
		}
	}
	return validateSnapshotTimes(s.Generation, s.CreatedAt, s.UpdatedAt)
}

// AttemptSnapshot records one execution attempt without defining which status
// transitions are legal.
type AttemptSnapshot struct {
	ID         AttemptID           `json:"id"`
	Status     NodeStatus          `json:"status"`
	Executor   ExecutorMetadata    `json:"executor"`
	Inputs     *values.ValueSetRef `json:"inputs,omitempty"`
	Outputs    *values.ValueSetRef `json:"outputs,omitempty"`
	Failure    *Failure            `json:"failure,omitempty"`
	StartedAt  time.Time           `json:"started_at"`
	FinishedAt time.Time           `json:"finished_at,omitempty"`
	Generation uint64              `json:"generation"`
	CreatedAt  time.Time           `json:"created_at"`
	UpdatedAt  time.Time           `json:"updated_at"`
}

// Validate checks attempt record integrity.
func (s AttemptSnapshot) Validate() error {
	if err := s.ID.Validate(); err != nil {
		return err
	}
	if !attemptStatusValid(s.Status) {
		return fmt.Errorf("unsupported attempt status %q", s.Status)
	}
	if err := s.Executor.Validate(); err != nil {
		return err
	}
	if err := validateOptionalValueSetRef(s.Inputs); err != nil {
		return fmt.Errorf("attempt inputs: %w", err)
	}
	if err := validateOptionalValueSetRef(s.Outputs); err != nil {
		return fmt.Errorf("attempt outputs: %w", err)
	}
	if s.Failure != nil {
		if err := s.Failure.Validate(); err != nil {
			return err
		}
	}
	if s.StartedAt.IsZero() || !s.StartedAt.Equal(s.CreatedAt) {
		return fmt.Errorf("attempt started_at must equal created_at")
	}
	if s.Status == NodeRunning {
		if !s.FinishedAt.IsZero() || s.Failure != nil || s.Outputs != nil {
			return fmt.Errorf("running attempt must not contain a finish outcome")
		}
	} else {
		if s.FinishedAt.IsZero() || !s.FinishedAt.Equal(s.UpdatedAt) || s.FinishedAt.Before(s.StartedAt) {
			return fmt.Errorf("finished attempt requires an ordered finished_at equal to updated_at")
		}
		if s.Status == NodeSucceeded && s.Failure != nil {
			return fmt.Errorf("succeeded attempt must not contain failure")
		}
		if s.Status != NodeSucceeded && s.Failure == nil {
			return fmt.Errorf("unsuccessful attempt requires failure")
		}
	}
	return validateSnapshotTimes(s.Generation, s.CreatedAt, s.UpdatedAt)
}

func attemptStatusValid(status NodeStatus) bool {
	switch status {
	case NodeRunning, NodeSucceeded, NodeFailed, NodeCanceled, NodeTimedOut, NodeCrashed:
		return true
	default:
		return false
	}
}

// WaitRef is the compact wait identity stored on a node invocation.
type WaitRef struct {
	ID WaitID `json:"id"`
}

// Validate reports malformed wait identity.
func (r WaitRef) Validate() error { return validateOpaqueID("wait id", string(r.ID)) }

// WaitSnapshot adds runtime identity, CAS, and storage chronology to the
// canonical workflow/wait Record. The anonymous record produces one flat JSON
// envelope; it does not duplicate status or resume fields.
type WaitSnapshot struct {
	Ref        WaitRef          `json:"ref"`
	Invocation NodeInvocationID `json:"invocation"`
	workflowwait.Record
	Generation uint64    `json:"generation"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	ResolvedAt time.Time `json:"resolved_at,omitempty"`
}

// Validate checks minimal wait persistence integrity.
func (s WaitSnapshot) Validate() error {
	if err := s.Ref.Validate(); err != nil {
		return err
	}
	if err := s.Invocation.Validate(); err != nil {
		return err
	}
	if err := s.Record.Validate(); err != nil {
		return err
	}
	if s.Resolution == nil {
		if !s.ResolvedAt.IsZero() {
			return fmt.Errorf("unresolved wait must not contain resolved_at")
		}
	} else if s.ResolvedAt.IsZero() || !s.ResolvedAt.Equal(s.Resolution.ResolvedAt) {
		return fmt.Errorf("wait resolved_at must equal resolution provenance")
	}
	if !s.ResolvedAt.IsZero() && s.ResolvedAt.Before(s.CreatedAt) {
		return fmt.Errorf("wait resolved_at must not precede created_at")
	}
	if !s.Deadline.IsZero() && s.Deadline.Before(s.CreatedAt) {
		return fmt.Errorf("wait deadline must not precede created_at")
	}
	if !s.WakeAt.IsZero() && s.WakeAt.Before(s.CreatedAt) {
		return fmt.Errorf("wait wake_at must not precede created_at")
	}
	return validateSnapshotTimes(s.Generation, s.CreatedAt, s.UpdatedAt)
}

// ValueOwner describes why a value set is persisted without prescribing a
// table or host record type.
type ValueOwner struct {
	Kind       string            `json:"kind"`
	RunID      RunID             `json:"run_id"`
	Invocation *NodeInvocationID `json:"invocation,omitempty"`
	Attempt    *AttemptID        `json:"attempt,omitempty"`
}

// Validate reports malformed value ownership metadata.
func (o ValueOwner) Validate() error {
	if err := validateRequiredText("value owner kind", o.Kind); err != nil {
		return err
	}
	if err := validateOpaqueID("value owner run id", string(o.RunID)); err != nil {
		return err
	}
	if o.Invocation != nil {
		if err := o.Invocation.Validate(); err != nil {
			return err
		}
		if o.Invocation.RunID != o.RunID {
			return fmt.Errorf("value owner invocation run does not match owner run")
		}
	}
	if o.Attempt != nil {
		if err := o.Attempt.Validate(); err != nil {
			return err
		}
		if o.Attempt.Invocation.RunID != o.RunID {
			return fmt.Errorf("value owner attempt run does not match owner run")
		}
	}
	if o.Invocation != nil && o.Attempt != nil && *o.Invocation != o.Attempt.Invocation {
		return fmt.Errorf("value owner attempt invocation does not match owner invocation")
	}
	return nil
}

// Event is an immutable, append-only operational fact. Sequence is assigned by
// the store and orders events within one run.
type Event struct {
	Sequence   uint64                `json:"sequence"`
	RunID      RunID                 `json:"run_id"`
	Invocation *NodeInvocationID     `json:"invocation,omitempty"`
	Attempt    *AttemptID            `json:"attempt,omitempty"`
	Type       string                `json:"type"`
	OccurredAt time.Time             `json:"occurred_at"`
	Attributes map[string]string     `json:"attributes,omitempty"`
	Values     *values.ValueSetRef   `json:"values,omitempty"`
	Redaction  values.RedactionClass `json:"redaction"`
	Retention  values.RetentionClass `json:"retention"`
}

// Validate reports malformed event metadata.
func (e Event) Validate() error {
	if e.Sequence == 0 {
		return fmt.Errorf("event sequence must be positive")
	}
	if err := validateOpaqueID("event run id", string(e.RunID)); err != nil {
		return err
	}
	if e.Invocation != nil {
		if err := e.Invocation.Validate(); err != nil {
			return err
		}
		if e.Invocation.RunID != e.RunID {
			return fmt.Errorf("event invocation run does not match event run")
		}
	}
	if e.Attempt != nil {
		if err := e.Attempt.Validate(); err != nil {
			return err
		}
		if e.Attempt.Invocation.RunID != e.RunID {
			return fmt.Errorf("event attempt run does not match event run")
		}
	}
	if e.Invocation != nil && e.Attempt != nil && *e.Invocation != e.Attempt.Invocation {
		return fmt.Errorf("event attempt invocation does not match event invocation")
	}
	if err := validateRequiredText("event type", e.Type); err != nil {
		return err
	}
	if e.OccurredAt.IsZero() {
		return fmt.Errorf("event occurred_at is required")
	}
	if err := validateStringMap("event attributes", e.Attributes); err != nil {
		return err
	}
	if err := validateOptionalValueSetRef(e.Values); err != nil {
		return err
	}
	if !e.Redaction.Valid() {
		return fmt.Errorf("unsupported event redaction %q", e.Redaction)
	}
	if !e.Retention.Valid() {
		return fmt.Errorf("unsupported event retention %q", e.Retention)
	}
	return nil
}

// CacheEntry references cached node outputs without embedding values.
type CacheEntry struct {
	Key         string             `json:"key"`
	PlanDigest  string             `json:"plan_digest"`
	NodeID      string             `json:"node_id"`
	InputDigest string             `json:"input_digest"`
	Outputs     values.ValueSetRef `json:"outputs"`
	CreatedAt   time.Time          `json:"created_at"`
	ExpiresAt   time.Time          `json:"expires_at,omitempty"`
}

// Validate reports malformed cache metadata.
func (e CacheEntry) Validate() error {
	if err := validateRequiredText("cache key", e.Key); err != nil {
		return err
	}
	if err := values.ValidateDigest(e.PlanDigest); err != nil {
		return err
	}
	if err := graph.ValidateID(e.NodeID); err != nil {
		return err
	}
	if err := values.ValidateDigest(e.InputDigest); err != nil {
		return err
	}
	if err := e.Outputs.Validate(); err != nil {
		return err
	}
	if e.CreatedAt.IsZero() {
		return fmt.Errorf("cache created_at is required")
	}
	if !e.ExpiresAt.IsZero() && !e.ExpiresAt.After(e.CreatedAt) {
		return fmt.Errorf("cache expiry must follow creation")
	}
	return nil
}

// PinnedValue gives a stable host-independent name to a persisted value.
type PinnedValue struct {
	Key       string    `json:"key"`
	Value     ValueRef  `json:"value"`
	PinnedAt  time.Time `json:"pinned_at"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
}

// Validate reports malformed pin metadata.
func (p PinnedValue) Validate() error {
	if err := validateRequiredText("pin key", p.Key); err != nil {
		return err
	}
	if err := p.Value.Validate(); err != nil {
		return err
	}
	if p.PinnedAt.IsZero() {
		return fmt.Errorf("pin time is required")
	}
	if !p.ExpiresAt.IsZero() && !p.ExpiresAt.After(p.PinnedAt) {
		return fmt.Errorf("pin expiry must follow pin time")
	}
	return nil
}

func validateBlocked(blocked bool, reason *BlockedReason) error {
	if blocked && reason == nil {
		return fmt.Errorf("blocked status requires a blocked reason")
	}
	if !blocked && reason != nil {
		return fmt.Errorf("blocked reason requires blocked status")
	}
	if reason != nil {
		return reason.Validate()
	}
	return nil
}

func validateSnapshotTimes(generation uint64, createdAt, updatedAt time.Time) error {
	if generation == 0 {
		return fmt.Errorf("snapshot generation must be positive")
	}
	if createdAt.IsZero() || updatedAt.IsZero() {
		return fmt.Errorf("snapshot created_at and updated_at are required")
	}
	if updatedAt.Before(createdAt) {
		return fmt.Errorf("snapshot updated_at must not precede created_at")
	}
	return nil
}

func validateOptionalValueSetRef(ref *values.ValueSetRef) error {
	if ref == nil {
		return nil
	}
	return ref.Validate()
}

func validateOpaqueID(field, value string) error {
	if err := validateRequiredText(field, value); err != nil {
		return err
	}
	if strings.ContainsAny(value, "\r\n\t") {
		return fmt.Errorf("%s must not contain control whitespace", field)
	}
	return nil
}

func validateRequiredText(field, value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s must contain valid UTF-8", field)
	}
	if strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) {
		return fmt.Errorf("%s is required without surrounding whitespace", field)
	}
	return nil
}

func validateStringMap(field string, entries map[string]string) error {
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := validateRequiredText(field+" key", key); err != nil {
			return err
		}
		if !utf8.ValidString(entries[key]) {
			return fmt.Errorf("%s[%q] must contain valid UTF-8", field, key)
		}
	}
	return nil
}
