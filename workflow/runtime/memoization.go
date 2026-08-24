package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	EventNodeOutcomeReused = "node.outcome_reused"

	CodeMemoMiss     diagnostic.Code = "HADR-RUNTIME-041"
	CodeMemoExpired  diagnostic.Code = "HADR-RUNTIME-042"
	CodeMemoRejected diagnostic.Code = "HADR-RUNTIME-043"
	CodePinRejected  diagnostic.Code = "HADR-RUNTIME-044"
)

var (
	ErrInvalidReuse = errors.New("invalid workflow value reuse")
	ErrReuseDenied  = errors.New("workflow value reuse denied")
)

const (
	maxReuseIdentityBytes       = 256
	maxReusePolicyCodeBytes     = 128
	maxReusePolicyReasonBytes   = 1024
	maxReuseAttributeCount      = 32
	maxReuseAttributeKeyBytes   = 128
	maxReuseAttributeValueBytes = 1024
)

// InvocationOrigin identifies how an invocation reached its durable outcome.
type InvocationOrigin string

const (
	OriginExecuted InvocationOrigin = "executed"
	OriginMemoized InvocationOrigin = "memoized"
	OriginReplayed InvocationOrigin = "replayed"
	OriginPinned   InvocationOrigin = "pinned"
)

func (o InvocationOrigin) Valid() bool {
	return o == OriginExecuted || o == OriginMemoized || o == OriginReplayed || o == OriginPinned
}

func validateInvocationOrigin(status NodeStatus, origin InvocationOrigin, outputs *values.ValueSetRef) error {
	if origin == "" {
		return nil // migration-compatible terminal records may predate origins.
	}
	if !origin.Valid() {
		return fmt.Errorf("unsupported invocation origin %q", origin)
	}
	if !status.Terminal() {
		return fmt.Errorf("invocation origin requires terminal status")
	}
	if origin == OriginMemoized || origin == OriginPinned {
		if status != NodeSucceeded || outputs == nil {
			return fmt.Errorf("%s origin requires succeeded status and outputs", origin)
		}
	}
	return nil
}

// ReuseAuthority is the application-neutral caller identity presented to host
// policy. Attributes must never contain credentials or resolved secrets.
type ReuseAuthority struct {
	Principal  string            `json:"principal"`
	Scope      string            `json:"scope,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

func (a ReuseAuthority) Validate() error {
	if err := validateReuseMetadataText("reuse principal", a.Principal, maxReuseIdentityBytes); err != nil {
		return err
	}
	if a.Scope != "" {
		if err := validateReuseMetadataText("reuse scope", a.Scope, maxReuseIdentityBytes); err != nil {
			return err
		}
	}
	return validateReuseMetadataMap("reuse authority attributes", a.Attributes)
}

// ReusePolicyDecision is an append-only, persistence-safe authorization fact.
type ReusePolicyDecision struct {
	Allow      bool              `json:"allow"`
	Code       string            `json:"code"`
	Reason     string            `json:"reason"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

func (d ReusePolicyDecision) Validate() error {
	if err := validateReuseMetadataText("reuse policy code", d.Code, maxReusePolicyCodeBytes); err != nil {
		return err
	}
	if err := validateReuseMetadataText("reuse policy reason", d.Reason, maxReusePolicyReasonBytes); err != nil {
		return err
	}
	return validateReuseMetadataMap("reuse policy attributes", d.Attributes)
}

func validateReuseMetadataMap(field string, entries map[string]string) error {
	if len(entries) > maxReuseAttributeCount {
		return fmt.Errorf("%s exceeds %d entries", field, maxReuseAttributeCount)
	}
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := validateReuseMetadataText(field+" key", key, maxReuseAttributeKeyBytes); err != nil {
			return err
		}
		if sensitiveReuseMetadataKey(key) {
			return fmt.Errorf("%s key %q is credential-sensitive", field, key)
		}
		if err := validateReuseMetadataText(field+"["+key+"]", entries[key], maxReuseAttributeValueBytes); err != nil {
			return err
		}
	}
	return nil
}

func validateReuseMetadataText(field, value string, limit int) error {
	if err := validateRequiredText(field, value); err != nil {
		return err
	}
	if len(value) > limit {
		return fmt.Errorf("%s exceeds %d bytes", field, limit)
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return fmt.Errorf("%s contains control characters", field)
		}
	}
	lower := strings.ToLower(value)
	for _, marker := range []string{"secret://", "bearer ", "basic ", "token=", "password=", "passwd=", "api_key=", "apikey=", "signature="} {
		if strings.Contains(lower, marker) {
			return fmt.Errorf("%s contains credential-shaped data", field)
		}
	}
	if strings.Contains(value, "://") {
		parsed, parseErr := url.Parse(value)
		if parseErr != nil {
			return fmt.Errorf("%s contains malformed URI data", field)
		}
		if parsed.User != nil || parsed.RawQuery != "" {
			return fmt.Errorf("%s contains credentialed URI or query data", field)
		}
	}
	return nil
}

func sensitiveReuseMetadataKey(key string) bool {
	normalized := strings.ToLower(strings.Map(func(character rune) rune {
		if unicode.IsLetter(character) || unicode.IsDigit(character) {
			return character
		}
		return '_'
	}, key))
	for _, marker := range []string{"secret", "token", "password", "passwd", "authorization", "cookie", "credential", "private_key", "api_key", "apikey", "signature"} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

// ReuseCandidate is the exact non-secret policy input for memo/pin reuse.
type ReuseCandidate struct {
	Origin             InvocationOrigin      `json:"origin"`
	Target             NodeInvocationID      `json:"target"`
	PlanDigest         string                `json:"plan_digest"`
	Definition         graph.Node            `json:"definition"`
	Spec               stepkind.StepKindSpec `json:"spec"`
	Effects            graph.EffectSet       `json:"effects"`
	Outputs            values.ValueSetRef    `json:"outputs"`
	OutputSchemaDigest string                `json:"output_schema_digest"`
	Source             *NodeInvocationID     `json:"source,omitempty"`
	Authority          ReuseAuthority        `json:"authority"`
}

// ReuseAuthorizer is the host policy seam. Read/compute memoization does not
// require it; materialize memoization and every caller-selected pin do.
type ReuseAuthorizer interface {
	AuthorizeReuse(context.Context, ReuseCandidate) (ReusePolicyDecision, error)
}

type ReuseAuthorizerFunc func(context.Context, ReuseCandidate) (ReusePolicyDecision, error)

func (f ReuseAuthorizerFunc) AuthorizeReuse(ctx context.Context, c ReuseCandidate) (ReusePolicyDecision, error) {
	return f(ctx, c)
}

// MemoEntry is an immutable cache publication with its exact source facts.
type MemoEntry struct {
	Key                string              `json:"key"`
	PlanDigest         string              `json:"plan_digest"`
	NodeID             string              `json:"node_id"`
	Kind               string              `json:"kind"`
	KindVersion        string              `json:"kind_version"`
	MemoKeyDigest      string              `json:"memo_key_digest"`
	InputDigest        string              `json:"input_digest"`
	OutputSchemaDigest string              `json:"output_schema_digest"`
	OutputDigest       string              `json:"output_digest"`
	Outputs            values.ValueSetRef  `json:"outputs"`
	Source             NodeInvocationID    `json:"source"`
	SourceAttempt      AttemptID           `json:"source_attempt"`
	SourceOrigin       InvocationOrigin    `json:"source_origin"`
	Effects            graph.EffectSet     `json:"effects"`
	Policy             ReusePolicyDecision `json:"policy"`
	CreatedAt          time.Time           `json:"created_at"`
	ExpiresAt          time.Time           `json:"expires_at"`
}

func (e MemoEntry) Validate() error {
	for _, item := range []struct{ label, digest string }{{"memo key", e.Key}, {"plan", e.PlanDigest}, {"memo expression", e.MemoKeyDigest}, {"input", e.InputDigest}, {"output schema", e.OutputSchemaDigest}, {"output", e.OutputDigest}} {
		if err := values.ValidateDigest(item.digest); err != nil {
			return fmt.Errorf("%s digest: %w", item.label, err)
		}
	}
	if err := graph.ValidateID(e.NodeID); err != nil {
		return err
	}
	if err := validateRequiredText("memo kind", e.Kind); err != nil {
		return err
	}
	if err := validateRequiredText("memo kind version", e.KindVersion); err != nil {
		return err
	}
	if err := e.Outputs.Validate(); err != nil {
		return err
	}
	if e.Outputs.Digest != e.OutputDigest {
		return fmt.Errorf("memo output digest differs from output reference")
	}
	if err := e.Source.Validate(); err != nil {
		return err
	}
	if err := e.SourceAttempt.Validate(); err != nil {
		return fmt.Errorf("memo source attempt: %w", err)
	}
	if e.SourceAttempt.Invocation != e.Source {
		return fmt.Errorf("memo source attempt invocation differs from source")
	}
	if e.SourceOrigin != OriginExecuted && e.SourceOrigin != OriginMemoized && e.SourceOrigin != OriginPinned {
		return fmt.Errorf("unsupported memo source origin %q", e.SourceOrigin)
	}
	if err := validateEffects(e.Effects); err != nil {
		return err
	}
	if len(e.Effects) == 0 || !reflect.DeepEqual(e.Effects, canonicalEffects(e.Effects)) {
		return fmt.Errorf("memo effects must be nonempty and canonical")
	}
	if containsEffect(e.Effects, graph.EffectMutate) || containsEffect(e.Effects, graph.EffectDestructive) {
		return fmt.Errorf("memo publication cannot contain unsafe effects")
	}
	if err := e.Policy.Validate(); err != nil {
		return err
	}
	if !e.Policy.Allow {
		return fmt.Errorf("memo publication requires allowed policy fact")
	}
	if e.CreatedAt.IsZero() || !e.ExpiresAt.After(e.CreatedAt) {
		return fmt.Errorf("memo entry requires ordered creation and expiry")
	}
	return nil
}

// FreshAt applies both the publication expiry and the caller's current max-age
// policy. This prevents an older, longer-lived declaration from widening a
// later invocation's freshness contract.
func (e MemoEntry) FreshAt(now time.Time, maxAge time.Duration) error {
	if err := e.Validate(); err != nil {
		return err
	}
	if now.IsZero() || maxAge <= 0 {
		return fmt.Errorf("memo freshness requires time and positive max age")
	}
	now = now.UTC()
	if !e.ExpiresAt.After(now) || !e.CreatedAt.Add(maxAge).After(now) {
		return fmt.Errorf("memo entry expired")
	}
	return nil
}

// PinBinding durably binds one target invocation to trusted source outputs.
type PinBinding struct {
	Target             NodeInvocationID    `json:"target"`
	PlanDigest         string              `json:"plan_digest"`
	Outputs            values.ValueSetRef  `json:"outputs"`
	OutputSchemaDigest string              `json:"output_schema_digest"`
	Source             NodeInvocationID    `json:"source"`
	SourcePlanDigest   string              `json:"source_plan_digest"`
	SourceOrigin       InvocationOrigin    `json:"source_origin"`
	Authority          ReuseAuthority      `json:"authority"`
	Policy             ReusePolicyDecision `json:"policy"`
	BoundAt            time.Time           `json:"bound_at"`
}

func (b PinBinding) Validate() error {
	if err := b.Target.Validate(); err != nil {
		return err
	}
	if err := b.Source.Validate(); err != nil {
		return err
	}
	if err := values.ValidateDigest(b.PlanDigest); err != nil {
		return err
	}
	if err := values.ValidateDigest(b.SourcePlanDigest); err != nil {
		return err
	}
	if b.PlanDigest != b.SourcePlanDigest {
		return fmt.Errorf("pin source plan is incompatible with target plan")
	}
	if err := b.Outputs.Validate(); err != nil {
		return err
	}
	if err := values.ValidateDigest(b.OutputSchemaDigest); err != nil {
		return err
	}
	if !b.SourceOrigin.Valid() {
		return fmt.Errorf("pin source origin is invalid")
	}
	if err := b.Authority.Validate(); err != nil {
		return err
	}
	if err := b.Policy.Validate(); err != nil {
		return err
	}
	if !b.Policy.Allow || b.BoundAt.IsZero() {
		return fmt.Errorf("pin binding requires allowed policy and timestamp")
	}
	return nil
}

type BindPinRequest struct {
	Binding            PinBinding `json:"binding"`
	ExpectedGeneration uint64     `json:"expected_generation"`
	IdempotencyKey     string     `json:"idempotency_key"`
}

func (r BindPinRequest) Validate() error {
	if err := r.Binding.Validate(); err != nil {
		return err
	}
	if r.ExpectedGeneration == 0 {
		return fmt.Errorf("pin binding expected generation is required")
	}
	return validateRequiredText("pin binding idempotency key", r.IdempotencyKey)
}

type BindPinResult struct {
	Outcome IdempotencyOutcome     `json:"outcome"`
	Binding PinBinding             `json:"binding"`
	Node    NodeInvocationSnapshot `json:"node"`
}

// SemanticallyEqualBindPinRequest ignores the target generation because it is
// an application fence, not immutable pin intent.
func SemanticallyEqualBindPinRequest(left, right BindPinRequest) bool {
	return left.IdempotencyKey == right.IdempotencyKey && reflect.DeepEqual(left.Binding, right.Binding)
}

type ReuseNodeOutputsRequest struct {
	InvocationID       NodeInvocationID   `json:"invocation_id"`
	ExpectedGeneration uint64             `json:"expected_generation"`
	Claim              ClaimProof         `json:"claim"`
	Origin             InvocationOrigin   `json:"origin"`
	Outputs            values.ValueSetRef `json:"outputs"`
	Source             NodeInvocationID   `json:"source"`
	// MemoEntryKey and SourceAttempt bind memoized reuse to the exact immutable
	// cache publication selected by the dispatcher. They are empty for pins,
	// which are instead fenced by the target's durable PinBinding.
	MemoEntryKey   string              `json:"memo_entry_key,omitempty"`
	SourceAttempt  *AttemptID          `json:"source_attempt,omitempty"`
	SourceOrigin   InvocationOrigin    `json:"source_origin"`
	PlanDigest     string              `json:"plan_digest"`
	Policy         ReusePolicyDecision `json:"policy"`
	IdempotencyKey string              `json:"idempotency_key"`
	At             time.Time           `json:"at"`
}

func (r ReuseNodeOutputsRequest) Validate() error {
	if err := r.InvocationID.Validate(); err != nil {
		return err
	}
	if r.ExpectedGeneration == 0 {
		return fmt.Errorf("reuse expected generation is required")
	}
	if err := r.Claim.Validate(); err != nil {
		return err
	}
	if r.Origin != OriginMemoized && r.Origin != OriginPinned {
		return fmt.Errorf("reuse origin must be memoized or pinned")
	}
	if err := r.Outputs.Validate(); err != nil {
		return err
	}
	if err := r.Source.Validate(); err != nil {
		return err
	}
	switch r.Origin {
	case OriginMemoized:
		if err := values.ValidateDigest(r.MemoEntryKey); err != nil {
			return fmt.Errorf("memo entry key: %w", err)
		}
		if r.SourceAttempt == nil {
			return fmt.Errorf("memoized reuse requires source attempt")
		}
		if err := r.SourceAttempt.Validate(); err != nil {
			return fmt.Errorf("memoized reuse source attempt: %w", err)
		}
		if r.SourceAttempt.Invocation != r.Source {
			return fmt.Errorf("memoized reuse source attempt differs from source")
		}
	case OriginPinned:
		if r.MemoEntryKey != "" || r.SourceAttempt != nil {
			return fmt.Errorf("pinned reuse cannot carry memo publication identity")
		}
	case OriginExecuted, OriginReplayed:
		return fmt.Errorf("reuse origin must be memoized or pinned")
	default:
		return fmt.Errorf("reuse origin is unsupported")
	}
	if !r.SourceOrigin.Valid() {
		return fmt.Errorf("reuse source origin is invalid")
	}
	if err := values.ValidateDigest(r.PlanDigest); err != nil {
		return err
	}
	if err := r.Policy.Validate(); err != nil {
		return err
	}
	if !r.Policy.Allow {
		return fmt.Errorf("reuse requires allowed policy")
	}
	if err := validateRequiredText("reuse idempotency key", r.IdempotencyKey); err != nil {
		return err
	}
	if r.At.IsZero() {
		return fmt.Errorf("reuse timestamp is required")
	}
	return nil
}

type ReuseNodeOutputsResult struct {
	Outcome IdempotencyOutcome     `json:"outcome"`
	Node    NodeInvocationSnapshot `json:"node"`
	Event   Event                  `json:"event"`
}

// SemanticallyEqualReuseRequest ignores generation and At, which fence the
// first application. Exact replay returns the originally persisted outcome.
func SemanticallyEqualReuseRequest(left, right ReuseNodeOutputsRequest) bool {
	left.ExpectedGeneration, right.ExpectedGeneration = 0, 0
	left.At, right.At = time.Time{}, time.Time{}
	return reflect.DeepEqual(left, right)
}

// MemoStore is the new runtime truth for append-only memo publications.
// Legacy StateStore cache CRUD remains compatibility scaffolding and is not
// consulted by the dispatcher.
type MemoStore interface {
	RecordMemoEntry(context.Context, MemoEntry) (MemoEntry, IdempotencyOutcome, error)
	LoadMemoEntry(context.Context, string) (MemoEntry, error)
}

// OutputReuseStore atomically completes one claimed, unattempted node from an
// immutable memo publication or pin binding.
type OutputReuseStore interface {
	ReuseNodeOutputs(context.Context, ReuseNodeOutputsRequest) (ReuseNodeOutputsResult, error)
}

// PinStore atomically installs immutable run-scoped pins before admission.
type PinStore interface {
	BindPin(context.Context, BindPinRequest) (BindPinResult, error)
	LoadPin(context.Context, NodeInvocationID) (PinBinding, error)
}

// ValueRecord exposes immutable value ownership for pin validation.
type ValueRecord struct {
	Ref    values.ValueSetRef `json:"ref"`
	Owner  ValueOwner         `json:"owner"`
	Values values.ValueSet    `json:"values"`
}

// Validate proves that a loaded value record is internally self-consistent.
// Host stores are untrusted boundaries: the reference, owner, and content
// digest must agree before source lookup or reuse authorization begins.
func (r ValueRecord) Validate() error {
	if err := r.Ref.Validate(); err != nil {
		return fmt.Errorf("value record reference: %w", err)
	}
	if err := r.Owner.Validate(); err != nil {
		return fmt.Errorf("value record owner: %w", err)
	}
	if err := r.Values.Validate(); err != nil {
		return fmt.Errorf("value record contents: %w", err)
	}
	digest, err := values.DigestValueSet(r.Values)
	if err != nil {
		return fmt.Errorf("digest value record contents: %w", err)
	}
	if digest != r.Ref.Digest {
		return fmt.Errorf("value record content digest differs from reference")
	}
	return nil
}

type ValueRecordStore interface {
	LoadValueRecord(context.Context, values.ValueSetRef) (ValueRecord, error)
}

// DigestSchema validates and hashes canonical JSON Schema bytes.
func DigestSchema(schema graph.Schema) (string, error) {
	if err := values.ValidateSchema(schema); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(schema)
	if err != nil {
		return "", fmt.Errorf("marshal schema: %w", err)
	}
	return values.SHA256Digest(encoded), nil
}

func validateEffects(effects graph.EffectSet) error {
	seen := map[graph.Effect]struct{}{}
	for _, effect := range effects {
		if !effect.Valid() {
			return fmt.Errorf("invalid effect %q", effect)
		}
		if _, ok := seen[effect]; ok {
			return fmt.Errorf("duplicate effect %q", effect)
		}
		seen[effect] = struct{}{}
	}
	return nil
}

func canonicalEffects(effects graph.EffectSet) graph.EffectSet {
	result := append(graph.EffectSet(nil), effects...)
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func (d *StepDispatcher) tryReuseOutputs(ctx, durableCtx context.Context, request DispatchRequest, node NodeInvocationSnapshot, spec stepkind.StepKindSpec, claim ClaimProof) (DispatchResult, bool, []diagnostic.Diagnostic, error) {
	run, err := d.store.LoadRun(durableCtx, node.ID.RunID)
	if err != nil {
		return DispatchResult{}, false, nil, err
	}
	schemaDigest, err := DigestSchema(spec.OutputSchema)
	if err != nil {
		return DispatchResult{}, false, nil, err
	}
	// Explicit run-scoped pins take precedence over memoization.
	if d.pins != nil {
		binding, loadErr := d.pins.LoadPin(durableCtx, node.ID)
		if loadErr == nil {
			if d.reuse == nil {
				return DispatchResult{}, false, []diagnostic.Diagnostic{reuseDiagnostic(CodePinRejected, diagnostic.SeverityError, "the state store cannot atomically consume pinned outputs", request.Node.Source)}, ErrInvalidReuse
			}
			if binding.PlanDigest != run.Plan.Digest || binding.OutputSchemaDigest != schemaDigest {
				return DispatchResult{}, false, []diagnostic.Diagnostic{reuseDiagnostic(CodePinRejected, diagnostic.SeverityError, "pinned outputs are incompatible with the target plan or output schema", request.Node.Source)}, ErrReuseDenied
			}
			set, loadValuesErr := d.store.LoadValues(durableCtx, binding.Outputs)
			if loadValuesErr != nil {
				return DispatchResult{}, false, nil, loadValuesErr
			}
			if schemaErr := values.ValidateValueSetSchema(spec.OutputSchema, set); schemaErr != nil {
				return DispatchResult{}, false, []diagnostic.Diagnostic{reuseDiagnostic(CodePinRejected, diagnostic.SeverityError, "pinned outputs no longer satisfy the target output schema", request.Node.Source)}, schemaErr
			}
			reused, reuseErr := d.reuse.ReuseNodeOutputs(durableCtx, ReuseNodeOutputsRequest{InvocationID: node.ID, ExpectedGeneration: node.Generation, Claim: claim, Origin: OriginPinned, Outputs: binding.Outputs, Source: binding.Source, SourceOrigin: binding.SourceOrigin, PlanDigest: run.Plan.Digest, Policy: binding.Policy, IdempotencyKey: "pin-reuse:" + controlIdentity(node.ID), At: d.atOrAfter(node.UpdatedAt)})
			if reuseErr != nil {
				return DispatchResult{}, false, nil, reuseErr
			}
			return DispatchResult{Node: reused.Node, Outputs: cloneValueSetRef(&binding.Outputs)}, true, nil, nil
		}
		if !errors.Is(loadErr, ErrNotFound) {
			return DispatchResult{}, false, nil, loadErr
		}
	}
	if request.Node.Memoization == nil {
		return DispatchResult{}, false, nil, nil
	}
	if d.memo == nil || d.reuse == nil {
		return DispatchResult{}, false, []diagnostic.Diagnostic{reuseDiagnostic(CodeMemoRejected, diagnostic.SeverityWarning, "the state store does not support durable memoized output reuse", request.Node.Source)}, nil
	}
	if node.MemoKeyDigest == "" || node.Inputs == nil {
		return DispatchResult{}, false, nil, fmt.Errorf("%w: memoized invocation is missing key or inputs", ErrInvalidReuse)
	}
	effects := effectiveEffects(request.Node.Effects, spec.Effects)
	if containsEffect(effects, graph.EffectMutate) || containsEffect(effects, graph.EffectDestructive) {
		return DispatchResult{}, false, []diagnostic.Diagnostic{reuseDiagnostic(CodeMemoRejected, diagnostic.SeverityError, "memoization is forbidden for mutating or destructive effects", request.Node.Source)}, ErrReuseDenied
	}
	if spec.Memoization == stepkind.MemoizationDisabled {
		return DispatchResult{}, false, []diagnostic.Diagnostic{reuseDiagnostic(CodeMemoRejected, diagnostic.SeverityWarning, "the executor disables memoization", request.Node.Source)}, nil
	}
	key := memoCacheKey(run.Plan.Digest, request.Node, node.MemoKeyDigest, schemaDigest)
	entry, loadErr := d.memo.LoadMemoEntry(durableCtx, key)
	if errors.Is(loadErr, ErrNotFound) {
		return DispatchResult{}, false, []diagnostic.Diagnostic{reuseDiagnostic(CodeMemoMiss, diagnostic.SeverityInfo, "no compatible memoized output exists", request.Node.Source)}, nil
	}
	if loadErr != nil {
		return DispatchResult{}, false, nil, loadErr
	}
	now := d.atOrAfter(node.UpdatedAt)
	maxAge, maxAgeErr := time.ParseDuration(string(request.Node.Memoization.MaxAge))
	if maxAgeErr != nil || maxAge <= 0 {
		return DispatchResult{}, false, nil, fmt.Errorf("%w: invalid memo max age", ErrInvalidReuse)
	}
	if !memoEntryFresh(entry, now, maxAge) {
		return DispatchResult{}, false, []diagnostic.Diagnostic{reuseDiagnostic(CodeMemoExpired, diagnostic.SeverityInfo, "the latest memoized output has expired", request.Node.Source)}, nil
	}
	if entry.PlanDigest != run.Plan.Digest || entry.NodeID != request.Node.ID || entry.Kind != request.Node.Kind || entry.KindVersion != request.Node.KindVersion || entry.MemoKeyDigest != node.MemoKeyDigest || entry.OutputSchemaDigest != schemaDigest || !reflect.DeepEqual(entry.Effects, canonicalEffects(effects)) {
		return DispatchResult{}, false, []diagnostic.Diagnostic{reuseDiagnostic(CodeMemoRejected, diagnostic.SeverityWarning, "memoized provenance is incompatible with this invocation", request.Node.Source)}, nil
	}
	if request.Node.Memoization.OutputDigest != "" && request.Node.Memoization.OutputDigest != entry.OutputDigest {
		return DispatchResult{}, false, []diagnostic.Diagnostic{reuseDiagnostic(CodeMemoRejected, diagnostic.SeverityWarning, "memoized output digest does not match the pinned digest", request.Node.Source)}, nil
	}
	set, loadValuesErr := d.store.LoadValues(durableCtx, entry.Outputs)
	if loadValuesErr != nil {
		return DispatchResult{}, false, nil, loadValuesErr
	}
	if !memoValueSetMatchesSchema(spec.OutputSchema, set) {
		return DispatchResult{}, false, []diagnostic.Diagnostic{reuseDiagnostic(CodeMemoRejected, diagnostic.SeverityWarning, "memoized outputs no longer satisfy the output schema", request.Node.Source)}, nil
	}
	if reason := cacheabilityRejection(set, false, ""); reason != "" {
		return DispatchResult{}, false, []diagnostic.Diagnostic{reuseDiagnostic(CodeMemoRejected, diagnostic.SeverityWarning, reason, request.Node.Source)}, nil
	}
	decision, authorized := d.authorizeMemoForReuse(ctx, request, spec, effects, entry, schemaDigest, set)
	if !authorized {
		return DispatchResult{}, false, []diagnostic.Diagnostic{reuseDiagnostic(CodeMemoRejected, diagnostic.SeverityWarning, "memoized outputs were not authorized for this invocation", request.Node.Source)}, nil
	}
	sourceAttempt := entry.SourceAttempt
	reused, reuseErr := d.reuse.ReuseNodeOutputs(durableCtx, ReuseNodeOutputsRequest{InvocationID: node.ID, ExpectedGeneration: node.Generation, Claim: claim, Origin: OriginMemoized, Outputs: entry.Outputs, Source: entry.Source, MemoEntryKey: entry.Key, SourceAttempt: &sourceAttempt, SourceOrigin: entry.SourceOrigin, PlanDigest: run.Plan.Digest, Policy: decision, IdempotencyKey: "memo-reuse:" + controlIdentity(node.ID), At: now})
	if reuseErr != nil {
		return DispatchResult{}, false, nil, reuseErr
	}
	return DispatchResult{Node: reused.Node, Outputs: cloneValueSetRef(&entry.Outputs)}, true, nil, nil
}

func memoEntryFresh(entry MemoEntry, now time.Time, maxAge time.Duration) bool {
	return entry.FreshAt(now, maxAge) == nil
}

func memoValueSetMatchesSchema(schema graph.Schema, set values.ValueSet) bool {
	return values.ValidateValueSetSchema(schema, set) == nil
}

func (d *StepDispatcher) authorizeMemoForReuse(ctx context.Context, request DispatchRequest, spec stepkind.StepKindSpec, effects graph.EffectSet, entry MemoEntry, schemaDigest string, set values.ValueSet) (ReusePolicyDecision, bool) {
	decision, err := d.authorizeMemo(ctx, request, spec, effects, entry, schemaDigest, set)
	return decision, err == nil
}

func (d *StepDispatcher) authorizeMemo(ctx context.Context, request DispatchRequest, spec stepkind.StepKindSpec, effects graph.EffectSet, entry MemoEntry, schemaDigest string, set values.ValueSet) (ReusePolicyDecision, error) {
	needsPolicy := containsEffect(effects, graph.EffectMaterialize) || valueSetNeedsAuthority(set)
	if containsEffect(effects, graph.EffectMaterialize) && spec.Memoization != stepkind.MemoizationApproved {
		return ReusePolicyDecision{}, ErrReuseDenied
	}
	if !needsPolicy {
		return ReusePolicyDecision{Allow: true, Code: "memo_safe_default", Reason: "read/compute public output is safe to reuse"}, nil
	}
	if d.reusePolicy == nil {
		return ReusePolicyDecision{}, ErrReuseDenied
	}
	source := entry.Source
	// Memo authorization may bind the authenticated caller from ctx; unlike a
	// caller-selected pin the core does not invent an identity envelope.
	candidate := ReuseCandidate{Origin: OriginMemoized, Target: request.Claim.Candidate.InvocationID, PlanDigest: entry.PlanDigest, Definition: request.Node, Spec: spec, Effects: canonicalEffects(effects), Outputs: entry.Outputs, OutputSchemaDigest: schemaDigest, Source: &source}
	decision, err := authorizeReuseDefensively(ctx, d.reusePolicy, candidate)
	if err != nil {
		return ReusePolicyDecision{}, err
	}
	if err := decision.Validate(); err != nil {
		return ReusePolicyDecision{}, err
	}
	if !decision.Allow {
		return ReusePolicyDecision{}, ErrReuseDenied
	}
	return decision, nil
}

func (d *StepDispatcher) publishMemoResult(ctx context.Context, request DispatchRequest, spec stepkind.StepKindSpec, inputs values.ValueSet, finished FinishNodeAttemptResult, outputRef values.ValueSetRef) *DispatchWarning {
	if finished.Node.MemoKeyDigest == "" {
		return nil
	}
	if d.memo == nil {
		return memoWarning(errors.New("state store does not support durable memo publications"))
	}
	effects := effectiveEffects(request.Node.Effects, spec.Effects)
	if containsEffect(effects, graph.EffectMutate) || containsEffect(effects, graph.EffectDestructive) || spec.Memoization == stepkind.MemoizationDisabled {
		return nil
	}
	set, err := d.store.LoadValues(ctx, outputRef)
	if err == nil {
		if reason := cacheabilityRejection(set, false, ""); reason != "" {
			err = errors.New(reason)
		}
	}
	if err != nil {
		return memoWarning(err)
	}
	run, err := d.store.LoadRun(ctx, finished.Node.ID.RunID)
	if err != nil {
		return memoWarning(err)
	}
	inputDigest, err := values.DigestValueSet(inputs)
	if err != nil {
		return memoWarning(err)
	}
	schemaDigest, err := DigestSchema(spec.OutputSchema)
	if err != nil {
		return memoWarning(err)
	}
	decision, decisionErr := d.authorizeMemo(ctx, request, spec, effects, MemoEntry{PlanDigest: run.Plan.Digest, Outputs: outputRef, Source: finished.Node.ID}, schemaDigest, set)
	if decisionErr != nil {
		return memoWarning(decisionErr)
	}
	maxAge, err := time.ParseDuration(string(request.Node.Memoization.MaxAge))
	if err != nil || maxAge <= 0 {
		return memoWarning(fmt.Errorf("invalid memo max age"))
	}
	created := finished.Attempt.FinishedAt.UTC()
	entry := MemoEntry{Key: memoCacheKey(run.Plan.Digest, request.Node, finished.Node.MemoKeyDigest, schemaDigest), PlanDigest: run.Plan.Digest, NodeID: request.Node.ID, Kind: request.Node.Kind, KindVersion: request.Node.KindVersion, MemoKeyDigest: finished.Node.MemoKeyDigest, InputDigest: inputDigest, OutputSchemaDigest: schemaDigest, OutputDigest: outputRef.Digest, Outputs: outputRef, Source: finished.Node.ID, SourceAttempt: finished.Attempt.ID, SourceOrigin: OriginExecuted, Effects: canonicalEffects(effects), Policy: decision, CreatedAt: created, ExpiresAt: created.Add(maxAge)}
	if request.Node.Memoization.OutputDigest != "" && request.Node.Memoization.OutputDigest != outputRef.Digest {
		return memoWarning(errors.New("executed output does not match memo output digest"))
	}
	if _, _, err := d.memo.RecordMemoEntry(ctx, entry); err != nil {
		return memoWarning(err)
	}
	return nil
}

func memoWarning(err error) *DispatchWarning {
	failure := Failure{Code: "memo_publication_failed", Message: "memo publication failed", Retryable: true, Details: map[string]string{"stage": string(DispatchPublishMemo)}}
	return &DispatchWarning{Stage: DispatchPublishMemo, Failure: failure, Cause: err}
}

func memoCacheKey(planDigest string, node graph.Node, memoKeyDigest, schemaDigest string) string {
	payload := struct{ Plan, Node, Kind, Version, Memo, Schema string }{planDigest, node.ID, node.Kind, node.KindVersion, memoKeyDigest, schemaDigest}
	encoded, _ := json.Marshal(payload)
	return values.SHA256Digest(encoded)
}

func containsEffect(effects graph.EffectSet, target graph.Effect) bool {
	for _, effect := range effects {
		if effect == target {
			return true
		}
	}
	return false
}

func valueSetNeedsAuthority(set values.ValueSet) bool {
	for _, value := range set {
		if value.Redaction != values.RedactionPublic {
			return true
		}
	}
	return false
}

func cacheabilityRejection(set values.ValueSet, pin bool, sameRun string) string {
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value := set[name]
		if !pin && value.Redaction == values.RedactionSecret {
			return fmt.Sprintf("output %q is secret and cannot be memoized", name)
		}
		if !pin && value.Retention != values.RetentionProject && value.Retention != values.RetentionExternal {
			return fmt.Sprintf("output %q retention %q is not cacheable", name, value.Retention)
		}
		if pin && value.Retention == values.RetentionRun && sameRun == "" {
			return fmt.Sprintf("output %q has run-only retention", name)
		}
	}
	return ""
}

// ValidateMemoizableValueSet rejects classifications or retention modes that
// cannot outlive the source attempt as a reusable cache publication.
func ValidateMemoizableValueSet(set values.ValueSet) error {
	if err := values.ValidatePersistableSet(set); err != nil {
		return err
	}
	if reason := cacheabilityRejection(set, false, ""); reason != "" {
		return fmt.Errorf("%w: %s", ErrReuseDenied, reason)
	}
	return nil
}

// ValidatePinnableValueSet enforces retention at the durable pin boundary.
// Run-retained outputs may only be pinned inside their source run.
func ValidatePinnableValueSet(set values.ValueSet, sameRun bool) error {
	if err := values.ValidatePersistableSet(set); err != nil {
		return err
	}
	withinRun := ""
	if sameRun {
		withinRun = "same-run"
	}
	if reason := cacheabilityRejection(set, true, withinRun); reason != "" {
		return fmt.Errorf("%w: %s", ErrReuseDenied, reason)
	}
	return nil
}

func reuseDiagnostic(code diagnostic.Code, severity diagnostic.Severity, message string, source *graph.SourceRef) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{Severity: severity, Code: code, Message: message, Source: cloneSourceRef(source)}
}

func authorizeReuseDefensively(ctx context.Context, authorizer ReuseAuthorizer, candidate ReuseCandidate) (ReusePolicyDecision, error) {
	encoded, marshalErr := json.Marshal(candidate)
	if marshalErr != nil {
		return ReusePolicyDecision{}, marshalErr
	}
	var cloned ReuseCandidate
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.UseNumber()
	if decodeErr := decoder.Decode(&cloned); decodeErr != nil {
		return ReusePolicyDecision{}, decodeErr
	}
	decision, authorizationErr := authorizer.AuthorizeReuse(ctx, cloned)
	decision.Attributes = cloneStringMapRuntime(decision.Attributes)
	return decision, authorizationErr
}

// PinCoordinator validates a transport-neutral node=<value-ref> request and
// atomically installs the immutable run-scoped binding before admission.
type PinCoordinator struct {
	Store      StateStore
	Pins       PinStore
	Values     ValueRecordStore
	Plans      RecoveryPlanSource
	Registry   stepkind.Registry
	Authorizer ReuseAuthorizer
}

type PinNodeRequest struct {
	Target         NodeInvocationID   `json:"target"`
	Outputs        values.ValueSetRef `json:"outputs"`
	Authority      ReuseAuthority     `json:"authority"`
	IdempotencyKey string             `json:"idempotency_key"`
	At             time.Time          `json:"at"`
}

func (c PinCoordinator) Bind(ctx context.Context, request PinNodeRequest) (BindPinResult, error) {
	if ctx == nil || nilStateStore(c.Store) || nilReflect(c.Pins) || nilReflect(c.Values) || nilRecoveryPlanSource(c.Plans) || nilStepKindRegistry(c.Registry) || nilReflect(c.Authorizer) {
		return BindPinResult{}, fmt.Errorf("%w: pin coordinator collaborators are required", ErrInvalidReuse)
	}
	if err := request.Target.Validate(); err != nil {
		return BindPinResult{}, err
	}
	if err := request.Outputs.Validate(); err != nil {
		return BindPinResult{}, err
	}
	if err := request.Authority.Validate(); err != nil {
		return BindPinResult{}, err
	}
	if err := validateRequiredText("pin idempotency key", request.IdempotencyKey); err != nil {
		return BindPinResult{}, err
	}
	if request.At.IsZero() {
		return BindPinResult{}, fmt.Errorf("pin timestamp is required")
	}
	request.Authority.Attributes = cloneStringMapRuntime(request.Authority.Attributes)
	target, targetErr := c.Store.LoadNodeInvocation(ctx, request.Target)
	if targetErr != nil {
		return BindPinResult{}, targetErr
	}
	run, runErr := c.Store.LoadRun(ctx, request.Target.RunID)
	if runErr != nil {
		return BindPinResult{}, runErr
	}
	plan, planErr := c.Plans.LoadRecoveryPlan(ctx, run)
	if planErr != nil {
		return BindPinResult{}, planErr
	}
	if validationErr := plan.Validate(); validationErr != nil || plan.Ref != run.Plan {
		return BindPinResult{}, fmt.Errorf("%w: invalid target plan", ErrInvalidReuse)
	}
	var definition *graph.Node
	for i := range plan.Plan.Graph.Nodes {
		if plan.Plan.Graph.Nodes[i].ID == request.Target.NodeID {
			definition = &plan.Plan.Graph.Nodes[i]
			break
		}
	}
	if definition == nil {
		return BindPinResult{}, fmt.Errorf("%w: target node absent from plan", ErrInvalidReuse)
	}
	_, spec, resolveErr := stepkind.Resolve(c.Registry, definition.Kind, definition.KindVersion)
	if resolveErr != nil {
		return BindPinResult{}, resolveErr
	}
	record, recordErr := c.Values.LoadValueRecord(ctx, request.Outputs)
	if recordErr != nil {
		return BindPinResult{}, recordErr
	}
	if validationErr := record.Validate(); validationErr != nil {
		return BindPinResult{}, fmt.Errorf("%w: %w", ErrInvalidReuse, validationErr)
	}
	if record.Ref != request.Outputs {
		return BindPinResult{}, fmt.Errorf("%w: loaded value record differs from requested reference", ErrInvalidReuse)
	}
	if record.Owner.Invocation == nil {
		return BindPinResult{}, fmt.Errorf("%w: pinned values are not node outputs", ErrInvalidReuse)
	}
	source, sourceErr := c.Store.LoadNodeInvocation(ctx, *record.Owner.Invocation)
	if sourceErr != nil {
		return BindPinResult{}, sourceErr
	}
	if source.Status != NodeSucceeded || source.Outputs == nil || *source.Outputs != request.Outputs || !source.Origin.Valid() {
		return BindPinResult{}, fmt.Errorf("%w: pin source is not a provenance-bearing succeeded output", ErrInvalidReuse)
	}
	sourceRun, sourceRunErr := c.Store.LoadRun(ctx, source.ID.RunID)
	if sourceRunErr != nil {
		return BindPinResult{}, sourceRunErr
	}
	if sourceRun.Plan.Digest != run.Plan.Digest {
		return BindPinResult{}, fmt.Errorf("%w: source plan differs from target plan", ErrInvalidReuse)
	}
	if schemaErr := values.ValidateValueSetSchema(spec.OutputSchema, record.Values); schemaErr != nil {
		return BindPinResult{}, fmt.Errorf("%w: pinned output schema: %w", ErrInvalidReuse, schemaErr)
	}
	if validationErr := ValidatePinnableValueSet(record.Values, source.ID.RunID == target.ID.RunID); validationErr != nil {
		return BindPinResult{}, validationErr
	}
	schemaDigest, digestErr := DigestSchema(spec.OutputSchema)
	if digestErr != nil {
		return BindPinResult{}, digestErr
	}
	sourceID := source.ID
	candidate := ReuseCandidate{Origin: OriginPinned, Target: target.ID, PlanDigest: run.Plan.Digest, Definition: *definition, Spec: spec, Effects: canonicalEffects(effectiveEffects(definition.Effects, spec.Effects)), Outputs: request.Outputs, OutputSchemaDigest: schemaDigest, Source: &sourceID, Authority: request.Authority}
	decision, authorizationErr := authorizeReuseDefensively(ctx, c.Authorizer, candidate)
	if authorizationErr != nil {
		return BindPinResult{}, authorizationErr
	}
	if validationErr := decision.Validate(); validationErr != nil {
		return BindPinResult{}, validationErr
	}
	if !decision.Allow {
		return BindPinResult{}, ErrReuseDenied
	}
	binding := PinBinding{Target: target.ID, PlanDigest: run.Plan.Digest, Outputs: request.Outputs, OutputSchemaDigest: schemaDigest, Source: source.ID, SourcePlanDigest: sourceRun.Plan.Digest, SourceOrigin: source.Origin, Authority: request.Authority, Policy: decision, BoundAt: request.At.UTC()}
	return c.Pins.BindPin(context.WithoutCancel(ctx), BindPinRequest{Binding: binding, ExpectedGeneration: target.Generation, IdempotencyKey: request.IdempotencyKey})
}
