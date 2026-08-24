package hoststate

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/parser"
	gosched "github.com/hollis-labs/go-scheduler"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

const (
	ActivationRegistrationVersionV1 = "v1"
	MaximumActivationBindings       = 128
	MaximumActivationEvents         = 256
	MaximumActivationPayloadBytes   = 1 << 20
	MaximumActivationTextBytes      = 1024
	MaximumActivationAttempts       = 1000
)

type ActivationSourceKind string

const (
	ActivationSourceSchedule ActivationSourceKind = "schedule"
	ActivationSourceWebhook  ActivationSourceKind = "webhook"
	ActivationSourceFile     ActivationSourceKind = "file"
	ActivationSourceExternal ActivationSourceKind = "external"
	ActivationSourceTimer    ActivationSourceKind = "timer"
	ActivationSourceCallback ActivationSourceKind = "callback"
)

func (k ActivationSourceKind) Valid() bool {
	switch k {
	case ActivationSourceSchedule, ActivationSourceWebhook, ActivationSourceFile,
		ActivationSourceExternal, ActivationSourceTimer, ActivationSourceCallback:
		return true
	default:
		return false
	}
}

type ActivationAuthority string

const (
	ActivationAuthorityProject  ActivationAuthority = "project_source"
	ActivationAuthorityOperator ActivationAuthority = "operator"
)

func (a ActivationAuthority) Valid() bool {
	return a == ActivationAuthorityProject || a == ActivationAuthorityOperator
}

type ActivationSource struct {
	Kind      ActivationSourceKind `json:"kind"`
	Reference string               `json:"reference"`
	Config    graph.Config         `json:"config,omitempty"`
	OneShot   bool                 `json:"one_shot,omitempty"`
}

func (s ActivationSource) Validate() error {
	if !s.Kind.Valid() || ValidatePublicText(s.Reference, MaximumActivationTextBytes, true) != nil {
		return errors.New("activation source kind or reference is invalid")
	}
	if err := validateActivationSourceConfig(s); err != nil {
		return err
	}
	if (s.Kind == ActivationSourceCallback || s.Kind == ActivationSourceTimer) != s.OneShot {
		return errors.New("only callback and timer activations must be one-shot")
	}
	return validateActivationJSON("activation source config", s.Config, MaximumActivationPayloadBytes)
}

func validateActivationSourceConfig(source ActivationSource) error {
	allowed := map[string]bool{}
	requireString := func(name string) (string, bool) {
		value, ok := source.Config[name].(string)
		return value, ok && validStableText(value, MaximumActivationTextBytes, true)
	}
	switch source.Kind {
	case ActivationSourceSchedule:
		allowed["cron"] = true
		cron, ok := requireString("cron")
		if !ok || gosched.ValidateCron(cron) != nil {
			return errors.New("scheduled activation requires a valid bounded cron expression")
		}
	case ActivationSourceWebhook:
		allowed["path"] = true
		if _, ok := requireString("path"); !ok {
			return errors.New("webhook activation requires a bounded path")
		}
	case ActivationSourceFile:
		allowed["path"], allowed["events"] = true, true
		if _, ok := requireString("path"); !ok || !validActivationStringList(source.Config["events"]) {
			return errors.New("file activation requires a path and bounded event list")
		}
	case ActivationSourceExternal:
		for _, name := range []string{"source", "to", "topic", "type"} {
			allowed[name] = true
		}
		selectors := 0
		for _, name := range []string{"to", "topic", "type"} {
			if _, ok := requireString(name); ok {
				selectors++
			}
		}
		if sourceValue, exists := source.Config["source"]; exists {
			value, ok := sourceValue.(string)
			if !ok || !validStableText(value, MaximumActivationTextBytes, true) {
				return errors.New("external activation source selector is invalid")
			}
		}
		if selectors != 1 {
			return errors.New("external activation requires a bounded source selector")
		}
		if _, hasSource := source.Config["source"]; hasSource && source.Config["type"] == nil {
			return errors.New("external activation source requires an event type")
		}
	case ActivationSourceTimer:
		allowed["fire_at"] = true
		fireAt, ok := requireString("fire_at")
		parsed, err := time.Parse(time.RFC3339Nano, fireAt)
		if !ok || err != nil || parsed.Location() != time.UTC {
			return errors.New("timer activation requires a UTC fire_at")
		}
	case ActivationSourceCallback:
		allowed["path"], allowed["ttl"] = true, true
		if _, ok := requireString("path"); !ok {
			return errors.New("callback activation requires a bounded path")
		}
		if ttlValue, exists := source.Config["ttl"]; exists {
			ttl, ok := ttlValue.(string)
			duration, err := time.ParseDuration(ttl)
			if !ok || err != nil || duration <= 0 {
				return errors.New("callback activation ttl is invalid")
			}
		}
	}
	keys := make([]string, 0, len(source.Config))
	for key := range source.Config {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if !allowed[key] {
			return errors.New("activation source config contains an unsupported field")
		}
	}
	return nil
}

func validActivationStringList(value any) bool {
	var entries []string
	switch input := value.(type) {
	case []string:
		entries = append(entries, input...)
	case []any:
		for _, item := range input {
			entry, ok := item.(string)
			if !ok {
				return false
			}
			entries = append(entries, entry)
		}
	default:
		return false
	}
	if len(entries) == 0 || len(entries) > MaximumActivationEvents {
		return false
	}
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if !validStableText(entry, 128, true) {
			return false
		}
		if _, exists := seen[entry]; exists {
			return false
		}
		seen[entry] = struct{}{}
	}
	return true
}

type ActivationPrincipal struct {
	Principal       string            `json:"principal"`
	SourceAuthority string            `json:"source_authority"`
	Trust           string            `json:"trust"`
	Grants          []string          `json:"grants,omitempty"`
	ExposureRef     string            `json:"exposure_ref"`
	Attributes      map[string]string `json:"attributes,omitempty"`
}

func (p ActivationPrincipal) Validate() error {
	for _, value := range []string{p.Principal, p.SourceAuthority, p.Trust, p.ExposureRef} {
		if ValidatePublicText(value, MaximumActivationTextBytes, true) != nil {
			return errors.New("activation principal or exposure reference is invalid")
		}
	}
	if len(p.Grants) > MaximumIdentityGrants || !sort.StringsAreSorted(p.Grants) {
		return errors.New("activation principal grants must be sorted and bounded")
	}
	for index, grant := range p.Grants {
		if ValidatePublicText(grant, 128, true) != nil || (index > 0 && grant == p.Grants[index-1]) {
			return errors.New("activation principal grants contain an invalid value")
		}
	}
	return ValidatePublicAttributes(p.Attributes)
}

func (p ActivationPrincipal) Clone() ActivationPrincipal {
	p.Grants = append([]string(nil), p.Grants...)
	p.Attributes = cloneStringMap(p.Attributes)
	return p
}

type ActivationRetryPolicy struct {
	MaxAttempts int           `json:"max_attempts"`
	Strategy    string        `json:"strategy,omitempty"`
	Initial     time.Duration `json:"initial_delay,omitempty"`
	Maximum     time.Duration `json:"maximum_delay,omitempty"`
}

func (p ActivationRetryPolicy) Validate() error {
	if p.MaxAttempts < 0 || p.MaxAttempts > MaximumActivationAttempts || p.Initial < 0 || p.Maximum < 0 {
		return errors.New("activation retry limits must be non-negative and bounded")
	}
	switch p.Strategy {
	case "", "none", "constant", "linear", "exponential":
	default:
		return errors.New("activation retry strategy is unsupported")
	}
	if p.Strategy == "" && (p.Initial != 0 || p.Maximum != 0) {
		return errors.New("activation retry delays require an explicit strategy")
	}
	if p.Maximum > 0 && p.Initial > p.Maximum {
		return errors.New("activation retry initial delay exceeds its maximum")
	}
	return nil
}

type ActivationPolicy struct {
	Overlap             graph.OverlapPolicy    `json:"overlap"`
	StartingDeadline    time.Duration          `json:"starting_deadline,omitempty"`
	Catchup             bool                   `json:"catchup,omitempty"`
	RunIDReuse          graph.RunIDReusePolicy `json:"run_id_reuse"`
	DefaultLogicalRunID string                 `json:"default_logical_run_id,omitempty"`
	DeduplicationKey    *graph.Expression      `json:"deduplication_key,omitempty"`
	Retry               ActivationRetryPolicy  `json:"retry"`
}

func (p ActivationPolicy) Validate() error {
	if !p.Overlap.Valid() || !p.RunIDReuse.Valid() || p.StartingDeadline < 0 {
		return errors.New("activation policy contains an unsupported value")
	}
	if p.DefaultLogicalRunID != "" && ValidatePublicText(p.DefaultLogicalRunID, 256, false) != nil {
		return errors.New("activation default logical run id is invalid")
	}
	if p.DeduplicationKey != nil {
		if err := validateActivationExpression(*p.DeduplicationKey); err != nil {
			return fmt.Errorf("activation deduplication key: %w", err)
		}
	}
	return p.Retry.Validate()
}

// ActivationRegistration is Hadron's single durable operational binding for
// schedules, triggers, timers, and callbacks. It is not workflow source.
type ActivationRegistration struct {
	Version         string                   `json:"version"`
	ID              string                   `json:"id"`
	Definition      graph.DefinitionRef      `json:"definition"`
	InputBindings   map[string]graph.Binding `json:"input_bindings,omitempty"`
	Principal       ActivationPrincipal      `json:"principal"`
	RunScope        RunScope                 `json:"run_scope"`
	ExecutionTarget *ExecutionTarget         `json:"execution_target,omitempty"`
	Source          ActivationSource         `json:"source"`
	Authority       ActivationAuthority      `json:"authority"`
	Provenance      graph.Provenance         `json:"provenance"`
	Policy          ActivationPolicy         `json:"policy"`
	Enabled         bool                     `json:"enabled"`
	ExpiresAt       time.Time                `json:"expires_at,omitempty"`
	CreatedAt       time.Time                `json:"created_at"`
	UpdatedAt       time.Time                `json:"updated_at"`
	Generation      uint64                   `json:"generation"`
}

func (r ActivationRegistration) Validate() error {
	if r.Version != ActivationRegistrationVersionV1 || graph.ValidateID(r.ID) != nil {
		return errors.New("activation registration version or id is invalid")
	}
	if err := validateExactDefinitionRef(r.Definition); err != nil {
		return err
	}
	if len(r.InputBindings) > MaximumActivationBindings {
		return errors.New("activation registration has too many input bindings")
	}
	names := make([]string, 0, len(r.InputBindings))
	for name := range r.InputBindings {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if values.ValidateExpressionLocalName(name) != nil {
			return errors.New("activation input binding name is invalid")
		}
		if err := validateActivationBinding(r.InputBindings[name]); err != nil {
			return fmt.Errorf("activation input binding %q: %w", name, err)
		}
	}
	if err := r.Principal.Validate(); err != nil {
		return err
	}
	if err := r.RunScope.Validate(); err != nil {
		return err
	}
	if r.ExecutionTarget != nil {
		if err := r.ExecutionTarget.Validate(); err != nil {
			return err
		}
	}
	if err := r.Source.Validate(); err != nil {
		return err
	}
	if !r.Authority.Valid() || !validProvenance(r.Provenance) {
		return errors.New("activation authority or provenance is invalid")
	}
	if err := r.Policy.Validate(); err != nil {
		return err
	}
	if r.CreatedAt.IsZero() || r.UpdatedAt.IsZero() || r.Generation == 0 || r.UpdatedAt.Before(r.CreatedAt) ||
		!isUTC(r.CreatedAt) || !isUTC(r.UpdatedAt) {
		return errors.New("activation registration timestamps and generation are invalid")
	}
	if !r.ExpiresAt.IsZero() {
		if !isUTC(r.ExpiresAt) || !r.ExpiresAt.After(r.CreatedAt) {
			return errors.New("activation registration expiry is invalid")
		}
	}
	return nil
}

func (r ActivationRegistration) Clone() (ActivationRegistration, error) {
	return cloneActivationJSON(r)
}

type ActivationEvent struct {
	RegistrationID string          `json:"registration_id"`
	IdempotencyKey string          `json:"idempotency_key"`
	OccurredAt     time.Time       `json:"occurred_at"`
	Payload        values.ValueSet `json:"payload"`
	LogicalRunID   string          `json:"logical_run_id,omitempty"`
	SourceRef      string          `json:"source_ref,omitempty"`
}

func (e ActivationEvent) Validate() error {
	if graph.ValidateID(e.RegistrationID) != nil || ValidatePublicText(e.IdempotencyKey, 512, true) != nil ||
		e.OccurredAt.IsZero() || !isUTC(e.OccurredAt) {
		return errors.New("activation event identity or time is invalid")
	}
	if e.LogicalRunID != "" && ValidatePublicText(e.LogicalRunID, 256, true) != nil {
		return errors.New("activation event logical run id is invalid")
	}
	if e.SourceRef != "" && ValidatePublicText(e.SourceRef, MaximumActivationTextBytes, false) != nil {
		return errors.New("activation event source reference is invalid")
	}
	if err := values.ValidatePersistableSet(e.Payload); err != nil {
		return errors.New("activation event payload must be a persistable typed value set")
	}
	encoded, err := json.Marshal(e.Payload)
	if err != nil || len(encoded) > MaximumActivationPayloadBytes {
		return errors.New("activation event payload is not bounded")
	}
	for _, value := range e.Payload {
		if value.Redaction != values.RedactionPrivate || value.Type == values.TypeArtifact || value.Type == values.TypeSecretRef {
			return errors.New("activation event payload currently supports only private inline values")
		}
	}
	return nil
}

func (e ActivationEvent) Clone() (ActivationEvent, error) { return cloneActivationJSON(e) }

type ActivationDispatchStatus string

const (
	ActivationDispatchPending   ActivationDispatchStatus = "pending"
	ActivationDispatchStarting  ActivationDispatchStatus = "starting"
	ActivationDispatchStarted   ActivationDispatchStatus = "started"
	ActivationDispatchSkipped   ActivationDispatchStatus = "skipped"
	ActivationDispatchExhausted ActivationDispatchStatus = "exhausted"
)

func (s ActivationDispatchStatus) Valid() bool {
	switch s {
	case ActivationDispatchPending, ActivationDispatchStarting, ActivationDispatchStarted,
		ActivationDispatchSkipped, ActivationDispatchExhausted:
		return true
	default:
		return false
	}
}

type ActivationDispatch struct {
	FireID         string                   `json:"fire_id"`
	RegistrationID string                   `json:"registration_id"`
	Attempt        int                      `json:"attempt"`
	Status         ActivationDispatchStatus `json:"status"`
	LogicalRunID   string                   `json:"logical_run_id"`
	PhysicalRunID  runtime.RunID            `json:"physical_run_id,omitempty"`
	HostStartKey   string                   `json:"host_start_key,omitempty"`
	ScheduledAt    time.Time                `json:"scheduled_at"`
	ObservedAt     time.Time                `json:"observed_at"`
	ReasonCode     string                   `json:"reason_code,omitempty"`
	Generation     uint64                   `json:"generation"`
}

func (d ActivationDispatch) Validate() error {
	if ValidatePublicText(d.FireID, 256, true) != nil || graph.ValidateID(d.RegistrationID) != nil ||
		d.Attempt < 0 || !d.Status.Valid() || ValidatePublicText(d.LogicalRunID, 256, true) != nil ||
		d.ScheduledAt.IsZero() || d.ObservedAt.IsZero() || !isUTC(d.ScheduledAt) || !isUTC(d.ObservedAt) || d.Generation == 0 {
		return errors.New("activation dispatch is invalid")
	}
	if d.Status == ActivationDispatchStarted || d.Status == ActivationDispatchStarting {
		if ValidatePublicText(string(d.PhysicalRunID), 256, true) != nil || ValidatePublicText(d.HostStartKey, 512, true) != nil {
			return errors.New("started activation dispatch requires physical run identity")
		}
	}
	if d.Status != ActivationDispatchStarted && d.Status != ActivationDispatchStarting && d.Status != ActivationDispatchExhausted &&
		(d.PhysicalRunID != "" || d.HostStartKey != "") {
		return errors.New("non-started activation dispatch cannot carry physical run identity")
	}
	if d.ObservedAt.Before(d.ScheduledAt) {
		return errors.New("activation dispatch observation precedes its scheduled time")
	}
	if d.ReasonCode != "" && ValidatePublicText(d.ReasonCode, 128, false) != nil {
		return errors.New("activation dispatch reason code is invalid")
	}
	return nil
}

// CallbackRegistration is a one-shot Hadron exposure over one canonical core
// wait. CredentialDigest is the only credential-derived fact persisted.
type CallbackRegistration struct {
	Version          string                  `json:"version"`
	ID               string                  `json:"id"`
	WaitID           runtime.WaitID          `json:"wait_id"`
	Correlation      string                  `json:"correlation"`
	WakeSource       workflowwait.WakeSource `json:"wake_source"`
	Responder        workflowwait.Responder  `json:"responder"`
	ValueSchema      graph.Schema            `json:"value_schema"`
	CredentialDigest string                  `json:"credential_digest"`
	ExposureRef      string                  `json:"exposure_ref"`
	ExpiresAt        time.Time               `json:"expires_at"`
	CreatedAt        time.Time               `json:"created_at"`
	ConsumedAt       time.Time               `json:"consumed_at,omitempty"`
	Generation       uint64                  `json:"generation"`
}

func (r CallbackRegistration) Validate() error {
	if r.Version != ActivationRegistrationVersionV1 || graph.ValidateID(r.ID) != nil ||
		(runtime.WaitRef{ID: r.WaitID}).Validate() != nil || ValidatePublicText(r.Correlation, 512, true) != nil ||
		!r.WakeSource.Valid() || r.Responder.Validate() != nil || values.ValidateDigest(r.CredentialDigest) != nil ||
		ValidatePublicText(r.ExposureRef, MaximumActivationTextBytes, true) != nil {
		return errors.New("callback registration identity is invalid")
	}
	if r.CreatedAt.IsZero() || r.ExpiresAt.IsZero() || !isUTC(r.CreatedAt) || !isUTC(r.ExpiresAt) ||
		!r.ExpiresAt.After(r.CreatedAt) || r.Generation == 0 {
		return errors.New("callback registration lifetime is invalid")
	}
	if !r.ConsumedAt.IsZero() && (!isUTC(r.ConsumedAt) || r.ConsumedAt.Before(r.CreatedAt)) {
		return errors.New("callback registration consumed_at is invalid")
	}
	encoded, err := json.Marshal(r.ValueSchema)
	if err != nil || len(encoded) > MaximumActivationPayloadBytes {
		return errors.New("callback value schema is invalid or too large")
	}
	return nil
}

func (r CallbackRegistration) Clone() (CallbackRegistration, error) { return cloneActivationJSON(r) }

func DigestCallbackCredential(raw string) (string, error) {
	if !validStableText(raw, 4096, true) {
		return "", errors.New("callback credential is invalid")
	}
	sum := sha256.Sum256([]byte(raw))
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func validateExactDefinitionRef(ref graph.DefinitionRef) error {
	for _, value := range []string{ref.Authority, ref.Kind, ref.ID, ref.Version} {
		if ValidatePublicText(value, MaximumActivationTextBytes, true) != nil {
			return errors.New("activation definition reference is incomplete or unsafe")
		}
	}
	if ref.Locator != "" && ValidatePublicText(ref.Locator, MaximumActivationTextBytes, false) != nil {
		return errors.New("activation definition locator is unsafe")
	}
	if values.ValidateDigest(ref.Digest) != nil {
		return errors.New("activation definition digest is invalid")
	}
	return nil
}

func validateActivationBinding(binding graph.Binding) error {
	if !binding.Kind.Valid() {
		return errors.New("binding kind is unsupported")
	}
	switch binding.Kind {
	case graph.BindingLiteral:
		if binding.Expression != nil || binding.Interpolation != "" {
			return errors.New("literal binding is ambiguous")
		}
		if _, err := values.DigestInline(binding.Literal); err != nil {
			return errors.New("literal binding is not JSON-compatible")
		}
	case graph.BindingExpression:
		if binding.Expression == nil || binding.Literal != nil || binding.Interpolation != "" {
			return errors.New("expression binding is ambiguous")
		}
		if err := validateActivationExpression(*binding.Expression); err != nil {
			return err
		}
	case graph.BindingInterpolation:
		if binding.Expression != nil || binding.Literal != nil || !validStableText(binding.Interpolation, MaximumActivationPayloadBytes, false) {
			return errors.New("interpolation binding is ambiguous or invalid")
		}
		for _, expression := range activationInterpolationExpressions(binding.Interpolation) {
			if err := validateActivationExpression(graph.Expression{Text: expression}); err != nil {
				return err
			}
		}
	}
	return nil
}

func validProvenance(p graph.Provenance) bool {
	if ValidatePublicText(p.Authority, 256, true) != nil || ValidatePublicText(p.Origin, MaximumActivationTextBytes, true) != nil {
		return false
	}
	for _, value := range []string{p.Locator, p.Revision} {
		if value != "" && ValidatePublicText(value, MaximumActivationTextBytes, false) != nil {
			return false
		}
	}
	if p.Digest != "" && values.ValidateDigest(p.Digest) != nil {
		return false
	}
	if len(p.Parents) > MaximumActivationEvents {
		return false
	}
	for _, parent := range p.Parents {
		if ValidatePublicText(parent.Authority, 256, true) != nil ||
			ValidatePublicText(parent.Locator, MaximumActivationTextBytes, true) != nil ||
			(parent.Digest != "" && values.ValidateDigest(parent.Digest) != nil) {
			return false
		}
	}
	return validateActivationJSON("activation provenance metadata", p.Metadata, MaximumActivationPayloadBytes) == nil
}

func validateActivationExpression(expression graph.Expression) error {
	if !validStableText(expression.Text, MaximumActivationPayloadBytes, true) {
		return errors.New("activation expression is invalid")
	}
	tree, err := parser.Parse(strings.TrimSpace(expression.Text))
	if err != nil {
		return errors.New("activation expression is invalid")
	}
	collector := activationRootCollector{}
	ast.Walk(&tree.Node, collector)
	for root := range collector {
		if !activationExpressionRoot(root) {
			return errors.New("activation expressions may reference only the activation input context")
		}
	}
	return nil
}

type activationRootCollector map[string]struct{}

func (c activationRootCollector) Visit(node *ast.Node) {
	if identifier, ok := (*node).(*ast.IdentifierNode); ok {
		c[identifier.Value] = struct{}{}
	}
}

func activationExpressionRoot(root string) bool {
	switch root {
	case "inputs", "body", "message", "event", "source", "schedule", "registration", "file":
		return true
	default:
		return false
	}
}

func activationInterpolationExpressions(template string) []string {
	result := make([]string, 0)
	for offset := 0; ; {
		start := strings.Index(template[offset:], "{{")
		if start < 0 {
			return result
		}
		start += offset + 2
		end := strings.Index(template[start:], "}}")
		if end < 0 {
			return append(result, "")
		}
		result = append(result, strings.TrimSpace(template[start:start+end]))
		offset = start + end + 2
	}
}

func validateActivationJSON(field string, input any, maximum int) error {
	encoded, err := json.Marshal(input)
	if err != nil || len(encoded) > maximum {
		return fmt.Errorf("%s is not bounded JSON", field)
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("%s is not JSON-compatible", field)
	}
	if activationJSONUnsafe(value) {
		return fmt.Errorf("%s contains secret-shaped or unsafe material", field)
	}
	return nil
}

func activationJSONUnsafe(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		if len(typed) > MaximumActivationEvents {
			return true
		}
		for key, nested := range typed {
			if sensitiveMetadataKey(key) || legacyWorkspaceKey(key) || activationJSONUnsafe(nested) {
				return true
			}
		}
	case []any:
		if len(typed) > MaximumActivationEvents {
			return true
		}
		for _, nested := range typed {
			if activationJSONUnsafe(nested) {
				return true
			}
		}
	case string:
		return unsafeMetadataValue(typed)
	}
	return false
}

func isUTC(value time.Time) bool { return value.Location() == time.UTC }

func cloneActivationJSON[T any](input T) (T, error) {
	var output T
	encoded, err := json.Marshal(input)
	if err != nil {
		return output, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&output); err != nil {
		return output, err
	}
	return output, nil
}
