package gate

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

const (
	maxPromptBytes      = 16 << 10
	maxLabelBytes       = 512
	maxOptions          = 128
	maxEscalations      = 32
	maximumEscalation   = 365 * 24 * time.Hour
	PolicyEnvironment   = "environment"
	AuthorityGatePolicy = "gate_policy"
)

// OptionKind distinguishes an ordinary decision from the explicit skip path
// required by an optional blocking gate.
type OptionKind string

const (
	OptionDecision OptionKind = "decision"
	OptionSkip     OptionKind = "skip"
)

// Valid reports whether k belongs to the closed gate option vocabulary.
func (k OptionKind) Valid() bool { return k == OptionDecision || k == OptionSkip }

// Option is one stable decision that a presentation layer may render. IDs are
// workflow data; labels are presentation text. Product-specific UI metadata is
// intentionally absent.
type Option struct {
	ID    string     `json:"id"`
	Label string     `json:"label"`
	Kind  OptionKind `json:"kind,omitempty"`
}

// Validate checks the portable option declaration.
func (o Option) Validate() error {
	if err := identifier("gate option id", o.ID); err != nil {
		return err
	}
	if err := stableText("gate option label", o.Label, maxLabelBytes); err != nil {
		return err
	}
	if o.Kind != "" && !o.Kind.Valid() {
		return fmt.Errorf("unsupported gate option kind %q", o.Kind)
	}
	return nil
}

// PolicySubject names application-owned policy without embedding its rules.
// Environment-bound gates use Kind=environment and Reference=<environment>.
type PolicySubject struct {
	Kind       string            `json:"kind"`
	Reference  string            `json:"reference"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// Validate checks that a policy subject is stable and transport-safe.
func (s PolicySubject) Validate() error {
	if err := identifier("gate policy subject kind", s.Kind); err != nil {
		return err
	}
	if err := stableText("gate policy subject reference", s.Reference, 4096); err != nil {
		return err
	}
	return stringMap("gate policy subject attributes", s.Attributes)
}

// Escalation is portable escalation vocabulary. After is a positive Go
// duration string; Subject identifies policy, routing, or notification logic
// supplied by an application.
type Escalation struct {
	After   string        `json:"after"`
	Subject PolicySubject `json:"subject"`
}

// Validate checks the bounded escalation declaration without performing it.
func (e Escalation) Validate() error {
	duration, err := time.ParseDuration(e.After)
	if err != nil || duration <= 0 || duration > maximumEscalation {
		return fmt.Errorf("gate escalation after must be a positive duration no greater than %s", maximumEscalation)
	}
	return e.Subject.Validate()
}

// Behavior describes whether a manual gate is required and whether it blocks
// graph readiness. Non-blocking gates must be optional. The v1 human-gate
// executor intentionally rejects non-blocking behavior because W07-T09 owns
// lowering it to ordinary wait/readiness graph semantics.
type Behavior struct {
	Optional bool `json:"optional"`
	Blocking bool `json:"blocking"`
}

// Validate rejects the incoherent required-but-non-blocking combination.
func (b Behavior) Validate() error {
	if !b.Blocking && !b.Optional {
		return fmt.Errorf("a non-blocking gate must be optional")
	}
	return nil
}

// Checkpoint is the immutable application-neutral gate declaration. Resume
// schema is digest-bound and must accept an object containing the selected
// decision. Prompt and options are suitable for a private gate payload; they
// are not responder credentials.
type Checkpoint struct {
	Prompt       string                 `json:"prompt"`
	Options      []Option               `json:"options"`
	ResumeSchema workflowwait.SchemaRef `json:"resume_schema"`
	Subject      PolicySubject          `json:"subject"`
	Correlation  string                 `json:"correlation"`
	Behavior     Behavior               `json:"behavior"`
	Escalations  []Escalation           `json:"escalations,omitempty"`
}

// Validate checks the complete shared gate contract deterministically.
func (c Checkpoint) Validate() error {
	if err := stableText("gate prompt", c.Prompt, maxPromptBytes); err != nil {
		return err
	}
	if len(c.Options) == 0 || len(c.Options) > maxOptions {
		return fmt.Errorf("gate options must contain between 1 and %d entries", maxOptions)
	}
	seen := make(map[string]struct{}, len(c.Options))
	skipCount := 0
	for index, option := range c.Options {
		if err := option.Validate(); err != nil {
			return fmt.Errorf("gate options[%d]: %w", index, err)
		}
		if _, duplicate := seen[option.ID]; duplicate {
			return fmt.Errorf("gate options contain duplicate id %q", option.ID)
		}
		seen[option.ID] = struct{}{}
		if option.Kind == OptionSkip {
			skipCount++
		}
	}
	if err := c.ResumeSchema.Validate(); err != nil {
		return fmt.Errorf("gate resume schema: %w", err)
	}
	if err := c.Subject.Validate(); err != nil {
		return err
	}
	if err := stableText("gate correlation", c.Correlation, 4096); err != nil {
		return err
	}
	if err := c.Behavior.Validate(); err != nil {
		return err
	}
	if c.Behavior.Optional && c.Behavior.Blocking && skipCount != 1 {
		return fmt.Errorf("an optional gate requires exactly one skip option")
	}
	if c.Behavior.Optional && !c.Behavior.Blocking && skipCount > 1 {
		return fmt.Errorf("an optional non-blocking gate may declare at most one skip option")
	}
	if !c.Behavior.Optional && skipCount != 0 {
		return fmt.Errorf("a required gate must not declare a skip option")
	}
	if len(c.Escalations) > maxEscalations {
		return fmt.Errorf("gate escalations exceed %d entries", maxEscalations)
	}
	previous := time.Duration(0)
	for index, escalation := range c.Escalations {
		if err := escalation.Validate(); err != nil {
			return fmt.Errorf("gate escalations[%d]: %w", index, err)
		}
		after, _ := time.ParseDuration(escalation.After)
		if after <= previous {
			return fmt.Errorf("gate escalations must be strictly ordered by after")
		}
		previous = after
	}
	return nil
}

// AuthorizationRequest supplies a validated immutable checkpoint to an
// application-owned authority resolver. Identity values are opaque execution
// references and must not be interpreted by workflow core.
type AuthorizationRequest struct {
	RunID      string     `json:"run_id"`
	NodeID     string     `json:"node_id"`
	Iteration  string     `json:"iteration,omitempty"`
	Attempt    int        `json:"attempt"`
	Checkpoint Checkpoint `json:"checkpoint"`
}

// AuthorityResolver selects the durable responder authority enforced by the
// runtime's generic resume authorizer. Implementations must be concurrency-safe
// and must not return bearer credentials in Authority fields.
type AuthorityResolver interface {
	ResolveGateAuthority(context.Context, AuthorizationRequest) (workflowwait.ResponderAuthority, error)
}

// AuthorityResolverFunc adapts a function to AuthorityResolver.
type AuthorityResolverFunc func(context.Context, AuthorizationRequest) (workflowwait.ResponderAuthority, error)

// ResolveGateAuthority implements AuthorityResolver.
func (f AuthorityResolverFunc) ResolveGateAuthority(ctx context.Context, request AuthorizationRequest) (workflowwait.ResponderAuthority, error) {
	if f == nil {
		return workflowwait.ResponderAuthority{}, fmt.Errorf("gate authority resolver is unavailable")
	}
	return f(ctx, request)
}

// PayloadRequest describes private checkpoint material to store before a wait
// is suspended. The returned immutable reference enters wait.Record.Payload.
type PayloadRequest struct {
	RunID      string     `json:"run_id"`
	NodeID     string     `json:"node_id"`
	Iteration  string     `json:"iteration,omitempty"`
	Attempt    int        `json:"attempt"`
	Checkpoint Checkpoint `json:"checkpoint"`
}

// PayloadStore persists prompt/options as a private, run-retained value set.
// Implementations must be concurrency-safe and idempotent for the same
// immutable PayloadRequest, returning the same immutable ref on executor
// retries. The host owns cleanup for a ref stored before durable suspension.
type PayloadStore interface {
	StoreGatePayload(context.Context, PayloadRequest) (values.ValueSetRef, error)
}

// PayloadStoreFunc adapts a function to PayloadStore.
type PayloadStoreFunc func(context.Context, PayloadRequest) (values.ValueSetRef, error)

// StoreGatePayload implements PayloadStore.
func (f PayloadStoreFunc) StoreGatePayload(ctx context.Context, request PayloadRequest) (values.ValueSetRef, error) {
	if f == nil {
		return values.ValueSetRef{}, fmt.Errorf("gate payload store is unavailable")
	}
	return f(ctx, request)
}

// CloneCheckpoint returns a defensive copy suitable for passing to a host.
func CloneCheckpoint(checkpoint Checkpoint) Checkpoint {
	copyCheckpoint := checkpoint
	copyCheckpoint.Options = append([]Option(nil), checkpoint.Options...)
	copyCheckpoint.Escalations = append([]Escalation(nil), checkpoint.Escalations...)
	copyCheckpoint.Subject.Attributes = cloneMap(checkpoint.Subject.Attributes)
	copyCheckpoint.ResumeSchema.Schema = cloneSchema(checkpoint.ResumeSchema.Schema)
	for i := range copyCheckpoint.Escalations {
		copyCheckpoint.Escalations[i].Subject.Attributes = cloneMap(checkpoint.Escalations[i].Subject.Attributes)
	}
	return copyCheckpoint
}

func cloneSchema(schema map[string]any) map[string]any {
	copySchema := make(map[string]any, len(schema))
	for key, value := range schema {
		copySchema[key] = cloneJSON(value)
	}
	return copySchema
}

func cloneJSON(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		copyMap := make(map[string]any, len(typed))
		for key, child := range typed {
			copyMap[key] = cloneJSON(child)
		}
		return copyMap
	case []any:
		copySlice := make([]any, len(typed))
		for i, child := range typed {
			copySlice[i] = cloneJSON(child)
		}
		return copySlice
	default:
		return value
	}
}

func cloneMap(entries map[string]string) map[string]string {
	if entries == nil {
		return nil
	}
	copyEntries := make(map[string]string, len(entries))
	for key, value := range entries {
		copyEntries[key] = value
	}
	return copyEntries
}

func identifier(field, value string) error {
	if err := stableText(field, value, 128); err != nil {
		return err
	}
	for index, r := range value {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || index > 0 && (r == '-' || r == '_' || r == '.') {
			continue
		}
		return fmt.Errorf("%s must use a normalized identifier", field)
	}
	return nil
}

func stableText(field, value string, maximum int) error {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) {
		return fmt.Errorf("%s must be non-empty stable UTF-8", field)
	}
	if len(value) > maximum {
		return fmt.Errorf("%s exceeds %d bytes", field, maximum)
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return fmt.Errorf("%s must not contain control characters", field)
		}
	}
	return nil
}

func stringMap(field string, entries map[string]string) error {
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := identifier(field+" key", key); err != nil {
			return err
		}
		if err := stableText(field+"["+key+"]", entries[key], 4096); err != nil {
			return err
		}
	}
	return nil
}
