package generatedchild

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	KindName    = "generated_child"
	KindVersion = "v1"

	FormatWorkflowSource MaterialFormat = "workflow_source"
	FormatGraphIR        MaterialFormat = "graph_ir"

	OutputDefinition = "definition"

	CodeInvalidInvocation = "generated_child_invalid_invocation"
	CodeValidationFailed  = "generated_child_validation_failed"
	CodePolicyDenied      = "generated_child_policy_denied"
	CodeRegistration      = "generated_child_registration_failed"
)

const (
	maxMaterialBytes = 1 << 20
	maxNodes         = 512
	maxEdges         = 4096
	maxStableBytes   = 4096
)

var (
	ErrInvalidOptions       = errors.New("invalid generated-child options")
	ErrInvalidMaterial      = errors.New("invalid generated-child material")
	ErrPolicyDenied         = errors.New("generated-child policy denied")
	ErrRegistrationConflict = errors.New("generated-child registration conflict")
)

// MaterialFormat selects the single typed input representation.
type MaterialFormat string

func (f MaterialFormat) Valid() bool { return f == FormatWorkflowSource || f == FormatGraphIR }

// ProcessRequest is the bounded, typed material handed to the core Processor.
// Value is defensively owned and never contains resolved secret material.
type ProcessRequest struct {
	Format    MaterialFormat              `json:"format"`
	Value     values.Value                `json:"value"`
	Authority string                      `json:"authority"`
	Identity  stepkind.InvocationIdentity `json:"identity"`
}

// PolicySummary is the exact validation-time union of authored declarations
// and frozen registered step-kind contracts. It contains digests rather than
// raw adapter config so authorization cannot become a generated-source oracle.
type PolicySummary struct {
	Effects              graph.EffectSet   `json:"effects"`
	RequiredCapabilities []string          `json:"required_capabilities,omitempty"`
	ConfigDigests        map[string]string `json:"config_digests"`
}

// ProcessedDefinition keeps validation and its effective policy summary
// inseparable through authorization and durable registration.
type ProcessedDefinition struct {
	Definition workflowcompile.ResolvedDefinition `json:"definition"`
	Policy     PolicySummary                      `json:"policy"`
}

// Processor applies the ordinary source compiler and graph validator. It must
// return either diagnostics or one immutable exact-digest definition.
type Processor interface {
	ProcessGenerated(context.Context, ProcessRequest) (ProcessedDefinition, error)
}

// ProcessorFunc adapts a function to Processor.
type ProcessorFunc func(context.Context, ProcessRequest) (ProcessedDefinition, error)

func (f ProcessorFunc) ProcessGenerated(ctx context.Context, request ProcessRequest) (ProcessedDefinition, error) {
	return f(ctx, request)
}

// AuthorizationRequest excludes source/config bytes while retaining exact
// immutable identity and conservative effects for host policy.
type AuthorizationRequest struct {
	Identity             stepkind.InvocationIdentity `json:"identity"`
	Definition           graph.DefinitionRef         `json:"definition"`
	NodeCount            int                         `json:"node_count"`
	EdgeCount            int                         `json:"edge_count"`
	Effects              graph.EffectSet             `json:"effects"`
	RequiredCapabilities []string                    `json:"required_capabilities,omitempty"`
	ConfigDigests        map[string]string           `json:"config_digests"`
}

// AuthorizationDecision is a persistence-safe closed allow/deny fact.
type AuthorizationDecision struct {
	Allow  bool   `json:"allow"`
	Code   string `json:"code"`
	Reason string `json:"reason"`
}

func (d AuthorizationDecision) Validate() error {
	if !stableText(d.Code, 128) || !stableText(d.Reason, 1024) {
		return fmt.Errorf("generated-child authorization decision is invalid")
	}
	return nil
}

// Authorizer owns namespace, trust, provenance, and definition-generation
// policy. It never receives the raw generated source.
type Authorizer interface {
	AuthorizeGenerated(context.Context, AuthorizationRequest) (AuthorizationDecision, error)
}

// AuthorizerFunc adapts a function to Authorizer.
type AuthorizerFunc func(context.Context, AuthorizationRequest) (AuthorizationDecision, error)

func (f AuthorizerFunc) AuthorizeGenerated(ctx context.Context, request AuthorizationRequest) (AuthorizationDecision, error) {
	return f(ctx, request)
}

// RegistrationRequest persists one already validated and authorized immutable
// definition. Registrars must apply exact request-digest idempotency durably.
type RegistrationRequest struct {
	Definition     workflowcompile.ResolvedDefinition `json:"definition"`
	Policy         PolicySummary                      `json:"policy"`
	Authorization  AuthorizationDecision              `json:"authorization"`
	IdempotencyKey string                             `json:"idempotency_key"`
}

// RegistrationOutcome distinguishes a new immutable publication from replay.
type RegistrationOutcome string

const (
	RegistrationApplied  RegistrationOutcome = "applied"
	RegistrationReplayed RegistrationOutcome = "replayed"
)

func (o RegistrationOutcome) Valid() bool {
	return o == RegistrationApplied || o == RegistrationReplayed
}

// Registrar is the host-owned durable definition publication boundary.
type Registrar interface {
	RegisterGenerated(context.Context, RegistrationRequest) (workflowcompile.ResolvedDefinition, RegistrationOutcome, error)
}

// RegistrarFunc adapts a function to Registrar.
type RegistrarFunc func(context.Context, RegistrationRequest) (workflowcompile.ResolvedDefinition, RegistrationOutcome, error)

func (f RegistrarFunc) RegisterGenerated(ctx context.Context, request RegistrationRequest) (workflowcompile.ResolvedDefinition, RegistrationOutcome, error) {
	return f(ctx, request)
}

// Options contains every required extraction-safe collaborator.
type Options struct {
	Processor  Processor
	Authorizer Authorizer
	Registrar  Registrar
	// Resolver must load the exact registered definition through the same
	// durable resolver path later used by ordinary call execution. Publication
	// is not accepted from registrar-local or process-local state alone.
	Resolver workflowcompile.DefinitionResolver
}

func stableText(value string, maximum int) bool {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) || len(value) > maximum {
		return false
	}
	for _, current := range value {
		if unicode.IsControl(current) {
			return false
		}
	}
	return true
}
