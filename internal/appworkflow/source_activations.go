package appworkflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	hadronregistry "github.com/hollis-labs/hadron/internal/registry"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

// SourceActivationRequest binds compiler-owned activation templates to the
// exact Hadron identity and exposure facts selected at registration time.
// ExposureRefs is keyed by graph activation declaration ID.
type SourceActivationRequest struct {
	Plan *workflowcompile.ExecutionPlan `json:"plan"`
	// SourceOwner identifies the stable logical source whose successive exact
	// plans replace one another. Empty defaults to Plan.Definition for existing
	// file/package callers. Registry lifecycle callers use authority+registry
	// name without version, digest, or ephemeral authoring locator.
	SourceOwner graph.DefinitionRef `json:"source_owner,omitempty"`
	// ExecutionDefinition is the exact immutable definition persisted on each
	// registration and resolved when it fires. Empty defaults to Plan.Definition.
	ExecutionDefinition       graph.DefinitionRef       `json:"execution_definition,omitempty"`
	Identity                  hoststate.IdentityBinding `json:"identity"`
	ExposureRefs              map[string]string         `json:"exposure_refs"`
	ExpectedCurrentPlanDigest string                    `json:"expected_current_plan_digest,omitempty"`
	Enabled                   bool                      `json:"enabled"`
	ExpiresAt                 time.Time                 `json:"expires_at,omitempty"`
	At                        time.Time                 `json:"at"`
}

// SourceActivationRetireRequest removes the current operational projection
// for one source identity while retaining every historical registration.
type SourceActivationRetireRequest struct {
	Definition                graph.DefinitionRef `json:"definition"`
	SourceOwner               graph.DefinitionRef `json:"source_owner,omitempty"`
	ExpectedCurrentPlanDigest string              `json:"expected_current_plan_digest"`
	At                        time.Time           `json:"at"`
}

// ReconcileSourcePlan materializes the exact compiled activation declarations
// and atomically retires registrations derived from the prior plan.
func (s ActivationService) ReconcileSourcePlan(ctx context.Context, request SourceActivationRequest) (hoststate.ActivationReconcileResult, error) {
	if ctx == nil || s.Store == nil || nilInterface(s.Store) {
		return hoststate.ActivationReconcileResult{}, fmt.Errorf("%w: source activation reconciliation requires context and store", ErrInvalidActivation)
	}
	if err := ctx.Err(); err != nil {
		return hoststate.ActivationReconcileResult{}, err
	}
	prepared, err := prepareSourceActivationReconcile(request)
	if err != nil {
		return hoststate.ActivationReconcileResult{}, err
	}
	return s.Store.ReconcileDerivedActivations(context.WithoutCancel(ctx), prepared)
}

func prepareSourceActivationReconcile(request SourceActivationRequest) (hoststate.ActivationReconcileRequest, error) {
	plan, err := validateSourceActivationPlan(request.Plan)
	if err != nil {
		return hoststate.ActivationReconcileRequest{}, err
	}
	identity := request.Identity.Clone()
	if validationErr := identity.Validate(); validationErr != nil {
		return hoststate.ActivationReconcileRequest{}, fmt.Errorf("%w: source activation identity is invalid", ErrInvalidActivation)
	}
	if targetErr := validateSourceActivationTarget(plan, identity.ExecutionTarget); targetErr != nil {
		return hoststate.ActivationReconcileRequest{}, targetErr
	}
	if request.At.IsZero() || request.At.Location() != time.UTC || (!request.ExpiresAt.IsZero() && request.ExpiresAt.Location() != time.UTC) {
		return hoststate.ActivationReconcileRequest{}, fmt.Errorf("%w: source activation reconciliation times must be UTC", ErrInvalidActivation)
	}
	if !request.ExpiresAt.IsZero() && !request.ExpiresAt.After(request.At) {
		return hoststate.ActivationReconcileRequest{}, fmt.Errorf("%w: source activation expiry must follow materialization", ErrInvalidActivation)
	}
	sourceOwner, err := validatedSourceActivationOwner(request.SourceOwner, plan.Definition)
	if err != nil {
		return hoststate.ActivationReconcileRequest{}, err
	}
	executionDefinition, err := validatedSourceActivationExecutionDefinition(request.ExecutionDefinition, plan.Definition)
	if err != nil {
		return hoststate.ActivationReconcileRequest{}, err
	}
	sourceOwnerKey, err := sourceActivationIdentityDigest(sourceOwner)
	if err != nil {
		return hoststate.ActivationReconcileRequest{}, err
	}
	exposures := cloneStringMap(request.ExposureRefs)
	declarations := append([]graph.ActivationDeclaration(nil), plan.Graph.Activations...)
	sort.Slice(declarations, func(left, right int) bool { return declarations[left].ID < declarations[right].ID })
	registrations := make([]hoststate.ActivationRegistration, 0, len(declarations))
	seen := make(map[string]struct{}, len(declarations))
	for _, declaration := range declarations {
		if _, duplicate := seen[declaration.ID]; duplicate {
			return hoststate.ActivationReconcileRequest{}, fmt.Errorf("%w: duplicate compiled activation declaration", ErrInvalidActivation)
		}
		seen[declaration.ID] = struct{}{}
		exposure, exists := exposures[declaration.ID]
		if !exists {
			return hoststate.ActivationReconcileRequest{}, fmt.Errorf("%w: every source activation requires an exposure reference", ErrInvalidActivation)
		}
		delete(exposures, declaration.ID)
		templateDigest, digestErr := hoststate.ActivationTemplateDigest(declaration)
		if digestErr != nil {
			return hoststate.ActivationReconcileRequest{}, fmt.Errorf("%w: activation template cannot be digested", ErrInvalidActivation)
		}
		registration, materializeErr := MaterializeActivationRegistration(ActivationMaterializationRequest{
			Declaration: declaration, Definition: executionDefinition, Identity: identity, ExposureRef: exposure,
			Enabled: request.Enabled, ExpiresAt: request.ExpiresAt, CreatedAt: request.At,
		})
		if materializeErr != nil {
			return hoststate.ActivationReconcileRequest{}, materializeErr
		}
		registration.Authority = hoststate.ActivationAuthorityProject
		materializationDigest, materializationErr := hoststate.ActivationMaterializationDigest(registration, declaration.ID)
		if materializationErr != nil {
			return hoststate.ActivationReconcileRequest{}, fmt.Errorf("%w: materialized activation cannot be digested", ErrInvalidActivation)
		}
		registration.ID, err = hoststate.DerivedActivationRegistrationID(sourceOwnerKey, plan.Digest, declaration.ID, templateDigest, materializationDigest)
		if err != nil {
			return hoststate.ActivationReconcileRequest{}, fmt.Errorf("%w: derived activation identity cannot be constructed", ErrInvalidActivation)
		}
		registration.Derivation = &hoststate.ActivationDerivation{
			SourceOwnerKey: sourceOwnerKey, SourceDigest: plan.Definition.Digest, PlanDigest: plan.Digest,
			TemplateID: declaration.ID, TemplateDigest: templateDigest, MaterializationDigest: materializationDigest,
			CurrentPlanDigest: plan.Digest, SourceGeneration: 1,
		}
		if validationErr := registration.Validate(); validationErr != nil {
			return hoststate.ActivationReconcileRequest{}, fmt.Errorf("%w: derived activation is invalid", ErrInvalidActivation)
		}
		registrations = append(registrations, registration)
	}
	if len(exposures) != 0 {
		return hoststate.ActivationReconcileRequest{}, fmt.Errorf("%w: exposure references contain an unknown activation", ErrInvalidActivation)
	}
	return hoststate.ActivationReconcileRequest{
		SourceOwnerKey: sourceOwnerKey, ExpectedCurrentPlanDigest: request.ExpectedCurrentPlanDigest,
		PlanDigest: plan.Digest, Registrations: registrations, At: request.At,
	}, nil
}

// RetireSourceActivations removes source-owned registrations from admission
// without deleting their immutable registrations or operational history.
func (s ActivationService) RetireSourceActivations(ctx context.Context, request SourceActivationRetireRequest) (hoststate.ActivationReconcileResult, error) {
	if ctx == nil || s.Store == nil || nilInterface(s.Store) {
		return hoststate.ActivationReconcileResult{}, fmt.Errorf("%w: source activation retirement requires context and store", ErrInvalidActivation)
	}
	if err := ctx.Err(); err != nil {
		return hoststate.ActivationReconcileResult{}, err
	}
	if request.At.IsZero() || request.At.Location() != time.UTC || values.ValidateDigest(request.ExpectedCurrentPlanDigest) != nil {
		return hoststate.ActivationReconcileResult{}, fmt.Errorf("%w: source activation retirement is invalid", ErrInvalidActivation)
	}
	sourceOwner, err := validatedSourceActivationOwner(request.SourceOwner, request.Definition)
	if err != nil {
		return hoststate.ActivationReconcileResult{}, err
	}
	sourceOwnerKey, err := sourceActivationIdentityDigest(sourceOwner)
	if err != nil {
		return hoststate.ActivationReconcileResult{}, err
	}
	return s.Store.ReconcileDerivedActivations(context.WithoutCancel(ctx), hoststate.ActivationReconcileRequest{
		SourceOwnerKey: sourceOwnerKey, ExpectedCurrentPlanDigest: request.ExpectedCurrentPlanDigest, At: request.At,
	})
}

func validateSourceActivationPlan(input *workflowcompile.ExecutionPlan) (*workflowcompile.ExecutionPlan, error) {
	if input == nil {
		return nil, fmt.Errorf("%w: compiled activation plan is required", ErrInvalidActivation)
	}
	encoded, err := jsonCanonical(input)
	if err != nil {
		return nil, fmt.Errorf("%w: compiled activation plan is not JSON-compatible", ErrInvalidActivation)
	}
	var plan workflowcompile.ExecutionPlan
	if decodeErr := decodeCanonical(encoded, &plan); decodeErr != nil {
		return nil, fmt.Errorf("%w: compiled activation plan cannot be cloned", ErrInvalidActivation)
	}
	digest, err := workflowcompile.PlanDigest(plan)
	graphDigest, graphErr := workflowcompile.GraphDigest(plan.Graph)
	if err != nil || graphErr != nil || digest != plan.Digest || graphDigest != plan.Graph.Digest ||
		values.ValidateDigest(plan.Digest) != nil || plan.Definition.Digest == "" ||
		plan.Definition.ID != plan.Graph.ID || plan.Definition.Version != plan.Graph.Version ||
		len(plan.SourceDigests) != 1 || !graphNativeActivationSourceFormat(plan.SourceDigests[0].Format) ||
		plan.SourceDigests[0].Digest != plan.Definition.Digest || plan.Provenance.Digest != plan.Definition.Digest {
		return nil, fmt.Errorf("%w: compiled activation plan identity is invalid", ErrInvalidActivation)
	}
	return &plan, nil
}

func graphNativeActivationSourceFormat(format graph.SourceFormat) bool {
	return format == graph.SourceWorkflow || format == graph.SourceAgent || format == graph.SourceSDK || format == graph.SourceUI
}

func validatedSourceActivationOwner(requested, fallback graph.DefinitionRef) (graph.DefinitionRef, error) {
	if requested == (graph.DefinitionRef{}) {
		return fallback, nil
	}
	if requested.Authority == "" || requested.Kind != DefinitionKindRegistry || requested.ID == "" ||
		requested.Locator != "" || requested.Version != "" || requested.Digest != "" || requested.Provenance != nil ||
		hadronregistry.ValidateWorkflowName(requested.ID) != nil || requested.Authority != fallback.Authority {
		return graph.DefinitionRef{}, fmt.Errorf("%w: source activation owner is invalid", ErrInvalidActivation)
	}
	return requested, nil
}

func validatedSourceActivationExecutionDefinition(requested, fallback graph.DefinitionRef) (graph.DefinitionRef, error) {
	if requested == (graph.DefinitionRef{}) {
		return fallback, nil
	}
	if requested.Authority == "" || requested.Kind != DefinitionKindRegistry || requested.ID == "" ||
		requested.Locator != "" || requested.Provenance != nil || hadronregistry.ValidateWorkflowName(requested.ID) != nil ||
		requested.Authority != fallback.Authority || requested.Version != fallback.Version || requested.Digest != fallback.Digest ||
		values.ValidateDigest(requested.Digest) != nil {
		return graph.DefinitionRef{}, fmt.Errorf("%w: source activation execution definition is invalid", ErrInvalidActivation)
	}
	return requested, nil
}

func validateSourceActivationTarget(plan *workflowcompile.ExecutionPlan, target *hoststate.ExecutionTarget) error {
	requirements := make(map[string]graph.ExecutionTargetRequirements, len(plan.Graph.Nodes)+1)
	requirements["$graph"] = plan.Graph.Target
	capabilitySet := make(map[string]struct{})
	for _, capability := range plan.Graph.Target.Capabilities {
		capabilitySet[capability] = struct{}{}
	}
	for _, node := range plan.Graph.Nodes {
		requirements[node.ID] = node.Target
		for _, capability := range node.Target.Capabilities {
			capabilitySet[capability] = struct{}{}
		}
	}
	capabilities := make([]string, 0, len(capabilitySet))
	for capability := range capabilitySet {
		capabilities = append(capabilities, capability)
	}
	sort.Strings(capabilities)
	if err := hoststate.ValidateExecutionTargetBinding(target, capabilities, requirements); err != nil {
		return fmt.Errorf("%w: source activation execution target does not satisfy the exact plan", ErrInvalidActivation)
	}
	return nil
}

func sourceActivationIdentityDigest(definition graph.DefinitionRef) (string, error) {
	if definition.Authority == "" || definition.Kind == "" || definition.ID == "" {
		return "", fmt.Errorf("%w: source definition identity is incomplete", ErrInvalidActivation)
	}
	identity := []any{definition.Authority, definition.Kind, definition.ID, definition.Locator}
	digest, err := values.DigestInline(identity)
	if err != nil {
		return "", fmt.Errorf("%w: source definition identity cannot be digested", ErrInvalidActivation)
	}
	return digest, nil
}

// These wrappers keep canonical exact-number cloning local to appworkflow.
func jsonCanonical(value any) ([]byte, error) { return json.Marshal(value) }

func decodeCanonical(encoded []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}
