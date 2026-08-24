package appworkflow

import (
	"context"
	"fmt"
	"strings"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	hadronregistry "github.com/hollis-labs/hadron/internal/registry"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

// SourceActivationRegistry is the immutable registry lookup required by the
// source-activation lifecycle hook. The hook never accepts a movable alias.
type SourceActivationRegistry interface {
	ResolveWorkflow(context.Context, hadronregistry.WorkflowQuery) (hadronregistry.WorkflowResolution, error)
}

// SourceActivationRegistrar is the sole qualified workflow-registration
// boundary used by source activation materialization. ContractRegistrationService
// implements this interface after namespace authorization and contract tests.
type SourceActivationRegistrar interface {
	Register(context.Context, RegisterWorkflowRequest) (hadronregistry.WorkflowRecord, error)
}

// SourceActivationCatalog supplies exact post-registration resolution and
// exact-CAS removal of the mutable current alias. Immutable version history is
// retained by the catalog.
type SourceActivationCatalog interface {
	SourceActivationRegistry
	RemoveCurrentWorkflowExact(context.Context, hadronregistry.WorkflowQuery) error
}

var _ SourceActivationRegistrar = (*ContractRegistrationService)(nil)

// SourceActivationLifecycle binds exact registered versions to the one
// ActivationService reconciliation path. RegistryName is explicit because the
// product catalog name may include a namespace while the graph ID does not.
// Canonical registration and removal are owned by
// SourceActivationRegistrationCoordinator; the lifecycle retains the narrow
// exact-version hooks needed for disable and post-registration replay.
type SourceActivationLifecycle struct {
	Registry    SourceActivationRegistry
	Activations ActivationService
}

type RegisteredSourceActivationRequest struct {
	RegistryName    string                  `json:"registry_name"`
	Materialization SourceActivationRequest `json:"materialization"`
}

// SourceActivationRegistrationRequest qualifies and registers one exact
// workflow through ContractRegistrationService, then reconciles its compiled
// activation templates.
type SourceActivationRegistrationRequest struct {
	Registration    RegisterWorkflowRequest `json:"registration"`
	Materialization SourceActivationRequest `json:"materialization"`
}

type SourceActivationRegistrationResult struct {
	Record      hadronregistry.WorkflowRecord       `json:"record"`
	Activations hoststate.ActivationReconcileResult `json:"activations"`
}

// SourceActivationRegistrationCoordinator is the supported Hadron ingress for
// a qualified workflow registration/current-version change with source-owned
// activations. Validation is completed before the catalog write. A crash after
// the immutable catalog commit but before reconciliation converges by exactly
// replaying RegisterCurrent with the same request.
type SourceActivationRegistrationCoordinator struct {
	Registrar   SourceActivationRegistrar
	Catalog     SourceActivationCatalog
	Activations ActivationService
}

func (c SourceActivationRegistrationCoordinator) RegisterCurrent(ctx context.Context, request SourceActivationRegistrationRequest) (SourceActivationRegistrationResult, error) {
	if ctx == nil || c.Registrar == nil || nilInterface(c.Registrar) || c.Catalog == nil || nilInterface(c.Catalog) ||
		c.Activations.Store == nil || nilInterface(c.Activations.Store) {
		return SourceActivationRegistrationResult{}, fmt.Errorf("%w: source activation registration requires context, registrar, catalog, and activation store", ErrInvalidActivation)
	}
	if err := ctx.Err(); err != nil {
		return SourceActivationRegistrationResult{}, err
	}
	plan, err := validateSourceActivationPlan(request.Materialization.Plan)
	if err != nil {
		return SourceActivationRegistrationResult{}, err
	}
	materialization := request.Materialization
	materialization.Plan = plan
	prepared, err := prepareSourceActivationReconcile(materialization)
	if err != nil {
		return SourceActivationRegistrationResult{}, err
	}
	if !request.Registration.MakeCurrent || !registrationRequestMatchesSourcePlan(request.Registration.Definition, plan.Definition) {
		return SourceActivationRegistrationResult{}, fmt.Errorf("%w: registration request does not select the exact activation plan as current", ErrInvalidActivation)
	}
	record, err := c.Registrar.Register(ctx, request.Registration)
	if err != nil {
		return SourceActivationRegistrationResult{}, err
	}
	if validationErr := validateSourceActivationCatalogRecord(record, plan); validationErr != nil {
		return SourceActivationRegistrationResult{Record: record}, validationErr
	}
	lifecycle := SourceActivationLifecycle{Registry: c.Catalog, Activations: c.Activations}
	activations, err := lifecycle.applyRegistered(context.WithoutCancel(ctx), record.Name, plan, prepared)
	if err != nil {
		return SourceActivationRegistrationResult{Record: record}, err
	}
	return SourceActivationRegistrationResult{Record: record, Activations: activations}, nil
}

func (c SourceActivationRegistrationCoordinator) Disable(ctx context.Context, request RegisteredSourceActivationRequest) (hoststate.ActivationReconcileResult, error) {
	return (SourceActivationLifecycle{Registry: c.Catalog, Activations: c.Activations}).OnDisabled(ctx, request)
}

func (c SourceActivationRegistrationCoordinator) Remove(ctx context.Context, registryName string, request SourceActivationRetireRequest, plan *workflowcompile.ExecutionPlan) (hoststate.ActivationReconcileResult, error) {
	if ctx == nil || c.Catalog == nil || nilInterface(c.Catalog) || c.Activations.Store == nil || nilInterface(c.Activations.Store) {
		return hoststate.ActivationReconcileResult{}, fmt.Errorf("%w: source activation removal requires context, catalog, and activation store", ErrInvalidActivation)
	}
	lifecycle := SourceActivationLifecycle{Registry: c.Catalog, Activations: c.Activations}
	if err := lifecycle.validateRegisteredPlan(ctx, registryName, plan); err != nil {
		return hoststate.ActivationReconcileResult{}, err
	}
	if !sameSourceActivationDefinition(request.Definition, plan.Definition) {
		return hoststate.ActivationReconcileResult{}, fmt.Errorf("%w: removal definition differs from the exact registered plan", ErrInvalidActivation)
	}
	if err := c.Catalog.RemoveCurrentWorkflowExact(ctx, hadronregistry.WorkflowQuery{
		Name: registryName, Version: plan.Definition.Version, Digest: plan.Definition.Digest,
	}); err != nil {
		return hoststate.ActivationReconcileResult{}, err
	}
	// Catalog and activation state have different durable substrates. Removing
	// the exact alias first prevents a stale version from retiring a newer
	// current plan; exact replay finishes retirement after a process loss.
	return lifecycle.retireRegistered(context.WithoutCancel(ctx), registryName, request, plan)
}

func (l SourceActivationLifecycle) OnRegistered(ctx context.Context, request RegisteredSourceActivationRequest) (hoststate.ActivationReconcileResult, error) {
	if err := l.validateRegisteredPlan(ctx, request.RegistryName, request.Materialization.Plan); err != nil {
		return hoststate.ActivationReconcileResult{}, err
	}
	prepared, err := prepareSourceActivationReconcile(request.Materialization)
	if err != nil {
		return hoststate.ActivationReconcileResult{}, err
	}
	return l.Activations.Store.ReconcileDerivedActivations(context.WithoutCancel(ctx), prepared)
}

func (l SourceActivationLifecycle) OnDisabled(ctx context.Context, request RegisteredSourceActivationRequest) (hoststate.ActivationReconcileResult, error) {
	request.Materialization.Enabled = false
	return l.OnRegistered(ctx, request)
}

func (l SourceActivationLifecycle) retireRegistered(ctx context.Context, registryName string, request SourceActivationRetireRequest, plan *workflowcompile.ExecutionPlan) (hoststate.ActivationReconcileResult, error) {
	if err := l.validateRegisteredPlan(ctx, registryName, plan); err != nil {
		return hoststate.ActivationReconcileResult{}, err
	}
	if !sameSourceActivationDefinition(request.Definition, plan.Definition) {
		return hoststate.ActivationReconcileResult{}, fmt.Errorf("%w: removal definition differs from the exact registered plan", ErrInvalidActivation)
	}
	return l.Activations.RetireSourceActivations(ctx, request)
}

func (l SourceActivationLifecycle) applyRegistered(ctx context.Context, registryName string, plan *workflowcompile.ExecutionPlan, prepared hoststate.ActivationReconcileRequest) (hoststate.ActivationReconcileResult, error) {
	if err := l.validateRegisteredPlan(ctx, registryName, plan); err != nil {
		return hoststate.ActivationReconcileResult{}, err
	}
	return l.Activations.Store.ReconcileDerivedActivations(context.WithoutCancel(ctx), prepared)
}

func sameSourceActivationDefinition(left, right graph.DefinitionRef) bool {
	return left.Authority == right.Authority && left.Kind == right.Kind && left.ID == right.ID &&
		left.Version == right.Version && left.Digest == right.Digest && left.Locator == right.Locator
}

func (l SourceActivationLifecycle) validateRegisteredPlan(ctx context.Context, registryName string, input *workflowcompile.ExecutionPlan) error {
	if ctx == nil || l.Registry == nil || nilInterface(l.Registry) || l.Activations.Store == nil || nilInterface(l.Activations.Store) {
		return fmt.Errorf("%w: source activation lifecycle requires context, registry, and activation store", ErrInvalidActivation)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	registryName = strings.TrimSpace(registryName)
	plan, err := validateSourceActivationPlan(input)
	if err != nil {
		return err
	}
	resolution, err := l.Registry.ResolveWorkflow(ctx, hadronregistry.WorkflowQuery{
		Name: registryName, Version: plan.Definition.Version, Digest: plan.Definition.Digest,
	})
	if err != nil {
		return fmt.Errorf("%w: exact source workflow is not registered: %w", ErrInvalidActivation, err)
	}
	record := resolution.Record
	if resolution.Movable || record.Name != registryName || record.Version != plan.Definition.Version ||
		record.Digest != plan.Definition.Digest || record.PlanDigest != plan.Digest || record.Authority != plan.Definition.Authority {
		return fmt.Errorf("%w: registered workflow does not match the exact compiled activation plan", ErrInvalidActivation)
	}
	return nil
}

func validateSourceActivationCatalogRecord(record hadronregistry.WorkflowRecord, plan *workflowcompile.ExecutionPlan) error {
	if strings.TrimSpace(record.Name) == "" || record.Version != plan.Definition.Version || record.Digest != plan.Definition.Digest ||
		record.PlanDigest != plan.Digest || record.Authority != plan.Definition.Authority || values.SHA256Digest(record.Source) != plan.Definition.Digest {
		return fmt.Errorf("%w: catalog record does not bind the exact compiled activation plan", ErrInvalidActivation)
	}
	return nil
}

func registrationRequestMatchesSourcePlan(request, plan graph.DefinitionRef) bool {
	return request.ID == plan.ID && request.Version == plan.Version && request.Digest == plan.Digest &&
		(request.Authority == "" || request.Authority == plan.Authority)
}
