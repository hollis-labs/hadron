package appworkflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	hadronregistry "github.com/hollis-labs/hadron/internal/registry"
	"github.com/hollis-labs/go-workflow/authoring"
	workflowcompile "github.com/hollis-labs/go-workflow/compile"
	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
)

const (
	MaximumLifecycleSearchResults = 100
	maximumLifecycleCandidates    = 512
	maximumLifecycleQueryBytes    = 1024
	maximumLifecycleActivations   = MaximumPublishedWorkflows
)

// WorkflowLifecycleOperations is the single authenticated authoring, catalog,
// qualification, packaging, publication, and exposure-management contract
// projected by Hadron transports.
type WorkflowLifecycleOperations interface {
	SearchWorkflowCatalog(context.Context, SearchWorkflowCatalogRequest) (WorkflowCatalogSearchResult, error)
	InspectWorkflowVersion(context.Context, InspectWorkflowVersionRequest) (WorkflowVersionDetail, error)
	ValidateWorkflowDraft(context.Context, ValidateWorkflowDraftRequest) (WorkflowDraftValidationResult, error)
	GenerateWorkflowContract(context.Context, GenerateWorkflowContractRequest) (WorkflowContractScaffoldResult, error)
	TestWorkflowDraft(context.Context, TestWorkflowDraftRequest) (WorkflowContractTestResult, error)
	RegisterWorkflowDraft(context.Context, RegisterWorkflowDraftRequest) (WorkflowRegistrationResult, error)
	PackageWorkflowVersion(context.Context, PackageWorkflowVersionRequest) (WorkflowPackageResult, error)
	PublishWorkflowVersion(context.Context, MutateWorkflowVersionRequest) (WorkflowVersionDetail, error)
	PinRegistryVersion(context.Context, MutateWorkflowVersionRequest) (WorkflowVersionDetail, error)
	UnpinRegistryVersion(context.Context, MutateWorkflowVersionRequest) (WorkflowVersionDetail, error)
	ClearWorkflowCurrentExact(context.Context, MutateWorkflowVersionRequest) (WorkflowVersionDetail, error)
	InspectWorkflowExposure(context.Context, InspectWorkflowExposureRequest) (hoststate.ExposureProfileSnapshot, error)
	PinWorkflowExposure(context.Context, MutateWorkflowExposureRequest) (hoststate.ExposureProfileSnapshot, error)
	UnpinWorkflowExposure(context.Context, MutateWorkflowExposureRequest) (hoststate.ExposureProfileSnapshot, error)
}

type WorkflowLifecycleOptions struct {
	Identity  IdentityProvider
	Contracts *ContractRegistrationService
	Authoring *AgentAuthoringService
	Exposure  *WorkflowExposureService
	// SourceActivations is the optional graph-native current-version projection.
	// It is host composition, never a transport-selected registration path.
	SourceActivations *SourceActivationLifecycle
}

type WorkflowLifecycleService struct {
	identity          IdentityProvider
	contracts         *ContractRegistrationService
	authoring         *AgentAuthoringService
	exposure          *WorkflowExposureService
	sourceActivations *SourceActivationLifecycle
}

func NewWorkflowLifecycleService(options WorkflowLifecycleOptions) (*WorkflowLifecycleService, error) {
	if nilInterface(options.Identity) || options.Contracts == nil || options.Authoring == nil || options.Exposure == nil {
		return nil, fmt.Errorf("%w: identity, contracts, authoring, and exposure are required", ErrInvalidHost)
	}
	if options.SourceActivations != nil && (nilInterface(options.SourceActivations.Registry) || nilInterface(options.SourceActivations.Activations.Store)) {
		return nil, fmt.Errorf("%w: source activation lifecycle is incomplete", ErrInvalidHost)
	}
	return &WorkflowLifecycleService{
		identity: options.Identity, contracts: options.Contracts, authoring: options.Authoring,
		exposure: options.Exposure, sourceActivations: options.SourceActivations,
	}, nil
}

type WorkflowDraft struct {
	Envelope  json.RawMessage `json:"envelope"`
	ID        string          `json:"id"`
	Version   string          `json:"version"`
	Namespace string          `json:"namespace"`
}

type WorkflowDraftValidationResult struct {
	Definition  *graph.DefinitionRef          `json:"definition,omitempty"`
	Plan        *workflowruntime.PlanRef      `json:"plan,omitempty"`
	Diagnostics []authoring.CompactDiagnostic `json:"diagnostics"`
}

type WorkflowContractScaffoldResult struct {
	Validation WorkflowDraftValidationResult `json:"validation"`
	Scaffold   *WorkflowContractSuite        `json:"scaffold,omitempty"`
}

type WorkflowContractCaseEvidence struct {
	Name         string          `json:"name"`
	Passed       bool            `json:"passed"`
	OutputDigest string          `json:"output_digest,omitempty"`
	FailureCode  string          `json:"failure_code,omitempty"`
	Effects      graph.EffectSet `json:"effects"`
}

type WorkflowContractEvidence struct {
	PlanDigest  string                         `json:"plan_digest"`
	SuiteDigest string                         `json:"suite_digest"`
	TestDigest  string                         `json:"test_digest"`
	Passed      bool                           `json:"passed"`
	Cases       []WorkflowContractCaseEvidence `json:"cases"`
}

type WorkflowContractTestResult struct {
	Validation WorkflowDraftValidationResult `json:"validation"`
	Evidence   *WorkflowContractEvidence     `json:"evidence,omitempty"`
}

type WorkflowRegistryState struct {
	Current        bool `json:"current"`
	RegistryPinned bool `json:"registry_pinned"`
	Published      bool `json:"published"`
}

type WorkflowVersionDetail struct {
	Descriptor   WorkflowExposureDescriptor     `json:"descriptor"`
	Registry     WorkflowRegistryState          `json:"registry"`
	Activations  []WorkflowActivationDescriptor `json:"activations"`
	RegisteredAt time.Time                      `json:"registered_at"`
}

// WorkflowActivationDescriptor is the compact public handle for one exact
// source-derived activation. It intentionally excludes principal, input
// bindings, trigger configuration, policy, payload, and provenance internals.
type WorkflowActivationDescriptor struct {
	TemplateID     string              `json:"template_id"`
	Kind           string              `json:"kind"`
	RegistrationID string              `json:"registration_id"`
	Enabled        bool                `json:"enabled"`
	Retired        bool                `json:"retired"`
	Definition     graph.DefinitionRef `json:"definition"`
}

type WorkflowCatalogMatch struct {
	Definition      graph.DefinitionRef           `json:"definition"`
	Name            string                        `json:"name"`
	Namespace       string                        `json:"namespace"`
	Description     string                        `json:"description,omitempty"`
	Tags            []string                      `json:"tags"`
	Effects         graph.EffectSet               `json:"effects"`
	Evidence        WorkflowQualificationEvidence `json:"evidence"`
	Registry        WorkflowRegistryState         `json:"registry"`
	Score           int                           `json:"score"`
	Reasons         []string                      `json:"reasons"`
	RecommendedNext string                        `json:"recommended_next"`
}

type WorkflowCatalogSearchResult struct {
	Matches   []WorkflowCatalogMatch `json:"matches"`
	Truncated bool                   `json:"truncated"`
	NextStep  string                 `json:"next_step"`
}

type SearchWorkflowCatalogRequest struct {
	Namespace string          `json:"namespace,omitempty"`
	Query     string          `json:"query,omitempty"`
	Limit     int             `json:"limit,omitempty"`
	Identity  IdentityRequest `json:"identity"`
}

type InspectWorkflowVersionRequest struct {
	Definition graph.DefinitionRef `json:"definition"`
	Identity   IdentityRequest     `json:"identity"`
}

type ValidateWorkflowDraftRequest struct {
	Draft    WorkflowDraft   `json:"draft"`
	Identity IdentityRequest `json:"identity"`
}

type GenerateWorkflowContractRequest = ValidateWorkflowDraftRequest

type TestWorkflowDraftRequest struct {
	Draft    WorkflowDraft         `json:"draft"`
	Suite    WorkflowContractSuite `json:"suite"`
	Identity IdentityRequest       `json:"identity"`
}

type RegisterWorkflowDraftRequest struct {
	Draft       WorkflowDraft         `json:"draft"`
	Suite       WorkflowContractSuite `json:"suite"`
	MakeCurrent bool                  `json:"make_current,omitempty"`
	Identity    IdentityRequest       `json:"identity"`
}

type WorkflowRegistrationResult struct {
	Detail      *WorkflowVersionDetail        `json:"detail,omitempty"`
	Evidence    *WorkflowContractEvidence     `json:"evidence,omitempty"`
	Diagnostics []authoring.CompactDiagnostic `json:"diagnostics"`
}

type PackageWorkflowVersionRequest struct {
	Definition graph.DefinitionRef   `json:"definition"`
	Suite      WorkflowContractSuite `json:"suite"`
	Identity   IdentityRequest       `json:"identity"`
}

type WorkflowPackageResult struct {
	Definition graph.DefinitionRef `json:"definition"`
	Digest     string              `json:"digest"`
	SizeBytes  int64               `json:"size_bytes"`
}

type MutateWorkflowVersionRequest struct {
	Definition graph.DefinitionRef `json:"definition"`
	Identity   IdentityRequest     `json:"identity"`
}

type InspectWorkflowExposureRequest struct {
	ProfileID string          `json:"profile_id"`
	Identity  IdentityRequest `json:"identity"`
}

type MutateWorkflowExposureRequest struct {
	ProfileID          string              `json:"profile_id"`
	Definition         graph.DefinitionRef `json:"definition"`
	ExpectedGeneration uint64              `json:"expected_generation"`
	Identity           IdentityRequest     `json:"identity"`
}

func (s *WorkflowLifecycleService) SearchWorkflowCatalog(ctx context.Context, request SearchWorkflowCatalogRequest) (WorkflowCatalogSearchResult, error) {
	binding, err := s.bind(ctx, request.Identity)
	if err != nil {
		return WorkflowCatalogSearchResult{}, err
	}
	query := strings.TrimSpace(request.Query)
	if len(query) > maximumLifecycleQueryBytes || !utf8.ValidString(query) || strings.IndexFunc(query, unicode.IsControl) >= 0 {
		return WorkflowCatalogSearchResult{}, ErrWorkflowInvalidRequest
	}
	limit := request.Limit
	if limit == 0 {
		limit = 20
	}
	if limit < 1 || limit > MaximumLifecycleSearchResults {
		return WorkflowCatalogSearchResult{}, ErrWorkflowInvalidRequest
	}
	records, err := s.contracts.Search(ctx, request.Namespace, "", binding.Principal)
	if err != nil {
		return WorkflowCatalogSearchResult{}, err
	}
	candidateTruncated := len(records) > maximumLifecycleCandidates
	if candidateTruncated {
		records = records[:maximumLifecycleCandidates]
	}
	terms := lifecycleSearchTerms(query)
	matches := make([]WorkflowCatalogMatch, 0, min(limit, len(records)))
	for _, record := range records {
		detail, detailErr := s.summaryDetailForRecord(ctx, record)
		if detailErr != nil {
			return WorkflowCatalogSearchResult{}, detailErr
		}
		score, reasons := rankWorkflow(detail.Descriptor, terms)
		if len(terms) != 0 && score == 0 {
			continue
		}
		matches = append(matches, catalogMatch(detail, score, reasons))
	}
	sort.SliceStable(matches, func(i, j int) bool {
		if matches[i].Score != matches[j].Score {
			return matches[i].Score > matches[j].Score
		}
		if matches[i].Name != matches[j].Name {
			return matches[i].Name < matches[j].Name
		}
		if matches[i].Definition.Version != matches[j].Definition.Version {
			return matches[i].Definition.Version < matches[j].Definition.Version
		}
		return matches[i].Definition.Digest < matches[j].Definition.Digest
	})
	truncated := candidateTruncated || len(matches) > limit
	if len(matches) > limit {
		matches = matches[:limit]
	}
	next := "draft_validate"
	if len(matches) != 0 {
		next = "inspect_exact"
	}
	if matches == nil {
		matches = []WorkflowCatalogMatch{}
	}
	return WorkflowCatalogSearchResult{Matches: matches, Truncated: truncated, NextStep: next}, nil
}

func (s *WorkflowLifecycleService) InspectWorkflowVersion(ctx context.Context, request InspectWorkflowVersionRequest) (WorkflowVersionDetail, error) {
	binding, err := s.bind(ctx, request.Identity)
	if err != nil {
		return WorkflowVersionDetail{}, err
	}
	query, err := exactLifecycleQuery(request.Definition)
	if err != nil {
		return WorkflowVersionDetail{}, err
	}
	record, err := s.contracts.Inspect(ctx, query, binding.Principal)
	if err != nil {
		return WorkflowVersionDetail{}, err
	}
	return s.detailForRecord(ctx, record)
}

func (s *WorkflowLifecycleService) ValidateWorkflowDraft(ctx context.Context, request ValidateWorkflowDraftRequest) (WorkflowDraftValidationResult, error) {
	binding, err := s.bind(ctx, request.Identity)
	if err != nil {
		return WorkflowDraftValidationResult{}, err
	}
	result, err := s.authoring.author(ctx, agentRequest(request.Draft, nil, false), binding.Principal, false)
	return draftValidation(result), err
}

func (s *WorkflowLifecycleService) GenerateWorkflowContract(ctx context.Context, request GenerateWorkflowContractRequest) (WorkflowContractScaffoldResult, error) {
	binding, err := s.bind(ctx, request.Identity)
	if err != nil {
		return WorkflowContractScaffoldResult{}, err
	}
	result, err := s.authoring.author(ctx, agentRequest(request.Draft, nil, false), binding.Principal, false)
	return WorkflowContractScaffoldResult{Validation: draftValidation(result), Scaffold: result.Scaffold}, err
}

func (s *WorkflowLifecycleService) TestWorkflowDraft(ctx context.Context, request TestWorkflowDraftRequest) (WorkflowContractTestResult, error) {
	binding, err := s.bind(ctx, request.Identity)
	if err != nil {
		return WorkflowContractTestResult{}, err
	}
	result, err := s.authoring.author(ctx, agentRequest(request.Draft, &request.Suite, false), binding.Principal, false)
	return WorkflowContractTestResult{Validation: draftValidation(result), Evidence: safeContractEvidence(result.Report)}, err
}

func (s *WorkflowLifecycleService) RegisterWorkflowDraft(ctx context.Context, request RegisterWorkflowDraftRequest) (WorkflowRegistrationResult, error) {
	binding, err := s.bind(ctx, request.Identity)
	if err != nil {
		return WorkflowRegistrationResult{}, err
	}
	var registration agentRegistrationFunc
	if request.MakeCurrent && s.sourceActivations != nil {
		registration = func(registrationCtx context.Context, contractRequest RegisterWorkflowRequest, authoredPlan *workflowcompile.ExecutionPlan) (hadronregistry.WorkflowRecord, error) {
			return s.registerCurrentWithSourceActivations(registrationCtx, contractRequest, authoredPlan, binding)
		}
	}
	result, authorErr := s.authoring.authorWithRegistration(ctx, agentRequest(request.Draft, &request.Suite, request.MakeCurrent), binding.Principal, true, registration)
	response := WorkflowRegistrationResult{Evidence: safeContractEvidence(result.Report), Diagnostics: append([]authoring.CompactDiagnostic(nil), result.Diagnostics...)}
	if result.Registration != nil {
		ref := graph.DefinitionRef{ID: result.Registration.Name, Version: result.Registration.Version, Digest: result.Registration.Digest}
		record, inspectErr := s.contracts.Inspect(ctx, hadronregistry.WorkflowQuery{Name: ref.ID, Version: ref.Version, Digest: ref.Digest}, binding.Principal)
		if inspectErr != nil {
			return response, inspectErr
		}
		detail, detailErr := s.detailForRecord(ctx, record)
		if detailErr != nil {
			return response, detailErr
		}
		response.Detail = &detail
	}
	return response, authorErr
}

func (s *WorkflowLifecycleService) registerCurrentWithSourceActivations(ctx context.Context, request RegisterWorkflowRequest, authoredPlan *workflowcompile.ExecutionPlan, binding hoststate.IdentityBinding) (hadronregistry.WorkflowRecord, error) {
	if authoredPlan == nil || s.sourceActivations == nil {
		return hadronregistry.WorkflowRecord{}, ErrHostNotReady
	}
	if len(authoredPlan.Graph.Activations) > maximumLifecycleActivations {
		return hadronregistry.WorkflowRecord{}, fmt.Errorf("%w: workflow activation catalog exceeds %d descriptors", ErrInvalidActivation, maximumLifecycleActivations)
	}
	exposureRefs, err := sourceActivationExposureRefs(request.Namespace+"/"+authoredPlan.ID, authoredPlan)
	if err != nil {
		return hadronregistry.WorkflowRecord{}, err
	}
	registryName := request.Namespace + "/" + authoredPlan.ID
	owner := lifecycleSourceActivationOwner(authoredPlan.Definition.Authority, registryName)
	executionDefinition := lifecycleSourceActivationExecutionDefinition(authoredPlan.Definition.Authority, registryName, authoredPlan.Definition)
	preflight := SourceActivationRequest{
		Plan: authoredPlan, SourceOwner: owner, ExecutionDefinition: executionDefinition,
		Identity: binding, ExposureRefs: exposureRefs, Enabled: true,
		At: time.Unix(1, 0).UTC(),
	}
	if _, preparationErr := prepareSourceActivationReconcile(preflight); preparationErr != nil {
		return hadronregistry.WorkflowRecord{}, preparationErr
	}
	record, err := s.contracts.Register(ctx, request)
	if err != nil {
		return hadronregistry.WorkflowRecord{}, err
	}
	ref := graph.DefinitionRef{Kind: DefinitionKindRegistry, ID: record.Name, Version: record.Version, Digest: record.Digest}
	resolved, err := s.exposure.definitions.ResolvePlan(ctx, ref)
	if err != nil {
		return record, err
	}
	if resolved.Digest != record.PlanDigest {
		return record, fmt.Errorf("%w: registered activation plan digest changed", ErrInvalidActivation)
	}
	owner = lifecycleSourceActivationOwner(record.Authority, record.Name)
	projection, err := currentSourceActivationProjection(ctx, s.sourceActivations.Activations.Store, owner)
	if err != nil {
		return record, err
	}
	at := s.sourceActivations.Activations.now()
	if at.Before(projection.UpdatedAt) {
		at = projection.UpdatedAt
	}
	resolvedExposureRefs, err := sourceActivationExposureRefs(record.Name, resolved)
	if err != nil {
		return record, err
	}
	_, err = s.sourceActivations.OnRegistered(context.WithoutCancel(ctx), RegisteredSourceActivationRequest{
		RegistryName: record.Name,
		Materialization: SourceActivationRequest{
			Plan: resolved, SourceOwner: owner,
			ExecutionDefinition: lifecycleSourceActivationExecutionDefinition(record.Authority, record.Name, resolved.Definition),
			Identity:            binding, ExposureRefs: resolvedExposureRefs,
			ExpectedCurrentPlanDigest: projection.PlanDigest, Enabled: true, At: at,
		},
	})
	return record, err
}

func lifecycleSourceActivationOwner(authority, registryName string) graph.DefinitionRef {
	return graph.DefinitionRef{Authority: authority, Kind: DefinitionKindRegistry, ID: registryName}
}

func lifecycleSourceActivationExecutionDefinition(authority, registryName string, source graph.DefinitionRef) graph.DefinitionRef {
	return graph.DefinitionRef{
		Authority: authority, Kind: DefinitionKindRegistry, ID: registryName,
		Version: source.Version, Digest: source.Digest,
	}
}

func sourceActivationExposureRefs(registryName string, plan *workflowcompile.ExecutionPlan) (map[string]string, error) {
	result := make(map[string]string, len(plan.Graph.Activations))
	definition := graph.DefinitionRef{Kind: DefinitionKindRegistry, ID: registryName, Version: plan.Graph.Version, Digest: plan.Definition.Digest}
	for _, activation := range plan.Graph.Activations {
		encoded, err := EncodeWorkflowActivationExposureRef(definition, activation.ID)
		if err != nil {
			return nil, err
		}
		result[activation.ID] = encoded
	}
	return result, nil
}

type sourceActivationProjection struct {
	PlanDigest string
	UpdatedAt  time.Time
}

func currentSourceActivationProjection(ctx context.Context, store hoststate.ActivationStore, definition graph.DefinitionRef) (sourceActivationProjection, error) {
	owner, err := sourceActivationIdentityDigest(definition)
	if err != nil {
		return sourceActivationProjection{}, err
	}
	registrations, err := store.ListDerivedActivations(ctx, owner)
	if err != nil {
		return sourceActivationProjection{}, err
	}
	projection := sourceActivationProjection{}
	for _, registration := range registrations {
		if registration.Derivation == nil {
			return sourceActivationProjection{}, fmt.Errorf("%w: derived activation has no source binding", ErrInvalidActivation)
		}
		candidate := registration.Derivation.CurrentPlanDigest
		if projection.PlanDigest != "" && candidate != projection.PlanDigest {
			return sourceActivationProjection{}, fmt.Errorf("%w: durable activation projection is inconsistent", ErrActivationConflict)
		}
		projection.PlanDigest = candidate
		if registration.UpdatedAt.After(projection.UpdatedAt) {
			projection.UpdatedAt = registration.UpdatedAt
		}
	}
	return projection, nil
}

func (s *WorkflowLifecycleService) PackageWorkflowVersion(ctx context.Context, request PackageWorkflowVersionRequest) (WorkflowPackageResult, error) {
	binding, err := s.bind(ctx, request.Identity)
	if err != nil {
		return WorkflowPackageResult{}, err
	}
	query, err := exactLifecycleQuery(request.Definition)
	if err != nil {
		return WorkflowPackageResult{}, err
	}
	if _, authorizeErr := s.contracts.authorizePackage(ctx, query, binding.Principal); authorizeErr != nil {
		return WorkflowPackageResult{}, authorizeErr
	}
	ref := graph.DefinitionRef{Kind: DefinitionKindRegistry, ID: query.Name, Version: query.Version, Digest: query.Digest}
	report, err := s.contracts.ExecuteContractTests(ctx, ref, request.Suite)
	if err != nil {
		return WorkflowPackageResult{}, err
	}
	archive, err := s.contracts.Package(ctx, query, request.Suite, report, binding.Principal)
	if err != nil {
		return WorkflowPackageResult{}, err
	}
	return WorkflowPackageResult{Definition: ref, Digest: archive.Digest, SizeBytes: int64(len(archive.Bytes))}, nil
}

func (s *WorkflowLifecycleService) PublishWorkflowVersion(ctx context.Context, request MutateWorkflowVersionRequest) (WorkflowVersionDetail, error) {
	binding, query, err := s.boundExact(ctx, request.Identity, request.Definition)
	if err != nil {
		return WorkflowVersionDetail{}, err
	}
	record, err := s.contracts.Publish(ctx, query, binding.Principal)
	if err != nil {
		return WorkflowVersionDetail{}, err
	}
	return s.detailForRecord(ctx, record)
}

func (s *WorkflowLifecycleService) PinRegistryVersion(ctx context.Context, request MutateWorkflowVersionRequest) (WorkflowVersionDetail, error) {
	binding, query, err := s.boundExact(ctx, request.Identity, request.Definition)
	if err != nil {
		return WorkflowVersionDetail{}, err
	}
	record, err := s.contracts.Pin(ctx, query, binding.Principal)
	if err != nil {
		return WorkflowVersionDetail{}, err
	}
	return s.detailForRecord(ctx, record)
}

func (s *WorkflowLifecycleService) UnpinRegistryVersion(ctx context.Context, request MutateWorkflowVersionRequest) (WorkflowVersionDetail, error) {
	binding, query, err := s.boundExact(ctx, request.Identity, request.Definition)
	if err != nil {
		return WorkflowVersionDetail{}, err
	}
	record, err := s.contracts.Inspect(ctx, query, binding.Principal)
	if err != nil {
		return WorkflowVersionDetail{}, err
	}
	if err := s.contracts.UnpinExact(ctx, query, binding.Principal); err != nil {
		return WorkflowVersionDetail{}, err
	}
	return s.detailForRecord(ctx, record)
}

type workflowCurrentAliasCatalog interface {
	RemoveCurrentWorkflowExact(context.Context, hadronregistry.WorkflowQuery) error
}

func (s *WorkflowLifecycleService) ClearWorkflowCurrentExact(ctx context.Context, request MutateWorkflowVersionRequest) (WorkflowVersionDetail, error) {
	binding, query, err := s.boundExact(ctx, request.Identity, request.Definition)
	if err != nil {
		return WorkflowVersionDetail{}, err
	}
	record, err := s.contracts.Inspect(ctx, query, binding.Principal)
	if err != nil {
		return WorkflowVersionDetail{}, err
	}
	catalog, ok := s.contracts.catalog.(workflowCurrentAliasCatalog)
	if !ok {
		return WorkflowVersionDetail{}, ErrHostNotReady
	}
	if err := s.contracts.authorizeRequested(ctx, NamespaceClearCurrent, record.Namespace, record.Name, binding.Principal); err != nil {
		return WorkflowVersionDetail{}, err
	}
	if err := s.contracts.authorizeRecord(ctx, NamespaceClearCurrent, record.Namespace, record.Name, record, binding.Principal); err != nil {
		return WorkflowVersionDetail{}, err
	}
	if s.sourceActivations == nil {
		if err := catalog.RemoveCurrentWorkflowExact(ctx, query); err != nil {
			return WorkflowVersionDetail{}, err
		}
	} else {
		plan, resolveErr := s.exposure.definitions.ResolvePlan(ctx, request.Definition)
		if resolveErr != nil {
			return WorkflowVersionDetail{}, resolveErr
		}
		activationCatalog, ok := s.contracts.catalog.(SourceActivationCatalog)
		if !ok {
			return WorkflowVersionDetail{}, ErrHostNotReady
		}
		owner := lifecycleSourceActivationOwner(record.Authority, record.Name)
		projection, projectionErr := currentSourceActivationProjection(ctx, s.sourceActivations.Activations.Store, owner)
		if projectionErr != nil {
			return WorkflowVersionDetail{}, projectionErr
		}
		at := s.sourceActivations.Activations.now()
		if at.Before(projection.UpdatedAt) {
			at = projection.UpdatedAt
		}
		if projection.PlanDigest == "" {
			if removeErr := activationCatalog.RemoveCurrentWorkflowExact(ctx, query); removeErr != nil {
				return WorkflowVersionDetail{}, removeErr
			}
			return s.detailForRecord(ctx, record)
		}
		coordinator := SourceActivationRegistrationCoordinator{Catalog: activationCatalog, Activations: s.sourceActivations.Activations}
		if _, removeErr := coordinator.Remove(ctx, record.Name, SourceActivationRetireRequest{
			Definition: plan.Definition, SourceOwner: owner,
			ExpectedCurrentPlanDigest: projection.PlanDigest, At: at,
		}, plan); removeErr != nil {
			return WorkflowVersionDetail{}, removeErr
		}
	}
	return s.detailForRecord(ctx, record)
}

func (s *WorkflowLifecycleService) InspectWorkflowExposure(ctx context.Context, request InspectWorkflowExposureRequest) (hoststate.ExposureProfileSnapshot, error) {
	if _, err := s.bind(ctx, request.Identity); err != nil {
		return hoststate.ExposureProfileSnapshot{}, err
	}
	return s.exposure.GetProfile(ctx, request.ProfileID)
}

func (s *WorkflowLifecycleService) PinWorkflowExposure(ctx context.Context, request MutateWorkflowExposureRequest) (hoststate.ExposureProfileSnapshot, error) {
	if _, err := s.bind(ctx, request.Identity); err != nil {
		return hoststate.ExposureProfileSnapshot{}, err
	}
	return s.exposure.PinProfileDefinition(ctx, request.ProfileID, request.Definition, request.ExpectedGeneration)
}

func (s *WorkflowLifecycleService) UnpinWorkflowExposure(ctx context.Context, request MutateWorkflowExposureRequest) (hoststate.ExposureProfileSnapshot, error) {
	if _, err := s.bind(ctx, request.Identity); err != nil {
		return hoststate.ExposureProfileSnapshot{}, err
	}
	return s.exposure.UnpinProfileDefinition(ctx, request.ProfileID, request.Definition, request.ExpectedGeneration)
}

func (s *WorkflowLifecycleService) boundExact(ctx context.Context, identity IdentityRequest, ref graph.DefinitionRef) (hoststate.IdentityBinding, hadronregistry.WorkflowQuery, error) {
	binding, err := s.bind(ctx, identity)
	if err != nil {
		return hoststate.IdentityBinding{}, hadronregistry.WorkflowQuery{}, err
	}
	query, err := exactLifecycleQuery(ref)
	return binding, query, err
}

func (s *WorkflowLifecycleService) bind(ctx context.Context, request IdentityRequest) (hoststate.IdentityBinding, error) {
	if ctx == nil || s == nil || nilInterface(s.identity) {
		return hoststate.IdentityBinding{}, ErrWorkflowUnauthenticated
	}
	request = normalizeIdentityRequest(request)
	binding, err := s.identity.BindIdentity(ctx, request)
	if err != nil {
		return hoststate.IdentityBinding{}, err
	}
	binding = normalizeIdentity(binding)
	if err := binding.Validate(); err != nil {
		return hoststate.IdentityBinding{}, ErrWorkflowUnauthenticated
	}
	if request.RunScope != nil && !request.RunScope.Matches(binding.RunScope) {
		return hoststate.IdentityBinding{}, ErrPolicyDenied
	}
	if request.ExecutionTarget != nil && (binding.ExecutionTarget == nil || !request.ExecutionTarget.Matches(*binding.ExecutionTarget)) {
		return hoststate.IdentityBinding{}, ErrPolicyDenied
	}
	return binding, nil
}

func (s *WorkflowLifecycleService) detailForRecord(ctx context.Context, record hadronregistry.WorkflowRecord) (WorkflowVersionDetail, error) {
	detail, err := s.summaryDetailForRecord(ctx, record)
	if err != nil {
		return WorkflowVersionDetail{}, err
	}
	activations, err := s.activationDescriptorsForRecord(ctx, record)
	if err != nil {
		return WorkflowVersionDetail{}, err
	}
	detail.Activations = activations
	return detail, nil
}

// summaryDetailForRecord intentionally omits operational activation handles.
// Ranked catalog search does not project them and must not turn one bounded
// search into an activation-store scan per candidate.
func (s *WorkflowLifecycleService) summaryDetailForRecord(ctx context.Context, record hadronregistry.WorkflowRecord) (WorkflowVersionDetail, error) {
	descriptor, err := s.exposure.describeRecord(ctx, WorkflowExposureSession{Authenticated: true, Profile: hoststate.ExposureProfileRecord{ID: "lifecycle", MaxDirectTools: hoststate.MaximumDirectTools, SearchScope: hoststate.ExposureSearchAll}}, record, "lifecycle_inspect")
	if err != nil {
		return WorkflowVersionDetail{}, err
	}
	state := WorkflowRegistryState{Published: record.Published}
	current, currentErr := s.contracts.catalog.ResolveWorkflow(ctx, hadronregistry.WorkflowQuery{Name: record.Name})
	if currentErr == nil {
		state.Current = sameRegistryRecord(current.Record, record)
	} else if !errors.Is(currentErr, hadronregistry.ErrWorkflowNotFound) {
		return WorkflowVersionDetail{}, currentErr
	}
	pinned, pinnedErr := s.contracts.catalog.ResolvePinnedWorkflow(ctx, record.Name)
	if pinnedErr == nil {
		state.RegistryPinned = sameRegistryRecord(pinned.Record, record)
	} else if !errors.Is(pinnedErr, hadronregistry.ErrWorkflowNotFound) {
		return WorkflowVersionDetail{}, pinnedErr
	}
	return WorkflowVersionDetail{Descriptor: descriptor, Registry: state, RegisteredAt: record.RegisteredAt.UTC()}, nil
}

func (s *WorkflowLifecycleService) activationDescriptorsForRecord(ctx context.Context, record hadronregistry.WorkflowRecord) ([]WorkflowActivationDescriptor, error) {
	if s.sourceActivations == nil {
		return []WorkflowActivationDescriptor{}, nil
	}
	owner := lifecycleSourceActivationOwner(record.Authority, record.Name)
	ownerKey, err := sourceActivationIdentityDigest(owner)
	if err != nil {
		return nil, err
	}
	registrations, err := s.sourceActivations.Activations.Store.ListDerivedActivations(ctx, ownerKey)
	if err != nil {
		return nil, err
	}
	result := make([]WorkflowActivationDescriptor, 0, min(len(registrations), maximumLifecycleActivations))
	for _, registration := range registrations {
		if registration.Derivation == nil || registration.Derivation.SourceOwnerKey != ownerKey {
			return nil, fmt.Errorf("%w: durable workflow activation owner binding is inconsistent", ErrHostNotReady)
		}
		if registration.Definition.Kind != DefinitionKindRegistry ||
			registration.Definition.ID != record.Name || registration.Definition.Version != record.Version || registration.Definition.Digest != record.Digest {
			continue
		}
		if registration.Definition.Authority != record.Authority || registration.Derivation.SourceDigest != record.Digest ||
			registration.Derivation.PlanDigest != record.PlanDigest {
			return nil, fmt.Errorf("%w: durable workflow activation immutable binding is inconsistent", ErrHostNotReady)
		}
		if len(result) == maximumLifecycleActivations {
			return nil, fmt.Errorf("%w: exact workflow activation catalog exceeds %d descriptors", ErrHostNotReady, maximumLifecycleActivations)
		}
		result = append(result, WorkflowActivationDescriptor{
			TemplateID: registration.Derivation.TemplateID, Kind: string(registration.Source.Kind), RegistrationID: registration.ID,
			Enabled: registration.Enabled, Retired: registration.Derivation.Retired,
			Definition: graph.DefinitionRef{Kind: DefinitionKindRegistry, ID: record.Name, Version: record.Version, Digest: record.Digest},
		})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].TemplateID != result[j].TemplateID {
			return result[i].TemplateID < result[j].TemplateID
		}
		return result[i].RegistrationID < result[j].RegistrationID
	})
	return result, nil
}

func exactLifecycleQuery(ref graph.DefinitionRef) (hadronregistry.WorkflowQuery, error) {
	if ref.Kind != DefinitionKindRegistry || ref.ID == "" || ref.Version == "" || ref.Digest == "" || ref.Authority != "" || ref.Locator != "" || ref.Provenance != nil {
		return hadronregistry.WorkflowQuery{}, ErrWorkflowInvalidRequest
	}
	query := hadronregistry.WorkflowQuery{Name: ref.ID, Version: ref.Version, Digest: ref.Digest}
	parts := strings.Split(ref.ID, "/")
	if len(parts) < 2 || hadronregistry.ValidateWorkflowNamespace(strings.Join(parts[:len(parts)-1], "/")) != nil {
		return hadronregistry.WorkflowQuery{}, ErrWorkflowInvalidRequest
	}
	return query, nil
}

func sameRegistryRecord(left, right hadronregistry.WorkflowRecord) bool {
	return left.Name == right.Name && left.Version == right.Version && left.Digest == right.Digest
}

func agentRequest(draft WorkflowDraft, suite *WorkflowContractSuite, makeCurrent bool) AgentAuthoringRequest {
	return AgentAuthoringRequest{Envelope: append([]byte(nil), draft.Envelope...), ID: draft.ID, Version: draft.Version, Namespace: draft.Namespace, Suite: suite, MakeCurrent: makeCurrent}
}

func draftValidation(result AgentAuthoringResult) WorkflowDraftValidationResult {
	return WorkflowDraftValidationResult{Definition: result.Definition, Plan: result.Plan, Diagnostics: append([]authoring.CompactDiagnostic(nil), result.Diagnostics...)}
}

func safeContractEvidence(report *ContractTestReport) *WorkflowContractEvidence {
	if report == nil {
		return nil
	}
	result := &WorkflowContractEvidence{PlanDigest: report.PlanDigest, SuiteDigest: report.SuiteDigest, TestDigest: report.Digest, Passed: report.Passed, Cases: make([]WorkflowContractCaseEvidence, len(report.Cases))}
	for index, current := range report.Cases {
		result.Cases[index] = WorkflowContractCaseEvidence{Name: current.Name, Passed: current.Passed, OutputDigest: current.OutputDigest, Effects: append(graph.EffectSet(nil), current.Effects...)}
		if current.Failure != nil {
			result.Cases[index].FailureCode = current.Failure.Code
		}
	}
	return result
}

func catalogMatch(detail WorkflowVersionDetail, score int, reasons []string) WorkflowCatalogMatch {
	descriptor := detail.Descriptor
	return WorkflowCatalogMatch{
		Definition: descriptor.Definition, Name: descriptor.Name, Namespace: descriptor.Namespace,
		Description: descriptor.Description, Tags: append([]string(nil), descriptor.Tags...),
		Effects: append(graph.EffectSet(nil), descriptor.Effects...), Evidence: descriptor.Evidence,
		Registry: detail.Registry, Score: score, Reasons: append([]string(nil), reasons...), RecommendedNext: "inspect_exact",
	}
}

func lifecycleSearchTerms(query string) []string {
	stop := map[string]struct{}{"a": {}, "an": {}, "and": {}, "for": {}, "i": {}, "need": {}, "please": {}, "the": {}, "to": {}, "workflow": {}}
	seen := make(map[string]struct{})
	var terms []string
	for _, term := range strings.FieldsFunc(strings.ToLower(query), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '-' && r != '_' }) {
		if len(term) < 2 {
			continue
		}
		if _, ignored := stop[term]; ignored {
			continue
		}
		if _, duplicate := seen[term]; duplicate {
			continue
		}
		seen[term] = struct{}{}
		terms = append(terms, term)
	}
	sort.Strings(terms)
	return terms
}

func rankWorkflow(descriptor WorkflowExposureDescriptor, terms []string) (int, []string) {
	if len(terms) == 0 {
		return 1, []string{"catalog listing"}
	}
	name := strings.ToLower(descriptor.Name)
	description := strings.ToLower(descriptor.Description)
	tags := strings.ToLower(strings.Join(descriptor.Tags, " "))
	score := 0
	seen := make(map[string]struct{})
	for _, term := range terms {
		switch {
		case name == term || strings.HasSuffix(name, "/"+term):
			score += 12
			seen["exact name"] = struct{}{}
		case strings.Contains(name, term):
			score += 8
			seen["name match"] = struct{}{}
		}
		if strings.Contains(tags, term) {
			score += 5
			seen["tag match"] = struct{}{}
		}
		if strings.Contains(description, term) {
			score += 2
			seen["description match"] = struct{}{}
		}
	}
	if score == 0 {
		return 0, nil
	}
	reasons := make([]string, 0, len(seen))
	for reason := range seen {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)
	return score, reasons
}
