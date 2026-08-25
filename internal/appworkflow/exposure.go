package appworkflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/registry"
	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	MaximumExposureSearchResults    = 100
	MaximumExposureQueryBytes       = 1024
	MaximumWorkflowToolNameBytes    = 128
	MaximumWorkflowDescriptionBytes = 2048
	MaximumWorkflowTags             = 32
	MaximumWorkflowTagBytes         = 128
	MaximumPublishedWorkflows       = 256
)

// WorkflowExposureStore is consumed by the application service. Its concrete
// SQLite implementation remains a Hadron composition detail.
type WorkflowExposureStore interface {
	PutExposureProfile(context.Context, hoststate.ExposureProfileRecord, uint64) (hoststate.ExposureProfileSnapshot, error)
	GetExposureProfile(context.Context, string) (hoststate.ExposureProfileSnapshot, error)
	ListExposureProfiles(context.Context, int) ([]hoststate.ExposureProfileSnapshot, error)
	DeleteExposureProfile(context.Context, string, uint64) error
	PutMCPPrincipal(context.Context, hoststate.MCPPrincipalRecord, uint64) (hoststate.MCPPrincipalSnapshot, error)
	GetMCPPrincipal(context.Context, string) (hoststate.MCPPrincipalSnapshot, error)
	ResolveMCPPrincipalDigest(context.Context, string) (hoststate.MCPPrincipalSnapshot, error)
	ListMCPPrincipals(context.Context, int) ([]hoststate.MCPPrincipalSnapshot, error)
	DeleteMCPPrincipal(context.Context, string, uint64) error
}

type WorkflowExposureCatalog interface {
	ResolveWorkflow(context.Context, registry.WorkflowQuery) (registry.WorkflowResolution, error)
	SearchWorkflows(context.Context, string, string) ([]registry.WorkflowRecord, error)
}

type ExposureManagementOperation string

const (
	ExposureManagePutProfile      ExposureManagementOperation = "put_profile"
	ExposureManageDeleteProfile   ExposureManagementOperation = "delete_profile"
	ExposureManagePutPrincipal    ExposureManagementOperation = "put_principal"
	ExposureManageDeletePrincipal ExposureManagementOperation = "delete_principal"
	ExposureManageRead            ExposureManagementOperation = "read"
)

type ExposureManagementAuthorization struct {
	Operation   ExposureManagementOperation
	ProfileID   string
	PrincipalID string
}

// ExposureManagementAuthorizer may only add a management restriction. Local
// record validation and referential rules are always enforced first.
type ExposureManagementAuthorizer interface {
	AuthorizeExposureManagement(context.Context, ExposureManagementAuthorization) error
}

// ExposureSessionAuthorizer receives no raw credential. It may deny a locally
// resolved principal but cannot synthesize one or widen its profile.
type ExposureSessionAuthorizer interface {
	AuthorizeExposureSession(context.Context, WorkflowExposureSession) error
}

type ExposureDefinitionAuthorization struct {
	Session    WorkflowExposureSession
	Definition graph.DefinitionRef
	Effects    graph.EffectSet
	Operation  string
	Display    values.DisplayPolicy
}

// ExposureDefinitionAuthorizer is an additive restriction applied after all
// local exposure decisions.
type ExposureDefinitionAuthorizer interface {
	AuthorizeExposedWorkflow(context.Context, ExposureDefinitionAuthorization) error
}

type ExposurePrivateDisplayAuthorizer interface {
	AuthorizePrivateWorkflowDisplay(context.Context, WorkflowExposureSession) error
}

type WorkflowExposureOptions struct {
	Store          WorkflowExposureStore
	Catalog        WorkflowExposureCatalog
	Definitions    DefinitionProvider
	StepKinds      stepkind.Registry
	Management     ExposureManagementAuthorizer
	Sessions       ExposureSessionAuthorizer
	DefinitionsACL ExposureDefinitionAuthorizer
	PrivateDisplay ExposurePrivateDisplayAuthorizer
}

type WorkflowExposureService struct {
	store          WorkflowExposureStore
	catalog        WorkflowExposureCatalog
	definitions    DefinitionProvider
	kinds          stepkind.Registry
	management     ExposureManagementAuthorizer
	sessions       ExposureSessionAuthorizer
	definitionsACL ExposureDefinitionAuthorizer
	privateDisplay ExposurePrivateDisplayAuthorizer
}

func NewWorkflowExposureService(options WorkflowExposureOptions) (*WorkflowExposureService, error) {
	if nilInterface(options.Store) {
		return nil, fmt.Errorf("%w: workflow exposure store is required", ErrInvalidHost)
	}
	for name, dependency := range map[string]any{
		"catalog": options.Catalog, "definitions": options.Definitions, "step kinds": options.StepKinds,
		"management authorizer": options.Management, "session authorizer": options.Sessions,
		"definition authorizer": options.DefinitionsACL, "private-display authorizer": options.PrivateDisplay,
	} {
		if dependency != nil && nilInterface(dependency) {
			return nil, fmt.Errorf("%w: workflow exposure %s must not be typed nil", ErrInvalidHost, name)
		}
	}
	return &WorkflowExposureService{
		store: options.Store, catalog: options.Catalog, definitions: options.Definitions, kinds: options.StepKinds,
		management: options.Management, sessions: options.Sessions, definitionsACL: options.DefinitionsACL,
		privateDisplay: options.PrivateDisplay,
	}, nil
}

func (s *WorkflowExposureService) PutProfile(ctx context.Context, record hoststate.ExposureProfileRecord, expected uint64) (hoststate.ExposureProfileSnapshot, error) {
	record = canonicalExposureProfile(record)
	if err := record.Validate(); err != nil {
		return hoststate.ExposureProfileSnapshot{}, fmt.Errorf("%w: exposure profile: %w", hoststate.ErrInvalidRecord, err)
	}
	if err := s.authorizeManagement(ctx, ExposureManagementAuthorization{Operation: ExposureManagePutProfile, ProfileID: record.ID}); err != nil {
		return hoststate.ExposureProfileSnapshot{}, err
	}
	return s.store.PutExposureProfile(ctx, record, expected)
}

func (s *WorkflowExposureService) GetProfile(ctx context.Context, id string) (hoststate.ExposureProfileSnapshot, error) {
	if err := s.authorizeManagement(ctx, ExposureManagementAuthorization{Operation: ExposureManageRead, ProfileID: id}); err != nil {
		return hoststate.ExposureProfileSnapshot{}, err
	}
	return s.store.GetExposureProfile(ctx, id)
}

func (s *WorkflowExposureService) ListProfiles(ctx context.Context, limit int) ([]hoststate.ExposureProfileSnapshot, error) {
	if err := s.authorizeManagement(ctx, ExposureManagementAuthorization{Operation: ExposureManageRead}); err != nil {
		return nil, err
	}
	return s.store.ListExposureProfiles(ctx, limit)
}

func (s *WorkflowExposureService) DeleteProfile(ctx context.Context, id string, expected uint64) error {
	if err := s.authorizeManagement(ctx, ExposureManagementAuthorization{Operation: ExposureManageDeleteProfile, ProfileID: id}); err != nil {
		return err
	}
	return s.store.DeleteExposureProfile(ctx, id, expected)
}

type PutMCPPrincipalRequest struct {
	Record             hoststate.MCPPrincipalRecord
	Token              string
	ExpectedGeneration uint64
}

func (s *WorkflowExposureService) PutPrincipal(ctx context.Context, request PutMCPPrincipalRequest) (hoststate.MCPPrincipalSnapshot, error) {
	record := request.Record.Clone()
	if record.CredentialDigest != "" {
		return hoststate.MCPPrincipalSnapshot{}, fmt.Errorf("%w: caller cannot supply a credential digest", hoststate.ErrInvalidRecord)
	}
	if record.Identity.SourceAuthority != "mcp" {
		return hoststate.MCPPrincipalSnapshot{}, fmt.Errorf("%w: MCP principal identity must use MCP source authority", hoststate.ErrInvalidRecord)
	}
	agentNamespace, err := hoststate.AgentNamespaceForPrincipal(record.ID, record.AgentNamespace)
	if err != nil {
		return hoststate.MCPPrincipalSnapshot{}, fmt.Errorf("%w: MCP principal agent namespace is invalid", hoststate.ErrInvalidRecord)
	}
	record.AgentNamespace = agentNamespace
	if record.Identity.Extension == nil {
		record.Identity.Extension = make(map[string]string)
	}
	if assigned := record.Identity.Extension["workflow_exposure_profile"]; assigned != "" && assigned != record.ProfileID {
		return hoststate.MCPPrincipalSnapshot{}, fmt.Errorf("%w: MCP principal exposure binding conflicts with its profile", hoststate.ErrInvalidRecord)
	}
	if record.ProfileID == "" {
		delete(record.Identity.Extension, "workflow_exposure_profile")
	} else {
		record.Identity.Extension["workflow_exposure_profile"] = record.ProfileID
	}
	digest, digestErr := hoststate.DigestMCPToken(request.Token)
	if digestErr != nil {
		return hoststate.MCPPrincipalSnapshot{}, fmt.Errorf("%w: MCP token is invalid", hoststate.ErrInvalidRecord)
	}
	record.CredentialDigest = digest
	if validationErr := record.Validate(); validationErr != nil {
		return hoststate.MCPPrincipalSnapshot{}, fmt.Errorf("%w: MCP principal: %w", hoststate.ErrInvalidRecord, validationErr)
	}
	if authorizationErr := s.authorizeManagement(ctx, ExposureManagementAuthorization{Operation: ExposureManagePutPrincipal, ProfileID: record.ProfileID, PrincipalID: record.ID}); authorizationErr != nil {
		return hoststate.MCPPrincipalSnapshot{}, authorizationErr
	}
	stored, err := s.store.PutMCPPrincipal(ctx, record, request.ExpectedGeneration)
	return stored.Public(), err
}

func (s *WorkflowExposureService) GetPrincipal(ctx context.Context, id string) (hoststate.MCPPrincipalSnapshot, error) {
	if err := s.authorizeManagement(ctx, ExposureManagementAuthorization{Operation: ExposureManageRead, PrincipalID: id}); err != nil {
		return hoststate.MCPPrincipalSnapshot{}, err
	}
	result, err := s.store.GetMCPPrincipal(ctx, id)
	return result.Public(), err
}

func (s *WorkflowExposureService) ListPrincipals(ctx context.Context, limit int) ([]hoststate.MCPPrincipalSnapshot, error) {
	if err := s.authorizeManagement(ctx, ExposureManagementAuthorization{Operation: ExposureManageRead}); err != nil {
		return nil, err
	}
	result, err := s.store.ListMCPPrincipals(ctx, limit)
	for index := range result {
		result[index] = result[index].Public()
	}
	return result, err
}

func (s *WorkflowExposureService) DeletePrincipal(ctx context.Context, id string, expected uint64) error {
	if err := s.authorizeManagement(ctx, ExposureManagementAuthorization{Operation: ExposureManageDeletePrincipal, PrincipalID: id}); err != nil {
		return err
	}
	return s.store.DeleteMCPPrincipal(ctx, id, expected)
}

func (s *WorkflowExposureService) authorizeManagement(ctx context.Context, request ExposureManagementAuthorization) error {
	if ctx == nil {
		return errors.New("workflow exposure context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.management == nil {
		return nil
	}
	if err := s.management.AuthorizeExposureManagement(ctx, request); err != nil {
		return fmt.Errorf("%w: workflow exposure management", ErrPolicyDenied)
	}
	return nil
}

type WorkflowExposureSession struct {
	SessionID         string                          `json:"session_id"`
	Authenticated     bool                            `json:"authenticated"`
	Principal         hoststate.MCPPrincipalSnapshot  `json:"principal,omitempty"`
	Profile           hoststate.ExposureProfileRecord `json:"profile"`
	ProfileGeneration uint64                          `json:"profile_generation"`
	AgentNamespace    string                          `json:"agent_namespace,omitempty"`
}

func (s WorkflowExposureSession) Clone() WorkflowExposureSession {
	s.Principal = s.Principal.Public()
	s.Profile = s.Profile.Clone()
	return s
}

type workflowExposureIdentityKey struct{}

// WithAuthenticatedIdentity binds one already-authenticated, validated caller
// to a context consumed by ContextIdentityProvider. Transport adapters must
// authenticate the caller before using this function; request DTO identity
// fields are never authority.
func WithAuthenticatedIdentity(ctx context.Context, binding hoststate.IdentityBinding) (context.Context, error) {
	if ctx == nil {
		return nil, ErrWorkflowUnauthenticated
	}
	binding = normalizeIdentity(binding)
	if err := binding.Validate(); err != nil {
		return nil, fmt.Errorf("%w: authenticated workflow identity is invalid", ErrWorkflowUnauthenticated)
	}
	return context.WithValue(ctx, workflowExposureIdentityKey{}, binding.Clone()), nil
}

// ResolveSession maps the transient token through the local durable identity
// record. Empty and unknown credentials intentionally receive the same safe
// meta-only session.
func (s *WorkflowExposureService) ResolveSession(ctx context.Context, sessionID, token string) (context.Context, WorkflowExposureSession, error) {
	if ctx == nil || strings.TrimSpace(sessionID) == "" {
		return ctx, WorkflowExposureSession{}, errors.New("workflow exposure session requires context and id")
	}
	base := WorkflowExposureSession{SessionID: sessionID, Profile: defaultExposureProfile()}
	if token == "" {
		return ctx, base, nil
	}
	digest, err := hoststate.DigestMCPToken(token)
	if err != nil {
		return ctx, base, nil //nolint:nilerr // Malformed credentials intentionally collapse to the unknown meta-only session.
	}
	principal, err := s.store.ResolveMCPPrincipalDigest(ctx, digest)
	if err != nil {
		if errors.Is(err, workflowruntime.ErrNotFound) {
			return ctx, base, nil
		}
		return ctx, WorkflowExposureSession{}, err
	}
	profile := defaultExposureProfile()
	var profileGeneration uint64
	if principal.Record.ProfileID != "" {
		snapshot, loadErr := s.store.GetExposureProfile(ctx, principal.Record.ProfileID)
		if loadErr != nil {
			return ctx, WorkflowExposureSession{}, fmt.Errorf("%w: assigned workflow exposure profile is unavailable", ErrPolicyDenied)
		}
		profile, profileGeneration = snapshot.Record, snapshot.Generation
	}
	resolved := WorkflowExposureSession{SessionID: sessionID, Authenticated: true, Principal: principal.Public(), Profile: profile, ProfileGeneration: profileGeneration, AgentNamespace: principal.Record.AgentNamespace}
	if s.sessions != nil {
		if authorizationErr := s.sessions.AuthorizeExposureSession(ctx, resolved.Clone()); authorizationErr != nil {
			return ctx, base, nil //nolint:nilerr // Session denial intentionally does not disclose a durable principal or profile.
		}
	}
	binding := principal.Record.Identity.Clone()
	bound, err := WithAuthenticatedIdentity(ctx, binding)
	if err != nil {
		return ctx, WorkflowExposureSession{}, err
	}
	return bound, resolved.Clone(), nil
}

// ContextIdentityProvider binds only identity facts placed in the context by
// a trusted transport authenticator. Selectors may narrow those facts but
// transported principals, attributes, or authorities can never replace them.
type ContextIdentityProvider struct{}

func (ContextIdentityProvider) BindIdentity(ctx context.Context, request IdentityRequest) (hoststate.IdentityBinding, error) {
	binding, ok := ctx.Value(workflowExposureIdentityKey{}).(hoststate.IdentityBinding)
	if !ok {
		return hoststate.IdentityBinding{}, ErrWorkflowUnauthenticated
	}
	if request.SourceAuthority != "" && request.SourceAuthority != binding.SourceAuthority ||
		request.PrincipalHint != "" && request.PrincipalHint != binding.Principal || len(request.Attributes) != 0 {
		return hoststate.IdentityBinding{}, fmt.Errorf("%w: authenticated workflow identity cannot be overridden", ErrPolicyDenied)
	}
	if request.RunScope != nil && !request.RunScope.Matches(binding.RunScope) {
		return hoststate.IdentityBinding{}, fmt.Errorf("%w: authenticated workflow scope cannot be widened", ErrPolicyDenied)
	}
	if request.ExecutionTarget != nil {
		if binding.ExecutionTarget == nil || !request.ExecutionTarget.Matches(*binding.ExecutionTarget) {
			return hoststate.IdentityBinding{}, fmt.Errorf("%w: authenticated workflow target cannot be widened", ErrPolicyDenied)
		}
	}
	return binding.Clone(), nil
}

// MCPIdentityProvider binds only the identity previously authenticated by
// ResolveSession. Transported principal/attributes can never replace it.
type MCPIdentityProvider struct{}

func (MCPIdentityProvider) BindIdentity(ctx context.Context, request IdentityRequest) (hoststate.IdentityBinding, error) {
	if request.SourceAuthority != "" && request.SourceAuthority != "mcp" {
		return hoststate.IdentityBinding{}, fmt.Errorf("%w: MCP caller identity cannot be overridden", ErrPolicyDenied)
	}
	request.SourceAuthority = "mcp"
	return (ContextIdentityProvider{}).BindIdentity(ctx, request)
}

type WorkflowExposureDescriptor struct {
	ToolName     string                        `json:"tool_name"`
	Name         string                        `json:"name"`
	Namespace    string                        `json:"namespace,omitempty"`
	Version      string                        `json:"version"`
	Digest       string                        `json:"digest"`
	Description  string                        `json:"description,omitempty"`
	Tags         []string                      `json:"tags"`
	Definition   graph.DefinitionRef           `json:"definition"`
	Provenance   WorkflowExposureProvenance    `json:"provenance"`
	InputSchema  graph.Schema                  `json:"input_schema"`
	OutputSchema graph.Schema                  `json:"output_schema"`
	Effects      graph.EffectSet               `json:"effects"`
	Evidence     WorkflowQualificationEvidence `json:"evidence"`
}

// WorkflowQualificationEvidence is the bounded, non-secret proof that an
// immutable workflow passed the canonical qualification boundary. It omits
// source bytes, publisher identity, attestations, and contract mock payloads.
type WorkflowQualificationEvidence struct {
	PlanDigest          string `json:"plan_digest"`
	ContractSuiteDigest string `json:"contract_suite_digest"`
	ContractTestDigest  string `json:"contract_test_digest"`
	TestsPassed         bool   `json:"tests_passed"`
}

// WorkflowExposureProvenance is the intentionally small, non-secret subset of
// registry provenance safe for discovery transports. Source locators, parent
// chains, and arbitrary metadata never cross this boundary.
type WorkflowExposureProvenance struct {
	Authority  string `json:"authority"`
	Origin     string `json:"origin"`
	Revision   string `json:"revision"`
	Digest     string `json:"digest"`
	TrustClass string `json:"trust_class,omitempty"`
}

type WorkflowExposureSummary struct {
	Name       string              `json:"name"`
	Namespace  string              `json:"namespace,omitempty"`
	Version    string              `json:"version"`
	Digest     string              `json:"digest"`
	Effects    graph.EffectSet     `json:"effects"`
	Definition graph.DefinitionRef `json:"definition"`
}

func (s *WorkflowExposureService) DirectWorkflows(ctx context.Context, session WorkflowExposureSession) ([]WorkflowExposureDescriptor, error) {
	if err := s.reauthorize(ctx, session); err != nil {
		return nil, err
	}
	if !session.Authenticated {
		return []WorkflowExposureDescriptor{}, nil
	}
	if err := s.requireGraphDependencies(); err != nil {
		return nil, err
	}
	exact := make(map[string]registry.WorkflowRecord)
	for _, pin := range session.Profile.Pins {
		resolution, err := s.catalog.ResolveWorkflow(ctx, registry.WorkflowQuery{Name: pin.ID, Version: pin.Version, Digest: pin.Digest})
		if err != nil {
			return nil, ErrWorkflowHidden
		}
		exact[resolution.Record.Name] = resolution.Record
	}
	if session.AgentNamespace != "" {
		records, err := s.catalog.SearchWorkflows(ctx, session.AgentNamespace, "")
		if err != nil {
			return nil, err
		}
		names := make(map[string]struct{})
		for _, record := range records {
			names[record.Name] = struct{}{}
		}
		for name := range names {
			if _, pinned := exact[name]; pinned {
				continue
			}
			resolution, resolveErr := s.catalog.ResolveWorkflow(ctx, registry.WorkflowQuery{Name: name})
			if resolveErr != nil {
				continue
			}
			exact[name] = resolution.Record
		}
	}
	names := make([]string, 0, len(exact))
	for name := range exact {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]WorkflowExposureDescriptor, 0, len(names))
	seenTools := make(map[string]string)
	for _, name := range names {
		descriptor, err := s.describeRecord(ctx, session, exact[name], "direct")
		if errors.Is(err, ErrWorkflowHidden) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if prior, collision := seenTools[descriptor.ToolName]; collision {
			return nil, fmt.Errorf("%w: exposed workflows %q and %q collide", hoststate.ErrConflict, prior, name)
		}
		seenTools[descriptor.ToolName] = name
		result = append(result, descriptor)
	}
	if len(result) > session.Profile.MaxDirectTools {
		return nil, fmt.Errorf("%w: direct workflow tools exceed the profile budget", ErrPolicyDenied)
	}
	return cloneExposureDescriptors(result)
}

func (s *WorkflowExposureService) Search(ctx context.Context, session WorkflowExposureSession, query string, limit int) ([]WorkflowExposureSummary, error) {
	if err := s.reauthorize(ctx, session); err != nil {
		return nil, err
	}
	if !session.Authenticated || session.Profile.SearchScope == hoststate.ExposureSearchNone {
		return []WorkflowExposureSummary{}, nil
	}
	if err := s.requireGraphDependencies(); err != nil {
		return nil, err
	}
	query = strings.TrimSpace(query)
	if len(query) > MaximumExposureQueryBytes {
		return nil, errors.New("workflow search query exceeds the supported bound")
	}
	if limit == 0 {
		limit = 20
	}
	if limit < 1 || limit > MaximumExposureSearchResults {
		return nil, errors.New("workflow search limit is invalid")
	}
	namespaces := sessionSearchNamespaces(session.Profile)
	seen := make(map[string]struct{})
	var result []WorkflowExposureSummary
	for _, namespace := range namespaces {
		records, err := s.catalog.SearchWorkflows(ctx, namespace, query)
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			if session.Profile.SearchScope == hoststate.ExposureSearchPublic && !record.Published {
				continue
			}
			key := record.Name + "\x00" + record.Version + "\x00" + record.Digest
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
			descriptor, describeErr := s.describeRecord(ctx, session, record, "search")
			if errors.Is(describeErr, ErrWorkflowHidden) {
				continue
			}
			if describeErr != nil {
				return nil, describeErr
			}
			result = append(result, WorkflowExposureSummary{Name: descriptor.Name, Namespace: descriptor.Namespace, Version: descriptor.Version, Digest: descriptor.Digest, Effects: descriptor.Effects, Definition: descriptor.Definition})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Name == result[j].Name {
			return result[i].Version < result[j].Version
		}
		return result[i].Name < result[j].Name
	})
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (s *WorkflowExposureService) Load(ctx context.Context, session WorkflowExposureSession, refs []graph.DefinitionRef) ([]WorkflowExposureDescriptor, error) {
	if err := s.reauthorize(ctx, session); err != nil {
		return nil, err
	}
	if !session.Authenticated || !session.Profile.LazyLoad || session.Profile.SearchScope == hoststate.ExposureSearchNone {
		return nil, ErrWorkflowHidden
	}
	if len(refs) == 0 || len(refs) > session.Profile.MaxDirectTools {
		return nil, fmt.Errorf("%w: lazy workflow tool request exceeds the profile budget", ErrPolicyDenied)
	}
	if err := s.requireGraphDependencies(); err != nil {
		return nil, err
	}
	result := make([]WorkflowExposureDescriptor, 0, len(refs))
	seen := make(map[string]struct{})
	for _, ref := range refs {
		if ref.Kind != "registry" || ref.ID == "" || ref.Version == "" || ref.Digest == "" {
			return nil, ErrWorkflowHidden
		}
		resolution, err := s.catalog.ResolveWorkflow(ctx, registry.WorkflowQuery{Name: ref.ID, Version: ref.Version, Digest: ref.Digest})
		if err != nil || !searchAllowsRecord(session.Profile, resolution.Record) {
			return nil, ErrWorkflowHidden
		}
		descriptor, err := s.describeRecord(ctx, session, resolution.Record, "load")
		if err != nil {
			return nil, err
		}
		if _, collision := seen[descriptor.ToolName]; collision {
			return nil, fmt.Errorf("%w: lazy workflow tool collision", hoststate.ErrConflict)
		}
		seen[descriptor.ToolName] = struct{}{}
		result = append(result, descriptor)
	}
	return cloneExposureDescriptors(result)
}

// Describe authorizes one exact definition through the same direct or
// discoverable profile rules used by mounts and returns its canonical schema.
func (s *WorkflowExposureService) Describe(ctx context.Context, session WorkflowExposureSession, ref graph.DefinitionRef, operation string) (WorkflowExposureDescriptor, error) {
	if err := s.reauthorize(ctx, session); err != nil {
		return WorkflowExposureDescriptor{}, err
	}
	if !session.Authenticated || ref.Kind != "registry" || ref.ID == "" || ref.Version == "" || ref.Digest == "" {
		return WorkflowExposureDescriptor{}, ErrWorkflowHidden
	}
	if err := s.requireGraphDependencies(); err != nil {
		return WorkflowExposureDescriptor{}, err
	}
	resolution, err := s.catalog.ResolveWorkflow(ctx, registry.WorkflowQuery{Name: ref.ID, Version: ref.Version, Digest: ref.Digest})
	if err != nil || !definitionAllowedBySession(session, resolution.Record) {
		return WorkflowExposureDescriptor{}, ErrWorkflowHidden
	}
	return s.describeRecord(ctx, session, resolution.Record, operation)
}

func (s *WorkflowExposureService) NamespaceCatalog(ctx context.Context, session WorkflowExposureSession) (map[string]int, error) {
	results, err := s.Search(ctx, session, "", MaximumExposureSearchResults)
	if err != nil {
		return nil, err
	}
	catalog := make(map[string]int)
	for _, result := range results {
		catalog[result.Namespace]++
	}
	return catalog, nil
}

// PublishedWorkflows returns transport-neutral, exact descriptors for public
// agent discovery. Publication is necessary but not sufficient: configured
// definition authorization may still hide a record.
func (s *WorkflowExposureService) PublishedWorkflows(ctx context.Context) ([]WorkflowExposureDescriptor, error) {
	if ctx == nil {
		return nil, errors.New("workflow exposure context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.requireGraphDependencies(); err != nil {
		return nil, err
	}
	records, err := s.catalog.SearchWorkflows(ctx, "", "")
	if err != nil {
		return nil, err
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].Name != records[j].Name {
			return records[i].Name < records[j].Name
		}
		if records[i].Version != records[j].Version {
			return records[i].Version < records[j].Version
		}
		return records[i].Digest < records[j].Digest
	})
	published := 0
	for _, record := range records {
		if record.Published {
			published++
		}
	}
	if published > MaximumPublishedWorkflows {
		return nil, fmt.Errorf("%w: published workflow catalog exceeds %d descriptors", ErrHostNotReady, MaximumPublishedWorkflows)
	}
	result := make([]WorkflowExposureDescriptor, 0, len(records))
	for _, record := range records {
		if !record.Published {
			continue
		}
		descriptor, describeErr := s.describeRecord(ctx, WorkflowExposureSession{}, record, "agent_card")
		if errors.Is(describeErr, ErrWorkflowHidden) {
			continue
		}
		if describeErr != nil {
			return nil, describeErr
		}
		result = append(result, descriptor)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Name != result[j].Name {
			return result[i].Name < result[j].Name
		}
		if result[i].Version != result[j].Version {
			return result[i].Version < result[j].Version
		}
		return result[i].Digest < result[j].Digest
	})
	return cloneExposureDescriptors(result)
}

func (s *WorkflowExposureService) DisplayPolicy(ctx context.Context, session WorkflowExposureSession, requested values.DisplayPolicy) (values.DisplayPolicy, error) {
	if err := s.reauthorize(ctx, session); err != nil {
		return values.DisplayPolicy{}, err
	}
	if err := requested.Validate(); err != nil {
		return values.DisplayPolicy{}, err
	}
	if !requested.RevealsPrivate() {
		return values.DisplayPolicy{}, nil
	}
	if !session.Authenticated || !session.Profile.Display.RevealsPrivate() {
		return values.DisplayPolicy{}, ErrPolicyDenied
	}
	if s.privateDisplay != nil {
		if err := s.privateDisplay.AuthorizePrivateWorkflowDisplay(ctx, session.Clone()); err != nil {
			return values.DisplayPolicy{}, ErrPolicyDenied
		}
	}
	return values.DisplayPolicy{Private: values.PrivateDisplayReveal}, nil
}

func (s *WorkflowExposureService) describeRecord(ctx context.Context, session WorkflowExposureSession, record registry.WorkflowRecord, operation string) (WorkflowExposureDescriptor, error) {
	ref := graph.DefinitionRef{Kind: "registry", ID: record.Name, Version: record.Version, Digest: record.Digest}
	plan, err := s.definitions.ResolvePlan(ctx, ref)
	if err != nil {
		return WorkflowExposureDescriptor{}, ErrWorkflowHidden
	}
	if exactErr := exactExposurePlan(record, plan); exactErr != nil {
		return WorkflowExposureDescriptor{}, exactErr
	}
	effects, err := workflowPlanEffects(plan, s.kinds)
	if err != nil {
		return WorkflowExposureDescriptor{}, err
	}
	if effectsDenied(effects, session.Profile.DeniedEffects) {
		return WorkflowExposureDescriptor{}, ErrWorkflowHidden
	}
	if s.definitionsACL != nil {
		request := ExposureDefinitionAuthorization{Session: session.Clone(), Definition: ref, Effects: append(graph.EffectSet(nil), effects...), Operation: operation, Display: values.DisplayPolicy{}}
		if authorizationErr := s.definitionsACL.AuthorizeExposedWorkflow(ctx, request); authorizationErr != nil {
			return WorkflowExposureDescriptor{}, ErrWorkflowHidden
		}
	}
	input, err := workflowIOSchema(plan.Graph.Inputs)
	if err != nil {
		return WorkflowExposureDescriptor{}, err
	}
	output, err := workflowOutputSchema(plan.Graph.Outputs)
	if err != nil {
		return WorkflowExposureDescriptor{}, err
	}
	description, tags, err := workflowDiscoveryMetadata(plan.Graph.Metadata)
	if err != nil {
		return WorkflowExposureDescriptor{}, err
	}
	provenance := WorkflowExposureProvenance{Authority: record.Authority, Origin: record.Provenance.Origin, Revision: record.Version, Digest: record.Digest, TrustClass: record.TrustClass}
	if err := provenance.Validate(); err != nil {
		return WorkflowExposureDescriptor{}, err
	}
	evidence := WorkflowQualificationEvidence{
		PlanDigest: record.PlanDigest, ContractSuiteDigest: record.ContractSuiteDigest,
		ContractTestDigest: record.ContractTestDigest, TestsPassed: record.TestsPassed,
	}
	return WorkflowExposureDescriptor{ToolName: workflowToolName(record.Name), Name: record.Name, Namespace: record.Namespace, Version: record.Version, Digest: record.Digest, Description: description, Tags: tags, Definition: ref, Provenance: provenance, InputSchema: input, OutputSchema: output, Effects: effects, Evidence: evidence}, nil
}

// PinProfileDefinition adds one published immutable workflow to a durable
// exposure profile with an exact generation CAS. Every resulting direct tool
// is resolved and policy-checked before the single profile write.
func (s *WorkflowExposureService) PinProfileDefinition(ctx context.Context, profileID string, ref graph.DefinitionRef, expected uint64) (hoststate.ExposureProfileSnapshot, error) {
	snapshot, err := s.GetProfile(ctx, profileID)
	if err != nil {
		return hoststate.ExposureProfileSnapshot{}, err
	}
	if expected == 0 || snapshot.Generation != expected {
		return hoststate.ExposureProfileSnapshot{}, hoststate.ErrConflict
	}
	if err := validateExactExposureRef(ref); err != nil {
		return hoststate.ExposureProfileSnapshot{}, err
	}
	profile := snapshot.Record.Clone()
	for _, pin := range profile.Pins {
		if pin.ID == ref.ID {
			if pin == ref {
				return snapshot.Clone(), nil
			}
			return hoststate.ExposureProfileSnapshot{}, hoststate.ErrConflict
		}
	}
	profile.Pins = append(profile.Pins, ref)
	profile = canonicalExposureProfile(profile)
	if err := s.preflightProfilePins(ctx, profile); err != nil {
		return hoststate.ExposureProfileSnapshot{}, err
	}
	return s.PutProfile(ctx, profile, expected)
}

// UnpinProfileDefinition removes only the supplied exact workflow from a
// profile. A stale or different digest conflicts; an exact absence replays.
func (s *WorkflowExposureService) UnpinProfileDefinition(ctx context.Context, profileID string, ref graph.DefinitionRef, expected uint64) (hoststate.ExposureProfileSnapshot, error) {
	snapshot, err := s.GetProfile(ctx, profileID)
	if err != nil {
		return hoststate.ExposureProfileSnapshot{}, err
	}
	if expected == 0 || snapshot.Generation != expected {
		return hoststate.ExposureProfileSnapshot{}, hoststate.ErrConflict
	}
	if err := validateExactExposureRef(ref); err != nil {
		return hoststate.ExposureProfileSnapshot{}, err
	}
	profile := snapshot.Record.Clone()
	index := -1
	for current, pin := range profile.Pins {
		if pin.ID == ref.ID {
			if pin != ref {
				return hoststate.ExposureProfileSnapshot{}, hoststate.ErrConflict
			}
			index = current
			break
		}
	}
	if index < 0 {
		return snapshot.Clone(), nil
	}
	profile.Pins = append(profile.Pins[:index:index], profile.Pins[index+1:]...)
	profile = canonicalExposureProfile(profile)
	if err := s.preflightProfilePins(ctx, profile); err != nil {
		return hoststate.ExposureProfileSnapshot{}, err
	}
	return s.PutProfile(ctx, profile, expected)
}

func validateExactExposureRef(ref graph.DefinitionRef) error {
	probe := hoststate.ExposureProfileRecord{ID: "preflight", Pins: []graph.DefinitionRef{ref}, MaxDirectTools: 1, SearchScope: hoststate.ExposureSearchNone}
	if err := probe.Validate(); err != nil {
		return fmt.Errorf("%w: exposure definition is invalid", ErrWorkflowInvalidRequest)
	}
	return nil
}

func (s *WorkflowExposureService) preflightProfilePins(ctx context.Context, profile hoststate.ExposureProfileRecord) error {
	if err := s.requireGraphDependencies(); err != nil {
		return err
	}
	if err := profile.Validate(); err != nil {
		return fmt.Errorf("%w: exposure profile is invalid", ErrWorkflowInvalidRequest)
	}
	if len(profile.Pins) > profile.MaxDirectTools {
		return fmt.Errorf("%w: direct workflow tools exceed the profile budget", ErrPolicyDenied)
	}
	session := WorkflowExposureSession{Authenticated: true, Profile: profile.Clone()}
	seen := make(map[string]string, len(profile.Pins))
	for _, pin := range profile.Pins {
		resolution, err := s.catalog.ResolveWorkflow(ctx, registry.WorkflowQuery{Name: pin.ID, Version: pin.Version, Digest: pin.Digest})
		if err != nil || !resolution.Record.Published {
			return ErrWorkflowHidden
		}
		descriptor, err := s.describeRecord(ctx, session, resolution.Record, "profile_pin")
		if err != nil {
			return err
		}
		if prior, exists := seen[descriptor.ToolName]; exists && prior != descriptor.Name {
			return fmt.Errorf("%w: exposed workflows %q and %q collide", hoststate.ErrConflict, prior, descriptor.Name)
		}
		seen[descriptor.ToolName] = descriptor.Name
	}
	return nil
}

func (p WorkflowExposureProvenance) Validate() error {
	for _, field := range []struct {
		value    string
		required bool
	}{
		{p.Authority, true}, {p.Origin, true}, {p.Revision, true}, {p.TrustClass, false},
	} {
		if err := hoststate.ValidatePublicText(field.value, 512, field.required); err != nil {
			return errors.New("workflow discovery provenance is invalid")
		}
	}
	if err := values.ValidateDigest(p.Digest); err != nil {
		return fmt.Errorf("workflow discovery provenance digest: %w", err)
	}
	return nil
}

func workflowDiscoveryMetadata(metadata graph.Metadata) (string, []string, error) {
	var description string
	if raw, exists := metadata["description"]; exists {
		var ok bool
		description, ok = raw.(string)
		if !ok || hoststate.ValidatePublicText(description, MaximumWorkflowDescriptionBytes, false) != nil {
			return "", nil, errors.New("workflow discovery description is invalid")
		}
	}
	var tags []string
	if raw, exists := metadata["tags"]; exists {
		switch typed := raw.(type) {
		case []string:
			tags = append(tags, typed...)
		case []any:
			for _, item := range typed {
				value, ok := item.(string)
				if !ok {
					return "", nil, errors.New("workflow discovery tags are invalid")
				}
				tags = append(tags, value)
			}
		default:
			return "", nil, errors.New("workflow discovery tags are invalid")
		}
	}
	if len(tags) > MaximumWorkflowTags {
		return "", nil, errors.New("workflow discovery tags exceed the supported bound")
	}
	sort.Strings(tags)
	for index, tag := range tags {
		if hoststate.ValidatePublicText(tag, MaximumWorkflowTagBytes, true) != nil || index > 0 && tag == tags[index-1] {
			return "", nil, errors.New("workflow discovery tags are invalid")
		}
	}
	if tags == nil {
		tags = []string{}
	}
	return description, tags, nil
}

func (s *WorkflowExposureService) reauthorize(ctx context.Context, session WorkflowExposureSession) error {
	if ctx == nil {
		return errors.New("workflow exposure context is required")
	}
	if !session.Authenticated {
		return nil
	}
	current, err := s.store.GetMCPPrincipal(ctx, session.Principal.Record.ID)
	if err != nil || current.Generation != session.Principal.Generation || !reflect.DeepEqual(current.Public(), session.Principal.Public()) {
		return fmt.Errorf("%w: MCP principal changed", ErrPolicyDenied)
	}
	if current.Record.ProfileID != session.Principal.Record.ProfileID || current.Record.AgentNamespace != session.AgentNamespace {
		return fmt.Errorf("%w: MCP exposure assignment changed", ErrPolicyDenied)
	}
	if current.Record.ProfileID != "" {
		profile, profileErr := s.store.GetExposureProfile(ctx, current.Record.ProfileID)
		if profileErr != nil || profile.Generation != session.ProfileGeneration || !reflect.DeepEqual(profile.Record, session.Profile) {
			return fmt.Errorf("%w: MCP exposure profile changed", ErrPolicyDenied)
		}
	} else if session.ProfileGeneration != 0 || !reflect.DeepEqual(session.Profile, defaultExposureProfile()) {
		return fmt.Errorf("%w: MCP default exposure profile changed", ErrPolicyDenied)
	}
	if s.sessions != nil {
		if err := s.sessions.AuthorizeExposureSession(ctx, session.Clone()); err != nil {
			return ErrPolicyDenied
		}
	}
	return nil
}

func (s *WorkflowExposureService) requireGraphDependencies() error {
	if nilInterface(s.catalog) || nilInterface(s.definitions) || nilInterface(s.kinds) {
		return fmt.Errorf("%w: graph workflow exposure is not composed", ErrHostNotReady)
	}
	return nil
}

func canonicalExposureProfile(record hoststate.ExposureProfileRecord) hoststate.ExposureProfileRecord {
	record = record.Clone()
	sort.Strings(record.Namespaces)
	record.Namespaces = uniqueExposureStrings(record.Namespaces)
	sort.Slice(record.Pins, func(i, j int) bool { return record.Pins[i].ID < record.Pins[j].ID })
	sort.Slice(record.DeniedEffects, func(i, j int) bool { return record.DeniedEffects[i] < record.DeniedEffects[j] })
	return record
}

func defaultExposureProfile() hoststate.ExposureProfileRecord {
	return hoststate.ExposureProfileRecord{ID: "default:meta-only", MaxDirectTools: hoststate.DefaultMaximumDirectTools, SearchScope: hoststate.ExposureSearchNone}
}

func uniqueExposureStrings(input []string) []string {
	if len(input) < 2 {
		return input
	}
	result := input[:1]
	for _, value := range input[1:] {
		if value != result[len(result)-1] {
			result = append(result, value)
		}
	}
	return result
}

func sessionSearchNamespaces(profile hoststate.ExposureProfileRecord) []string {
	if profile.SearchScope == hoststate.ExposureSearchNamespaces {
		return append([]string(nil), profile.Namespaces...)
	}
	return []string{""}
}

func searchAllowsRecord(profile hoststate.ExposureProfileRecord, record registry.WorkflowRecord) bool {
	switch profile.SearchScope {
	case hoststate.ExposureSearchPublic:
		return record.Published
	case hoststate.ExposureSearchAll:
		return true
	case hoststate.ExposureSearchNamespaces:
		index := sort.SearchStrings(profile.Namespaces, record.Namespace)
		return index < len(profile.Namespaces) && profile.Namespaces[index] == record.Namespace
	default:
		return false
	}
}

func definitionAllowedBySession(session WorkflowExposureSession, record registry.WorkflowRecord) bool {
	for _, pin := range session.Profile.Pins {
		if pin.ID == record.Name && pin.Version == record.Version && pin.Digest == record.Digest {
			return true
		}
	}
	if session.AgentNamespace != "" && record.Namespace == session.AgentNamespace {
		return true
	}
	return searchAllowsRecord(session.Profile, record)
}

func exactExposurePlan(record registry.WorkflowRecord, plan *compile.ExecutionPlan) error {
	sourceID := record.SourceDefinitionID()
	expectedName := sourceID
	if record.Namespace != "" {
		expectedName = record.Namespace + "/" + sourceID
	}
	if plan == nil || graph.ValidateID(sourceID) != nil || record.Name != expectedName ||
		plan.ID != sourceID || plan.Definition.Kind != "workflow" || plan.Definition.ID != sourceID ||
		plan.Definition.Version != record.Version || plan.Definition.Digest != record.Digest ||
		plan.Definition.Authority != record.Authority || plan.Definition.Locator != record.Provenance.Locator ||
		plan.Definition.Provenance == nil || plan.Graph.ID != sourceID || plan.Graph.Namespace != record.Namespace ||
		plan.Graph.Version != record.Version || len(plan.SourceDigests) != 1 ||
		plan.SourceDigests[0].Format != record.SourceFormat || plan.SourceDigests[0].Digest != record.Digest ||
		record.PlanDigest != "" && plan.Digest != record.PlanDigest {
		return errors.New("resolved workflow does not match its immutable catalog definition")
	}
	expectedProvenance := record.Provenance
	if expectedProvenance.Authority == "" {
		expectedProvenance.Authority = record.Authority
	}
	expectedProvenance.Metadata = make(graph.Metadata, len(record.Provenance.Metadata)+1)
	for key, value := range record.Provenance.Metadata {
		expectedProvenance.Metadata[key] = value
	}
	if record.TrustClass != "" {
		expectedProvenance.Metadata["trust_class"] = record.TrustClass
	}
	if !reflect.DeepEqual(*plan.Definition.Provenance, expectedProvenance) ||
		!reflect.DeepEqual(plan.Provenance, expectedProvenance) ||
		!reflect.DeepEqual(plan.Graph.Provenance, expectedProvenance) {
		return errors.New("resolved workflow does not match its immutable catalog definition")
	}
	return nil
}

func workflowPlanEffects(plan *compile.ExecutionPlan, kinds stepkind.Registry) (graph.EffectSet, error) {
	set := make(map[graph.Effect]struct{})
	for _, node := range plan.Graph.Nodes {
		_, spec, err := stepkind.Resolve(kinds, node.Kind, node.KindVersion)
		if err != nil {
			return nil, err
		}
		for _, effect := range spec.Effects {
			set[effect] = struct{}{}
		}
		for _, effect := range node.Effects {
			set[effect] = struct{}{}
		}
	}
	result := make(graph.EffectSet, 0, len(set))
	for effect := range set {
		result = append(result, effect)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result, nil
}

func effectsDenied(effects, denied graph.EffectSet) bool {
	for _, effect := range effects {
		for _, forbidden := range denied {
			if effect == forbidden {
				return true
			}
		}
	}
	return false
}

func workflowIOSchema(inputs []graph.InputSpec) (graph.Schema, error) {
	properties := make(map[string]any, len(inputs))
	required := make([]string, 0, len(inputs))
	for _, input := range inputs {
		cloned, err := cloneExposureSchema(input.Schema)
		if err != nil {
			return nil, err
		}
		if input.Description != "" {
			cloned["description"] = input.Description
		}
		if input.Default != nil {
			if err := values.ValidateLiteralBindingSchema(input.Default, input.Schema); err != nil {
				return nil, fmt.Errorf("workflow input %q default: %w", input.Name, err)
			}
			literal, err := cloneExposureJSONValue(input.Default.Literal)
			if err != nil {
				return nil, fmt.Errorf("workflow input %q default: %w", input.Name, err)
			}
			cloned["default"] = literal
		}
		properties[input.Name] = map[string]any(cloned)
		if input.Required && input.Default == nil {
			required = append(required, input.Name)
		}
	}
	result := graph.Schema{"type": "object", "additionalProperties": false, "properties": properties}
	if len(required) != 0 {
		result["required"] = required
	}
	return result, values.ValidateSchema(result)
}

func workflowOutputSchema(outputs []graph.OutputSpec) (graph.Schema, error) {
	properties := make(map[string]any, len(outputs))
	required := make([]string, 0, len(outputs))
	for _, output := range outputs {
		cloned, err := cloneExposureSchema(output.Schema)
		if err != nil {
			return nil, err
		}
		if output.Description != "" {
			cloned["description"] = output.Description
		}
		properties[output.Name] = map[string]any(cloned)
		required = append(required, output.Name)
	}
	result := graph.Schema{"type": "object", "additionalProperties": false, "properties": properties}
	if len(required) != 0 {
		result["required"] = required
	}
	return result, values.ValidateSchema(result)
}

func cloneExposureJSONValue(input any) (any, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var result any
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("workflow value has trailing data")
	}
	return result, nil
}

func cloneExposureSchema(input graph.Schema) (graph.Schema, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var result graph.Schema
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("workflow schema has trailing data")
	}
	return result, nil
}

func workflowToolName(name string) string {
	var result strings.Builder
	result.WriteString("workflow_")
	for _, current := range name {
		if result.Len() >= MaximumWorkflowToolNameBytes {
			break
		}
		if current >= 'A' && current <= 'Z' || current >= 'a' && current <= 'z' || current >= '0' && current <= '9' || current == '_' || current == '-' {
			result.WriteRune(current)
		} else {
			result.WriteByte('_')
		}
	}
	return result.String()
}

func cloneExposureDescriptors(input []WorkflowExposureDescriptor) ([]WorkflowExposureDescriptor, error) {
	result := make([]WorkflowExposureDescriptor, len(input))
	for index, descriptor := range input {
		inputSchema, err := cloneExposureSchema(descriptor.InputSchema)
		if err != nil {
			return nil, err
		}
		outputSchema, err := cloneExposureSchema(descriptor.OutputSchema)
		if err != nil {
			return nil, err
		}
		descriptor.InputSchema, descriptor.OutputSchema = inputSchema, outputSchema
		descriptor.Effects = append(graph.EffectSet(nil), descriptor.Effects...)
		descriptor.Tags = append([]string(nil), descriptor.Tags...)
		result[index] = descriptor
	}
	return result, nil
}
