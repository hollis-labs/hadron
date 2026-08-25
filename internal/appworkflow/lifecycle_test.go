package appworkflow

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	hadronregistry "github.com/hollis-labs/hadron/internal/registry"
	"github.com/hollis-labs/hadron/workflow/adapters/transform"
	"github.com/hollis-labs/hadron/workflow/authoring"
	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const lifecyclePrincipal = "principal:lifecycle-bound"

type lifecycleIdentityProvider struct{}

func (lifecycleIdentityProvider) BindIdentity(_ context.Context, _ IdentityRequest) (hoststate.IdentityBinding, error) {
	return hoststate.IdentityBinding{
		Principal: lifecyclePrincipal, SourceAuthority: "test", Trust: "trusted",
		RunScope: hoststate.RunScope{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeUser, ID: "lifecycle-user"},
	}, nil
}

type lifecycleFixture struct {
	service   *WorkflowLifecycleService
	contracts *ContractRegistrationService
	catalog   *hadronregistry.WorkflowIndex
	resolver  *DefinitionResolver
	kinds     *stepkind.MemoryRegistry
	exposure  *WorkflowExposureService
	store     *lifecycleExposureStore
}

func newLifecycleFixture(t *testing.T, authorize func(NamespaceAuthorization) error) *lifecycleFixture {
	t.Helper()
	stager := NewAuthoringSourceStager()
	catalog := hadronregistry.NewWorkflowIndex()
	kinds := stepkind.NewRegistry()
	if err := kinds.Register(transform.New()); err != nil {
		t.Fatal(err)
	}
	resolver, err := NewDefinitionResolver(DefinitionResolverOptions{
		Roots: []string{t.TempDir()}, Authoring: stager, Registry: catalog,
		Authorizer: DefinitionAuthorizerFunc(func(context.Context, DefinitionAuthorization) error { return nil }),
		Compile:    DefinitionCompileOptions{StepKinds: kinds, SemanticRevision: "lifecycle-test-v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	nowCalls := 0
	contracts, err := NewContractRegistrationService(ContractRegistrationOptions{
		Definitions: resolver, StepKinds: kinds, Catalog: catalog,
		Authorizer: NamespaceAuthorizerFunc(func(_ context.Context, request NamespaceAuthorization) error {
			if authorize != nil {
				return authorize(request)
			}
			return nil
		}),
		Attestor: testContractAttestor{}, Policy: ContractTestPolicy{MinimumCases: 1, Repetitions: 1},
		Now: func() time.Time {
			nowCalls++
			return time.Date(2026, time.August, 24, 20, 0, nowCalls, 0, time.UTC)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	agent, err := NewAgentAuthoringService(AgentAuthoringOptions{
		Stager: stager, Contracts: contracts,
		HostIdentity: AgentAuthoringHostIdentity{Authority: "host:lifecycle", TrustClass: "reviewed", Principal: "configured-but-not-used"},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := newLifecycleExposureStore()
	exposure, err := NewWorkflowExposureService(WorkflowExposureOptions{Store: store, Catalog: catalog, Definitions: resolver, StepKinds: kinds})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewWorkflowLifecycleService(WorkflowLifecycleOptions{Identity: lifecycleIdentityProvider{}, Contracts: contracts, Authoring: agent, Exposure: exposure})
	if err != nil {
		t.Fatal(err)
	}
	return &lifecycleFixture{service: service, contracts: contracts, catalog: catalog, resolver: resolver, kinds: kinds, exposure: exposure, store: store}
}

func TestWorkflowLifecycleFlywheelReportsIndependentStateAndTrustedIdentity(t *testing.T) {
	var authorizations []NamespaceAuthorization
	fixture := newLifecycleFixture(t, func(request NamespaceAuthorization) error {
		authorizations = append(authorizations, request)
		if request.Principal != lifecyclePrincipal {
			return ErrPolicyDenied
		}
		return nil
	})
	identity := IdentityRequest{SourceAuthority: "transport", PrincipalHint: "principal:forged", Attributes: map[string]string{"principal": "forged"}}
	empty, err := fixture.service.SearchWorkflowCatalog(t.Context(), SearchWorkflowCatalogRequest{Namespace: "team", Query: "agent demo", Identity: identity})
	if err != nil || len(empty.Matches) != 0 || empty.NextStep != "draft_validate" {
		t.Fatalf("empty search = %#v, %v", empty, err)
	}
	draft := workflowLifecycleDraft(t, "agent-demo", "team")
	validated, err := fixture.service.ValidateWorkflowDraft(t.Context(), ValidateWorkflowDraftRequest{Draft: draft, Identity: identity})
	if err != nil || validated.Plan == nil || len(validated.Diagnostics) != 0 {
		t.Fatalf("validate = %#v, %v", validated, err)
	}
	scaffold, err := fixture.service.GenerateWorkflowContract(t.Context(), GenerateWorkflowContractRequest{Draft: draft, Identity: identity})
	if err != nil || scaffold.Scaffold == nil || scaffold.Validation.Plan == nil {
		t.Fatalf("scaffold = %#v, %v", scaffold, err)
	}
	suite := completedAgentSuite(t, *scaffold.Scaffold, validated.Plan.ID)
	tested, err := fixture.service.TestWorkflowDraft(t.Context(), TestWorkflowDraftRequest{Draft: draft, Suite: *suite, Identity: identity})
	if err != nil || tested.Evidence == nil || !tested.Evidence.Passed {
		t.Fatalf("test = %#v, %v", tested, err)
	}
	if records, searchErr := fixture.catalog.SearchWorkflows(t.Context(), "team", ""); searchErr != nil || len(records) != 0 {
		t.Fatalf("test mutated catalog = %#v, %v", records, searchErr)
	}
	registered, err := fixture.service.RegisterWorkflowDraft(t.Context(), RegisterWorkflowDraftRequest{Draft: draft, Suite: *suite, MakeCurrent: true, Identity: identity})
	if err != nil || registered.Detail == nil || registered.Evidence == nil || !registered.Evidence.Passed {
		t.Fatalf("register = %#v, %v", registered, err)
	}
	detail := *registered.Detail
	if !detail.Registry.Current || detail.Registry.RegistryPinned || detail.Registry.Published {
		t.Fatalf("initial registry state = %#v", detail.Registry)
	}
	ref := detail.Descriptor.Definition
	detail, err = fixture.service.PinRegistryVersion(t.Context(), MutateWorkflowVersionRequest{Definition: ref, Identity: identity})
	if err != nil || !detail.Registry.Current || !detail.Registry.RegistryPinned || detail.Registry.Published {
		t.Fatalf("registry pin state = %#v, %v", detail.Registry, err)
	}
	detail, err = fixture.service.PublishWorkflowVersion(t.Context(), MutateWorkflowVersionRequest{Definition: ref, Identity: identity})
	if err != nil || !detail.Registry.Current || !detail.Registry.RegistryPinned || !detail.Registry.Published {
		t.Fatalf("published state = %#v, %v", detail.Registry, err)
	}
	profile := fixture.store.createProfile(t, hoststate.ExposureProfileRecord{ID: "agents", MaxDirectTools: 2, SearchScope: hoststate.ExposureSearchPublic})
	pinnedProfile, err := fixture.service.PinWorkflowExposure(t.Context(), MutateWorkflowExposureRequest{ProfileID: "agents", Definition: ref, ExpectedGeneration: profile.Generation, Identity: identity})
	if err != nil || len(pinnedProfile.Record.Pins) != 1 {
		t.Fatalf("exposure pin = %#v, %v", pinnedProfile, err)
	}
	inspected, err := fixture.service.InspectWorkflowVersion(t.Context(), InspectWorkflowVersionRequest{Definition: ref, Identity: identity})
	if err != nil || inspected.Registry != detail.Registry {
		t.Fatalf("profile pin altered registry state = %#v, %v", inspected.Registry, err)
	}
	unpinnedProfile, err := fixture.service.UnpinWorkflowExposure(t.Context(), MutateWorkflowExposureRequest{ProfileID: "agents", Definition: ref, ExpectedGeneration: pinnedProfile.Generation, Identity: identity})
	if err != nil || len(unpinnedProfile.Record.Pins) != 0 {
		t.Fatalf("exposure unpin = %#v, %v", unpinnedProfile, err)
	}
	search, err := fixture.service.SearchWorkflowCatalog(t.Context(), SearchWorkflowCatalogRequest{Namespace: "team", Query: "demo", Identity: identity})
	if err != nil || len(search.Matches) != 1 || search.Matches[0].Evidence.ContractTestDigest != detail.Descriptor.Evidence.ContractTestDigest || search.NextStep != "inspect_exact" {
		t.Fatalf("qualified search = %#v, %v", search, err)
	}
	for _, authorization := range authorizations {
		if authorization.Principal != lifecyclePrincipal {
			t.Fatalf("transported identity reached namespace policy: %#v", authorization)
		}
	}
}

func TestWorkflowExposurePinPreflightFailuresDoNotMutateProfile(t *testing.T) {
	fixture := newLifecycleFixture(t, nil)
	identity := IdentityRequest{SourceAuthority: "test"}
	first := registerLifecycleWorkflow(t, fixture, workflowLifecycleDraft(t, "b", "team/a"), identity, false)
	second := registerLifecycleWorkflow(t, fixture, workflowLifecycleDraft(t, "b", "team_a"), identity, false)
	third := registerLifecycleWorkflow(t, fixture, workflowLifecycleDraft(t, "c", "team"), identity, false)
	profile := fixture.store.createProfile(t, hoststate.ExposureProfileRecord{ID: "strict", DeniedEffects: graph.EffectSet{graph.EffectCompute}, MaxDirectTools: 2, SearchScope: hoststate.ExposureSearchNone})
	original := profile.Clone()
	for name, request := range map[string]MutateWorkflowExposureRequest{
		"stale generation": {ProfileID: "strict", Definition: first, ExpectedGeneration: profile.Generation + 1, Identity: identity},
		"unpublished":      {ProfileID: "strict", Definition: first, ExpectedGeneration: profile.Generation, Identity: identity},
		"non-exact fields": {ProfileID: "strict", Definition: graph.DefinitionRef{Kind: first.Kind, ID: first.ID, Version: first.Version, Digest: first.Digest, Authority: "caller:forged"}, ExpectedGeneration: profile.Generation, Identity: identity},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := fixture.service.PinWorkflowExposure(t.Context(), request); err == nil {
				t.Fatal("pin unexpectedly succeeded")
			}
			assertLifecycleProfileUnchanged(t, fixture.store, original)
		})
	}
	if _, err := fixture.service.PinRegistryVersion(t.Context(), MutateWorkflowVersionRequest{Definition: first, Identity: identity}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.service.PublishWorkflowVersion(t.Context(), MutateWorkflowVersionRequest{Definition: first, Identity: identity}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.service.PinWorkflowExposure(t.Context(), MutateWorkflowExposureRequest{ProfileID: "strict", Definition: first, ExpectedGeneration: profile.Generation, Identity: identity}); err == nil {
		t.Fatal("denied effect entered profile")
	}
	assertLifecycleProfileUnchanged(t, fixture.store, original)
	aclProfile := fixture.store.createProfile(t, hoststate.ExposureProfileRecord{ID: "acl-denied", MaxDirectTools: 2, SearchScope: hoststate.ExposureSearchNone})
	deniedExposure, err := NewWorkflowExposureService(WorkflowExposureOptions{Store: fixture.store, Catalog: fixture.catalog, Definitions: fixture.resolver, StepKinds: fixture.kinds, DefinitionsACL: lifecycleDenyDefinition{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, pinErr := deniedExposure.PinProfileDefinition(t.Context(), aclProfile.Record.ID, first, aclProfile.Generation); pinErr == nil {
		t.Fatal("definition policy denial entered profile")
	}
	assertLifecycleProfileUnchanged(t, fixture.store, aclProfile)

	allowed := original.Record.Clone()
	allowed.DeniedEffects = nil
	profile, _ = fixture.exposure.PutProfile(t.Context(), allowed, profile.Generation)
	for _, ref := range []graph.DefinitionRef{second, third} {
		if _, pinErr := fixture.service.PinRegistryVersion(t.Context(), MutateWorkflowVersionRequest{Definition: ref, Identity: identity}); pinErr != nil {
			t.Fatal(pinErr)
		}
		if _, publishErr := fixture.service.PublishWorkflowVersion(t.Context(), MutateWorkflowVersionRequest{Definition: ref, Identity: identity}); publishErr != nil {
			t.Fatal(publishErr)
		}
	}
	pinned, err := fixture.service.PinWorkflowExposure(t.Context(), MutateWorkflowExposureRequest{ProfileID: "strict", Definition: first, ExpectedGeneration: profile.Generation, Identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	beforeCollision := pinned.Clone()
	if _, pinErr := fixture.service.PinWorkflowExposure(t.Context(), MutateWorkflowExposureRequest{ProfileID: "strict", Definition: second, ExpectedGeneration: pinned.Generation, Identity: identity}); pinErr == nil {
		t.Fatal("colliding tool entered profile")
	}
	assertLifecycleProfileUnchanged(t, fixture.store, beforeCollision)

	budget := fixture.store.createProfile(t, hoststate.ExposureProfileRecord{ID: "budget", MaxDirectTools: 1, SearchScope: hoststate.ExposureSearchNone})
	budgetPinned, err := fixture.service.PinWorkflowExposure(t.Context(), MutateWorkflowExposureRequest{ProfileID: "budget", Definition: first, ExpectedGeneration: budget.Generation, Identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	beforeBudget := budgetPinned.Clone()
	if _, pinErr := fixture.service.PinWorkflowExposure(t.Context(), MutateWorkflowExposureRequest{ProfileID: "budget", Definition: third, ExpectedGeneration: budgetPinned.Generation, Identity: identity}); pinErr == nil {
		t.Fatal("over-budget tool entered profile")
	}
	assertLifecycleProfileUnchanged(t, fixture.store, beforeBudget)
}

func TestWorkflowLifecycleExactRegistryMutationsConflictAfterStateMoves(t *testing.T) {
	fixture := newLifecycleFixture(t, nil)
	identity := IdentityRequest{SourceAuthority: "test"}
	first := registerLifecycleWorkflow(t, fixture, workflowLifecycleDraftVersion(t, "move", "v1", "team"), identity, true)
	second := registerLifecycleWorkflow(t, fixture, workflowLifecycleDraftVersion(t, "move", "v2", "team"), identity, true)
	if _, err := fixture.service.PinRegistryVersion(t.Context(), MutateWorkflowVersionRequest{Definition: first, Identity: identity}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.service.PinRegistryVersion(t.Context(), MutateWorkflowVersionRequest{Definition: second, Identity: identity}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.service.UnpinRegistryVersion(t.Context(), MutateWorkflowVersionRequest{Definition: first, Identity: identity}); err == nil {
		t.Fatal("stale exact registry unpin unexpectedly succeeded")
	}
	pinned, err := fixture.catalog.ResolvePinnedWorkflow(t.Context(), first.ID)
	if err != nil || pinned.Record.Version != second.Version || pinned.Record.Digest != second.Digest {
		t.Fatalf("registry pin changed = %#v, %v", pinned, err)
	}
	if _, clearErr := fixture.service.ClearWorkflowCurrentExact(t.Context(), MutateWorkflowVersionRequest{Definition: first, Identity: identity}); clearErr == nil {
		t.Fatal("stale exact current clear unexpectedly succeeded")
	}
	current, err := fixture.catalog.ResolveWorkflow(t.Context(), hadronregistry.WorkflowQuery{Name: first.ID})
	if err != nil || current.Record.Version != second.Version || current.Record.Digest != second.Digest {
		t.Fatalf("current alias changed = %#v, %v", current, err)
	}
}

func TestWorkflowLifecyclePackageAuthorizesBeforeContractExecution(t *testing.T) {
	denyPackage := false
	fixture := newLifecycleFixture(t, func(request NamespaceAuthorization) error {
		if denyPackage && request.Operation == NamespacePackage {
			return ErrNamespaceUnauthorized
		}
		return nil
	})
	identity := IdentityRequest{SourceAuthority: "test"}
	draft := workflowLifecycleDraft(t, "package-auth", "team")
	scaffold, err := fixture.service.GenerateWorkflowContract(t.Context(), GenerateWorkflowContractRequest{Draft: draft, Identity: identity})
	if err != nil || scaffold.Scaffold == nil || scaffold.Validation.Plan == nil {
		t.Fatalf("scaffold = %#v, %v", scaffold, err)
	}
	suite := completedAgentSuite(t, *scaffold.Scaffold, scaffold.Validation.Plan.ID)
	registered, err := fixture.service.RegisterWorkflowDraft(t.Context(), RegisterWorkflowDraftRequest{Draft: draft, Suite: *suite, Identity: identity})
	if err != nil || registered.Detail == nil {
		t.Fatalf("register = %#v, %v", registered, err)
	}
	runner := &lifecycleContractRunnerSpy{}
	fixture.contracts.runner = runner
	denyPackage = true
	if _, err := fixture.service.PackageWorkflowVersion(t.Context(), PackageWorkflowVersionRequest{Definition: registered.Detail.Descriptor.Definition, Suite: *suite, Identity: identity}); !errors.Is(err, ErrNamespaceUnauthorized) {
		t.Fatalf("denied package error = %v", err)
	}
	if runner.calls != 0 {
		t.Fatalf("denied package executed contract runner %d times", runner.calls)
	}
}

func registerLifecycleWorkflow(t *testing.T, fixture *lifecycleFixture, draft WorkflowDraft, identity IdentityRequest, current bool) graph.DefinitionRef {
	t.Helper()
	scaffold, err := fixture.service.GenerateWorkflowContract(t.Context(), GenerateWorkflowContractRequest{Draft: draft, Identity: identity})
	if err != nil || scaffold.Scaffold == nil || scaffold.Validation.Plan == nil {
		t.Fatalf("scaffold %s = %#v, %v", draft.ID, scaffold, err)
	}
	suite := completedAgentSuite(t, *scaffold.Scaffold, scaffold.Validation.Plan.ID)
	registered, err := fixture.service.RegisterWorkflowDraft(t.Context(), RegisterWorkflowDraftRequest{Draft: draft, Suite: *suite, MakeCurrent: current, Identity: identity})
	if err != nil || registered.Detail == nil {
		t.Fatalf("register %s = %#v, %v", draft.ID, registered, err)
	}
	return registered.Detail.Descriptor.Definition
}

func workflowLifecycleDraft(t *testing.T, id, namespace string) WorkflowDraft {
	return workflowLifecycleDraftVersion(t, id, "v1", namespace)
}

func workflowLifecycleDraftVersion(t *testing.T, id, version, namespace string) WorkflowDraft {
	t.Helper()
	var envelope authoring.Envelope
	if err := json.Unmarshal(agentGraphEnvelope(t), &envelope); err != nil {
		t.Fatal(err)
	}
	envelope.Graph.ID = id
	envelope.Graph.Version = version
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return WorkflowDraft{Envelope: encoded, ID: id, Version: version, Namespace: namespace}
}

func assertLifecycleProfileUnchanged(t *testing.T, store *lifecycleExposureStore, want hoststate.ExposureProfileSnapshot) {
	t.Helper()
	got, err := store.GetExposureProfile(t.Context(), want.Record.ID)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("profile changed = %#v, %v; want %#v", got, err, want)
	}
}

type lifecycleExposureStore struct {
	mu       sync.Mutex
	profiles map[string]hoststate.ExposureProfileSnapshot
}

type lifecycleContractRunnerSpy struct{ calls int }

func (r *lifecycleContractRunnerSpy) Execute(context.Context, *compile.ExecutionPlan, stepkind.Registry, WorkflowContractSuite, int) (ContractTestReport, error) {
	r.calls++
	return ContractTestReport{}, errors.New("runner should not execute")
}

type lifecycleDenyDefinition struct{}

func (lifecycleDenyDefinition) AuthorizeExposedWorkflow(context.Context, ExposureDefinitionAuthorization) error {
	return ErrPolicyDenied
}

func newLifecycleExposureStore() *lifecycleExposureStore {
	return &lifecycleExposureStore{profiles: make(map[string]hoststate.ExposureProfileSnapshot)}
}

func (s *lifecycleExposureStore) createProfile(t *testing.T, record hoststate.ExposureProfileRecord) hoststate.ExposureProfileSnapshot {
	t.Helper()
	result, err := s.PutExposureProfile(t.Context(), record, 0)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func (s *lifecycleExposureStore) PutExposureProfile(_ context.Context, record hoststate.ExposureProfileRecord, expected uint64) (hoststate.ExposureProfileSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prior, exists := s.profiles[record.ID]
	if !exists && expected != 0 || exists && prior.Generation != expected {
		return hoststate.ExposureProfileSnapshot{}, hoststate.ErrConflict
	}
	now := time.Date(2026, time.August, 24, 20, 0, 0, int(expected), time.UTC)
	created := now
	if exists {
		created = prior.CreatedAt
	}
	result := hoststate.ExposureProfileSnapshot{Record: record.Clone(), Generation: expected + 1, CreatedAt: created, UpdatedAt: now}
	s.profiles[record.ID] = result.Clone()
	return result, nil
}

func (s *lifecycleExposureStore) GetExposureProfile(_ context.Context, id string) (hoststate.ExposureProfileSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, ok := s.profiles[id]
	if !ok {
		return hoststate.ExposureProfileSnapshot{}, workflowruntime.ErrNotFound
	}
	return result.Clone(), nil
}

func (s *lifecycleExposureStore) ListExposureProfiles(context.Context, int) ([]hoststate.ExposureProfileSnapshot, error) {
	return nil, nil
}
func (s *lifecycleExposureStore) DeleteExposureProfile(context.Context, string, uint64) error {
	return nil
}
func (s *lifecycleExposureStore) PutMCPPrincipal(context.Context, hoststate.MCPPrincipalRecord, uint64) (hoststate.MCPPrincipalSnapshot, error) {
	return hoststate.MCPPrincipalSnapshot{}, errors.New("not implemented")
}
func (s *lifecycleExposureStore) GetMCPPrincipal(context.Context, string) (hoststate.MCPPrincipalSnapshot, error) {
	return hoststate.MCPPrincipalSnapshot{}, workflowruntime.ErrNotFound
}
func (s *lifecycleExposureStore) ResolveMCPPrincipalDigest(context.Context, string) (hoststate.MCPPrincipalSnapshot, error) {
	return hoststate.MCPPrincipalSnapshot{}, workflowruntime.ErrNotFound
}
func (s *lifecycleExposureStore) ListMCPPrincipals(context.Context, int) ([]hoststate.MCPPrincipalSnapshot, error) {
	return nil, nil
}
func (s *lifecycleExposureStore) DeleteMCPPrincipal(context.Context, string, uint64) error {
	return nil
}

var _ WorkflowExposureStore = (*lifecycleExposureStore)(nil)
var _ stepkind.Registry = (*stepkind.MemoryRegistry)(nil)
var _ compile.PolicyHook = compile.PolicyHookFunc(nil)
var _ = values.TypeString
