package appworkflow_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/registry"
	"github.com/hollis-labs/hadron/workflow/authoring"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestWorkflowExposureSessionIdentitySchemasEffectsAndGeneration(t *testing.T) {
	fixture := newExposureFixture(t)
	profile := hoststate.ExposureProfileRecord{
		ID: "profile:reviewer", Namespaces: []string{"team/review"}, DeniedEffects: graph.EffectSet{graph.EffectDestructive},
		Pins:           []graph.DefinitionRef{exposureDefinitionRef(fixture.catalog.records[0])},
		MaxDirectTools: 4, SearchScope: hoststate.ExposureSearchNamespaces, LazyLoad: true,
		Display: values.DisplayPolicy{Private: values.PrivateDisplayReveal},
	}
	profileSnapshot, err := fixture.service.PutProfile(t.Context(), profile, 0)
	if err != nil {
		t.Fatal(err)
	}
	token := "reviewer-token"
	principalSnapshot, err := fixture.service.PutPrincipal(t.Context(), appworkflow.PutMCPPrincipalRequest{Record: exposurePrincipal("agent:nanite/reviewer", profile.ID), Token: token})
	if err != nil {
		t.Fatal(err)
	}
	if principalSnapshot.Record.CredentialDigest != "" {
		t.Fatal("service exposed a credential digest")
	}
	agentOwned := exposureRecord("nanite/reviewer/private", "nanite/reviewer", false)
	fixture.catalog.records = append(fixture.catalog.records, agentOwned)
	fixture.definitions.plans[agentOwned.Name] = exposurePlan(agentOwned, "noop", graph.EffectCompute)

	unknownContext, unknown, err := fixture.service.ResolveSession(t.Context(), "unknown", "unknown-token")
	if err != nil || unknown.Authenticated || len(unknown.Profile.Namespaces) != 0 {
		t.Fatalf("unknown session = %#v, %v", unknown, err)
	}
	if unknownDirect, directErr := fixture.service.DirectWorkflows(unknownContext, unknown); directErr != nil || len(unknownDirect) != 0 {
		t.Fatalf("unknown direct tools = %#v, %v", unknownDirect, directErr)
	}

	ctx, session, err := fixture.service.ResolveSession(t.Context(), "session-one", token)
	if err != nil {
		t.Fatal(err)
	}
	if !session.Authenticated || session.ProfileGeneration != profileSnapshot.Generation {
		t.Fatalf("session = %#v", session)
	}
	if len(session.Profile.Namespaces) != 1 || session.Profile.Namespaces[0] != "team/review" || session.AgentNamespace != "nanite/reviewer" {
		t.Fatalf("agent-owned namespace default profile=%#v agent=%q", session.Profile.Namespaces, session.AgentNamespace)
	}
	binding, err := (appworkflow.MCPIdentityProvider{}).BindIdentity(ctx, appworkflow.IdentityRequest{SourceAuthority: "mcp"})
	if err != nil || binding.Principal != "agent:nanite/reviewer" {
		t.Fatalf("identity binding = %#v, %v", binding, err)
	}
	if _, forgedErr := (appworkflow.MCPIdentityProvider{}).BindIdentity(ctx, appworkflow.IdentityRequest{PrincipalHint: "agent:forged"}); !errors.Is(forgedErr, appworkflow.ErrPolicyDenied) {
		t.Fatalf("forged principal = %v", forgedErr)
	}

	direct, err := fixture.service.DirectWorkflows(ctx, session)
	if err != nil {
		t.Fatal(err)
	}
	if len(direct) != 2 {
		t.Fatalf("direct = %#v", direct)
	}
	directByName := make(map[string]appworkflow.WorkflowExposureDescriptor, len(direct))
	for _, descriptor := range direct {
		directByName[descriptor.Name] = descriptor
	}
	pinned := directByName["team/review/summarize"]
	agentDescriptor := directByName[agentOwned.Name]
	if pinned.ToolName != "workflow_team_review_summarize" || pinned.Effects[0] != graph.EffectCompute || pinned.InputSchema["required"] == nil {
		t.Fatalf("derived contract = %#v", pinned)
	}
	inputProperties := pinned.InputSchema["properties"].(map[string]any)
	toneSchema := inputProperties["tone"].(map[string]any)
	inputRequired, inputRequiredOK := pinned.InputSchema["required"].([]any)
	outputRequired, outputRequiredOK := pinned.OutputSchema["required"].([]any)
	if toneSchema["default"] != "brief" || !inputRequiredOK || len(inputRequired) != 1 || inputRequired[0] != "message" || !outputRequiredOK || len(outputRequired) != 1 || outputRequired[0] != "result" {
		t.Fatalf("default/required IO fidelity input=%#v output=%#v", pinned.InputSchema, pinned.OutputSchema)
	}
	if described, describeErr := fixture.service.Describe(ctx, session, agentDescriptor.Definition, "run"); describeErr != nil || described.Definition != exposureDefinitionRef(agentOwned) {
		t.Fatalf("agent-owned exact invocation descriptor = %#v, %v", described, describeErr)
	}
	pinned.InputSchema["type"] = "array"
	again, err := fixture.service.DirectWorkflows(ctx, session)
	if err != nil {
		t.Fatal(err)
	}
	againByName := make(map[string]appworkflow.WorkflowExposureDescriptor, len(again))
	for _, descriptor := range again {
		againByName[descriptor.Name] = descriptor
	}
	if againByName[pinned.Name].InputSchema["type"] != "object" {
		t.Fatalf("mutable descriptor escaped service: %#v, %v", again, err)
	}

	search, err := fixture.service.Search(ctx, session, "sum", 10)
	if err != nil || len(search) != 1 || search[0].Name != pinned.Name {
		t.Fatalf("search = %#v, %v", search, err)
	}
	loaded, err := fixture.service.Load(ctx, session, []graph.DefinitionRef{search[0].Definition})
	if err != nil || len(loaded) != 1 {
		t.Fatalf("load = %#v, %v", loaded, err)
	}
	if _, displayErr := fixture.service.DisplayPolicy(ctx, session, values.DisplayPolicy{Private: values.PrivateDisplayReveal}); displayErr != nil {
		t.Fatalf("private display = %v", displayErr)
	}
	catalog, err := fixture.service.NamespaceCatalog(ctx, session)
	if err != nil || catalog["team/review"] != 2 {
		t.Fatalf("namespace catalog = %#v, %v", catalog, err)
	}
	forged := session.Clone()
	forged.Profile.SearchScope = hoststate.ExposureSearchAll
	if _, forgedErr := fixture.service.DirectWorkflows(ctx, forged); !errors.Is(forgedErr, appworkflow.ErrPolicyDenied) {
		t.Fatalf("forged profile widening = %v", forgedErr)
	}
	forged = session.Clone()
	forged.AgentNamespace = "team/review"
	if _, forgedErr := fixture.service.DirectWorkflows(ctx, forged); !errors.Is(forgedErr, appworkflow.ErrPolicyDenied) {
		t.Fatalf("forged agent namespace = %v", forgedErr)
	}

	profile.LazyLoad = false
	if _, err := fixture.service.PutProfile(t.Context(), profile, profileSnapshot.Generation); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.service.DirectWorkflows(ctx, session); !errors.Is(err, appworkflow.ErrPolicyDenied) {
		t.Fatalf("stale profile generation = %v", err)
	}
}

func TestWorkflowExposurePublishedWorkflowsAreExactBoundedAndPublic(t *testing.T) {
	fixture := newExposureFixture(t)
	fixture.catalog.records[1].Published = false
	fixture.definitions.plans[fixture.catalog.records[0].Name].Graph.Metadata = graph.Metadata{
		"description": "Summarize a review.", "tags": []any{"review", "summary"},
	}
	descriptors, err := fixture.service.PublishedWorkflows(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(descriptors) != 2 || descriptors[0].Name != "team/review/discover" || descriptors[1].Name != "team/review/summarize" {
		t.Fatalf("published descriptors = %#v", descriptors)
	}
	var summarize appworkflow.WorkflowExposureDescriptor
	for _, descriptor := range descriptors {
		if descriptor.Name == fixture.catalog.records[0].Name {
			summarize = descriptor
		}
	}
	if summarize.Description != "Summarize a review." || !reflect.DeepEqual(summarize.Tags, []string{"review", "summary"}) || summarize.Definition != exposureDefinitionRef(fixture.catalog.records[0]) || summarize.Provenance.Authority != fixture.catalog.records[0].Authority || summarize.Provenance.Digest != fixture.catalog.records[0].Digest || summarize.InputSchema["required"] == nil || summarize.OutputSchema["required"] == nil {
		t.Fatalf("published exact descriptor = %#v", summarize)
	}
	summarize.Tags[0] = "mutated"
	again, err := fixture.service.PublishedWorkflows(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, descriptor := range again {
		if descriptor.Name == summarize.Name && descriptor.Tags[0] == "mutated" {
			t.Fatal("published descriptor mutation escaped")
		}
	}

	for len(fixture.catalog.records) <= appworkflow.MaximumPublishedWorkflows+1 {
		record := exposureRecord(fmt.Sprintf("team/review/bounded-%03d", len(fixture.catalog.records)), "team/review", true)
		fixture.catalog.records = append(fixture.catalog.records, record)
	}
	if _, err := fixture.service.PublishedWorkflows(t.Context()); !errors.Is(err, appworkflow.ErrHostNotReady) {
		t.Fatalf("oversized published catalog error = %v", err)
	}
}

func TestWorkflowExposureNamespaceSearchScopeAndCorruptionBoundaries(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		scope     hoststate.ExposureSearchScope
		private   bool
		workflow  string
		published bool
	}{
		{name: "none cannot reach namespace", scope: hoststate.ExposureSearchNone, workflow: "team/review/summarize", published: true},
		{name: "public cannot reach private namespace record", scope: hoststate.ExposureSearchPublic, workflow: "team/review/private", private: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newExposureFixture(t)
			if testCase.private {
				privateRecord := exposureRecord(testCase.workflow, "team/review", testCase.published)
				fixture.catalog.records = append(fixture.catalog.records, privateRecord)
				fixture.definitions.plans[privateRecord.Name] = exposurePlan(privateRecord, "noop", graph.EffectRead)
			}
			profile := hoststate.ExposureProfileRecord{ID: "profile:narrow", Namespaces: []string{"team/review"}, MaxDirectTools: 4, SearchScope: testCase.scope, LazyLoad: true}
			if _, putErr := fixture.service.PutProfile(t.Context(), profile, 0); putErr != nil {
				t.Fatal(putErr)
			}
			if _, putErr := fixture.service.PutPrincipal(t.Context(), appworkflow.PutMCPPrincipalRequest{Record: exposurePrincipal("user:narrow", profile.ID), Token: "narrow"}); putErr != nil {
				t.Fatal(putErr)
			}
			ctx, session, resolveErr := fixture.service.ResolveSession(t.Context(), "narrow", "narrow")
			if resolveErr != nil {
				t.Fatal(resolveErr)
			}
			var target registry.WorkflowRecord
			for _, record := range fixture.catalog.records {
				if record.Name == testCase.workflow {
					target = record
					break
				}
			}
			if _, describeErr := fixture.service.Describe(ctx, session, exposureDefinitionRef(target), "run"); !errors.Is(describeErr, appworkflow.ErrWorkflowHidden) {
				t.Fatalf("namespace widened %s exact access: %v", testCase.scope, describeErr)
			}
		})
	}

	fixture := newExposureFixture(t)
	profile := hoststate.ExposureProfileRecord{ID: "profile:corruption", MaxDirectTools: 4, SearchScope: hoststate.ExposureSearchPublic}
	if _, putErr := fixture.service.PutProfile(t.Context(), profile, 0); putErr != nil {
		t.Fatal(putErr)
	}
	if _, putErr := fixture.service.PutPrincipal(t.Context(), appworkflow.PutMCPPrincipalRequest{Record: exposurePrincipal("user:corruption", profile.ID), Token: "corruption"}); putErr != nil {
		t.Fatal(putErr)
	}
	ctx, session, resolveErr := fixture.service.ResolveSession(t.Context(), "corruption", "corruption")
	if resolveErr != nil {
		t.Fatal(resolveErr)
	}
	fixture.definitions.plans["team/review/summarize"].Graph.Inputs[0].Schema["type"] = "not-a-schema-type"
	if results, searchErr := fixture.service.Search(ctx, session, "summarize", 10); searchErr == nil || errors.Is(searchErr, appworkflow.ErrWorkflowHidden) {
		t.Fatalf("corrupt immutable definition was hidden as an empty result: %#v, %v", results, searchErr)
	}
	fixture.definitions.plans["team/review/summarize"].Graph.Inputs[0].Schema["type"] = "string"
	fixture.definitions.plans["team/review/summarize"].Graph.Namespace = "substituted/namespace"
	if _, searchErr := fixture.service.Search(ctx, session, "summarize", 10); searchErr == nil || errors.Is(searchErr, appworkflow.ErrWorkflowHidden) {
		t.Fatalf("namespace-substituted plan was authorized: %v", searchErr)
	}
	fixture.definitions.plans["team/review/summarize"].Graph.Namespace = "team/review"
	localID := fixture.catalog.records[0].SourceDefinitionID()
	fixture.definitions.plans["team/review/summarize"].ID = fixture.catalog.records[0].Name
	fixture.definitions.plans["team/review/summarize"].Definition.ID = fixture.catalog.records[0].Name
	fixture.definitions.plans["team/review/summarize"].Graph.ID = fixture.catalog.records[0].Name
	if _, searchErr := fixture.service.Search(ctx, session, "summarize", 10); searchErr == nil || errors.Is(searchErr, appworkflow.ErrWorkflowHidden) {
		t.Fatalf("qualified catalog name was accepted as source-local plan identity: %v", searchErr)
	}
	fixture.definitions.plans["team/review/summarize"].ID = localID
	fixture.definitions.plans["team/review/summarize"].Definition.ID = localID
	fixture.definitions.plans["team/review/summarize"].Graph.ID = localID
	fixture.definitions.plans["team/review/summarize"].Definition.Kind = "file"
	fixture.definitions.plans["team/review/summarize"].Definition.Locator = "/substituted.workflow.yaml"
	if _, searchErr := fixture.service.Search(ctx, session, "summarize", 10); searchErr == nil || errors.Is(searchErr, appworkflow.ErrWorkflowHidden) {
		t.Fatalf("source-substituted plan was authorized: %v", searchErr)
	}
}

func TestWorkflowExposureBudgetCollisionAndAdditionalAuthoritiesFailClosed(t *testing.T) {
	budget := newExposureFixture(t)
	extra := exposureRecord("team/review/other", "team/review", false)
	budget.catalog.records = append(budget.catalog.records, extra)
	budget.definitions.plans[extra.Name] = exposurePlan(extra, "noop", graph.EffectCompute)
	budgetProfile := hoststate.ExposureProfileRecord{ID: "profile:budget", Namespaces: []string{"team/review"}, Pins: []graph.DefinitionRef{exposureDefinitionRef(budget.catalog.records[0]), exposureDefinitionRef(extra)}, MaxDirectTools: 1, SearchScope: hoststate.ExposureSearchAll}
	if _, err := budget.service.PutProfile(t.Context(), budgetProfile, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := budget.service.PutPrincipal(t.Context(), appworkflow.PutMCPPrincipalRequest{Record: exposurePrincipal("user:budget", budgetProfile.ID), Token: "budget"}); err != nil {
		t.Fatal(err)
	}
	budgetContext, budgetSession, err := budget.service.ResolveSession(t.Context(), "budget", "budget")
	if err != nil {
		t.Fatal(err)
	}
	if _, directErr := budget.service.DirectWorkflows(budgetContext, budgetSession); !errors.Is(directErr, appworkflow.ErrPolicyDenied) {
		t.Fatalf("direct-tool budget = %v", directErr)
	}

	fixture := newExposureFixture(t)
	truncatedToolPrefix := strings.Repeat("a", 108)
	fixture.catalog.records = append(fixture.catalog.records,
		exposureRecord("team/review/"+truncatedToolPrefix+"-one", "team/review", false),
		exposureRecord("team/review/"+truncatedToolPrefix+"-two", "team/review", false),
	)
	for _, record := range fixture.catalog.records[len(fixture.catalog.records)-2:] {
		fixture.definitions.plans[record.Name] = exposurePlan(record, "noop", graph.EffectCompute)
	}
	collisionRecords := fixture.catalog.records[len(fixture.catalog.records)-2:]
	profile := hoststate.ExposureProfileRecord{ID: "profile:collision", Namespaces: []string{"team/review"}, Pins: []graph.DefinitionRef{exposureDefinitionRef(collisionRecords[0]), exposureDefinitionRef(collisionRecords[1])}, MaxDirectTools: 10, SearchScope: hoststate.ExposureSearchAll}
	if _, putErr := fixture.service.PutProfile(t.Context(), profile, 0); putErr != nil {
		t.Fatal(putErr)
	}
	if _, putErr := fixture.service.PutPrincipal(t.Context(), appworkflow.PutMCPPrincipalRequest{Record: exposurePrincipal("user:collision", profile.ID), Token: "collision"}); putErr != nil {
		t.Fatal(putErr)
	}
	ctx, session, err := fixture.service.ResolveSession(t.Context(), "collision", "collision")
	if err != nil {
		t.Fatal(err)
	}
	if _, directErr := fixture.service.DirectWorkflows(ctx, session); !errors.Is(directErr, hoststate.ErrConflict) {
		t.Fatalf("tool-name collision = %v", directErr)
	}

	denying, err := appworkflow.NewWorkflowExposureService(appworkflow.WorkflowExposureOptions{
		Store: fixture.store, Catalog: fixture.catalog, Definitions: fixture.definitions, StepKinds: fixture.kinds,
		DefinitionsACL: denyExposureDefinition{}, PrivateDisplay: denyPrivateDisplay{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if results, err := denying.Search(ctx, session, "", 10); err != nil || len(results) != 0 {
		t.Fatalf("additional definition denial = %#v, %v", results, err)
	}
	session.Profile.Display = values.DisplayPolicy{Private: values.PrivateDisplayReveal}
	if _, err := denying.DisplayPolicy(ctx, session, session.Profile.Display); !errors.Is(err, appworkflow.ErrPolicyDenied) {
		t.Fatalf("additional private-display denial = %v", err)
	}
}

func TestWorkflowExposureConstructorRejectsTypedNilAuthorities(t *testing.T) {
	fixture := newExposureFixture(t)
	var authority *typedNilExposureAuthority
	for _, testCase := range []struct {
		name      string
		configure func(*appworkflow.WorkflowExposureOptions)
	}{
		{name: "management", configure: func(options *appworkflow.WorkflowExposureOptions) { options.Management = authority }},
		{name: "session", configure: func(options *appworkflow.WorkflowExposureOptions) { options.Sessions = authority }},
		{name: "definition", configure: func(options *appworkflow.WorkflowExposureOptions) { options.DefinitionsACL = authority }},
		{name: "private display", configure: func(options *appworkflow.WorkflowExposureOptions) { options.PrivateDisplay = authority }},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			options := appworkflow.WorkflowExposureOptions{Store: fixture.store, Catalog: fixture.catalog, Definitions: fixture.definitions, StepKinds: fixture.kinds}
			testCase.configure(&options)
			if _, err := appworkflow.NewWorkflowExposureService(options); !errors.Is(err, appworkflow.ErrInvalidHost) {
				t.Fatalf("typed-nil %s authority = %v", testCase.name, err)
			}
		})
	}
}

func TestWorkflowExposureListPrincipalsAlwaysRemovesCredentialVerifier(t *testing.T) {
	fixture := newExposureFixture(t)
	leaking := &leakingExposureStore{WorkflowExposureStore: fixture.store, principal: hoststate.MCPPrincipalSnapshot{Record: hoststate.MCPPrincipalRecord{ID: "user:leak", CredentialDigest: "sha256:should-not-cross-the-service-boundary"}}}
	service, err := appworkflow.NewWorkflowExposureService(appworkflow.WorkflowExposureOptions{Store: leaking})
	if err != nil {
		t.Fatal(err)
	}
	principals, err := service.ListPrincipals(t.Context(), 10)
	if err != nil || len(principals) != 1 || principals[0].Record.CredentialDigest != "" {
		t.Fatalf("public principal list = %#v, %v", principals, err)
	}
}

type exposureFixture struct {
	service     *appworkflow.WorkflowExposureService
	store       *persistence.WorkflowExposureStore
	catalog     *exposureCatalog
	definitions *exposureDefinitions
	kinds       *stepkind.MemoryRegistry
}

type typedNilExposureAuthority struct{}

type leakingExposureStore struct {
	*persistence.WorkflowExposureStore
	principal hoststate.MCPPrincipalSnapshot
}

func (s *leakingExposureStore) ListMCPPrincipals(context.Context, int) ([]hoststate.MCPPrincipalSnapshot, error) {
	return []hoststate.MCPPrincipalSnapshot{s.principal}, nil
}

func (*typedNilExposureAuthority) AuthorizeExposureManagement(context.Context, appworkflow.ExposureManagementAuthorization) error {
	return nil
}

func (*typedNilExposureAuthority) AuthorizeExposureSession(context.Context, appworkflow.WorkflowExposureSession) error {
	return nil
}

func (*typedNilExposureAuthority) AuthorizeExposedWorkflow(context.Context, appworkflow.ExposureDefinitionAuthorization) error {
	return nil
}

func (*typedNilExposureAuthority) AuthorizePrivateWorkflowDisplay(context.Context, appworkflow.WorkflowExposureSession) error {
	return nil
}

func newExposureFixture(t *testing.T) exposureFixture {
	t.Helper()
	db, err := persistence.Open(filepath.Join(t.TempDir(), "exposure.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := persistence.NewWorkflowExposureStore(db)
	if err != nil {
		t.Fatal(err)
	}
	kinds := stepkind.NewRegistry()
	compute := stepkindtest.NewNoopKind("noop", "v1")
	if registerErr := kinds.Register(compute); registerErr != nil {
		t.Fatal(registerErr)
	}
	destructive := stepkindtest.NewNoopKind("destroy", "v1")
	destructive.SpecValue.Effects = graph.EffectSet{graph.EffectDestructive}
	if registerErr := kinds.Register(destructive); registerErr != nil {
		t.Fatal(registerErr)
	}
	records := []registry.WorkflowRecord{
		exposureRecord("team/review/summarize", "team/review", true),
		exposureRecord("team/review/delete", "team/review", true),
		exposureRecord("team/review/discover", "team/review", true),
	}
	catalog := &exposureCatalog{records: records}
	definitions := &exposureDefinitions{plans: map[string]*workflowcompile.ExecutionPlan{
		records[0].Name: exposurePlan(records[0], "noop", graph.EffectCompute),
		records[1].Name: exposurePlan(records[1], "destroy", graph.EffectDestructive),
		records[2].Name: exposurePlan(records[2], "noop", graph.EffectRead),
	}}
	service, err := appworkflow.NewWorkflowExposureService(appworkflow.WorkflowExposureOptions{Store: store, Catalog: catalog, Definitions: definitions, StepKinds: kinds})
	if err != nil {
		t.Fatal(err)
	}
	return exposureFixture{service: service, store: store, catalog: catalog, definitions: definitions, kinds: kinds}
}

func exposureRecord(name, namespace string, published bool) registry.WorkflowRecord {
	digest := values.SHA256Digest([]byte(name))
	return registry.WorkflowRecord{
		Name: name, Namespace: namespace, Version: "v1", Digest: digest, Published: published,
		SourceFormat: graph.SourceWorkflow, SourceSchemaID: authoring.WorkflowSourceSchemaID, SourceSchemaVersion: authoring.WorkflowSourceSchemaVersion,
		Authority: "registry.test", TrustClass: "project", PlanDigest: values.SHA256Digest([]byte("plan-" + name)),
		Provenance: graph.Provenance{Origin: "fixture", Locator: "registry://" + name + "/v1", Authority: "registry.test", Revision: "v1", Digest: digest},
	}
}

func exposureDefinitionRef(r registry.WorkflowRecord) graph.DefinitionRef {
	return graph.DefinitionRef{Kind: "registry", ID: r.Name, Version: r.Version, Digest: r.Digest}
}

func exposurePlan(record registry.WorkflowRecord, kind string, effect graph.Effect) *workflowcompile.ExecutionPlan {
	provenance := record.Provenance
	provenance.Metadata = graph.Metadata{"trust_class": record.TrustClass}
	sourceID := record.SourceDefinitionID()
	definition := graph.DefinitionRef{Authority: record.Authority, Kind: "workflow", ID: sourceID, Locator: provenance.Locator, Version: record.Version, Digest: record.Digest, Provenance: &provenance}
	return &workflowcompile.ExecutionPlan{
		SchemaVersion: workflowcompile.ExecutionPlanSchemaVersion, ID: sourceID, Digest: values.SHA256Digest([]byte("plan-" + record.Name)), Definition: definition,
		Provenance: provenance, SourceDigests: []workflowcompile.SourceDigest{{Format: record.SourceFormat, Digest: record.Digest}},
		Graph: graph.Graph{ID: sourceID, Namespace: record.Namespace, Version: record.Version, Digest: values.SHA256Digest([]byte("graph-" + record.Name)), Provenance: provenance,
			Inputs: []graph.InputSpec{
				{Name: "message", Description: "Message", Required: true, Schema: graph.Schema{"type": "string"}},
				{Name: "tone", Required: true, Default: &graph.Binding{Kind: graph.BindingLiteral, Literal: "brief"}, Schema: graph.Schema{"type": "string"}},
			},
			Outputs: []graph.OutputSpec{{Name: "result", Schema: graph.Schema{"type": "integer"}}},
			Nodes:   []graph.Node{{ID: "root", Kind: kind, KindVersion: "v1", Effects: graph.EffectSet{effect}}}},
	}
}

func exposurePrincipal(id, profile string) hoststate.MCPPrincipalRecord {
	return hoststate.MCPPrincipalRecord{ID: id, ProfileID: profile, Identity: hoststate.IdentityBinding{
		Principal: id, SourceAuthority: "mcp", Trust: "local", Grants: []string{"workflow.run"},
		RunScope: hoststate.RunScope{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeUser, ID: id},
	}}
}

type exposureCatalog struct{ records []registry.WorkflowRecord }

func (c *exposureCatalog) ResolveWorkflow(_ context.Context, query registry.WorkflowQuery) (registry.WorkflowResolution, error) {
	for _, record := range c.records {
		if record.Name == query.Name && (query.Version == "" || record.Version == query.Version) && (query.Digest == "" || record.Digest == query.Digest) {
			return registry.WorkflowResolution{Record: record, Movable: query.Version == "" && query.Digest == ""}, nil
		}
	}
	return registry.WorkflowResolution{}, registry.ErrWorkflowNotFound
}

func (c *exposureCatalog) SearchWorkflows(_ context.Context, namespace, text string) ([]registry.WorkflowRecord, error) {
	var result []registry.WorkflowRecord
	for _, record := range c.records {
		if (namespace == "" || record.Namespace == namespace) && (text == "" || strings.Contains(strings.ToLower(record.Name), strings.ToLower(text))) {
			result = append(result, record)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

type exposureDefinitions struct {
	plans map[string]*workflowcompile.ExecutionPlan
}

func (d *exposureDefinitions) ResolvePlan(_ context.Context, ref graph.DefinitionRef) (*workflowcompile.ExecutionPlan, error) {
	plan := d.plans[ref.ID]
	if plan == nil || plan.Definition.Version != ref.Version || plan.Definition.Digest != ref.Digest {
		return nil, appworkflow.ErrDefinitionUnresolved
	}
	return plan, nil
}

func (*exposureDefinitions) LoadPlan(context.Context, string) (*workflowcompile.ExecutionPlan, error) {
	return nil, appworkflow.ErrDefinitionUnresolved
}

type denyExposureDefinition struct{}

func (denyExposureDefinition) AuthorizeExposedWorkflow(context.Context, appworkflow.ExposureDefinitionAuthorization) error {
	return errors.New("denied")
}

type denyPrivateDisplay struct{}

func (denyPrivateDisplay) AuthorizePrivateWorkflowDisplay(context.Context, appworkflow.WorkflowExposureSession) error {
	return errors.New("denied")
}
