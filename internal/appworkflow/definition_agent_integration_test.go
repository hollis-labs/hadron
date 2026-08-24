package appworkflow_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/workflow/adapters/agent"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestHadronDefinitionResolverCompilesAgentSugarAndReloadsDurableBundle(t *testing.T) {
	root := t.TempDir()
	sourcePath := filepath.Join(root, "agent.workflow.yaml")
	if err := os.WriteFile(sourcePath, []byte(`workflow: {id: agent-root, version: v1}
steps:
  - id: review
    agent_launch:
      substrate: remote
      logical_agent_id: reviewer
      wait: false
`), 0o600); err != nil {
		t.Fatal(err)
	}
	databasePath := filepath.Join(t.TempDir(), "workflow.db")
	store, err := persistence.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := persistence.NewWorkflowHostStore(store)
	if err != nil {
		t.Fatal(err)
	}
	kinds := agentResolverKinds(t)
	resolver, err := appworkflow.NewDefinitionResolver(appworkflow.DefinitionResolverOptions{
		Roots: []string{root}, Authorizer: appworkflow.DefinitionAuthorizerFunc(allowAgentDefinition),
		BundledDefinitions: journal,
		Compile: appworkflow.DefinitionCompileOptions{
			StepKinds: kinds, SemanticRevision: "agent-source-v1",
			NodeExpanders: []workflowcompile.NodeExpander{agent.SourceExpander{}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	requestedRoot := graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: "agent-root", Locator: sourcePath, Version: "v1"}
	plan, err := resolver.ResolvePlan(t.Context(), requestedRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Graph.Nodes) != 1 || plan.Graph.Nodes[0].Kind != "call" || plan.Graph.Nodes[0].Call == nil || len(plan.BundledDefinitions) != 1 {
		t.Fatalf("expanded Hadron plan = %#v", plan)
	}
	childRef := plan.Graph.Nodes[0].Call.Definition
	resolvedBeforeStart, err := resolver.ResolveDefinition(t.Context(), childRef)
	if err != nil || resolvedBeforeStart.Graph.Nodes[0].Kind != agent.KindName {
		t.Fatalf("pre-start cached bundle = %#v, %v", resolvedBeforeStart, err)
	}

	recordedAt := time.Date(2026, 8, 24, 17, 0, 0, 0, time.UTC)
	planRef := workflowruntime.PlanRef{ID: plan.ID, Version: plan.Graph.Version, Digest: plan.Digest, SchemaVersion: plan.SchemaVersion}
	identity := hoststate.IdentityBinding{
		Principal: "user:agent", SourceAuthority: "project", Trust: "project",
		Grants: []string{"workflow.run"}, RunScope: "project:test", ExecutionTarget: "local",
	}
	facts := hoststate.PolicyFacts{
		Operation: "start", RunID: "agent-parent-run", Plan: planRef, Identity: identity,
		NodeCount: len(plan.Graph.Nodes), BlastRadius: map[string]int{},
	}
	decision := hoststate.PolicyDecision{
		ID: "agent-start-decision", RunID: facts.RunID, Operation: facts.Operation,
		Outcome: hoststate.PolicyAllow, Reason: "test", DecidedAt: recordedAt,
	}
	record := hoststate.StartRecord{
		Run: workflowruntime.BoundRun{
			ID: facts.RunID, Plan: planRef,
			InputsRef: values.ValueSetRef{ID: "agent-inputs", Digest: values.SHA256Digest([]byte("agent-inputs"))},
			CreatedAt: recordedAt, Provenance: plan.Provenance,
		},
		Plan: *plan, Requested: requestedRoot, StartKey: "agent-start-key",
		RequestDigest: values.SHA256Digest([]byte("agent-request")), CallerInputHash: values.SHA256Digest([]byte("agent-caller-inputs")),
		Identity: identity, Facts: facts, Decision: decision, RecordedAt: recordedAt,
	}
	if _, _, recordErr := journal.RecordStart(t.Context(), record); recordErr != nil {
		t.Fatal(recordErr)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if removeErr := os.Remove(sourcePath); removeErr != nil {
		t.Fatal(removeErr)
	}

	reopenedStore, err := persistence.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedStore.Close() })
	reopenedJournal, err := persistence.NewWorkflowHostStore(reopenedStore)
	if err != nil {
		t.Fatal(err)
	}
	var authorization []appworkflow.DefinitionAuthorization
	restarted, err := appworkflow.NewDefinitionResolver(appworkflow.DefinitionResolverOptions{
		Roots: []string{root}, BundledDefinitions: reopenedJournal,
		Authorizer: appworkflow.DefinitionAuthorizerFunc(func(_ context.Context, request appworkflow.DefinitionAuthorization) error {
			authorization = append(authorization, request)
			return nil
		}),
		Compile: appworkflow.DefinitionCompileOptions{
			StepKinds: kinds, SemanticRevision: "agent-source-v1",
			NodeExpanders: []workflowcompile.NodeExpander{agent.SourceExpander{}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	resolvedAfterRestart, err := restarted.ResolveDefinition(t.Context(), childRef)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(resolvedAfterRestart, resolvedBeforeStart) {
		t.Fatalf("restarted bundle differs: %#v / %#v", resolvedAfterRestart, resolvedBeforeStart)
	}
	if len(authorization) != 2 || authorization[0].Stage != appworkflow.AuthorizationRequested ||
		authorization[1].Stage != appworkflow.AuthorizationResolved || authorization[1].TrustClass != "project" ||
		authorization[1].Resolved == nil || authorization[1].Resolved.Digest != childRef.Digest ||
		authorization[1].Container == nil || *authorization[1].Container != planRef {
		t.Fatalf("bundle authorization order = %#v", authorization)
	}
	resolvedAfterRestart.Graph.Nodes[0].ID = "caller-mutation"
	replayed, err := restarted.ResolveDefinition(t.Context(), childRef)
	if err != nil || replayed.Graph.Nodes[0].ID != "session" {
		t.Fatalf("defensive replay = %#v, %v", replayed, err)
	}
}

func TestBundledDefinitionAuthorizationIsContainerScoped(t *testing.T) {
	composition, err := agent.Compose(agent.CompositionRequest{
		NodeID: "review", Config: graph.Config{"substrate": "remote", "logical_agent_id": "reviewer", "launch_id": "review"},
		ParentClose: graph.ParentCloseCancel,
	})
	if err != nil {
		t.Fatal(err)
	}
	allowedPlan := workflowruntime.PlanRef{
		ID: "allowed-root", Version: "v1", Digest: values.SHA256Digest([]byte("allowed-root")),
		SchemaVersion: workflowcompile.ExecutionPlanSchemaVersion,
	}
	deniedPlan := workflowruntime.PlanRef{
		ID: "denied-root", Version: "v1", Digest: values.SHA256Digest([]byte("denied-root")),
		SchemaVersion: workflowcompile.ExecutionPlanSchemaVersion,
	}
	conflicting := composition.Definition
	conflicting.InputBindings = map[string]graph.Binding{
		"parent-correlation": {Kind: graph.BindingLiteral, Literal: "different"},
	}
	source := &staticBundleSource{candidates: []hoststate.BundledDefinitionCandidate{
		{Definition: composition.Definition, Container: deniedPlan, TrustClass: "project"},
		{Definition: conflicting, Container: allowedPlan, TrustClass: "project"},
	}}
	requestedDenied, err := appworkflow.NewDefinitionResolver(appworkflow.DefinitionResolverOptions{
		Roots: []string{t.TempDir()}, BundledDefinitions: source,
		Authorizer: appworkflow.DefinitionAuthorizerFunc(func(context.Context, appworkflow.DefinitionAuthorization) error {
			return errors.New("request denied")
		}),
		Compile: appworkflow.DefinitionCompileOptions{StepKinds: agentResolverKinds(t), SemanticRevision: "bundle-auth-v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, deniedErr := requestedDenied.ResolveDefinition(t.Context(), composition.Definition.Definition); !errors.Is(deniedErr, appworkflow.ErrDefinitionUnauthorized) {
		t.Fatalf("requested-stage denial error = %v", deniedErr)
	}
	if source.calls != 0 {
		t.Fatalf("bundle source calls before requested authorization = %d", source.calls)
	}

	var resolvedContainers []workflowruntime.PlanRef
	resolver, err := appworkflow.NewDefinitionResolver(appworkflow.DefinitionResolverOptions{
		Roots: []string{t.TempDir()}, BundledDefinitions: source,
		Authorizer: appworkflow.DefinitionAuthorizerFunc(func(_ context.Context, request appworkflow.DefinitionAuthorization) error {
			if request.Stage != appworkflow.AuthorizationResolved {
				return nil
			}
			if request.Container == nil {
				return errors.New("bundle container is required")
			}
			resolvedContainers = append(resolvedContainers, *request.Container)
			if *request.Container == deniedPlan {
				return errors.New("container denied")
			}
			return nil
		}),
		Compile: appworkflow.DefinitionCompileOptions{StepKinds: agentResolverKinds(t), SemanticRevision: "bundle-auth-v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := resolver.ResolveDefinition(t.Context(), composition.Definition.Definition)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(resolved.InputBindings, conflicting.InputBindings) {
		t.Fatalf("authorized container definition = %#v", resolved.InputBindings)
	}
	if len(resolvedContainers) != 2 || !containsPlanRef(resolvedContainers, allowedPlan) || !containsPlanRef(resolvedContainers, deniedPlan) {
		t.Fatalf("resolved authorization containers = %#v", resolvedContainers)
	}

	denyAll, err := appworkflow.NewDefinitionResolver(appworkflow.DefinitionResolverOptions{
		Roots: []string{t.TempDir()}, BundledDefinitions: source,
		Authorizer: appworkflow.DefinitionAuthorizerFunc(func(_ context.Context, request appworkflow.DefinitionAuthorization) error {
			if request.Stage == appworkflow.AuthorizationResolved {
				return errors.New("container denied")
			}
			return nil
		}),
		Compile: appworkflow.DefinitionCompileOptions{StepKinds: agentResolverKinds(t), SemanticRevision: "bundle-auth-v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, denyErr := denyAll.ResolveDefinition(t.Context(), composition.Definition.Definition); !errors.Is(denyErr, appworkflow.ErrDefinitionUnauthorized) {
		t.Fatalf("all-container denial error = %v", denyErr)
	}

	allowAll, err := appworkflow.NewDefinitionResolver(appworkflow.DefinitionResolverOptions{
		Roots: []string{t.TempDir()}, BundledDefinitions: source,
		Authorizer: appworkflow.DefinitionAuthorizerFunc(allowAgentDefinition),
		Compile:    appworkflow.DefinitionCompileOptions{StepKinds: agentResolverKinds(t), SemanticRevision: "bundle-auth-v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, conflictErr := allowAll.ResolveDefinition(t.Context(), composition.Definition.Definition); !errors.Is(conflictErr, appworkflow.ErrDefinitionPinConflict) {
		t.Fatalf("authorized cross-container conflict error = %v", conflictErr)
	}
}

type staticBundleSource struct {
	candidates []hoststate.BundledDefinitionCandidate
	calls      int
}

func (s *staticBundleSource) FindBundledDefinitions(context.Context, graph.DefinitionRef) ([]hoststate.BundledDefinitionCandidate, error) {
	s.calls++
	return append([]hoststate.BundledDefinitionCandidate(nil), s.candidates...), nil
}

func containsPlanRef(values []workflowruntime.PlanRef, target workflowruntime.PlanRef) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func agentResolverKinds(t *testing.T) stepkind.Registry {
	t.Helper()
	registry := stepkind.NewRegistry()
	for _, name := range []string{"agent_session", "call"} {
		if err := registry.Register(stepkindtest.NewNoopKind(name, "v1")); err != nil {
			t.Fatal(err)
		}
	}
	return registry
}

func allowAgentDefinition(context.Context, appworkflow.DefinitionAuthorization) error { return nil }
