package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hollis-labs/hadron/internal/a2a"
	"github.com/hollis-labs/hadron/internal/agentcard"
	"github.com/hollis-labs/hadron/internal/api"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/artifacts"
	"github.com/hollis-labs/hadron/internal/config"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
	"github.com/hollis-labs/hadron/internal/scheduler"
	"github.com/hollis-labs/hadron/internal/trigger"
	gateadapter "github.com/hollis-labs/go-workflow/adapters/gate"
	scriptadapter "github.com/hollis-labs/go-workflow/adapters/script"
	"github.com/hollis-labs/go-workflow/adapters/transform"
	waitadapter "github.com/hollis-labs/go-workflow/adapters/wait"
	workflowgate "github.com/hollis-labs/go-workflow/gate"
	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/stepkind"
	"github.com/hollis-labs/go-workflow/values"
	workflowwait "github.com/hollis-labs/go-workflow/wait"
)

const (
	workflowClaimLease    = 90 * time.Second
	workflowClaimRenew    = 30 * time.Second
	workflowIdlePoll      = 100 * time.Millisecond
	workflowRecoveryBatch = 100
)

// productionWorkflowKindBoundary is the daemon host capability profile, not
// an engine limitation. Other public adapters remain available to embedded
// hosts, but hadrond accepts only this fully composed exact set.
func productionWorkflowKindBoundary() []appworkflow.KindRef {
	return []appworkflow.KindRef{
		{Name: gateadapter.Name, Version: "v1"},
		{Name: waitadapter.MessageWaitName, Version: "v1"},
		{Name: scriptadapter.Name, Version: "v1"},
		{Name: waitadapter.SleepName, Version: "v1"},
		{Name: transform.Name, Version: "v1"},
		{Name: waitadapter.WaitForName, Version: "v1"},
	}
}

// productionWorkflowRuntime owns every graph-native process collaborator.
// Transports receive only its shared application-service facades.
type productionWorkflowRuntime struct {
	host                *appworkflow.Host
	operations          *appworkflow.WorkflowOperator
	exposure            *appworkflow.WorkflowExposureService
	lifecycle           *appworkflow.WorkflowLifecycleService
	a2a                 *a2a.Handler
	card                *agentcard.Builder
	auth                api.WorkflowRequestAuthenticator
	catalog             *productionWorkflowCatalog
	activationStore     hoststate.ActivationStore
	sourceActivations   *appworkflow.SourceActivationLifecycle
	workers             *workflowWorkers
	activation          *workflowActivationBridge
	externalActivations trigger.ActivationManager
}

func newProductionWorkflowRuntime(store *persistence.Store, cfg *config.Config, workerCount int) (*productionWorkflowRuntime, error) {
	if store == nil || cfg == nil {
		return nil, errors.New("workflow runtime requires store and config")
	}
	if workerCount < 1 {
		workerCount = 1
	}
	workflowRoot := workflowSourceRoot(cfg)
	if err := os.MkdirAll(workflowRoot, 0o750); err != nil {
		return nil, fmt.Errorf("create workflow source root: %w", err)
	}
	state, err := persistence.NewWorkflowStateStore(store)
	if err != nil {
		return nil, err
	}
	journal, err := persistence.NewWorkflowHostStore(store)
	if err != nil {
		return nil, err
	}
	activationStore, err := persistence.NewWorkflowActivationStore(store)
	if err != nil {
		return nil, err
	}
	exposureStore, err := persistence.NewWorkflowExposureStore(store)
	if err != nil {
		return nil, err
	}
	a2aStore, err := persistence.NewWorkflowA2ATaskStore(store)
	if err != nil {
		return nil, err
	}
	catalog, err := openProductionWorkflowCatalog(filepath.Join(cfg.DataDir, "workflow-catalog.json"))
	if err != nil {
		return nil, err
	}
	artifactStore, err := artifacts.New(filepath.Join(cfg.DataDir, "workflow-artifacts"), values.ArtifactAuthorizerFunc(
		func(_ context.Context, request values.ArtifactAuthorization) error {
			if request.Owner == nil || request.Owner.ID == "" {
				return values.ErrArtifactUnauthorized
			}
			return nil
		}), nil)
	if err != nil {
		return nil, err
	}

	waitAuthority := waitadapter.AuthorityResolverFunc(func(ctx context.Context, request waitadapter.AuthorityRequest) (workflowwait.ResponderAuthority, error) {
		return workflowRunAuthority(ctx, journal, workflowruntime.RunID(request.Identity.RunID))
	})
	waitFor, err := waitadapter.NewWaitFor(waitadapter.Options{Authority: waitAuthority})
	if err != nil {
		return nil, err
	}
	messageWait, err := waitadapter.NewMessageWait(waitadapter.Options{Authority: waitAuthority})
	if err != nil {
		return nil, err
	}
	gate, err := gateadapter.New(gateadapter.Options{
		Authority: workflowgate.AuthorityResolverFunc(func(ctx context.Context, request workflowgate.AuthorizationRequest) (workflowwait.ResponderAuthority, error) {
			return workflowRunAuthority(ctx, journal, workflowruntime.RunID(request.RunID))
		}),
		Payloads: workflowgate.PayloadStoreFunc(func(ctx context.Context, request workflowgate.PayloadRequest) (values.ValueSetRef, error) {
			return storeWorkflowGatePayload(ctx, state, request)
		}),
	})
	if err != nil {
		return nil, err
	}
	kinds := []stepkind.StepKind{transform.New(), scriptadapter.New(), waitadapter.NewSleep(nil), waitFor, messageWait, gate}
	compileKinds := stepkind.NewRegistry()
	for _, kind := range kinds {
		if registerErr := compileKinds.Register(kind); registerErr != nil {
			return nil, fmt.Errorf("register production workflow kind: %w", registerErr)
		}
	}
	required := productionWorkflowKindBoundary()
	stager := appworkflow.NewAuthoringSourceStager()
	resolver, err := appworkflow.NewDefinitionResolver(appworkflow.DefinitionResolverOptions{
		Roots: []string{workflowRoot}, FileAuthority: "local", FileTrustClass: "local",
		Registry: catalog, Authoring: stager,
		Authorizer: appworkflow.DefinitionAuthorizerFunc(func(context.Context, appworkflow.DefinitionAuthorization) error { return nil }),
		Compile:    appworkflow.DefinitionCompileOptions{StepKinds: compileKinds, SemanticRevision: "hadrond-production-v1"},
	})
	if err != nil {
		return nil, err
	}

	identity := appworkflow.ContextIdentityProvider{}
	activation := newWorkflowActivationBridge()
	waits := &workflowruntime.WaitCoordinator{
		Store: state, Scheduler: activation,
		Authorizer: workflowResponderAuthorizer{},
	}
	childRuns, err := appworkflow.NewPinnedChildRunMaterializer(appworkflow.ChildRunMaterializerOptions{State: state})
	if err != nil {
		return nil, err
	}
	host, err := appworkflow.New(appworkflow.Options{
		State: state, Journal: journal, Definitions: resolver, Identity: identity,
		Policy: appworkflow.PolicyEvaluatorFunc(productionWorkflowPolicy),
		Kinds:  kinds, RequiredKinds: required, DryRun: productionDryRunSupport{},
		Activations: activation, ActivationStore: activationStore, Waits: waits,
		ReuseAuthorizer: workflowruntime.ReuseAuthorizerFunc(productionReusePolicy),
		ChildRuns:       childRuns, Artifacts: artifactStore,
		RecoveryInterval: 2 * time.Second, RecoveryBatchLimit: workflowRecoveryBatch,
	})
	if err != nil {
		return nil, err
	}
	activation.bindWaits(waits)
	activationService := appworkflow.ActivationService{Host: host, Store: activationStore, CurrentRegistry: catalog, RequireCurrentFence: true}
	scheduleEngine, err := scheduler.NewWorkflowWithService(activationService)
	if err != nil {
		return nil, err
	}
	activation.bindSourceScheduler(scheduleEngine)

	plans := appworkflow.PinnedRecoveryPlanSource{
		Roots: journal, Children: journal, State: state, Replays: state,
		DependencyOptions: resolver.RecoveryDependencyOptions(),
	}
	diagnostics := rundiagnostics.Service{
		State: state, Plans: plans, Control: state, Replay: state, Pins: state,
		Resources: state, Starts: journal,
	}
	replay := &workflowruntime.ReplayService{Store: state, Replay: state, Inputs: state, Control: state, Plans: plans, Registry: host.Registry()}
	operations, err := appworkflow.NewWorkflowOperator(appworkflow.WorkflowOperatorOptions{Host: host, Diagnostics: diagnostics, Replay: replay})
	if err != nil {
		return nil, err
	}
	management := workflowExposureManagement{identity: identity}
	exposure, err := appworkflow.NewWorkflowExposureService(appworkflow.WorkflowExposureOptions{
		Store: exposureStore, Catalog: catalog, Definitions: resolver, StepKinds: host.Registry(), Management: management,
	})
	if err != nil {
		return nil, err
	}
	attestor, err := openWorkflowContractAttestor(filepath.Join(cfg.DataDir, "workflow-contract-attestor.key"))
	if err != nil {
		return nil, err
	}
	contracts, err := appworkflow.NewContractRegistrationService(appworkflow.ContractRegistrationOptions{
		Definitions: resolver, StepKinds: host.Registry(), Catalog: catalog,
		Authorizer: workflowNamespaceAuthorizer{identity: identity}, Attestor: attestor,
		Policy: appworkflow.ContractTestPolicy{MinimumCases: 1, Repetitions: 2, RequireEffectCoverage: true},
	})
	if err != nil {
		return nil, err
	}
	authoring, err := appworkflow.NewAgentAuthoringService(appworkflow.AgentAuthoringOptions{
		Stager: stager, Contracts: contracts,
		HostIdentity: appworkflow.AgentAuthoringHostIdentity{Authority: "hadron", TrustClass: "local", Principal: "operator:local"},
	})
	if err != nil {
		return nil, err
	}
	sourceActivationLifecycle := &appworkflow.SourceActivationLifecycle{Registry: catalog, Activations: activationService}
	lifecycle, err := appworkflow.NewWorkflowLifecycleService(appworkflow.WorkflowLifecycleOptions{
		Identity: identity, Contracts: contracts, Authoring: authoring, Exposure: exposure,
		SourceActivations: sourceActivationLifecycle,
	})
	if err != nil {
		return nil, err
	}
	correlations, err := appworkflow.NewA2ATaskCorrelations(appworkflow.A2ATaskCorrelationsOptions{Host: host, Store: a2aStore})
	if err != nil {
		return nil, err
	}
	a2aHandler, err := a2a.NewHandler(a2a.Options{Correlations: correlations, Workflows: operations, Reads: operations})
	if err != nil {
		return nil, err
	}
	card, err := agentcard.NewBuilder(exposure)
	if err != nil {
		return nil, err
	}
	nonce, err := workflowNonce()
	if err != nil {
		return nil, err
	}
	workers := newWorkflowWorkers(state, plans, journal, journal, state, host.Registry(), host.Dispatcher(), workerCount, nonce)
	auth := &workflowHTTPAuthenticator{exposure: exposure, local: localWorkflowIdentity()}
	return &productionWorkflowRuntime{
		host: host, operations: operations, exposure: exposure, lifecycle: lifecycle, a2a: a2aHandler, card: card,
		auth: auth, catalog: catalog, activationStore: activationStore, workers: workers, activation: activation,
		sourceActivations:   sourceActivationLifecycle,
		externalActivations: trigger.ActivationManager{Service: activationService},
	}, nil
}

const (
	workflowMCPProfileID   = "profile:local-operator"
	workflowMCPPrincipalID = "operator:mcp-local"
)

// BootstrapMCP idempotently binds one operator-supplied token to a durable
// digest-only principal and bounded local profile. It never returns or stores
// the raw credential outside the exposure service's hashing boundary.
func (r *productionWorkflowRuntime) BootstrapMCP(ctx context.Context, token string) error {
	if r == nil || r.exposure == nil {
		return appworkflow.ErrHostNotReady
	}
	if err := hoststate.ValidateMCPToken(token); err != nil {
		return errors.New("MCP workflow token is invalid")
	}
	managementCtx, err := appworkflow.WithAuthenticatedIdentity(ctx, localWorkflowIdentity())
	if err != nil {
		return err
	}
	wantProfile := hoststate.ExposureProfileRecord{
		ID: workflowMCPProfileID, MaxDirectTools: hoststate.DefaultMaximumDirectTools,
		SearchScope: hoststate.ExposureSearchAll, LazyLoad: true,
	}
	_, err = r.exposure.GetProfile(managementCtx, workflowMCPProfileID)
	if errors.Is(err, workflowruntime.ErrNotFound) {
		_, err = r.exposure.PutProfile(managementCtx, wantProfile, 0)
		if errors.Is(err, hoststate.ErrConflict) {
			_, err = r.exposure.GetProfile(managementCtx, workflowMCPProfileID)
		}
	}
	if err != nil {
		return err
	}
	wantPrincipal := workflowMCPPrincipal()
	_, session, resolveErr := r.exposure.ResolveSession(ctx, "bootstrap", token)
	if resolveErr != nil {
		return resolveErr
	}
	if session.Authenticated {
		if !reflect.DeepEqual(session.Principal.Record, wantPrincipal) {
			return errors.New("durable MCP workflow token is bound to a different principal or profile")
		}
		return nil
	}
	_, err = r.exposure.PutPrincipal(managementCtx, appworkflow.PutMCPPrincipalRequest{Record: wantPrincipal, Token: token})
	if errors.Is(err, hoststate.ErrConflict) {
		_, session, resolveErr = r.exposure.ResolveSession(ctx, "bootstrap-replay", token)
		if resolveErr == nil && session.Authenticated && reflect.DeepEqual(session.Principal.Record, wantPrincipal) {
			return nil
		}
		return errors.New("durable MCP workflow principal conflicts with the supplied token")
	}
	return err
}

func workflowMCPPrincipal() hoststate.MCPPrincipalRecord {
	binding := localWorkflowIdentity()
	binding.Principal = workflowMCPPrincipalID
	binding.SourceAuthority = "mcp"
	binding.Extension = map[string]string{"workflow_exposure_profile": workflowMCPProfileID}
	return hoststate.MCPPrincipalRecord{ID: workflowMCPPrincipalID, ProfileID: workflowMCPProfileID, Identity: binding}
}

func (r *productionWorkflowRuntime) Start(ctx context.Context) error {
	if r == nil {
		return errors.New("workflow runtime is nil")
	}
	r.activation.Prepare()
	if err := r.host.Start(ctx); err != nil {
		r.activation.Stop()
		return err
	}
	r.activation.Start()
	r.workers.Start()
	return nil
}

func (r *productionWorkflowRuntime) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	workerErr := r.workers.Stop(ctx)
	if workerErr != nil {
		// Cancellation has already been delivered. Drain without a deadline so
		// no live dispatch can retain a claim while Host or its store closes.
		_ = r.workers.Stop(context.Background())
	}
	r.activation.Stop()
	hostCtx := ctx
	if ctx == nil || ctx.Err() != nil {
		hostCtx = context.Background()
	}
	hostErr := r.host.Shutdown(hostCtx)
	return errors.Join(workerErr, hostErr)
}

type productionDryRunSupport struct{}

func (productionDryRunSupport) SupportsDryRun(_ context.Context, spec stepkind.StepKindSpec) (bool, error) {
	return spec.Name == transform.Name || spec.Name == scriptadapter.Name || spec.Name == waitadapter.SleepName || spec.Name == waitadapter.WaitForName || spec.Name == waitadapter.MessageWaitName || spec.Name == gateadapter.Name, nil
}

func productionWorkflowPolicy(_ context.Context, facts hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
	if err := facts.Validate(); err != nil {
		return invalidWorkflowPolicyFactsDecision()
	}
	if facts.ConfirmationAdvised {
		return hoststate.PolicyDecision{Outcome: hoststate.PolicyConfirm, Reason: "workflow effects require explicit confirmation"}, nil
	}
	return hoststate.PolicyDecision{Outcome: hoststate.PolicyAllow, Reason: "authenticated graph workflow request"}, nil
}

func invalidWorkflowPolicyFactsDecision() (hoststate.PolicyDecision, error) {
	return hoststate.PolicyDecision{Outcome: hoststate.PolicyDeny, Reason: "valid bound workflow policy facts are required"}, nil
}

func productionReusePolicy(_ context.Context, candidate workflowruntime.ReuseCandidate) (workflowruntime.ReusePolicyDecision, error) {
	if candidate.Authority.Principal == "" {
		return workflowruntime.ReusePolicyDecision{Allow: false, Code: "reuse_identity_required", Reason: "pin reuse requires an authenticated authority"}, nil
	}
	return workflowruntime.ReusePolicyDecision{Allow: true, Code: "reuse_authenticated", Reason: "authenticated caller requested exact immutable output reuse"}, nil
}

type workflowResponderAuthorizer struct{}

func (workflowResponderAuthorizer) AuthorizeResume(_ context.Context, request workflowwait.AuthorizationRequest) error {
	if request.Source == workflowwait.WakeTimer && request.Record.Kind == workflowwait.KindTimer && request.Responder.Kind == "system" && request.Responder.Reference == "wait-timer" {
		return nil
	}
	if request.Responder.Reference == "" || request.Record.Authority.Reference == "" || request.Responder.Reference != request.Record.Authority.Reference {
		return appworkflow.ErrPolicyDenied
	}
	if request.Record.Authority.Kind != request.Responder.Kind && (request.Record.Authority.Kind != "principal" || request.Responder.Kind != "principal") {
		return appworkflow.ErrPolicyDenied
	}
	return nil
}

func workflowRunAuthority(ctx context.Context, journal hoststate.Journal, runID workflowruntime.RunID) (workflowwait.ResponderAuthority, error) {
	start, err := journal.LoadStart(ctx, runID)
	if err != nil {
		return workflowwait.ResponderAuthority{}, err
	}
	return workflowwait.ResponderAuthority{Kind: "principal", Reference: start.Record.Identity.Principal}, nil
}

func storeWorkflowGatePayload(ctx context.Context, state workflowruntime.StateStore, request workflowgate.PayloadRequest) (values.ValueSetRef, error) {
	invocation := workflowruntime.NodeInvocationID{RunID: workflowruntime.RunID(request.RunID), NodeID: request.NodeID, Iteration: request.Iteration}
	attempt := workflowruntime.AttemptID{Invocation: invocation, Number: request.Attempt}
	payload, err := values.NewInline(workflowgate.CloneCheckpoint(request.Checkpoint), values.Metadata{
		Producer:  values.Producer{Kind: "human_gate", Reference: request.NodeID, Output: "checkpoint"},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		return values.ValueSetRef{}, err
	}
	return state.SaveValues(ctx, workflowruntime.SaveValuesRequest{
		Owner:  workflowruntime.ValueOwner{Kind: "gate-payload", RunID: invocation.RunID, Invocation: &invocation, Attempt: &attempt},
		Values: values.ValueSet{"checkpoint": payload},
	})
}

func localWorkflowIdentity() hoststate.IdentityBinding {
	checkedAt := time.Unix(1, 0).UTC()
	capabilities := []string{gateadapter.CapabilityGate, waitadapter.CapabilityMessage, waitadapter.CapabilityWait}
	sort.Strings(capabilities)
	target := hoststate.ExecutionTarget{
		Version: hoststate.ScopeTargetVersionV1, ID: "local", Kind: hoststate.ExecutionTargetLocal,
		Capabilities: capabilities, Sandbox: hoststate.SandboxPolicy{Mode: hoststate.SandboxHostDefault},
		Readiness:  hoststate.TargetReadiness{State: hoststate.TargetReady, CheckedAt: checkedAt},
		Provenance: hoststate.TargetProvenance{Authority: "hadron", Reference: "local-runtime", Revision: "v1"},
	}
	return hoststate.IdentityBinding{
		Principal: "operator:local", SourceAuthority: "http", Trust: "local",
		Grants:          []string{"workflow.manage", "workflow.run"},
		RunScope:        hoststate.RunScope{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeProject, ID: "local"},
		ExecutionTarget: &target,
	}
}

func hasWorkflowGrant(binding hoststate.IdentityBinding, grant string) bool {
	for _, current := range binding.Grants {
		if current == grant {
			return true
		}
	}
	return false
}

type workflowNamespaceAuthorizer struct{ identity appworkflow.IdentityProvider }

func (a workflowNamespaceAuthorizer) AuthorizeNamespace(ctx context.Context, request appworkflow.NamespaceAuthorization) error {
	binding, err := a.identity.BindIdentity(ctx, appworkflow.IdentityRequest{})
	if err != nil || binding.Principal != request.Principal || !hasWorkflowGrant(binding, "workflow.manage") {
		return appworkflow.ErrPolicyDenied
	}
	return nil
}

type workflowExposureManagement struct{ identity appworkflow.IdentityProvider }

func (a workflowExposureManagement) AuthorizeExposureManagement(ctx context.Context, _ appworkflow.ExposureManagementAuthorization) error {
	binding, err := a.identity.BindIdentity(ctx, appworkflow.IdentityRequest{})
	if err != nil || !hasWorkflowGrant(binding, "workflow.manage") {
		return appworkflow.ErrPolicyDenied
	}
	return nil
}

type workflowHTTPAuthenticator struct {
	exposure *appworkflow.WorkflowExposureService
	local    hoststate.IdentityBinding
}

func (a *workflowHTTPAuthenticator) AuthenticateWorkflowRequest(request *http.Request, intent appworkflow.WorkflowAccessIntent) (context.Context, error) {
	if request == nil || a == nil || a.exposure == nil {
		return nil, appworkflow.ErrWorkflowUnauthenticated
	}
	if !sameOriginRequest(request) {
		return nil, appworkflow.ErrPolicyDenied
	}
	authorization := request.Header.Get("Authorization")
	if authorization == "" {
		if !loopbackRemote(request.RemoteAddr) || !loopbackHost(request.Host) {
			return nil, appworkflow.ErrWorkflowUnauthenticated
		}
		return appworkflow.WithAuthenticatedIdentity(request.Context(), a.local)
	}
	if strings.Count(authorization, " ") != 1 || !strings.HasPrefix(authorization, "Bearer ") {
		return nil, appworkflow.ErrWorkflowUnauthenticated
	}
	token := strings.TrimPrefix(authorization, "Bearer ")
	if token == "" || strings.TrimSpace(token) != token {
		return nil, appworkflow.ErrWorkflowUnauthenticated
	}
	ctx, session, err := a.exposure.ResolveSession(request.Context(), "http", token)
	if err != nil || !session.Authenticated {
		return nil, appworkflow.ErrWorkflowUnauthenticated
	}
	if lifecycleManagementOperation(intent.Operation) && !hasWorkflowGrant(session.Principal.Record.Identity, "workflow.manage") {
		return nil, appworkflow.ErrPolicyDenied
	}
	if intent.Definition != nil {
		if _, err := a.exposure.Describe(ctx, session, *intent.Definition, string(intent.Operation)); err != nil {
			return nil, err
		}
	}
	if intent.Display != nil {
		if _, err := a.exposure.DisplayPolicy(ctx, session, *intent.Display); err != nil {
			return nil, err
		}
	}
	binding := session.Principal.Record.Identity.Clone()
	binding.SourceAuthority = "http"
	return appworkflow.WithAuthenticatedIdentity(ctx, binding)
}

func lifecycleManagementOperation(operation appworkflow.WorkflowAccessOperation) bool {
	return strings.HasPrefix(string(operation), "author_") || strings.HasPrefix(string(operation), "registry_") || strings.HasPrefix(string(operation), "exposure_") || operation == appworkflow.WorkflowAccessCatalogSearch || operation == appworkflow.WorkflowAccessCatalogInspect
}

func sameOriginRequest(request *http.Request) bool {
	origin := request.Header.Get("Origin")
	if origin == "" {
		return true
	}
	parsed, err := url.Parse(origin)
	expectedScheme := "http"
	if request.TLS != nil {
		expectedScheme = "https"
	}
	return err == nil && parsed.Scheme == expectedScheme && strings.EqualFold(parsed.Host, request.Host) && parsed.User == nil && parsed.RawQuery == "" && parsed.Fragment == ""
}

func loopbackRemote(remote string) bool {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

func loopbackHost(authority string) bool {
	host := authority
	if parsed, _, err := net.SplitHostPort(authority); err == nil {
		host = parsed
	}
	host = strings.Trim(strings.ToLower(host), "[]")
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

type workflowContractAttestor struct{ key []byte }

func openWorkflowContractAttestor(path string) (*workflowContractAttestor, error) {
	lock, err := openPrivateWorkflowLock(path+".lock", "workflow contract attestor")
	if err != nil {
		return nil, err
	}
	defer func() { _ = lock.Close() }()
	if lockErr := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); lockErr != nil { // #nosec G115 -- an open file descriptor is representable by the platform syscall API.
		return nil, lockErr
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }() // #nosec G115 -- same validated open file descriptor.

	key, err := readWorkflowContractAttestorKey(path)
	if errors.Is(err, os.ErrNotExist) {
		candidate := make([]byte, 32)
		if _, randomErr := rand.Read(candidate); randomErr != nil {
			return nil, randomErr
		}
		file, openErr := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600) // #nosec G304 -- host-owned config path.
		if openErr != nil {
			if errors.Is(openErr, os.ErrExist) {
				winner, readErr := readWorkflowContractAttestorKey(path)
				if readErr != nil {
					return nil, readErr
				}
				key = winner
			} else {
				return nil, openErr
			}
		} else {
			created, statErr := file.Stat()
			published, lstatErr := os.Lstat(path)
			if statErr != nil || lstatErr != nil || !created.Mode().IsRegular() || created.Mode().Perm()&0o077 != 0 || published.Mode()&os.ModeSymlink != 0 || !os.SameFile(created, published) {
				_ = file.Close()
				removeWorkflowContractAttestorKey(path, created)
				return nil, errors.New("workflow contract attestor key publication is unsafe")
			}
			_, writeErr := file.Write(candidate)
			syncErr := file.Sync()
			closeErr := file.Close()
			if writeErr != nil || syncErr != nil || closeErr != nil {
				removeWorkflowContractAttestorKey(path, created)
				return nil, errors.Join(writeErr, syncErr, closeErr)
			}
			key, err = readWorkflowContractAttestorKey(path)
			if err != nil {
				return nil, err
			}
		}
	} else if err != nil {
		return nil, err
	}
	if len(key) != 32 {
		return nil, errors.New("workflow contract attestor key is invalid")
	}
	// The key authenticates durable qualification evidence. Sync its directory
	// on every accepted open so both a new publication and a prior publication
	// whose directory sync was interrupted become durable before use.
	if err := syncWorkflowParentDirectory(path, "workflow contract attestor"); err != nil {
		return nil, err
	}
	return &workflowContractAttestor{key: append([]byte(nil), key...)}, nil
}

func syncWorkflowParentDirectory(path, label string) error {
	directory, err := os.Open(filepath.Dir(path)) // #nosec G304 -- directory derives from the explicitly configured host path.
	if err != nil {
		return fmt.Errorf("open %s directory: %w", label, err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil || closeErr != nil {
		return fmt.Errorf("sync %s directory: %w", label, errors.Join(syncErr, closeErr))
	}
	return nil
}

func removeWorkflowContractAttestorKey(path string, created os.FileInfo) {
	published, err := os.Lstat(path)
	if err == nil && created != nil && os.SameFile(created, published) {
		_ = os.Remove(path)
	}
}

func readWorkflowContractAttestorKey(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 || info.Size() != 32 {
		return nil, errors.New("workflow contract attestor key must be a private 32-byte regular file")
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0) // #nosec G304 -- host-owned config path validated above and again after open.
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) || opened.Size() != 32 || opened.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("workflow contract attestor key changed or is not private")
	}
	key := make([]byte, 32)
	if _, err := io.ReadFull(file, key); err != nil {
		return nil, err
	}
	var trailing [1]byte
	if count, err := file.Read(trailing[:]); count != 0 || !errors.Is(err, io.EOF) {
		return nil, errors.New("workflow contract attestor key exceeds 32 bytes")
	}
	return key, nil
}

func (a *workflowContractAttestor) AttestContractReport(_ context.Context, digest string) (string, error) {
	mac := hmac.New(sha256.New, a.key)
	_, _ = mac.Write([]byte(digest))
	return "sha256:" + hex.EncodeToString(mac.Sum(nil)), nil
}

func (a *workflowContractAttestor) VerifyContractReport(ctx context.Context, digest, attestation string) error {
	expected, err := a.AttestContractReport(ctx, digest)
	if err != nil || !hmac.Equal([]byte(expected), []byte(attestation)) {
		return errors.New("workflow contract report attestation is invalid")
	}
	return nil
}

func workflowNonce() (string, error) {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return hex.EncodeToString(buffer), nil
}

// workflowActivationBridge is bound before Host.Start. Source schedules use
// scheduler.NewWorkflow; wait wake/deadline activations use process timers and
// are reconstructed from durable waits by Host recovery after restart.
type workflowActivationBridge struct {
	mu                  sync.Mutex
	waits               *workflowruntime.WaitCoordinator
	source              *scheduler.Engine
	timers              map[workflowwait.ActivationID]*workflowActivationTimer
	accepting           bool
	dispatchReady       bool
	sourceStarted       bool
	stopping            bool
	timerWG             sync.WaitGroup
	beforeTimerDispatch func()
}

type workflowActivationTimer struct {
	timer *time.Timer
}

func newWorkflowActivationBridge() *workflowActivationBridge {
	return &workflowActivationBridge{timers: make(map[workflowwait.ActivationID]*workflowActivationTimer)}
}

func (b *workflowActivationBridge) bindWaits(waits *workflowruntime.WaitCoordinator) { b.waits = waits }
func (b *workflowActivationBridge) bindSourceScheduler(source *scheduler.Engine)     { b.source = source }

// Prepare accepts recovery schedules before Host.Start. Due timers remain
// fenced until Start marks the recovered Host ready, so startup cannot drop a
// reconstructed wait or race its durable recovery transition.
func (b *workflowActivationBridge) Prepare() {
	b.mu.Lock()
	if !b.stopping {
		b.accepting = true
	}
	b.mu.Unlock()
}

func (b *workflowActivationBridge) Start() {
	b.mu.Lock()
	if b.stopping {
		b.mu.Unlock()
		return
	}
	b.accepting, b.dispatchReady = true, true
	source := b.source
	startSource := source != nil && !b.sourceStarted
	b.sourceStarted = b.sourceStarted || startSource
	b.mu.Unlock()
	if startSource {
		source.Start()
	}
}

func (b *workflowActivationBridge) Stop() {
	b.mu.Lock()
	b.stopping = true
	b.accepting, b.dispatchReady = false, false
	for id, timer := range b.timers {
		if timer.timer.Stop() {
			b.timerWG.Done()
		}
		delete(b.timers, id)
	}
	source := b.source
	stopSource := b.sourceStarted
	b.sourceStarted = false
	b.mu.Unlock()
	if source != nil && stopSource {
		source.Stop()
	}
	b.timerWG.Wait()
}

func (b *workflowActivationBridge) Schedule(ctx context.Context, activation workflowwait.Activation) error {
	if ctx == nil {
		return appworkflow.ErrHostNotReady
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := activation.Validate(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.waits == nil || b.source == nil || !b.accepting {
		return appworkflow.ErrHostNotReady
	}
	if _, exists := b.timers[activation.ID]; exists {
		return nil
	}
	b.armLocked(activation, time.Until(activation.FireAt))
	return nil
}

func (b *workflowActivationBridge) Cancel(_ context.Context, id workflowwait.ActivationID) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if timer := b.timers[id]; timer != nil {
		if timer.timer.Stop() {
			b.timerWG.Done()
		}
		delete(b.timers, id)
	}
	return nil
}

func (b *workflowActivationBridge) fire(activation workflowwait.Activation, timer *workflowActivationTimer) {
	b.mu.Lock()
	if b.timers[activation.ID] != timer {
		b.mu.Unlock()
		return
	}
	if !b.accepting {
		delete(b.timers, activation.ID)
		b.mu.Unlock()
		return
	}
	if !b.dispatchReady {
		b.armLocked(activation, workflowIdlePoll)
		b.mu.Unlock()
		return
	}
	delete(b.timers, activation.ID)
	waits := b.waits
	beforeDispatch := b.beforeTimerDispatch
	b.mu.Unlock()
	if beforeDispatch != nil {
		beforeDispatch()
	}
	if waits == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if activation.Kind == workflowruntime.ActivationWaitWake {
		_, _ = waits.WakeTimer(ctx, workflowruntime.TimerWakeCommand{WaitID: workflowruntime.WaitID(activation.WaitID), FiredAt: time.Now().UTC()})
		return
	}
	_, _ = waits.Recover(ctx, workflowruntime.OpenWaitQuery{RunID: workflowruntime.RunID(activation.RunID), Limit: workflowRecoveryBatch}, time.Now().UTC())
}

func (b *workflowActivationBridge) armLocked(activation workflowwait.Activation, delay time.Duration) {
	if delay < 0 {
		delay = 0
	}
	b.timerWG.Add(1)
	timer := &workflowActivationTimer{}
	b.timers[activation.ID] = timer
	timer.timer = time.AfterFunc(delay, func() {
		defer b.timerWG.Done()
		b.fire(activation, timer)
	})
}

type workflowWorkers struct {
	state      workflowruntime.StateStore
	plans      workflowruntime.RecoveryPlanSource
	roots      hoststate.Journal
	children   appworkflow.ChildRunDefinitionSource
	replays    workflowruntime.ReplayStore
	kinds      stepkind.Registry
	dispatcher *workflowruntime.StepDispatcher
	queue      *workflowruntime.ReadyQueueCoordinator
	count      int
	nonce      string
	sequence   atomic.Uint64
	mu         sync.Mutex
	cancel     context.CancelFunc
	done       chan struct{}
	wg         sync.WaitGroup
}

func newWorkflowWorkers(state workflowruntime.StateStore, plans workflowruntime.RecoveryPlanSource, roots hoststate.Journal, children appworkflow.ChildRunDefinitionSource, replays workflowruntime.ReplayStore, kinds stepkind.Registry, dispatcher *workflowruntime.StepDispatcher, count int, nonce string) *workflowWorkers {
	workers := &workflowWorkers{state: state, plans: plans, roots: roots, children: children, replays: replays, kinds: kinds, dispatcher: dispatcher, count: count, nonce: nonce}
	admission := productionSchedulerAdmission{state: state, plans: plans, kinds: kinds, workers: count}
	workers.queue = workflowruntime.NewResourceReadyQueueCoordinator(state, nil, admission)
	return workers
}

type productionSchedulerAdmission struct {
	state   workflowruntime.StateStore
	plans   workflowruntime.RecoveryPlanSource
	kinds   stepkind.Registry
	workers int
}

func (a productionSchedulerAdmission) Requirements(ctx context.Context, candidate workflowruntime.ReadyCandidate) ([]workflowruntime.SchedulerResourceRequirement, error) {
	run, err := a.state.LoadRun(ctx, candidate.InvocationID.RunID)
	if err != nil {
		return nil, err
	}
	pinned, err := a.plans.LoadRecoveryPlan(ctx, run)
	if err != nil {
		return nil, err
	}
	var node *graph.Node
	for index := range pinned.Plan.Graph.Nodes {
		if pinned.Plan.Graph.Nodes[index].ID == candidate.InvocationID.NodeID {
			node = &pinned.Plan.Graph.Nodes[index]
			break
		}
	}
	if node == nil {
		return nil, fmt.Errorf("pinned workflow node %q is unavailable", candidate.InvocationID.NodeID)
	}
	_, spec, err := stepkind.Resolve(a.kinds, node.Kind, node.KindVersion)
	if err != nil {
		return nil, err
	}
	effects := append(graph.EffectSet(nil), spec.Effects...)
	effects = append(effects, node.Effects...)
	capabilities := append([]string(nil), spec.RequiredCapabilities...)
	capabilities = append(capabilities, pinned.Plan.Graph.Target.Capabilities...)
	capabilities = append(capabilities, node.Target.Capabilities...)
	named := make(map[string]int, len(pinned.Plan.Graph.Concurrency.Resources))
	for _, resource := range pinned.Plan.Graph.Concurrency.Resources {
		named[resource.Name] = resource.Limit
	}
	return workflowruntime.BuildSchedulerRequirements(candidate.InvocationID.RunID, workflowruntime.SchedulerLimits{
		Workers: a.workers, PerRun: pinned.Plan.Graph.Concurrency.MaxRun, Named: named,
	}, workflowruntime.SchedulerDemand{Effects: effects, Capabilities: capabilities, Concurrency: append([]graph.ConcurrencyClaim(nil), node.Concurrency...)})
}

func (w *workflowWorkers) Start() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel, w.done = cancel, make(chan struct{})
	for index := 0; index < w.count; index++ {
		w.wg.Add(1)
		go w.loop(ctx, index)
	}
	go func() { w.wg.Wait(); close(w.done) }()
}

func (w *workflowWorkers) Stop(ctx context.Context) error {
	if ctx == nil {
		return errors.New("workflow worker stop requires context")
	}
	w.mu.Lock()
	cancel, done := w.cancel, w.done
	w.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		w.mu.Lock()
		if w.done == done {
			w.cancel, w.done = nil, nil
		}
		w.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *workflowWorkers) loop(ctx context.Context, index int) {
	defer w.wg.Done()
	owner := fmt.Sprintf("hadrond-%s-%d", w.nonce, index)
	for ctx.Err() == nil {
		sequence := w.sequence.Add(1)
		now := time.Now().UTC()
		token := fmt.Sprintf("claim-%s-%d-%d", w.nonce, index, sequence)
		claim, acquired, err := w.queue.ClaimNext(ctx, workflowruntime.ReadyClaimRequest{
			Owner: owner, Token: token, IdempotencyKey: token, Now: now, LeaseUntil: now.Add(workflowClaimLease),
		})
		if err != nil || !acquired {
			if !waitWorkflowPoll(ctx) {
				return
			}
			continue
		}
		w.dispatch(ctx, claim)
	}
}

func (w *workflowWorkers) dispatch(parent context.Context, claim workflowruntime.ReadyClaim) {
	run, err := w.state.LoadRun(parent, claim.Candidate.InvocationID.RunID)
	if err != nil {
		w.release(claim)
		return
	}
	pinned, err := w.plans.LoadRecoveryPlan(parent, run)
	if err != nil {
		w.release(claim)
		return
	}
	var definition *graph.Node
	for index := range pinned.Plan.Graph.Nodes {
		if pinned.Plan.Graph.Nodes[index].ID == claim.Candidate.InvocationID.NodeID {
			definition = &pinned.Plan.Graph.Nodes[index]
			break
		}
	}
	if definition == nil {
		w.release(claim)
		return
	}
	identity, err := w.loadRunIdentity(parent, run.ID, make(map[workflowruntime.RunID]struct{}), 0)
	if err != nil {
		w.release(claim)
		return
	}
	target := ""
	if identity.ExecutionTarget != nil {
		target = identity.ExecutionTarget.ID
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	renewDone := make(chan error, 1)
	go w.renew(ctx, cancel, claim, renewDone)
	dispatchKey := workflowDispatchIdempotencyKey(claim.Candidate.InvocationID)
	_, dispatchErr := w.dispatcher.Dispatch(ctx, workflowruntime.DispatchRequest{Claim: claim, Node: *definition, IdempotencyKey: dispatchKey, Target: target})
	cancel()
	renewErr := <-renewDone
	if dispatchErr != nil || renewErr != nil {
		w.release(claim)
	}
}

func (w *workflowWorkers) loadRunIdentity(ctx context.Context, runID workflowruntime.RunID, seen map[workflowruntime.RunID]struct{}, depth int) (hoststate.IdentityBinding, error) {
	if depth > 64 {
		return hoststate.IdentityBinding{}, errors.New("workflow run identity lineage exceeds 64")
	}
	if _, duplicate := seen[runID]; duplicate {
		return hoststate.IdentityBinding{}, errors.New("workflow run identity lineage contains a cycle")
	}
	seen[runID] = struct{}{}
	defer delete(seen, runID)
	start, err := w.roots.LoadStart(ctx, runID)
	if err == nil {
		return start.Record.Identity.Clone(), nil
	}
	if !errors.Is(err, workflowruntime.ErrNotFound) {
		return hoststate.IdentityBinding{}, err
	}
	child, childErr := w.children.LoadChildRunRequest(ctx, runID)
	if childErr == nil {
		return w.loadRunIdentity(ctx, workflowruntime.RunID(child.Parent.RunID), seen, depth+1)
	}
	if !errors.Is(childErr, workflowruntime.ErrNotFound) {
		return hoststate.IdentityBinding{}, childErr
	}
	replay, replayErr := w.replays.LoadReplayProvenance(ctx, runID)
	if replayErr != nil {
		return hoststate.IdentityBinding{}, replayErr
	}
	return w.loadRunIdentity(ctx, replay.SourceRunID, seen, depth+1)
}

func workflowDispatchIdempotencyKey(invocation workflowruntime.NodeInvocationID) string {
	identity := string(invocation.RunID) + "\x00" + invocation.NodeID + "\x00" + invocation.Iteration
	return "dispatch-" + values.SHA256Digest([]byte(identity))[7:39]
}

func (w *workflowWorkers) renew(ctx context.Context, cancel context.CancelFunc, claim workflowruntime.ReadyClaim, done chan<- error) {
	ticker := time.NewTicker(workflowClaimRenew)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			done <- nil
			return
		case now := <-ticker.C:
			lease, err := w.queue.Renew(ctx, workflowruntime.RenewLeaseRequest{
				InvocationID: claim.Candidate.InvocationID, Owner: claim.Lease.Owner, Token: claim.Lease.Token,
				Generation: claim.Lease.Generation, Now: now.UTC(), LeaseUntil: now.UTC().Add(workflowClaimLease),
			})
			if err != nil {
				cancel()
				done <- err
				return
			}
			claim.Lease = lease
		}
	}
}

func (w *workflowWorkers) release(claim workflowruntime.ReadyClaim) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = w.queue.Release(ctx, workflowruntime.ReleaseClaimRequest{
		InvocationID: claim.Candidate.InvocationID, Owner: claim.Lease.Owner, Token: claim.Lease.Token,
		Generation: claim.Lease.Generation, Now: time.Now().UTC(),
	})
}

func waitWorkflowPoll(ctx context.Context) bool {
	timer := time.NewTimer(workflowIdlePoll)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
