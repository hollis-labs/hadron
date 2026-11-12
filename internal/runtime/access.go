package runtime

import (
	"context"
	"errors"
	"slices"
	"sort"
	"strings"
	"sync"

	libauth "github.com/hollis-labs/libs/security/auth"
	libpolicy "github.com/hollis-labs/libs/security/policy"
	"github.com/hollis-labs/libs/workflow/engine/graph"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/registry"
)

// Which workflows a caller may use.
//
// A verified principal that holds hadron.manage administers the catalog and is
// unrestricted. Every other caller is bound by an exposure profile: the profile
// id is a claim the token carries as a scope, hadron.profile.<id>. A caller
// with no profile claim gets the default profile, which exposes nothing, and a
// caller with two profile claims is treated the same (an ambiguous claim never
// widens access). The profile pins exact definitions and may grant a search
// scope (public, a set of namespaces, or all); see hoststate.ExposureProfileRecord.
//
// Enforcement is a policy decision (action ActionResolve, resource type
// ResourceWorkflow) so the policy module's mode (shadow or enforce) and audit
// apply: in shadow mode a denial is logged as would-block and allowed.
//
// Scopes are the claims a Tether token carries today. If the token grows real
// claims, ProfileClaim is the one place to read them.
const (
	// ProfileScopePrefix prefixes the scope that names a caller's exposure profile.
	ProfileScopePrefix = "hadron.profile."
	// ActionResolve is the policy action for using an exact workflow definition.
	ActionResolve = "workflow.resolve"
	// ResourceWorkflow is the policy resource type of a workflow definition.
	ResourceWorkflow = "workflow"
)

// Subject is who a request acts for, as far as workflow access goes.
type Subject struct {
	Principal    string
	Kind         string
	ProfileID    string
	Unrestricted bool
}

// ProfileClaim returns the exposure profile the principal's token names. ok is
// false when it names none or more than one.
func ProfileClaim(p libauth.Principal) (id string, ok bool) {
	for _, s := range p.Scopes {
		if v, found := strings.CutPrefix(s, ProfileScopePrefix); found && v != "" {
			if id != "" && id != v {
				return "", false
			}
			id = v
		}
	}
	return id, id != ""
}

// SubjectFromPrincipal derives the access subject of a verified principal.
func SubjectFromPrincipal(p libauth.Principal) Subject {
	s := Subject{Principal: p.ID, Kind: p.Kind, Unrestricted: p.HasScope(ScopeManage)}
	if id, ok := ProfileClaim(p); ok {
		s.ProfileID = id
	}
	return s
}

// localSubject is the off-dial local operator: unrestricted.
func localSubject() Subject {
	return Subject{Principal: localOperatorPrincipal, Kind: "user", Unrestricted: true}
}

type subjectKey struct{}

func withSubject(ctx context.Context, s Subject) context.Context {
	return context.WithValue(ctx, subjectKey{}, s)
}

// SubjectFrom returns the subject Authenticate attached to ctx.
func SubjectFrom(ctx context.Context) (Subject, bool) {
	s, ok := ctx.Value(subjectKey{}).(Subject)
	return s, ok
}

// AccessChecker decides whether the subject may use the exact definition. A
// nil error allows it; appworkflow.ErrWorkflowHidden denies it.
type AccessChecker func(ctx context.Context, s Subject, ref graph.DefinitionRef) error

// WithAccess replaces the default checker (which evaluates the exposure profile
// directly) so a host can route the decision through its policy engine.
func WithAccess(c AccessChecker) Option { return func(o *runtimeOptions) { o.access = c } }

// WithAccessPDP binds the policy decision point the host registered with the
// policy module to this runtime's exposure profiles. Create it with
// NewAccessPDP before the policy module starts.
func WithAccessPDP(p *AccessPDP) Option { return func(o *runtimeOptions) { o.pdp = p } }

// accessGate connects the definition resolver to the exposure profiles. It is
// built before the exposure service exists and bound afterwards.
type accessGate struct {
	mu       sync.RWMutex
	exposure *appworkflow.WorkflowExposureService
	custom   AccessChecker
}

func (g *accessGate) bind(e *appworkflow.WorkflowExposureService) {
	g.mu.Lock()
	g.exposure = e
	g.mu.Unlock()
}

func (g *accessGate) service() (*appworkflow.WorkflowExposureService, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.exposure == nil {
		return nil, appworkflow.ErrHostNotReady
	}
	return g.exposure, nil
}

// authorize is the definition resolver's authorizer. A context with no subject
// is the system's own (recovery, workers, activations): the run it continues
// was authorized when it started.
func (g *accessGate) authorize(ctx context.Context, a appworkflow.DefinitionAuthorization) error {
	s, ok := SubjectFrom(ctx)
	if !ok || s.Unrestricted || a.Stage != appworkflow.AuthorizationResolved || a.Container != nil || a.Resolved == nil {
		return nil
	}
	// The profile pins registry definitions. The resolver reports the source
	// identity it resolved (kind workflow, the unqualified id), so the registry
	// identity is the requested name with the resolved version and digest. Any
	// other way to name a definition (a file, a bare digest) is outside a
	// profile and hidden.
	if a.Requested.Kind != appworkflow.DefinitionKindRegistry {
		return appworkflow.ErrWorkflowHidden
	}
	ref := graph.DefinitionRef{Kind: appworkflow.DefinitionKindRegistry, ID: a.Requested.ID, Version: a.Resolved.Version, Digest: a.Resolved.Digest}
	if g.custom != nil {
		return g.custom(ctx, s, ref)
	}
	return g.direct(ctx, s, ref)
}

// deniedEffect returns an effect of the start that the caller's profile denies,
// or "". Administrators, system contexts and starts with no profile effects to
// deny are never affected. The profile is read per start, so a change applies at once.
func (g *accessGate) deniedEffect(ctx context.Context, effects graph.EffectSet) string {
	s, ok := SubjectFrom(ctx)
	if !ok || s.Unrestricted || len(effects) == 0 {
		return ""
	}
	e, err := g.service()
	if err != nil {
		return ""
	}
	session, err := e.ResolveProfile(ctx, s.ProfileID)
	if err != nil {
		return ""
	}
	for _, denied := range session.Profile.DeniedEffects {
		if slices.Contains(effects, denied) {
			return string(denied)
		}
	}
	return ""
}

// direct evaluates the subject's profile without the policy engine.
func (g *accessGate) direct(ctx context.Context, s Subject, ref graph.DefinitionRef) error {
	e, err := g.service()
	if err != nil {
		return err
	}
	return g.allowed(ctx, e, s.ProfileID, ref)
}

func (g *accessGate) allowed(ctx context.Context, e *appworkflow.WorkflowExposureService, profileID string, ref graph.DefinitionRef) error {
	session, err := e.ResolveProfile(ctx, profileID)
	if err != nil {
		return appworkflow.ErrWorkflowHidden
	}
	return e.AuthorizeDefinition(ctx, session, ref)
}

// AccessPDP is the policy decision point for workflow access. Register it with
// policy.UsePDP before boot and hand the same value to New (WithAccessPDP).
type AccessPDP struct {
	mu   sync.RWMutex
	gate *accessGate
}

// NewAccessPDP returns a decision point that denies everything until bound.
func NewAccessPDP() *AccessPDP { return &AccessPDP{} }

func (p *AccessPDP) bind(g *accessGate) {
	p.mu.Lock()
	p.gate = g
	p.mu.Unlock()
}

// Evaluate allows ActionResolve on an exact registry definition the caller's
// profile (Context["profile"]) covers. Every other request is denied; an
// unavailable profile store is an error, which the policy module fails closed.
func (p *AccessPDP) Evaluate(ctx context.Context, req libpolicy.Request) (libpolicy.Decision, error) {
	deny := func(reason string) (libpolicy.Decision, error) {
		return libpolicy.Decision{Effect: libpolicy.Deny, Reason: reason}, nil
	}
	if req.Action != ActionResolve || req.Resource.Type != ResourceWorkflow {
		return deny("not a workflow access request")
	}
	p.mu.RLock()
	g := p.gate
	p.mu.RUnlock()
	if g == nil {
		return libpolicy.Decision{}, errors.New("workflow access policy is not bound to a runtime")
	}
	e, err := g.service()
	if err != nil {
		return libpolicy.Decision{}, err
	}
	str := func(k string) string { v, _ := req.Context[k].(string); return v }
	ref := graph.DefinitionRef{Kind: str("kind"), ID: str("name"), Version: str("version"), Digest: str("digest")}
	switch err := g.allowed(ctx, e, str("profile"), ref); {
	case err == nil:
		return libpolicy.Decision{Effect: libpolicy.Allow, Reason: "covered by the caller's exposure profile"}, nil
	case errors.Is(err, appworkflow.ErrWorkflowHidden):
		return deny("not covered by the caller's exposure profile")
	default:
		return libpolicy.Decision{}, err
	}
}

// PolicyRequest builds the policy request for a subject using a definition, the
// shape AccessPDP evaluates. Hosts that route access through the policy module
// call Evaluate with it from their AccessChecker.
func PolicyRequest(s Subject, ref graph.DefinitionRef) libpolicy.Request {
	kind := s.Kind
	if kind == "" {
		kind = "user"
	}
	urn := s.Principal
	if strings.ContainsAny(urn, " \t\r\n") || !strings.Contains(urn, ":") {
		urn = "principal:" + strings.Map(func(r rune) rune {
			if r == ' ' || r == '\t' || r == '\r' || r == '\n' {
				return '_'
			}
			return r
		}, urn)
	}
	return libpolicy.Request{
		Principal: libpolicy.Principal{URN: urn, Kind: kind},
		Action:    ActionResolve,
		Resource:  libpolicy.Resource{Type: ResourceWorkflow, ID: ref.ID + "@" + ref.Version + "#" + ref.Digest},
		Scope:     "hadron",
		Context:   map[string]any{"profile": s.ProfileID, "kind": ref.Kind, "name": ref.ID, "version": ref.Version, "digest": ref.Digest},
	}
}

// VisibleWorkflows lists the workflows the caller in ctx may see as tools or
// agent-card skills: every published workflow for an unrestricted caller, and
// for others the ones their profile pins.
func (r *Runtime) VisibleWorkflows(ctx context.Context) ([]appworkflow.WorkflowExposureDescriptor, error) {
	s, ok := SubjectFrom(ctx)
	if !ok || s.Unrestricted {
		return r.exposure.PublishedWorkflows(ctx)
	}
	session, err := r.exposure.ResolveProfile(ctx, s.ProfileID)
	if err != nil {
		return []appworkflow.WorkflowExposureDescriptor{}, nil
	}
	return r.exposure.DirectWorkflows(ctx, session)
}

// visibleSource adapts VisibleWorkflows to the agent card builder.
type visibleSource struct {
	mu sync.RWMutex
	r  *Runtime
}

func (v *visibleSource) bind(r *Runtime) { v.mu.Lock(); v.r = r; v.mu.Unlock() }

func (v *visibleSource) PublishedWorkflows(ctx context.Context) ([]appworkflow.WorkflowExposureDescriptor, error) {
	v.mu.RLock()
	r := v.r
	v.mu.RUnlock()
	if r == nil {
		return nil, appworkflow.ErrHostNotReady
	}
	return r.VisibleWorkflows(ctx)
}

// ExposedWorkflows lists every workflow some caller may be shown as a typed
// tool: the published ones and whatever any profile pins, one descriptor per
// tool name. Where several versions share a tool name the catalog's current
// version wins (a profile that pins another version still resolves its own at
// call time, see Runtime.VisibleWorkflows). It runs as the system, not as a
// caller.
func (r *Runtime) ExposedWorkflows(ctx context.Context) ([]appworkflow.WorkflowExposureDescriptor, error) {
	sys, err := r.systemContext(ctx)
	if err != nil {
		return nil, err
	}
	all, err := r.exposure.PublishedWorkflows(sys)
	if err != nil {
		return nil, err
	}
	profiles, err := r.exposure.ListProfiles(sys, 1000)
	if err != nil {
		return nil, err
	}
	for _, p := range profiles {
		session, err := r.exposure.ResolveProfile(sys, p.Record.ID)
		if err != nil {
			continue
		}
		pinned, err := r.exposure.DirectWorkflows(sys, session)
		if err != nil {
			continue // a profile that cannot be described must not hide the others
		}
		all = append(all, pinned...)
	}
	byTool := map[string]appworkflow.WorkflowExposureDescriptor{}
	for _, d := range all {
		prior, seen := byTool[d.ToolName]
		if !seen || r.isCurrent(sys, d) && !r.isCurrent(sys, prior) {
			byTool[d.ToolName] = d
		}
	}
	out := make([]appworkflow.WorkflowExposureDescriptor, 0, len(byTool))
	for _, d := range byTool {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ToolName < out[j].ToolName })
	return out, nil
}

func (r *Runtime) isCurrent(ctx context.Context, d appworkflow.WorkflowExposureDescriptor) bool {
	cur, err := r.catalog.ResolveWorkflow(ctx, registry.WorkflowQuery{Name: d.Name})
	return err == nil && cur.Record.Version == d.Version && cur.Record.Digest == d.Digest
}

// systemContext is the runtime acting for itself: the local operator's identity,
// unrestricted. Never handed to a caller.
func (r *Runtime) systemContext(ctx context.Context) (context.Context, error) {
	return bindWithSubject(ctx, localWorkflowIdentity(), localSubject())
}
