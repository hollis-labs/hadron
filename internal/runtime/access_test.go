package runtime

import (
	"context"
	"testing"

	libauth "github.com/hollis-labs/libs/security/auth"
	libpolicy "github.com/hollis-labs/libs/security/policy"
	"github.com/hollis-labs/libs/workflow/engine/graph"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
)

func principalCtx(t *testing.T, scopes ...string) context.Context {
	t.Helper()
	ctx := libauth.WithPrincipal(t.Context(), libauth.Principal{Verified: true, ID: "agent:test/caller", Kind: "agent", Scopes: scopes})
	bound, err := Authenticate(ctx, libauth.ModeEnforce, "http")
	if err != nil {
		t.Fatal(err)
	}
	return bound
}

func TestProfileClaimIsOneScopeOrNone(t *testing.T) {
	for name, c := range map[string]struct {
		scopes []string
		id     string
		ok     bool
	}{
		"none":       {[]string{"hadron.run"}, "", false},
		"one":        {[]string{"hadron.run", "hadron.profile.reviewers"}, "reviewers", true},
		"same twice": {[]string{"hadron.profile.a", "hadron.profile.a"}, "a", true},
		"two":        {[]string{"hadron.profile.a", "hadron.profile.b"}, "", false},
		"empty id":   {[]string{"hadron.profile."}, "", false},
		"wildcard":   {[]string{"hadron.*"}, "", false},
	} {
		id, ok := ProfileClaim(libauth.Principal{Scopes: c.scopes})
		if id != c.id || ok != c.ok {
			t.Errorf("%s: got (%q, %v), want (%q, %v)", name, id, ok, c.id, c.ok)
		}
	}
	if !SubjectFromPrincipal(libauth.Principal{Scopes: []string{"hadron.manage"}}).Unrestricted {
		t.Error("a manager must be unrestricted")
	}
	if SubjectFromPrincipal(libauth.Principal{Scopes: []string{"hadron.run", "hadron.profile.x"}}).Unrestricted {
		t.Error("a runner with a profile must be restricted")
	}
}

// A profile-bound caller can use exactly what its profile covers; a caller with
// no profile, an unknown one or two of them can use nothing; an administrator
// can use anything. The decision is the same through the policy module's
// evaluator, in enforce mode.
func TestExposureProfileDecidesWhichWorkflowsACallerCanUse(t *testing.T) {
	pdp := NewAccessPDP()
	rt, _, _ := newTestRuntimeWith(t, nil, WithAccessPDP(pdp))
	if err := rt.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	admin := localCtx(t.Context())
	ctx, err := Authenticate(libauth.WithPrincipal(t.Context(), libauth.Principal{Verified: true, ID: "user:admin", Scopes: []string{ScopeManage, ScopeRun}}), libauth.ModeEnforce, "http")
	if err != nil {
		t.Fatal(err)
	}
	admin = ctx

	registryRef := func(name string) graph.DefinitionRef {
		d := registerProductionActivationDraft(admin, t, rt.lifecycle, productionActivationDraft(t, name, "v1")).Descriptor.Definition
		return d
	}
	pinned, other := registryRef("pinned-flow"), registryRef("other-flow")
	for _, ref := range []graph.DefinitionRef{pinned, other} {
		if _, err := rt.lifecycle.PinRegistryVersion(admin, appworkflow.MutateWorkflowVersionRequest{Definition: ref}); err != nil {
			t.Fatalf("pin %v: %v", ref.ID, err)
		}
		if _, err := rt.lifecycle.PublishWorkflowVersion(admin, appworkflow.MutateWorkflowVersionRequest{Definition: ref}); err != nil {
			t.Fatalf("publish %v: %v", ref.ID, err)
		}
	}
	if _, err := rt.exposure.PutProfile(admin, hoststate.ExposureProfileRecord{
		ID: "reviewers", Pins: []graph.DefinitionRef{pinned}, MaxDirectTools: 5, SearchScope: hoststate.ExposureSearchNone,
	}, 0); err != nil {
		t.Fatal(err)
	}

	// A denied definition validates to the "not authorized" diagnostic (HADR-HOST-021);
	// an allowed one to a plan.
	allowed := func(ctx context.Context, ref graph.DefinitionRef) bool {
		res, err := rt.operations.ValidateWorkflow(ctx, appworkflow.ValidateWorkflowRequest{Definition: ref, Identity: appworkflow.IdentityRequest{SourceAuthority: "http"}})
		if err != nil {
			t.Fatalf("validate %s: %v", ref.ID, err)
		}
		if res.Plan != nil && len(res.Diagnostics) == 0 {
			return true
		}
		if len(res.Diagnostics) != 1 || res.Diagnostics[0].Code != "HADR-HOST-021" {
			t.Fatalf("validate %s: diagnostics %v, want one HADR-HOST-021", ref.ID, res.Diagnostics)
		}
		return false
	}
	runner := principalCtx(t, ScopeRun, ProfileScopePrefix+"reviewers")
	for name, c := range map[string]struct {
		ctx  context.Context
		ref  graph.DefinitionRef
		want bool
	}{
		"profile covers the pinned one":      {runner, pinned, true},
		"profile does not cover the other":   {runner, other, false},
		"no profile covers nothing":          {principalCtx(t, ScopeRun), pinned, false},
		"unknown profile covers nothing":     {principalCtx(t, ScopeRun, ProfileScopePrefix+"nope"), pinned, false},
		"two profiles cover nothing":         {principalCtx(t, ScopeRun, ProfileScopePrefix+"reviewers", ProfileScopePrefix+"other"), pinned, false},
		"an administrator covers everything": {admin, other, true},
		"the off-dial operator covers all":   {localCtx(t.Context()), other, true},
	} {
		if got := allowed(c.ctx, c.ref); got != c.want {
			t.Errorf("%s: allowed = %v, want %v", name, got, c.want)
		}
	}

	// What a caller sees as tools or card skills follows the same profile.
	visible := func(ctx context.Context) []string {
		ds, err := rt.VisibleWorkflows(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, d := range ds {
			names = append(names, d.Name)
		}
		return names
	}
	if got := visible(runner); len(got) != 1 || got[0] != pinned.ID {
		t.Errorf("profile caller sees %v, want only %s", got, pinned.ID)
	}
	if got := visible(principalCtx(t, ScopeRun)); len(got) != 0 {
		t.Errorf("caller with no profile sees %v", got)
	}
	if got := visible(admin); len(got) != 2 {
		t.Errorf("administrator sees %v, want both published workflows", got)
	}

	// The same decision through the policy evaluator, as the host wires it.
	ev := libpolicy.Evaluator{PDP: pdp, Audit: discardAudit{}}
	opts := libpolicy.Options{Mode: libpolicy.Enforce, FailMode: libpolicy.FailClosed}
	subject := SubjectFromPrincipal(libauth.Principal{ID: "agent:test/caller", Kind: "agent", Scopes: []string{ScopeRun, ProfileScopePrefix + "reviewers"}})
	for ref, want := range map[graph.DefinitionRef]libpolicy.Effect{pinned: libpolicy.Allow, other: libpolicy.Deny} {
		res, err := ev.Evaluate(t.Context(), PolicyRequest(subject, ref), opts)
		if err != nil || res.Admission != want {
			t.Errorf("policy for %s: admission %q, err %v, want %q", ref.ID, res.Admission, err, want)
		}
	}
	// An unbound decision point fails closed.
	if res, err := ev.Evaluate(t.Context(), PolicyRequest(subject, pinned), opts); err != nil || res.Admission != libpolicy.Allow {
		t.Fatalf("bound PDP: %v %v", res, err)
	}
	unbound := libpolicy.Evaluator{PDP: NewAccessPDP(), Audit: discardAudit{}}
	if res, _ := unbound.Evaluate(t.Context(), PolicyRequest(subject, pinned), opts); res.Admission != libpolicy.Deny {
		t.Errorf("an unbound PDP admitted: %v", res)
	}
}

type discardAudit struct{}

func (discardAudit) Record(context.Context, libpolicy.AuditEvent) error { return nil }
