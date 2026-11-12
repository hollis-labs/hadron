package runtime

import (
	"context"
	"errors"
	"slices"
	"testing"

	libauth "github.com/hollis-labs/libs/security/auth"

	"github.com/hollis-labs/hadron/internal/appworkflow"
)

func TestScopesBecomeGrantsAndAgentsNeverConfirm(t *testing.T) {
	for _, c := range []struct {
		name   string
		p      libauth.Principal
		grants []string
		trust  string
	}{
		{"viewer with no scopes", libauth.Principal{Verified: true, ID: "svc/v"}, nil, "verified"},
		{"runner", libauth.Principal{Verified: true, ID: "svc/r", Scopes: []string{ScopeRun}}, []string{"workflow.run"}, "verified"},
		{"manager", libauth.Principal{Verified: true, ID: "svc/m", Scopes: []string{ScopeRun, ScopeManage}}, []string{"workflow.manage", "workflow.run"}, "verified"},
		{"operator with confirm", libauth.Principal{Verified: true, ID: "op/c", Scopes: []string{ScopeRun, ScopeConfirm}}, []string{"workflow.confirm", "workflow.run"}, "verified"},
		{"wildcard", libauth.Principal{Verified: true, ID: "svc/all", Scopes: []string{"hadron.*"}}, []string{"workflow.confirm", "workflow.manage", "workflow.run"}, "verified"},
		{"agent keeps run, loses confirm", libauth.Principal{Verified: true, ID: "agent:p", Kind: "agent", Scopes: []string{"hadron.*"}}, []string{"workflow.manage", "workflow.run"}, "agent"},
	} {
		b := BindingFromPrincipal(c.p, "http")
		if !slices.Equal(b.Grants, c.grants) || b.Trust != c.trust || b.Principal != c.p.ID || b.SourceAuthority != "http" {
			t.Errorf("%s: grants=%v trust=%s principal=%s", c.name, b.Grants, b.Trust, b.Principal)
		}
	}
}

func TestOnlyTheLocalOperatorOrAConfirmingNonAgentMayConfirm(t *testing.T) {
	ctx := context.Background()
	if !operatorConfirmAuthorizer(ctx, localWorkflowIdentity()) {
		t.Error("the local operator must be able to confirm")
	}
	confirmer := BindingFromPrincipal(libauth.Principal{Verified: true, ID: "op/c", Scopes: []string{ScopeConfirm}}, "http")
	if !operatorConfirmAuthorizer(ctx, confirmer) {
		t.Error("a verified principal with the confirm scope must be able to confirm")
	}
	runnerOnly := BindingFromPrincipal(libauth.Principal{Verified: true, ID: "svc/r", Scopes: []string{ScopeRun}}, "http")
	agent := BindingFromPrincipal(libauth.Principal{Verified: true, ID: "agent:p", Kind: "agent", Scopes: []string{"hadron.*"}}, "mcp")
	if operatorConfirmAuthorizer(ctx, runnerOnly) {
		t.Error("a runner without the confirm scope was allowed to confirm")
	}
	if operatorConfirmAuthorizer(ctx, agent) {
		t.Error("an agent was allowed to confirm")
	}
}

func TestAuthenticateFollowsTheDial(t *testing.T) {
	// off dial, no principal: the local operator
	ctx, err := Authenticate(context.Background(), libauth.ModeOff, "http")
	if err != nil || ctx == nil {
		t.Fatalf("off dial: %v", err)
	}
	// permissive or enforce with nobody verified: refused
	for _, mode := range []libauth.Mode{libauth.ModePermissive, libauth.ModeEnforce} {
		if _, err := Authenticate(context.Background(), mode, "http"); !errors.Is(err, appworkflow.ErrWorkflowUnauthenticated) {
			t.Errorf("%s with no principal: %v", mode, err)
		}
	}
	// a verified principal is mapped in any mode
	withP := libauth.WithPrincipal(context.Background(), libauth.Principal{Verified: true, ID: "svc/r", Scopes: []string{ScopeRun}})
	if _, err := Authenticate(withP, libauth.ModeEnforce, "http"); err != nil {
		t.Errorf("verified principal: %v", err)
	}
	// an unverified (off-mode synthetic) principal does not authenticate as a user in enforce
	synthetic := libauth.WithPrincipal(context.Background(), libauth.Principal{Verified: false, ID: "anonymous"})
	if _, err := Authenticate(synthetic, libauth.ModeEnforce, "http"); err == nil {
		t.Error("an unverified principal authenticated in enforce mode")
	}
}
