package runtime

import (
	"context"
	"sort"

	libauth "github.com/hollis-labs/libs/security/auth"

	gateadapter "github.com/hollis-labs/libs/workflow/engine/adapters/gate"
	waitadapter "github.com/hollis-labs/libs/workflow/engine/adapters/wait"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
)

// Scopes a caller's token carries. The framework's auth module verifies the
// token and enforces the scope on each route; this file turns the verified
// principal into the identity binding the workflow host works with. There is no
// local authentication.
//
//	scope             grant               allows
//	hadron.run        workflow.run        start, inspect, cancel and resume runs
//	hadron.manage     workflow.manage     author, register, expose and retire workflows
//	hadron.confirm    workflow.confirm    satisfy a Confirm decision (never an agent)
//
// Wildcard grants in the token ("hadron.*", "*") are honoured by the principal.
const (
	ScopeRun     = "hadron.run"
	ScopeManage  = "hadron.manage"
	ScopeConfirm = "hadron.confirm"

	grantRun     = "workflow.run"
	grantManage  = "workflow.manage"
	grantConfirm = "workflow.confirm"

	// localOperatorPrincipal is the synthetic operator of the dev default (auth
	// dial off): it holds every grant and is the only principal with local trust.
	localOperatorPrincipal = "operator:local"
	trustLocal             = "local"
	trustVerified          = "verified"
	trustAgent             = "agent"
)

// BindingFromPrincipal builds the host identity binding for a verified principal.
// sourceAuthority names the transport the request arrived on ("http", "mcp").
func BindingFromPrincipal(p libauth.Principal, sourceAuthority string) hoststate.IdentityBinding {
	binding := localWorkflowIdentity()
	binding.Principal = p.ID
	binding.SourceAuthority = sourceAuthority
	binding.Trust = trustVerified
	binding.Grants = nil
	// A verified principal gets the receiving capabilities, and the call capability
	// only with the run grant. The agent-session capabilities belong to the local
	// operator's target: a token never widens into them.
	target := *binding.ExecutionTarget
	target.Capabilities = []string{gateadapter.CapabilityGate, waitadapter.CapabilityMessage, waitadapter.CapabilityWait}
	if p.HasScope(ScopeRun) {
		target.Capabilities = append(target.Capabilities, capabilityWorkflowCall)
	}
	sort.Strings(target.Capabilities)
	binding.ExecutionTarget = &target
	agent := p.Kind == "agent"
	if agent {
		binding.Trust = trustAgent
	}
	for scope, grant := range map[string]string{ScopeRun: grantRun, ScopeManage: grantManage, ScopeConfirm: grantConfirm} {
		if grant == grantConfirm && agent {
			continue // an agent's "confirmed" is not the operator's confirmation
		}
		if p.HasScope(scope) {
			binding.Grants = append(binding.Grants, grant)
		}
	}
	sort.Strings(binding.Grants)
	return binding
}

// Authenticate puts the caller's identity on ctx for the workflow host. With the
// auth dial off and no verified principal it is the local operator; in
// permissive or enforce mode an unverified caller authenticates nothing.
func Authenticate(ctx context.Context, mode libauth.Mode, sourceAuthority string) (context.Context, error) {
	if p, ok := libauth.FromContext(ctx); ok && p.Verified {
		return bindWithSubject(ctx, BindingFromPrincipal(p, sourceAuthority), SubjectFromPrincipal(p))
	}
	if mode == libauth.ModeOff {
		binding := localWorkflowIdentity()
		binding.SourceAuthority = sourceAuthority
		return bindWithSubject(ctx, binding, localSubject())
	}
	return nil, appworkflow.ErrWorkflowUnauthenticated
}

// operatorConfirmAuthorizer lets only the local operator or a verified,
// non-agent principal holding the confirm grant satisfy a Confirm decision. An
// agent that sets confirmed is refused (confirmation_not_permitted): an agent's
// "confirmed" is not the operator's confirmation (CW-20260930-0234).
func operatorConfirmAuthorizer(_ context.Context, identity hoststate.IdentityBinding) bool {
	if identity.Principal == localOperatorPrincipal && identity.Trust == trustLocal {
		return true
	}
	return identity.Trust == trustVerified && hasWorkflowGrant(identity, grantConfirm)
}

// bindWithSubject attaches both the host identity and the access subject, so
// the definition authorizer can tell an administrator from a profile-bound caller.
func bindWithSubject(ctx context.Context, binding hoststate.IdentityBinding, subject Subject) (context.Context, error) {
	bound, err := appworkflow.WithAuthenticatedIdentity(ctx, binding)
	if err != nil {
		return nil, err
	}
	return withSubject(bound, subject), nil
}
