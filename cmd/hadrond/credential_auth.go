package main

import (
	"context"
	"net/http"

	"github.com/hollis-labs/hadron/internal/appworkflow"
)

func (a *workflowHTTPAuthenticator) AuthenticateCredentialAdminRequest(request *http.Request) (context.Context, error) {
	if a == nil || a.operator == nil || a.operator.verifier == nil || request == nil || !sameOriginRequest(request) {
		return nil, appworkflow.ErrWorkflowUnauthenticated
	}
	// A secret-bearing response requires either TLS or actual local transport.
	if request.TLS == nil && (!loopbackRemote(request.RemoteAddr) || !loopbackHost(request.Host)) {
		return nil, appworkflow.ErrPolicyDenied
	}
	token, present := bearerToken(request)
	if !present || token == "" || !a.operator.verifier.Verify(token) {
		return nil, appworkflow.ErrWorkflowUnauthenticated
	}
	ctx, err := appworkflow.WithAuthenticatedIdentity(request.Context(), a.local)
	if err != nil {
		return nil, err
	}
	return appworkflow.WithCredentialAdministrator(ctx, "http:"+request.RemoteAddr, func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !a.operator.verifier.Verify(token) {
			return appworkflow.ErrPolicyDenied
		}
		return nil
	})
}
