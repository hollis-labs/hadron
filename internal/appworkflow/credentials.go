package appworkflow

import (
	"context"
	"errors"
	"fmt"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
)

type credentialAdminKey struct{}

// WithCredentialAdministrator binds the already authenticated operator and a
// fresh verifier. Public credential DTOs cannot supply these authority facts.
func WithCredentialAdministrator(ctx context.Context, source string, recheck func(context.Context) error) (context.Context, error) {
	binding, err := (ContextIdentityProvider{}).BindIdentity(ctx, IdentityRequest{})
	if err != nil || binding.Principal != "operator:local" || binding.Trust != "local" || binding.SourceAuthority != "http" || recheck == nil {
		return nil, ErrPolicyDenied
	}
	admin := hoststate.CredentialAdministrator{Actor: binding.Principal, Source: source, Recheck: recheck}
	if admin.Validate(ctx) != nil {
		return nil, ErrPolicyDenied
	}
	return context.WithValue(ctx, credentialAdminKey{}, admin), nil
}

type WorkflowCredentialStore interface {
	ListMCPCredentials(context.Context, string, hoststate.CredentialAdministrator) (hoststate.CredentialList, error)
	ListMCPCredentialAudit(context.Context, string, hoststate.CredentialAdministrator) ([]hoststate.CredentialAudit, error)
	IssueMCPCredential(context.Context, hoststate.IssueCredentialRequest, hoststate.CredentialAdministrator) (hoststate.CredentialIssue, error)
	RevokeMCPCredential(context.Context, hoststate.RevokeCredentialRequest, hoststate.CredentialAdministrator) (hoststate.CredentialMetadata, error)
}

func (s *WorkflowExposureService) CredentialAudit(ctx context.Context, principal string) ([]hoststate.CredentialAudit, error) {
	store, admin, err := s.credentialAuthority(ctx)
	if err != nil {
		return nil, err
	}
	if hoststate.ValidatePublicText(principal, 256, true) != nil {
		return nil, fmt.Errorf("%w: invalid credential principal", hoststate.ErrInvalidRecord)
	}
	result, storeErr := store.ListMCPCredentialAudit(ctx, principal, admin)
	return result, credentialStoreError(storeErr)
}

func (s *WorkflowExposureService) credentialAuthority(ctx context.Context) (WorkflowCredentialStore, hoststate.CredentialAdministrator, error) {
	if ctx == nil || s == nil {
		return nil, hoststate.CredentialAdministrator{}, ErrWorkflowUnauthenticated
	}
	admin, ok := ctx.Value(credentialAdminKey{}).(hoststate.CredentialAdministrator)
	if !ok || admin.Validate(ctx) != nil {
		return nil, hoststate.CredentialAdministrator{}, ErrPolicyDenied
	}
	store, ok := s.store.(WorkflowCredentialStore)
	if !ok || nilInterface(store) {
		return nil, hoststate.CredentialAdministrator{}, ErrHostNotReady
	}
	return store, admin, nil
}

func (s *WorkflowExposureService) ListCredentials(ctx context.Context, principal string) (hoststate.CredentialList, error) {
	store, admin, err := s.credentialAuthority(ctx)
	if err != nil {
		return hoststate.CredentialList{}, err
	}
	if hoststate.ValidatePublicText(principal, 256, true) != nil {
		return hoststate.CredentialList{}, fmt.Errorf("%w: invalid credential principal", hoststate.ErrInvalidRecord)
	}
	result, storeErr := store.ListMCPCredentials(ctx, principal, admin)
	return result, credentialStoreError(storeErr)
}

func (s *WorkflowExposureService) IssueCredential(ctx context.Context, request hoststate.IssueCredentialRequest) (hoststate.CredentialIssue, error) {
	store, admin, err := s.credentialAuthority(ctx)
	if err != nil {
		return hoststate.CredentialIssue{}, err
	}
	result, storeErr := store.IssueMCPCredential(ctx, request, admin)
	return result, credentialStoreError(storeErr)
}

func (s *WorkflowExposureService) RevokeCredential(ctx context.Context, request hoststate.RevokeCredentialRequest) (hoststate.CredentialMetadata, error) {
	store, admin, err := s.credentialAuthority(ctx)
	if err != nil {
		return hoststate.CredentialMetadata{}, err
	}
	result, storeErr := store.RevokeMCPCredential(ctx, request, admin)
	return result, credentialStoreError(storeErr)
}

func credentialStoreError(err error) error {
	if errors.Is(err, hoststate.ErrCredentialAdministrator) {
		return ErrPolicyDenied
	}
	return err
}
