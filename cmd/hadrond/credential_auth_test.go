package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/internal/api"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/localauth"
)

func credentialAuthHarness(t *testing.T) *authHarness {
	t.Helper()
	runtime, cfg, _ := newTestProductionWorkflowRuntime(t)
	if err := runtime.BootstrapMCP(t.Context(), "synthetic-workflow-credential"); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(api.NewServer("", api.Dependencies{WorkflowCredentials: runtime.exposure, CredentialAuth: runtime.credentialAuth, OperatorAuth: runtime.operator}).Handler())
	t.Cleanup(srv.Close)
	token, err := localauth.ReadToken(localauth.TokenPath(cfg.DataDir))
	if err != nil {
		t.Fatal(err)
	}
	return &authHarness{t: t, srv: srv, runtime: runtime, cfg: cfg, token: token}
}

func credentialList(t *testing.T, h *authHarness) hoststate.CredentialList {
	t.Helper()
	response := h.do(http.MethodGet, "/v1/auth/credentials/list?principal_id="+workflowMCPPrincipalID, "", h.bearer())
	var list hoststate.CredentialList
	if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&list) != nil {
		t.Fatal("authenticated list unavailable")
	}
	if response.Header.Get("Cache-Control") != "no-store" {
		t.Fatal("credential metadata cacheable")
	}
	return list
}

func TestCredentialAdminAPIRequiresIndependentOperatorAndStrictArguments(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	h := credentialAuthHarness(t)
	h.runtime.operator.allowUnauthenticatedLoopback = true
	cookie := h.signIn()
	list := credentialList(t, h)
	if len(list.Credentials) != 1 {
		t.Fatal("bootstrap credential missing")
	}
	request := hoststate.IssueCredentialRequest{PrincipalID: list.PrincipalID, CredentialID: list.Credentials[0].CredentialID, ExpectedGeneration: list.Generation, IdempotencyKey: "api-issue"}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	for _, headers := range []map[string]string{nil, withCookie(cookie, map[string]string{"Origin": h.srv.URL}), {"Authorization": "Bearer synthetic-workflow-credential"}, {"Authorization": "Bearer " + strings.Repeat("a", 64)}, {"Authorization": "Bearer " + h.token, "Origin": "http://foreign.invalid"}} {
		resp := h.do(http.MethodPost, "/v1/auth/credentials/issue", string(encoded), headers)
		if resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden {
			t.Fatalf("nonoperator request returned %d", resp.StatusCode)
		}
	}
	// A service principal with workflow.manage is still not the operator.
	serviceCtx, _, err := h.runtime.exposure.ResolveSession(t.Context(), "service-admin-attempt", "synthetic-workflow-credential")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.runtime.exposure.IssueCredential(serviceCtx, request); err == nil {
		t.Fatal("workflow permission granted credential authority")
	}
	for _, body := range []string{
		strings.TrimSuffix(string(encoded), "}") + `,"expected_generation":1}`,
		strings.TrimSuffix(string(encoded), "}") + `,"raw_secret":"synthetic-forbidden"}`,
		strings.TrimSuffix(string(encoded), "}") + `,"ttl_seconds":null}`,
		strings.TrimSuffix(string(encoded), "}") + `,"EXPECTED_GENERATION":1}`,
		strings.Replace(string(encoded), `"expected_generation":1`, `"expected_generation":1.5`, 1),
		string(encoded) + ` {}`,
	} {
		resp := h.do(http.MethodPost, "/v1/auth/credentials/issue", body, h.bearer())
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatal("invalid arguments admitted")
		}
	}
	if credentialList(t, h).Generation != list.Generation {
		t.Fatal("refusal changed generation")
	}
	response := h.do(http.MethodPost, "/v1/auth/credentials/issue", string(encoded), h.bearer())
	var first hoststate.CredentialIssue
	if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&first) != nil || !first.SecretAvailable || first.Secret == "" {
		t.Fatal("private first issuance failed")
	}
	if response.Header.Get("Cache-Control") != "no-store" {
		t.Fatal("secret response cacheable")
	}
	if _, session, resolveErr := h.runtime.exposure.ResolveSession(t.Context(), "new-credential", first.Secret); resolveErr != nil || !session.Authenticated {
		t.Fatal("new credential failed original workflow authorization")
	}
	if _, session, resolveErr := h.runtime.exposure.ResolveSession(t.Context(), "old-overlap", "synthetic-workflow-credential"); resolveErr != nil || !session.Authenticated {
		t.Fatal("old credential lost overlap")
	}
	response = h.do(http.MethodPost, "/v1/auth/credentials/issue", string(encoded), h.bearer())
	var replay hoststate.CredentialIssue
	if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&replay) != nil || !replay.Replayed || replay.SecretAvailable || replay.Secret != "" || replay.CredentialID != first.CredentialID {
		t.Fatal("retry replayed secret or minted credential")
	}
	response = h.do(http.MethodGet, "/v1/auth/credentials/audit?principal_id="+list.PrincipalID, "", h.bearer())
	var audit []hoststate.CredentialAudit
	if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&audit) != nil || len(audit) != 1 || audit[0].Actor != "operator:local" || audit[0].CredentialID != first.CredentialID || audit[0].Generation != first.Generation {
		t.Fatal("atomic secret-free issuer audit unavailable")
	}
	if resp := h.do(http.MethodGet, "/v1/auth/credentials/audit?principal_id="+list.PrincipalID, "", map[string]string{"Authorization": "Bearer " + first.Secret}); resp.StatusCode != http.StatusUnauthorized {
		t.Fatal("service credential read issuer audit")
	}
	revoke := hoststate.RevokeCredentialRequest{PrincipalID: list.PrincipalID, CredentialID: first.CredentialID, ExpectedGeneration: first.Generation, IdempotencyKey: "api-revoke"}
	encoded, err = json.Marshal(revoke)
	if err != nil {
		t.Fatal(err)
	}
	response = h.do(http.MethodPost, "/v1/auth/credentials/revoke", string(encoded), h.bearer())
	if response.StatusCode != http.StatusOK {
		t.Fatal("revocation failed")
	}
	if _, session, resolveErr := h.runtime.exposure.ResolveSession(t.Context(), "revoked-api", first.Secret); resolveErr != nil || session.Authenticated {
		t.Fatal("revoked credential authenticated")
	}
	revoke.IdempotencyKey = "already-revoked"
	revoke.ExpectedGeneration = first.Generation + 1
	encoded, err = json.Marshal(revoke)
	if err != nil {
		t.Fatal(err)
	}
	response = h.do(http.MethodPost, "/v1/auth/credentials/revoke", string(encoded), h.bearer())
	var refusal struct {
		Code       string                       `json:"code"`
		Credential hoststate.CredentialMetadata `json:"credential"`
	}
	if response.StatusCode != http.StatusConflict || json.NewDecoder(response.Body).Decode(&refusal) != nil || refusal.Credential.Status != "revoked" || refusal.Credential.Generation != first.Generation+1 {
		t.Fatal("already-revoked refusal omitted current metadata")
	}
	if strings.Contains(logs.String(), first.Secret) || strings.Contains(logs.String(), h.token) || strings.Contains(logs.String(), "synthetic-workflow-credential") {
		t.Fatal("credential entered production logs")
	}
}

func TestCredentialAdminRechecksOperatorAfterAuthenticationBeforeStore(t *testing.T) {
	h := credentialAuthHarness(t)
	list := credentialList(t, h)
	r := httptest.NewRequest(http.MethodPost, "http://localhost/v1/auth/credentials/issue", nil)
	r.RemoteAddr = "127.0.0.1:1234"
	r.Header.Set("Authorization", "Bearer "+h.token)
	ctx, err := h.runtime.credentialAuth.AuthenticateCredentialAdminRequest(r)
	if err != nil {
		t.Fatal(err)
	}
	if _, rotateErr := localauth.RotateToken(localauth.TokenPath(h.cfg.DataDir)); rotateErr != nil {
		t.Fatal(rotateErr)
	}
	_, err = h.runtime.exposure.IssueCredential(ctx, hoststate.IssueCredentialRequest{PrincipalID: list.PrincipalID, CredentialID: list.Credentials[0].CredentialID, ExpectedGeneration: list.Generation, IdempotencyKey: "revoked-admin"})
	if err == nil {
		t.Fatal("stale authenticated operator changed credentials")
	}
	if errors.Is(err, hoststate.ErrConflict) {
		t.Fatal("authority failure mistaken for generation conflict")
	}
}
