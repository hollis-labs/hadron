package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
)

func credentialCLIArgs(path string) []string {
	return []string{"issue", "--principal-id", "operator:mcp-local", "--credential-id", "cred_" + strings.Repeat("1", 32), "--expected-generation", "1", "--idempotency-key", "cli-operation", "--secret-file", path}
}

func TestCredentialCLIPrivateDeliveryAndMetadataOnlyRetry(t *testing.T) {
	secret := "synthetic-new-private-credential"
	metadata := hoststate.CredentialMetadata{PrincipalID: "operator:mcp-local", CredentialID: "cred_" + strings.Repeat("2", 32), Generation: 2, OperationGeneration: 2, CreatedAt: time.Now().UTC(), Status: "active", SecretAvailable: true}
	var token string
	requests := 0
	daemon := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("not authenticated with operator file")
		}
		requests++
		result := hoststate.CredentialIssue{CredentialMetadata: metadata, Secret: secret}
		if requests > 1 {
			result.Secret = ""
			result.SecretAvailable = false
			result.Replayed = true
		}
		_ = json.NewEncoder(w).Encode(result)
	}))
	t.Cleanup(daemon.Close)
	_, token = withTokenFile(t, daemon.URL+"/")
	t.Setenv("HADRON_TOKEN", strings.Repeat("f", 64))
	path := filepath.Join(t.TempDir(), "new.token")
	var out, meta bytes.Buffer
	cmd := buildCredentialCmd()
	cmd.SetArgs(credentialCLIArgs(path))
	cmd.SetOut(&out)
	cmd.SetErr(&meta)
	if executeErr := cmd.Execute(); executeErr != nil {
		t.Fatal(executeErr)
	}
	value, err := os.ReadFile(path)
	if err != nil || string(value) != secret+"\n" {
		t.Fatal("private first delivery missing")
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("credential file not private")
	}
	if strings.Contains(meta.String(), secret) || out.Len() != 0 {
		t.Fatal("secret entered metadata or stdout")
	}
	unused := filepath.Join(t.TempDir(), "retry-must-not-create.token")
	cmd = buildCredentialCmd()
	cmd.SetArgs(credentialCLIArgs(unused))
	cmd.SetOut(&out)
	cmd.SetErr(&meta)
	if executeErr := cmd.Execute(); executeErr != nil {
		t.Fatal(executeErr)
	}
	if _, err = os.Lstat(unused); !os.IsNotExist(err) {
		t.Fatal("metadata replay created secret file")
	}
	if requests != 2 || !strings.Contains(meta.String(), `"replayed":true`) {
		t.Fatal("retry missing metadata")
	}
}

func TestCredentialCLIPipeDeliveryAndPremutationSinkRefusals(t *testing.T) {
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = read.Close(); _ = write.Close() })
	if deliverErr := deliverCredentialSecret(write, "", true, "synthetic-pipe-secret"); deliverErr != nil {
		t.Fatal(deliverErr)
	}
	_ = write.Close()
	data, err := io.ReadAll(read)
	if err != nil || string(data) != "synthetic-pipe-secret\n" {
		t.Fatal("pipe delivery failed")
	}
	path := filepath.Join(t.TempDir(), "existing.token")
	if err := os.WriteFile(path, []byte("unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "linked.token")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	requests := 0
	daemon := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(daemon.Close)
	withTokenFile(t, daemon.URL)
	for _, args := range [][]string{credentialCLIArgs(path), credentialCLIArgs(link), append(credentialCLIArgs(filepath.Join(t.TempDir(), "new")), "--ttl", "0s"), append(credentialCLIArgs(filepath.Join(t.TempDir(), "new")), "--overlap", "1.5s")} {
		cmd := buildCredentialCmd()
		cmd.SetOut(&bytes.Buffer{})
		cmd.SetErr(&bytes.Buffer{})
		cmd.SetArgs(args)
		if err := cmd.Execute(); err == nil {
			t.Fatal("unsafe private sink or bounds admitted")
		}
	}
	duplicate := buildCredentialCmd()
	duplicate.SetOut(&bytes.Buffer{})
	duplicate.SetErr(&bytes.Buffer{})
	duplicate.SetArgs(append(credentialCLIArgs(filepath.Join(t.TempDir(), "new")), "--expected-generation", "1"))
	if err := duplicate.Execute(); err == nil {
		t.Fatal("duplicate CLI flag admitted")
	}
	if err := validateSecretSink(&bytes.Buffer{}, "", true); err == nil {
		t.Fatal("memory writer accepted as pipe")
	}
	if requests != 0 {
		t.Fatal("invalid sink issued credential")
	}
	value, _ := os.ReadFile(path)
	if string(value) != "unchanged" {
		t.Fatal("existing credential overwritten")
	}
}

func TestCredentialCLILostDeliveryReportsIDAndRefusesResponseSecretsInErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raced.token")
	secret := "synthetic-secret-must-not-leak"
	id := "cred_" + strings.Repeat("3", 32)
	daemon := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := os.WriteFile(path, []byte("other-owner-content"), 0600); err != nil {
			t.Error(err)
		}
		_ = json.NewEncoder(w).Encode(hoststate.CredentialIssue{CredentialMetadata: hoststate.CredentialMetadata{PrincipalID: "operator:mcp-local", CredentialID: id, Generation: 2, SecretAvailable: true}, Secret: secret})
	}))
	t.Cleanup(daemon.Close)
	withTokenFile(t, daemon.URL)
	var out, meta bytes.Buffer
	cmd := buildCredentialCmd()
	cmd.SetArgs(credentialCLIArgs(path))
	cmd.SetOut(&out)
	cmd.SetErr(&meta)
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), id) || strings.Contains(err.Error()+meta.String()+out.String(), secret) {
		t.Fatal("lost delivery lacks safe revocable ID or leaks secret")
	}
	data, _ := os.ReadFile(path)
	if string(data) != "other-owner-content" {
		t.Fatal("raced file overwritten")
	}
	refusal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, secret)
	}))
	t.Cleanup(refusal.Close)
	globalAddr = refusal.URL
	cmd = buildCredentialCmd()
	cmd.SetArgs(credentialCLIArgs(filepath.Join(t.TempDir(), "new")))
	cmd.SetOut(&out)
	cmd.SetErr(&meta)
	err = cmd.Execute()
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatal("issuer error body leaked")
	}
}

func TestCredentialCLIListsRetainedHistoryBeyondSmallIssuanceResponse(t *testing.T) {
	metadata := hoststate.CredentialList{PrincipalID: "operator:mcp-local", Generation: 400}
	for i := range 400 {
		metadata.Credentials = append(metadata.Credentials, hoststate.CredentialMetadata{PrincipalID: metadata.PrincipalID, CredentialID: fmt.Sprintf("cred_%032x", i+1), Generation: metadata.Generation, CreatedAt: time.Now().UTC(), Status: "revoked"})
	}
	encoded, err := json.Marshal(metadata)
	if err != nil || len(encoded) <= 64*1024 {
		t.Fatal("large retained history fixture unavailable")
	}
	daemon := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(encoded) }))
	t.Cleanup(daemon.Close)
	withTokenFile(t, daemon.URL)
	var output bytes.Buffer
	cmd := buildCredentialCmd()
	cmd.SetArgs([]string{"list", "--principal-id", metadata.PrincipalID})
	cmd.SetOut(&output)
	cmd.SetErr(&bytes.Buffer{})
	if executeErr := cmd.Execute(); executeErr != nil {
		t.Fatal(executeErr)
	}
	var actual hoststate.CredentialList
	if json.Unmarshal(output.Bytes(), &actual) != nil || len(actual.Credentials) != len(metadata.Credentials) || actual.Credentials[len(actual.Credentials)-1].CredentialID != metadata.Credentials[len(metadata.Credentials)-1].CredentialID {
		t.Fatal("retained history truncated by issuer client")
	}
}
