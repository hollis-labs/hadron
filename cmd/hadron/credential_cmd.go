package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/localauth"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func buildCredentialCmd() *cobra.Command {
	root := &cobra.Command{Use: "credential", Short: "Manage an existing MCP principal's credentials using the operator token file"}
	var principal, credential, key, secretFile string
	var generation uint64
	var overlap, ttl time.Duration
	var secretStdout bool
	issue := &cobra.Command{Use: "issue", Short: "Issue once with bounded overlap; never print a credential to a terminal", Args: cobra.NoArgs, SilenceUsage: true, SilenceErrors: true}
	issue.Flags().StringVar(&principal, "principal-id", "", "existing MCP principal")
	issue.Flags().StringVar(&credential, "credential-id", "", "retained member of that principal's credential set")
	issue.Flags().Uint64Var(&generation, "expected-generation", 0, "current principal generation")
	issue.Flags().StringVar(&key, "idempotency-key", "", "stable operation key for exact retries")
	issue.Flags().DurationVar(&overlap, "overlap", 15*time.Minute, "old credential validation overlap (0 to 24h)")
	issue.Flags().DurationVar(&ttl, "ttl", 0, "optional new credential lifetime (positive, at most 365d)")
	issue.Flags().StringVar(&secretFile, "secret-file", "", "new exclusive owner-only file for the one-time secret")
	issue.Flags().BoolVar(&secretStdout, "secret-stdout", false, "deliver only to an actual stdout pipe")
	issue.RunE = func(cmd *cobra.Command, _ []string) error {
		if overlap%time.Second != 0 || overlap < 0 || overlap > 24*time.Hour {
			return errors.New("overlap must be integral seconds between 0 and 24h")
		}
		seconds := int64(overlap / time.Second)
		request := hoststate.IssueCredentialRequest{PrincipalID: principal, CredentialID: credential, ExpectedGeneration: generation, IdempotencyKey: key, OverlapSeconds: &seconds}
		if cmd.Flags().Changed("ttl") {
			if ttl%time.Second != 0 || ttl <= 0 || ttl > 365*24*time.Hour {
				return errors.New("TTL must be positive integral seconds at most 365d")
			}
			value := int64(ttl / time.Second)
			request.TTLSeconds = &value
		}
		if request.Validate() != nil {
			return errors.New("principal, credential ID, expected generation and idempotency key are required")
		}
		if err := validateSecretSink(cmd.OutOrStdout(), secretFile, secretStdout); err != nil {
			return err
		}
		var result hoststate.CredentialIssue
		if err := credentialRequest(cmd, http.MethodPost, "issue", request, &result); err != nil {
			return err
		}
		if result.CredentialID == "" || !hoststate.ValidCredentialID(result.CredentialID) || result.PrincipalID != principal || result.Generation == 0 {
			return errors.New("daemon returned invalid credential metadata")
		}
		// Safe metadata is always emitted separately, including when delivery
		// fails. The caller can revoke this outstanding ID or retry metadata.
		if err := json.NewEncoder(cmd.ErrOrStderr()).Encode(result.CredentialMetadata); err != nil {
			return errors.New("credential metadata delivery failed")
		}
		if !result.SecretAvailable {
			if result.Secret != "" || !result.Replayed {
				return errors.New("daemon returned invalid credential delivery state")
			}
			return nil
		}
		if result.Replayed || hoststate.ValidateMCPToken(result.Secret) != nil {
			return errors.New("daemon returned invalid credential delivery state")
		}
		if err := deliverCredentialSecret(cmd.OutOrStdout(), secretFile, secretStdout, result.Secret); err != nil {
			return fmt.Errorf("credential %s was issued but private delivery failed; inspect or revoke that ID, or use a new authorized operation", result.CredentialID)
		}
		return nil
	}
	var revokePrincipal, revokeID, revokeKey string
	var revokeGeneration uint64
	revoke := &cobra.Command{Use: "revoke", Short: "Revoke one bound credential ID with generation and retry guards", Args: cobra.NoArgs, SilenceUsage: true, SilenceErrors: true}
	revoke.Flags().StringVar(&revokePrincipal, "principal-id", "", "existing principal")
	revoke.Flags().StringVar(&revokeID, "credential-id", "", "credential ID to revoke")
	revoke.Flags().Uint64Var(&revokeGeneration, "expected-generation", 0, "current family generation")
	revoke.Flags().StringVar(&revokeKey, "idempotency-key", "", "stable exact retry key")
	revoke.RunE = func(cmd *cobra.Command, _ []string) error {
		request := hoststate.RevokeCredentialRequest{PrincipalID: revokePrincipal, CredentialID: revokeID, ExpectedGeneration: revokeGeneration, IdempotencyKey: revokeKey}
		if request.Validate() != nil {
			return errors.New("invalid credential revocation arguments")
		}
		var result hoststate.CredentialMetadata
		if err := credentialRequest(cmd, http.MethodPost, "revoke", request, &result); err != nil {
			return err
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(result)
	}
	var listPrincipal string
	list := &cobra.Command{Use: "list", Short: "List retained credential metadata, never values or digests", Args: cobra.NoArgs, SilenceUsage: true, SilenceErrors: true}
	list.Flags().StringVar(&listPrincipal, "principal-id", "", "existing principal")
	list.RunE = func(cmd *cobra.Command, _ []string) error {
		if hoststate.ValidatePublicText(listPrincipal, 256, true) != nil {
			return errors.New("principal ID required")
		}
		var result hoststate.CredentialList
		if err := credentialRequest(cmd, http.MethodGet, "list?principal_id="+url.QueryEscape(listPrincipal), nil, &result); err != nil {
			return err
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(result)
	}
	root.AddCommand(list, issue, revoke)
	var auditPrincipal string
	audit := &cobra.Command{Use: "audit", Short: "Read the newest 100 secret-free credential mutation records", Args: cobra.NoArgs, SilenceUsage: true, SilenceErrors: true}
	audit.Flags().StringVar(&auditPrincipal, "principal-id", "", "existing principal")
	audit.RunE = func(cmd *cobra.Command, _ []string) error {
		if hoststate.ValidatePublicText(auditPrincipal, 256, true) != nil {
			return errors.New("principal ID required")
		}
		var records []hoststate.CredentialAudit
		if err := credentialRequest(cmd, http.MethodGet, "audit?principal_id="+url.QueryEscape(auditPrincipal), nil, &records); err != nil {
			return err
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(records)
	}
	root.AddCommand(audit)
	for _, command := range root.Commands() {
		command.Flags().VisitAll(func(flag *pflag.Flag) { flag.Value = &credentialSingleValue{Value: flag.Value} })
	}
	return root
}

type credentialSingleValue struct {
	pflag.Value
	set bool
}

func (v *credentialSingleValue) Set(value string) error {
	if v.set {
		return errors.New("duplicate credential argument refused")
	}
	v.set = true
	return v.Value.Set(value)
}

func credentialRequest(cmd *cobra.Command, method, action string, body, out any) error {
	target, err := url.Parse(globalAddr)
	if err != nil || target.User != nil || target.RawQuery != "" || target.Fragment != "" || target.Host == "" || target.Path != "" && target.Path != "/" {
		return errors.New("invalid daemon address")
	}
	ip := net.ParseIP(target.Hostname())
	if target.Scheme != "https" && (target.Scheme != "http" || target.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback())) {
		return errors.New("credential administration requires local HTTP or TLS")
	}
	// Unlike the legacy global transport, issuance never sources authority from
	// HADRON_TOKEN or a raw command argument.
	token, err := localauth.ReadToken(operatorTokenPath())
	if err != nil {
		return errors.New("usable operator token file required")
	}
	var payload []byte
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return errors.New("invalid credential request")
		}
	}
	// action is locally constructed; query is separated from its path.
	endpoint := strings.TrimRight(globalAddr, "/") + "/v1/auth/credentials/" + action
	request, err := http.NewRequestWithContext(cmd.Context(), method, endpoint, bytes.NewReader(payload))
	if err != nil {
		return errors.New("invalid credential request")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	client := *httpClient
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(request)
	if err != nil {
		return errors.New("credential issuer unavailable")
	}
	defer closeBody(response.Body)
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("credential operation refused (HTTP %d)", response.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 64*1024))
	if decoder.Decode(out) != nil {
		return errors.New("invalid credential issuer response")
	}
	return nil
}

func validateSecretSink(out io.Writer, path string, pipe bool) error {
	if (path != "") == pipe {
		return errors.New("choose exactly one private secret-file or secret-stdout pipe")
	}
	if pipe {
		file, ok := out.(*os.File)
		if !ok {
			return errors.New("secret stdout requires an actual pipe")
		}
		info, err := file.Stat()
		if err != nil || info.Mode()&os.ModeNamedPipe == 0 {
			return errors.New("secret stdout requires an actual pipe")
		}
		return nil
	}
	if !filepath.IsAbs(path) {
		return errors.New("secret file must be an absolute new path")
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return errors.New("secret file must not exist")
	}
	parent, err := os.Stat(filepath.Dir(path))
	if err != nil || !parent.IsDir() {
		return errors.New("secret file parent directory unavailable")
	}
	return nil
}

func deliverCredentialSecret(out io.Writer, path string, pipe bool, secret string) error {
	if err := validateSecretSink(out, path, pipe); err != nil {
		return err
	}
	if pipe {
		_, err := io.WriteString(out, secret+"\n")
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600) // #nosec G304 -- explicit new private sink, exclusive creation refuses existing files/symlinks.
	if err != nil {
		return errors.New("private credential file creation failed")
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return errors.New("private credential file refused")
	}
	if _, err := io.WriteString(file, secret+"\n"); err != nil {
		return errors.New("private credential file write failed")
	}
	if file.Sync() != nil {
		return errors.New("private credential file sync failed")
	}
	if file.Close() != nil {
		return errors.New("private credential file close failed")
	}
	return nil
}
