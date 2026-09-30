package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/hollis-labs/hadron/internal/config"
	"github.com/hollis-labs/hadron/internal/localauth"
	"github.com/spf13/cobra"
)

// globalTokenFile is --token-file. Empty means $HADRON_TOKEN, then
// <data dir>/operator.token.
var globalTokenFile string

// operatorTokenPath is the token file the CLI reads and `auth rotate`
// rewrites.
func operatorTokenPath() string {
	if globalTokenFile != "" {
		return globalTokenFile
	}
	return localauth.TokenPath(config.Default().DataDir)
}

// operatorToken resolves the operator credential: --token-file, else
// $HADRON_TOKEN, else the default token file. The file is preferred: an
// exported variable is inherited by everything started from that shell.
func operatorToken() (string, error) {
	if globalTokenFile == "" {
		if value := strings.TrimSpace(os.Getenv(localauth.EnvToken)); value != "" {
			if !localauth.ValidToken(value) {
				return "", fmt.Errorf("%s is set but is not a Hadron operator token", localauth.EnvToken)
			}
			return value, nil
		}
	}
	return localauth.ReadToken(operatorTokenPath())
}

// operatorTransport adds the operator token to requests for the configured
// daemon, and only to those.
type operatorTransport struct {
	base http.RoundTripper
	addr func() string
}

func (t operatorTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Header.Get("Authorization") == "" && sameDaemon(request.URL, t.addr()) {
		if token, err := operatorToken(); err == nil {
			request = request.Clone(request.Context())
			request.Header.Set("Authorization", "Bearer "+token)
		}
	}
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(request)
}

func sameDaemon(target *url.URL, addr string) bool {
	daemon, err := url.Parse(addr)
	return err == nil && target != nil && strings.EqualFold(target.Host, daemon.Host) && target.Scheme == daemon.Scheme
}

// unauthenticatedHint explains a 401 from the daemon.
func unauthenticatedHint() string {
	if _, err := operatorToken(); err != nil {
		return fmt.Sprintf("no usable operator credential (%v); hadrond creates %s on first start, or pass --token-file", err, operatorTokenPath())
	}
	return fmt.Sprintf("the daemon rejected the operator token from %s; it may have been rotated or belong to a different data dir", operatorTokenPath())
}

func buildUICmd() *cobra.Command {
	var noOpen bool
	command := &cobra.Command{
		Use:   "ui",
		Short: "Sign in to the browser UI with a one-time link",
		Long: `Ask the daemon for a single-use sign-in link (valid 60 seconds) and open it.

The link sets an HttpOnly browser session cookie; the operator token itself
never reaches the browser.`,
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			request, err := http.NewRequestWithContext(command.Context(), http.MethodPost, strings.TrimRight(globalAddr, "/")+"/v1/auth/code", nil)
			if err != nil {
				return err
			}
			resp, err := httpClient.Do(request)
			if err != nil {
				return fmt.Errorf("daemon not reachable: %w", err)
			}
			defer closeBody(resp.Body)
			if resp.StatusCode == http.StatusUnauthorized {
				return errors.New(unauthenticatedHint())
			}
			if resp.StatusCode != http.StatusOK {
				return printAPIError(resp)
			}
			var issued struct {
				LoginPath string `json:"login_path"`
			}
			if decodeErr := json.NewDecoder(resp.Body).Decode(&issued); decodeErr != nil || !strings.HasPrefix(issued.LoginPath, "/auth/login?") {
				return errors.New("daemon returned an invalid sign-in link")
			}
			link := strings.TrimRight(globalAddr, "/") + issued.LoginPath
			if noOpen {
				_, err = fmt.Fprintf(command.OutOrStdout(), "sign-in link (single use, valid 60s):\n%s\n", link)
				return err
			}
			if openErr := openInBrowser(link); openErr != nil {
				_, _ = fmt.Fprintf(command.OutOrStdout(), "could not open a browser (%v); open this single-use link within 60s:\n%s\n", openErr, link)
				return nil
			}
			_, err = fmt.Fprintln(command.OutOrStdout(), "opened the Hadron UI in your browser")
			return err
		},
	}
	command.Flags().BoolVar(&noOpen, "no-open", false, "print the sign-in link instead of opening a browser")
	return command
}

func buildAuthCmd() *cobra.Command {
	command := &cobra.Command{Use: "auth", Short: "Manage the operator credential"}
	command.AddCommand(&cobra.Command{
		Use:   "rotate",
		Short: "Replace the operator token; the daemon ends every browser session",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			path := operatorTokenPath()
			if _, err := localauth.RotateToken(path); err != nil {
				return err
			}
			_, err := fmt.Fprintf(command.OutOrStdout(), "rotated %s; the old token and every browser session stop working now (run `hadron ui` to sign in again)\n", path)
			return err
		},
	})
	return command
}

func openInBrowser(target string) error {
	var command *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		command = exec.Command("open", target) // #nosec G204 -- target is the daemon's sign-in URL.
	case "windows":
		command = exec.Command("rundll32", "url.dll,FileProtocolHandler", target) // #nosec G204 -- target is the daemon's sign-in URL.
	default:
		command = exec.Command("xdg-open", target) // #nosec G204 -- target is the daemon's sign-in URL.
	}
	command.Env = localauth.ScrubEnv(os.Environ())
	return command.Start()
}
