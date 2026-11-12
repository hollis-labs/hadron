package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/spf13/cobra"

	"github.com/hollis-labs/substrate/module"
	"github.com/hollis-labs/substrate/modules/auth"
)

// workflowCmd is the operator's view of the daemon: each subcommand is one call
// to the /api/workflows routes with the framework's outbound token, so the
// daemon alone executes work and decides access. Output is the response JSON.
func workflowCmd(ctx context.Context, app *module.App) *cobra.Command {
	var server string
	c := &cobra.Command{Use: "workflow", Short: "Operate workflows on a running daemon"}
	c.PersistentFlags().StringVar(&server, "server", "", "daemon base URL (default: http://<httphost.addr>)")
	client := func() (*apiClient, error) { return newAPIClient(app, server) }

	var inputs, runID, key string
	var confirmed bool
	run := &cobra.Command{
		Use: "run NAME VERSION DIGEST", Short: "Start a run of one exact workflow definition", Args: cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, a []string) error {
			cl, err := client()
			if err != nil {
				return err
			}
			body := map[string]any{"definition": definition(a), "run_id": orGenerated(runID, "cli-run"), "idempotency_key": orGenerated(key, "cli-start"), "identity": map[string]any{}, "confirmed": confirmed}
			if inputs != "" {
				var in map[string]any
				if err := json.Unmarshal([]byte(inputs), &in); err != nil {
					return fmt.Errorf("--inputs must be a JSON object: %w", err)
				}
				body["inputs"] = in
			}
			return cl.post(ctx, cmd.OutOrStdout(), "/api/workflows/runs", body, body["idempotency_key"].(string))
		},
	}
	run.Flags().StringVar(&inputs, "inputs", "", "run inputs as a JSON object")
	run.Flags().StringVar(&runID, "run-id", "", "run id (generated when omitted)")
	run.Flags().StringVar(&key, "idempotency-key", "", "idempotency key (generated when omitted)")
	run.Flags().BoolVar(&confirmed, "confirmed", false, "confirm a step that needs the operator's confirmation")

	validate := &cobra.Command{
		Use: "validate NAME VERSION DIGEST", Short: "Validate one exact workflow definition", Args: cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, a []string) error {
			cl, err := client()
			if err != nil {
				return err
			}
			return cl.post(ctx, cmd.OutOrStdout(), "/api/workflows/validate", map[string]any{"definition": definition(a), "identity": map[string]any{}}, "")
		},
	}

	inspect := &cobra.Command{
		Use: "inspect RUN_ID", Short: "Inspect a run", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, a []string) error {
			cl, err := client()
			if err != nil {
				return err
			}
			return cl.post(ctx, cmd.OutOrStdout(), runPath(a[0], "inspect"), map[string]any{"run_id": a[0], "identity": map[string]any{}}, "")
		},
	}

	var reason string
	cancel := &cobra.Command{
		Use: "cancel RUN_ID", Short: "Cancel a run", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, a []string) error {
			cl, err := client()
			if err != nil {
				return err
			}
			k := orGenerated(key, "cli-cancel")
			return cl.post(ctx, cmd.OutOrStdout(), runPath(a[0], "cancel"), map[string]any{"run_id": a[0], "identity": map[string]any{}, "idempotency_key": k, "reason": reason}, k)
		},
	}
	cancel.Flags().StringVar(&reason, "reason", "", "why the run is canceled")
	cancel.Flags().StringVar(&key, "idempotency-key", "", "idempotency key (generated when omitted)")

	var after string
	var limit, waitMS int
	events := &cobra.Command{
		Use: "events RUN_ID", Short: "Read a run's events after a cursor", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, a []string) error {
			cl, err := client()
			if err != nil {
				return err
			}
			body := map[string]any{"run_id": a[0], "identity": map[string]any{}, "after": after, "event_limit": limit, "wait_ms": waitMS}
			return cl.post(ctx, cmd.OutOrStdout(), runPath(a[0], "events"), body, "")
		},
	}
	events.Flags().StringVar(&after, "after", "", "next_cursor from an earlier call")
	events.Flags().IntVar(&limit, "limit", 100, "events per call")
	events.Flags().IntVar(&waitMS, "wait-ms", 0, "hold the call until a newer event exists (max 30000)")

	var namespace string
	var searchLimit int
	search := &cobra.Command{
		Use: "search [QUERY]", Short: "Search the workflow catalog", Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, a []string) error {
			cl, err := client()
			if err != nil {
				return err
			}
			q := ""
			if len(a) == 1 {
				q = a[0]
			}
			return cl.post(ctx, cmd.OutOrStdout(), "/api/workflows/lifecycle/catalog/search", map[string]any{"query": q, "namespace": namespace, "limit": searchLimit, "identity": map[string]any{}}, "")
		},
	}
	search.Flags().StringVar(&namespace, "namespace", "", "namespace to search (required: the catalog is searched one namespace at a time)")
	_ = search.MarkFlagRequired("namespace")
	search.Flags().IntVar(&searchLimit, "limit", 20, "matches to return")

	c.AddCommand(run, validate, inspect, cancel, events, search)
	return c
}

func definition(a []string) map[string]any {
	return map[string]any{"kind": "registry", "id": a[0], "version": a[1], "digest": a[2]}
}

func runPath(runID, action string) string {
	return "/api/workflows/runs/" + url.PathEscape(runID) + "/" + action
}

func orGenerated(v, prefix string) string {
	if v != "" {
		return v
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	return prefix + "-" + hex.EncodeToString(b[:])
}

type apiClient struct {
	base *url.URL
	http *http.Client
}

func newAPIClient(app *module.App, server string) (*apiClient, error) {
	if server == "" {
		cfg, _ := app.Config["httphost"].(map[string]any)
		addr, _ := cfg["addr"].(string)
		if addr == "" {
			return nil, errors.New("no daemon address: pass --server or set httphost.addr")
		}
		server = "http://" + addr
	}
	u, err := url.Parse(server)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("--server %q is not an http(s) URL", server)
	}
	// The outbound token goes to this daemon only.
	return &apiClient{base: u, http: auth.HTTPClient(app, u.Host)}, nil
}

// post sends the JSON body and writes the response body to out. A non-2xx
// response is an error carrying the flat {code, message} body.
func (c *apiClient) post(ctx context.Context, out io.Writer, path string, body any, idempotencyKey string) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base.JoinPath(path).String(), bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("daemon unreachable at %s: %w", c.base.Host, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		var flat struct{ Code, Message string }
		if json.Unmarshal(data, &flat) == nil && flat.Code != "" {
			return fmt.Errorf("%s (%d): %s", flat.Code, resp.StatusCode, flat.Message)
		}
		return fmt.Errorf("daemon answered %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	var pretty bytes.Buffer
	if json.Indent(&pretty, data, "", "  ") != nil {
		_, err = out.Write(data)
		return err
	}
	pretty.WriteByte('\n')
	_, err = out.Write(pretty.Bytes())
	return err
}
