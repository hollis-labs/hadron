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

	"github.com/hollis-labs/libs/workflow/engine/diagnostic"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/hollis-labs/substrate/module"
	"github.com/hollis-labs/substrate/modules/auth"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
)

// workflowCmd is the operator's view of the daemon: each subcommand is one call
// to the /api/workflows routes with the framework's outbound token, so the
// daemon alone executes work and decides access.
//
// Output follows -o/--output: text (the default; diagnostics as
// "severity CODE file:line:col: message", summaries as lines), json (the
// response body as the daemon sent it) or yaml. Commands without a dedicated
// text form print the response as indented JSON in text mode.
func workflowCmd(ctx context.Context, app *module.App) *cobra.Command {
	e := &cliEnv{ctx: ctx, app: app}
	c := &cobra.Command{Use: "workflow", Short: "Operate workflows on a running daemon"}
	c.PersistentFlags().StringVar(&e.server, "server", "", "daemon base URL (default: http://<httphost.addr>)")
	c.PersistentFlags().StringVarP(&e.format, "output", "o", "text", "output format: text, json or yaml")
	c.AddCommand(
		e.validateCmd(), e.explainCmd(), e.runCmd(), e.inspectCmd(), e.cancelCmd(), e.eventsCmd(),
		e.readCmd("waits", "List a run's waits"), e.readCmd("values", "Fetch a run's values"),
		e.resumeCmd(), e.signalCmd(), e.rerunCmd(), e.searchCmd(),
	)
	e.addLifecycle(c)
	return c
}

// cliEnv carries what every subcommand shares: the daemon, the format.
type cliEnv struct {
	ctx    context.Context
	app    *module.App
	server string
	format string
}

func (e *cliEnv) validFormat() error {
	switch e.format {
	case "text", "json", "yaml":
		return nil
	}
	return fmt.Errorf("--output %q: want text, json or yaml", e.format)
}

// do posts the request and prints the reply. text renders a successful reply
// in text mode; nil prints indented JSON. A failure is printed in the chosen
// format and returned so the exit status is non-zero.
func (e *cliEnv) do(cmd *cobra.Command, path string, body any, key string, text func(io.Writer, []byte) error) error {
	if err := e.validFormat(); err != nil {
		return err
	}
	cl, err := newAPIClient(e.app, e.server)
	if err != nil {
		return err
	}
	reply, err := cl.post(e.ctx, path, body, key)
	out := cmd.OutOrStdout()
	var de *daemonError
	if errors.As(err, &de) {
		return e.failure(out, de)
	}
	if err != nil {
		return err
	}
	return e.render(out, reply, text)
}

func (e *cliEnv) render(w io.Writer, raw []byte, text func(io.Writer, []byte) error) error {
	switch e.format {
	case "json":
		return writeIndented(w, raw)
	case "yaml":
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			return err
		}
		enc := yaml.NewEncoder(w)
		enc.SetIndent(2)
		return enc.Encode(v)
	}
	if text != nil {
		return text(w, raw)
	}
	return writeIndented(w, raw)
}

// failure prints a daemon error in the chosen format and returns it. In text
// mode the diagnostics it carries are listed first, one per line.
func (e *cliEnv) failure(w io.Writer, de *daemonError) error {
	if e.format != "text" {
		_ = e.render(w, de.Body, nil)
		return de
	}
	var details struct {
		Diagnostics []diagnostic.Diagnostic     `json:"diagnostics"`
		Result      *appworkflow.StartRunResult `json:"result"`
	}
	if json.Unmarshal(de.Details, &details) == nil {
		_ = writeWorkflowDiagnostics(w, details.Diagnostics)
		if r := details.Result; r != nil {
			switch {
			case r.RejectedBeforeAdmission():
				_ = writeWorkflowRejectedStart(w, *r)
			case r.Decision.Outcome.Valid() && !r.DryRun && r.Run == nil:
				_ = writeWorkflowDryRunUnsupported(w, *r)
			}
		}
	}
	return de
}

func writeIndented(w io.Writer, raw []byte) error {
	var buf bytes.Buffer
	if json.Indent(&buf, bytes.TrimSpace(raw), "", "  ") != nil {
		buf.Reset()
		buf.Write(raw)
	}
	buf.WriteByte('\n')
	_, err := w.Write(buf.Bytes())
	return err
}

// ---- the daemon client ----

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

// daemonError is a non-2xx reply: the flat {code, message, details?} body.
type daemonError struct {
	Status        int
	Code, Message string
	Details, Body json.RawMessage
}

func (e *daemonError) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("daemon answered %d: %s", e.Status, strings.TrimSpace(string(e.Body)))
	}
	return fmt.Sprintf("%s (%d): %s", e.Code, e.Status, e.Message)
}

func (c *apiClient) post(ctx context.Context, path string, body any, idempotencyKey string) ([]byte, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base.JoinPath(path).String(), bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("daemon unreachable at %s: %w", c.base.Host, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		de := &daemonError{Status: resp.StatusCode, Body: data}
		var flat struct {
			Code, Message string
			Details       json.RawMessage
		}
		if json.Unmarshal(data, &flat) == nil && flat.Code != "" {
			de.Code, de.Message, de.Details = flat.Code, flat.Message, flat.Details
		}
		return nil, de
	}
	return data, nil
}

// ---- shared flag sets ----

type startFlags struct {
	identity                         workflowIdentityFlags
	inputFile, inputJSON, runID, key string
	confirmed                        bool
}

func (f *startFlags) bind(c *cobra.Command) {
	f.identity.bind(c)
	c.Flags().StringVar(&f.inputFile, "input", "", "JSON input object file, or - for stdin")
	c.Flags().StringVar(&f.inputJSON, "input-json", "", "inline JSON input object")
	c.Flags().StringVar(&f.runID, "run-id", "", "run identity (generated when omitted)")
	c.Flags().StringVar(&f.key, "idempotency-key", "", "idempotency key (generated when omitted)")
	c.Flags().BoolVar(&f.confirmed, "confirm", false, "confirm a policy-gated operation")
}

func randomID(prefix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return prefix + "-" + hex.EncodeToString(b[:])
}

func orGenerated(v, prefix string) string {
	if v != "" {
		return v
	}
	return randomID(prefix)
}

func runPath(runID, action string) string {
	return "/api/workflows/runs/" + url.PathEscape(runID) + "/" + action
}

// ---- commands with a dedicated text form ----

func (e *cliEnv) validateCmd() *cobra.Command {
	var id workflowIdentityFlags
	c := &cobra.Command{Use: "validate <file|registry-ref>", Short: "Validate a workflow without starting it", Args: cobra.ExactArgs(1)}
	id.bind(c)
	c.RunE = func(cmd *cobra.Command, a []string) error {
		ref, err := parseWorkflowDefinitionRef(a[0])
		if err != nil {
			return err
		}
		ident, err := id.request()
		if err != nil {
			return err
		}
		return e.do(cmd, "/api/workflows/validate", appworkflow.ValidateWorkflowRequest{Definition: ref, Identity: ident}, "", func(w io.Writer, raw []byte) error {
			var r appworkflow.ValidateWorkflowResult
			if err := json.Unmarshal(raw, &r); err != nil {
				return err
			}
			if len(r.Diagnostics) != 0 {
				if err := writeWorkflowDiagnostics(w, r.Diagnostics); err != nil {
					return err
				}
				return errors.New("workflow validation failed")
			}
			if r.Plan == nil {
				return errors.New("workflow validation returned no plan")
			}
			if _, err := fmt.Fprintf(w, "valid %s@%s (%s)\n", r.Plan.ID, r.Plan.Version, r.Plan.Digest); err != nil {
				return err
			}
			if r.GraphDigest != "" {
				_, err := fmt.Fprintf(w, "graph %s\n", r.GraphDigest)
				return err
			}
			return nil
		})
	}
	return c
}

func (e *cliEnv) startRequest(raw string, f startFlags) (appworkflow.RunWorkflowRequest, error) {
	ref, err := parseWorkflowDefinitionRef(raw)
	if err != nil {
		return appworkflow.RunWorkflowRequest{}, err
	}
	inputs, err := readWorkflowInputs(f.inputFile, f.inputJSON)
	if err != nil {
		return appworkflow.RunWorkflowRequest{}, err
	}
	ident, err := f.identity.request()
	if err != nil {
		return appworkflow.RunWorkflowRequest{}, err
	}
	return appworkflow.RunWorkflowRequest{
		RunID: appworkflow.RunID(orGenerated(f.runID, "run")), Definition: ref, Inputs: inputs,
		IdempotencyKey: orGenerated(f.key, "start"), Identity: ident, Confirmed: f.confirmed,
	}, nil
}

func (e *cliEnv) explainCmd() *cobra.Command {
	var f startFlags
	c := &cobra.Command{Use: "explain <file|registry-ref>", Short: "Policy-check effects, capabilities and blast radius without admitting work", Args: cobra.ExactArgs(1)}
	f.bind(c)
	c.RunE = func(cmd *cobra.Command, a []string) error {
		r, err := e.startRequest(a[0], f)
		if err != nil {
			return err
		}
		body := appworkflow.ExplainWorkflowRequest{RunID: r.RunID, Definition: r.Definition, Inputs: r.Inputs, IdempotencyKey: r.IdempotencyKey, Identity: r.Identity, Confirmed: r.Confirmed}
		return e.do(cmd, "/api/workflows/explain", body, r.IdempotencyKey, func(w io.Writer, raw []byte) error {
			var res appworkflow.StartRunResult
			if err := json.Unmarshal(raw, &res); err != nil {
				return err
			}
			if len(res.Diagnostics) != 0 {
				if err := writeWorkflowDiagnostics(w, res.Diagnostics); err != nil {
					return err
				}
				return errors.New("workflow explanation failed validation")
			}
			if err := validateWorkflowPreview(res); err != nil {
				return err
			}
			return writeWorkflowExplanation(w, res)
		})
	}
	return c
}

func (e *cliEnv) runCmd() *cobra.Command {
	var f startFlags
	var dryRun bool
	var rawPins []string
	c := &cobra.Command{Use: "run <file|registry-ref>", Short: "Start a workflow", Args: cobra.ExactArgs(1)}
	f.bind(c)
	c.Flags().BoolVar(&dryRun, "dry-run", false, "policy-check a side-effect-free preview without admitting work")
	c.Flags().StringArrayVar(&rawPins, "pin", nil, `bind node outputs as node={"id":...,"digest":...} (repeatable)`)
	c.RunE = func(cmd *cobra.Command, a []string) error {
		r, err := e.startRequest(a[0], f)
		if err != nil {
			return err
		}
		pins, err := parseWorkflowPins(rawPins)
		if err != nil {
			return err
		}
		if dryRun && len(pins) != 0 {
			return errors.New("--dry-run and --pin are mutually exclusive")
		}
		r.DryRun, r.Pins = dryRun, pins
		return e.do(cmd, "/api/workflows/runs", r, r.IdempotencyKey, func(w io.Writer, raw []byte) error {
			var res appworkflow.StartRunResult
			if err := json.Unmarshal(raw, &res); err != nil {
				return err
			}
			if len(res.Diagnostics) != 0 {
				if err := writeWorkflowDiagnostics(w, res.Diagnostics); err != nil {
					return err
				}
				return errors.New("workflow run was not admitted")
			}
			if dryRun {
				if err := validateWorkflowPreview(res); err != nil {
					return err
				}
				return writeWorkflowExplanation(w, res)
			}
			if res.Bound == nil || res.Run == nil || res.Phase != hoststate.StartRunning {
				return errors.New("workflow daemon returned an invalid admitted run")
			}
			_, err := fmt.Fprintf(w, "run %s phase=%s status=%s\n", res.Bound.ID, res.Phase, res.Run.Status)
			return err
		})
	}
	return c
}

func (e *cliEnv) inspectCmd() *cobra.Command {
	var id workflowIdentityFlags
	c := &cobra.Command{Use: "inspect <run-id>", Short: "Inspect redacted durable run state", Args: cobra.ExactArgs(1)}
	id.bind(c)
	c.RunE = func(cmd *cobra.Command, a []string) error {
		ident, err := id.request()
		if err != nil {
			return err
		}
		body := appworkflow.InspectWorkflowRunRequest{RunID: appworkflow.RunID(a[0]), Identity: ident}
		return e.do(cmd, runPath(a[0], "inspect"), body, "", func(w io.Writer, raw []byte) error {
			var r rundiagnostics.Result
			if err := json.Unmarshal(raw, &r); err != nil {
				return err
			}
			return writeWorkflowInspection(w, r)
		})
	}
	return c
}

func (e *cliEnv) cancelCmd() *cobra.Command {
	var key, reason string
	c := &cobra.Command{Use: "cancel <run-id>", Short: "Request cancellation of a run", Args: cobra.ExactArgs(1)}
	c.Flags().StringVar(&key, "idempotency-key", "", "idempotency key (generated when omitted)")
	c.Flags().StringVar(&reason, "reason", "", "why the run is canceled")
	c.RunE = func(cmd *cobra.Command, a []string) error {
		k := orGenerated(key, "cancel")
		body := appworkflow.CancelWorkflowRunRequest{RunID: appworkflow.RunID(a[0]), IdempotencyKey: k, Reason: reason}
		return e.do(cmd, runPath(a[0], "cancel"), body, k, nil)
	}
	return c
}

func (e *cliEnv) eventsCmd() *cobra.Command {
	var after string
	var limit, waitMS int
	c := &cobra.Command{Use: "events <run-id>", Short: "Read a run's events after a cursor", Args: cobra.ExactArgs(1)}
	c.Flags().StringVar(&after, "after", "", "next_cursor from an earlier call")
	c.Flags().IntVar(&limit, "limit", 100, "events per call")
	c.Flags().IntVar(&waitMS, "wait-ms", 0, "hold the call until a newer event exists (max 30000)")
	c.RunE = func(cmd *cobra.Command, a []string) error {
		body := appworkflow.WorkflowRunReadRequest{RunID: appworkflow.RunID(a[0]), After: after, EventLimit: limit, WaitMS: waitMS}
		return e.do(cmd, runPath(a[0], "events"), body, "", nil)
	}
	return c
}

func (e *cliEnv) readCmd(action, short string) *cobra.Command {
	return &cobra.Command{
		Use: action + " <run-id>", Short: short, Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, a []string) error {
			return e.do(cmd, runPath(a[0], action), appworkflow.WorkflowRunReadRequest{RunID: appworkflow.RunID(a[0])}, "", nil)
		},
	}
}

func (e *cliEnv) searchCmd() *cobra.Command {
	var namespace string
	var limit int
	c := &cobra.Command{Use: "search [query]", Short: "Search one namespace of the workflow catalog", Args: cobra.MaximumNArgs(1)}
	c.Flags().StringVar(&namespace, "namespace", "", "namespace to search (required: the catalog is searched one namespace at a time)")
	c.Flags().IntVar(&limit, "limit", 20, "matches to return")
	_ = c.MarkFlagRequired("namespace")
	c.RunE = func(cmd *cobra.Command, a []string) error {
		q := ""
		if len(a) == 1 {
			q = a[0]
		}
		return e.do(cmd, "/api/workflows/lifecycle/catalog/search", appworkflow.SearchWorkflowCatalogRequest{Namespace: namespace, Query: q, Limit: limit}, "", nil)
	}
	return c
}
