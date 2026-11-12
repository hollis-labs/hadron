package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
)

// jsonArg reads a JSON value from a file, or from stdin when path is "-".
func jsonArg(path string, in io.Reader) (json.RawMessage, error) {
	var data []byte
	var err error
	if path == "-" {
		data, err = io.ReadAll(in)
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		return nil, err
	}
	if !json.Valid(data) {
		return nil, fmt.Errorf("%s does not hold valid JSON", path)
	}
	return data, nil
}

// addWaitCommands adds resume and signal: the calls that wake a waiting run.
func addWaitCommands(ctx context.Context, c *cobra.Command, client func() (*apiClient, error)) {
	var waitID, correlation, token, payload, wake, key string
	resume := &cobra.Command{
		Use: "resume RUN_ID", Short: "Resume a waiting run with a typed payload", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, a []string) error {
			cl, err := client()
			if err != nil {
				return err
			}
			p, err := jsonArg(payload, cmd.InOrStdin())
			if err != nil {
				return err
			}
			k := orGenerated(key, "cli-resume")
			return cl.post(ctx, cmd.OutOrStdout(), runPath(a[0], "resume"), map[string]any{
				"run_id": a[0], "identity": map[string]any{}, "wait_id": waitID, "correlation": correlation,
				"token": token, "wake_source": wake, "payload": p, "idempotency_key": k,
			}, k)
		},
	}
	resume.Flags().StringVar(&waitID, "wait-id", "", "the wait to resume")
	resume.Flags().StringVar(&correlation, "correlation", "", "the wait's correlation")
	resume.Flags().StringVar(&token, "token", "", "the wait's resume token, when it has one")
	resume.Flags().StringVar(&wake, "wake-source", "callback", "callback, gate or message")
	resume.Flags().StringVar(&payload, "payload", "", "file holding the typed payload JSON ('-' for stdin)")
	resume.Flags().StringVar(&key, "idempotency-key", "", "idempotency key (generated when omitted)")
	for _, f := range []string{"wait-id", "correlation", "payload"} {
		_ = resume.MarkFlagRequired(f)
	}

	var name, sigCorrelation, sigPayload, sigKey string
	var confirmed bool
	signal := &cobra.Command{
		Use: "signal RUN_ID", Short: "Deliver a named signal to a run", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, a []string) error {
			cl, err := client()
			if err != nil {
				return err
			}
			p, err := jsonArg(sigPayload, cmd.InOrStdin())
			if err != nil {
				return err
			}
			k := orGenerated(sigKey, "cli-signal")
			return cl.post(ctx, cmd.OutOrStdout(), runPath(a[0], "signal"), map[string]any{
				"run_id": a[0], "identity": map[string]any{}, "name": name, "correlation": sigCorrelation,
				"payload": p, "idempotency_key": k, "confirmed": confirmed,
			}, k)
		},
	}
	signal.Flags().StringVar(&name, "name", "", "signal name")
	signal.Flags().StringVar(&sigCorrelation, "correlation", "", "signal correlation")
	signal.Flags().StringVar(&sigPayload, "payload", "", "file holding the typed payload JSON ('-' for stdin)")
	signal.Flags().StringVar(&sigKey, "idempotency-key", "", "idempotency key (generated when omitted)")
	signal.Flags().BoolVar(&confirmed, "confirmed", false, "confirm a step that needs the operator's confirmation")
	for _, f := range []string{"name", "correlation", "payload"} {
		_ = signal.MarkFlagRequired(f)
	}

	// rerun, waits and values round out the per-run calls.
	var fromNode, newRunID, rerunKey string
	rerun := &cobra.Command{
		Use: "rerun RUN_ID", Short: "Start a new run from a node of an earlier run", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, a []string) error {
			cl, err := client()
			if err != nil {
				return err
			}
			k := orGenerated(rerunKey, "cli-rerun")
			return cl.post(ctx, cmd.OutOrStdout(), runPath(a[0], "rerun"), map[string]any{
				"source_run_id": a[0], "run_id": orGenerated(newRunID, "cli-run"), "from_node_id": fromNode,
				"idempotency_key": k, "identity": map[string]any{},
			}, k)
		},
	}
	rerun.Flags().StringVar(&fromNode, "from-node", "", "node id to restart from")
	rerun.Flags().StringVar(&newRunID, "run-id", "", "id of the new run (generated when omitted)")
	rerun.Flags().StringVar(&rerunKey, "idempotency-key", "", "idempotency key (generated when omitted)")
	_ = rerun.MarkFlagRequired("from-node")
	read := func(action, short string) *cobra.Command {
		return &cobra.Command{
			Use: action + " RUN_ID", Short: short, Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, a []string) error {
				cl, err := client()
				if err != nil {
					return err
				}
				return cl.post(ctx, cmd.OutOrStdout(), runPath(a[0], action), map[string]any{"run_id": a[0], "identity": map[string]any{}}, "")
			},
		}
	}
	c.AddCommand(resume, signal, rerun, read("waits", "List a run's waits"), read("values", "Fetch a run's values"))
}

// addLifecycleCommands adds catalog inspect, authoring, registry and exposure.
func addLifecycleCommands(ctx context.Context, c *cobra.Command, client func() (*apiClient, error)) {
	const base = "/api/workflows/lifecycle/"
	call := func(path string, body func(cmd *cobra.Command, a []string) (map[string]any, error)) func(*cobra.Command, []string) error {
		return func(cmd *cobra.Command, a []string) error {
			cl, err := client()
			if err != nil {
				return err
			}
			b, err := body(cmd, a)
			if err != nil {
				return err
			}
			b["identity"] = map[string]any{}
			return cl.post(ctx, cmd.OutOrStdout(), base+path, b, "")
		}
	}
	defOnly := func(_ *cobra.Command, a []string) (map[string]any, error) {
		return map[string]any{"definition": definition(a)}, nil
	}

	inspect := &cobra.Command{Use: "inspect-version NAME VERSION DIGEST", Short: "Inspect one exact catalog version", Args: cobra.ExactArgs(3), RunE: call("catalog/inspect", defOnly)}

	// author: a draft (and, to test or register, a contract suite) from JSON files.
	var draftFile, suiteFile string
	var makeCurrent bool
	authorBody := func(withSuite bool) func(*cobra.Command, []string) (map[string]any, error) {
		return func(cmd *cobra.Command, _ []string) (map[string]any, error) {
			d, err := jsonArg(draftFile, cmd.InOrStdin())
			if err != nil {
				return nil, err
			}
			b := map[string]any{"draft": d}
			if withSuite {
				if suiteFile == "" {
					return nil, fmt.Errorf("--suite is required")
				}
				s, err := jsonArg(suiteFile, cmd.InOrStdin())
				if err != nil {
					return nil, err
				}
				b["suite"] = s
			}
			if makeCurrent {
				b["make_current"] = true
			}
			return b, nil
		}
	}
	author := &cobra.Command{Use: "author", Short: "Author and register workflow drafts"}
	for _, spec := range []struct {
		use, short, path string
		suite            bool
	}{
		{"validate", "Validate a draft without changing the catalog", "author/validate", false},
		{"scaffold", "Validate a draft and generate a contract-test scaffold", "author/scaffold", false},
		{"test", "Validate a draft and run its contract suite", "author/test", true},
		{"register", "Validate, test and register a draft as an exact immutable version", "author/register", true},
	} {
		sub := &cobra.Command{Use: spec.use, Short: spec.short, Args: cobra.NoArgs, RunE: call(spec.path, authorBody(spec.suite))}
		sub.Flags().StringVar(&draftFile, "draft", "", "file holding the draft JSON ('-' for stdin)")
		_ = sub.MarkFlagRequired("draft")
		if spec.suite {
			sub.Flags().StringVar(&suiteFile, "suite", "", "file holding the contract suite JSON")
		}
		if spec.use == "register" {
			sub.Flags().BoolVar(&makeCurrent, "make-current", false, "also move the current alias to this version")
		}
		author.AddCommand(sub)
	}

	registry := &cobra.Command{Use: "registry", Short: "Publish and pin exact workflow versions"}
	for _, spec := range []struct{ use, short, path string }{
		{"publish", "Publish one exact version", "registry/publish"},
		{"pin", "Pin one exact version", "registry/pin-version"},
		{"unpin", "Unpin one exact version", "registry/unpin-version"},
		{"clear-current", "Clear the current alias if it names this exact version", "registry/clear-current"},
	} {
		registry.AddCommand(&cobra.Command{Use: spec.use + " NAME VERSION DIGEST", Short: spec.short, Args: cobra.ExactArgs(3), RunE: call(spec.path, defOnly)})
	}
	var pkgSuite string
	pkg := &cobra.Command{
		Use: "package NAME VERSION DIGEST", Short: "Package one exact version with its contract suite", Args: cobra.ExactArgs(3),
		RunE: call("registry/package", func(cmd *cobra.Command, a []string) (map[string]any, error) {
			s, err := jsonArg(pkgSuite, cmd.InOrStdin())
			if err != nil {
				return nil, err
			}
			return map[string]any{"definition": definition(a), "suite": s}, nil
		}),
	}
	pkg.Flags().StringVar(&pkgSuite, "suite", "", "file holding the contract suite JSON")
	_ = pkg.MarkFlagRequired("suite")
	registry.AddCommand(pkg)

	exposure := &cobra.Command{Use: "exposure", Short: "Inspect and change exposure profiles"}
	exposure.AddCommand(&cobra.Command{
		Use: "inspect PROFILE_ID", Short: "Inspect an exposure profile", Args: cobra.ExactArgs(1),
		RunE: call("exposure/inspect", func(_ *cobra.Command, a []string) (map[string]any, error) {
			return map[string]any{"profile_id": a[0]}, nil
		}),
	})
	var generation uint64
	for _, spec := range []struct{ use, short, path string }{
		{"pin", "Pin one exact definition in a profile", "exposure/pin-definition"},
		{"unpin", "Unpin one exact definition from a profile", "exposure/unpin-definition"},
	} {
		sub := &cobra.Command{
			Use: spec.use + " PROFILE_ID NAME VERSION DIGEST", Short: spec.short, Args: cobra.ExactArgs(4),
			RunE: call(spec.path, func(_ *cobra.Command, a []string) (map[string]any, error) {
				return map[string]any{"profile_id": a[0], "definition": definition(a[1:]), "expected_generation": generation}, nil
			}),
		}
		sub.Flags().Uint64Var(&generation, "expected-generation", 0, "the profile generation this change is based on (from inspect)")
		_ = sub.MarkFlagRequired("expected-generation")
		exposure.AddCommand(sub)
	}
	c.AddCommand(inspect, author, registry, exposure)
}
