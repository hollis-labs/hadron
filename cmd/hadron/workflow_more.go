package main

import (
	"encoding/json"
	"fmt"

	"github.com/hollis-labs/libs/workflow/engine/graph"
	"github.com/spf13/cobra"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
)

// payloadFlags read a typed value from --payload FILE ('-' for stdin) or --payload-json.
type payloadFlags struct{ file, inline string }

func (p *payloadFlags) bind(c *cobra.Command) {
	c.Flags().StringVar(&p.file, "payload", "", "file holding the typed payload JSON, or - for stdin")
	c.Flags().StringVar(&p.inline, "payload-json", "", "inline typed payload JSON")
}

func (e *cliEnv) resumeCmd() *cobra.Command {
	var waitID, correlation, token, wake, key string
	var id workflowIdentityFlags
	var pl payloadFlags
	c := &cobra.Command{Use: "resume <run-id>", Short: "Resume a waiting run with a typed payload", Args: cobra.ExactArgs(1)}
	id.bind(c)
	pl.bind(c)
	c.Flags().StringVar(&waitID, "wait-id", "", "the wait to resume")
	c.Flags().StringVar(&correlation, "correlation", "", "the wait's correlation")
	c.Flags().StringVar(&token, "token", "", "the wait's resume token, when it has one")
	c.Flags().StringVar(&wake, "wake-source", "callback", "callback, gate or message")
	c.Flags().StringVar(&key, "idempotency-key", "", "idempotency key (generated when omitted)")
	_ = c.MarkFlagRequired("wait-id")
	_ = c.MarkFlagRequired("correlation")
	c.RunE = func(cmd *cobra.Command, a []string) error {
		payload, err := readTypedWorkflowValue(pl.file, pl.inline)
		if err != nil {
			return err
		}
		ident, err := id.request()
		if err != nil {
			return err
		}
		k := orGenerated(key, "resume")
		body := appworkflow.ResumeWorkflowRunRequest{
			RunID: appworkflow.RunID(a[0]), Identity: ident, WaitID: appworkflow.WaitID(waitID), Correlation: correlation,
			Token: token, WakeSource: appworkflow.WorkflowWakeSource(wake), Payload: payload, IdempotencyKey: k,
		}
		return e.do(cmd, runPath(a[0], "resume"), body, k, nil)
	}
	return c
}

func (e *cliEnv) signalCmd() *cobra.Command {
	var name, correlation, key string
	var confirmed bool
	var id workflowIdentityFlags
	var pl payloadFlags
	c := &cobra.Command{Use: "signal <run-id>", Short: "Deliver a named signal to a run", Args: cobra.ExactArgs(1)}
	id.bind(c)
	pl.bind(c)
	c.Flags().StringVar(&name, "name", "", "signal name")
	c.Flags().StringVar(&correlation, "correlation", "", "signal correlation")
	c.Flags().StringVar(&key, "idempotency-key", "", "idempotency key (generated when omitted)")
	c.Flags().BoolVar(&confirmed, "confirm", false, "confirm a policy-gated operation")
	_ = c.MarkFlagRequired("name")
	_ = c.MarkFlagRequired("correlation")
	c.RunE = func(cmd *cobra.Command, a []string) error {
		payload, err := readTypedWorkflowValue(pl.file, pl.inline)
		if err != nil {
			return err
		}
		ident, err := id.request()
		if err != nil {
			return err
		}
		k := orGenerated(key, "signal")
		body := appworkflow.SignalWorkflowRunRequest{
			RunID: appworkflow.RunID(a[0]), Name: name, Correlation: correlation, Payload: payload,
			IdempotencyKey: k, Identity: ident, Confirmed: confirmed,
		}
		return e.do(cmd, runPath(a[0], "signal"), body, k, nil)
	}
	return c
}

func (e *cliEnv) rerunCmd() *cobra.Command {
	var fromNode, newRunID, key string
	var id workflowIdentityFlags
	c := &cobra.Command{Use: "rerun <source-run-id>", Short: "Start a new run from a node of an earlier run", Args: cobra.ExactArgs(1)}
	id.bind(c)
	c.Flags().StringVar(&fromNode, "from-node", "", "node id to restart from")
	c.Flags().StringVar(&newRunID, "run-id", "", "id of the new run (generated when omitted)")
	c.Flags().StringVar(&key, "idempotency-key", "", "idempotency key (generated when omitted)")
	_ = c.MarkFlagRequired("from-node")
	c.RunE = func(cmd *cobra.Command, a []string) error {
		ident, err := id.request()
		if err != nil {
			return err
		}
		k := orGenerated(key, "rerun")
		body := appworkflow.RerunWorkflowRequest{
			SourceRunID: appworkflow.RunID(a[0]), RunID: appworkflow.RunID(orGenerated(newRunID, "run")),
			FromNodeID: fromNode, IdempotencyKey: k, Identity: ident,
		}
		return e.do(cmd, runPath(a[0], "rerun"), body, k, nil)
	}
	return c
}

// addLifecycle adds the catalog, authoring, registry and exposure commands.
// They take the exact reference namespace/name@version#sha256:digest.
func (e *cliEnv) addLifecycle(root *cobra.Command) {
	const base = "/api/workflows/lifecycle/"
	exact := func(raw string) (graph.DefinitionRef, error) { return parseExactWorkflowRegistryRef(raw) }
	refCmd := func(use, short, path string, build func(graph.DefinitionRef) any) *cobra.Command {
		return &cobra.Command{
			Use: use + " <namespace/name@version#sha256:digest>", Short: short, Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, a []string) error {
				ref, err := exact(a[0])
				if err != nil {
					return err
				}
				return e.do(cmd, base+path, build(ref), "", nil)
			},
		}
	}
	mutate := func(ref graph.DefinitionRef) any { return appworkflow.MutateWorkflowVersionRequest{Definition: ref} }

	inspect := refCmd("inspect-version", "Inspect one exact catalog version", "catalog/inspect",
		func(ref graph.DefinitionRef) any { return appworkflow.InspectWorkflowVersionRequest{Definition: ref} })

	// author: a draft (and, to test or register, a contract suite) from JSON files.
	var draftFile, suiteFile string
	var makeCurrent bool
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
		sub := &cobra.Command{Use: spec.use + " <authoring-envelope.json>", Short: spec.short, Args: cobra.NoArgs}
		sub.Use = spec.use
		sub.Flags().StringVar(&draftFile, "draft", "", "file holding the draft JSON, or - for stdin")
		_ = sub.MarkFlagRequired("draft")
		if spec.suite {
			sub.Flags().StringVar(&suiteFile, "suite", "", "file holding the contract suite JSON")
			_ = sub.MarkFlagRequired("suite")
		}
		if spec.use == "register" {
			sub.Flags().BoolVar(&makeCurrent, "make-current", false, "also move the current alias to this version")
		}
		spec := spec
		sub.RunE = func(cmd *cobra.Command, _ []string) error {
			var body struct {
				Draft       any  `json:"draft"`
				Suite       any  `json:"suite,omitempty"`
				MakeCurrent bool `json:"make_current,omitempty"`
			}
			d, err := jsonArg(draftFile, cmd.InOrStdin())
			if err != nil {
				return err
			}
			body.Draft = d
			if spec.suite {
				s, err := jsonArg(suiteFile, cmd.InOrStdin())
				if err != nil {
					return err
				}
				body.Suite = s
			}
			body.MakeCurrent = makeCurrent
			return e.do(cmd, base+spec.path, body, "", nil)
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
		registry.AddCommand(refCmd(spec.use, spec.short, spec.path, mutate))
	}
	var pkgSuite string
	pkg := refCmd("package", "Package one exact version with its contract suite", "registry/package", nil)
	pkg.Flags().StringVar(&pkgSuite, "suite", "", "file holding the contract suite JSON")
	_ = pkg.MarkFlagRequired("suite")
	pkg.RunE = func(cmd *cobra.Command, a []string) error {
		ref, err := exact(a[0])
		if err != nil {
			return err
		}
		s, err := jsonArg(pkgSuite, cmd.InOrStdin())
		if err != nil {
			return err
		}
		return e.do(cmd, base+"registry/package", map[string]any{"definition": ref, "suite": s}, "", nil)
	}
	registry.AddCommand(pkg)

	exposure := &cobra.Command{Use: "exposure", Short: "Inspect and change exposure profiles"}
	exposure.AddCommand(&cobra.Command{
		Use: "inspect <profile-id>", Short: "Inspect an exposure profile", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, a []string) error {
			return e.do(cmd, base+"exposure/inspect", appworkflow.InspectWorkflowExposureRequest{ProfileID: a[0]}, "", nil)
		},
	})
	var putGeneration uint64
	put := &cobra.Command{
		Use: "put <profile.json>", Short: "Create (--expected-generation 0) or replace an exposure profile; callers hold it with the scope hadron.profile.<id>", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, a []string) error {
			p, err := jsonArg(a[0], cmd.InOrStdin())
			if err != nil {
				return err
			}
			var rec hoststate.ExposureProfileRecord
			if err := json.Unmarshal(p, &rec); err != nil {
				return fmt.Errorf("%s is not an exposure profile: %w", a[0], err)
			}
			return e.do(cmd, base+"exposure/put-profile", appworkflow.PutWorkflowExposureProfileRequest{Profile: rec, ExpectedGeneration: putGeneration}, "", nil)
		},
	}
	put.Flags().Uint64Var(&putGeneration, "expected-generation", 0, "the generation being replaced; 0 creates")
	var delGeneration uint64
	del := &cobra.Command{
		Use: "delete <profile-id>", Short: "Delete an exposure profile; callers holding its claim then use nothing", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, a []string) error {
			return e.do(cmd, base+"exposure/delete-profile", appworkflow.DeleteWorkflowExposureProfileRequest{ProfileID: a[0], ExpectedGeneration: delGeneration}, "", nil)
		},
	}
	del.Flags().Uint64Var(&delGeneration, "expected-generation", 0, "the generation being deleted (from inspect)")
	_ = del.MarkFlagRequired("expected-generation")
	exposure.AddCommand(put, del)
	var generation uint64
	for _, spec := range []struct{ use, short, path string }{
		{"pin", "Pin one exact definition in a profile", "exposure/pin-definition"},
		{"unpin", "Unpin one exact definition from a profile", "exposure/unpin-definition"},
	} {
		spec := spec
		sub := &cobra.Command{
			Use: spec.use + " <profile-id> <namespace/name@version#sha256:digest>", Short: spec.short, Args: cobra.ExactArgs(2),
			RunE: func(cmd *cobra.Command, a []string) error {
				ref, err := exact(a[1])
				if err != nil {
					return err
				}
				body := appworkflow.MutateWorkflowExposureRequest{ProfileID: a[0], Definition: ref, ExpectedGeneration: generation}
				return e.do(cmd, base+spec.path, body, "", nil)
			},
		}
		sub.Flags().Uint64Var(&generation, "expected-generation", 0, "the profile generation this change is based on (from inspect)")
		_ = sub.MarkFlagRequired("expected-generation")
		exposure.AddCommand(sub)
	}
	root.AddCommand(inspect, author, registry, exposure)
}
