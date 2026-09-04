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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
	"github.com/hollis-labs/go-workflow/diagnostic"
	"github.com/hollis-labs/go-workflow/graph"
	"github.com/hollis-labs/go-workflow/values"
	workflowwait "github.com/hollis-labs/go-workflow/wait"
	"github.com/spf13/cobra"
)

const (
	maximumWorkflowInputBytes    = 1 << 20
	maximumWorkflowRequestBytes  = 2 << 20
	maximumWorkflowTokenBytes    = 64 << 10
	maximumWorkflowResponseBytes = 8 << 20
)

type workflowCommandDependencies struct {
	service   appworkflow.WorkflowOperations
	lifecycle appworkflow.WorkflowLifecycleOperations
	now       func() time.Time
	random    func([]byte) error
}

func buildWorkflowCmd() *cobra.Command {
	return buildWorkflowCmdWithDependencies(workflowCommandDependencies{
		service:   workflowDaemonClient{baseURL: func() string { return globalAddr }, client: httpClient},
		lifecycle: workflowDaemonClient{baseURL: func() string { return globalAddr }, client: httpClient},
		now:       func() time.Time { return time.Now().UTC() },
		random: func(buffer []byte) error {
			_, err := rand.Read(buffer)
			return err
		},
	})
}

func buildWorkflowCmdWithDependencies(dependencies workflowCommandDependencies) *cobra.Command {
	command := &cobra.Command{Use: "workflow", Short: "Validate and operate graph-native workflows", SilenceUsage: true}
	command.PersistentPreRun = func(command *cobra.Command, _ []string) { command.Root().SilenceUsage = true }
	command.AddCommand(
		buildWorkflowValidateCmd(dependencies), buildWorkflowExplainCmd(dependencies), buildWorkflowRunCmd(dependencies),
		buildWorkflowInspectCmd(dependencies), buildWorkflowCancelCmd(dependencies), buildWorkflowResumeCmd(dependencies),
		buildWorkflowRerunCmd(dependencies),
		buildWorkflowCatalogCmd(dependencies), buildWorkflowAuthorCmd(dependencies),
		buildWorkflowRegistryLifecycleCmd(dependencies), buildWorkflowExposureCmd(dependencies),
	)
	return command
}

type workflowIdentityFlags struct {
	principal, scopeKind, scopeID string
	targetID                      string
	targetKinds                   []string
	targetCapabilities            []string
	targetLabels                  []string
	targetSandboxes               []string
}

func (flags *workflowIdentityFlags) bind(command *cobra.Command) {
	command.Flags().StringVar(&flags.principal, "principal", "", "caller identity hint (authentication remains daemon-owned)")
	command.Flags().StringVar(&flags.scopeKind, "scope-kind", "", "run scope kind: project, account, session, team, or user")
	command.Flags().StringVar(&flags.scopeID, "scope-id", "", "run scope identity")
	command.Flags().StringVar(&flags.targetID, "target-id", "", "exact execution target identity")
	command.Flags().StringSliceVar(&flags.targetKinds, "target-kind", nil, "allowed execution target kind (repeatable)")
	command.Flags().StringSliceVar(&flags.targetCapabilities, "target-capability", nil, "required execution capability (repeatable)")
	command.Flags().StringArrayVar(&flags.targetLabels, "target-label", nil, "required execution target label key=value (repeatable)")
	command.Flags().StringSliceVar(&flags.targetSandboxes, "target-sandbox", nil, "allowed sandbox mode (repeatable)")
}

func (flags workflowIdentityFlags) request() (appworkflow.IdentityRequest, error) {
	request := appworkflow.IdentityRequest{PrincipalHint: flags.principal, SourceAuthority: "cli"}
	if (flags.scopeKind == "") != (flags.scopeID == "") {
		return appworkflow.IdentityRequest{}, errors.New("--scope-kind and --scope-id must be supplied together")
	}
	if flags.scopeKind != "" {
		request.RunScope = &hoststate.RunScopeSelector{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeKind(flags.scopeKind), ID: flags.scopeID}
		if err := request.RunScope.Validate(); err != nil {
			return appworkflow.IdentityRequest{}, fmt.Errorf("run scope: %w", err)
		}
	}
	if flags.targetID != "" || len(flags.targetKinds) != 0 || len(flags.targetCapabilities) != 0 || len(flags.targetLabels) != 0 || len(flags.targetSandboxes) != 0 {
		target := hoststate.ExecutionTargetSelector{Version: hoststate.ScopeTargetVersionV1, ID: flags.targetID, RequiredLabels: make(map[string]string)}
		for _, value := range flags.targetKinds {
			target.Kinds = append(target.Kinds, hoststate.ExecutionTargetKind(value))
		}
		target.RequiredCapabilities = append(target.RequiredCapabilities, flags.targetCapabilities...)
		for _, value := range flags.targetSandboxes {
			target.SandboxModes = append(target.SandboxModes, hoststate.SandboxMode(value))
		}
		for _, label := range flags.targetLabels {
			key, value, ok := strings.Cut(label, "=")
			if !ok || strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
				return appworkflow.IdentityRequest{}, fmt.Errorf("invalid --target-label %q", label)
			}
			if _, duplicate := target.RequiredLabels[key]; duplicate {
				return appworkflow.IdentityRequest{}, fmt.Errorf("duplicate target label %q", key)
			}
			target.RequiredLabels[key] = value
		}
		if len(target.RequiredLabels) == 0 {
			target.RequiredLabels = nil
		}
		sort.Slice(target.Kinds, func(i, j int) bool { return target.Kinds[i] < target.Kinds[j] })
		sort.Strings(target.RequiredCapabilities)
		sort.Slice(target.SandboxModes, func(i, j int) bool { return target.SandboxModes[i] < target.SandboxModes[j] })
		if duplicateStringers(target.Kinds) || duplicateStrings(target.RequiredCapabilities) || duplicateStringers(target.SandboxModes) {
			return appworkflow.IdentityRequest{}, errors.New("execution target selectors must not contain duplicates")
		}
		if err := target.Validate(); err != nil {
			return appworkflow.IdentityRequest{}, fmt.Errorf("execution target: %w", err)
		}
		request.ExecutionTarget = &target
	}
	return request, nil
}

type workflowOutputFlags struct{ json bool }

func (flags *workflowOutputFlags) bind(command *cobra.Command) {
	command.Flags().BoolVar(&flags.json, "json", false, "emit typed JSON")
}

func buildWorkflowValidateCmd(dependencies workflowCommandDependencies) *cobra.Command {
	var identity workflowIdentityFlags
	var output workflowOutputFlags
	command := &cobra.Command{Use: "validate <file|registry-ref>", Short: "Validate a graph-native workflow without starting it", Args: cobra.ExactArgs(1)}
	identity.bind(command)
	output.bind(command)
	command.RunE = func(command *cobra.Command, arguments []string) error {
		ref, err := parseWorkflowDefinitionRef(arguments[0])
		if err != nil {
			return err
		}
		caller, err := identity.request()
		if err != nil {
			return err
		}
		result, err := dependencies.service.ValidateWorkflow(command.Context(), appworkflow.ValidateWorkflowRequest{Definition: ref, Identity: caller})
		if err != nil {
			return err
		}
		if output.json {
			if err := writeWorkflowJSON(command.OutOrStdout(), result); err != nil {
				return err
			}
			if len(result.Diagnostics) != 0 {
				return errors.New("workflow validation failed")
			}
			return nil
		}
		if len(result.Diagnostics) == 0 {
			if result.Plan == nil {
				return errors.New("workflow validation returned no plan")
			}
			_, err := fmt.Fprintf(command.OutOrStdout(), "valid %s@%s (%s)\n", result.Plan.ID, result.Plan.Version, result.Plan.Digest)
			return err
		}
		if err := writeWorkflowDiagnostics(command.OutOrStdout(), result.Diagnostics); err != nil {
			return err
		}
		return errors.New("workflow validation failed")
	}
	return command
}

type workflowStartFlags struct {
	identity                                    workflowIdentityFlags
	output                                      workflowOutputFlags
	inputFile, inputJSON, runID, idempotencyKey string
	confirmed                                   bool
}

func (flags *workflowStartFlags) bind(command *cobra.Command) {
	flags.identity.bind(command)
	flags.output.bind(command)
	command.Flags().StringVar(&flags.inputFile, "input", "", "JSON input object file, or - for stdin")
	command.Flags().StringVar(&flags.inputJSON, "input-json", "", "inline JSON input object")
	command.Flags().StringVar(&flags.runID, "run-id", "", "client-generated run identity")
	command.Flags().StringVar(&flags.idempotencyKey, "idempotency-key", "", "idempotency key")
	command.Flags().BoolVar(&flags.confirmed, "confirm", false, "confirm a policy-gated operation")
}

func buildWorkflowExplainCmd(dependencies workflowCommandDependencies) *cobra.Command {
	var flags workflowStartFlags
	command := &cobra.Command{Use: "explain <file|registry-ref>", Short: "Policy-check effects, capabilities, and blast radius without admitting work", Args: cobra.ExactArgs(1)}
	flags.bind(command)
	command.RunE = func(command *cobra.Command, arguments []string) error {
		ref, inputs, identity, runID, key, err := workflowStartRequest(arguments[0], flags, dependencies)
		if err != nil {
			return err
		}
		result, err := dependencies.service.ExplainWorkflow(command.Context(), appworkflow.ExplainWorkflowRequest{RunID: runID, Definition: ref, Inputs: inputs, IdempotencyKey: key, Identity: identity, Confirmed: flags.confirmed})
		if err != nil {
			if errors.Is(err, appworkflow.ErrDryRunUnsupported) && (result.Decision.Outcome.Valid() || len(result.Diagnostics) != 0) {
				if flags.output.json {
					if writeErr := writeWorkflowJSON(command.OutOrStdout(), result); writeErr != nil {
						return writeErr
					}
				} else if writeErr := writeWorkflowDryRunUnsupported(command.OutOrStdout(), result); writeErr != nil {
					return writeErr
				}
			}
			return err
		}
		if len(result.Diagnostics) != 0 {
			if flags.output.json {
				if err := writeWorkflowJSON(command.OutOrStdout(), result); err != nil {
					return err
				}
			} else {
				if err := writeWorkflowDiagnostics(command.OutOrStdout(), result.Diagnostics); err != nil {
					return err
				}
			}
			return errors.New("workflow explanation failed validation")
		}
		if err := validateWorkflowPreview(result); err != nil {
			return err
		}
		if flags.output.json {
			return writeWorkflowJSON(command.OutOrStdout(), result)
		}
		return writeWorkflowExplanation(command.OutOrStdout(), result)
	}
	return command
}

func buildWorkflowRunCmd(dependencies workflowCommandDependencies) *cobra.Command {
	var flags workflowStartFlags
	var dryRun bool
	var rawPins []string
	command := &cobra.Command{Use: "run <file|registry-ref>", Short: "Start a graph-native workflow", Args: cobra.ExactArgs(1)}
	flags.bind(command)
	command.Flags().BoolVar(&dryRun, "dry-run", false, "policy-check a side-effect-free preview without admitting work")
	command.Flags().StringArrayVar(&rawPins, "pin", nil, "bind node outputs as node={\"id\":...,\"digest\":...} (repeatable)")
	command.RunE = func(command *cobra.Command, arguments []string) error {
		ref, inputs, identity, runID, key, err := workflowStartRequest(arguments[0], flags, dependencies)
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
		result, err := dependencies.service.RunWorkflow(command.Context(), appworkflow.RunWorkflowRequest{RunID: runID, Definition: ref, Inputs: inputs, IdempotencyKey: key, Identity: identity, Confirmed: flags.confirmed, DryRun: dryRun, Pins: pins})
		if err != nil {
			if result.RejectedBeforeAdmission() {
				if flags.output.json {
					if writeErr := writeWorkflowJSON(command.OutOrStdout(), result); writeErr != nil {
						return writeErr
					}
				} else if writeErr := writeWorkflowRejectedStart(command.OutOrStdout(), result); writeErr != nil {
					return writeErr
				}
			}
			if dryRun && errors.Is(err, appworkflow.ErrDryRunUnsupported) && (result.Decision.Outcome.Valid() || len(result.Diagnostics) != 0) {
				if flags.output.json {
					if writeErr := writeWorkflowJSON(command.OutOrStdout(), result); writeErr != nil {
						return writeErr
					}
				} else if writeErr := writeWorkflowDryRunUnsupported(command.OutOrStdout(), result); writeErr != nil {
					return writeErr
				}
			}
			return err
		}
		if len(result.Diagnostics) != 0 {
			if flags.output.json {
				if writeErr := writeWorkflowJSON(command.OutOrStdout(), result); writeErr != nil {
					return writeErr
				}
			} else {
				if writeErr := writeWorkflowDiagnostics(command.OutOrStdout(), result.Diagnostics); writeErr != nil {
					return writeErr
				}
			}
			return errors.New("workflow run was not admitted")
		}
		if dryRun {
			if previewErr := validateWorkflowPreview(result); previewErr != nil {
				return previewErr
			}
		} else if result.Bound == nil || result.Run == nil || result.Phase != hoststate.StartRunning {
			return errors.New("workflow daemon returned an invalid admitted run")
		}
		if flags.output.json {
			return writeWorkflowJSON(command.OutOrStdout(), result)
		}
		if dryRun {
			return writeWorkflowExplanation(command.OutOrStdout(), result)
		}
		status := " status=" + string(result.Run.Status)
		_, err = fmt.Fprintf(command.OutOrStdout(), "run %s phase=%s%s\n", result.Bound.ID, result.Phase, status)
		return err
	}
	return command
}

func buildWorkflowInspectCmd(dependencies workflowCommandDependencies) *cobra.Command {
	var identity workflowIdentityFlags
	var output workflowOutputFlags
	var revealPrivate bool
	command := &cobra.Command{Use: "inspect <run-id>", Short: "Inspect redacted durable graph-native run state", Args: cobra.ExactArgs(1)}
	identity.bind(command)
	output.bind(command)
	command.Flags().BoolVar(&revealPrivate, "reveal-private", false, "reveal private values; secret values remain masked")
	command.RunE = func(command *cobra.Command, arguments []string) error {
		caller, err := identity.request()
		if err != nil {
			return err
		}
		display := values.DisplayPolicy{}
		if revealPrivate {
			display.Private = values.PrivateDisplayReveal
		}
		result, err := dependencies.service.InspectWorkflowRun(command.Context(), appworkflow.InspectWorkflowRunRequest{RunID: appworkflow.RunID(arguments[0]), Identity: caller, Display: display})
		if err != nil {
			return err
		}
		if output.json {
			return writeWorkflowJSON(command.OutOrStdout(), result)
		}
		return writeWorkflowInspection(command.OutOrStdout(), result)
	}
	return command
}

func buildWorkflowCancelCmd(dependencies workflowCommandDependencies) *cobra.Command {
	var identity workflowIdentityFlags
	var output workflowOutputFlags
	var key, reason string
	command := &cobra.Command{Use: "cancel <run-id>", Short: "Request policy-authorized workflow cancellation", Args: cobra.ExactArgs(1)}
	identity.bind(command)
	output.bind(command)
	command.Flags().StringVar(&key, "idempotency-key", "", "idempotency key")
	command.Flags().StringVar(&reason, "reason", "", "operator-visible cancellation reason")
	command.RunE = func(command *cobra.Command, arguments []string) error {
		caller, err := identity.request()
		if err != nil {
			return err
		}
		if key == "" {
			key, err = newWorkflowIdentity(dependencies, "cancel")
		}
		if err != nil {
			return err
		}
		result, err := dependencies.service.CancelWorkflowRun(command.Context(), appworkflow.CancelWorkflowRunRequest{RunID: appworkflow.RunID(arguments[0]), Identity: caller, IdempotencyKey: key, Reason: reason})
		if err != nil {
			return err
		}
		if output.json {
			return writeWorkflowJSON(command.OutOrStdout(), result)
		}
		suffix := ""
		if len(result.Failures) != 0 {
			suffix = fmt.Sprintf(" (%d finalizer failures)", len(result.Failures))
		}
		_, err = fmt.Fprintf(command.OutOrStdout(), "cancellation requested for %s%s\n", arguments[0], suffix)
		return err
	}
	return command
}

func buildWorkflowResumeCmd(dependencies workflowCommandDependencies) *cobra.Command {
	var identity workflowIdentityFlags
	var output workflowOutputFlags
	var waitID, correlation, tokenFile, payloadFile, payloadJSON, key, wakeSource string
	command := &cobra.Command{Use: "resume <run-id>", Short: "Resume an authorized durable workflow wait", Args: cobra.ExactArgs(1)}
	identity.bind(command)
	output.bind(command)
	command.Flags().StringVar(&waitID, "wait", "", "wait identity (required)")
	command.Flags().StringVar(&correlation, "correlation", "", "wait correlation (required)")
	command.Flags().StringVar(&tokenFile, "token-file", "", "file containing the one-time resume token (required)")
	command.Flags().StringVar(&payloadFile, "payload", "", "typed Value JSON file, or - for stdin")
	command.Flags().StringVar(&payloadJSON, "payload-json", "", "inline typed Value JSON")
	command.Flags().StringVar(&wakeSource, "source", "", "wake source: gate, message, callback, or signal")
	command.Flags().StringVar(&key, "idempotency-key", "", "idempotency key")
	command.RunE = func(command *cobra.Command, arguments []string) error {
		if waitID == "" || correlation == "" || tokenFile == "" || wakeSource == "" {
			return errors.New("--wait, --correlation, --token-file, and --source are required")
		}
		caller, err := identity.request()
		if err != nil {
			return err
		}
		tokenBytes, err := readBoundedWorkflowFile(tokenFile, maximumWorkflowTokenBytes, false)
		if err != nil {
			return fmt.Errorf("read token file: %w", err)
		}
		token := strings.TrimSuffix(string(tokenBytes), "\n")
		token = strings.TrimSuffix(token, "\r")
		if token == "" {
			return errors.New("resume token file is empty")
		}
		if strings.ContainsRune(token, '\x00') {
			return errors.New("resume token file contains invalid text")
		}
		payload, err := readTypedWorkflowValue(payloadFile, payloadJSON)
		if err != nil {
			return err
		}
		if key == "" {
			key, err = newWorkflowIdentity(dependencies, "resume")
		}
		if err != nil {
			return err
		}
		result, err := dependencies.service.ResumeWorkflowRun(command.Context(), appworkflow.ResumeWorkflowRunRequest{RunID: appworkflow.RunID(arguments[0]), Identity: caller, WaitID: appworkflow.WaitID(waitID), Correlation: correlation, Token: token, WakeSource: workflowwait.WakeSource(wakeSource), Payload: payload, IdempotencyKey: key})
		if err != nil {
			return redactWorkflowTokenError(err, token)
		}
		if output.json {
			return writeWorkflowJSON(command.OutOrStdout(), result)
		}
		_, err = fmt.Fprintf(command.OutOrStdout(), "wait %s resumed\n", waitID)
		return err
	}
	return command
}

func buildWorkflowRerunCmd(dependencies workflowCommandDependencies) *cobra.Command {
	var identity workflowIdentityFlags
	var output workflowOutputFlags
	var from, runID, key string
	command := &cobra.Command{Use: "rerun <source-run-id>", Short: "Rerun a policy-approved downstream graph slice", Args: cobra.ExactArgs(1)}
	identity.bind(command)
	output.bind(command)
	command.Flags().StringVar(&from, "from", "", "first node to execute again (required)")
	command.Flags().StringVar(&runID, "run-id", "", "new run identity")
	command.Flags().StringVar(&key, "idempotency-key", "", "idempotency key")
	command.RunE = func(command *cobra.Command, arguments []string) error {
		if from == "" {
			return errors.New("--from is required")
		}
		caller, err := identity.request()
		if err != nil {
			return err
		}
		if runID == "" {
			runID, err = newWorkflowIdentity(dependencies, "run")
		}
		if err != nil {
			return err
		}
		if key == "" {
			key, err = newWorkflowIdentity(dependencies, "rerun")
		}
		if err != nil {
			return err
		}
		result, err := dependencies.service.RerunWorkflow(command.Context(), appworkflow.RerunWorkflowRequest{SourceRunID: appworkflow.RunID(arguments[0]), RunID: appworkflow.RunID(runID), FromNodeID: from, IdempotencyKey: key, Identity: caller})
		if err != nil {
			return err
		}
		if output.json {
			return writeWorkflowJSON(command.OutOrStdout(), result)
		}
		_, err = fmt.Fprintf(command.OutOrStdout(), "rerun %s created from %s at %s\n", result.Run.ID, arguments[0], from)
		return err
	}
	return command
}

func workflowStartRequest(rawRef string, flags workflowStartFlags, dependencies workflowCommandDependencies) (graph.DefinitionRef, map[string]any, appworkflow.IdentityRequest, appworkflow.RunID, string, error) {
	ref, err := parseWorkflowDefinitionRef(rawRef)
	if err != nil {
		return graph.DefinitionRef{}, nil, appworkflow.IdentityRequest{}, "", "", err
	}
	inputs, err := readWorkflowInputs(flags.inputFile, flags.inputJSON)
	if err != nil {
		return graph.DefinitionRef{}, nil, appworkflow.IdentityRequest{}, "", "", err
	}
	identity, err := flags.identity.request()
	if err != nil {
		return graph.DefinitionRef{}, nil, appworkflow.IdentityRequest{}, "", "", err
	}
	runID, key := flags.runID, flags.idempotencyKey
	if runID == "" {
		runID, err = newWorkflowIdentity(dependencies, "run")
	}
	if err != nil {
		return graph.DefinitionRef{}, nil, appworkflow.IdentityRequest{}, "", "", err
	}
	if key == "" {
		key, err = newWorkflowIdentity(dependencies, "start")
	}
	if err != nil {
		return graph.DefinitionRef{}, nil, appworkflow.IdentityRequest{}, "", "", err
	}
	return ref, inputs, identity, appworkflow.RunID(runID), key, nil
}

func parseWorkflowDefinitionRef(raw string) (graph.DefinitionRef, error) {
	if !utf8.ValidString(raw) || strings.TrimSpace(raw) != raw || raw == "" || len(raw) > 4096 || strings.IndexFunc(raw, unicode.IsControl) >= 0 {
		return graph.DefinitionRef{}, errors.New("workflow reference is invalid or too large")
	}
	if info, err := os.Stat(raw); err == nil {
		if info.Mode().IsRegular() || info.IsDir() {
			return graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, Locator: filepath.Clean(raw)}, nil
		}
		return graph.DefinitionRef{}, errors.New("workflow file reference is not a regular file or directory")
	}
	if strings.HasSuffix(raw, ".workflow.yaml") || strings.HasSuffix(raw, ".workflow.yml") || raw == "workflow.yaml" || raw == "workflow.yml" || filepath.IsAbs(raw) || strings.HasPrefix(raw, "."+string(filepath.Separator)) {
		return graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, Locator: filepath.Clean(raw)}, nil
	}
	separator := strings.LastIndexByte(raw, '@')
	if separator <= 0 || separator == len(raw)-1 || strings.Count(raw, "@") != 1 {
		return graph.DefinitionRef{}, errors.New("registry workflow reference must be namespace/name@version or namespace/name@sha256:digest")
	}
	name, selector := raw[:separator], raw[separator+1:]
	segments := strings.Split(name, "/")
	if len(segments) < 2 || strings.ContainsAny(name, "\\") {
		return graph.DefinitionRef{}, errors.New("registry workflow identity must be an unambiguous namespace/name")
	}
	for _, segment := range segments {
		if segment == "" || segment == "." || segment == ".." {
			return graph.DefinitionRef{}, errors.New("registry workflow identity must be an unambiguous namespace/name")
		}
	}
	ref := graph.DefinitionRef{Kind: appworkflow.DefinitionKindRegistry, ID: name}
	if strings.HasPrefix(selector, "sha256:") {
		if err := values.ValidateDigest(selector); err != nil {
			return graph.DefinitionRef{}, fmt.Errorf("registry workflow digest: %w", err)
		}
		ref.Digest = selector
	} else {
		if strings.TrimSpace(selector) != selector || strings.IndexFunc(selector, unicode.IsControl) >= 0 {
			return graph.DefinitionRef{}, errors.New("registry workflow version is invalid")
		}
		ref.Version = selector
	}
	return ref, nil
}

func readWorkflowInputs(file, inline string) (map[string]any, error) {
	if file != "" && inline != "" {
		return nil, errors.New("--input and --input-json are mutually exclusive")
	}
	if file == "" && inline == "" {
		return map[string]any{}, nil
	}
	var data []byte
	var err error
	if inline != "" {
		if len(inline) > maximumWorkflowInputBytes {
			return nil, errors.New("inline workflow input exceeds 1 MiB")
		}
		data = []byte(inline)
	} else {
		data, err = readBoundedWorkflowFile(file, maximumWorkflowInputBytes, true)
		if err != nil {
			return nil, fmt.Errorf("read workflow input: %w", err)
		}
	}
	value, err := decodeUniqueWorkflowJSON(data)
	if err != nil {
		return nil, fmt.Errorf("decode workflow input: %w", err)
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("workflow input must be a JSON object")
	}
	return object, nil
}

func readTypedWorkflowValue(file, inline string) (values.Value, error) {
	if file != "" && inline != "" {
		return values.Value{}, errors.New("--payload and --payload-json are mutually exclusive")
	}
	if file == "" && inline == "" {
		return values.Value{}, errors.New("--payload or --payload-json is required")
	}
	var data []byte
	var err error
	if inline != "" {
		if len(inline) > maximumWorkflowInputBytes {
			return values.Value{}, errors.New("inline resume payload exceeds 1 MiB")
		}
		data = []byte(inline)
	} else {
		data, err = readBoundedWorkflowFile(file, maximumWorkflowInputBytes, true)
		if err != nil {
			return values.Value{}, fmt.Errorf("read resume payload: %w", err)
		}
	}
	var value values.Value
	if err := decodeUniqueTypedWorkflowJSON(data, &value); err != nil {
		return values.Value{}, fmt.Errorf("decode typed resume payload: %w", err)
	}
	if err := value.Validate(); err != nil {
		return values.Value{}, fmt.Errorf("validate typed resume payload: %w", err)
	}
	return value, nil
}

func readBoundedWorkflowFile(path string, limit int64, stdin bool) ([]byte, error) {
	var reader io.Reader
	var closeFile func() error
	if path == "-" {
		if !stdin {
			return nil, errors.New("stdin is not accepted here")
		}
		reader = os.Stdin
	} else {
		// #nosec G304 -- the CLI intentionally reads the exact user-selected input or token file.
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		reader, closeFile = file, file.Close
	}
	if closeFile != nil {
		defer func() { _ = closeFile() }()
	}
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("input exceeds %d bytes", limit)
	}
	if !utf8.Valid(data) {
		return nil, errors.New("input is not valid UTF-8")
	}
	return data, nil
}

func decodeUniqueWorkflowJSON(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	value, err := decodeUniqueWorkflowJSONValue(decoder, 0)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("JSON contains trailing data")
		}
		return nil, err
	}
	return value, nil
}

func decodeUniqueTypedWorkflowJSON(data []byte, output any) error {
	decoded, err := decodeUniqueWorkflowJSON(data)
	if err != nil {
		return err
	}
	canonical, err := json.Marshal(decoded)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("typed JSON contains trailing data")
		}
		return err
	}
	return nil
}

func decodeUniqueWorkflowJSONValue(decoder *json.Decoder, depth int) (any, error) {
	if depth > 128 {
		return nil, errors.New("JSON nesting exceeds 128 levels")
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch token := token.(type) {
	case json.Delim:
		switch token {
		case '{':
			object := map[string]any{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, errors.New("JSON object key is not a string")
				}
				if _, exists := object[key]; exists {
					return nil, fmt.Errorf("duplicate JSON key %q", key)
				}
				value, err := decodeUniqueWorkflowJSONValue(decoder, depth+1)
				if err != nil {
					return nil, err
				}
				object[key] = value
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
				return nil, errors.New("unterminated JSON object")
			}
			return object, nil
		case '[':
			array := []any{}
			for decoder.More() {
				value, err := decodeUniqueWorkflowJSONValue(decoder, depth+1)
				if err != nil {
					return nil, err
				}
				array = append(array, value)
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim(']') {
				return nil, errors.New("unterminated JSON array")
			}
			return array, nil
		}
	}
	return token, nil
}

func parseWorkflowPins(raw []string) ([]hoststate.StartPin, error) {
	result := make([]hoststate.StartPin, 0, len(raw))
	seen := map[string]struct{}{}
	totalBytes := 0
	for _, item := range raw {
		totalBytes += len(item)
		if totalBytes > maximumWorkflowInputBytes {
			return nil, errors.New("pinned value references exceed 1 MiB")
		}
		node, encoded, ok := strings.Cut(item, "=")
		if !ok || node == "" || encoded == "" {
			return nil, fmt.Errorf("invalid --pin %q", item)
		}
		if _, duplicate := seen[node]; duplicate {
			return nil, fmt.Errorf("duplicate pin for node %q", node)
		}
		var ref values.ValueSetRef
		if err := decodeUniqueTypedWorkflowJSON([]byte(encoded), &ref); err != nil {
			return nil, fmt.Errorf("pin %s value reference: %w", node, err)
		}
		pin := hoststate.StartPin{NodeID: node, Outputs: ref}
		if err := pin.Validate(); err != nil {
			return nil, err
		}
		seen[node] = struct{}{}
		result = append(result, pin)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].NodeID < result[j].NodeID })
	return result, nil
}

func newWorkflowIdentity(dependencies workflowCommandDependencies, prefix string) (string, error) {
	if dependencies.now == nil || dependencies.random == nil {
		return "", errors.New("workflow command identity generator is unavailable")
	}
	buffer := make([]byte, 8)
	if err := dependencies.random(buffer); err != nil {
		return "", errors.New("generate workflow command identity")
	}
	return fmt.Sprintf("%s-%s-%s", prefix, dependencies.now().UTC().Format("20060102T150405.000000000Z"), hex.EncodeToString(buffer)), nil
}

func duplicateStrings(values []string) bool {
	for index := 1; index < len(values); index++ {
		if values[index-1] == values[index] {
			return true
		}
	}
	return false
}
func duplicateStringers[T ~string](values []T) bool {
	for index := 1; index < len(values); index++ {
		if values[index-1] == values[index] {
			return true
		}
	}
	return false
}

func writeWorkflowJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
func writeWorkflowDiagnostics(writer io.Writer, findings []diagnostic.Diagnostic) error {
	for _, finding := range findings {
		location := ""
		if finding.Source != nil {
			location = fmt.Sprintf(" %s:%d:%d", finding.Source.Locator, finding.Source.StartLine, finding.Source.StartColumn)
		}
		if _, err := fmt.Fprintf(writer, "%s %s%s: %s\n", finding.Severity, finding.Code, location, finding.Message); err != nil {
			return err
		}
	}
	return nil
}
func writeWorkflowExplanation(writer io.Writer, result appworkflow.StartRunResult) error {
	if _, err := fmt.Fprintf(writer, "policy: %s (%s)\neffects: %s\ncapabilities: %s\npreview-record: %s\nwork-admitted: false\ndry-run-supported: %t\n", result.Decision.Outcome, result.Decision.Reason, strings.Join(effectStrings(result.Facts.Effects), ","), strings.Join(result.Facts.RequiredCapabilities, ","), result.Phase, result.Facts.DryRunAvailable); err != nil {
		return err
	}
	keys := make([]string, 0, len(result.Facts.BlastRadius))
	for key := range result.Facts.BlastRadius {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, err := fmt.Fprintf(writer, "blast-radius %s=%d\n", key, result.Facts.BlastRadius[key]); err != nil {
			return err
		}
	}
	return nil
}
func writeWorkflowDryRunUnsupported(writer io.Writer, result appworkflow.StartRunResult) error {
	if _, err := fmt.Fprintf(writer, "policy: %s (%s)\neffects: %s\ncapabilities: %s\nwork-admitted: false\ndry-run-supported: false\n", result.Decision.Outcome, result.Decision.Reason, strings.Join(effectStrings(result.Facts.Effects), ","), strings.Join(result.Facts.RequiredCapabilities, ",")); err != nil {
		return err
	}
	return writeWorkflowDiagnostics(writer, result.Diagnostics)
}
func writeWorkflowRejectedStart(writer io.Writer, result appworkflow.StartRunResult) error {
	_, err := fmt.Fprintf(writer, "run %s phase=%s status=%s\nwork-admitted: false\n", result.Run.ID, result.Phase, result.Run.Status)
	return err
}
func validateWorkflowPreview(result appworkflow.StartRunResult) error {
	if !result.DryRun || result.Phase != hoststate.StartDryRunComplete || result.Bound == nil || result.Run != nil || !result.Facts.DryRunAvailable {
		return errors.New("workflow daemon returned an invalid non-effecting preview")
	}
	return nil
}
func effectStrings(effects graph.EffectSet) []string {
	result := make([]string, len(effects))
	for index, effect := range effects {
		result[index] = string(effect)
	}
	return result
}
func writeWorkflowInspection(writer io.Writer, result rundiagnostics.Result) error {
	if _, err := fmt.Fprintf(writer, "run %s status=%s plan=%s\n", result.Run.ID, result.Run.Status, result.Plan.Digest); err != nil {
		return err
	}
	for _, node := range result.Nodes {
		if _, err := fmt.Fprintf(writer, "%s %s origin=%s\n", node.ID.NodeID, node.Status, node.Origin); err != nil {
			return err
		}
	}
	if len(result.Omissions) != 0 {
		if _, err := fmt.Fprintf(writer, "omissions: %s\n", strings.Join(result.Omissions, ", ")); err != nil {
			return err
		}
	}
	return nil
}
func redactWorkflowTokenError(err error, token string) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	if token != "" {
		message = strings.ReplaceAll(message, token, "[redacted]")
	}
	return errors.New(message)
}

type workflowDaemonClient struct {
	baseURL func() string
	client  *http.Client
}
type workflowDaemonRejectedError struct {
	status int
	code   string
}

func (e *workflowDaemonRejectedError) Error() string {
	return fmt.Sprintf("workflow daemon rejected request (status %d, code %s)", e.status, e.code)
}

func (e *workflowDaemonRejectedError) Unwrap() error {
	switch e.code {
	case appworkflow.WorkflowErrorCodeDryRunUnsupported:
		return appworkflow.ErrDryRunUnsupported
	case appworkflow.WorkflowErrorCodePolicyDenied:
		return appworkflow.ErrPolicyDenied
	case appworkflow.WorkflowErrorCodeConfirmationRequired:
		return appworkflow.ErrConfirmationRequired
	default:
		return nil
	}
}

func (client workflowDaemonClient) post(ctx context.Context, path string, input, output any) error {
	data, err := json.Marshal(input)
	if err != nil {
		return errors.New("encode workflow daemon request")
	}
	if len(data) > maximumWorkflowRequestBytes {
		return errors.New("workflow daemon request exceeds 2 MiB")
	}
	if client.baseURL == nil || client.client == nil {
		return errors.New("workflow daemon client is unavailable")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(client.baseURL(), "/")+path, bytes.NewReader(data))
	if err != nil {
		return errors.New("build workflow daemon request")
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.client.Do(request)
	if err != nil {
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("workflow daemon request failed")
	}
	defer closeBody(response.Body)
	body, err := io.ReadAll(io.LimitReader(response.Body, maximumWorkflowResponseBytes+1))
	if err != nil {
		return errors.New("read workflow daemon response")
	}
	if len(body) > maximumWorkflowResponseBytes {
		return errors.New("workflow daemon response exceeds 8 MiB")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var apiError appworkflow.WorkflowOperationError
		_ = json.Unmarshal(body, &apiError)
		code := strings.TrimSpace(apiError.Code)
		if !validWorkflowDaemonErrorCode(code) {
			code = "request_rejected"
		}
		if typed, ok := output.(*appworkflow.StartRunResult); ok && apiError.Result != nil {
			*typed = *apiError.Result
			if len(typed.Diagnostics) == 0 && len(apiError.Diagnostics) != 0 {
				typed.Diagnostics = append([]diagnostic.Diagnostic(nil), apiError.Diagnostics...)
			}
		}
		return &workflowDaemonRejectedError{status: response.StatusCode, code: code}
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(output); err != nil {
		return errors.New("decode workflow daemon response")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("workflow daemon response contains trailing data")
	}
	return nil
}
func validWorkflowDaemonErrorCode(code string) bool {
	if code == "" || len(code) > 128 {
		return false
	}
	for _, character := range code {
		alphaNumeric := (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9')
		if !alphaNumeric && character != '_' && character != '-' && character != '.' {
			return false
		}
	}
	return true
}
func (client workflowDaemonClient) ValidateWorkflow(ctx context.Context, request appworkflow.ValidateWorkflowRequest) (result appworkflow.ValidateWorkflowResult, err error) {
	err = client.post(ctx, "/v1/workflows/validate", request, &result)
	return
}
func (client workflowDaemonClient) ExplainWorkflow(ctx context.Context, request appworkflow.ExplainWorkflowRequest) (result appworkflow.StartRunResult, err error) {
	err = client.post(ctx, "/v1/workflows/explain", request, &result)
	return
}
func (client workflowDaemonClient) RunWorkflow(ctx context.Context, request appworkflow.RunWorkflowRequest) (result appworkflow.StartRunResult, err error) {
	err = client.post(ctx, "/v1/workflows/runs", request, &result)
	return
}
func (client workflowDaemonClient) InspectWorkflowRun(ctx context.Context, request appworkflow.InspectWorkflowRunRequest) (result rundiagnostics.Result, err error) {
	err = client.post(ctx, "/v1/workflows/runs/"+url.PathEscape(string(request.RunID))+"/inspect", request, &result)
	return
}
func (client workflowDaemonClient) CancelWorkflowRun(ctx context.Context, request appworkflow.CancelWorkflowRunRequest) (result appworkflow.CancelWorkflowRunResult, err error) {
	err = client.post(ctx, "/v1/workflows/runs/"+url.PathEscape(string(request.RunID))+"/cancel", request, &result)
	return
}
func (client workflowDaemonClient) ResumeWorkflowRun(ctx context.Context, request appworkflow.ResumeWorkflowRunRequest) (result appworkflow.ResumeWorkflowRunResult, err error) {
	err = client.post(ctx, "/v1/workflows/runs/"+url.PathEscape(string(request.RunID))+"/resume", request, &result)
	return
}
func (client workflowDaemonClient) RerunWorkflow(ctx context.Context, request appworkflow.RerunWorkflowRequest) (result appworkflow.RerunWorkflowResult, err error) {
	err = client.post(ctx, "/v1/workflows/runs/"+url.PathEscape(string(request.SourceRunID))+"/rerun", request, &result)
	return
}

func (client workflowDaemonClient) SearchWorkflowCatalog(ctx context.Context, request appworkflow.SearchWorkflowCatalogRequest) (result appworkflow.WorkflowCatalogSearchResult, err error) {
	err = client.post(ctx, "/v1/workflows/lifecycle/catalog/search", request, &result)
	return
}
func (client workflowDaemonClient) InspectWorkflowVersion(ctx context.Context, request appworkflow.InspectWorkflowVersionRequest) (result appworkflow.WorkflowVersionDetail, err error) {
	err = client.post(ctx, "/v1/workflows/lifecycle/catalog/inspect", request, &result)
	return
}
func (client workflowDaemonClient) ValidateWorkflowDraft(ctx context.Context, request appworkflow.ValidateWorkflowDraftRequest) (result appworkflow.WorkflowDraftValidationResult, err error) {
	err = client.post(ctx, "/v1/workflows/lifecycle/author/validate", request, &result)
	return
}
func (client workflowDaemonClient) GenerateWorkflowContract(ctx context.Context, request appworkflow.GenerateWorkflowContractRequest) (result appworkflow.WorkflowContractScaffoldResult, err error) {
	err = client.post(ctx, "/v1/workflows/lifecycle/author/scaffold", request, &result)
	return
}
func (client workflowDaemonClient) TestWorkflowDraft(ctx context.Context, request appworkflow.TestWorkflowDraftRequest) (result appworkflow.WorkflowContractTestResult, err error) {
	err = client.post(ctx, "/v1/workflows/lifecycle/author/test", request, &result)
	return
}
func (client workflowDaemonClient) RegisterWorkflowDraft(ctx context.Context, request appworkflow.RegisterWorkflowDraftRequest) (result appworkflow.WorkflowRegistrationResult, err error) {
	err = client.post(ctx, "/v1/workflows/lifecycle/author/register", request, &result)
	return
}
func (client workflowDaemonClient) PackageWorkflowVersion(ctx context.Context, request appworkflow.PackageWorkflowVersionRequest) (result appworkflow.WorkflowPackageResult, err error) {
	err = client.post(ctx, "/v1/workflows/lifecycle/registry/package", request, &result)
	return
}
func (client workflowDaemonClient) PublishWorkflowVersion(ctx context.Context, request appworkflow.MutateWorkflowVersionRequest) (result appworkflow.WorkflowVersionDetail, err error) {
	err = client.post(ctx, "/v1/workflows/lifecycle/registry/publish", request, &result)
	return
}
func (client workflowDaemonClient) PinRegistryVersion(ctx context.Context, request appworkflow.MutateWorkflowVersionRequest) (result appworkflow.WorkflowVersionDetail, err error) {
	err = client.post(ctx, "/v1/workflows/lifecycle/registry/pin-version", request, &result)
	return
}
func (client workflowDaemonClient) UnpinRegistryVersion(ctx context.Context, request appworkflow.MutateWorkflowVersionRequest) (result appworkflow.WorkflowVersionDetail, err error) {
	err = client.post(ctx, "/v1/workflows/lifecycle/registry/unpin-version", request, &result)
	return
}
func (client workflowDaemonClient) ClearWorkflowCurrentExact(ctx context.Context, request appworkflow.MutateWorkflowVersionRequest) (result appworkflow.WorkflowVersionDetail, err error) {
	err = client.post(ctx, "/v1/workflows/lifecycle/registry/clear-current", request, &result)
	return
}
func (client workflowDaemonClient) InspectWorkflowExposure(ctx context.Context, request appworkflow.InspectWorkflowExposureRequest) (result hoststate.ExposureProfileSnapshot, err error) {
	err = client.post(ctx, "/v1/workflows/lifecycle/exposure/inspect", request, &result)
	return
}
func (client workflowDaemonClient) PinWorkflowExposure(ctx context.Context, request appworkflow.MutateWorkflowExposureRequest) (result hoststate.ExposureProfileSnapshot, err error) {
	err = client.post(ctx, "/v1/workflows/lifecycle/exposure/pin-definition", request, &result)
	return
}
func (client workflowDaemonClient) UnpinWorkflowExposure(ctx context.Context, request appworkflow.MutateWorkflowExposureRequest) (result hoststate.ExposureProfileSnapshot, err error) {
	err = client.post(ctx, "/v1/workflows/lifecycle/exposure/unpin-definition", request, &result)
	return
}

var _ appworkflow.WorkflowOperations = workflowDaemonClient{}
var _ appworkflow.WorkflowLifecycleOperations = workflowDaemonClient{}
