package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/go-workflow/graph"
	"github.com/hollis-labs/go-workflow/values"
	"github.com/spf13/cobra"
)

func buildWorkflowCatalogCmd(dependencies workflowCommandDependencies) *cobra.Command {
	command := &cobra.Command{Use: "catalog", Short: "Search and inspect qualified workflow versions"}
	command.AddCommand(buildWorkflowCatalogSearchCmd(dependencies), buildWorkflowCatalogInspectCmd(dependencies))
	return command
}

func buildWorkflowCatalogSearchCmd(dependencies workflowCommandDependencies) *cobra.Command {
	var identity workflowIdentityFlags
	var output workflowOutputFlags
	var namespace string
	var limit int
	command := &cobra.Command{Use: "search [query]", Short: "Return bounded ranked workflow recommendations", Args: cobra.MaximumNArgs(1)}
	identity.bind(command)
	output.bind(command)
	command.Flags().StringVar(&namespace, "namespace", "", "restrict results to one authorized namespace")
	command.Flags().IntVar(&limit, "limit", 20, "maximum results (1-100)")
	command.RunE = func(command *cobra.Command, arguments []string) error {
		service, err := requireWorkflowLifecycle(dependencies)
		if err != nil {
			return err
		}
		caller, err := identity.request()
		if err != nil {
			return err
		}
		query := ""
		if len(arguments) == 1 {
			query = arguments[0]
		}
		result, err := service.SearchWorkflowCatalog(command.Context(), appworkflow.SearchWorkflowCatalogRequest{Namespace: namespace, Query: query, Limit: limit, Identity: caller})
		if err != nil {
			return err
		}
		if output.json {
			return writeWorkflowJSON(command.OutOrStdout(), result)
		}
		for _, match := range result.Matches {
			if _, writeErr := fmt.Fprintf(command.OutOrStdout(), "%s@%s %s score=%d current=%t registry-pinned=%t published=%t next=%s\n", match.Name, match.Definition.Version, match.Definition.Digest, match.Score, match.Registry.Current, match.Registry.RegistryPinned, match.Registry.Published, match.RecommendedNext); writeErr != nil {
				return writeErr
			}
		}
		_, err = fmt.Fprintf(command.OutOrStdout(), "next-step: %s\n", result.NextStep)
		return err
	}
	return command
}

func buildWorkflowCatalogInspectCmd(dependencies workflowCommandDependencies) *cobra.Command {
	var identity workflowIdentityFlags
	var output workflowOutputFlags
	command := &cobra.Command{Use: "inspect <namespace/name@version#sha256:digest>", Short: "Inspect one exact qualified workflow version", Args: cobra.ExactArgs(1)}
	identity.bind(command)
	output.bind(command)
	command.RunE = func(command *cobra.Command, arguments []string) error {
		service, err := requireWorkflowLifecycle(dependencies)
		if err != nil {
			return err
		}
		ref, err := parseExactWorkflowRegistryRef(arguments[0])
		if err != nil {
			return err
		}
		caller, err := identity.request()
		if err != nil {
			return err
		}
		result, err := service.InspectWorkflowVersion(command.Context(), appworkflow.InspectWorkflowVersionRequest{Definition: ref, Identity: caller})
		if err != nil {
			return err
		}
		return writeWorkflowVersionDetail(command, output, result)
	}
	return command
}

func buildWorkflowAuthorCmd(dependencies workflowCommandDependencies) *cobra.Command {
	command := &cobra.Command{Use: "author", Short: "Validate, contract-test, and register graph-native authoring envelopes"}
	command.AddCommand(
		buildWorkflowDraftCommand(dependencies, "validate"),
		buildWorkflowDraftCommand(dependencies, "scaffold"),
		buildWorkflowDraftCommand(dependencies, "test"),
		buildWorkflowDraftCommand(dependencies, "register"),
	)
	return command
}

func buildWorkflowDraftCommand(dependencies workflowCommandDependencies, action string) *cobra.Command {
	var identity workflowIdentityFlags
	var output workflowOutputFlags
	var id, version, namespace, suiteFile string
	var makeCurrent bool
	command := &cobra.Command{Use: action + " <authoring-envelope.json>", Short: workflowDraftShort(action), Args: cobra.ExactArgs(1)}
	identity.bind(command)
	output.bind(command)
	command.Flags().StringVar(&id, "id", "", "source-local workflow ID")
	command.Flags().StringVar(&version, "version", "", "immutable workflow version")
	command.Flags().StringVar(&namespace, "namespace", "", "authorized registry namespace")
	_ = command.MarkFlagRequired("id")
	_ = command.MarkFlagRequired("version")
	_ = command.MarkFlagRequired("namespace")
	if action == "test" || action == "register" {
		command.Flags().StringVar(&suiteFile, "suite", "", "completed contract suite JSON")
		_ = command.MarkFlagRequired("suite")
	}
	if action == "register" {
		command.Flags().BoolVar(&makeCurrent, "make-current", false, "move the catalog current alias to this qualified version")
	}
	command.RunE = func(command *cobra.Command, arguments []string) error {
		service, err := requireWorkflowLifecycle(dependencies)
		if err != nil {
			return err
		}
		envelope, err := readBoundedWorkflowFile(arguments[0], maximumWorkflowRequestBytes, true)
		if err != nil {
			return err
		}
		if _, decodeErr := decodeUniqueWorkflowJSON(envelope); decodeErr != nil {
			return fmt.Errorf("authoring envelope: %w", decodeErr)
		}
		caller, err := identity.request()
		if err != nil {
			return err
		}
		draft := appworkflow.WorkflowDraft{Envelope: envelope, ID: id, Version: version, Namespace: namespace}
		var result any
		switch action {
		case "validate":
			result, err = service.ValidateWorkflowDraft(command.Context(), appworkflow.ValidateWorkflowDraftRequest{Draft: draft, Identity: caller})
		case "scaffold":
			result, err = service.GenerateWorkflowContract(command.Context(), appworkflow.GenerateWorkflowContractRequest{Draft: draft, Identity: caller})
		case "test", "register":
			var suite appworkflow.WorkflowContractSuite
			if err = readWorkflowLifecycleJSONFile(suiteFile, &suite); err != nil {
				return err
			}
			if action == "test" {
				result, err = service.TestWorkflowDraft(command.Context(), appworkflow.TestWorkflowDraftRequest{Draft: draft, Suite: suite, Identity: caller})
			} else {
				result, err = service.RegisterWorkflowDraft(command.Context(), appworkflow.RegisterWorkflowDraftRequest{Draft: draft, Suite: suite, MakeCurrent: makeCurrent, Identity: caller})
			}
		}
		if err != nil {
			return err
		}
		if output.json {
			return writeWorkflowJSON(command.OutOrStdout(), result)
		}
		return writeWorkflowJSON(command.OutOrStdout(), result)
	}
	return command
}

func workflowDraftShort(action string) string {
	switch action {
	case "validate":
		return "Validate a bounded graph-native authoring envelope without mutation"
	case "scaffold":
		return "Generate a deterministic editable contract scaffold"
	case "test":
		return "Execute deterministic contract tests without registration"
	default:
		return "Register an exactly qualified immutable workflow version"
	}
}

func buildWorkflowRegistryLifecycleCmd(dependencies workflowCommandDependencies) *cobra.Command {
	command := &cobra.Command{Use: "registry", Short: "Manage exact registry qualification, publication, packaging, and current state"}
	for _, action := range []string{"pin-version", "unpin-version", "publish", "clear-current"} {
		command.AddCommand(buildWorkflowRegistryMutationCmd(dependencies, action))
	}
	command.AddCommand(buildWorkflowPackageCmd(dependencies))
	return command
}

func buildWorkflowRegistryMutationCmd(dependencies workflowCommandDependencies, action string) *cobra.Command {
	var identity workflowIdentityFlags
	var output workflowOutputFlags
	command := &cobra.Command{Use: action + " <namespace/name@version#sha256:digest>", Short: "Mutate one exact registry state without conflating current or exposure pins", Args: cobra.ExactArgs(1)}
	identity.bind(command)
	output.bind(command)
	command.RunE = func(command *cobra.Command, arguments []string) error {
		service, err := requireWorkflowLifecycle(dependencies)
		if err != nil {
			return err
		}
		ref, err := parseExactWorkflowRegistryRef(arguments[0])
		if err != nil {
			return err
		}
		caller, err := identity.request()
		if err != nil {
			return err
		}
		request := appworkflow.MutateWorkflowVersionRequest{Definition: ref, Identity: caller}
		var result appworkflow.WorkflowVersionDetail
		switch action {
		case "pin-version":
			result, err = service.PinRegistryVersion(command.Context(), request)
		case "unpin-version":
			result, err = service.UnpinRegistryVersion(command.Context(), request)
		case "publish":
			result, err = service.PublishWorkflowVersion(command.Context(), request)
		case "clear-current":
			result, err = service.ClearWorkflowCurrentExact(command.Context(), request)
		}
		if err != nil {
			return err
		}
		return writeWorkflowVersionDetail(command, output, result)
	}
	return command
}

func buildWorkflowPackageCmd(dependencies workflowCommandDependencies) *cobra.Command {
	var identity workflowIdentityFlags
	var output workflowOutputFlags
	var suiteFile string
	command := &cobra.Command{Use: "package <namespace/name@version#sha256:digest>", Short: "Build and verify a package, returning safe artifact metadata only", Args: cobra.ExactArgs(1)}
	identity.bind(command)
	output.bind(command)
	command.Flags().StringVar(&suiteFile, "suite", "", "completed contract suite JSON")
	_ = command.MarkFlagRequired("suite")
	command.RunE = func(command *cobra.Command, arguments []string) error {
		service, err := requireWorkflowLifecycle(dependencies)
		if err != nil {
			return err
		}
		ref, err := parseExactWorkflowRegistryRef(arguments[0])
		if err != nil {
			return err
		}
		var suite appworkflow.WorkflowContractSuite
		if readErr := readWorkflowLifecycleJSONFile(suiteFile, &suite); readErr != nil {
			return readErr
		}
		caller, err := identity.request()
		if err != nil {
			return err
		}
		result, err := service.PackageWorkflowVersion(command.Context(), appworkflow.PackageWorkflowVersionRequest{Definition: ref, Suite: suite, Identity: caller})
		if err != nil {
			return err
		}
		if output.json {
			return writeWorkflowJSON(command.OutOrStdout(), result)
		}
		_, err = fmt.Fprintf(command.OutOrStdout(), "%s bytes=%d\n", result.Digest, result.SizeBytes)
		return err
	}
	return command
}

func buildWorkflowExposureCmd(dependencies workflowCommandDependencies) *cobra.Command {
	command := &cobra.Command{Use: "exposure", Short: "Inspect or CAS-mutate exact profile tool visibility"}
	command.AddCommand(buildWorkflowExposureInspectCmd(dependencies), buildWorkflowExposureMutationCmd(dependencies, false), buildWorkflowExposureMutationCmd(dependencies, true))
	return command
}

func buildWorkflowExposureInspectCmd(dependencies workflowCommandDependencies) *cobra.Command {
	var identity workflowIdentityFlags
	var output workflowOutputFlags
	command := &cobra.Command{Use: "inspect <profile-id>", Short: "Inspect one authorized exposure profile", Args: cobra.ExactArgs(1)}
	identity.bind(command)
	output.bind(command)
	command.RunE = func(command *cobra.Command, arguments []string) error {
		service, err := requireWorkflowLifecycle(dependencies)
		if err != nil {
			return err
		}
		caller, err := identity.request()
		if err != nil {
			return err
		}
		result, err := service.InspectWorkflowExposure(command.Context(), appworkflow.InspectWorkflowExposureRequest{ProfileID: arguments[0], Identity: caller})
		if err != nil {
			return err
		}
		return writeWorkflowJSON(command.OutOrStdout(), result)
	}
	return command
}

func buildWorkflowExposureMutationCmd(dependencies workflowCommandDependencies, remove bool) *cobra.Command {
	var identity workflowIdentityFlags
	var output workflowOutputFlags
	var expected uint64
	action := "pin-definition"
	if remove {
		action = "unpin-definition"
	}
	command := &cobra.Command{Use: action + " <profile-id> <namespace/name@version#sha256:digest>", Short: "CAS-mutate one exact profile definition pin", Args: cobra.ExactArgs(2)}
	identity.bind(command)
	output.bind(command)
	command.Flags().Uint64Var(&expected, "expected-generation", 0, "required profile generation CAS")
	_ = command.MarkFlagRequired("expected-generation")
	command.RunE = func(command *cobra.Command, arguments []string) error {
		service, err := requireWorkflowLifecycle(dependencies)
		if err != nil {
			return err
		}
		ref, err := parseExactWorkflowRegistryRef(arguments[1])
		if err != nil {
			return err
		}
		caller, err := identity.request()
		if err != nil {
			return err
		}
		request := appworkflow.MutateWorkflowExposureRequest{ProfileID: arguments[0], Definition: ref, ExpectedGeneration: expected, Identity: caller}
		var result any
		if remove {
			result, err = service.UnpinWorkflowExposure(command.Context(), request)
		} else {
			result, err = service.PinWorkflowExposure(command.Context(), request)
		}
		if err != nil {
			return err
		}
		return writeWorkflowJSON(command.OutOrStdout(), result)
	}
	return command
}

func requireWorkflowLifecycle(dependencies workflowCommandDependencies) (appworkflow.WorkflowLifecycleOperations, error) {
	if dependencies.lifecycle == nil {
		return nil, errors.New("workflow lifecycle service is unavailable")
	}
	return dependencies.lifecycle, nil
}

func parseExactWorkflowRegistryRef(input string) (graph.DefinitionRef, error) {
	if strings.Count(input, "#") != 1 {
		return graph.DefinitionRef{}, errors.New("exact registry reference must be namespace/name@version#sha256:digest")
	}
	separator := strings.LastIndexByte(input, '#')
	if separator <= 0 || separator == len(input)-1 {
		return graph.DefinitionRef{}, errors.New("exact registry reference must be namespace/name@version#sha256:digest")
	}
	digest := input[separator+1:]
	if err := values.ValidateDigest(digest); err != nil {
		return graph.DefinitionRef{}, fmt.Errorf("exact registry reference digest: %w", err)
	}
	ref, err := parseWorkflowDefinitionRef(input[:separator])
	if err != nil {
		return graph.DefinitionRef{}, err
	}
	if ref.Kind != appworkflow.DefinitionKindRegistry || ref.ID == "" || ref.Version == "" || strings.LastIndexByte(ref.ID, '/') <= 0 {
		return graph.DefinitionRef{}, errors.New("exact registry reference must be namespace/name@version#sha256:digest")
	}
	ref.Digest = digest
	return ref, nil
}

func readWorkflowLifecycleJSONFile(path string, output any) error {
	data, err := readBoundedWorkflowFile(path, maximumWorkflowRequestBytes, true)
	if err != nil {
		return err
	}
	if err := decodeUniqueTypedWorkflowJSON(data, output); err != nil {
		return fmt.Errorf("workflow lifecycle JSON: %w", err)
	}
	return nil
}

func writeWorkflowVersionDetail(command *cobra.Command, output workflowOutputFlags, result appworkflow.WorkflowVersionDetail) error {
	if output.json {
		return writeWorkflowJSON(command.OutOrStdout(), result)
	}
	state := result.Registry
	_, err := fmt.Fprintf(command.OutOrStdout(), "%s@%s %s current=%t registry-pinned=%t published=%t effects=%s\n", result.Descriptor.Name, result.Descriptor.Version, result.Descriptor.Digest, state.Current, state.RegistryPinned, state.Published, strings.Join(effectStrings(result.Descriptor.Effects), ","))
	return err
}
