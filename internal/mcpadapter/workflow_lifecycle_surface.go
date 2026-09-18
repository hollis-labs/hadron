package mcpadapter

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/go-workflow/graph"
)

func (w *workflowSurface) lifecycleSession(ctx context.Context, args map[string]any) (context.Context, appworkflow.WorkflowExposureSession, error) {
	bound, session, err := w.requestSession(ctx, args)
	if err != nil {
		return bound, session, err
	}
	if nilInterfaceValue(w.lifecycle) {
		return bound, session, appworkflow.ErrHostNotReady
	}
	return bound, session, nil
}

func (w *workflowSurface) handleLifecycleCatalogInspect(ctx context.Context, args map[string]any) (any, error) {
	bound, _, err := w.lifecycleSession(ctx, args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	result, err := w.lifecycle.InspectWorkflowVersion(bound, appworkflow.InspectWorkflowVersionRequest{Definition: workflowRefFromArgs(args), Identity: workflowIdentityRequest()})
	return workflowResult(result, err)
}

func (w *workflowSurface) handleLifecycleCatalogSearch(ctx context.Context, args map[string]any) (any, error) {
	bound, session, err := w.lifecycleSession(ctx, args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	limit, err := exactWorkflowLimitArgument(args, 20)
	if err != nil {
		return nil, workflowFailure(err)
	}
	if limit < 1 || limit > appworkflow.MaximumLifecycleSearchResults {
		return nil, workflowFailure(errors.New("workflow catalog search limit is invalid"))
	}
	visible, err := w.exposure.Search(bound, session, "", appworkflow.MaximumLifecycleSearchResults)
	if err != nil {
		return nil, workflowFailure(err)
	}
	requestedNamespace := argString(args, "namespace", "")
	namespaces := lifecycleNamespaces(visible)
	if requestedNamespace != "" {
		namespaces = []string{requestedNamespace}
	}
	query := argString(args, "query", "")
	var ranked []appworkflow.WorkflowCatalogMatch
	truncated := false
	for _, namespace := range namespaces {
		current, searchErr := w.lifecycle.SearchWorkflowCatalog(bound, appworkflow.SearchWorkflowCatalogRequest{Namespace: namespace, Query: query, Limit: appworkflow.MaximumLifecycleSearchResults, Identity: workflowIdentityRequest()})
		if searchErr != nil {
			return nil, workflowFailure(searchErr)
		}
		ranked = append(ranked, current.Matches...)
		truncated = truncated || current.Truncated
	}
	sort.SliceStable(ranked, func(i, j int) bool {
		if ranked[i].Score != ranked[j].Score {
			return ranked[i].Score > ranked[j].Score
		}
		if ranked[i].Name != ranked[j].Name {
			return ranked[i].Name < ranked[j].Name
		}
		return ranked[i].Definition.Digest < ranked[j].Definition.Digest
	})
	matches := filterLifecycleMatches(visible, ranked, limit)
	next := "draft_validate"
	if len(matches) != 0 {
		next = "inspect_exact"
	}
	return workflowSuccess(appworkflow.WorkflowCatalogSearchResult{Matches: matches, Truncated: truncated || len(ranked) > len(matches), NextStep: next}), nil
}

func (w *workflowSurface) handleLifecycleAuthorValidate(ctx context.Context, args map[string]any) (any, error) {
	bound, _, err := w.lifecycleSession(ctx, args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	draft, err := lifecycleDraftArgument(args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	result, err := w.lifecycle.ValidateWorkflowDraft(bound, appworkflow.ValidateWorkflowDraftRequest{Draft: draft, Identity: workflowIdentityRequest()})
	return workflowResult(result, err)
}

func (w *workflowSurface) handleLifecycleAuthorScaffold(ctx context.Context, args map[string]any) (any, error) {
	bound, _, err := w.lifecycleSession(ctx, args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	draft, err := lifecycleDraftArgument(args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	result, err := w.lifecycle.GenerateWorkflowContract(bound, appworkflow.GenerateWorkflowContractRequest{Draft: draft, Identity: workflowIdentityRequest()})
	return workflowResult(result, err)
}

func (w *workflowSurface) handleLifecycleAuthorTest(ctx context.Context, args map[string]any) (any, error) {
	bound, _, err := w.lifecycleSession(ctx, args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	input, err := lifecycleContractArguments(args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	result, err := w.lifecycle.TestWorkflowDraft(bound, appworkflow.TestWorkflowDraftRequest{Draft: input.Draft, Suite: input.Suite, Identity: workflowIdentityRequest()})
	return workflowResult(result, err)
}

func (w *workflowSurface) handleLifecycleAuthorRegister(ctx context.Context, args map[string]any) (any, error) {
	bound, _, err := w.lifecycleSession(ctx, args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	input, err := lifecycleContractArguments(args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	result, err := w.lifecycle.RegisterWorkflowDraft(bound, appworkflow.RegisterWorkflowDraftRequest{Draft: input.Draft, Suite: input.Suite, MakeCurrent: input.MakeCurrent, Identity: workflowIdentityRequest()})
	return workflowResult(result, err)
}

func (w *workflowSurface) handleLifecyclePackage(ctx context.Context, args map[string]any) (any, error) {
	bound, _, err := w.lifecycleSession(ctx, args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	var input struct {
		Name    string                             `json:"name"`
		Version string                             `json:"version"`
		Digest  string                             `json:"digest"`
		Suite   appworkflow.WorkflowContractSuite `json:"suite"`
	}
	if err = decodeWorkflowArguments(args, &input); err != nil {
		return nil, workflowFailure(err)
	}
	ref := graph.DefinitionRef{Kind: appworkflow.DefinitionKindRegistry, ID: input.Name, Version: input.Version, Digest: input.Digest}
	result, err := w.lifecycle.PackageWorkflowVersion(bound, appworkflow.PackageWorkflowVersionRequest{Definition: ref, Suite: input.Suite, Identity: workflowIdentityRequest()})
	return workflowResult(result, err)
}

func (w *workflowSurface) handleLifecycleRegistryPin(ctx context.Context, args map[string]any) (any, error) {
	return w.handleLifecycleRegistryMutation(ctx, args, "pin")
}

func (w *workflowSurface) handleLifecycleRegistryUnpin(ctx context.Context, args map[string]any) (any, error) {
	return w.handleLifecycleRegistryMutation(ctx, args, "unpin")
}

func (w *workflowSurface) handleLifecycleRegistryPublish(ctx context.Context, args map[string]any) (any, error) {
	return w.handleLifecycleRegistryMutation(ctx, args, "publish")
}

func (w *workflowSurface) handleLifecycleClearCurrent(ctx context.Context, args map[string]any) (any, error) {
	return w.handleLifecycleRegistryMutation(ctx, args, "clear_current")
}

func (w *workflowSurface) handleLifecycleRegistryMutation(ctx context.Context, args map[string]any, operation string) (any, error) {
	bound, _, err := w.lifecycleSession(ctx, args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	input := appworkflow.MutateWorkflowVersionRequest{Definition: workflowRefFromArgs(args), Identity: workflowIdentityRequest()}
	var result appworkflow.WorkflowVersionDetail
	var opErr error
	switch operation {
	case "pin":
		result, opErr = w.lifecycle.PinRegistryVersion(bound, input)
	case "unpin":
		result, opErr = w.lifecycle.UnpinRegistryVersion(bound, input)
	case "publish":
		result, opErr = w.lifecycle.PublishWorkflowVersion(bound, input)
	case "clear_current":
		result, opErr = w.lifecycle.ClearWorkflowCurrentExact(bound, input)
	}
	return workflowResult(result, opErr)
}

func (w *workflowSurface) handleLifecycleExposureInspect(ctx context.Context, args map[string]any) (any, error) {
	bound, _, err := w.lifecycleSession(ctx, args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	result, err := w.lifecycle.InspectWorkflowExposure(bound, appworkflow.InspectWorkflowExposureRequest{ProfileID: argString(args, "profile_id", ""), Identity: workflowIdentityRequest()})
	return workflowResult(result, err)
}

func (w *workflowSurface) handleLifecycleExposurePin(ctx context.Context, args map[string]any) (any, error) {
	return w.handleLifecycleExposureMutation(ctx, args, false)
}

func (w *workflowSurface) handleLifecycleExposureUnpin(ctx context.Context, args map[string]any) (any, error) {
	return w.handleLifecycleExposureMutation(ctx, args, true)
}

func (w *workflowSurface) handleLifecycleExposureMutation(ctx context.Context, args map[string]any, remove bool) (any, error) {
	bound, _, err := w.lifecycleSession(ctx, args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	expected, err := exactWorkflowUint64Argument(args, "expected_generation")
	if err != nil {
		return nil, workflowFailure(err)
	}
	if expected == 0 {
		return nil, workflowFailure(errors.New("expected_generation must be an exact positive integer"))
	}
	input := appworkflow.MutateWorkflowExposureRequest{
		ProfileID: argString(args, "profile_id", ""), Definition: workflowRefFromArgs(args),
		ExpectedGeneration: expected, Identity: workflowIdentityRequest(),
	}
	var result any
	if remove {
		result, err = w.lifecycle.UnpinWorkflowExposure(bound, input)
	} else {
		result, err = w.lifecycle.PinWorkflowExposure(bound, input)
	}
	if err != nil {
		return nil, workflowFailure(err)
	}
	// Re-resolve the authenticated session after the profile generation change
	// so stale tools disappear and newly authorized pins mount atomically.
	if _, _, refreshErr := w.current(ctx, w.adapter.token); refreshErr != nil {
		return nil, workflowFailure(refreshErr)
	}
	return workflowSuccess(result), nil
}

func lifecycleDraftArgument(args map[string]any) (appworkflow.WorkflowDraft, error) {
	var input struct {
		Draft appworkflow.WorkflowDraft `json:"draft"`
	}
	if err := decodeWorkflowArguments(args, &input); err != nil {
		return appworkflow.WorkflowDraft{}, err
	}
	return input.Draft, nil
}

type lifecycleContractInput struct {
	Draft       appworkflow.WorkflowDraft         `json:"draft"`
	Suite       appworkflow.WorkflowContractSuite `json:"suite"`
	MakeCurrent bool                              `json:"make_current,omitempty"`
}

func lifecycleContractArguments(args map[string]any) (lifecycleContractInput, error) {
	var input lifecycleContractInput
	err := decodeWorkflowArguments(args, &input)
	return input, err
}

func exactWorkflowUint64Argument(args map[string]any, key string) (uint64, error) {
	arguments, err := boundedWorkflowArguments(args)
	if err != nil {
		return 0, err
	}
	value, exists := arguments[key]
	if !exists {
		return 0, errors.New("exact integer argument is required")
	}
	number, ok := value.(json.Number)
	if !ok {
		encoded, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			return 0, errors.New("exact integer argument is invalid")
		}
		number = json.Number(string(encoded))
	}
	return parseWorkflowUint64(number)
}

func parseWorkflowUint64(number json.Number) (uint64, error) {
	parsed, err := strconv.ParseUint(string(number), 10, 64)
	if err != nil {
		return 0, errors.New("exact integer argument is invalid")
	}
	return parsed, nil
}

func filterLifecycleMatches(visible []appworkflow.WorkflowExposureSummary, ranked []appworkflow.WorkflowCatalogMatch, limit int) []appworkflow.WorkflowCatalogMatch {
	allowed := make(map[graph.DefinitionRef]struct{}, len(visible))
	for _, summary := range visible {
		allowed[summary.Definition] = struct{}{}
	}
	result := make([]appworkflow.WorkflowCatalogMatch, 0, min(limit, len(ranked)))
	seen := make(map[graph.DefinitionRef]struct{})
	for _, match := range ranked {
		if _, ok := allowed[match.Definition]; !ok {
			continue
		}
		if _, duplicate := seen[match.Definition]; duplicate {
			continue
		}
		seen[match.Definition] = struct{}{}
		result = append(result, match)
		if len(result) == limit {
			break
		}
	}
	return result
}

func lifecycleNamespaces(visible []appworkflow.WorkflowExposureSummary) []string {
	seen := make(map[string]struct{})
	for _, summary := range visible {
		seen[summary.Namespace] = struct{}{}
	}
	result := make([]string, 0, len(seen))
	for namespace := range seen {
		result = append(result, namespace)
	}
	sort.Strings(result)
	return result
}
