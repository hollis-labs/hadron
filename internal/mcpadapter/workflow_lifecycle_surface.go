package mcpadapter

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/mark3labs/mcp-go/mcp"
)

func (w *workflowSurface) lifecycleSession(ctx context.Context, request mcp.CallToolRequest) (context.Context, appworkflow.WorkflowExposureSession, *mcp.CallToolResult) {
	bound, session, result := w.requestSession(ctx, request)
	if result != nil {
		return bound, session, result
	}
	if nilInterfaceValue(w.lifecycle) {
		return bound, session, workflowFailure(appworkflow.ErrHostNotReady)
	}
	return bound, session, nil
}

func (w *workflowSurface) handleLifecycleCatalogInspect(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bound, _, failure := w.lifecycleSession(ctx, request)
	if failure != nil {
		return failure, nil
	}
	result, err := w.lifecycle.InspectWorkflowVersion(bound, appworkflow.InspectWorkflowVersionRequest{Definition: workflowRefFromRequest(request), Identity: workflowIdentityRequest()})
	return workflowResult(result, err)
}

func (w *workflowSurface) handleLifecycleCatalogSearch(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bound, session, failure := w.lifecycleSession(ctx, request)
	if failure != nil {
		return failure, nil
	}
	limit, err := exactWorkflowLimitArgument(request, 20)
	if err != nil {
		return workflowFailure(err), nil
	}
	if limit < 1 || limit > appworkflow.MaximumLifecycleSearchResults {
		return workflowFailure(errors.New("workflow catalog search limit is invalid")), nil
	}
	visible, err := w.exposure.Search(bound, session, "", appworkflow.MaximumLifecycleSearchResults)
	if err != nil {
		return workflowFailure(err), nil
	}
	requestedNamespace := request.GetString("namespace", "")
	namespaces := lifecycleNamespaces(visible)
	if requestedNamespace != "" {
		namespaces = []string{requestedNamespace}
	}
	query := request.GetString("query", "")
	var ranked []appworkflow.WorkflowCatalogMatch
	truncated := false
	for _, namespace := range namespaces {
		current, searchErr := w.lifecycle.SearchWorkflowCatalog(bound, appworkflow.SearchWorkflowCatalogRequest{Namespace: namespace, Query: query, Limit: appworkflow.MaximumLifecycleSearchResults, Identity: workflowIdentityRequest()})
		if searchErr != nil {
			return workflowFailure(searchErr), nil
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

func (w *workflowSurface) handleLifecycleAuthorValidate(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bound, _, failure := w.lifecycleSession(ctx, request)
	if failure != nil {
		return failure, nil
	}
	draft, err := lifecycleDraftArgument(request)
	if err != nil {
		return workflowFailure(err), nil
	}
	result, err := w.lifecycle.ValidateWorkflowDraft(bound, appworkflow.ValidateWorkflowDraftRequest{Draft: draft, Identity: workflowIdentityRequest()})
	return workflowResult(result, err)
}

func (w *workflowSurface) handleLifecycleAuthorScaffold(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bound, _, failure := w.lifecycleSession(ctx, request)
	if failure != nil {
		return failure, nil
	}
	draft, err := lifecycleDraftArgument(request)
	if err != nil {
		return workflowFailure(err), nil
	}
	result, err := w.lifecycle.GenerateWorkflowContract(bound, appworkflow.GenerateWorkflowContractRequest{Draft: draft, Identity: workflowIdentityRequest()})
	return workflowResult(result, err)
}

func (w *workflowSurface) handleLifecycleAuthorTest(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bound, _, failure := w.lifecycleSession(ctx, request)
	if failure != nil {
		return failure, nil
	}
	input, err := lifecycleContractArguments(request)
	if err != nil {
		return workflowFailure(err), nil
	}
	result, err := w.lifecycle.TestWorkflowDraft(bound, appworkflow.TestWorkflowDraftRequest{Draft: input.Draft, Suite: input.Suite, Identity: workflowIdentityRequest()})
	return workflowResult(result, err)
}

func (w *workflowSurface) handleLifecycleAuthorRegister(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bound, _, failure := w.lifecycleSession(ctx, request)
	if failure != nil {
		return failure, nil
	}
	input, err := lifecycleContractArguments(request)
	if err != nil {
		return workflowFailure(err), nil
	}
	result, err := w.lifecycle.RegisterWorkflowDraft(bound, appworkflow.RegisterWorkflowDraftRequest{Draft: input.Draft, Suite: input.Suite, MakeCurrent: input.MakeCurrent, Identity: workflowIdentityRequest()})
	return workflowResult(result, err)
}

func (w *workflowSurface) handleLifecyclePackage(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bound, _, failure := w.lifecycleSession(ctx, request)
	if failure != nil {
		return failure, nil
	}
	var input struct {
		Name    string                            `json:"name"`
		Version string                            `json:"version"`
		Digest  string                            `json:"digest"`
		Suite   appworkflow.WorkflowContractSuite `json:"suite"`
	}
	if err := decodeWorkflowArguments(request, &input); err != nil {
		return workflowFailure(err), nil
	}
	ref := graph.DefinitionRef{Kind: appworkflow.DefinitionKindRegistry, ID: input.Name, Version: input.Version, Digest: input.Digest}
	result, err := w.lifecycle.PackageWorkflowVersion(bound, appworkflow.PackageWorkflowVersionRequest{Definition: ref, Suite: input.Suite, Identity: workflowIdentityRequest()})
	return workflowResult(result, err)
}

func (w *workflowSurface) handleLifecycleRegistryPin(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return w.handleLifecycleRegistryMutation(ctx, request, "pin")
}

func (w *workflowSurface) handleLifecycleRegistryUnpin(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return w.handleLifecycleRegistryMutation(ctx, request, "unpin")
}

func (w *workflowSurface) handleLifecycleRegistryPublish(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return w.handleLifecycleRegistryMutation(ctx, request, "publish")
}

func (w *workflowSurface) handleLifecycleClearCurrent(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return w.handleLifecycleRegistryMutation(ctx, request, "clear_current")
}

func (w *workflowSurface) handleLifecycleRegistryMutation(ctx context.Context, request mcp.CallToolRequest, operation string) (*mcp.CallToolResult, error) {
	bound, _, failure := w.lifecycleSession(ctx, request)
	if failure != nil {
		return failure, nil
	}
	input := appworkflow.MutateWorkflowVersionRequest{Definition: workflowRefFromRequest(request), Identity: workflowIdentityRequest()}
	var result appworkflow.WorkflowVersionDetail
	var err error
	switch operation {
	case "pin":
		result, err = w.lifecycle.PinRegistryVersion(bound, input)
	case "unpin":
		result, err = w.lifecycle.UnpinRegistryVersion(bound, input)
	case "publish":
		result, err = w.lifecycle.PublishWorkflowVersion(bound, input)
	case "clear_current":
		result, err = w.lifecycle.ClearWorkflowCurrentExact(bound, input)
	}
	return workflowResult(result, err)
}

func (w *workflowSurface) handleLifecycleExposureInspect(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bound, _, failure := w.lifecycleSession(ctx, request)
	if failure != nil {
		return failure, nil
	}
	result, err := w.lifecycle.InspectWorkflowExposure(bound, appworkflow.InspectWorkflowExposureRequest{ProfileID: request.GetString("profile_id", ""), Identity: workflowIdentityRequest()})
	return workflowResult(result, err)
}

func (w *workflowSurface) handleLifecycleExposurePin(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return w.handleLifecycleExposureMutation(ctx, request, false)
}

func (w *workflowSurface) handleLifecycleExposureUnpin(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return w.handleLifecycleExposureMutation(ctx, request, true)
}

func (w *workflowSurface) handleLifecycleExposureMutation(ctx context.Context, request mcp.CallToolRequest, remove bool) (*mcp.CallToolResult, error) {
	bound, session, failure := w.lifecycleSession(ctx, request)
	if failure != nil {
		return failure, nil
	}
	expected, err := exactWorkflowUint64Argument(request, "expected_generation")
	if err != nil {
		return workflowFailure(err), nil
	}
	if expected == 0 {
		return workflowFailure(errors.New("expected_generation must be an exact positive integer")), nil
	}
	input := appworkflow.MutateWorkflowExposureRequest{
		ProfileID: request.GetString("profile_id", ""), Definition: workflowRefFromRequest(request),
		ExpectedGeneration: expected, Identity: workflowIdentityRequest(),
	}
	var result any
	if remove {
		result, err = w.lifecycle.UnpinWorkflowExposure(bound, input)
	} else {
		result, err = w.lifecycle.PinWorkflowExposure(bound, input)
	}
	if err != nil {
		return workflowFailure(err), nil
	}
	// Re-resolve the authenticated session after the profile generation change
	// so stale tools disappear and newly authorized pins mount atomically.
	if _, _, refreshErr := w.current(ctx, session.SessionID, w.adapter.token); refreshErr != nil {
		return workflowFailure(refreshErr), nil
	}
	return workflowSuccess(result), nil
}

func lifecycleDraftArgument(request mcp.CallToolRequest) (appworkflow.WorkflowDraft, error) {
	var input struct {
		Draft appworkflow.WorkflowDraft `json:"draft"`
	}
	if err := decodeWorkflowArguments(request, &input); err != nil {
		return appworkflow.WorkflowDraft{}, err
	}
	return input.Draft, nil
}

type lifecycleContractInput struct {
	Draft       appworkflow.WorkflowDraft         `json:"draft"`
	Suite       appworkflow.WorkflowContractSuite `json:"suite"`
	MakeCurrent bool                              `json:"make_current,omitempty"`
}

func lifecycleContractArguments(request mcp.CallToolRequest) (lifecycleContractInput, error) {
	var input lifecycleContractInput
	err := decodeWorkflowArguments(request, &input)
	return input, err
}

func exactWorkflowUint64Argument(request mcp.CallToolRequest, key string) (uint64, error) {
	arguments, err := boundedWorkflowArguments(request)
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
