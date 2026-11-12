package mcptools

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/hollis-labs/libs/workflow/engine/graph"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
)

func (s *Surface) lifecycle() (appworkflow.WorkflowLifecycleOperations, error) {
	if nilInterfaceValue(s.deps.Lifecycle) {
		return nil, workflowFailure(appworkflow.ErrHostNotReady)
	}
	return s.deps.Lifecycle, nil
}

func (s *Surface) catalogSearch(ctx context.Context, a args) (any, error) {
	l, err := s.lifecycle()
	if err != nil {
		return nil, err
	}
	limit, err := exactWorkflowLimitArgument(a, 20)
	if err != nil {
		return nil, workflowFailure(err)
	}
	if limit < 1 || limit > appworkflow.MaximumLifecycleSearchResults {
		return nil, workflowFailure(errors.New("workflow catalog search limit is invalid"))
	}
	r, err := l.SearchWorkflowCatalog(ctx, appworkflow.SearchWorkflowCatalogRequest{
		Namespace: argString(a, "namespace", ""), Query: argString(a, "query", ""), Limit: limit, Identity: identity(),
	})
	return workflowResult(r, err)
}

func (s *Surface) catalogInspect(ctx context.Context, a args) (any, error) {
	l, err := s.lifecycle()
	if err != nil {
		return nil, err
	}
	r, err := l.InspectWorkflowVersion(ctx, appworkflow.InspectWorkflowVersionRequest{Definition: refFromArgs(a), Identity: identity()})
	return workflowResult(r, err)
}

type contractInput struct {
	Draft       appworkflow.WorkflowDraft         `json:"draft"`
	Suite       appworkflow.WorkflowContractSuite `json:"suite"`
	MakeCurrent bool                              `json:"make_current,omitempty"`
}

func (s *Surface) authorValidate(ctx context.Context, a args) (any, error) {
	l, err := s.lifecycle()
	if err != nil {
		return nil, err
	}
	var in struct {
		Draft appworkflow.WorkflowDraft `json:"draft"`
	}
	if err := decodeWorkflowArguments(a, &in); err != nil {
		return nil, workflowFailure(err)
	}
	r, err := l.ValidateWorkflowDraft(ctx, appworkflow.ValidateWorkflowDraftRequest{Draft: in.Draft, Identity: identity()})
	return workflowResult(r, err)
}

func (s *Surface) authorScaffold(ctx context.Context, a args) (any, error) {
	l, err := s.lifecycle()
	if err != nil {
		return nil, err
	}
	var in struct {
		Draft appworkflow.WorkflowDraft `json:"draft"`
	}
	if err := decodeWorkflowArguments(a, &in); err != nil {
		return nil, workflowFailure(err)
	}
	r, err := l.GenerateWorkflowContract(ctx, appworkflow.GenerateWorkflowContractRequest{Draft: in.Draft, Identity: identity()})
	return workflowResult(r, err)
}

func (s *Surface) authorTest(ctx context.Context, a args) (any, error) {
	l, err := s.lifecycle()
	if err != nil {
		return nil, err
	}
	var in contractInput
	if err := decodeWorkflowArguments(a, &in); err != nil {
		return nil, workflowFailure(err)
	}
	r, err := l.TestWorkflowDraft(ctx, appworkflow.TestWorkflowDraftRequest{Draft: in.Draft, Suite: in.Suite, Identity: identity()})
	return workflowResult(r, err)
}

func (s *Surface) authorRegister(ctx context.Context, a args) (any, error) {
	l, err := s.lifecycle()
	if err != nil {
		return nil, err
	}
	var in contractInput
	if err := decodeWorkflowArguments(a, &in); err != nil {
		return nil, workflowFailure(err)
	}
	r, err := l.RegisterWorkflowDraft(ctx, appworkflow.RegisterWorkflowDraftRequest{Draft: in.Draft, Suite: in.Suite, MakeCurrent: in.MakeCurrent, Identity: identity()})
	return workflowResult(r, err)
}

func (s *Surface) registryPackage(ctx context.Context, a args) (any, error) {
	l, err := s.lifecycle()
	if err != nil {
		return nil, err
	}
	var in struct {
		Name    string                            `json:"name"`
		Version string                            `json:"version"`
		Digest  string                            `json:"digest"`
		Suite   appworkflow.WorkflowContractSuite `json:"suite"`
	}
	if err := decodeWorkflowArguments(a, &in); err != nil {
		return nil, workflowFailure(err)
	}
	ref := graph.DefinitionRef{Kind: appworkflow.DefinitionKindRegistry, ID: in.Name, Version: in.Version, Digest: in.Digest}
	r, err := l.PackageWorkflowVersion(ctx, appworkflow.PackageWorkflowVersionRequest{Definition: ref, Suite: in.Suite, Identity: identity()})
	return workflowResult(r, err)
}

func (s *Surface) registryMutation(op string) func(context.Context, args) (any, error) {
	return func(ctx context.Context, a args) (any, error) {
		l, err := s.lifecycle()
		if err != nil {
			return nil, err
		}
		in := appworkflow.MutateWorkflowVersionRequest{Definition: refFromArgs(a), Identity: identity()}
		var r appworkflow.WorkflowVersionDetail
		switch op {
		case "pin":
			r, err = l.PinRegistryVersion(ctx, in)
		case "unpin":
			r, err = l.UnpinRegistryVersion(ctx, in)
		case "publish":
			r, err = l.PublishWorkflowVersion(ctx, in)
		case "clear_current":
			r, err = l.ClearWorkflowCurrentExact(ctx, in)
		}
		return workflowResult(r, err)
	}
}

func (s *Surface) exposureInspect(ctx context.Context, a args) (any, error) {
	l, err := s.lifecycle()
	if err != nil {
		return nil, err
	}
	r, err := l.InspectWorkflowExposure(ctx, appworkflow.InspectWorkflowExposureRequest{ProfileID: argString(a, "profile_id", ""), Identity: identity()})
	return workflowResult(r, err)
}

func (s *Surface) exposureMutation(remove bool) func(context.Context, args) (any, error) {
	return func(ctx context.Context, a args) (any, error) {
		l, err := s.lifecycle()
		if err != nil {
			return nil, err
		}
		expected, err := exactUint64Argument(a, "expected_generation")
		if err != nil || expected == 0 {
			return nil, workflowFailure(errors.New("expected_generation must be an exact positive integer"))
		}
		in := appworkflow.MutateWorkflowExposureRequest{
			ProfileID: argString(a, "profile_id", ""), Definition: refFromArgs(a), ExpectedGeneration: expected, Identity: identity(),
		}
		var r any
		if remove {
			r, err = l.UnpinWorkflowExposure(ctx, in)
		} else {
			r, err = l.PinWorkflowExposure(ctx, in)
		}
		return workflowResult(r, err)
	}
}

func exactUint64Argument(a args, key string) (uint64, error) {
	bounded, err := boundedWorkflowArguments(a)
	if err != nil {
		return 0, err
	}
	v, ok := bounded[key]
	if !ok {
		return 0, errors.New("exact integer argument is required")
	}
	n, ok := v.(json.Number)
	if !ok {
		return 0, errors.New("exact integer argument is invalid")
	}
	return strconv.ParseUint(string(n), 10, 64)
}

func (s *Surface) exposurePut(ctx context.Context, a args) (any, error) {
	l, err := s.lifecycle()
	if err != nil {
		return nil, err
	}
	var in struct {
		Profile            hoststate.ExposureProfileRecord `json:"profile"`
		ExpectedGeneration uint64                          `json:"expected_generation"`
	}
	if err := decodeWorkflowArguments(a, &in); err != nil {
		return nil, workflowFailure(err)
	}
	r, err := l.PutWorkflowExposureProfile(ctx, appworkflow.PutWorkflowExposureProfileRequest{Profile: in.Profile, ExpectedGeneration: in.ExpectedGeneration, Identity: identity()})
	return workflowResult(r, err)
}

func (s *Surface) exposureDelete(ctx context.Context, a args) (any, error) {
	l, err := s.lifecycle()
	if err != nil {
		return nil, err
	}
	expected, err := exactUint64Argument(a, "expected_generation")
	if err != nil || expected == 0 {
		return nil, workflowFailure(errors.New("expected_generation must be an exact positive integer"))
	}
	r, err := l.DeleteWorkflowExposureProfile(ctx, appworkflow.DeleteWorkflowExposureProfileRequest{ProfileID: argString(a, "profile_id", ""), ExpectedGeneration: expected, Identity: identity()})
	return workflowResult(r, err)
}
