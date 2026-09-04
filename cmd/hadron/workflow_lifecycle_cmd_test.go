package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/go-workflow/graph"
	"github.com/hollis-labs/go-workflow/values"
)

type workflowLifecycleCLISpy struct {
	appworkflow.WorkflowLifecycleOperations
	inspect func(context.Context, appworkflow.InspectWorkflowVersionRequest) (appworkflow.WorkflowVersionDetail, error)
	unpin   func(context.Context, appworkflow.MutateWorkflowVersionRequest) (appworkflow.WorkflowVersionDetail, error)
}

func (s *workflowLifecycleCLISpy) InspectWorkflowVersion(ctx context.Context, request appworkflow.InspectWorkflowVersionRequest) (appworkflow.WorkflowVersionDetail, error) {
	return s.inspect(ctx, request)
}

func (s *workflowLifecycleCLISpy) UnpinRegistryVersion(ctx context.Context, request appworkflow.MutateWorkflowVersionRequest) (appworkflow.WorkflowVersionDetail, error) {
	return s.unpin(ctx, request)
}

func TestParseExactWorkflowRegistryRefGrammar(t *testing.T) {
	digest := values.SHA256Digest([]byte("exact-lifecycle"))
	input := "team/release@v1#" + digest
	ref, err := parseExactWorkflowRegistryRef(input)
	if err != nil || ref != (graph.DefinitionRef{Kind: appworkflow.DefinitionKindRegistry, ID: "team/release", Version: "v1", Digest: digest}) {
		t.Fatalf("parse %q = %#v, %v", input, ref, err)
	}
	for _, invalid := range []string{
		"team/release@v1@" + digest,
		"team/release@v1#",
		"release@v1#" + digest,
		"team/release#" + digest,
		"team/../release@v1#" + digest,
		"team/release@v1#sha256:not-hex",
		"team/release@v1#" + digest + "#extra",
	} {
		if _, parseErr := parseExactWorkflowRegistryRef(invalid); parseErr == nil {
			t.Fatalf("accepted ambiguous exact ref %q", invalid)
		}
	}
}

func TestWorkflowLifecycleCLICommandsRouteExactRefs(t *testing.T) {
	digest := values.SHA256Digest([]byte("cli-lifecycle"))
	refText := "team/release@v1#" + digest
	wantRef := graph.DefinitionRef{Kind: appworkflow.DefinitionKindRegistry, ID: "team/release", Version: "v1", Digest: digest}
	inspectCalls, unpinCalls := 0, 0
	result := appworkflow.WorkflowVersionDetail{
		Descriptor: appworkflow.WorkflowExposureDescriptor{Name: wantRef.ID, Version: wantRef.Version, Digest: wantRef.Digest, Definition: wantRef, Effects: graph.EffectSet{graph.EffectRead}},
		Registry:   appworkflow.WorkflowRegistryState{Current: true, RegistryPinned: true, Published: true},
	}
	spy := &workflowLifecycleCLISpy{
		inspect: func(_ context.Context, request appworkflow.InspectWorkflowVersionRequest) (appworkflow.WorkflowVersionDetail, error) {
			inspectCalls++
			if request.Definition != wantRef || request.Identity.SourceAuthority != "cli" {
				t.Fatalf("inspect request = %#v", request)
			}
			return result, nil
		},
		unpin: func(_ context.Context, request appworkflow.MutateWorkflowVersionRequest) (appworkflow.WorkflowVersionDetail, error) {
			unpinCalls++
			if request.Definition != wantRef || request.Identity.SourceAuthority != "cli" {
				t.Fatalf("unpin request = %#v", request)
			}
			return result, nil
		},
	}
	for _, arguments := range [][]string{{"catalog", "inspect", refText, "--json"}, {"registry", "unpin-version", refText, "--json"}} {
		dependencies := testWorkflowDependencies(completeWorkflowSpy())
		dependencies.lifecycle = spy
		command := buildWorkflowCmdWithDependencies(dependencies)
		var output bytes.Buffer
		command.SetOut(&output)
		command.SetErr(&output)
		command.SetArgs(arguments)
		if err := command.Execute(); err != nil {
			t.Fatalf("%s: %v\n%s", strings.Join(arguments, " "), err, output.String())
		}
		var decoded appworkflow.WorkflowVersionDetail
		if err := json.Unmarshal(output.Bytes(), &decoded); err != nil || decoded.Descriptor.Definition != wantRef {
			t.Fatalf("%s output = %#v, %v; raw=%s", strings.Join(arguments, " "), decoded, err, output.String())
		}
	}
	if inspectCalls != 1 || unpinCalls != 1 {
		t.Fatalf("inspect calls=%d unpin calls=%d", inspectCalls, unpinCalls)
	}
}

func TestWorkflowLifecycleCLIAuthorCommandsRequireDraftIdentity(t *testing.T) {
	for _, action := range []string{"validate", "scaffold", "test", "register"} {
		t.Run(action, func(t *testing.T) {
			dependencies := testWorkflowDependencies(completeWorkflowSpy())
			dependencies.lifecycle = &workflowLifecycleCLISpy{}
			command := buildWorkflowCmdWithDependencies(dependencies)
			var output bytes.Buffer
			command.SetOut(&output)
			command.SetErr(&output)
			arguments := []string{"author", action, "draft.json"}
			if action == "test" || action == "register" {
				arguments = append(arguments, "--suite", "suite.json")
			}
			command.SetArgs(arguments)
			err := command.Execute()
			if err == nil {
				t.Fatal("author command accepted missing source-local identity")
			}
			for _, flag := range []string{"id", "version", "namespace"} {
				if !strings.Contains(err.Error(), `"`+flag+`"`) {
					t.Fatalf("error %q does not identify missing --%s", err, flag)
				}
			}
		})
	}
}

var _ appworkflow.WorkflowLifecycleOperations = (*workflowLifecycleCLISpy)(nil)
