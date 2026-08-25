package appworkflow

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestWorkflowActivationExposureRefRoundTripsDelimiterBearingIdentity(t *testing.T) {
	definition := graph.DefinitionRef{
		Kind:    DefinitionKindRegistry,
		ID:      "team@alpha#?&/nested/workflow-one",
		Version: "v1@#/?&",
		Digest:  values.SHA256Digest([]byte("delimiter-bearing-workflow")),
	}
	encoded, err := EncodeWorkflowActivationExposureRef(definition, "external-hook")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(encoded, definition.ID) || strings.Contains(encoded, definition.Version) {
		t.Fatalf("delimiter-bearing fields were not escaped: %q", encoded)
	}
	decoded, err := DecodeWorkflowActivationExposureRef(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Definition != definition || decoded.ActivationID != "external-hook" {
		t.Fatalf("decoded exposure = %#v", decoded)
	}
}

func TestWorkflowActivationExposureRefRejectsMalformedDuplicateUnknownAndNonCanonical(t *testing.T) {
	digest := values.SHA256Digest([]byte("activation-exposure"))
	valid, err := EncodeWorkflowActivationExposureRef(graph.DefinitionRef{Kind: DefinitionKindRegistry, ID: "team/workflow-one", Version: "v1", Digest: digest}, "hook")
	if err != nil {
		t.Fatal(err)
	}
	raw := func(value string) string {
		return workflowActivationExposurePrefix + base64.RawURLEncoding.EncodeToString([]byte(value))
	}
	for _, encoded := range []string{
		"",
		valid + "=",
		raw(`{"activation":"hook","activation":"other","digest":"` + digest + `","name":"team/workflow-one","version":"v1"}`),
		raw(`{"activation":"hook","digest":"` + digest + `","name":"team/workflow-one","version":"v1","unknown":"value"}`),
		raw(`{"digest":"` + digest + `","name":"team/workflow-one","version":"v1"}`),
		raw(`{"name":"team/workflow-one","activation":"hook","digest":"` + digest + `","version":"v1"}`),
		strings.Repeat("x", 1025),
	} {
		if _, err := DecodeWorkflowActivationExposureRef(encoded); err == nil {
			t.Fatalf("malformed exposure accepted: %q", encoded)
		}
	}
}

func TestWorkflowActivationExposureRefRejectsSemanticFieldsItCannotEncode(t *testing.T) {
	digest := values.SHA256Digest([]byte("activation-exposure-fields"))
	base := graph.DefinitionRef{Kind: DefinitionKindRegistry, ID: "team/workflow-one", Version: "v1", Digest: digest}
	for _, mutate := range []func(*graph.DefinitionRef){
		func(ref *graph.DefinitionRef) { ref.Authority = "project" },
		func(ref *graph.DefinitionRef) { ref.Locator = "workflow.yaml" },
		func(ref *graph.DefinitionRef) { ref.Provenance = &graph.Provenance{Authority: "project"} },
	} {
		ref := base
		mutate(&ref)
		if _, err := EncodeWorkflowActivationExposureRef(ref, "hook"); err == nil {
			t.Fatalf("semantic fields were silently dropped: %#v", ref)
		}
	}
}
