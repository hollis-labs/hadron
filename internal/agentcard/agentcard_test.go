package agentcard

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/go-workflow/graph"
	"github.com/hollis-labs/go-workflow/values"
)

type fakePublishedWorkflows struct {
	descriptors []appworkflow.WorkflowExposureDescriptor
	err         error
}

func (f *fakePublishedWorkflows) PublishedWorkflows(context.Context) ([]appworkflow.WorkflowExposureDescriptor, error) {
	return f.descriptors, f.err
}

func TestBuilderUsesExactPublishedContractsAndDefensiveCopies(t *testing.T) {
	digestOne := values.SHA256Digest([]byte("one"))
	digestTwo := values.SHA256Digest([]byte("two"))
	source := &fakePublishedWorkflows{descriptors: []appworkflow.WorkflowExposureDescriptor{
		publishedDescriptor("team/report", "v1", digestOne),
		publishedDescriptor("team/report", "v2", digestTwo),
	}}
	builder, err := NewBuilder(source)
	if err != nil {
		t.Fatal(err)
	}
	card, err := builder.Card(t.Context(), "https://hadron.test")
	if err != nil {
		t.Fatal(err)
	}
	if len(card.Skills) != 2 || card.Skills[0].ID != "team/report@v1@"+digestOne || card.Skills[1].ID != "team/report@v2@"+digestTwo || card.Skills[0].Definition != source.descriptors[0].Definition {
		t.Fatalf("skills = %#v", card.Skills)
	}
	if card.Skills[0].Provenance.Authority != "registry.test" || card.Skills[0].InputSchema["required"] == nil || card.Skills[0].OutputSchema["required"] == nil || card.Skills[0].Effects[0] != graph.EffectRead {
		t.Fatalf("exact contract = %#v", card.Skills[0])
	}
	if card.Skills[0].Evidence.ContractTestDigest != source.descriptors[0].Evidence.ContractTestDigest || !card.Skills[0].Evidence.TestsPassed {
		t.Fatalf("qualification evidence = %#v", card.Skills[0].Evidence)
	}
	card.Skills[0].Tags[0] = "mutated"
	card.Skills[0].InputSchema["type"] = "array"
	if source.descriptors[0].Tags[0] != "reports" || source.descriptors[0].InputSchema["type"] != "object" {
		t.Fatal("caller mutation escaped the agent-card snapshot")
	}
	encoded, err := card.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "registry://private/source") || strings.Contains(string(encoded), "publisher_metadata") {
		t.Fatalf("card leaked private provenance: %s", encoded)
	}
	var roundTrip AgentCard
	if err := json.Unmarshal(encoded, &roundTrip); err != nil || len(roundTrip.Skills) != 2 {
		t.Fatalf("round trip = %#v, %v", roundTrip, err)
	}
}

func TestBuilderFailsClosedWhenCardExceedsBound(t *testing.T) {
	descriptor := publishedDescriptor("team/large", "v1", values.SHA256Digest([]byte("large")))
	descriptor.Description = strings.Repeat("x", MaximumAgentCardBytes)
	builder, err := NewBuilder(&fakePublishedWorkflows{descriptors: []appworkflow.WorkflowExposureDescriptor{descriptor}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := builder.Card(t.Context(), "https://hadron.test"); err == nil {
		t.Fatal("oversized agent card was accepted")
	}
}

func publishedDescriptor(name, version, digest string) appworkflow.WorkflowExposureDescriptor {
	return appworkflow.WorkflowExposureDescriptor{
		Name: name, Namespace: "team", Version: version, Digest: digest,
		Description: "Generate a report.", Tags: []string{"reports"},
		Definition:   graph.DefinitionRef{Kind: "registry", ID: name, Version: version, Digest: digest},
		Provenance:   appworkflow.WorkflowExposureProvenance{Authority: "registry.test", Origin: "publisher", Revision: version, Digest: digest, TrustClass: "signed"},
		Evidence:     appworkflow.WorkflowQualificationEvidence{PlanDigest: values.SHA256Digest([]byte("plan:" + digest)), ContractSuiteDigest: values.SHA256Digest([]byte("suite:" + digest)), ContractTestDigest: values.SHA256Digest([]byte("test:" + digest)), TestsPassed: true},
		Effects:      graph.EffectSet{graph.EffectRead},
		InputSchema:  graph.Schema{"type": "object", "additionalProperties": false, "properties": map[string]any{"query": map[string]any{"type": "string"}}, "required": []string{"query"}},
		OutputSchema: graph.Schema{"type": "object", "additionalProperties": false, "properties": map[string]any{"report": map[string]any{"type": "string"}}, "required": []string{"report"}},
	}
}
