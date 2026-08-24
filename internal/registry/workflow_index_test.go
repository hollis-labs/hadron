package registry

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/workflow/graph"
)

func TestWorkflowIndexPinsVersionsAndMovesOnlyExplicitAlias(t *testing.T) {
	index := NewWorkflowIndex()
	first := workflowRecord("orders", "1.0.0", "workflow:\n  name: orders\n  version: 1.0.0\n")
	second := workflowRecord("orders", "2.0.0", "workflow:\n  name: orders\n  version: 2.0.0\n")
	var err error
	if first, err = index.RegisterWorkflow(t.Context(), first, true); err != nil {
		t.Fatal(err)
	}
	if second, err = index.RegisterWorkflow(t.Context(), second, false); err != nil {
		t.Fatal(err)
	}

	current, err := index.ResolveWorkflow(t.Context(), WorkflowQuery{Name: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if !current.Movable || current.Record.Version != first.Version {
		t.Fatalf("current = %+v", current)
	}
	exact, err := index.ResolveWorkflow(t.Context(), WorkflowQuery{Name: "orders", Version: second.Version, Digest: second.Digest})
	if err != nil {
		t.Fatal(err)
	}
	if exact.Movable || exact.Record.Digest != second.Digest {
		t.Fatalf("exact = %+v", exact)
	}

	current.Record.Source[0] = 'x'
	current.Record.Provenance.Metadata["publisher"] = "changed"
	reloaded, err := index.ResolveWorkflow(t.Context(), WorkflowQuery{Name: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if string(reloaded.Record.Source) != string(first.Source) || reloaded.Record.Provenance.Metadata["publisher"] != "test" {
		t.Fatalf("registry result was not defensively copied: %+v", reloaded.Record)
	}

	if _, moveErr := index.RegisterWorkflow(t.Context(), second, true); moveErr != nil {
		t.Fatal(moveErr)
	}
	moved, err := index.ResolveWorkflow(t.Context(), WorkflowQuery{Name: "orders"})
	if err != nil || moved.Record.Version != second.Version {
		t.Fatalf("moved current = %+v, %v", moved, err)
	}
}

func TestWorkflowIndexRejectsDivergentReplayPinsAndAmbiguousText(t *testing.T) {
	index := NewWorkflowIndex()
	record := workflowRecord("orders", "1.0.0", "workflow:\n  name: orders\n  version: 1.0.0\n")
	if _, err := index.RegisterWorkflow(t.Context(), record, true); err != nil {
		t.Fatal(err)
	}
	changed := record
	changed.Source = []byte("workflow:\n  name: other\n")
	changed.Digest = ""
	changed.Provenance.Digest = ""
	if _, err := index.RegisterWorkflow(t.Context(), changed, false); !errors.Is(err, ErrWorkflowConflict) {
		t.Fatalf("divergent replay error = %v", err)
	}
	if _, err := index.ResolveWorkflow(t.Context(), WorkflowQuery{Name: record.Name, Version: record.Version, Digest: strings.Repeat("sha256:0", 1)}); !errors.Is(err, ErrInvalidWorkflow) {
		t.Fatalf("malformed digest error = %v", err)
	}
	for _, mutation := range []func(*WorkflowRecord){
		func(value *WorkflowRecord) { value.Version = "v1\x00shadow" },
		func(value *WorkflowRecord) { value.Authority = "registry\nshadow" },
		func(value *WorkflowRecord) { value.TrustClass = "trusted\x7fshadow" },
		func(value *WorkflowRecord) { value.Provenance.Revision = "v1\x00shadow" },
	} {
		invalid := workflowRecord("safe", "v1", "workflow:\n  name: safe\n  version: v1\n")
		mutation(&invalid)
		if _, err := index.RegisterWorkflow(context.Background(), invalid, false); !errors.Is(err, ErrInvalidWorkflow) {
			t.Fatalf("ambiguous text error = %v", err)
		}
	}
	if _, err := index.ResolveWorkflow(t.Context(), WorkflowQuery{Name: record.Name, Version: "1.0.0\x00shadow"}); !errors.Is(err, ErrInvalidWorkflow) {
		t.Fatalf("ambiguous query version error = %v", err)
	}
	conflictingRevision := workflowRecord("revision", "v1", "workflow:\n  name: revision\n  version: v1\n")
	conflictingRevision.Provenance.Revision = "v2"
	if _, err := index.RegisterWorkflow(t.Context(), conflictingRevision, false); !errors.Is(err, ErrWorkflowConflict) {
		t.Fatalf("provenance revision conflict = %v", err)
	}
}

func workflowRecord(name, version, source string) WorkflowRecord {
	return WorkflowRecord{
		Name: name, Version: version, Source: []byte(source), Authority: "registry.test", TrustClass: "signed",
		Provenance: graph.Provenance{Origin: "test-publisher", Locator: "registry://" + name + "/" + version + "/" + name + ".workflow.yaml", Metadata: graph.Metadata{"publisher": "test"}},
	}
}
