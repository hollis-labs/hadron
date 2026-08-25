package authoring

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/hollis-labs/hadron/workflow/graph"
	graphschema "github.com/hollis-labs/hadron/workflow/graph/schema"
)

// Builder is an immutable authoring view over graph.Graph. Every method returns
// an independently owned value; there is no execution or in-flight plan state.
type Builder struct {
	value graph.Graph
	err   error
}

// New creates an SDK-authored graph with an explicit stable locator.
func New(id, version string) Builder {
	locator := "sdk:" + id + "@" + version
	ref := &graph.SourceRef{Format: graph.SourceSDK, Locator: locator}
	return Builder{value: graph.Graph{
		ID: id, Version: version, Nodes: []graph.Node{}, Source: ref,
		SourceMap:  graph.SourceMap{Graph: ref},
		Provenance: graph.Provenance{Origin: "sdk-graph-ir", Locator: locator, Revision: version},
	}}
}

// Namespace returns a builder with the graph namespace set.
func (b Builder) Namespace(value string) Builder {
	next := b.clone()
	next.value.Namespace = value
	return next
}

// Authority returns a builder with source authority set consistently.
func (b Builder) Authority(value string) Builder {
	next := b.clone()
	next.value.Provenance.Authority = value
	return next
}

// Input appends one workflow input declaration.
func (b Builder) Input(value graph.InputSpec) Builder {
	next := b.clone()
	if next.err != nil {
		return next
	}
	next.value.Inputs = append(next.value.Inputs, value)
	return next.clone()
}

// Output appends one workflow output declaration.
func (b Builder) Output(value graph.OutputSpec) Builder {
	next := b.clone()
	if next.err != nil {
		return next
	}
	next.value.Outputs = append(next.value.Outputs, value)
	return next.clone()
}

// Node appends one executable/control node.
func (b Builder) Node(value graph.Node) Builder {
	next := b.clone()
	if next.err != nil {
		return next
	}
	next.value.Nodes = append(next.value.Nodes, value)
	return next.clone()
}

// Edge appends one explicit normalized edge.
func (b Builder) Edge(value graph.Edge) Builder {
	next := b.clone()
	if next.err != nil {
		return next
	}
	next.value.Edges = append(next.value.Edges, value)
	return next.clone()
}

// Graph returns a defensive copy of the authored canonical IR.
func (b Builder) Graph() graph.Graph { return b.clone().value }

// Envelope returns the generated-client transport shape for this graph.
func (b Builder) Envelope() Envelope {
	value := b.Graph()
	return Envelope{
		SchemaID: EnvelopeSchemaID, SchemaVersion: EnvelopeSchemaVersion,
		MaterialSchemaID: graphschema.ID, MaterialSchemaVersion: graphschema.Version,
		Format: FormatGraphIR, Graph: &value,
	}
}

// Compile delegates to the shared graph compile, dependency inference, and
// validation path.
func (b Builder) Compile(ctx context.Context, options CompileOptions) Result {
	if b.err != nil {
		return Result{Diagnostics: envelopeFindings("SDK graph cannot be encoded canonically: "+b.err.Error(), "Use native JSON-compatible values.")}
	}
	envelope := b.Envelope()
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return Result{Diagnostics: envelopeFindings("SDK graph cannot be encoded canonically", "Use native JSON-compatible values.")}
	}
	decoded, findings := DecodeEnvelope(encoded, options.Limits)
	if len(findings) != 0 {
		return Result{Diagnostics: findings}
	}
	return CompileEnvelope(ctx, decoded, graph.SourceSDK, options)
}

func (b Builder) clone() Builder {
	if b.err != nil {
		return Builder{err: b.err}
	}
	encoded, err := json.Marshal(b.value)
	if err != nil {
		return Builder{err: fmt.Errorf("clone authored graph: %w", err)}
	}
	var value graph.Graph
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return Builder{err: fmt.Errorf("clone authored graph: %w", err)}
	}
	return Builder{value: value}
}
