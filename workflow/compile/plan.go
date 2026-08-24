package compile

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
)

const (
	// ExecutionPlanSchemaVersion is the serialized plan envelope version.
	ExecutionPlanSchemaVersion = "1"

	// CodeInvalidWorkflowShape identifies source structure that cannot be
	// represented faithfully by the graph IR.
	CodeInvalidWorkflowShape diagnostic.Code = "HADR-SOURCE-010"
	// CodeInvalidWorkflowID identifies an identity that cannot normalize to a
	// canonical graph identifier.
	CodeInvalidWorkflowID diagnostic.Code = "HADR-SOURCE-011"
	// CodeUnsupportedSourceField identifies a source field that this compiler
	// does not lower and therefore must not silently discard.
	CodeUnsupportedSourceField diagnostic.Code = "HADR-SOURCE-012"
	// CodeInvalidBindingSource identifies an ambiguous or malformed typed value
	// binding.
	CodeInvalidBindingSource diagnostic.Code = "HADR-SOURCE-013"
)

// SourceDigest identifies immutable source content without embedding its
// relocation-sensitive locator. Digest uses SHA-256 over the exact loaded
// bytes, including comments and authoring layout.
type SourceDigest struct {
	Format graph.SourceFormat `json:"format"`
	Digest string             `json:"digest"`
}

// ExecutionPlan is the immutable, extraction-ready build artifact consumed by
// later binding and runtime packages. Definition and Graph contain executable
// identity and semantics. The original locator appears only in provenance and
// source-location fields: the plan-level SourceMap and Graph's root, nested,
// and full-map SourceRef carriers.
//
// SourceMap.Nodes is keyed by normalized node ID, which is the compact source
// reference a later node invocation persists. Inputs and Outputs use their
// normalized names. Edges use EdgeSourceKey.
type ExecutionPlan struct {
	SchemaVersion string              `json:"schema_version"`
	ID            string              `json:"id"`
	Digest        string              `json:"digest"`
	Definition    graph.DefinitionRef `json:"definition"`
	Provenance    graph.Provenance    `json:"provenance"`
	SourceDigests []SourceDigest      `json:"source_digests"`
	Graph         graph.Graph         `json:"graph"`
	SourceMap     graph.SourceMap     `json:"source_map"`
}

// CompileResult is the outcome of lowering a loaded source. Plan is present
// only when Diagnostics is empty.
type CompileResult struct {
	Plan        *ExecutionPlan
	Diagnostics []diagnostic.Diagnostic
}

// EdgeSourceKey returns the stable compact key used by SourceMap.Edges.
func EdgeSourceKey(from, to string, kind graph.EdgeKind) string {
	return from + "->" + to + ":" + string(kind)
}

func sourceDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func digestGraph(value graph.Graph) (string, error) {
	canonical, err := cloneGraph(value)
	if err != nil {
		return "", fmt.Errorf("clone semantic graph for digest: %w", err)
	}
	canonical.Digest = ""
	stripGraphLocations(&canonical)
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("marshal semantic graph for digest: %w", err)
	}
	return sourceDigest(encoded), nil
}

func digestPlan(value ExecutionPlan) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("clone execution plan for digest: %w", err)
	}
	var canonical ExecutionPlan
	if decodeErr := decodeCanonicalJSON(encoded, &canonical); decodeErr != nil {
		return "", fmt.Errorf("clone execution plan for digest: %w", decodeErr)
	}
	canonical.Digest = ""
	canonical.Provenance = graph.Provenance{}
	canonical.SourceMap = graph.SourceMap{}
	canonical.Definition.Locator = ""
	canonical.Definition.Provenance = nil
	stripGraphLocations(&canonical.Graph)
	encoded, err = json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("marshal execution plan for digest: %w", err)
	}
	return sourceDigest(encoded), nil
}

func cloneGraph(value graph.Graph) (graph.Graph, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return graph.Graph{}, err
	}
	var cloned graph.Graph
	if err := decodeCanonicalJSON(encoded, &cloned); err != nil {
		return graph.Graph{}, err
	}
	return cloned, nil
}

func decodeCanonicalJSON(encoded []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	return requireJSONEnd(decoder)
}

func stripGraphLocations(value *graph.Graph) {
	value.Provenance = graph.Provenance{}
	value.Source = nil
	value.SourceMap = graph.SourceMap{}
	stripExtension(&value.Concurrency.Extension)
	if value.Completion != nil {
		stripExtension(&value.Completion.Extension)
	}
	if value.Durability != nil {
		stripExtension(&value.Durability.Extension)
	}
	for name, extension := range value.Extensions {
		stripExtension(&extension)
		value.Extensions[name] = extension
	}
	for i := range value.Inputs {
		value.Inputs[i].Source = nil
		stripBinding(value.Inputs[i].Default)
	}
	for i := range value.Outputs {
		stripOutput(&value.Outputs[i])
	}
	for i := range value.Edges {
		value.Edges[i].Source = nil
	}
	for i := range value.Activations {
		activation := &value.Activations[i]
		activation.Source = nil
		for name, binding := range activation.Inputs {
			stripBinding(&binding)
			activation.Inputs[name] = binding
		}
		stripExpression(activation.Policy.DeduplicationKey)
	}
	for i := range value.Nodes {
		stripNodeLocations(&value.Nodes[i])
	}
}

func stripNodeLocations(node *graph.Node) {
	node.Source = nil
	node.Provenance = graph.Provenance{}
	for i := range node.Needs {
		node.Needs[i].Source = nil
	}
	stripExpression(node.If)
	if node.ForEach != nil {
		stripExpression(&node.ForEach.Items)
	}
	for name, binding := range node.InputBindings {
		stripBinding(&binding)
		node.InputBindings[name] = binding
	}
	for i := range node.Outputs {
		stripOutput(&node.Outputs[i])
	}
	if node.Idempotency != nil {
		stripExpression(node.Idempotency.Key)
	}
	for i := range node.Catch {
		node.Catch[i].Source = nil
		stripExpression(node.Catch[i].When)
	}
	if node.Switch != nil {
		for i := range node.Switch.Arms {
			node.Switch.Arms[i].Source = nil
			stripExpression(&node.Switch.Arms[i].When)
		}
	}
	if node.Call != nil {
		node.Call.Definition.Provenance = nil
	}
	if node.Verification != nil {
		stripVerification(node.Verification)
	}
	if node.Memoization != nil {
		stripExpression(&node.Memoization.Key)
		stripExtension(&node.Memoization.Extension)
	}
	if node.Durability != nil {
		stripExtension(&node.Durability.Extension)
	}
	if node.Service != nil {
		stripVerification(node.Service.ReadyCheck)
		stripExtension(&node.Service.Extension)
	}
	if node.Compensation != nil {
		stripExtension(&node.Compensation.Extension)
	}
	for name, extension := range node.Extensions {
		stripExtension(&extension)
		node.Extensions[name] = extension
	}
}

func stripOutput(output *graph.OutputSpec) {
	output.Source = nil
	stripBinding(output.Value)
}

func stripBinding(binding *graph.Binding) {
	if binding == nil {
		return
	}
	binding.Source = nil
	stripExpression(binding.Expression)
}

func stripExpression(expression *graph.Expression) {
	if expression != nil {
		expression.Source = nil
	}
}

func stripVerification(verification *graph.VerificationSpec) {
	if verification == nil {
		return
	}
	for i := range verification.Checks {
		verification.Checks[i].Source = nil
	}
	stripExtension(&verification.Extension)
}

func stripExtension(extension *graph.Extension) {
	if extension != nil {
		extension.Source = nil
	}
}
