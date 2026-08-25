package compile

import (
	"strings"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

// GraphCompileOptions supplies exact source identity for graph IR emitted by
// an SDK, UI, or agent. CompileGraph performs the same expansion and immutable
// plan finalization as source lowering; dependency inference and host-selected
// validation remain the caller's explicit subsequent phases.
type GraphCompileOptions struct {
	SourceFormat graph.SourceFormat
	SourceDigest string
	Definition   graph.DefinitionRef
	Compile      CompileOptions
}

// CompileGraph finalizes canonical graph IR into an immutable execution plan.
// It never executes a plan or grants definition authority.
func CompileGraph(input graph.Graph, options GraphCompileOptions) CompileResult {
	cloned, err := cloneGraph(input)
	if err != nil {
		return graphCompileFailure(input.Source, "graph IR is not unambiguous JSON", "Use only native JSON values in graph config, schemas, metadata, and literals.")
	}
	if !options.SourceFormat.Valid() || options.SourceFormat == graph.SourceWorkflow ||
		options.SourceFormat == graph.SourceArchivedBlueprint || options.SourceFormat == graph.SourceArchivedPipeline {
		return graphCompileFailure(cloned.Source, "graph IR source format is unsupported", "Use sdk, ui, or agent for graph IR material.")
	}
	if err := values.ValidateDigest(options.SourceDigest); err != nil {
		return graphCompileFailure(cloned.Source, "graph IR source digest is invalid", "Hash the exact schema-validated graph JSON before compilation.")
	}
	definition := options.Definition
	if strings.TrimSpace(definition.Kind) == "" {
		definition.Kind = "workflow"
	}
	if definition.ID == "" {
		definition.ID = cloned.ID
	}
	if definition.Version == "" {
		definition.Version = cloned.Version
	}
	if definition.Digest == "" {
		definition.Digest = options.SourceDigest
	}
	if definition.Kind != "workflow" || definition.ID != cloned.ID || definition.Version != cloned.Version || definition.Digest != options.SourceDigest {
		return graphCompileFailure(cloned.Source, "graph IR definition identity conflicts with its exact material", "Use the graph ID/version and exact source digest in the immutable DefinitionRef.")
	}
	if cloned.Source == nil {
		locator := definition.Locator
		if locator == "" {
			locator = string(options.SourceFormat) + ":" + strings.TrimPrefix(options.SourceDigest, "sha256:")
		}
		cloned.Source = &graph.SourceRef{Format: options.SourceFormat, Locator: locator}
	}
	if cloned.Source.Format != options.SourceFormat {
		return graphCompileFailure(cloned.Source, "graph IR source reference format conflicts with its envelope", "Use one explicit source format consistently across the envelope and source map.")
	}
	if cloned.Provenance.Authority == "" {
		cloned.Provenance.Authority = definition.Authority
	}
	if cloned.Provenance.Origin == "" {
		cloned.Provenance.Origin = string(options.SourceFormat) + "-graph-ir"
	}
	if cloned.Provenance.Locator == "" {
		cloned.Provenance.Locator = cloned.Source.Locator
	}
	if cloned.Provenance.Revision == "" {
		cloned.Provenance.Revision = cloned.Version
	}
	if cloned.Provenance.Digest == "" {
		cloned.Provenance.Digest = options.SourceDigest
	}
	definition.Authority = cloned.Provenance.Authority
	definition.Locator = cloned.Provenance.Locator
	provenance := cloned.Provenance
	definition.Provenance = &provenance
	return finalizeGraph(cloned, definition, []SourceDigest{{Format: options.SourceFormat, Digest: options.SourceDigest}}, options.Compile)
}

func finalizeGraph(input graph.Graph, definition graph.DefinitionRef, sources []SourceDigest, options CompileOptions) CompileResult {
	bundled, findings := expandGraph(input, options)
	if len(findings) != 0 {
		return CompileResult{Diagnostics: findings}
	}
	compiledGraph := bundled.Graph
	graphDigest, err := digestGraph(compiledGraph)
	if err != nil {
		return graphCompileFailure(compiledGraph.Source, "graph IR digest could not be computed", "Use only canonical JSON-compatible graph values.")
	}
	compiledGraph.Digest = graphDigest
	plan := ExecutionPlan{
		SchemaVersion:      ExecutionPlanSchemaVersion,
		ID:                 compiledGraph.ID,
		Definition:         definition,
		Provenance:         compiledGraph.Provenance,
		SourceDigests:      append([]SourceDigest(nil), sources...),
		Graph:              compiledGraph,
		SourceMap:          compiledGraph.SourceMap,
		BundledDefinitions: bundled.Definitions,
	}
	planDigest, err := digestPlan(plan)
	if err != nil {
		return graphCompileFailure(compiledGraph.Source, "execution plan digest could not be computed", "Use only canonical JSON-compatible graph values.")
	}
	plan.Digest = planDigest
	return CompileResult{Plan: &plan}
}

func graphCompileFailure(source *graph.SourceRef, message, remediation string) CompileResult {
	finding := diagnostic.Diagnostic{
		Severity:    diagnostic.SeverityError,
		Code:        CodeInvalidWorkflowShape,
		Message:     message,
		Source:      cloneSource(source),
		Remediation: &diagnostic.Remediation{Message: remediation},
	}
	return CompileResult{Diagnostics: []diagnostic.Diagnostic{finding}}
}
