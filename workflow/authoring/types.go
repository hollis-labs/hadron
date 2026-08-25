package authoring

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	graphschema "github.com/hollis-labs/hadron/workflow/graph/schema"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	// EnvelopeSchemaID is the closed authoring transport contract.
	EnvelopeSchemaID = "https://schemas.hollis-labs.dev/workflow/authoring/v1/envelope.schema.json"
	// EnvelopeSchemaVersion is the compact negotiation version for EnvelopeSchemaID.
	EnvelopeSchemaVersion = "1"
	// WorkflowSourceSchemaID identifies the graph-native YAML source contract.
	WorkflowSourceSchemaID = "https://schemas.hollis-labs.dev/workflow/source/v1/workflow-source"
	// WorkflowSourceSchemaVersion is the supported workflow-source negotiation version.
	WorkflowSourceSchemaVersion = "1"

	FormatGraphIR        MaterialFormat = "graph_ir"
	FormatWorkflowSource MaterialFormat = "workflow_source"

	DefaultMaximumBytes = 2 << 20
	DefaultMaximumDepth = 128
	DefaultMaximumNodes = 512
	DefaultMaximumEdges = 4096

	maximumCompactDiagnostics   = 64
	maximumCompactMessageBytes  = 1024
	maximumCompactHelpBytes     = 1024
	maximumCompactLocatorBytes  = 512
	maximumCompactPathSegments  = 32
	maximumCompactPathPartBytes = 128

	CodeUnsupportedSchema    diagnostic.Code = "HADR-SOURCE-038"
	CodeInvalidEnvelope      diagnostic.Code = "HADR-SOURCE-039"
	CodeDiagnosticsTruncated diagnostic.Code = "HADR-SOURCE-040"
)

// MaterialFormat selects exactly one authoring payload.
type MaterialFormat string

func (f MaterialFormat) Valid() bool { return f == FormatGraphIR || f == FormatWorkflowSource }

// SourceSchemaFor returns the only accepted material schema for a persisted
// source discriminator. No caller should infer the format from source bytes.
func SourceSchemaFor(format graph.SourceFormat) (id, version string, supported bool) {
	switch format {
	case graph.SourceWorkflow:
		return WorkflowSourceSchemaID, WorkflowSourceSchemaVersion, true
	case graph.SourceSDK, graph.SourceUI, graph.SourceAgent:
		return graphschema.ID, graphschema.Version, true
	default:
		return "", "", false
	}
}

// Envelope is the single strict ingress shape shared by generated clients and
// agent adapters. SchemaID and SchemaVersion identify this envelope; Material
// fields identify the selected graph/source schema without implicit fallback.
type Envelope struct {
	SchemaID              string         `json:"schema_id"`
	SchemaVersion         string         `json:"schema_version"`
	MaterialSchemaID      string         `json:"material_schema_id"`
	MaterialSchemaVersion string         `json:"material_schema_version"`
	Format                MaterialFormat `json:"format"`
	Graph                 *graph.Graph   `json:"graph,omitempty"`
	Source                string         `json:"source,omitempty"`
}

// Limits bounds JSON work before compilation or validation.
type Limits struct {
	MaximumBytes int
	MaximumDepth int
	MaximumNodes int
	MaximumEdges int
}

// CompileOptions supplies the exact ordinary compiler and validator catalogs.
type CompileOptions struct {
	Compile    compile.CompileOptions
	Dependency compile.DependencyOptions
	Validation compile.ValidationOptions
	Definition graph.DefinitionRef
	Limits     Limits
}

// Result carries either a fully inferred and validated plan or diagnostics.
type Result struct {
	Plan        *compile.ExecutionPlan
	Visibility  compile.ValueVisibilityPlan
	Deferred    []compile.DeferredDependency
	Diagnostics []diagnostic.Diagnostic
}

// CompactDiagnostic is the bounded generated-client projection. Full
// diagnostics remain available to Go callers.
type CompactDiagnostic struct {
	Severity string   `json:"severity"`
	Code     string   `json:"code"`
	Message  string   `json:"message"`
	Path     []string `json:"path,omitempty"`
	Locator  string   `json:"locator,omitempty"`
	Help     string   `json:"help,omitempty"`
}

// CompactDiagnostics returns a deterministic, bounded, presentation-safe
// projection. A final sentinel makes finding-count omissions explicit; an
// ellipsis marks truncated fields and path segments.
func CompactDiagnostics(input []diagnostic.Diagnostic) []CompactDiagnostic {
	count := len(input)
	truncated := count > maximumCompactDiagnostics
	if truncated {
		count = maximumCompactDiagnostics - 1
	}
	result := make([]CompactDiagnostic, 0, count+1)
	for _, finding := range input[:count] {
		compact := CompactDiagnostic{
			Severity: string(finding.Severity),
			Code:     string(finding.Code),
			Message:  truncateUTF8(finding.Message, maximumCompactMessageBytes),
		}
		if finding.Source != nil {
			compact.Path = compactPath(finding.Source.Path)
			compact.Locator = truncateUTF8(finding.Source.Locator, maximumCompactLocatorBytes)
		}
		if finding.Remediation != nil {
			compact.Help = truncateUTF8(finding.Remediation.Message, maximumCompactHelpBytes)
		}
		result = append(result, compact)
	}
	if truncated {
		result = append(result, CompactDiagnostic{
			Severity: string(diagnostic.SeverityWarning),
			Code:     string(CodeDiagnosticsTruncated),
			Message:  fmt.Sprintf("%d additional diagnostics were omitted from this compact result", len(input)-count),
		})
	}
	return result
}

func compactPath(input []string) []string {
	count := len(input)
	truncated := count > maximumCompactPathSegments
	if truncated {
		count = maximumCompactPathSegments - 1
	}
	result := make([]string, 0, count+1)
	for _, part := range input[:count] {
		result = append(result, truncateUTF8(part, maximumCompactPathPartBytes))
	}
	if truncated {
		result = append(result, "…")
	}
	return result
}

func truncateUTF8(input string, maximumBytes int) string {
	if len(input) <= maximumBytes {
		return input
	}
	const marker = "…"
	end := maximumBytes - len(marker)
	for end > 0 && !utf8.RuneStart(input[end]) {
		end--
	}
	return input[:end] + marker
}

// DecodeEnvelope enforces bounds, JSON shape, schema negotiation, and graph
// JSON Schema before any compiler work begins.
func DecodeEnvelope(data []byte, limits Limits) (Envelope, []diagnostic.Diagnostic) {
	limits = normalizeLimits(limits)
	if len(data) == 0 || len(data) > limits.MaximumBytes {
		return Envelope{}, envelopeFindings("authoring envelope exceeds the configured byte bound", "Send one non-empty bounded envelope.")
	}
	if err := validateJSONDepth(data, limits.MaximumDepth); err != nil {
		return Envelope{}, envelopeFindings("authoring envelope exceeds the configured JSON depth", "Reduce nested graph config, schema, metadata, or literal values.")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	var envelope Envelope
	if err := decoder.Decode(&envelope); err != nil {
		return Envelope{}, envelopeFindings("authoring envelope has an invalid or unknown field", "Use the generated authoring envelope type without additional fields.")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return Envelope{}, envelopeFindings("authoring envelope contains trailing JSON", "Send exactly one JSON object.")
	}
	if envelope.SchemaID != EnvelopeSchemaID || envelope.SchemaVersion != EnvelopeSchemaVersion {
		return Envelope{}, schemaFindings(envelope.SchemaID, envelope.SchemaVersion)
	}
	if !envelope.Format.Valid() {
		return Envelope{}, envelopeFindings("authoring material format is unsupported", "Use graph_ir or workflow_source.")
	}
	switch envelope.Format {
	case FormatGraphIR:
		if envelope.MaterialSchemaID != graphschema.ID || envelope.MaterialSchemaVersion != graphschema.Version || envelope.Graph == nil || envelope.Source != "" {
			return Envelope{}, schemaFindings(envelope.MaterialSchemaID, envelope.MaterialSchemaVersion)
		}
		if len(envelope.Graph.Nodes) > limits.MaximumNodes || len(envelope.Graph.Edges) > limits.MaximumEdges {
			return Envelope{}, envelopeFindings("graph IR exceeds the configured structural bounds", "Reduce nodes or edges before authoring submission.")
		}
		encoded, err := json.Marshal(envelope.Graph)
		if err != nil || graphschema.Validate(encoded) != nil {
			return Envelope{}, envelopeFindings("graph IR does not satisfy the committed schema", "Validate against the negotiated graph schema before submission.")
		}
	case FormatWorkflowSource:
		if envelope.MaterialSchemaID != WorkflowSourceSchemaID || envelope.MaterialSchemaVersion != WorkflowSourceSchemaVersion || envelope.Graph != nil || strings.TrimSpace(envelope.Source) == "" {
			return Envelope{}, schemaFindings(envelope.MaterialSchemaID, envelope.MaterialSchemaVersion)
		}
	}
	return envelope, nil
}

// CompileEnvelope applies the ordinary compiler, dependency inference, and
// validator to already decoded material.
func CompileEnvelope(ctx context.Context, envelope Envelope, sourceFormat graph.SourceFormat, options CompileOptions) Result {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return Result{Diagnostics: envelopeFindings(err.Error(), "Retry with a live context.")}
	}
	var compiled compile.CompileResult
	switch envelope.Format {
	case FormatGraphIR:
		if envelope.Graph == nil {
			return Result{Diagnostics: envelopeFindings("graph IR is required", "Decode a valid graph_ir envelope first.")}
		}
		encoded, err := json.Marshal(envelope.Graph)
		if err != nil {
			return Result{Diagnostics: envelopeFindings("graph IR cannot be encoded canonically", "Use native JSON-compatible values.")}
		}
		definition := options.Definition
		if definition.Digest == "" {
			definition.Digest = values.SHA256Digest(encoded)
		}
		compiled = compile.CompileGraph(*envelope.Graph, compile.GraphCompileOptions{
			SourceFormat: sourceFormat, SourceDigest: definition.Digest, Definition: definition, Compile: options.Compile,
		})
	case FormatWorkflowSource:
		locator := options.Definition.Locator
		if locator == "" {
			locator = "authored.workflow.yaml"
		}
		loaded := compile.LoadBytes(locator, []byte(envelope.Source))
		if loaded.Source == nil || len(loaded.Diagnostics) != 0 {
			return Result{Diagnostics: loaded.Diagnostics}
		}
		compiled = compile.CompileWithOptions(loaded.Source, options.Compile)
	default:
		return Result{Diagnostics: envelopeFindings("authoring material format is unsupported", "Decode a supported envelope first.")}
	}
	if compiled.Plan == nil || len(compiled.Diagnostics) != 0 {
		return Result{Diagnostics: compiled.Diagnostics}
	}
	if findings := ValidatePlanLimits(compiled.Plan, options.Limits); len(findings) != 0 {
		return Result{Diagnostics: findings}
	}
	inferred := compile.InferValueDependencies(compiled.Plan, options.Dependency)
	result := Result{Visibility: inferred.Visibility, Deferred: append([]compile.DeferredDependency(nil), inferred.Deferred...), Diagnostics: append([]diagnostic.Diagnostic(nil), inferred.Diagnostics...)}
	if inferred.Plan == nil || len(inferred.Diagnostics) != 0 {
		return result
	}
	if findings := ValidatePlanLimits(inferred.Plan, options.Limits); len(findings) != 0 {
		result.Diagnostics = append(result.Diagnostics, findings...)
		return result
	}
	result.Diagnostics = append(result.Diagnostics, compile.ValidatePlan(ctx, inferred.Plan, options.Validation)...)
	if len(result.Diagnostics) == 0 {
		result.Plan = inferred.Plan
	}
	return result
}

// ValidatePlanLimits applies authoring ingress structural ceilings to the
// final expanded plan, including bundled child definitions. It does not alter
// the full compiler diagnostics or execution plan.
func ValidatePlanLimits(plan *compile.ExecutionPlan, limits Limits) []diagnostic.Diagnostic {
	limits = normalizeLimits(limits)
	if plan == nil {
		return envelopeFindings("compiled execution plan is unavailable", "Repair compiler diagnostics before authoring submission.")
	}
	nodes, edges := len(plan.Graph.Nodes), len(plan.Graph.Edges)
	for _, bundled := range plan.BundledDefinitions {
		nodes += len(bundled.Graph.Nodes)
		edges += len(bundled.Graph.Edges)
	}
	if nodes <= limits.MaximumNodes && edges <= limits.MaximumEdges {
		return nil
	}
	return envelopeFindings(
		fmt.Sprintf("compiled execution plan exceeds the configured structural bounds: nodes %d/%d, edges %d/%d", nodes, limits.MaximumNodes, edges, limits.MaximumEdges),
		"Reduce nodes, edges, or expansion output before authoring submission.",
	)
}

func normalizeLimits(input Limits) Limits {
	if input.MaximumBytes <= 0 {
		input.MaximumBytes = DefaultMaximumBytes
	}
	if input.MaximumDepth <= 0 {
		input.MaximumDepth = DefaultMaximumDepth
	}
	if input.MaximumNodes <= 0 {
		input.MaximumNodes = DefaultMaximumNodes
	}
	if input.MaximumEdges <= 0 {
		input.MaximumEdges = DefaultMaximumEdges
	}
	return input
}

func validateJSONDepth(data []byte, maximum int) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	depth := 0
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			continue
		}
		switch delimiter {
		case '{', '[':
			depth++
			if depth > maximum {
				return fmt.Errorf("JSON depth exceeds %d", maximum)
			}
		case '}', ']':
			depth--
		}
	}
}

func envelopeFindings(message, remediation string) []diagnostic.Diagnostic {
	return []diagnostic.Diagnostic{{Severity: diagnostic.SeverityError, Code: CodeInvalidEnvelope, Message: message, Remediation: &diagnostic.Remediation{Message: remediation}}}
}

func schemaFindings(id, version string) []diagnostic.Diagnostic {
	return []diagnostic.Diagnostic{{
		Severity: diagnostic.SeverityError, Code: CodeUnsupportedSchema,
		Message:     fmt.Sprintf("unsupported authoring schema %q version %q", id, version),
		Remediation: &diagnostic.Remediation{Message: "Negotiate and send an explicitly supported schema ID and version."},
	}}
}
