package compile

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
)

const (
	// DefaultMaxCallDepth bounds definition traversal when a validation caller
	// supplies a DefinitionResolver without an explicit limit.
	DefaultMaxCallDepth = 32

	// CodeDuplicateNodeID identifies node identities that collide after graph
	// normalization.
	CodeDuplicateNodeID diagnostic.Code = "HADR-SOURCE-020"
	// CodeGraphCycle identifies a cycle in explicit needs or normalized edges.
	CodeGraphCycle diagnostic.Code = "HADR-SOURCE-021"
	// CodeUnknownStepKind identifies a kind/version unavailable from the
	// supplied registry, including an unpinned kind with multiple versions.
	CodeUnknownStepKind diagnostic.Code = "HADR-SOURCE-022"
	// CodeInvalidCallMode identifies a call mode outside inline and run.
	CodeInvalidCallMode diagnostic.Code = "HADR-SOURCE-023"
	// CodeUnsupportedReadinessRule identifies an unsupported node readiness
	// rule.
	CodeUnsupportedReadinessRule diagnostic.Code = "HADR-SOURCE-024"
	// CodeInvalidForEach identifies a structurally invalid fan-out declaration.
	CodeInvalidForEach diagnostic.Code = "HADR-SOURCE-025"
	// CodeInvalidStepConfig identifies config rejected by a registered kind's
	// JSON Schema or by an adapter diagnostic without its own valid code.
	CodeInvalidStepConfig diagnostic.Code = "HADR-SOURCE-026"
	// CodeInvalidValidationInput identifies a nil plan passed to validation.
	CodeInvalidValidationInput diagnostic.Code = "HADR-SOURCE-027"
	// CodeInvalidCallShape identifies a call kind without a call declaration or
	// a non-call kind carrying call-only semantics.
	CodeInvalidCallShape diagnostic.Code = "HADR-SOURCE-029"

	// CodeUnknownDependency identifies an explicit dependency endpoint absent
	// from the graph.
	CodeUnknownDependency diagnostic.Code = "HADR-REF-002"
	// CodeCallCycle identifies a recursive definition already active in the
	// current call path.
	CodeCallCycle diagnostic.Code = "HADR-REF-003"
	// CodeCallDepthExceeded identifies a definition call beyond the configured
	// traversal depth.
	CodeCallDepthExceeded diagnostic.Code = "HADR-REF-004"
	// CodeDefinitionResolution identifies a resolver failure during opt-in call
	// graph validation.
	CodeDefinitionResolution diagnostic.Code = "HADR-REF-005"

	// CodePolicyViolation is the fallback assignment for a policy hook finding
	// that omits or supplies an invalid code.
	CodePolicyViolation diagnostic.Code = "HADR-POLICY-001"
	// CodeInvalidKindSchema identifies registered metadata that is JSON-shaped
	// but cannot compile as JSON Schema.
	CodeInvalidKindSchema diagnostic.Code = "HADR-HOST-010"
)

// StepKindLookup is the read-only registry surface required by validation.
// A stepkind.Registry satisfies this interface.
type StepKindLookup interface {
	Lookup(name, version string) (stepkind.StepKind, bool)
	List() []stepkind.StepKindSpec
}

// NodeValidation is the application-neutral input supplied to a PolicyHook.
// Kind is nil when the node's step kind is unavailable; hooks may still report
// policy findings based on graph declarations alone.
type NodeValidation struct {
	GraphID string
	Node    graph.Node
	Kind    *stepkind.StepKindSpec
}

// PolicyHook validates effect, retry, idempotency, or other host-selected
// policy combinations without importing a concrete policy implementation.
type PolicyHook interface {
	ValidateNode(ctx context.Context, input NodeValidation) []diagnostic.Diagnostic
}

// PolicyHookFunc adapts a function to PolicyHook.
type PolicyHookFunc func(context.Context, NodeValidation) []diagnostic.Diagnostic

// ValidateNode implements PolicyHook.
func (f PolicyHookFunc) ValidateNode(ctx context.Context, input NodeValidation) []diagnostic.Diagnostic {
	return f(ctx, input)
}

// ResolvedDefinition is the immutable definition identity and graph returned
// by a DefinitionResolver for opt-in call traversal and execution. InputBindings
// are resolver-owned partial-application defaults (for example, an imported
// definition's with: bindings). Call sites overlay their node-local bindings
// after these defaults are evaluated.
type ResolvedDefinition struct {
	Definition    graph.DefinitionRef
	Graph         graph.Graph
	InputBindings map[string]graph.Binding
}

// DefinitionResolver resolves a child definition for call-cycle and depth
// validation. Ordinary structural validation never invokes it.
type DefinitionResolver interface {
	ResolveDefinition(ctx context.Context, ref graph.DefinitionRef) (ResolvedDefinition, error)
}

// DefinitionResolverFunc adapts a function to DefinitionResolver.
type DefinitionResolverFunc func(context.Context, graph.DefinitionRef) (ResolvedDefinition, error)

// ResolveDefinition implements DefinitionResolver.
func (f DefinitionResolverFunc) ResolveDefinition(ctx context.Context, ref graph.DefinitionRef) (ResolvedDefinition, error) {
	return f(ctx, ref)
}

// ValidationOptions supplies extraction-safe collaborators for validation.
// Definitions is optional; when nil, validation does not resolve calls.
// MaxCallDepth defaults to DefaultMaxCallDepth when it is not positive.
type ValidationOptions struct {
	StepKinds    StepKindLookup
	PolicyHooks  []PolicyHook
	Definitions  DefinitionResolver
	MaxCallDepth int
}

// ValidatePlan validates a compiled plan without binding or starting runtime
// work. A nil plan produces one structured diagnostic.
func ValidatePlan(ctx context.Context, plan *ExecutionPlan, options ValidationOptions) []diagnostic.Diagnostic {
	if plan == nil {
		return []diagnostic.Diagnostic{{
			Severity: diagnostic.SeverityError,
			Code:     CodeInvalidValidationInput,
			Message:  "execution plan is required for validation",
			Remediation: &diagnostic.Remediation{
				Message: "Compile a workflow source successfully before validation.",
			},
		}}
	}
	return validate(ctx, plan.Graph, plan.Definition, options)
}

// ValidateGraph validates an application-neutral graph directly. Its root
// definition identity is derived from immutable graph fields for call-cycle
// detection.
func ValidateGraph(ctx context.Context, value graph.Graph, options ValidationOptions) []diagnostic.Diagnostic {
	root := graph.DefinitionRef{
		Kind:    "workflow",
		ID:      value.ID,
		Locator: value.Provenance.Locator,
		Version: value.Version,
		Digest:  value.Digest,
	}
	return validate(ctx, value, root, options)
}

type validator struct {
	ctx         context.Context
	graph       graph.Graph
	root        graph.DefinitionRef
	options     ValidationOptions
	kinds       kindCatalog
	diagnostics []diagnostic.Diagnostic
}

func validate(ctx context.Context, value graph.Graph, root graph.DefinitionRef, options ValidationOptions) []diagnostic.Diagnostic {
	if ctx == nil {
		ctx = context.Background()
	}
	v := validator{
		ctx:     ctx,
		graph:   value,
		root:    root,
		options: options,
		kinds:   newKindCatalog(options.StepKinds),
	}
	v.validateStructure()
	v.validateNodes()
	v.validateCalls()
	sortDiagnostics(v.diagnostics)
	return append([]diagnostic.Diagnostic(nil), v.diagnostics...)
}

func (v *validator) add(code diagnostic.Code, source *graph.SourceRef, message, remediation string, related ...diagnostic.RelatedReference) {
	v.diagnostics = append(v.diagnostics, diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError,
		Code:     code,
		Message:  message,
		Source:   cloneSource(source),
		Related:  cloneRelated(related),
		Remediation: &diagnostic.Remediation{
			Message: remediation,
		},
	})
}

func (v *validator) nodeSource(node graph.Node) *graph.SourceRef {
	if node.Source != nil {
		return node.Source
	}
	if source, ok := v.graph.SourceMap.Nodes[node.ID]; ok {
		return &source
	}
	if normalized := graph.NormalizeID(node.ID); normalized != node.ID {
		if source, ok := v.graph.SourceMap.Nodes[normalized]; ok {
			return &source
		}
	}
	return graphSource(v.graph)
}

func graphSource(value graph.Graph) *graph.SourceRef {
	if value.Source != nil {
		return value.Source
	}
	return value.SourceMap.Graph
}

func cloneSource(source *graph.SourceRef) *graph.SourceRef {
	if source == nil {
		return nil
	}
	cloned := *source
	cloned.Path = append([]string(nil), source.Path...)
	return &cloned
}

func cloneRelated(related []diagnostic.RelatedReference) []diagnostic.RelatedReference {
	if len(related) == 0 {
		return nil
	}
	cloned := make([]diagnostic.RelatedReference, len(related))
	copy(cloned, related)
	for i := range cloned {
		cloned[i].Source.Path = append([]string(nil), related[i].Source.Path...)
	}
	return cloned
}

func relatedSource(message string, source *graph.SourceRef) []diagnostic.RelatedReference {
	if source == nil {
		return nil
	}
	return []diagnostic.RelatedReference{{Message: message, Source: *cloneSource(source)}}
}

func normalizeFinding(finding diagnostic.Diagnostic, source *graph.SourceRef, fallbackCode diagnostic.Code, fallbackMessage, fallbackRemediation string) diagnostic.Diagnostic {
	if !finding.Severity.Valid() {
		finding.Severity = diagnostic.SeverityError
	}
	if finding.Code.Validate() != nil {
		finding.Code = fallbackCode
	}
	if strings.TrimSpace(finding.Message) == "" {
		finding.Message = fallbackMessage
	}
	if finding.Source == nil {
		finding.Source = cloneSource(source)
	} else {
		finding.Source = cloneSource(finding.Source)
	}
	finding.Related = cloneRelated(finding.Related)
	if finding.Remediation == nil || strings.TrimSpace(finding.Remediation.Message) == "" {
		finding.Remediation = &diagnostic.Remediation{Message: fallbackRemediation}
	} else {
		remediation := *finding.Remediation
		finding.Remediation = &remediation
	}
	return finding
}

func sortDiagnostics(findings []diagnostic.Diagnostic) {
	sort.SliceStable(findings, func(i, j int) bool {
		left, right := diagnosticSortKey(findings[i]), diagnosticSortKey(findings[j])
		return left < right
	})
}

func diagnosticSortKey(finding diagnostic.Diagnostic) string {
	source := finding.Source
	if source == nil {
		return "\xff\xff" + string(finding.Code) + "\x00" + finding.Message
	}
	return source.Locator + "\x00" +
		fmt.Sprintf("%010d:%010d:%010d:%010d", source.StartLine, source.StartColumn, source.EndLine, source.EndColumn) + "\x00" +
		strings.Join(source.Path, "\x00") + "\x00" + string(finding.Code) + "\x00" + finding.Message
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func quotedDefinition(ref graph.DefinitionRef) string {
	return strconv.Quote(definitionKey(ref, graph.Graph{}))
}
