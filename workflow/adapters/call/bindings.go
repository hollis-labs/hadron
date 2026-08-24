package call

import (
	"fmt"
	"sort"
	"strings"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	CodeInputBinding diagnostic.Code = "HADR-CALL-010"
	CodeInputShape   diagnostic.Code = "HADR-CALL-011"
	CodeInputSchema  diagnostic.Code = "HADR-CALL-012"
)

// BindInputsRequest contains the parent-scoped expression data required to
// bind one call. Resolved.InputBindings are import-level partial defaults;
// LocalInputs are the call node's already-evaluated with: bindings.
type BindInputsRequest struct {
	Invocation  stepkind.InvocationIdentity
	Resolved    workflowcompile.ResolvedDefinition
	LocalInputs values.ValueSet
	Context     values.ExpressionContext
	Options     values.ExpressionOptions
}

// BindInputsResult contains either a complete typed child input set or stable
// diagnostics. Inputs is nil whenever Diagnostics is non-empty.
type BindInputsResult struct {
	Inputs      values.ValueSet
	Diagnostics []diagnostic.Diagnostic
}

// BindInputs evaluates child declaration defaults first and resolver/import
// partial bindings second, then overlays the already-evaluated node-local with:
// inputs last. All default expression evaluation uses the caller-supplied
// parent context; partial outputs never become an implicit expression root.
func BindInputs(request BindInputsRequest) BindInputsResult {
	var findings []diagnostic.Diagnostic
	if err := request.Invocation.Validate(); err != nil {
		findings = append(findings, inputDiagnostic(CodeInputShape, nil, fmt.Sprintf("call invocation identity is invalid: %v", err)))
	}
	resolved, err := normalizeResolvedDefinition(request.Resolved)
	if err != nil {
		findings = append(findings, inputDiagnostic(CodeInputShape, nil, fmt.Sprintf("resolved definition is invalid: %v", err)))
		sortInputDiagnostics(findings)
		return BindInputsResult{Diagnostics: findings}
	}
	declarations := make(map[string]graph.InputSpec, len(resolved.Graph.Inputs))
	for _, declaration := range resolved.Graph.Inputs {
		if err := graph.ValidateID(declaration.Name); err != nil {
			findings = append(findings, inputDiagnostic(CodeInputShape, declaration.Source, fmt.Sprintf("child input name %q is invalid", declaration.Name)))
			continue
		}
		if _, duplicate := declarations[declaration.Name]; duplicate {
			findings = append(findings, inputDiagnostic(CodeInputShape, declaration.Source, fmt.Sprintf("child input %q is declared more than once", declaration.Name)))
			continue
		}
		declarations[declaration.Name] = declaration
	}
	if len(findings) != 0 {
		sortInputDiagnostics(findings)
		return BindInputsResult{Diagnostics: findings}
	}

	engine := values.NewExpressionEngine()
	bound := make(values.ValueSet, len(declarations))
	defaultBindings := make(map[string]graph.Binding)
	for _, declaration := range resolved.Graph.Inputs {
		if declaration.Default != nil {
			defaultBindings[declaration.Name] = *declaration.Default
		}
	}
	layers := []struct {
		kind     string
		bindings map[string]graph.Binding
	}{
		{kind: "definition_default", bindings: defaultBindings},
		{kind: "import_default", bindings: resolved.InputBindings},
	}
	for _, layer := range layers {
		for _, name := range sortedBindingNames(layer.bindings) {
			binding := layer.bindings[name]
			if _, declared := declarations[name]; !declared {
				findings = append(findings, inputDiagnostic(CodeInputShape, bindingSource(binding), fmt.Sprintf("%s binding %q is not declared by the child workflow", layer.kind, name)))
				continue
			}
			value, evaluationErr := engine.EvaluateBinding(binding, request.Context, request.Options, inputMetadata(request.Invocation, layer.kind, name))
			if evaluationErr != nil {
				findings = append(findings, inputDiagnostic(CodeInputBinding, bindingSource(binding), fmt.Sprintf("%s binding %q could not be evaluated: %v", layer.kind, name, evaluationErr)))
				continue
			}
			bound[name] = value
		}
	}
	if request.LocalInputs == nil {
		findings = append(findings, inputDiagnostic(CodeInputShape, nil, "node-local call inputs must be an object"))
	} else {
		for _, name := range sortedValueNames(request.LocalInputs) {
			if _, declared := declarations[name]; !declared {
				findings = append(findings, inputDiagnostic(CodeInputShape, nil, fmt.Sprintf("node-local binding %q is not declared by the child workflow", name)))
				continue
			}
			valueSet := values.ValueSet{name: request.LocalInputs[name]}
			if err := values.ValidatePersistableSet(valueSet); err != nil {
				findings = append(findings, inputDiagnostic(CodeInputShape, nil, fmt.Sprintf("node-local binding %q is not persistable: %v", name, err)))
				continue
			}
			bound[name] = cloneValueSet(valueSet)[name]
		}
	}

	names := make([]string, 0, len(declarations))
	for name := range declarations {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		declaration := declarations[name]
		value, present := bound[name]
		if !present {
			if declaration.Required {
				findings = append(findings, inputDiagnostic(CodeInputShape, declaration.Source, fmt.Sprintf("required child input %q has no default or call binding", name)))
			}
			if err := values.ValidateSchema(declaration.Schema); err != nil {
				findings = append(findings, inputDiagnostic(CodeInputSchema, declaration.Source, fmt.Sprintf("child input %q has an invalid schema: %v", name, err)))
			}
			continue
		}
		if err := values.ValidateValueSchema(declaration.Schema, value); err != nil {
			findings = append(findings, inputDiagnostic(CodeInputSchema, declaration.Source, fmt.Sprintf("child input %q does not satisfy its schema: %v", name, err)))
		}
	}
	if len(findings) != 0 {
		sortInputDiagnostics(findings)
		return BindInputsResult{Diagnostics: findings}
	}
	if err := values.ValidatePersistableSet(bound); err != nil {
		return BindInputsResult{Diagnostics: []diagnostic.Diagnostic{inputDiagnostic(CodeInputShape, nil, fmt.Sprintf("bound child inputs are not persistable: %v", err))}}
	}
	return BindInputsResult{Inputs: cloneValueSet(bound)}
}

func inputMetadata(identity stepkind.InvocationIdentity, kind, output string) values.Metadata {
	reference := strings.Join([]string{identity.RunID, identity.NodeID, identity.Iteration}, "/")
	return values.Metadata{
		Producer:  values.Producer{Kind: kind, Reference: reference, Output: output},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	}
}

func bindingSource(binding graph.Binding) *graph.SourceRef {
	if binding.Kind == graph.BindingExpression && binding.Expression != nil && binding.Expression.Source != nil {
		return binding.Expression.Source
	}
	return binding.Source
}

func sortedBindingNames(bindings map[string]graph.Binding) []string {
	names := make([]string, 0, len(bindings))
	for name := range bindings {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedValueNames(input values.ValueSet) []string {
	names := make([]string, 0, len(input))
	for name := range input {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func inputDiagnostic(code diagnostic.Code, source *graph.SourceRef, message string) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError, Code: code, Message: message, Source: source,
		Remediation: &diagnostic.Remediation{Message: "Fix the child input declaration or call binding so it produces one complete typed input set."},
	}
}

func sortInputDiagnostics(findings []diagnostic.Diagnostic) {
	sort.SliceStable(findings, func(i, j int) bool {
		return inputDiagnosticKey(findings[i]) < inputDiagnosticKey(findings[j])
	})
}

func inputDiagnosticKey(finding diagnostic.Diagnostic) string {
	location := ""
	if finding.Source != nil {
		location = fmt.Sprintf("%s\x00%09d\x00%09d", finding.Source.Locator, finding.Source.StartLine, finding.Source.StartColumn)
	}
	return location + "\x00" + string(finding.Code) + "\x00" + finding.Message
}
