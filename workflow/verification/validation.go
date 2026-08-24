package verification

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

// ValidateSpec resolves and validates every check in source order. Unknown
// verifier kinds and unsupported extension semantics fail closed. Returned
// diagnostics are defensively copied and sorted by source location.
func ValidateSpec(ctx context.Context, registry Registry, spec *graph.VerificationSpec) []diagnostic.Diagnostic {
	if spec == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var findings []diagnostic.Diagnostic
	if len(spec.Checks) == 0 {
		findings = append(findings, verificationDiagnostic(CodeInvalidCheck, spec.Extension.Source, "verification requires at least one check", "Declare one or more registered verification checks."))
	}
	if spec.Extension.Version != "" || len(spec.Extension.Config) != 0 || len(spec.Extension.Metadata) != 0 {
		findings = append(findings, verificationDiagnostic(CodeInvalidCheck, spec.Extension.Source, "verification extension semantics are not registered", "Remove the unsupported extension or bind it through a registered verifier check."))
	}
	for index, check := range spec.Checks {
		if err := validateText("verification check kind", check.Kind, true); err != nil {
			findings = append(findings, verificationDiagnostic(CodeInvalidCheck, check.Source, fmt.Sprintf("verification check[%d] kind is invalid", index), "Use a non-empty registered verifier kind."))
			continue
		}
		verifier, verifierSpec, err := Resolve(registry, check.Kind)
		if err != nil {
			findings = append(findings, verificationDiagnostic(CodeUnknownCheck, check.Source, fmt.Sprintf("verification check %q is not registered", check.Kind), fmt.Sprintf("Register verifier %q or update the check kind.", check.Kind)))
			continue
		}
		cloned, err := CloneCheck(check)
		if err != nil {
			findings = append(findings, verificationDiagnostic(CodeInvalidCheck, check.Source, fmt.Sprintf("verification check %q config is not JSON-compatible", check.Kind), "Use JSON-compatible verifier configuration."))
			continue
		}
		config := cloned.Config
		if config == nil {
			config = graph.Config{}
		}
		configValue, valueErr := values.NewInline(config, values.Metadata{
			Producer:  values.Producer{Kind: "workflow-verification", Reference: check.Kind, Output: "config"},
			MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionNone,
		})
		if valueErr != nil || values.ValidateValueSchema(verifierSpec.ConfigSchema, configValue) != nil {
			findings = append(findings, verificationDiagnostic(CodeInvalidCheck, check.Source, fmt.Sprintf("verification check %q config does not satisfy its registered schema", check.Kind), "Update the verification config to satisfy the registered verifier schema."))
			continue
		}
		for _, finding := range verifier.ValidateConfig(ctx, cloned) {
			findings = append(findings, normalizeDiagnostic(finding, check.Source, check.Kind))
		}
	}
	sort.SliceStable(findings, func(i, j int) bool { return diagnosticKey(findings[i]) < diagnosticKey(findings[j]) })
	return findings
}

func verificationDiagnostic(code diagnostic.Code, source *graph.SourceRef, message, remediation string) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError, Code: code, Message: message, Source: cloneSource(source),
		Remediation: &diagnostic.Remediation{Message: remediation},
	}
}

func normalizeDiagnostic(finding diagnostic.Diagnostic, source *graph.SourceRef, kind string) diagnostic.Diagnostic {
	if !finding.Severity.Valid() {
		finding.Severity = diagnostic.SeverityError
	}
	if finding.Code.Validate() != nil {
		finding.Code = CodeInvalidCheck
	}
	if strings.TrimSpace(finding.Message) == "" {
		finding.Message = fmt.Sprintf("verification check %q is invalid", kind)
	}
	if finding.Source == nil {
		finding.Source = cloneSource(source)
	} else {
		finding.Source = cloneSource(finding.Source)
	}
	if finding.Remediation == nil || strings.TrimSpace(finding.Remediation.Message) == "" {
		finding.Remediation = &diagnostic.Remediation{Message: "Update the verification check to satisfy its registered verifier contract."}
	} else {
		copyRemediation := *finding.Remediation
		finding.Remediation = &copyRemediation
	}
	return finding
}

func diagnosticKey(finding diagnostic.Diagnostic) string {
	if finding.Source == nil {
		return "\xff" + string(finding.Code) + finding.Message
	}
	return finding.Source.Locator + fmt.Sprintf("\x00%010d:%010d:%010d:%010d\x00", finding.Source.StartLine, finding.Source.StartColumn, finding.Source.EndLine, finding.Source.EndColumn) + strings.Join(finding.Source.Path, "\x00") + "\x00" + string(finding.Code) + finding.Message
}
