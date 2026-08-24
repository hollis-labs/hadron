package diagnostic

import (
	"fmt"

	"github.com/hollis-labs/hadron/workflow/graph"
)

const (
	// CodeUnresolvedReference identifies a value reference that is not visible
	// through the graph's declared dependencies.
	CodeUnresolvedReference Code = "HADR-REF-001"
	// CodeUnsafeEffectRetry identifies an effect/retry combination without an
	// idempotency proof.
	CodeUnsafeEffectRetry Code = "HADR-EFFECT-001"
	// CodeArchivedOutputShim identifies legacy output capture from log text.
	CodeArchivedOutputShim Code = "HADR-OUTPUT-002"
)

// UnresolvedReference reports a missing or dependency-invisible node output.
func UnresolvedReference(source graph.SourceRef, nodeID, reference, dependency string) Diagnostic {
	return Diagnostic{
		Severity: SeverityError,
		Code:     CodeUnresolvedReference,
		Message:  fmt.Sprintf("node %s references %s without a declared dependency", nodeID, reference),
		Source:   &source,
		Remediation: &Remediation{
			Message:         fmt.Sprintf("Declare %s as a dependency before using its outputs.", dependency),
			SuggestedSyntax: fmt.Sprintf("needs: [%s]", dependency),
		},
	}
}

// UnsafeEffectRetry reports a retry policy that lacks an idempotency proof for
// an effectful node.
func UnsafeEffectRetry(source graph.SourceRef, nodeID string) Diagnostic {
	return Diagnostic{
		Severity: SeverityError,
		Code:     CodeUnsafeEffectRetry,
		Message:  fmt.Sprintf("destructive node %s has a retry policy without idempotency proof", nodeID),
		Source:   &source,
		Remediation: &Remediation{
			Message:         "Remove the retry policy or declare a valid idempotency strategy.",
			SuggestedSyntax: "idempotency:\n  mode: keyed\n  key: <stable-expression>",
		},
	}
}

// ArchivedOutputShim reports legacy output extraction from an archived stage's
// log stream.
func ArchivedOutputShim(source graph.SourceRef, stageName string) Diagnostic {
	return Diagnostic{
		Severity: SeverityWarning,
		Code:     CodeArchivedOutputShim,
		Message:  fmt.Sprintf("stage %s captures ::set-output from the log stream", stageName),
		Source:   &source,
		Remediation: &Remediation{
			Message:         "Replace log scraping with a declared typed node output.",
			SuggestedSyntax: "outputs:\n  - name: <output-name>\n    schema: <json-schema>",
		},
	}
}
