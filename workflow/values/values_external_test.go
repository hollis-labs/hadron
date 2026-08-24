package values_test

import (
	"testing"

	"github.com/hollis-labs/hadron/workflow/values"
)

func TestPublicValueContractsAreUsableOutsidePackage(t *testing.T) {
	t.Parallel()

	value, err := values.NewInline(
		map[string]any{"status": "ready"},
		values.Metadata{
			Producer:  values.Producer{Kind: "workflow_input", Reference: "request-1", Output: "payload"},
			MediaType: "application/json",
			Redaction: values.RedactionPrivate,
			Retention: values.RetentionRun,
		},
	)
	if err != nil {
		t.Fatalf("NewInline failed: %v", err)
	}
	set := values.ValueSet{"payload": value}
	ref, err := values.NewValueSetRef("input-values-1", set)
	if err != nil {
		t.Fatalf("NewValueSetRef failed: %v", err)
	}
	if validationErr := ref.Validate(); validationErr != nil {
		t.Fatalf("ValueSetRef.Validate failed: %v", validationErr)
	}
}
