package runtime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

func resolveDynamicCallSpec(spec *graph.CallSpec, inputs values.ValueSet) (*graph.CallSpec, error) {
	if spec == nil || spec.DefinitionInput == "" {
		return spec, nil
	}
	if !zeroRuntimeDefinition(spec.Definition) {
		return nil, fmt.Errorf("dynamic call definition conflicts with static definition")
	}
	value, exists := inputs[spec.DefinitionInput]
	if !exists {
		return nil, fmt.Errorf("dynamic call definition input %q is missing", spec.DefinitionInput)
	}
	if err := value.Validate(); err != nil {
		return nil, fmt.Errorf("dynamic call definition input is invalid: %w", err)
	}
	if value.Type != values.TypeObject || value.Artifact != nil || value.SecretRef != nil || value.Redaction == values.RedactionSecret || value.Retention == values.RetentionNone {
		return nil, fmt.Errorf("dynamic call definition input must be a persistable non-secret inline object")
	}
	encoded, err := json.Marshal(value.Inline)
	if err != nil {
		return nil, fmt.Errorf("encode dynamic call definition: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	var ref graph.DefinitionRef
	if err := decoder.Decode(&ref); err != nil {
		return nil, fmt.Errorf("decode dynamic call definition: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("dynamic call definition contains trailing data")
	}
	if err := validateExactDynamicDefinition(ref); err != nil {
		return nil, err
	}
	cloned := *spec
	cloned.Definition = ref
	cloned.DefinitionInput = ""
	return &cloned, nil
}

func validateExactDynamicDefinition(ref graph.DefinitionRef) error {
	for _, field := range []struct{ name, value string }{
		{"authority", ref.Authority}, {"kind", ref.Kind}, {"id", ref.ID}, {"version", ref.Version}, {"digest", ref.Digest},
	} {
		if strings.TrimSpace(field.value) == "" || field.value != strings.TrimSpace(field.value) {
			return fmt.Errorf("dynamic call definition %s is required as canonical text", field.name)
		}
	}
	if ref.Kind != "workflow" {
		return fmt.Errorf("dynamic call definition kind must be workflow")
	}
	if err := graph.ValidateID(ref.ID); err != nil {
		return fmt.Errorf("dynamic call definition id: %w", err)
	}
	if err := values.ValidateDigest(ref.Digest); err != nil {
		return fmt.Errorf("dynamic call definition digest: %w", err)
	}
	if ref.Provenance == nil || ref.Provenance.Authority != ref.Authority || ref.Provenance.Digest != ref.Digest {
		return fmt.Errorf("dynamic call definition provenance must match authority and digest")
	}
	return nil
}

func zeroRuntimeDefinition(ref graph.DefinitionRef) bool {
	return ref.Authority == "" && ref.Kind == "" && ref.ID == "" && ref.Locator == "" && ref.Version == "" && ref.Digest == "" && ref.Provenance == nil
}
