package values

import "fmt"

// PrivateDisplayPolicy controls whether private (never secret) payloads are
// visible to a renderer. Its zero value is fail-closed masking.
type PrivateDisplayPolicy string

const (
	PrivateDisplayMask   PrivateDisplayPolicy = "mask"
	PrivateDisplayReveal PrivateDisplayPolicy = "reveal"
)

// Valid reports whether p is a supported policy. Empty is the default mask.
func (p PrivateDisplayPolicy) Valid() bool {
	return p == "" || p == PrivateDisplayMask || p == PrivateDisplayReveal
}

// DisplayPolicy is shared by value and event renderers.
type DisplayPolicy struct {
	Private PrivateDisplayPolicy `json:"private,omitempty"`
}

// Validate rejects policy values other than the closed mask/reveal vocabulary.
func (p DisplayPolicy) Validate() error {
	if !p.Private.Valid() {
		return fmt.Errorf("unsupported private display policy %q", p.Private)
	}
	return nil
}

// RevealsPrivate reports the explicit private-payload decision.
func (p DisplayPolicy) RevealsPrivate() bool { return p.Private == PrivateDisplayReveal }

// RenderedValue is the stable display projection of a Value. Payload contains
// an inline value, ArtifactRef, or RedactedMarker and never an unmasked secret.
type RenderedValue struct {
	Type      Type           `json:"type"`
	Payload   any            `json:"payload"`
	Producer  Producer       `json:"producer"`
	MediaType string         `json:"media_type"`
	Digest    string         `json:"digest"`
	Redaction RedactionClass `json:"redaction"`
	Retention RetentionClass `json:"retention"`
	Masked    bool           `json:"masked"`
}

// RenderedValueSet is a named display projection.
type RenderedValueSet map[string]RenderedValue

// RenderValue applies secret and private display policy. Secret values are
// always masked, including secret ArtifactRef URI metadata and SecretRef URIs.
func RenderValue(value Value, policy DisplayPolicy) (RenderedValue, error) {
	if err := policy.Validate(); err != nil {
		return RenderedValue{}, err
	}
	if err := value.Validate(); err != nil {
		return RenderedValue{}, err
	}
	rendered := RenderedValue{
		Type: value.Type, Producer: value.Producer, MediaType: value.MediaType,
		Digest: value.Digest, Redaction: value.Redaction, Retention: value.Retention,
	}
	if value.Redaction == RedactionSecret ||
		(value.Redaction == RedactionPrivate && !policy.RevealsPrivate()) {
		rendered.Payload = RedactedMarker
		rendered.Masked = true
		return rendered, nil
	}
	switch value.Type {
	case TypeArtifact:
		artifact := *value.Artifact
		rendered.Payload = artifact
	case TypeSecretRef:
		// SecretRef validation requires secret classification, so this branch is
		// unreachable for a valid value and remains fail-closed.
		rendered.Payload = RedactedMarker
		rendered.Masked = true
	default:
		rendered.Payload = cloneInlineForRender(value.Inline)
	}
	return rendered, nil
}

// RenderValueSet renders every value under one shared display policy.
func RenderValueSet(set ValueSet, policy DisplayPolicy) (RenderedValueSet, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	if err := set.Validate(); err != nil {
		return nil, err
	}
	result := make(RenderedValueSet, len(set))
	for name, value := range set {
		rendered, err := RenderValue(value, policy)
		if err != nil {
			return nil, fmt.Errorf("render value-set[%q]: %w", name, err)
		}
		result[name] = rendered
	}
	return result, nil
}

func cloneInlineForRender(inline any) any {
	// Valid Values already hold recursively normalized JSON data. Copy mutable
	// containers so display callers cannot mutate the data-plane envelope.
	switch typed := inline.(type) {
	case []any:
		copyValue := make([]any, len(typed))
		for index, item := range typed {
			copyValue[index] = cloneInlineForRender(item)
		}
		return copyValue
	case map[string]any:
		copyValue := make(map[string]any, len(typed))
		for key, item := range typed {
			copyValue[key] = cloneInlineForRender(item)
		}
		return copyValue
	default:
		return typed
	}
}
