package values

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"unicode/utf8"
)

var valueCommonFields = []string{
	"type", "producer", "media_type", "digest", "redaction", "retention",
}

// MarshalJSON emits the stable Value envelope and preserves inline null as an
// explicitly present "inline": null payload.
func (v Value) MarshalJSON() ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	if v.Type == TypeSecretRef {
		wire := struct {
			Type      Type           `json:"type"`
			SecretRef *SecretRef     `json:"secret_ref"`
			Producer  Producer       `json:"producer"`
			MediaType string         `json:"media_type"`
			Digest    string         `json:"digest"`
			Redaction RedactionClass `json:"redaction"`
			Retention RetentionClass `json:"retention"`
		}{
			Type: v.Type, SecretRef: v.SecretRef, Producer: v.Producer,
			MediaType: v.MediaType, Digest: v.Digest,
			Redaction: v.Redaction, Retention: v.Retention,
		}
		return json.Marshal(wire)
	}
	if v.Type == TypeArtifact {
		wire := struct {
			Type      Type           `json:"type"`
			Artifact  *ArtifactRef   `json:"artifact"`
			Producer  Producer       `json:"producer"`
			MediaType string         `json:"media_type"`
			Digest    string         `json:"digest"`
			Redaction RedactionClass `json:"redaction"`
			Retention RetentionClass `json:"retention"`
		}{
			Type: v.Type, Artifact: v.Artifact, Producer: v.Producer,
			MediaType: v.MediaType, Digest: v.Digest,
			Redaction: v.Redaction, Retention: v.Retention,
		}
		return json.Marshal(wire)
	}

	normalized, _, err := normalizeInline(v.Inline)
	if err != nil {
		return nil, err
	}
	wire := struct {
		Type      Type           `json:"type"`
		Inline    any            `json:"inline"`
		Producer  Producer       `json:"producer"`
		MediaType string         `json:"media_type"`
		Digest    string         `json:"digest"`
		Redaction RedactionClass `json:"redaction"`
		Retention RetentionClass `json:"retention"`
	}{
		Type: v.Type, Inline: normalized, Producer: v.Producer,
		MediaType: v.MediaType, Digest: v.Digest,
		Redaction: v.Redaction, Retention: v.Retention,
	}
	return json.Marshal(wire)
}

// UnmarshalJSON decodes and validates a strict, unambiguous Value envelope.
func (v *Value) UnmarshalJSON(data []byte) error {
	if v == nil {
		return fmt.Errorf("%w: cannot unmarshal into nil Value", ErrInvalidValue)
	}
	fields, err := decodeObjectFields(data)
	if err != nil {
		return err
	}
	if err := requireFields(fields, valueCommonFields...); err != nil {
		return err
	}
	allowedFields := append(append([]string(nil), valueCommonFields...), "inline", "artifact", "secret_ref")
	if err := rejectUnknownFields(fields, allowedFields...); err != nil {
		return err
	}

	var decoded Value
	if err := decodeField(fields, "type", &decoded.Type); err != nil {
		return err
	}
	if err := decodeField(fields, "producer", &decoded.Producer); err != nil {
		return err
	}
	if err := decodeField(fields, "media_type", &decoded.MediaType); err != nil {
		return err
	}
	if err := decodeField(fields, "digest", &decoded.Digest); err != nil {
		return err
	}
	if err := decodeField(fields, "redaction", &decoded.Redaction); err != nil {
		return err
	}
	if err := decodeField(fields, "retention", &decoded.Retention); err != nil {
		return err
	}

	_, hasInline := fields["inline"]
	_, hasArtifact := fields["artifact"]
	_, hasSecretRef := fields["secret_ref"]
	if countTrue(hasInline, hasArtifact, hasSecretRef) != 1 {
		return fmt.Errorf("%w: exactly one of inline, artifact, or secret_ref is required", ErrAmbiguousEnvelope)
	}
	switch decoded.Type {
	case TypeArtifact:
		if !hasArtifact {
			return fmt.Errorf("%w: artifact type requires artifact payload", ErrAmbiguousEnvelope)
		}
		var artifact ArtifactRef
		if err := decodeField(fields, "artifact", &artifact); err != nil {
			return err
		}
		decoded.Artifact = &artifact
	case TypeSecretRef:
		if !hasSecretRef {
			return fmt.Errorf("%w: secret_ref type requires secret_ref payload", ErrAmbiguousEnvelope)
		}
		var ref SecretRef
		if err := decodeField(fields, "secret_ref", &ref); err != nil {
			return err
		}
		decoded.SecretRef = &ref
	default:
		if !hasInline {
			return fmt.Errorf("%w: inline type requires inline payload", ErrAmbiguousEnvelope)
		}
		inline, err := decodeInline(fields["inline"])
		if err != nil {
			return fmt.Errorf("inline: %w", err)
		}
		decoded.Inline = inline
	}

	if err := decoded.Validate(); err != nil {
		return err
	}
	*v = decoded
	return nil
}

func countTrue(values ...bool) int {
	count := 0
	for _, value := range values {
		if value {
			count++
		}
	}
	return count
}

// MarshalJSON validates standalone producer metadata before encoding it.
func (p Producer) MarshalJSON() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	type wire Producer
	return json.Marshal(wire(p))
}

// UnmarshalJSON rejects duplicate and unknown producer fields.
func (p *Producer) UnmarshalJSON(data []byte) error {
	if p == nil {
		return fmt.Errorf("%w: cannot unmarshal into nil Producer", ErrInvalidValue)
	}
	if _, err := decodeObjectFields(data); err != nil {
		return err
	}
	type wire Producer
	var decoded wire
	if err := decodeStrict(data, &decoded); err != nil {
		return err
	}
	producer := Producer(decoded)
	if err := producer.Validate(); err != nil {
		return err
	}
	*p = producer
	return nil
}

// MarshalJSON validates an ArtifactRef before encoding it.
func (r ArtifactRef) MarshalJSON() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	type wire ArtifactRef
	return json.Marshal(wire(r))
}

// UnmarshalJSON rejects duplicate and unknown artifact fields and validates
// the decoded reference without resolving it.
func (r *ArtifactRef) UnmarshalJSON(data []byte) error {
	if r == nil {
		return fmt.Errorf("%w: cannot unmarshal into nil ArtifactRef", ErrInvalidValue)
	}
	if _, err := decodeObjectFields(data); err != nil {
		return err
	}
	type wire ArtifactRef
	var decoded wire
	if err := decodeStrict(data, &decoded); err != nil {
		return err
	}
	ref := ArtifactRef(decoded)
	if err := ref.Validate(); err != nil {
		return err
	}
	*r = ref
	return nil
}

// MarshalJSON validates every Value before encoding a ValueSet.
func (s ValueSet) MarshalJSON() ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	type wire ValueSet
	return json.Marshal(wire(s))
}

// UnmarshalJSON rejects duplicate value names and validates the complete set.
func (s *ValueSet) UnmarshalJSON(data []byte) error {
	if s == nil {
		return fmt.Errorf("%w: cannot unmarshal into nil ValueSet", ErrInvalidValue)
	}
	fields, err := decodeObjectFields(data)
	if err != nil {
		return err
	}
	decoded := make(ValueSet, len(fields))
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		var value Value
		if err := json.Unmarshal(fields[name], &value); err != nil {
			return fmt.Errorf("value-set[%q]: %w", name, err)
		}
		decoded[name] = value
	}
	if err := decoded.Validate(); err != nil {
		return err
	}
	*s = decoded
	return nil
}

// MarshalJSON validates a ValueSetRef before encoding it.
func (r ValueSetRef) MarshalJSON() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	type wire ValueSetRef
	return json.Marshal(wire(r))
}

// UnmarshalJSON rejects duplicate and unknown reference fields.
func (r *ValueSetRef) UnmarshalJSON(data []byte) error {
	if r == nil {
		return fmt.Errorf("%w: cannot unmarshal into nil ValueSetRef", ErrInvalidValue)
	}
	if _, err := decodeObjectFields(data); err != nil {
		return err
	}
	type wire ValueSetRef
	var decoded wire
	if err := decodeStrict(data, &decoded); err != nil {
		return err
	}
	ref := ValueSetRef(decoded)
	if err := ref.Validate(); err != nil {
		return err
	}
	*r = ref
	return nil
}

func decodeObjectFields(data []byte) (map[string]json.RawMessage, error) {
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("%w: JSON input must contain valid UTF-8", ErrInvalidValue)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("%w: decode JSON object: %w", ErrInvalidValue, err)
	}
	delimiter, ok := token.(json.Delim)
	if !ok || delimiter != '{' {
		return nil, fmt.Errorf("%w: expected JSON object", ErrInvalidValue)
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("%w: decode JSON field: %w", ErrInvalidValue, err)
		}
		name, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("%w: JSON object field is not a string", ErrInvalidValue)
		}
		if _, duplicate := fields[name]; duplicate {
			return nil, fmt.Errorf("%w: duplicate field %q", ErrAmbiguousEnvelope, name)
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return nil, fmt.Errorf("%w: decode field %q: %w", ErrInvalidValue, name, err)
		}
		fields[name] = raw
	}
	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("%w: close JSON object: %w", ErrInvalidValue, err)
	}
	if err := requireEOF(decoder); err != nil {
		return nil, err
	}
	return fields, nil
}

func decodeInline(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	value, err := decodeJSONTokenValue(decoder)
	if err != nil {
		return nil, err
	}
	if err := requireEOF(decoder); err != nil {
		return nil, err
	}
	return value, nil
}

func decodeJSONTokenValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("%w: decode inline JSON: %w", ErrInvalidValue, err)
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		switch value := token.(type) {
		case nil, bool, string, json.Number:
			return value, nil
		default:
			return nil, fmt.Errorf("%w: unsupported JSON token %T", ErrInvalidValue, token)
		}
	}

	switch delimiter {
	case '[':
		items := make([]any, 0)
		for decoder.More() {
			item, err := decodeJSONTokenValue(decoder)
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		if token, err := decoder.Token(); err != nil || token != json.Delim(']') {
			return nil, fmt.Errorf("%w: malformed JSON array", ErrInvalidValue)
		}
		return items, nil
	case '{':
		object := make(map[string]any)
		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return nil, fmt.Errorf("%w: decode object key: %w", ErrInvalidValue, err)
			}
			name, ok := token.(string)
			if !ok {
				return nil, fmt.Errorf("%w: object key is not a string", ErrInvalidValue)
			}
			if _, duplicate := object[name]; duplicate {
				return nil, fmt.Errorf("%w: duplicate inline object key %q", ErrAmbiguousEnvelope, name)
			}
			item, err := decodeJSONTokenValue(decoder)
			if err != nil {
				return nil, err
			}
			object[name] = item
		}
		if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
			return nil, fmt.Errorf("%w: malformed JSON object", ErrInvalidValue)
		}
		return object, nil
	default:
		return nil, fmt.Errorf("%w: unexpected JSON delimiter %q", ErrInvalidValue, delimiter)
	}
}

func decodeField(fields map[string]json.RawMessage, name string, target any) error {
	if err := json.Unmarshal(fields[name], target); err != nil {
		return fmt.Errorf("%w: decode field %q: %w", ErrInvalidValue, name, err)
	}
	return nil
}

func decodeStrict(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidValue, err)
	}
	return requireEOF(decoder)
}

func requireEOF(decoder *json.Decoder) error {
	if token, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return fmt.Errorf("%w: trailing JSON: %w", ErrInvalidValue, err)
		}
		return fmt.Errorf("%w: trailing JSON token %v", ErrInvalidValue, token)
	}
	return nil
}

func requireFields(fields map[string]json.RawMessage, names ...string) error {
	for _, name := range names {
		if _, ok := fields[name]; !ok {
			return fmt.Errorf("%w: required field %q is missing", ErrInvalidValue, name)
		}
	}
	return nil
}

func rejectUnknownFields(fields map[string]json.RawMessage, allowed ...string) error {
	allowedSet := make(map[string]bool, len(allowed))
	for _, name := range allowed {
		allowedSet[name] = true
	}
	unknown := make([]string, 0)
	for name := range fields {
		if !allowedSet[name] {
			unknown = append(unknown, name)
		}
	}
	sort.Strings(unknown)
	if len(unknown) > 0 {
		return fmt.Errorf("%w: unknown fields %v", ErrInvalidValue, unknown)
	}
	return nil
}
