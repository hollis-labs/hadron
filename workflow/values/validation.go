package values

import (
	"errors"
	"fmt"
	"mime"
	"sort"
	"strings"
	"unicode/utf8"
)

var (
	// ErrInvalidValue marks a malformed value, artifact, metadata, or value-set
	// reference contract.
	ErrInvalidValue = errors.New("invalid workflow value")
	// ErrAmbiguousEnvelope marks a Value JSON object that has both payloads,
	// neither payload, or a payload inconsistent with its declared type.
	ErrAmbiguousEnvelope = errors.New("ambiguous workflow value envelope")
	// ErrDigestMismatch marks inline content whose computed digest differs from
	// the digest recorded in its Value envelope.
	ErrDigestMismatch = errors.New("workflow value digest mismatch")
)

// Validate reports missing or ambiguous producer metadata.
func (p Producer) Validate() error {
	if err := validateUTF8("producer.kind", p.Kind); err != nil {
		return err
	}
	if strings.TrimSpace(p.Kind) == "" {
		return fmt.Errorf("%w: producer.kind is required", ErrInvalidValue)
	}
	if err := validateUTF8("producer.reference", p.Reference); err != nil {
		return err
	}
	if strings.TrimSpace(p.Reference) == "" {
		return fmt.Errorf("%w: producer.reference is required", ErrInvalidValue)
	}
	if err := validateUTF8("producer.output", p.Output); err != nil {
		return err
	}
	if p.Output != "" && strings.TrimSpace(p.Output) == "" {
		return fmt.Errorf("%w: producer.output must not be whitespace", ErrInvalidValue)
	}
	return nil
}

// Validate reports incomplete or unsupported envelope metadata.
func (m Metadata) Validate() error {
	if err := m.Producer.Validate(); err != nil {
		return err
	}
	if err := validateMediaType(m.MediaType); err != nil {
		return err
	}
	if !m.Redaction.Valid() {
		return fmt.Errorf("%w: unsupported redaction class %q", ErrInvalidValue, m.Redaction)
	}
	if !m.Retention.Valid() {
		return fmt.Errorf("%w: unsupported retention class %q", ErrInvalidValue, m.Retention)
	}
	return nil
}

// Validate reports incomplete or unsupported artifact metadata. It does not
// resolve the URI, access storage, or verify artifact bytes.
func (r ArtifactRef) Validate() error {
	if err := validateUTF8("artifact.store", r.Store); err != nil {
		return err
	}
	if strings.TrimSpace(r.Store) == "" {
		return fmt.Errorf("%w: artifact.store is required", ErrInvalidValue)
	}
	if err := validateUTF8("artifact.uri", r.URI); err != nil {
		return err
	}
	if strings.TrimSpace(r.URI) == "" {
		return fmt.Errorf("%w: artifact.uri is required", ErrInvalidValue)
	}
	if err := ValidateDigest(r.Digest); err != nil {
		return fmt.Errorf("artifact.digest: %w", err)
	}
	if err := (Metadata{
		Producer: r.Producer, MediaType: r.MediaType,
		Redaction: r.Redaction, Retention: r.Retention,
	}).Validate(); err != nil {
		return fmt.Errorf("artifact: %w", err)
	}
	if r.SizeBytes < 0 {
		return fmt.Errorf("%w: artifact.size_bytes must not be negative", ErrInvalidValue)
	}
	return nil
}

// Validate checks the closed enums, exact payload mode, JSON compatibility,
// metadata, and digest consistency of v.
func (v Value) Validate() error {
	if !v.Type.Valid() {
		return fmt.Errorf("%w: unsupported type %q", ErrInvalidValue, v.Type)
	}
	metadata := Metadata{
		Producer: v.Producer, MediaType: v.MediaType,
		Redaction: v.Redaction, Retention: v.Retention,
	}
	if err := metadata.Validate(); err != nil {
		return err
	}
	if err := ValidateDigest(v.Digest); err != nil {
		return fmt.Errorf("value.digest: %w", err)
	}
	if v.Type == TypeSecretRef {
		if v.SecretRef == nil || v.Inline != nil || v.Artifact != nil {
			return fmt.Errorf("%w: secret_ref type requires only secret_ref payload", ErrAmbiguousEnvelope)
		}
		if v.Redaction != RedactionSecret {
			return fmt.Errorf("%w: secret_ref values require secret redaction", ErrInvalidValue)
		}
		if err := v.SecretRef.Validate(); err != nil {
			return err
		}
		digest, err := DigestInline(string(*v.SecretRef))
		if err != nil {
			return err
		}
		if v.Digest != digest {
			return fmt.Errorf("%w: recorded %q, computed %q", ErrDigestMismatch, v.Digest, digest)
		}
		return nil
	}

	if v.Type == TypeArtifact {
		if v.Artifact == nil || v.Inline != nil || v.SecretRef != nil {
			return fmt.Errorf("%w: artifact type requires only artifact payload", ErrAmbiguousEnvelope)
		}
		if err := v.Artifact.Validate(); err != nil {
			return err
		}
		if v.Digest != v.Artifact.Digest || v.MediaType != v.Artifact.MediaType ||
			v.Producer != v.Artifact.Producer || v.Redaction != v.Artifact.Redaction ||
			v.Retention != v.Artifact.Retention {
			return fmt.Errorf("%w: artifact and value metadata diverge", ErrAmbiguousEnvelope)
		}
		return nil
	}

	if v.Artifact != nil || v.SecretRef != nil {
		return fmt.Errorf("%w: inline type must not carry artifact payload", ErrAmbiguousEnvelope)
	}
	if v.Redaction == RedactionSecret {
		return fmt.Errorf("%w: secret-classified inline payloads must use an opaque SecretRef", ErrSecretMaterial)
	}
	normalized, inferred, err := normalizeInline(v.Inline)
	if err != nil {
		return err
	}
	if inferred != v.Type {
		return fmt.Errorf("%w: type %q does not match inline %q", ErrAmbiguousEnvelope, v.Type, inferred)
	}
	digest, err := digestNormalized(normalized)
	if err != nil {
		return err
	}
	if v.Digest != digest {
		return fmt.Errorf("%w: recorded %q, computed %q", ErrDigestMismatch, v.Digest, digest)
	}
	return nil
}

// Validate checks every named value in deterministic name order.
func (s ValueSet) Validate() error {
	if s == nil {
		return fmt.Errorf("%w: value set must be an object, not null", ErrInvalidValue)
	}
	names := make([]string, 0, len(s))
	for name := range s {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := validateUTF8("value-set name", name); err != nil {
			return err
		}
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("%w: value-set name is required", ErrInvalidValue)
		}
		if err := s[name].Validate(); err != nil {
			return fmt.Errorf("value-set[%q]: %w", name, err)
		}
	}
	return nil
}

// Validate reports an incomplete or malformed value-set reference.
func (r ValueSetRef) Validate() error {
	if err := validateUTF8("value-set reference id", r.ID); err != nil {
		return err
	}
	if strings.TrimSpace(r.ID) == "" {
		return fmt.Errorf("%w: value-set reference id is required", ErrInvalidValue)
	}
	if err := ValidateDigest(r.Digest); err != nil {
		return fmt.Errorf("value-set reference digest: %w", err)
	}
	return nil
}

func validateMediaType(value string) error {
	if err := validateUTF8("media_type", value); err != nil {
		return err
	}
	if value == "" || strings.TrimSpace(value) != value {
		return fmt.Errorf("%w: media_type is required without surrounding whitespace", ErrInvalidValue)
	}
	parsed, _, err := mime.ParseMediaType(value)
	if err != nil {
		return fmt.Errorf("%w: invalid media_type %q: %w", ErrInvalidValue, value, err)
	}
	parts := strings.SplitN(parsed, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("%w: media_type %q must contain type and subtype", ErrInvalidValue, value)
	}
	return nil
}

func validateUTF8(path, value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s must contain valid UTF-8", ErrInvalidValue, path)
	}
	return nil
}
