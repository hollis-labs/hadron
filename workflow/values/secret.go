package values

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"
)

var (
	// ErrInvalidSecretRef marks a malformed opaque secret reference.
	ErrInvalidSecretRef = errors.New("invalid workflow secret reference")
	// ErrSecretMaterial marks secret material presented to a persistable Value
	// envelope or a value-expression operation that would unwrap it.
	ErrSecretMaterial = errors.New("workflow secret material is not persistable")
	// ErrSecretDerivation marks expression or interpolation use that would
	// unwrap, transform, or otherwise downgrade an opaque secret reference.
	ErrSecretDerivation = errors.New("workflow secret reference cannot be used in a computed expression")
)

var secretAuthorityPattern = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$`)

// SecretRef is a canonical, opaque reference resolved only by a host adapter.
// Its URI form is secret://authority/path with an optional #field fragment.
// The core validates structure but assigns no meaning to authority, path, or
// field names.
type SecretRef string

// ParseSecretRef validates and returns an opaque secret reference.
func ParseSecretRef(raw string) (SecretRef, error) {
	if !utf8.ValidString(raw) || strings.TrimSpace(raw) != raw || raw == "" {
		return "", fmt.Errorf("%w: reference must be non-empty UTF-8 without surrounding whitespace", ErrInvalidSecretRef)
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("%w: parse URI: %w", ErrInvalidSecretRef, err)
	}
	if parsed.Scheme != "secret" || parsed.Opaque != "" {
		return "", fmt.Errorf("%w: scheme must be secret", ErrInvalidSecretRef)
	}
	if parsed.User != nil || parsed.Port() != "" || parsed.Hostname() != parsed.Host ||
		parsed.Host != strings.ToLower(parsed.Host) || !secretAuthorityPattern.MatchString(parsed.Host) {
		return "", fmt.Errorf("%w: authority must be a static name without user info or port", ErrInvalidSecretRef)
	}
	if parsed.RawQuery != "" || parsed.ForceQuery {
		return "", fmt.Errorf("%w: query parameters are not allowed", ErrInvalidSecretRef)
	}
	if parsed.Path == "" || !strings.HasPrefix(parsed.Path, "/") || strings.Contains(parsed.Path, "\\") {
		return "", fmt.Errorf("%w: path must be absolute and non-empty", ErrInvalidSecretRef)
	}
	if parsed.RawPath != "" && parsed.EscapedPath() != parsed.RawPath {
		return "", fmt.Errorf("%w: path escape is not canonical", ErrInvalidSecretRef)
	}
	for _, segment := range strings.Split(strings.TrimPrefix(parsed.EscapedPath(), "/"), "/") {
		decoded, decodeErr := url.PathUnescape(segment)
		if decodeErr != nil || decoded == "" || decoded == "." || decoded == ".." ||
			strings.ContainsAny(decoded, "/\\\x00\r\n\t") || url.PathEscape(decoded) != segment {
			return "", fmt.Errorf("%w: path contains an empty, traversal, escaped separator, or control segment", ErrInvalidSecretRef)
		}
	}
	if strings.ContainsAny(parsed.Fragment, "/\\\x00\r\n\t") || (strings.HasSuffix(raw, "#") && parsed.Fragment == "") {
		return "", fmt.Errorf("%w: field fragment must be non-empty and static when present", ErrInvalidSecretRef)
	}
	if parsed.Fragment != "" {
		decoded, decodeErr := url.PathUnescape(parsed.EscapedFragment())
		if decodeErr != nil || decoded != parsed.Fragment || decoded == "." || decoded == ".." ||
			url.PathEscape(decoded) != parsed.EscapedFragment() {
			return "", fmt.Errorf("%w: field fragment must use canonical unescaped text", ErrInvalidSecretRef)
		}
	}
	if parsed.String() != raw {
		return "", fmt.Errorf("%w: reference must use canonical URI encoding", ErrInvalidSecretRef)
	}
	return SecretRef(raw), nil
}

// Validate reports whether r is a canonical secret reference.
func (r SecretRef) Validate() error {
	_, err := ParseSecretRef(string(r))
	return err
}

// Authority returns the structurally validated reference authority.
func (r SecretRef) Authority() string {
	parsed, _ := url.Parse(string(r))
	return parsed.Host
}

// Path returns the structurally validated escaped reference path.
func (r SecretRef) Path() string {
	parsed, _ := url.Parse(string(r))
	return parsed.EscapedPath()
}

// Field returns the optional structurally validated field fragment.
func (r SecretRef) Field() string {
	parsed, _ := url.Parse(string(r))
	return parsed.Fragment
}

// MarshalJSON validates the reference before encoding its canonical string.
func (r SecretRef) MarshalJSON() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(string(r))
}

// UnmarshalJSON validates a canonical reference string.
func (r *SecretRef) UnmarshalJSON(data []byte) error {
	if r == nil {
		return fmt.Errorf("%w: cannot unmarshal into nil SecretRef", ErrInvalidSecretRef)
	}
	var raw string
	if err := decodeStrict(data, &raw); err != nil {
		return fmt.Errorf("%w: decode JSON: %w", ErrInvalidSecretRef, err)
	}
	parsed, err := ParseSecretRef(raw)
	if err != nil {
		return err
	}
	*r = parsed
	return nil
}

// SecretResolver is the narrow host-adapter seam for resolving references.
// Implementations must not cache or persist returned material in workflow
// state. Callers should Forget the result after injection and masking setup.
type SecretResolver interface {
	ResolveSecret(context.Context, SecretRef) (*ResolvedSecret, error)
}

// SecretResolverFunc adapts a function to SecretResolver.
type SecretResolverFunc func(context.Context, SecretRef) (*ResolvedSecret, error)

// ResolveSecret calls f.
func (f SecretResolverFunc) ResolveSecret(ctx context.Context, ref SecretRef) (*ResolvedSecret, error) {
	return f(ctx, ref)
}

// ResolvedSecret holds short-lived adapter-boundary material. Its bytes are
// intentionally unexported, copied on ingress and access, rejected by JSON,
// and never represented by Value.
type ResolvedSecret struct {
	reference SecretRef
	material  []byte
}

// NewResolvedSecret constructs ephemeral resolved material for a reference.
func NewResolvedSecret(ref SecretRef, material []byte) (*ResolvedSecret, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	if len(material) == 0 {
		return nil, fmt.Errorf("%w: resolved material must not be empty", ErrSecretMaterial)
	}
	return &ResolvedSecret{reference: ref, material: append([]byte(nil), material...)}, nil
}

// Reference returns the non-secret provenance reference.
func (s *ResolvedSecret) Reference() SecretRef {
	if s == nil {
		return ""
	}
	return s.reference
}

// Bytes returns a defensive copy for immediate adapter injection.
func (s *ResolvedSecret) Bytes() []byte {
	if s == nil {
		return nil
	}
	return append([]byte(nil), s.material...)
}

// Forget overwrites and releases the held material. It is safe to call more
// than once. Previously returned defensive copies remain caller-owned.
func (s *ResolvedSecret) Forget() {
	if s == nil {
		return
	}
	for index := range s.material {
		s.material[index] = 0
	}
	s.material = nil
}

// String never reveals material or the underlying reference.
func (*ResolvedSecret) String() string { return RedactedMarker }

// GoString never reveals material or the underlying reference.
func (*ResolvedSecret) GoString() string { return RedactedMarker }

// MarshalJSON prevents resolved material from entering persisted JSON.
func (*ResolvedSecret) MarshalJSON() ([]byte, error) {
	return nil, fmt.Errorf("%w: resolved secret material cannot be marshaled", ErrSecretMaterial)
}
