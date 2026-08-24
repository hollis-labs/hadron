package values

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
)

const digestPrefix = "sha256:"

// SHA256Digest returns the package's stable digest representation for bytes.
// It does not retain, inspect, or classify the supplied content.
func SHA256Digest(content []byte) string {
	sum := sha256.Sum256(content)
	return digestPrefix + hex.EncodeToString(sum[:])
}

// ValidateDigest reports whether digest is a lower-case SHA-256 digest in the
// stable "sha256:<hex>" representation.
func ValidateDigest(digest string) error {
	if !strings.HasPrefix(digest, digestPrefix) {
		return fmt.Errorf("%w: digest must use sha256 prefix", ErrInvalidValue)
	}
	hexValue := strings.TrimPrefix(digest, digestPrefix)
	if len(hexValue) != sha256.Size*2 || strings.ToLower(hexValue) != hexValue {
		return fmt.Errorf("%w: digest must contain %d lower-case hex characters", ErrInvalidValue, sha256.Size*2)
	}
	if _, err := hex.DecodeString(hexValue); err != nil {
		return fmt.Errorf("%w: digest is not hexadecimal: %w", ErrInvalidValue, err)
	}
	return nil
}

// DigestInline returns a SHA-256 digest of the normalized JSON representation
// of inline. Object key order does not affect the result. Only native JSON
// shapes are accepted; binary slices, structs, pointers, NaN, and infinities
// are rejected.
func DigestInline(inline any) (string, error) {
	normalized, _, err := normalizeInline(inline)
	if err != nil {
		return "", err
	}
	return digestNormalized(normalized)
}

// DigestValueSet returns a stable SHA-256 digest over the complete named value
// envelopes, including classification and producer metadata.
func DigestValueSet(set ValueSet) (string, error) {
	if err := set.Validate(); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(set)
	if err != nil {
		return "", fmt.Errorf("%w: marshal value set: %w", ErrInvalidValue, err)
	}
	return SHA256Digest(encoded), nil
}

// NewInline constructs a validated inline Value, normalizes its native JSON
// shape, infers its Type, and records its deterministic payload digest.
func NewInline(inline any, metadata Metadata) (Value, error) {
	if err := metadata.Validate(); err != nil {
		return Value{}, err
	}
	normalized, valueType, err := normalizeInline(inline)
	if err != nil {
		return Value{}, err
	}
	digest, err := digestNormalized(normalized)
	if err != nil {
		return Value{}, err
	}
	value := Value{
		Type: valueType, Inline: normalized,
		Producer: metadata.Producer, MediaType: metadata.MediaType, Digest: digest,
		Redaction: metadata.Redaction, Retention: metadata.Retention,
	}
	if err := value.Validate(); err != nil {
		return Value{}, err
	}
	return value, nil
}

// NewArtifact constructs a validated artifact Value and mirrors the reference
// metadata into the common Value envelope.
func NewArtifact(ref ArtifactRef) (Value, error) {
	if err := ref.Validate(); err != nil {
		return Value{}, err
	}
	refCopy := ref
	value := Value{
		Type: TypeArtifact, Artifact: &refCopy,
		Producer: ref.Producer, MediaType: ref.MediaType, Digest: ref.Digest,
		Redaction: ref.Redaction, Retention: ref.Retention,
	}
	if err := value.Validate(); err != nil {
		return Value{}, err
	}
	return value, nil
}

// NewSecretRef constructs a validated reference Value. Only the opaque URI and
// its producer/classification metadata enter the data plane; resolved material
// is not accepted by this constructor.
func NewSecretRef(ref SecretRef, metadata Metadata) (Value, error) {
	if err := ref.Validate(); err != nil {
		return Value{}, err
	}
	if err := metadata.Validate(); err != nil {
		return Value{}, err
	}
	if metadata.Redaction != RedactionSecret {
		return Value{}, fmt.Errorf("%w: secret_ref values require secret redaction", ErrInvalidValue)
	}
	digest, err := DigestInline(string(ref))
	if err != nil {
		return Value{}, err
	}
	copyRef := ref
	value := Value{
		Type: TypeSecretRef, SecretRef: &copyRef,
		Producer: metadata.Producer, MediaType: metadata.MediaType, Digest: digest,
		Redaction: metadata.Redaction, Retention: metadata.Retention,
	}
	if err := value.Validate(); err != nil {
		return Value{}, err
	}
	return value, nil
}

// NewValueSetRef constructs a validated opaque reference bound to set's
// deterministic digest.
func NewValueSetRef(id string, set ValueSet) (ValueSetRef, error) {
	digest, err := DigestValueSet(set)
	if err != nil {
		return ValueSetRef{}, err
	}
	ref := ValueSetRef{ID: id, Digest: digest}
	if err := ref.Validate(); err != nil {
		return ValueSetRef{}, err
	}
	return ref, nil
}

func digestNormalized(normalized any) (string, error) {
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return "", fmt.Errorf("%w: marshal normalized inline value: %w", ErrInvalidValue, err)
	}
	return SHA256Digest(encoded), nil
}

func normalizeInline(inline any) (any, Type, error) {
	normalized, err := normalizeJSONValue(reflect.ValueOf(inline), make(map[visit]bool))
	if err != nil {
		return nil, "", err
	}
	valueType, err := inlineType(normalized)
	if err != nil {
		return nil, "", err
	}
	return normalized, valueType, nil
}

type visit struct {
	kind reflect.Kind
	ptr  uintptr
}

func normalizeJSONValue(value reflect.Value, visiting map[visit]bool) (any, error) {
	if !value.IsValid() {
		return nil, nil
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil, nil
		}
		return normalizeJSONValue(value.Elem(), visiting)
	}
	if value.CanInterface() {
		switch typed := value.Interface().(type) {
		case json.Number:
			return normalizeNumber(typed)
		case SecretRef:
			return nil, fmt.Errorf("%w: SecretRef must use NewSecretRef", ErrSecretDerivation)
		case ResolvedSecret, *ResolvedSecret:
			return nil, fmt.Errorf("%w: resolved secret material cannot be normalized inline", ErrSecretMaterial)
		}
	}

	switch value.Kind() {
	case reflect.Bool:
		return value.Bool(), nil
	case reflect.String:
		stringValue := value.String()
		if err := validateUTF8("inline string", stringValue); err != nil {
			return nil, err
		}
		return stringValue, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return json.Number(strconv.FormatInt(value.Int(), 10)), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return json.Number(strconv.FormatUint(value.Uint(), 10)), nil
	case reflect.Float32, reflect.Float64:
		float := value.Float()
		if math.IsNaN(float) || math.IsInf(float, 0) {
			return nil, fmt.Errorf("%w: non-finite numbers are not JSON-compatible", ErrInvalidValue)
		}
		bits := value.Type().Bits()
		return json.Number(strconv.FormatFloat(float, 'g', -1, bits)), nil
	case reflect.Array, reflect.Slice:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return nil, fmt.Errorf("%w: binary byte sequences require an ArtifactRef", ErrInvalidValue)
		}
		if value.Kind() == reflect.Slice {
			if err := enterVisit(value, visiting); err != nil {
				return nil, err
			}
			defer leaveVisit(value, visiting)
		}
		items := make([]any, value.Len())
		for index := 0; index < value.Len(); index++ {
			item, err := normalizeJSONValue(value.Index(index), visiting)
			if err != nil {
				return nil, fmt.Errorf("array[%d]: %w", index, err)
			}
			items[index] = item
		}
		return items, nil
	case reflect.Map:
		if value.Type().Key().Kind() != reflect.String {
			return nil, fmt.Errorf("%w: object keys must be strings", ErrInvalidValue)
		}
		if err := enterVisit(value, visiting); err != nil {
			return nil, err
		}
		defer leaveVisit(value, visiting)
		object := make(map[string]any, value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			key := iterator.Key().String()
			if err := validateUTF8("inline object key", key); err != nil {
				return nil, err
			}
			item, err := normalizeJSONValue(iterator.Value(), visiting)
			if err != nil {
				return nil, fmt.Errorf("object[%q]: %w", key, err)
			}
			object[key] = item
		}
		return object, nil
	case reflect.Pointer:
		return nil, fmt.Errorf("%w: pointers are not native JSON values", ErrInvalidValue)
	default:
		return nil, fmt.Errorf("%w: %s values are not native JSON values", ErrInvalidValue, value.Kind())
	}
}

func normalizeNumber(number json.Number) (json.Number, error) {
	encoded, err := json.Marshal(number)
	if err != nil {
		return "", fmt.Errorf("%w: invalid JSON number %q: %w", ErrInvalidValue, number, err)
	}
	return json.Number(encoded), nil
}

func enterVisit(value reflect.Value, visiting map[visit]bool) error {
	pointer := value.Pointer()
	if pointer == 0 {
		return nil
	}
	key := visit{kind: value.Kind(), ptr: pointer}
	if visiting[key] {
		return fmt.Errorf("%w: cyclic JSON value", ErrInvalidValue)
	}
	visiting[key] = true
	return nil
}

func leaveVisit(value reflect.Value, visiting map[visit]bool) {
	pointer := value.Pointer()
	if pointer != 0 {
		delete(visiting, visit{kind: value.Kind(), ptr: pointer})
	}
}

func inlineType(value any) (Type, error) {
	switch value.(type) {
	case nil:
		return TypeNull, nil
	case string:
		return TypeString, nil
	case json.Number:
		return TypeNumber, nil
	case bool:
		return TypeBoolean, nil
	case []any:
		return TypeArray, nil
	case map[string]any:
		return TypeObject, nil
	default:
		return "", fmt.Errorf("%w: unsupported normalized JSON type %T", ErrInvalidValue, value)
	}
}
