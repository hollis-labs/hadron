package values

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestClosedEnums(t *testing.T) {
	t.Parallel()

	for _, valueType := range []Type{
		TypeNull, TypeString, TypeNumber, TypeBoolean, TypeArray, TypeObject, TypeArtifact, TypeSecretRef,
	} {
		if !valueType.Valid() {
			t.Errorf("declared value type %q is invalid", valueType)
		}
	}
	if Type("binary").Valid() {
		t.Fatal("unknown value type is valid")
	}
	for _, class := range []RedactionClass{RedactionPublic, RedactionPrivate, RedactionSecret} {
		if !class.Valid() {
			t.Errorf("declared redaction class %q is invalid", class)
		}
	}
	if RedactionClass("masked").Valid() {
		t.Fatal("unknown redaction class is valid")
	}
	for _, class := range []RetentionClass{RetentionNone, RetentionRun, RetentionProject, RetentionExternal} {
		if !class.Valid() {
			t.Errorf("declared retention class %q is invalid", class)
		}
	}
	if RetentionClass("forever").Valid() {
		t.Fatal("unknown retention class is valid")
	}
}

func TestInlineValueJSONRoundTripsEveryType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		inline    any
		valueType Type
		metadata  Metadata
	}{
		{name: "null", inline: nil, valueType: TypeNull, metadata: testMetadata(RedactionPublic, RetentionNone)},
		{name: "string", inline: "hello", valueType: TypeString, metadata: testMetadata(RedactionPrivate, RetentionRun)},
		{name: "number", inline: json.Number("9007199254740993123456789"), valueType: TypeNumber, metadata: testMetadata(RedactionPublic, RetentionProject)},
		{name: "boolean", inline: true, valueType: TypeBoolean, metadata: testMetadata(RedactionPublic, RetentionExternal)},
		{
			name: "array", valueType: TypeArray,
			inline:   []any{nil, "item", json.Number("12.50"), false, map[string]any{"nested": "value"}},
			metadata: testMetadata(RedactionPrivate, RetentionRun),
		},
		{
			name: "object", valueType: TypeObject,
			inline: map[string]any{
				"name":  "example",
				"items": []any{json.Number("1"), map[string]any{"ok": true}},
			},
			metadata: testMetadata(RedactionPublic, RetentionProject),
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			value, err := NewInline(test.inline, test.metadata)
			if err != nil {
				t.Fatalf("NewInline failed: %v", err)
			}
			if value.Type != test.valueType {
				t.Fatalf("Type = %q, want %q", value.Type, test.valueType)
			}

			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatalf("Marshal failed: %v", err)
			}
			var decoded Value
			if unmarshalErr := json.Unmarshal(encoded, &decoded); unmarshalErr != nil {
				t.Fatalf("Unmarshal failed: %v\n%s", unmarshalErr, encoded)
			}
			if !reflect.DeepEqual(decoded, value) {
				t.Fatalf("round trip changed Value:\nwant: %#v\ngot:  %#v", value, decoded)
			}
			reencoded, err := json.Marshal(decoded)
			if err != nil {
				t.Fatalf("second Marshal failed: %v", err)
			}
			if string(reencoded) != string(encoded) {
				t.Fatalf("JSON is unstable:\nfirst:  %s\nsecond: %s", encoded, reencoded)
			}
			if decoded.Producer != test.metadata.Producer || decoded.MediaType != test.metadata.MediaType ||
				decoded.Redaction != test.metadata.Redaction || decoded.Retention != test.metadata.Retention {
				t.Fatalf("metadata was not preserved: %#v", decoded)
			}
			if test.valueType == TypeNull && !strings.Contains(string(encoded), `"inline":null`) {
				t.Fatalf("inline null presence was lost: %s", encoded)
			}
		})
	}
}

func TestArtifactValueJSONRoundTrip(t *testing.T) {
	t.Parallel()

	ref := testArtifactRef()
	value, err := NewArtifact(ref)
	if err != nil {
		t.Fatalf("NewArtifact failed: %v", err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if strings.Contains(string(encoded), `"inline"`) {
		t.Fatalf("artifact envelope contains inline payload: %s", encoded)
	}
	if strings.Contains(string(encoded), "artifact bytes") {
		t.Fatalf("artifact envelope contains raw artifact content: %s", encoded)
	}

	var decoded Value
	if unmarshalErr := json.Unmarshal(encoded, &decoded); unmarshalErr != nil {
		t.Fatalf("Unmarshal failed: %v", unmarshalErr)
	}
	if !reflect.DeepEqual(decoded, value) {
		t.Fatalf("round trip changed artifact Value:\nwant: %#v\ngot:  %#v", value, decoded)
	}
	if decoded.Artifact == nil || decoded.Artifact.Producer != ref.Producer ||
		decoded.Artifact.Redaction != ref.Redaction || decoded.Artifact.Retention != ref.Retention {
		t.Fatalf("artifact metadata was not preserved: %#v", decoded.Artifact)
	}
	reencoded, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("second Marshal failed: %v", err)
	}
	if string(reencoded) != string(encoded) {
		t.Fatalf("artifact JSON is unstable:\nfirst:  %s\nsecond: %s", encoded, reencoded)
	}
}

func TestNullValueHasStableExplicitJSON(t *testing.T) {
	t.Parallel()

	metadata := testMetadata(RedactionPublic, RetentionRun)
	value, err := NewInline(nil, metadata)
	if err != nil {
		t.Fatalf("NewInline failed: %v", err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	want := fmt.Sprintf(
		`{"type":"null","inline":null,"producer":{"kind":"node_output","reference":"run-1/node-1/iteration-0/attempt-1","output":"result"},"media_type":"application/json","digest":%q,"redaction":"public","retention":"run"}`,
		value.Digest,
	)
	if string(encoded) != want {
		t.Fatalf("stable null JSON:\nwant: %s\ngot:  %s", want, encoded)
	}
}

func TestDigestInlineIsDeterministicForNestedObjectOrder(t *testing.T) {
	t.Parallel()

	first := map[string]any{
		"z": []any{map[string]any{"b": json.Number("2"), "a": json.Number("1")}},
		"a": map[string]any{"second": false, "first": "value"},
	}
	second := map[string]any{
		"a": map[string]any{"first": "value", "second": false},
		"z": []any{map[string]any{"a": json.Number("1"), "b": json.Number("2")}},
	}
	firstDigest, err := DigestInline(first)
	if err != nil {
		t.Fatalf("DigestInline(first) failed: %v", err)
	}
	secondDigest, err := DigestInline(second)
	if err != nil {
		t.Fatalf("DigestInline(second) failed: %v", err)
	}
	if firstDigest != secondDigest {
		t.Fatalf("object key order changed digest: %q != %q", firstDigest, secondDigest)
	}

	second["a"].(map[string]any)["first"] = "changed"
	changedDigest, err := DigestInline(second)
	if err != nil {
		t.Fatalf("DigestInline(changed) failed: %v", err)
	}
	if changedDigest == firstDigest {
		t.Fatalf("nested value change did not change digest %q", firstDigest)
	}
}

func TestLargeJSONNumberPreservesLexeme(t *testing.T) {
	t.Parallel()

	want := json.Number("123456789012345678901234567890.0001")
	value, err := NewInline(want, testMetadata(RedactionPublic, RetentionRun))
	if err != nil {
		t.Fatalf("NewInline failed: %v", err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if !strings.Contains(string(encoded), `"inline":123456789012345678901234567890.0001`) {
		t.Fatalf("large number was coerced: %s", encoded)
	}
	var decoded Value
	if unmarshalErr := json.Unmarshal(encoded, &decoded); unmarshalErr != nil {
		t.Fatalf("Unmarshal failed: %v", unmarshalErr)
	}
	if got, ok := decoded.Inline.(json.Number); !ok || got != want {
		t.Fatalf("decoded number = %#v (%T), want %q", decoded.Inline, decoded.Inline, want)
	}
}

func TestDigestValueSetIsStableAndMetadataSensitive(t *testing.T) {
	t.Parallel()

	alpha, err := NewInline(map[string]any{"b": 2, "a": 1}, testMetadata(RedactionPublic, RetentionRun))
	if err != nil {
		t.Fatal(err)
	}
	beta, err := NewInline("value", testMetadata(RedactionPrivate, RetentionProject))
	if err != nil {
		t.Fatal(err)
	}
	first := ValueSet{"alpha": alpha, "beta": beta}
	second := ValueSet{"beta": beta, "alpha": alpha}

	firstDigest, err := DigestValueSet(first)
	if err != nil {
		t.Fatalf("DigestValueSet(first) failed: %v", err)
	}
	secondDigest, err := DigestValueSet(second)
	if err != nil {
		t.Fatalf("DigestValueSet(second) failed: %v", err)
	}
	if firstDigest != secondDigest {
		t.Fatalf("value-set map order changed digest: %q != %q", firstDigest, secondDigest)
	}

	changed := ValueSet{"alpha": alpha, "beta": beta}
	changedBeta := changed["beta"]
	changedBeta.Retention = RetentionExternal
	changed["beta"] = changedBeta
	changedDigest, err := DigestValueSet(changed)
	if err != nil {
		t.Fatalf("DigestValueSet(changed) failed: %v", err)
	}
	if changedDigest == firstDigest {
		t.Fatal("classification change did not change value-set digest")
	}
}

func TestRejectsNonNativeInlineValues(t *testing.T) {
	t.Parallel()

	type record struct{ Value string }
	pointer := "value"
	cyclic := map[string]any{}
	cyclic["self"] = cyclic
	tests := map[string]any{
		"nan":             math.NaN(),
		"positive inf":    math.Inf(1),
		"negative inf":    math.Inf(-1),
		"binary":          []byte("data"),
		"struct":          record{Value: "data"},
		"pointer":         &pointer,
		"function":        func() {},
		"non-string keys": map[int]string{1: "one"},
		"cyclic":          cyclic,
		"invalid number":  json.Number("01"),
	}
	for name, input := range tests {
		name, input := name, input
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewInline(input, testMetadata(RedactionPublic, RetentionRun)); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("error = %v, want ErrInvalidValue", err)
			}
		})
	}
}

func TestRejectsInvalidUTF8InlineValues(t *testing.T) {
	t.Parallel()

	invalidString := string([]byte{0xff})
	tests := map[string]any{
		"string": invalidString,
		"object key": map[string]any{
			string([]byte{0xff}): "first",
			string([]byte{0xfe}): "second",
		},
	}
	for name, inline := range tests {
		name, inline := name, inline
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewInline(inline, testMetadata(RedactionPublic, RetentionRun)); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("error = %v, want ErrInvalidValue", err)
			}
		})
	}
}

func TestStrictJSONRejectsInvalidUTF8BeforeReplacement(t *testing.T) {
	t.Parallel()

	value, err := NewInline("hello", testMetadata(RedactionPublic, RetentionRun))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	inline := bytes.Index(encoded, []byte(`"hello"`))
	if inline < 0 {
		t.Fatalf("inline string not found in %s", encoded)
	}
	encoded[inline+1] = 0xff

	var decoded Value
	if unmarshalErr := json.Unmarshal(encoded, &decoded); !errors.Is(unmarshalErr, ErrInvalidValue) {
		t.Fatalf("Unmarshal error = %v, want ErrInvalidValue", unmarshalErr)
	}
}

func TestRejectsInvalidUTF8TransportMetadata(t *testing.T) {
	t.Parallel()

	invalid := string([]byte{0xff})
	validValue, err := NewInline("value", testMetadata(RedactionPublic, RetentionRun))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		validate func() error
	}{
		{
			name: "producer kind",
			validate: func() error {
				producer := testMetadata(RedactionPublic, RetentionRun).Producer
				producer.Kind = invalid
				return producer.Validate()
			},
		},
		{
			name: "producer reference",
			validate: func() error {
				producer := testMetadata(RedactionPublic, RetentionRun).Producer
				producer.Reference = invalid
				return producer.Validate()
			},
		},
		{
			name: "producer output",
			validate: func() error {
				producer := testMetadata(RedactionPublic, RetentionRun).Producer
				producer.Output = invalid
				return producer.Validate()
			},
		},
		{
			name: "media type",
			validate: func() error {
				metadata := testMetadata(RedactionPublic, RetentionRun)
				metadata.MediaType = invalid
				return metadata.Validate()
			},
		},
		{
			name: "artifact store",
			validate: func() error {
				artifact := testArtifactRef()
				artifact.Store = invalid
				return artifact.Validate()
			},
		},
		{
			name: "artifact URI",
			validate: func() error {
				artifact := testArtifactRef()
				artifact.URI = invalid
				return artifact.Validate()
			},
		},
		{
			name: "value-set name",
			validate: func() error {
				return ValueSet{invalid: validValue}.Validate()
			},
		},
		{
			name: "value-set reference ID",
			validate: func() error {
				return (ValueSetRef{ID: invalid, Digest: SHA256Digest(nil)}).Validate()
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if validationErr := test.validate(); !errors.Is(validationErr, ErrInvalidValue) {
				t.Fatalf("Validate error = %v, want ErrInvalidValue", validationErr)
			}
		})
	}
}

func TestValueValidationRejectsInvalidEnumsAndEnvelopes(t *testing.T) {
	t.Parallel()

	valid, err := NewInline("value", testMetadata(RedactionPublic, RetentionRun))
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := NewArtifact(testArtifactRef())
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		edit func(*Value)
		want error
	}{
		{name: "unknown type", edit: func(v *Value) { v.Type = Type("binary") }, want: ErrInvalidValue},
		{name: "missing producer", edit: func(v *Value) { v.Producer = Producer{} }, want: ErrInvalidValue},
		{name: "invalid media type", edit: func(v *Value) { v.MediaType = "not a media type" }, want: ErrInvalidValue},
		{name: "unknown redaction", edit: func(v *Value) { v.Redaction = RedactionClass("masked") }, want: ErrInvalidValue},
		{name: "unknown retention", edit: func(v *Value) { v.Retention = RetentionClass("forever") }, want: ErrInvalidValue},
		{name: "invalid digest", edit: func(v *Value) { v.Digest = "sha256:nope" }, want: ErrInvalidValue},
		{name: "digest mismatch", edit: func(v *Value) { v.Digest = SHA256Digest([]byte("other")) }, want: ErrDigestMismatch},
		{name: "inline type mismatch", edit: func(v *Value) { v.Type = TypeBoolean }, want: ErrAmbiguousEnvelope},
		{name: "inline with artifact", edit: func(v *Value) { ref := testArtifactRef(); v.Artifact = &ref }, want: ErrAmbiguousEnvelope},
		{name: "artifact without ref", edit: func(v *Value) { *v = artifact; v.Artifact = nil }, want: ErrAmbiguousEnvelope},
		{name: "artifact with inline", edit: func(v *Value) { *v = artifact; v.Inline = "extra" }, want: ErrAmbiguousEnvelope},
		{name: "artifact metadata divergence", edit: func(v *Value) { *v = artifact; v.MediaType = "text/plain" }, want: ErrAmbiguousEnvelope},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			candidate := valid
			test.edit(&candidate)
			if validationErr := candidate.Validate(); !errors.Is(validationErr, test.want) {
				t.Fatalf("Validate error = %v, want %v", validationErr, test.want)
			}
		})
	}
}

func TestArtifactValidation(t *testing.T) {
	t.Parallel()

	valid := testArtifactRef()
	tests := []struct {
		name string
		edit func(*ArtifactRef)
	}{
		{name: "missing store", edit: func(r *ArtifactRef) { r.Store = "" }},
		{name: "missing URI", edit: func(r *ArtifactRef) { r.URI = "" }},
		{name: "bad digest", edit: func(r *ArtifactRef) { r.Digest = "md5:value" }},
		{name: "bad media type", edit: func(r *ArtifactRef) { r.MediaType = "bad" }},
		{name: "negative size", edit: func(r *ArtifactRef) { r.SizeBytes = -1 }},
		{name: "bad producer", edit: func(r *ArtifactRef) { r.Producer = Producer{} }},
		{name: "bad redaction", edit: func(r *ArtifactRef) { r.Redaction = "bad" }},
		{name: "bad retention", edit: func(r *ArtifactRef) { r.Retention = "bad" }},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			candidate := valid
			test.edit(&candidate)
			if validationErr := candidate.Validate(); !errors.Is(validationErr, ErrInvalidValue) {
				t.Fatalf("Validate error = %v, want ErrInvalidValue", validationErr)
			}
		})
	}
}

func TestStrictJSONRejectsAmbiguousValueEnvelopes(t *testing.T) {
	t.Parallel()

	digest, err := DigestInline("hello")
	if err != nil {
		t.Fatal(err)
	}
	base := fmt.Sprintf(
		`{"type":"string","inline":"hello","producer":{"kind":"node_output","reference":"node-1","output":"result"},"media_type":"application/json","digest":%q,"redaction":"public","retention":"run"}`,
		digest,
	)
	tests := []struct {
		name string
		json string
		want error
	}{
		{name: "both payloads", json: strings.Replace(base, `"inline":"hello"`, `"inline":"hello","artifact":{}`, 1), want: ErrAmbiguousEnvelope},
		{name: "neither payload", json: strings.Replace(base, `,"inline":"hello"`, "", 1), want: ErrAmbiguousEnvelope},
		{name: "artifact type with inline", json: strings.Replace(base, `"type":"string"`, `"type":"artifact"`, 1), want: ErrAmbiguousEnvelope},
		{name: "duplicate envelope field", json: strings.Replace(base, `{`, `{"type":"null",`, 1), want: ErrAmbiguousEnvelope},
		{name: "unknown envelope field", json: strings.TrimSuffix(base, "}") + `,"logs":"not-data"}`, want: ErrInvalidValue},
		{name: "missing metadata", json: strings.Replace(base, `,"producer":{"kind":"node_output","reference":"node-1","output":"result"}`, "", 1), want: ErrInvalidValue},
		{name: "digest mismatch", json: strings.Replace(base, digest, SHA256Digest([]byte("different")), 1), want: ErrDigestMismatch},
		{name: "duplicate nested key", json: strings.Replace(base, `"type":"string","inline":"hello"`, `"type":"object","inline":{"key":1,"key":2}`, 1), want: ErrAmbiguousEnvelope},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var value Value
			if unmarshalErr := json.Unmarshal([]byte(test.json), &value); !errors.Is(unmarshalErr, test.want) {
				t.Fatalf("Unmarshal error = %v, want %v\n%s", unmarshalErr, test.want, test.json)
			}
		})
	}
}

func TestStrictJSONRejectsUnknownArtifactAndProducerFields(t *testing.T) {
	t.Parallel()

	ref := testArtifactRef()
	encoded, err := json.Marshal(ref)
	if err != nil {
		t.Fatal(err)
	}
	withUnknown := strings.TrimSuffix(string(encoded), "}") + `,"authority":"host-owned"}`
	var decoded ArtifactRef
	if unmarshalErr := json.Unmarshal([]byte(withUnknown), &decoded); !errors.Is(unmarshalErr, ErrInvalidValue) {
		t.Fatalf("unknown artifact field error = %v", unmarshalErr)
	}

	producerJSON := `{"kind":"node_output","reference":"node-1","host_id":"hadron-specific"}`
	var producer Producer
	if unmarshalErr := json.Unmarshal([]byte(producerJSON), &producer); !errors.Is(unmarshalErr, ErrInvalidValue) {
		t.Fatalf("unknown producer field error = %v", unmarshalErr)
	}
}

func TestValueSetAndReferenceJSONRoundTrip(t *testing.T) {
	t.Parallel()

	input, err := NewInline(map[string]any{"enabled": true}, testMetadata(RedactionPrivate, RetentionRun))
	if err != nil {
		t.Fatal(err)
	}
	set := ValueSet{"input": input}
	encoded, err := json.Marshal(set)
	if err != nil {
		t.Fatalf("Marshal set failed: %v", err)
	}
	var decoded ValueSet
	if unmarshalErr := json.Unmarshal(encoded, &decoded); unmarshalErr != nil {
		t.Fatalf("Unmarshal set failed: %v", unmarshalErr)
	}
	if !reflect.DeepEqual(decoded, set) {
		t.Fatalf("set round trip changed value: %#v != %#v", decoded, set)
	}
	valueJSON, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	duplicateNames := fmt.Sprintf(`{"input":%s,"input":%s}`, valueJSON, valueJSON)
	if unmarshalErr := json.Unmarshal([]byte(duplicateNames), &decoded); !errors.Is(unmarshalErr, ErrAmbiguousEnvelope) {
		t.Fatalf("duplicate value-set name error = %v", unmarshalErr)
	}
	if validationErr := (ValueSet(nil)).Validate(); !errors.Is(validationErr, ErrInvalidValue) {
		t.Fatalf("nil value-set error = %v", validationErr)
	}

	ref, err := NewValueSetRef("valueset-01", set)
	if err != nil {
		t.Fatalf("NewValueSetRef failed: %v", err)
	}
	refJSON, err := json.Marshal(ref)
	if err != nil {
		t.Fatalf("Marshal ref failed: %v", err)
	}
	var decodedRef ValueSetRef
	if unmarshalErr := json.Unmarshal(refJSON, &decodedRef); unmarshalErr != nil {
		t.Fatalf("Unmarshal ref failed: %v", unmarshalErr)
	}
	if decodedRef != ref {
		t.Fatalf("ref round trip changed value: %#v != %#v", decodedRef, ref)
	}
	for _, invalid := range []ValueSetRef{{Digest: ref.Digest}, {ID: ref.ID, Digest: "bad"}} {
		if validationErr := invalid.Validate(); !errors.Is(validationErr, ErrInvalidValue) {
			t.Fatalf("invalid ref error = %v", validationErr)
		}
	}
}

func testMetadata(redaction RedactionClass, retention RetentionClass) Metadata {
	return Metadata{
		Producer: Producer{
			Kind: "node_output", Reference: "run-1/node-1/iteration-0/attempt-1", Output: "result",
		},
		MediaType: "application/json",
		Redaction: redaction,
		Retention: retention,
	}
}

func testArtifactRef() ArtifactRef {
	return ArtifactRef{
		Store: "external", URI: "artifact://reports/run-1/report.pdf",
		Digest: SHA256Digest([]byte("artifact bytes")), MediaType: "application/pdf", SizeBytes: 14,
		Producer: Producer{
			Kind: "node_output", Reference: "run-1/render/iteration-0/attempt-1", Output: "report",
		},
		Redaction: RedactionSecret,
		Retention: RetentionExternal,
	}
}
