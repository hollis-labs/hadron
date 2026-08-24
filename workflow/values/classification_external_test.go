package values_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestSecretRefGrammarJSONAndEnvelope(t *testing.T) {
	t.Parallel()
	valid := []string{
		"secret://project/api/token",
		"secret://vault-prod/team/service#password",
		"secret://aws.secrets-manager/prod%20key#token",
	}
	for _, raw := range valid {
		ref, err := values.ParseSecretRef(raw)
		if err != nil {
			t.Fatalf("ParseSecretRef(%q): %v", raw, err)
		}
		encoded, err := json.Marshal(ref)
		if err != nil {
			t.Fatalf("Marshal(%q): %v", raw, err)
		}
		var decoded values.SecretRef
		if err := json.Unmarshal(encoded, &decoded); err != nil || decoded != ref {
			t.Fatalf("SecretRef round trip %q => %q, %v", raw, decoded, err)
		}
		if ref.Authority() == "" || ref.Path() == "" {
			t.Fatalf("SecretRef accessors lost identity: %q %q", ref.Authority(), ref.Path())
		}
	}

	invalid := []string{
		"", " secret://project/path", "SECRET://project/path", "secret:path",
		"secret:///path", "secret://project", "secret://user@project/path",
		"secret://Project/path", "secret://project/%70ath", "secret://project/path#%66ield",
		"secret://project:443/path", "secret://project/path?version=1",
		"secret://project//path", "secret://project/../path", "secret://project/%2e%2e/path",
		"secret://project/a%2Fb", "secret://project/path#", "secret://project/path#a/b",
	}
	for _, raw := range invalid {
		if _, err := values.ParseSecretRef(raw); !errors.Is(err, values.ErrInvalidSecretRef) {
			t.Errorf("ParseSecretRef(%q) error = %v", raw, err)
		}
	}

	ref, _ := values.ParseSecretRef("secret://project/api#token")
	value, err := values.NewSecretRef(ref, classificationMetadata(values.RedactionSecret, values.RetentionProject))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(value)
	if err != nil || !bytes.Contains(encoded, []byte(`"type":"secret_ref"`)) || bytes.Contains(encoded, []byte(`"inline"`)) {
		t.Fatalf("secret Value JSON = %s, %v", encoded, err)
	}
	var decoded values.Value
	if err := json.Unmarshal(encoded, &decoded); err != nil || !reflect.DeepEqual(decoded, value) {
		t.Fatalf("secret Value round trip = %#v, %v", decoded, err)
	}
	if _, err := values.NewSecretRef(ref, classificationMetadata(values.RedactionPrivate, values.RetentionProject)); !errors.Is(err, values.ErrInvalidValue) {
		t.Fatalf("non-secret SecretRef error = %v", err)
	}
	if _, err := values.NewInline("raw-secret", classificationMetadata(values.RedactionSecret, values.RetentionRun)); !errors.Is(err, values.ErrSecretMaterial) {
		t.Fatalf("secret inline error = %v", err)
	}
}

func TestSecretArtifactAndPersistenceClassification(t *testing.T) {
	t.Parallel()
	artifact, err := values.NewArtifact(values.ArtifactRef{
		Store: "external", URI: "artifact://vault/blob", Digest: values.SHA256Digest([]byte("secret bytes")),
		MediaType: "application/octet-stream", SizeBytes: 12,
		Producer:  classificationMetadata(values.RedactionSecret, values.RetentionExternal).Producer,
		Redaction: values.RedactionSecret, Retention: values.RetentionExternal,
	})
	if err != nil {
		t.Fatal(err)
	}
	if validationErr := values.ValidatePersistable(artifact); validationErr != nil {
		t.Fatalf("secret ArtifactRef was rejected: %v", validationErr)
	}
	none, err := values.NewInline("ephemeral", classificationMetadata(values.RedactionPrivate, values.RetentionNone))
	if err != nil {
		t.Fatal(err)
	}
	if err := values.ValidatePersistableSet(values.ValueSet{"ephemeral": none}); !errors.Is(err, values.ErrRetentionViolation) {
		t.Fatalf("retention none persistence error = %v", err)
	}
}

func TestSecretRefSchemaBoundariesAndLiteralNonMutation(t *testing.T) {
	t.Parallel()
	ref, _ := values.ParseSecretRef("secret://project/api#token")
	secret, _ := values.NewSecretRef(ref, classificationMetadata(values.RedactionSecret, values.RetentionProject))
	inline, _ := values.NewInline(string(ref), classificationMetadata(values.RedactionPrivate, values.RetentionRun))

	for name, schema := range map[string]graph.Schema{
		"direct": {"type": "secret_ref"},
		"local ref": {
			"$ref": "#/$defs/credential", "$defs": map[string]any{"credential": map[string]any{"type": "secret_ref"}},
		},
		"allOf": {
			"allOf": []any{map[string]any{"$ref": "#/$defs/credential"}, map[string]any{"minLength": json.Number("8")}},
			"$defs": map[string]any{"credential": map[string]any{"type": "secret_ref"}},
		},
		"anyOf": {"anyOf": []any{map[string]any{"type": "null"}, map[string]any{"type": "secret_ref"}}},
		"oneOf": {"oneOf": []any{map[string]any{"type": "secret_ref"}, map[string]any{"type": "artifact"}}},
	} {
		if err := values.ValidateSchema(schema); err != nil {
			t.Errorf("ValidateSchema(%s): %v", name, err)
		}
		if err := values.ValidateValueSchema(schema, secret); err != nil {
			t.Errorf("ValidateValueSchema(%s): %v", name, err)
		}
	}
	if err := values.ValidateValueSchema(graph.Schema{"type": "string"}, secret); !errors.Is(err, values.ErrSchemaMismatch) {
		t.Fatalf("plain string accepted SecretRef: %v", err)
	}
	if err := values.ValidateValueSchema(graph.Schema{"type": "secret_ref"}, inline); !errors.Is(err, values.ErrSchemaMismatch) {
		t.Fatalf("secret_ref schema accepted inline string: %v", err)
	}
	if err := values.ValidateValueSchema(nil, secret); err != nil {
		t.Fatalf("empty schema rejected SecretRef: %v", err)
	}

	schema := graph.Schema{
		"type":     "object",
		"const":    map[string]any{"type": "secret_ref"},
		"enum":     []any{map[string]any{"type": "artifact"}},
		"default":  map[string]any{"type": "secret_ref"},
		"examples": []any{map[string]any{"type": "artifact"}},
	}
	want := cloneSchema(t, schema)
	if err := values.ValidateSchema(schema); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(schema, want) {
		t.Fatalf("schema literal data mutated:\ngot  %#v\nwant %#v", schema, want)
	}
}

func TestResolvedSecretAndDeterministicRedactor(t *testing.T) {
	t.Parallel()
	ref, _ := values.ParseSecretRef("secret://project/service#token")
	resolver := values.SecretResolverFunc(func(_ context.Context, got values.SecretRef) (*values.ResolvedSecret, error) {
		if got != ref {
			t.Fatalf("resolver ref = %q, want %q", got, ref)
		}
		return values.NewResolvedSecret(got, []byte("token-123"))
	})
	var resolutionSeam values.SecretResolver = resolver
	long, err := resolutionSeam.ResolveSecret(t.Context(), ref)
	if err != nil || long.Reference() != ref {
		t.Fatalf("ResolveSecret = %v, %v", long, err)
	}
	short, _ := values.NewResolvedSecret(ref, []byte("token"))
	duplicate, _ := values.NewResolvedSecret(ref, []byte("token-123"))
	redactor, err := values.NewRedactor(short, long, duplicate)
	if err != nil {
		t.Fatal(err)
	}
	input := []byte("before token-123 / token after")
	want := "before [REDACTED] / [REDACTED] after"
	if got := string(redactor.MaskBytes(input)); got != want {
		t.Fatalf("MaskBytes = %q, want %q", got, want)
	}
	if string(input) != "before token-123 / token after" {
		t.Fatal("MaskBytes mutated input")
	}
	if got := redactor.MaskString("token-123token"); got != "[REDACTED][REDACTED]" {
		t.Fatalf("overlap ordering = %q", got)
	}
	binary, _ := values.NewResolvedSecret(ref, []byte{0, 0xff, 'x'})
	binaryRedactor, _ := values.NewRedactor(binary)
	if got := binaryRedactor.MaskBytes([]byte{'a', 0, 0xff, 'x', 'b'}); string(got) != "a[REDACTED]b" {
		t.Fatalf("binary mask = %q", got)
	}
	for split := 0; split <= len(input); split++ {
		var output bytes.Buffer
		writer := redactor.Writer(&output)
		if _, err := writer.Write(input[:split]); err != nil {
			t.Fatalf("split %d first write: %v", split, err)
		}
		if _, err := writer.Write(input[split:]); err != nil {
			t.Fatalf("split %d second write: %v", split, err)
		}
		if err := writer.Close(); err != nil || output.String() != want {
			t.Fatalf("split %d output = %q, %v", split, output.String(), err)
		}
	}

	channels := []string{"command", "prompt", "message", "http", "mcp"}
	for _, channel := range channels {
		masked := redactor.MaskObservation(values.Observation{
			Channel: channel, Attributes: map[string]string{"request": "Bearer token-123"}, Payload: []byte("token response"),
		})
		if masked.Attributes["request"] != "Bearer [REDACTED]" || string(masked.Payload) != "[REDACTED] response" {
			t.Errorf("%s observation leaked: %#v", channel, masked)
		}
	}
	if _, err := json.Marshal(long); !errors.Is(err, values.ErrSecretMaterial) {
		t.Fatalf("resolved secret JSON error = %v", err)
	}
	if _, err := values.NewInline(map[string]any{"secret": long}, classificationMetadata(values.RedactionPrivate, values.RetentionRun)); !errors.Is(err, values.ErrSecretMaterial) {
		t.Fatalf("resolved secret inline error = %v", err)
	}
	if _, err := values.NewInline(map[string]any{"ref": ref}, classificationMetadata(values.RedactionPrivate, values.RetentionRun)); !errors.Is(err, values.ErrSecretDerivation) {
		t.Fatalf("typed SecretRef inline error = %v", err)
	}
	copyBytes := long.Bytes()
	long.Forget()
	if got := long.Bytes(); got != nil || string(copyBytes) != "token-123" || long.String() != values.RedactedMarker {
		t.Fatalf("Forget/access contract = %q %q %q", got, copyBytes, long.String())
	}
	if _, err := values.NewRedactor(long); !errors.Is(err, values.ErrSecretMaterial) {
		t.Fatalf("forgotten material accepted: %v", err)
	}
}

func TestStreamingRedactorPropagatesWriterFailure(t *testing.T) {
	t.Parallel()
	ref, _ := values.ParseSecretRef("secret://project/service#token")
	secret, _ := values.NewResolvedSecret(ref, []byte("secret"))
	redactor, _ := values.NewRedactor(secret)
	writer := redactor.Writer(zeroWriter{})
	if _, err := writer.Write([]byte("secret plus enough buffered data")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short writer error = %v", err)
	}
}

func TestRenderValuesMasksByDefaultAndNeverRevealsSecrets(t *testing.T) {
	t.Parallel()
	public, _ := values.NewInline("visible", classificationMetadata(values.RedactionPublic, values.RetentionRun))
	private, _ := values.NewInline("private", classificationMetadata(values.RedactionPrivate, values.RetentionProject))
	secretRef, _ := values.ParseSecretRef("secret://project/api#token")
	secret, _ := values.NewSecretRef(secretRef, classificationMetadata(values.RedactionSecret, values.RetentionProject))
	secretArtifact, _ := values.NewArtifact(values.ArtifactRef{
		Store: "external", URI: "artifact://vault/private", Digest: values.SHA256Digest([]byte("secret")),
		MediaType: "application/octet-stream", SizeBytes: 6, Producer: classificationMetadata(values.RedactionSecret, values.RetentionExternal).Producer,
		Redaction: values.RedactionSecret, Retention: values.RetentionExternal,
	})

	masked, err := values.RenderValueSet(values.ValueSet{
		"public": public, "private": private, "secret": secret, "secret_artifact": secretArtifact,
	}, values.DisplayPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if masked["public"].Payload != "visible" || masked["private"].Payload != values.RedactedMarker ||
		masked["secret"].Payload != values.RedactedMarker || masked["secret_artifact"].Payload != values.RedactedMarker {
		t.Fatalf("default render = %#v", masked)
	}
	revealed, err := values.RenderValue(private, values.DisplayPolicy{Private: values.PrivateDisplayReveal})
	if err != nil || revealed.Payload != "private" || revealed.Masked {
		t.Fatalf("private reveal = %#v, %v", revealed, err)
	}
	stillSecret, _ := values.RenderValue(secretArtifact, values.DisplayPolicy{Private: values.PrivateDisplayReveal})
	if stillSecret.Payload != values.RedactedMarker || !stillSecret.Masked {
		t.Fatalf("secret artifact revealed: %#v", stillSecret)
	}
	if _, err := values.RenderValue(public, values.DisplayPolicy{Private: "show"}); err == nil {
		t.Fatal("unknown display policy accepted")
	}
}

func TestSecretRefExpressionTaintAndExactPassthrough(t *testing.T) {
	t.Parallel()
	ref, _ := values.ParseSecretRef("secret://project/api#token")
	secret, _ := values.NewSecretRef(ref, classificationMetadata(values.RedactionSecret, values.RetentionProject))
	secretArtifact, _ := values.NewArtifact(values.ArtifactRef{
		Store: "external", URI: "artifact://vault/credential", Digest: values.SHA256Digest([]byte("credential")),
		MediaType: "application/octet-stream", SizeBytes: 10,
		Producer:  classificationMetadata(values.RedactionSecret, values.RetentionExternal).Producer,
		Redaction: values.RedactionSecret, Retention: values.RetentionExternal,
	})
	public, _ := values.NewInline("ok", classificationMetadata(values.RedactionPublic, values.RetentionRun))
	context := values.ExpressionContext{Inputs: values.ValueSet{
		"credential": secret, "credential_artifact": secretArtifact, "ordinary": public,
	}}
	engine := values.NewExpressionEngine()
	metadata := classificationMetadata(values.RedactionPrivate, values.RetentionRun)

	exact, err := engine.EvaluateBinding(graph.Binding{
		Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "inputs.credential"},
	}, context, values.ExpressionOptions{}, metadata)
	if err != nil || !reflect.DeepEqual(exact, secret) {
		t.Fatalf("exact SecretRef passthrough = %#v, %v", exact, err)
	}
	exactArtifact, err := engine.EvaluateBinding(graph.Binding{
		Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "inputs.credential_artifact"},
	}, context, values.ExpressionOptions{}, metadata)
	if err != nil || !reflect.DeepEqual(exactArtifact, secretArtifact) {
		t.Fatalf("exact secret ArtifactRef passthrough = %#v, %v", exactArtifact, err)
	}
	for name, binding := range map[string]graph.Binding{
		"expression":             {Kind: graph.BindingExpression, Expression: &graph.Expression{Text: `string(inputs.credential)`}},
		"interpolation":          {Kind: graph.BindingInterpolation, Interpolation: `token={{ inputs.credential }}`},
		"artifact expression":    {Kind: graph.BindingExpression, Expression: &graph.Expression{Text: `inputs.credential_artifact.uri`}},
		"artifact interpolation": {Kind: graph.BindingInterpolation, Interpolation: `uri={{ inputs.credential_artifact.uri }}`},
	} {
		if _, evaluationErr := engine.EvaluateBinding(binding, context, values.ExpressionOptions{}, metadata); !errors.Is(evaluationErr, values.ErrSecretDerivation) {
			t.Errorf("%s secret derivation error = %v", name, evaluationErr)
		}
	}
	ordinary, err := engine.EvaluateBinding(graph.Binding{
		Kind: graph.BindingExpression, Expression: &graph.Expression{Text: `inputs.ordinary + "!"`},
	}, context, values.ExpressionOptions{}, metadata)
	if err != nil || ordinary.Inline != "ok!" {
		t.Fatalf("unrelated expression with secret context = %#v, %v", ordinary, err)
	}
	if _, evaluationErr := engine.EvaluateRaw(graph.Expression{Text: "inputs[run.key]"}, values.ExpressionContext{
		Inputs: context.Inputs, Run: map[string]any{"key": "credential"},
	}, values.ExpressionOptions{}); !errors.Is(evaluationErr, values.ErrSecretDerivation) {
		t.Fatalf("dynamic secret selection error = %v", evaluationErr)
	}
	status, err := engine.EvaluateRaw(graph.Expression{Text: "steps.secure.status"}, values.ExpressionContext{
		Steps: map[string]values.StepContext{
			"secure": {Status: "succeeded", Outputs: values.ValueSet{"credential": secret}},
		},
	}, values.ExpressionOptions{})
	if err != nil || status != "succeeded" {
		t.Fatalf("non-secret step status was tainted: %#v, %v", status, err)
	}
}

func classificationMetadata(redaction values.RedactionClass, retention values.RetentionClass) values.Metadata {
	return values.Metadata{
		Producer:  values.Producer{Kind: "test", Reference: "classification-test", Output: "value"},
		MediaType: "application/json", Redaction: redaction, Retention: retention,
	}
}

func cloneSchema(t *testing.T, schema graph.Schema) graph.Schema {
	t.Helper()
	encoded, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	var cloned graph.Schema
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&cloned); err != nil {
		t.Fatal(err)
	}
	return cloned
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

var _ io.Writer = zeroWriter{}
