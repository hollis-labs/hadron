package generatedapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"

	generatedapi "github.com/hollis-labs/hadron/workflow/adapters/generatedapi"
	httpadapter "github.com/hollis-labs/hadron/workflow/adapters/http"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

type allowPolicy struct {
	mu           sync.Mutex
	declarations []httpadapter.RequestDeclaration
	order        []string
}

func (p *allowPolicy) DescribeRequest(ctx context.Context, declaration httpadapter.RequestDeclaration) (httpadapter.PolicyDescription, error) {
	p.mu.Lock()
	p.declarations = append(p.declarations, declaration)
	p.order = append(p.order, "policy")
	p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return httpadapter.PolicyDescription{}, err
	}
	if declaration.Method == http.MethodGet || declaration.Method == http.MethodHead || declaration.Method == http.MethodOptions {
		return httpadapter.PolicyDescription{
			Trusted: true, Effects: graph.EffectSet{graph.EffectRead},
			Idempotency: graph.IdempotencyIntrinsic, RetrySafety: stepkind.RetrySafe,
		}, nil
	}
	return httpadapter.PolicyDescription{}, nil
}

func (*allowPolicy) AuthorizeDestination(ctx context.Context, _ httpadapter.DestinationRequest) (httpadapter.DestinationAuthorization, error) {
	return httpadapter.DestinationAuthorization{}, ctx.Err()
}

func (p *allowPolicy) recordSecretResolution() {
	p.mu.Lock()
	p.order = append(p.order, "secret")
	p.mu.Unlock()
}

func (p *allowPolicy) snapshot() ([]httpadapter.RequestDeclaration, []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]httpadapter.RequestDeclaration(nil), p.declarations...), append([]string(nil), p.order...)
}

func TestOpenAPICatalogSnapshotDeterminismAndIsolation(t *testing.T) {
	source := readFixture(t)
	adapter, _ := newHTTPAdapter(t, nil)
	var first []byte
	for iteration := 0; iteration < 25; iteration++ {
		family, err := generatedapi.GenerateOpenAPI(t.Context(), source, generatedapi.Options{Namespace: "widgets", HTTP: adapter})
		if err != nil {
			t.Fatalf("GenerateOpenAPI() error = %v", err)
		}
		encoded, err := json.Marshal(family.Operations())
		if err != nil {
			t.Fatalf("Marshal() error = %v", err)
		}
		encoded = append(encoded, '\n')
		if iteration == 0 {
			first = encoded
		} else if !reflect.DeepEqual(encoded, first) {
			t.Fatalf("iteration %d catalog differs", iteration)
		}
	}
	want, err := os.ReadFile("testdata/widgets.family.golden.json")
	if err != nil {
		t.Fatalf("ReadFile(golden) error = %v\nactual:\n%s", err, first)
	}
	if !reflect.DeepEqual(first, want) {
		t.Fatalf("generated catalog is stale\nactual:\n%s", first)
	}
	if !strings.Contains(string(first), "9007199254740993") {
		t.Fatal("catalog snapshot lost exact JSON number")
	}

	family := generateFixture(t, source, adapter)
	original := family.Operations()
	mutated := family.Operations()
	mutated[0].InputSchema["type"] = "string"
	mutated[0].Effects[0] = graph.EffectDestructive
	mutated[0].RequiredCapabilities[0] = "forged"
	mutated[0].Credentials[0].Input = "forged"
	mutated[0].SuccessStatuses[0] = 599
	if !reflect.DeepEqual(family.Operations(), original) {
		t.Fatal("catalog projection retained caller mutation")
	}
	kind := family.Kinds()[0]
	spec := kind.Spec()
	spec.InputSchema["type"] = "string"
	spec.Effects[0] = graph.EffectDestructive
	if reflect.DeepEqual(kind.Spec(), spec) {
		t.Fatal("generated kind Spec retained caller mutation")
	}
	registry := stepkind.NewRegistry()
	if err := family.Register(registry); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	for _, operation := range original {
		if err := graph.ValidateID(operation.Name); err != nil {
			t.Fatalf("generated name %q is not graph-valid: %v", operation.Name, err)
		}
		if err := graph.ValidateID(operation.Version); err != nil {
			t.Fatalf("generated version %q is not graph-valid: %v", operation.Version, err)
		}
		if _, ok := registry.Lookup(operation.Name, operation.Version); !ok {
			t.Fatalf("registered kind %s@%s missing", operation.Name, operation.Version)
		}
	}
}

func TestOpenAPIGeneratedKindsExposePolicyFactsToCompiler(t *testing.T) {
	adapter, _ := newHTTPAdapter(t, nil)
	family := generateFixture(t, readFixture(t), adapter)
	registry := stepkind.NewRegistry()
	if err := family.Register(registry); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	operations := family.Operations()
	var create generatedapi.OperationDescription
	for _, operation := range operations {
		if operation.OperationID == "createWidget" {
			create = operation
		}
	}
	if create.Name == "" {
		t.Fatal("createWidget operation missing")
	}
	called := 0
	hook := workflowcompile.PolicyHookFunc(func(_ context.Context, input workflowcompile.NodeValidation) []diagnostic.Diagnostic {
		called++
		if input.Kind == nil {
			t.Fatal("policy hook did not receive generated kind")
		}
		if !reflect.DeepEqual(input.Kind.Effects, graph.EffectSet{
			graph.EffectCompute, graph.EffectDestructive, graph.EffectMaterialize, graph.EffectMutate, graph.EffectRead,
		}) {
			t.Fatalf("policy-visible effects = %v", input.Kind.Effects)
		}
		if !reflect.DeepEqual(input.Kind.RequiredCapabilities, []string{"network.http", "secrets.resolve", "widgets.write"}) {
			t.Fatalf("policy-visible capabilities = %v", input.Kind.RequiredCapabilities)
		}
		properties := input.Kind.InputSchema["properties"].(map[string]any)
		credential := properties["credential.apikeyauth"].(map[string]any)
		if credential["type"] != "secret_ref" {
			t.Fatalf("credential schema = %#v", credential)
		}
		return nil
	})
	findings := workflowcompile.ValidateGraph(t.Context(), graph.Graph{
		ID: "generated-policy", Version: "v1", Digest: "sha256:generated-policy",
		Nodes: []graph.Node{{ID: "create", Kind: create.Name, KindVersion: create.Version, Config: graph.Config{}}},
	}, workflowcompile.ValidationOptions{StepKinds: registry, PolicyHooks: []workflowcompile.PolicyHook{hook}})
	if len(findings) != 0 {
		t.Fatalf("ValidateGraph() findings = %+v", findings)
	}
	if called != 1 {
		t.Fatalf("policy hook calls = %d", called)
	}
}

func TestOpenAPIGeneratedKindsExecuteThroughHTTPWithOpaqueCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.Method + " " + request.URL.Path {
		case "GET /widgets/widget-1":
			if request.Header.Get("Authorization") != "Bearer bearer-token" {
				t.Errorf("Authorization = %q", request.Header.Get("Authorization"))
			}
			if request.URL.Query()["include"][0] != "owner" || request.URL.Query()["include"][1] != "history" || request.URL.Query().Get("revision") != "9007199254740993" {
				t.Errorf("query = %v", request.URL.Query())
			}
			_, _ = writer.Write([]byte(`{"id":"widget-1","name":"First","revision":9007199254740993}`))
		case "POST /widgets":
			if request.Header.Get("X-Widget-Key") != "api-key" {
				t.Errorf("X-Widget-Key = %q", request.Header.Get("X-Widget-Key"))
			}
			if request.Header.Get("Idempotency-Key") != "create-widget-1" {
				t.Errorf("Idempotency-Key = %q", request.Header.Get("Idempotency-Key"))
			}
			var body map[string]any
			decoder := json.NewDecoder(request.Body)
			decoder.UseNumber()
			if err := decoder.Decode(&body); err != nil || body["name"] != "Created" {
				t.Errorf("body = %#v, err = %v", body, err)
			}
			writer.WriteHeader(http.StatusCreated)
			_, _ = writer.Write([]byte(`{"id":"widget-2","name":"Created","revision":1}`))
		case "GET /profile":
			username, password, ok := request.BasicAuth()
			if !ok || username != "service-account" || password != "basic-password" {
				t.Errorf("BasicAuth = %q / %q / %t", username, password, ok)
			}
			_, _ = writer.Write([]byte(`{"id":"widget-profile","name":"Profile","revision":2}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	policy := &allowPolicy{}
	secrets := values.SecretResolverFunc(func(_ context.Context, ref values.SecretRef) (*values.ResolvedSecret, error) {
		policy.recordSecretResolution()
		material := map[values.SecretRef][]byte{
			"secret://test/bearer":  []byte("bearer-token"),
			"secret://test/api-key": []byte("api-key"),
			"secret://test/basic":   []byte("basic-password"),
		}[ref]
		if len(material) == 0 {
			return nil, errors.New("unknown test secret")
		}
		return values.NewResolvedSecret(ref, material)
	})
	adapter, _ := newHTTPAdapter(t, struct {
		policy  *allowPolicy
		secrets values.SecretResolver
	}{policy: policy, secrets: secrets})
	source := strings.Replace(string(readFixture(t)), "https://api.example.test", server.URL, 1)
	family := generateFixture(t, []byte(source), adapter)
	byID := make(map[string]stepkind.StepKind)
	for _, kind := range family.Kinds() {
		byID[kind.(*generatedapi.Kind).Description().OperationID] = kind
	}

	t.Run("bearer GET", func(t *testing.T) {
		result, err := byID["getWidget"].Execute(t.Context(), invocation(t, values.ValueSet{
			"path.widgetid":         inline(t, "widget-1"),
			"query.revision":        inline(t, json.Number("9007199254740993")),
			"query.include":         inline(t, []any{"owner", "history"}),
			"credential.bearerauth": secretRef(t, "secret://test/bearer"),
		}))
		if err != nil {
			t.Fatalf("Execute(GET) error = %v", err)
		}
		assertBody(t, result, "widget-1", json.Number("9007199254740993"))
	})

	t.Run("header API key POST", func(t *testing.T) {
		inputs := values.ValueSet{
			"body":                  inline(t, map[string]any{"name": "Created"}),
			"credential.apikeyauth": secretRef(t, "secret://test/api-key"),
		}
		missing, err := byID["createWidget"].Execute(t.Context(), invocation(t, inputs))
		var execution *stepkind.ExecutionError
		if missing.Outcome != "" || !errors.As(err, &execution) || execution.Code != "generated_api_idempotency_required" {
			t.Fatalf("Execute(POST without key) error = %T %v", err, err)
		}
		prepared := invocation(t, inputs)
		prepared.Invocation.IdempotencyKey = "create-widget-1"
		result, err := byID["createWidget"].Execute(t.Context(), prepared)
		if err != nil {
			t.Fatalf("Execute(POST) error = %v", err)
		}
		assertBody(t, result, "widget-2", json.Number("1"))
	})

	t.Run("basic GET", func(t *testing.T) {
		result, err := byID["getProfile"].Execute(t.Context(), invocation(t, values.ValueSet{
			"credential.basicauth": secretRef(t, "secret://test/basic"),
		}))
		if err != nil {
			t.Fatalf("Execute(basic GET) error = %v", err)
		}
		assertBody(t, result, "widget-profile", json.Number("2"))
	})

	declarations, order := policy.snapshot()
	if len(declarations) < 4 {
		t.Fatalf("policy declarations = %d, want at least two per execution", len(declarations))
	}
	for _, declaration := range declarations {
		if !declaration.HasSecretRefs || !contains(declaration.Capabilities, "secrets.resolve") {
			t.Fatalf("credential/effect facts not visible before execution: %+v", declaration)
		}
	}
	policySinceSecret := 0
	for _, item := range order {
		switch item {
		case "policy":
			policySinceSecret++
		case "secret":
			if policySinceSecret < 2 {
				t.Fatalf("secret resolved before generated and HTTP policy descriptions: %v", order)
			}
			policySinceSecret = 0
		}
	}
}

func TestOpenAPIRejectsUnsafeOrAmbiguousSources(t *testing.T) {
	adapter, _ := newHTTPAdapter(t, nil)
	base := string(readFixture(t))
	cycle := strings.Replace(base, `$ref: "#/components/schemas/CreateWidget"`, `$ref: "#/components/schemas/Loop"`, 1)
	cycle = strings.Replace(cycle, "  schemas:\n", "  schemas:\n    Loop:\n      $ref: \"#/components/schemas/Loop\"\n", 1)
	cases := []struct {
		name   string
		source string
		want   string
	}{
		{"duplicate JSON key", `{"openapi":"3.1.0","openapi":"3.0.3"}`, "duplicate object key"},
		{"trailing document", base + "\n---\n{}\n", "multiple documents"},
		{"external schema ref", strings.Replace(base, `#/components/schemas/Widget`, `https://schemas.example.test/widget.json`, 1), "external or non-schema reference"},
		{"local ref cycle", cycle, "reference cycle"},
		{"duplicate operation id", strings.Replace(base, "operationId: createWidget", "operationId: getWidget", 1), "collides"},
		{"normalized name collision", strings.Replace(base, "operationId: createWidget", "operationId: getwidget", 1), "collides"},
		{"server path", strings.Replace(base, "https://api.example.test", "https://api.example.test/base", 1), "server URL path"},
		{"server encoded authority", strings.Replace(base, "https://api.example.test", "https://api.example.test/%2f%2fevil.test", 1), "server URL path"},
		{"path traversal", strings.Replace(base, "/widgets/{widgetId}:", "/../{widgetId}:", 1), "traversal"},
		{"path encoded authority", strings.Replace(base, "/widgets/{widgetId}:", "/%2f%2fevil/{widgetId}:", 1), "authority"},
		{"query credential", strings.Replace(base, "in: header\n      name: X-Widget-Key", "in: query\n      name: api_key", 1), "must use a header"},
		{"webhooks", strings.Replace(base, "paths:\n", "webhooks:\n  incoming: {}\npaths:\n", 1), "webhooks"},
		{"missing basic username", strings.Replace(base, "      x-hadron-basic-username: service-account\n", "", 1), "requires a bounded fixed"},
		{"invalid credential header token", strings.Replace(base, "name: X-Widget-Key", "name: Bad:Header", 1), "invalid header"},
		{"api key cookie header", strings.Replace(base, "name: X-Widget-Key", "name: Cookie", 1), "invalid header"},
		{"api key set-cookie header", strings.Replace(base, "name: X-Widget-Key", "name: Set-Cookie", 1), "invalid header"},
		{"api key proxy authorization header", strings.Replace(base, "name: X-Widget-Key", "name: Proxy-Authorization", 1), "invalid header"},
		{"api key authorization header", strings.Replace(base, "name: X-Widget-Key", "name: Authorization", 1), "invalid header"},
		{"credential header collision", strings.Replace(base, "      operationId: createWidget\n", "      operationId: createWidget\n      parameters:\n        - name: x-widget-key\n          in: header\n          schema:\n            type: string\n", 1), "collides with credential"},
		{"invalid normal header token", strings.Replace(base, "      operationId: createWidget\n", "      operationId: createWidget\n      parameters:\n        - name: Bad:Header\n          in: header\n          schema:\n            type: string\n", 1), "reserved, sensitive, or invalid"},
		{"sensitive normal query", strings.Replace(base, "name: revision", "name: access_token", 1), "sensitive or invalid"},
		{"client secret query", strings.Replace(base, "name: revision", "name: client_secret", 1), "sensitive or invalid"},
		{"auth token header", strings.Replace(base, "      operationId: createWidget\n", "      operationId: createWidget\n      parameters:\n        - name: X-Auth-Token\n          in: header\n          schema:\n            type: string\n", 1), "reserved, sensitive, or invalid"},
		{"access token header", strings.Replace(base, "      operationId: createWidget\n", "      operationId: createWidget\n      parameters:\n        - name: X-Access-Token\n          in: header\n          schema:\n            type: string\n", 1), "reserved, sensitive, or invalid"},
		{"safe method consequential effect", strings.Replace(base, "      operationId: getWidget\n", "      operationId: getWidget\n      x-hadron-effects:\n        - mutate\n", 1), "safe HTTP method"},
		{"HEAD JSON output", strings.Replace(base, "    get:\n      operationId: getWidget", "    head:\n      operationId: getWidget", 1), "HEAD operations"},
		{"no-content JSON output", strings.Replace(base, `"200":`, `"204":`, 1), "cannot satisfy a JSON body"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := generatedapi.GenerateOpenAPI(t.Context(), []byte(test.source), generatedapi.Options{Namespace: "widgets", HTTP: adapter})
			if !errors.Is(err, generatedapi.ErrInvalidSource) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("GenerateOpenAPI() error = %v, want %q", err, test.want)
			}
		})
	}
	oversized := make([]byte, generatedapi.DefaultMaxSpecBytes+1)
	if _, err := generatedapi.GenerateOpenAPI(t.Context(), oversized, generatedapi.Options{Namespace: "widgets", HTTP: adapter}); !errors.Is(err, generatedapi.ErrInvalidSource) {
		t.Fatalf("oversized GenerateOpenAPI() error = %v", err)
	}
	if _, err := generatedapi.GenerateOpenAPI(t.Context(), deepReferenceSource(t, 70, false), generatedapi.Options{Namespace: "bounds", HTTP: adapter}); !errors.Is(err, generatedapi.ErrInvalidSource) || !strings.Contains(err.Error(), "depth bound") {
		t.Fatalf("deep-reference GenerateOpenAPI() error = %v", err)
	}
	if _, err := generatedapi.GenerateOpenAPI(t.Context(), deepReferenceSource(t, 18, true), generatedapi.Options{Namespace: "bounds", HTTP: adapter}); !errors.Is(err, generatedapi.ErrInvalidSource) || !strings.Contains(err.Error(), "structural size bound") {
		t.Fatalf("branching-reference GenerateOpenAPI() error = %v", err)
	}
	version30 := strings.Replace(base, "openapi: 3.1.0", "openapi: 3.0.3", 1)
	if _, err := generatedapi.GenerateOpenAPI(t.Context(), []byte(version30), generatedapi.Options{Namespace: "widgets", HTTP: adapter}); err != nil {
		t.Fatalf("OpenAPI 3.0 GenerateOpenAPI() error = %v", err)
	}
	benignAuthorHeader := strings.Replace(base, "      operationId: createWidget\n", "      operationId: createWidget\n      parameters:\n        - name: X-Document-Author\n          in: header\n          schema:\n            type: string\n", 1)
	if _, err := generatedapi.GenerateOpenAPI(t.Context(), []byte(benignAuthorHeader), generatedapi.Options{Namespace: "widgets", HTTP: adapter}); err != nil {
		t.Fatalf("benign author header GenerateOpenAPI() error = %v", err)
	}
}

func TestGeneratedKindRejectsOverridesAndCredentialMasquerade(t *testing.T) {
	adapter, _ := newHTTPAdapter(t, nil)
	family := generateFixture(t, readFixture(t), adapter)
	var get stepkind.StepKind
	for _, kind := range family.Kinds() {
		if kind.(*generatedapi.Kind).Description().OperationID == "getWidget" {
			get = kind
		}
	}
	for _, field := range []string{"url", "method", "path", "auth", "expected_json_schema", "effects", "capabilities", "idempotency_key"} {
		t.Run(field, func(t *testing.T) {
			config := graph.Config{field: "forged"}
			if findings := get.ValidateConfig(t.Context(), config); len(findings) != 1 {
				t.Fatalf("ValidateConfig(%s) = %+v", field, findings)
			}
		})
	}
	forgedKey := invocation(t, values.ValueSet{
		"path.widgetid":         inline(t, "widget-1"),
		"credential.bearerauth": secretRef(t, "secret://test/bearer"),
	})
	forgedKey.Invocation.Config = graph.Config{"idempotency_key": "forged"}
	forgedKey.Invocation.IdempotencyKey = "runtime-bound"
	_, err := get.Execute(t.Context(), forgedKey)
	var execution *stepkind.ExecutionError
	if !errors.As(err, &execution) || execution.Code != "generated_api_invalid_config" {
		t.Fatalf("forged config idempotency key Execute() error = %T %v", err, err)
	}
	inputs := values.ValueSet{
		"path.widgetid":         inline(t, "widget-1"),
		"credential.bearerauth": inline(t, "secret://test/bearer"),
	}
	_, err = get.Execute(t.Context(), invocation(t, inputs))
	if !errors.As(err, &execution) || execution.Code != "generated_api_invalid_inputs" {
		t.Fatalf("inline credential Execute() error = %T %v", err, err)
	}
	badPath := values.ValueSet{
		"path.widgetid":         inline(t, "../admin"),
		"credential.bearerauth": secretRef(t, "secret://test/bearer"),
	}
	_, err = get.Execute(t.Context(), invocation(t, badPath))
	if !errors.As(err, &execution) || execution.Code != "generated_api_invalid_inputs" {
		t.Fatalf("traversal input Execute() error = %T %v", err, err)
	}
}

func newHTTPAdapter(t *testing.T, injected any) (*httpadapter.Kind, *allowPolicy) {
	t.Helper()
	policy := &allowPolicy{}
	var secrets values.SecretResolver
	if value, ok := injected.(struct {
		policy  *allowPolicy
		secrets values.SecretResolver
	}); ok {
		policy, secrets = value.policy, value.secrets
	}
	adapter, err := httpadapter.New(httpadapter.Options{Policy: policy, Secrets: secrets})
	if err != nil {
		t.Fatalf("http.New() error = %v", err)
	}
	return adapter, policy
}

func readFixture(t *testing.T) []byte {
	t.Helper()
	source, err := os.ReadFile("testdata/widgets.openapi.yaml")
	if err != nil {
		t.Fatalf("ReadFile(fixture) error = %v", err)
	}
	return source
}

func generateFixture(t *testing.T, source []byte, adapter *httpadapter.Kind) *generatedapi.Family {
	t.Helper()
	family, err := generatedapi.GenerateOpenAPI(t.Context(), source, generatedapi.Options{Namespace: "widgets", HTTP: adapter})
	if err != nil {
		t.Fatalf("GenerateOpenAPI() error = %v", err)
	}
	return family
}

func invocation(t *testing.T, inputs values.ValueSet) stepkind.PreparedInvocation {
	t.Helper()
	return stepkind.PreparedInvocation{Invocation: stepkind.Invocation{
		Identity: stepkind.InvocationIdentity{RunID: "run-generated", NodeID: "generated-call", Attempt: 1},
		Config:   graph.Config{}, Inputs: inputs,
	}}
}

func inline(t *testing.T, payload any) values.Value {
	t.Helper()
	value, err := values.NewInline(payload, values.Metadata{
		Producer: values.Producer{Kind: "test", Reference: "generated-input"}, MediaType: "application/json",
		Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		t.Fatalf("NewInline() error = %v", err)
	}
	return value
}

func secretRef(t *testing.T, raw string) values.Value {
	t.Helper()
	ref, err := values.ParseSecretRef(raw)
	if err != nil {
		t.Fatalf("ParseSecretRef() error = %v", err)
	}
	value, err := values.NewSecretRef(ref, values.Metadata{
		Producer: values.Producer{Kind: "test", Reference: "generated-credential"}, MediaType: "application/json",
		Redaction: values.RedactionSecret, Retention: values.RetentionRun,
	})
	if err != nil {
		t.Fatalf("NewSecretRef() error = %v", err)
	}
	return value
}

func assertBody(t *testing.T, result stepkind.StepResult, id string, revision json.Number) {
	t.Helper()
	if result.Outcome != stepkind.StepCompleted {
		t.Fatalf("outcome = %q", result.Outcome)
	}
	body := result.Outputs[httpadapter.OutputBodyJSON].Inline.(map[string]any)
	if body["id"] != id || body["revision"] != revision {
		t.Fatalf("body_json = %#v", body)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func deepReferenceSource(t *testing.T, depth int, branching bool) []byte {
	t.Helper()
	components := make(map[string]any, depth+1)
	for index := depth; index >= 0; index-- {
		name := fmt.Sprintf("S%d", index)
		if index == depth {
			components[name] = map[string]any{"type": "string"}
			continue
		}
		reference := map[string]any{"$ref": fmt.Sprintf("#/components/schemas/S%d", index+1)}
		if branching {
			components[name] = map[string]any{
				"type": "object", "properties": map[string]any{"left": reference, "right": reference},
			}
		} else {
			components[name] = reference
		}
	}
	document := map[string]any{
		"openapi": "3.1.0", "info": map[string]any{"title": "Bound", "version": "v1"},
		"servers": []any{map[string]any{"url": "https://api.example.test"}}, "security": []any{},
		"paths": map[string]any{"/bound": map[string]any{"get": map[string]any{
			"operationId": "getBound", "responses": map[string]any{"200": map[string]any{
				"description": "bounded", "content": map[string]any{"application/json": map[string]any{
					"schema": map[string]any{"$ref": "#/components/schemas/S0"},
				}},
			}},
		}}},
		"components": map[string]any{"schemas": components},
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatalf("Marshal(deep source) error = %v", err)
	}
	return encoded
}
