package generatedapi

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	maxPaths      = 512
	maxOperations = 1_024
	maxParameters = 256
)

var operationMethods = []string{"delete", "get", "options", "patch", "post", "put"}

type securityScheme struct {
	kind     string
	header   string
	username string
}

type generator struct {
	options       Options
	document      map[string]any
	digest        string
	title         string
	sourceVersion string
	server        *url.URL
	origin        string
	resolver      schemaResolver
	security      map[string]securityScheme
	rootSecurity  any
	names         map[string]string
}

// GenerateOpenAPI generates ordinary first-class step kinds from one bounded
// OpenAPI 3.0 or 3.1 JSON/YAML document.
func GenerateOpenAPI(ctx context.Context, source []byte, options Options) (*Family, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	validated, err := validateOptions(options)
	if err != nil {
		return nil, err
	}
	document, err := decodeSource(source, validated.MaxSpecBytes)
	if err != nil {
		return nil, err
	}
	digest, err := canonicalDigest(document)
	if err != nil {
		return nil, err
	}
	generator, err := newGenerator(document, digest, validated)
	if err != nil {
		return nil, err
	}
	operations, err := generator.generate(ctx)
	if err != nil {
		return nil, err
	}
	return &Family{digest: digest, operations: operations}, nil
}

func newGenerator(document map[string]any, digest string, options Options) (*generator, error) {
	if _, webhooks := document["webhooks"]; webhooks {
		return nil, invalidSource("OpenAPI webhooks are not supported")
	}
	version, err := requiredString(document, "openapi", "document")
	if err != nil || (!strings.HasPrefix(version, "3.0.") && !strings.HasPrefix(version, "3.1.")) {
		return nil, invalidSource("openapi must select version 3.0.x or 3.1.x")
	}
	info, err := objectField(document, "info")
	if err != nil {
		return nil, err
	}
	title, err := requiredString(info, "title", "info")
	if err != nil {
		return nil, err
	}
	sourceVersion, err := requiredString(info, "version", "info")
	if err != nil {
		return nil, err
	}
	server, origin, err := parseRootServer(document)
	if err != nil {
		return nil, err
	}
	components := map[string]any{}
	security := map[string]securityScheme{}
	if rawComponents, ok := document["components"]; ok {
		componentObject, ok := rawComponents.(map[string]any)
		if !ok {
			return nil, invalidSource("components must be an object")
		}
		if rawSchemas, ok := componentObject["schemas"]; ok {
			components, ok = rawSchemas.(map[string]any)
			if !ok {
				return nil, invalidSource("components.schemas must be an object")
			}
		}
		if rawSecurity, ok := componentObject["securitySchemes"]; ok {
			securityObject, ok := rawSecurity.(map[string]any)
			if !ok {
				return nil, invalidSource("components.securitySchemes must be an object")
			}
			security, err = parseSecuritySchemes(securityObject)
			if err != nil {
				return nil, err
			}
		}
	}
	return &generator{
		options: options, document: document, digest: digest, title: title,
		sourceVersion: sourceVersion, server: server, origin: origin,
		resolver: schemaResolver{components: components}, security: security,
		rootSecurity: document["security"], names: make(map[string]string),
	}, nil
}

func (g *generator) generate(ctx context.Context) ([]*operation, error) {
	paths, err := objectField(g.document, "paths")
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 || len(paths) > maxPaths {
		return nil, invalidSource("paths must contain between 1 and %d entries", maxPaths)
	}
	pathNames := make([]string, 0, len(paths))
	for path := range paths {
		pathNames = append(pathNames, path)
	}
	sort.Strings(pathNames)
	var result []*operation
	for _, path := range pathNames {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := validatePathTemplate(path); err != nil {
			return nil, err
		}
		pathItem, ok := paths[path].(map[string]any)
		if !ok {
			return nil, invalidSource("path %q must be an object", path)
		}
		if _, hasRef := pathItem["$ref"]; hasRef {
			return nil, invalidSource("path-item references are not supported")
		}
		if _, hasServers := pathItem["servers"]; hasServers {
			return nil, invalidSource("path-level servers are not supported")
		}
		pathParameters, err := g.parseParameters(pathItem["parameters"], "path-item "+path)
		if err != nil {
			return nil, err
		}
		for _, methodName := range operationMethods {
			rawOperation, exists := pathItem[methodName]
			if !exists {
				continue
			}
			operationObject, ok := rawOperation.(map[string]any)
			if !ok {
				return nil, invalidSource("operation %s %s must be an object", strings.ToUpper(methodName), path)
			}
			operation, err := g.generateOperation(strings.ToUpper(methodName), path, pathParameters, operationObject)
			if err != nil {
				return nil, err
			}
			result = append(result, operation)
			if len(result) > maxOperations {
				return nil, invalidSource("document exceeds %d-operation bound", maxOperations)
			}
		}
		if _, trace := pathItem["trace"]; trace {
			return nil, invalidSource("TRACE operations are not supported")
		}
		if _, head := pathItem["head"]; head {
			return nil, invalidSource("HEAD operations cannot satisfy the generated JSON output contract")
		}
	}
	if len(result) == 0 {
		return nil, invalidSource("paths contain no supported operations")
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].description.Name < result[right].description.Name
	})
	return result, nil
}

func (g *generator) generateOperation(method, path string, inherited []parameter, raw map[string]any) (*operation, error) {
	if _, hasServers := raw["servers"]; hasServers {
		return nil, invalidSource("operation-level servers are not supported")
	}
	if _, callbacks := raw["callbacks"]; callbacks {
		return nil, invalidSource("callbacks are not supported")
	}
	operationID, err := requiredString(raw, "operationId", method+" "+path)
	if err != nil {
		return nil, err
	}
	normalizedID := graph.NormalizeID(operationID)
	if identityErr := graph.ValidateID(normalizedID); identityErr != nil {
		return nil, invalidSource("operationId %q cannot produce a graph-valid identity", operationID)
	}
	name := "openapi-" + g.options.Namespace + "-" + normalizedID
	if identityErr := graph.ValidateID(name); identityErr != nil {
		return nil, invalidSource("generated kind name for operationId %q: %v", operationID, identityErr)
	}
	if prior, collision := g.names[name]; collision {
		return nil, invalidSource("operationId %q collides with %q as generated name %q", operationID, prior, name)
	}
	g.names[name] = operationID
	parameters, err := g.parseParameters(raw["parameters"], method+" "+path)
	if err != nil {
		return nil, err
	}
	parameters, err = mergeParameters(inherited, parameters)
	if err != nil {
		return nil, invalidSource("operation %q: %v", operationID, err)
	}
	if pathErr := requirePathParameters(path, parameters); pathErr != nil {
		return nil, invalidSource("operation %q: %v", operationID, pathErr)
	}
	bodyInput, bodySchema, bodyRequired, err := g.parseRequestBody(raw["requestBody"])
	if err != nil {
		return nil, invalidSource("operation %q: %v", operationID, err)
	}
	statuses, responseSchema, err := g.parseResponses(raw["responses"])
	if err != nil {
		return nil, invalidSource("operation %q: %v", operationID, err)
	}
	securityValue, hasOperationSecurity := raw["security"]
	if !hasOperationSecurity {
		securityValue = g.rootSecurity
	}
	credential, err := g.resolveSecurity(securityValue)
	if err != nil {
		return nil, invalidSource("operation %q: %v", operationID, err)
	}
	if collisionErr := rejectCredentialHeaderCollision(parameters, credential); collisionErr != nil {
		return nil, invalidSource("operation %q: %v", operationID, collisionErr)
	}
	effects, err := generatedEffects(method, raw["x-hadron-effects"])
	if err != nil {
		return nil, invalidSource("operation %q: %v", operationID, err)
	}
	capabilities, err := generatedCapabilities(raw["x-hadron-capabilities"], credential != nil)
	if err != nil {
		return nil, invalidSource("operation %q: %v", operationID, err)
	}
	inputSchema := buildInputSchema(parameters, bodyInput, bodySchema, bodyRequired, credential)
	outputSchema := buildOutputSchema(statuses, responseSchema)
	configSchema := generatedConfigSchema()
	for field, schema := range map[string]graph.Schema{"config": configSchema, "input": inputSchema, "output": outputSchema} {
		if schemaErr := values.ValidateSchema(schema); schemaErr != nil {
			return nil, invalidSource("operation %q generated invalid %s schema: %v", operationID, field, schemaErr)
		}
	}
	version := "gen-" + strings.TrimPrefix(g.digest, "sha256:")
	description := OperationDescription{
		SourceFamily: SourceFamilyOpenAPI, SourceDigest: g.digest, SourceTitle: g.title,
		SourceVersion: g.sourceVersion, Name: name, Version: version, OperationID: operationID,
		Method: method, Origin: g.origin, PathTemplate: path,
		ConfigSchema: configSchema, InputSchema: inputSchema, OutputSchema: outputSchema,
		Effects: effects, RequiredCapabilities: capabilities, SuccessStatuses: statuses,
	}
	if credential != nil {
		description.Credentials = []CredentialDescription{{
			Input: credential.InputName, Scheme: credential.SourceName, Kind: credential.Kind,
			Header: credential.Header, Username: credential.Username,
		}}
	}
	return &operation{
		http: g.options.HTTP, description: description, server: g.server.String(), parameters: parameters,
		bodyInput: bodyInput, credential: credential, responseSchema: responseSchema,
	}, nil
}

func parseRootServer(document map[string]any) (*url.URL, string, error) {
	raw, ok := document["servers"].([]any)
	if !ok || len(raw) != 1 {
		return nil, "", invalidSource("servers must contain exactly one fixed server")
	}
	server, ok := raw[0].(map[string]any)
	if !ok {
		return nil, "", invalidSource("server must be an object")
	}
	if _, variables := server["variables"]; variables {
		return nil, "", invalidSource("server variables are not supported")
	}
	rawURL, err := requiredString(server, "url", "servers[0]")
	if err != nil {
		return nil, "", err
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Opaque != "" || !parsed.IsAbs() {
		return nil, "", invalidSource("server URL must be absolute HTTP(S)")
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || !validServerHost(host) {
		return nil, "", invalidSource("server URL must be absolute HTTP(S)")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.ForceQuery || strings.Contains(rawURL, "{") {
		return nil, "", invalidSource("server URL cannot contain credentials, query, fragment, or variables")
	}
	if parsed.Path != "" && parsed.Path != "/" || parsed.RawPath != "" {
		return nil, "", invalidSource("server URL path must be empty or root")
	}
	port := parsed.Port()
	if port == "" {
		if parsed.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	} else if value, parseErr := strconv.ParseUint(port, 10, 16); parseErr != nil || value == 0 {
		return nil, "", invalidSource("server URL port is invalid")
	}
	parsed.Path, parsed.RawPath = "", ""
	parsed.Host = net.JoinHostPort(host, port)
	origin := parsed.Scheme + "://" + parsed.Host
	return parsed, origin, nil
}

func validServerHost(host string) bool {
	if address, err := netip.ParseAddr(host); err == nil {
		return address.Zone() == ""
	}
	if host == "" || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for index := 0; index < len(label); index++ {
			character := label[index]
			if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '-' {
				continue
			}
			return false
		}
	}
	return true
}

func parseSecuritySchemes(raw map[string]any) (map[string]securityScheme, error) {
	result := make(map[string]securityScheme, len(raw))
	normalized := make(map[string]string)
	for name, value := range raw {
		if !stableText(name) {
			return nil, invalidSource("security scheme name is invalid")
		}
		inputName := graph.NormalizeID(name)
		if err := graph.ValidateID(inputName); err != nil {
			return nil, invalidSource("security scheme %q cannot produce a stable input name", name)
		}
		if prior, duplicate := normalized[inputName]; duplicate {
			return nil, invalidSource("security schemes %q and %q normalize to the same input", prior, name)
		}
		normalized[inputName] = name
		object, ok := value.(map[string]any)
		if !ok {
			return nil, invalidSource("security scheme %q must be an object", name)
		}
		kind, err := requiredString(object, "type", "security scheme "+name)
		if err != nil {
			return nil, err
		}
		scheme := securityScheme{}
		switch kind {
		case "http":
			httpScheme, err := requiredString(object, "scheme", "security scheme "+name)
			if err != nil {
				return nil, err
			}
			switch strings.ToLower(httpScheme) {
			case "bearer":
				scheme.kind = "bearer"
			case "basic":
				username, err := requiredString(object, "x-hadron-basic-username", "security scheme "+name)
				if err != nil || len(username) > 256 || strings.Contains(username, ":") {
					return nil, invalidSource("basic security scheme %q requires a bounded fixed x-hadron-basic-username without a colon", name)
				}
				scheme.kind, scheme.username = "basic", username
			default:
				return nil, invalidSource("security scheme %q supports only HTTP bearer or basic", name)
			}
		case "apiKey":
			location, err := requiredString(object, "in", "security scheme "+name)
			if err != nil || location != "header" {
				return nil, invalidSource("API key security scheme %q must use a header", name)
			}
			header, err := requiredString(object, "name", "security scheme "+name)
			if err != nil || !safeCredentialHeader(header) {
				return nil, invalidSource("API key security scheme %q has an invalid header", name)
			}
			scheme.kind, scheme.header = "header", http.CanonicalHeaderKey(header)
		default:
			return nil, invalidSource("security scheme %q type %q is outside the portable subset", name, kind)
		}
		result[name] = scheme
	}
	return result, nil
}

func (g *generator) resolveSecurity(raw any) (*credential, error) {
	if raw == nil {
		return nil, nil
	}
	alternatives, ok := raw.([]any)
	if !ok {
		return nil, invalidSource("security must be an array")
	}
	if len(alternatives) == 0 {
		return nil, nil
	}
	if len(alternatives) != 1 {
		return nil, invalidSource("security alternatives are not supported")
	}
	requirement, ok := alternatives[0].(map[string]any)
	if !ok || len(requirement) != 1 {
		return nil, invalidSource("security must require exactly one scheme")
	}
	for name, rawScopes := range requirement {
		scopes, ok := rawScopes.([]any)
		if !ok || len(scopes) != 0 {
			return nil, invalidSource("security scheme %q cannot require scopes", name)
		}
		scheme, exists := g.security[name]
		if !exists {
			return nil, invalidSource("security scheme %q was not found", name)
		}
		return &credential{
			SourceName: name, InputName: "credential." + graph.NormalizeID(name), Kind: scheme.kind,
			Header: scheme.header, Username: scheme.username,
		}, nil
	}
	return nil, nil
}

func generatedEffects(method string, extension any) (graph.EffectSet, error) {
	result := graph.EffectSet{graph.EffectRead}
	if method != "GET" && method != "HEAD" && method != "OPTIONS" {
		result = graph.EffectSet{graph.EffectRead, graph.EffectMaterialize, graph.EffectMutate, graph.EffectDestructive}
	}
	if extension != nil {
		items, ok := extension.([]any)
		if !ok || len(items) == 0 || len(items) > 5 {
			return nil, invalidSource("x-hadron-effects must be a non-empty bounded array")
		}
		for _, item := range items {
			text, ok := item.(string)
			effect := graph.Effect(text)
			if !ok || !effect.Valid() {
				return nil, invalidSource("x-hadron-effects contains unsupported effect")
			}
			if !containsEffect(result, effect) {
				result = append(result, effect)
			}
		}
	}
	if method == http.MethodGet || method == http.MethodHead || method == http.MethodOptions {
		for _, effect := range result {
			if effect == graph.EffectMaterialize || effect == graph.EffectMutate || effect == graph.EffectDestructive {
				return nil, invalidSource("safe HTTP method cannot declare materialize, mutate, or destructive effects")
			}
		}
	}
	sort.Slice(result, func(left, right int) bool { return result[left] < result[right] })
	return result, nil
}

func generatedCapabilities(extension any, hasCredential bool) ([]string, error) {
	result := []string{"network.http"}
	if hasCredential {
		result = append(result, "secrets.resolve")
	}
	if extension != nil {
		items, ok := extension.([]any)
		if !ok || len(items) > 64 {
			return nil, invalidSource("x-hadron-capabilities must be a bounded array")
		}
		for _, item := range items {
			text, ok := item.(string)
			if !ok || !stableText(text) || len(text) > 128 {
				return nil, invalidSource("x-hadron-capabilities contains an invalid capability")
			}
			result = append(result, text)
		}
	}
	sort.Strings(result)
	for index := 1; index < len(result); index++ {
		if result[index] == result[index-1] {
			return nil, invalidSource("duplicate generated capability %q", result[index])
		}
	}
	return result, nil
}

func generatedConfigSchema() graph.Schema {
	return graph.Schema{
		"type": "object", "additionalProperties": false,
		"properties": map[string]any{
			"timeout": map[string]any{"type": "string", "minLength": json.Number("1")},
			"max_response_bytes": map[string]any{
				"type": "integer", "minimum": json.Number("1"), "maximum": json.Number(strconv.FormatInt(defaultMaxResponse, 10)),
			},
		},
	}
}

func buildInputSchema(parameters []parameter, bodyInput string, bodySchema graph.Schema, bodyRequired bool, credential *credential) graph.Schema {
	properties := make(map[string]any, len(parameters)+2)
	var required []string
	for _, parameter := range parameters {
		properties[parameter.InputName] = cloneSchema(parameter.Schema)
		if parameter.Required {
			required = append(required, parameter.InputName)
		}
	}
	if bodyInput != "" {
		properties[bodyInput] = cloneSchema(bodySchema)
		if bodyRequired {
			required = append(required, bodyInput)
		}
	}
	if credential != nil {
		properties[credential.InputName] = map[string]any{"type": "secret_ref"}
		required = append(required, credential.InputName)
	}
	sort.Strings(required)
	result := graph.Schema{"type": "object", "additionalProperties": false, "properties": properties}
	if len(required) != 0 {
		items := make([]any, len(required))
		for index, value := range required {
			items[index] = value
		}
		result["required"] = items
	}
	return result
}

func buildOutputSchema(statuses []int, response graph.Schema) graph.Schema {
	statusValues := make([]any, len(statuses))
	for index, status := range statuses {
		statusValues[index] = json.Number(strconv.Itoa(status))
	}
	return graph.Schema{
		"type": "object", "additionalProperties": false,
		"required": []any{"status", "headers", "body", "body_json", "request_metadata"},
		"properties": map[string]any{
			"status":           map[string]any{"type": "integer", "enum": statusValues},
			"headers":          map[string]any{"type": "object"},
			"body":             map[string]any{"oneOf": []any{map[string]any{"type": "string"}, map[string]any{"type": "artifact"}}},
			"body_json":        cloneSchema(response),
			"request_metadata": map[string]any{"type": "object"},
		},
	}
}

func digestNative(value any) (string, error) { return values.DigestInline(value) }

func safeCredentialHeader(name string) bool {
	if !validHeaderToken(name) {
		return false
	}
	canonical := http.CanonicalHeaderKey(name)
	lower := strings.ToLower(canonical)
	forbidden := map[string]bool{
		"connection": true, "content-length": true, "host": true, "proxy-connection": true,
		"te": true, "trailer": true, "transfer-encoding": true, "upgrade": true, "idempotency-key": true,
		"authorization": true, "proxy-authorization": true, "cookie": true, "set-cookie": true,
		"www-authenticate": true, "proxy-authenticate": true, "authentication-info": true,
	}
	return !forbidden[lower]
}
