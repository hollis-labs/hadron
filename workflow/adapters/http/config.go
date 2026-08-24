package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime"
	"net"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	defaultTimeout           = 30 * time.Second
	maximumTimeout           = 24 * time.Hour
	defaultMaxResponseBytes  = int64(8 << 20)
	maximumMaxResponseBytes  = int64(64 << 20)
	defaultMaxRedirects      = 5
	maximumMaxRedirects      = 20
	defaultMaxResponseHeader = int64(256 << 10)
)

var conservativeEffects = graph.EffectSet{
	graph.EffectRead,
	graph.EffectMaterialize,
	graph.EffectMutate,
	graph.EffectDestructive,
}

// RedirectMode controls whether the adapter can follow a redirect. Policy
// mode still requires destination authorization for every address of every
// hop; it is not an allow-by-configuration switch.
type RedirectMode string

const (
	RedirectDeny       RedirectMode = "deny"
	RedirectSameOrigin RedirectMode = "same_origin"
	RedirectPolicy     RedirectMode = "policy"
)

func (m RedirectMode) valid() bool {
	return m == RedirectDeny || m == RedirectSameOrigin || m == RedirectPolicy
}

type authConfig struct {
	Kind      string
	SecretRef values.SecretRef
	Username  string
	Header    string
}

type redirectConfig struct {
	Mode               RedirectMode
	MaxHops            int
	AllowMethodRewrite bool
}

type config struct {
	Method                string
	URL                   *url.URL
	Origin                string
	Headers               map[string]string
	Body                  any
	HasBody               bool
	Auth                  *authConfig
	Timeout               time.Duration
	MaxResponseBytes      int64
	InlineLimit           int64
	ExpectedStatuses      map[int]struct{}
	ExpectedContentTypes  []string
	ExpectedJSONSchema    graph.Schema
	HasExpectedJSONSchema bool
	Redirects             redirectConfig
	IdempotencyKey        string
	Effects               graph.EffectSet
	Capabilities          []string
	HasSecretReferences   bool
	HasBodySecretRefs     bool
	SecretRequestHeaders  map[string]struct{}
}

func configSchema() graph.Schema {
	return graph.Schema{
		"type": "object", "additionalProperties": false,
		"required": []any{"url"},
		"properties": map[string]any{
			"method":  map[string]any{"type": "string", "enum": []any{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}},
			"url":     map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("8192")},
			"headers": map[string]any{"type": "object", "maxProperties": json.Number("256"), "additionalProperties": map[string]any{"type": "string"}},
			"body":    map[string]any{},
			"auth": map[string]any{"oneOf": []any{
				map[string]any{"type": "object", "additionalProperties": false, "required": []any{"type", "secret_ref"}, "properties": map[string]any{"type": map[string]any{"const": "bearer"}, "secret_ref": map[string]any{"type": "string", "minLength": json.Number("1")}}},
				map[string]any{"type": "object", "additionalProperties": false, "required": []any{"type", "secret_ref", "username"}, "properties": map[string]any{"type": map[string]any{"const": "basic"}, "secret_ref": map[string]any{"type": "string", "minLength": json.Number("1")}, "username": map[string]any{"type": "string", "minLength": json.Number("1")}}},
				map[string]any{"type": "object", "additionalProperties": false, "required": []any{"type", "secret_ref", "header"}, "properties": map[string]any{"type": map[string]any{"const": "header"}, "secret_ref": map[string]any{"type": "string", "minLength": json.Number("1")}, "header": map[string]any{"type": "string", "minLength": json.Number("1")}}},
			}},
			"timeout":                map[string]any{"type": "string", "minLength": json.Number("1")},
			"max_response_bytes":     map[string]any{"type": "integer", "minimum": json.Number("1"), "maximum": json.Number(strconv.FormatInt(maximumMaxResponseBytes, 10))},
			"inline_limit":           map[string]any{"type": "integer", "minimum": json.Number("1"), "maximum": json.Number(strconv.FormatInt(values.MaximumInlineLimit, 10))},
			"expected_status":        map[string]any{"type": "array", "minItems": json.Number("1"), "maxItems": json.Number("500"), "uniqueItems": true, "items": map[string]any{"type": "integer", "minimum": json.Number("100"), "maximum": json.Number("599")}},
			"expected_content_types": map[string]any{"type": "array", "minItems": json.Number("1"), "maxItems": json.Number("64"), "uniqueItems": true, "items": map[string]any{"type": "string", "minLength": json.Number("3")}},
			"expected_json_schema":   map[string]any{},
			"redirects": map[string]any{"type": "object", "additionalProperties": false, "properties": map[string]any{
				"mode":                 map[string]any{"type": "string", "enum": []any{"deny", "same_origin", "policy"}},
				"max_hops":             map[string]any{"type": "integer", "minimum": json.Number("1"), "maximum": json.Number(strconv.Itoa(maximumMaxRedirects))},
				"allow_method_rewrite": map[string]any{"type": "boolean"},
			}},
			"idempotency_key": map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("512")},
			"effects": map[string]any{"type": "array", "minItems": json.Number("1"), "maxItems": json.Number("5"), "uniqueItems": true,
				"items": map[string]any{"type": "string", "enum": []any{"read", "compute", "materialize", "mutate", "destructive"}}},
			"capabilities": map[string]any{"type": "array", "maxItems": json.Number("64"), "uniqueItems": true,
				"items": map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("128")}},
		},
	}
}

func outputSchema() graph.Schema {
	return graph.Schema{
		"type": "object", "additionalProperties": false,
		"required": []any{OutputStatus, OutputHeaders, OutputBody, OutputMetadata},
		"properties": map[string]any{
			OutputStatus:   map[string]any{"type": "integer"},
			OutputHeaders:  map[string]any{"type": "object"},
			OutputBody:     map[string]any{"oneOf": []any{map[string]any{"type": "string"}, map[string]any{"type": "artifact"}}},
			OutputBodyJSON: map[string]any{},
			OutputMetadata: map[string]any{"type": "object"},
		},
	}
}

func parseConfig(input graph.Config) (config, error) {
	if input == nil {
		return config{}, fmt.Errorf("%w: config must be an object", ErrInvalidConfig)
	}
	if _, err := values.DigestInline(map[string]any(input)); err != nil {
		return config{}, fmt.Errorf("%w: config must be native unambiguous JSON: %w", ErrInvalidConfig, err)
	}
	raw, err := normalizeObject(input)
	if err != nil {
		return config{}, err
	}
	allowed := map[string]bool{
		"method": true, "url": true, "headers": true, "body": true, "auth": true,
		"timeout": true, "max_response_bytes": true, "inline_limit": true,
		"expected_status": true, "expected_content_types": true, "expected_json_schema": true,
		"redirects": true, "idempotency_key": true, "effects": true, "capabilities": true,
	}
	keys := sortedKeys(raw)
	for _, key := range keys {
		if !allowed[key] {
			return config{}, fmt.Errorf("%w: unknown field %q", ErrInvalidConfig, key)
		}
	}

	parsed := config{
		Method:               "GET",
		Headers:              make(map[string]string),
		Timeout:              defaultTimeout,
		MaxResponseBytes:     defaultMaxResponseBytes,
		InlineLimit:          values.DefaultInlineLimit,
		ExpectedStatuses:     make(map[int]struct{}),
		Redirects:            redirectConfig{Mode: RedirectDeny, MaxHops: defaultMaxRedirects},
		SecretRequestHeaders: make(map[string]struct{}),
		Capabilities:         []string{"network.http"},
	}
	if value, ok := raw["method"]; ok {
		text, ok := value.(string)
		if !ok {
			return config{}, fmt.Errorf("%w: method must be a string", ErrInvalidConfig)
		}
		parsed.Method = text
	}
	if parsed.Method != strings.ToUpper(parsed.Method) || !validMethod(parsed.Method) {
		return config{}, fmt.Errorf("%w: unsupported method", ErrInvalidConfig)
	}
	rawURL, ok := raw["url"].(string)
	if !ok || rawURL == "" {
		return config{}, fmt.Errorf("%w: url must be a non-empty string", ErrInvalidConfig)
	}
	parsed.URL, parsed.Origin, err = normalizeURL(rawURL)
	if err != nil {
		return config{}, err
	}
	if queryErr := validateURLQuery(parsed.URL, true); queryErr != nil {
		return config{}, queryErr
	}

	if value, ok := raw["headers"]; ok {
		headers, ok := value.(map[string]any)
		if !ok {
			return config{}, fmt.Errorf("%w: headers must be an object", ErrInvalidConfig)
		}
		if len(headers) > 256 {
			return config{}, fmt.Errorf("%w: headers cannot contain more than 256 fields", ErrInvalidConfig)
		}
		headerBytes := 0
		for _, name := range sortedKeys(headers) {
			text, ok := headers[name].(string)
			if !ok {
				return config{}, fmt.Errorf("%w: header %q must be a string", ErrInvalidConfig, name)
			}
			canonical, secret, headerErr := validateHeader(name, text)
			if headerErr != nil {
				return config{}, headerErr
			}
			if _, exists := parsed.Headers[canonical]; exists {
				return config{}, fmt.Errorf("%w: duplicate header %q", ErrInvalidConfig, canonical)
			}
			parsed.Headers[canonical] = text
			headerBytes += len(canonical) + len(text)
			if headerBytes > int(defaultMaxResponseHeader) {
				return config{}, fmt.Errorf("%w: headers exceed the configured structural bound", ErrInvalidConfig)
			}
			if secret {
				parsed.HasSecretReferences = true
				parsed.SecretRequestHeaders[strings.ToLower(canonical)] = struct{}{}
			}
		}
	}
	if body, ok := raw["body"]; ok {
		parsed.Body, parsed.HasBody = body, true
		parsed.HasBodySecretRefs, err = validateBodySecretRefs(body)
		if err != nil {
			return config{}, err
		}
		parsed.HasSecretReferences = parsed.HasSecretReferences || parsed.HasBodySecretRefs
		encodedBody, encodeErr := json.Marshal(body)
		if encodeErr != nil || int64(len(encodedBody)) > maximumMaxResponseBytes {
			return config{}, fmt.Errorf("%w: body exceeds the request bound", ErrInvalidConfig)
		}
	}
	if value, ok := raw["auth"]; ok {
		parsed.Auth, err = parseAuth(value)
		if err != nil {
			return config{}, err
		}
		parsed.HasSecretReferences = true
		header := authHeader(parsed.Auth)
		if _, exists := parsed.Headers[header]; exists {
			return config{}, fmt.Errorf("%w: auth conflicts with configured header %q", ErrInvalidConfig, header)
		}
		parsed.SecretRequestHeaders[strings.ToLower(header)] = struct{}{}
	}
	if value, ok := raw["timeout"]; ok {
		text, ok := value.(string)
		if !ok {
			return config{}, fmt.Errorf("%w: timeout must be a duration string", ErrInvalidConfig)
		}
		parsed.Timeout, err = time.ParseDuration(text)
		if err != nil || parsed.Timeout <= 0 || parsed.Timeout > maximumTimeout {
			return config{}, fmt.Errorf("%w: timeout must be between 1ns and 24h", ErrInvalidConfig)
		}
	}
	if value, ok := raw["max_response_bytes"]; ok {
		parsed.MaxResponseBytes, err = parseBoundedInt(value, 1, maximumMaxResponseBytes)
		if err != nil {
			return config{}, fmt.Errorf("%w: max_response_bytes: %w", ErrInvalidConfig, err)
		}
	}
	if value, ok := raw["inline_limit"]; ok {
		parsed.InlineLimit, err = parseBoundedInt(value, 1, values.MaximumInlineLimit)
		if err != nil {
			return config{}, fmt.Errorf("%w: inline_limit: %w", ErrInvalidConfig, err)
		}
	}
	if parsed.InlineLimit > parsed.MaxResponseBytes {
		return config{}, fmt.Errorf("%w: inline_limit cannot exceed max_response_bytes", ErrInvalidConfig)
	}
	if value, ok := raw["expected_status"]; ok {
		array, ok := value.([]any)
		if !ok || len(array) == 0 {
			return config{}, fmt.Errorf("%w: expected_status must be a non-empty array", ErrInvalidConfig)
		}
		if len(array) > 500 {
			return config{}, fmt.Errorf("%w: expected_status has too many entries", ErrInvalidConfig)
		}
		for _, item := range array {
			status, parseErr := parseBoundedInt(item, 100, 599)
			if parseErr != nil {
				return config{}, fmt.Errorf("%w: expected_status contains an invalid status", ErrInvalidConfig)
			}
			if _, duplicate := parsed.ExpectedStatuses[int(status)]; duplicate {
				return config{}, fmt.Errorf("%w: expected_status contains a duplicate status", ErrInvalidConfig)
			}
			parsed.ExpectedStatuses[int(status)] = struct{}{}
		}
	}
	if value, ok := raw["expected_content_types"]; ok {
		array, ok := value.([]any)
		if !ok || len(array) == 0 {
			return config{}, fmt.Errorf("%w: expected_content_types must be a non-empty array", ErrInvalidConfig)
		}
		seen := make(map[string]bool)
		if len(array) > 64 {
			return config{}, fmt.Errorf("%w: expected_content_types has too many entries", ErrInvalidConfig)
		}
		for _, item := range array {
			text, ok := item.(string)
			mediaType, parameters, parseErr := mime.ParseMediaType(text)
			if !ok || parseErr != nil || len(parameters) != 0 || mediaType != strings.ToLower(mediaType) {
				return config{}, fmt.Errorf("%w: expected_content_types contains an invalid media type", ErrInvalidConfig)
			}
			if seen[mediaType] {
				return config{}, fmt.Errorf("%w: duplicate expected content type %q", ErrInvalidConfig, mediaType)
			}
			seen[mediaType] = true
			parsed.ExpectedContentTypes = append(parsed.ExpectedContentTypes, mediaType)
		}
		sort.Strings(parsed.ExpectedContentTypes)
	}
	if value, ok := raw["expected_json_schema"]; ok {
		schema, ok := value.(map[string]any)
		if !ok {
			return config{}, fmt.Errorf("%w: expected_json_schema must be an object", ErrInvalidConfig)
		}
		parsed.ExpectedJSONSchema = graph.Schema(schema)
		parsed.HasExpectedJSONSchema = true
		if schemaErr := values.ValidateSchema(parsed.ExpectedJSONSchema); schemaErr != nil {
			return config{}, fmt.Errorf("%w: expected_json_schema: %w", ErrInvalidConfig, schemaErr)
		}
	}
	if value, ok := raw["redirects"]; ok {
		parsed.Redirects, err = parseRedirects(value)
		if err != nil {
			return config{}, err
		}
	}
	if value, ok := raw["idempotency_key"]; ok {
		parsed.IdempotencyKey, ok = value.(string)
		if !ok || validateStableText("idempotency_key", parsed.IdempotencyKey) != nil || len(parsed.IdempotencyKey) > 512 {
			return config{}, fmt.Errorf("%w: idempotency_key is invalid", ErrInvalidConfig)
		}
		if _, exists := parsed.Headers["Idempotency-Key"]; exists {
			return config{}, fmt.Errorf("%w: idempotency_key conflicts with configured header", ErrInvalidConfig)
		}
	}
	if value, ok := raw["effects"]; ok {
		parsed.Effects, err = parseEffects(value)
		if err != nil {
			return config{}, err
		}
	}
	if value, ok := raw["capabilities"]; ok {
		parsed.Capabilities, err = parseCapabilities(value)
		if err != nil {
			return config{}, err
		}
	}
	return parsed, nil
}

func normalizeObject(input map[string]any) (map[string]any, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("%w: config must be JSON-compatible", ErrInvalidConfig)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var result map[string]any
	if err := decoder.Decode(&result); err != nil || result == nil {
		return nil, fmt.Errorf("%w: config must be an object", ErrInvalidConfig)
	}
	return result, nil
}

func sortedKeys(input map[string]any) []string {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func validMethod(method string) bool {
	switch method {
	case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS":
		return true
	default:
		return false
	}
}

func normalizeURL(raw string) (*url.URL, string, error) {
	if validateStableText("url", raw) != nil || len(raw) > 8192 {
		return nil, "", fmt.Errorf("%w: url is invalid", ErrInvalidConfig)
	}
	parsed, err := url.Parse(raw)
	if err != nil || !parsed.IsAbs() || parsed.Opaque != "" || parsed.User != nil || parsed.Fragment != "" {
		return nil, "", fmt.Errorf("%w: url must be an absolute hierarchical URL without userinfo or fragment", ErrInvalidConfig)
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, "", fmt.Errorf("%w: url scheme must be http or https", ErrInvalidConfig)
	}
	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if host == "" || !validURLHost(host) {
		return nil, "", fmt.Errorf("%w: url host is invalid", ErrInvalidConfig)
	}
	portText := parsed.Port()
	if portText == "" {
		if parsed.Scheme == "https" {
			portText = "443"
		} else {
			portText = "80"
		}
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return nil, "", fmt.Errorf("%w: url port is invalid", ErrInvalidConfig)
	}
	parsed.Host = net.JoinHostPort(host, portText)
	if parsed.Path == "" {
		parsed.Path = "/"
	}
	origin := parsed.Scheme + "://" + parsed.Host
	return parsed, origin, nil
}

func validURLHost(host string) bool {
	if strings.Contains(host, ":") {
		_, err := netip.ParseAddr(host)
		return err == nil
	}
	if len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for index := 0; index < len(label); index++ {
			current := label[index]
			if current >= 'a' && current <= 'z' || current >= '0' && current <= '9' || current == '-' {
				continue
			}
			return false
		}
	}
	return true
}

func validateURLQuery(parsed *url.URL, rejectSensitiveNames bool) error {
	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return fmt.Errorf("%w: URL query is malformed", ErrInvalidConfig)
	}
	for key, entries := range query {
		if rejectSensitiveNames {
			normalizedKey := strings.ToLower(strings.NewReplacer("-", "", "_", "", ".", "").Replace(key))
			switch normalizedKey {
			case "token", "accesskey", "apikey", "authorization", "auth", "password", "passwd", "secret", "signature", "sig", "credential":
				return fmt.Errorf("%w: sensitive URL query parameters are not allowed", ErrInvalidConfig)
			}
		}
		if _, err := values.ParseSecretRef(key); err == nil || strings.HasPrefix(key, "secret://") {
			return fmt.Errorf("%w: URL query keys cannot contain secret references", ErrInvalidConfig)
		}
		for _, entry := range entries {
			if _, err := values.ParseSecretRef(entry); err == nil || strings.HasPrefix(entry, "secret://") {
				return fmt.Errorf("%w: URL query values cannot contain secret references", ErrInvalidConfig)
			}
		}
	}
	return nil
}

func parseBoundedInt(value any, minimum, maximum int64) (int64, error) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, fmt.Errorf("must be an integer")
	}
	parsed, err := number.Int64()
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, fmt.Errorf("must be between %d and %d", minimum, maximum)
	}
	return parsed, nil
}

func parseRedirects(value any) (redirectConfig, error) {
	result := redirectConfig{Mode: RedirectDeny, MaxHops: defaultMaxRedirects}
	object, ok := value.(map[string]any)
	if !ok {
		return result, fmt.Errorf("%w: redirects must be an object", ErrInvalidConfig)
	}
	allowed := map[string]bool{"mode": true, "max_hops": true, "allow_method_rewrite": true}
	for _, key := range sortedKeys(object) {
		if !allowed[key] {
			return result, fmt.Errorf("%w: unknown redirects field %q", ErrInvalidConfig, key)
		}
	}
	if value, exists := object["mode"]; exists {
		text, isString := value.(string)
		result.Mode = RedirectMode(text)
		if !isString || !result.Mode.valid() {
			return result, fmt.Errorf("%w: redirect mode is invalid", ErrInvalidConfig)
		}
	}
	if value, exists := object["max_hops"]; exists {
		parsed, err := parseBoundedInt(value, 1, maximumMaxRedirects)
		if err != nil {
			return result, fmt.Errorf("%w: max_hops is invalid", ErrInvalidConfig)
		}
		result.MaxHops = int(parsed)
	}
	if value, exists := object["allow_method_rewrite"]; exists {
		result.AllowMethodRewrite, ok = value.(bool)
		if !ok {
			return result, fmt.Errorf("%w: allow_method_rewrite must be a boolean", ErrInvalidConfig)
		}
	}
	return result, nil
}

func parseEffects(value any) (graph.EffectSet, error) {
	array, ok := value.([]any)
	if !ok || len(array) == 0 {
		return nil, fmt.Errorf("%w: effects must be a non-empty array", ErrInvalidConfig)
	}
	seen := make(map[graph.Effect]bool)
	result := make(graph.EffectSet, 0, len(array))
	for _, item := range array {
		text, ok := item.(string)
		effect := graph.Effect(text)
		if !ok || !effect.Valid() || seen[effect] {
			return nil, fmt.Errorf("%w: effects contains an invalid or duplicate value", ErrInvalidConfig)
		}
		seen[effect] = true
		result = append(result, effect)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result, nil
}

func parseCapabilities(value any) ([]string, error) {
	array, ok := value.([]any)
	if !ok || len(array) > 64 {
		return nil, fmt.Errorf("%w: capabilities must be an array of at most 64 entries", ErrInvalidConfig)
	}
	seen := make(map[string]bool)
	result := make([]string, 0, len(array)+1)
	for _, item := range array {
		text, isString := item.(string)
		if !isString || len(text) > 128 || validateStableText("capability", text) != nil || seen[text] {
			return nil, fmt.Errorf("%w: capabilities contains an invalid or duplicate value", ErrInvalidConfig)
		}
		seen[text] = true
		result = append(result, text)
	}
	if !seen["network.http"] {
		result = append(result, "network.http")
	}
	sort.Strings(result)
	return result, nil
}
