package generatedapi

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

func (g *generator) parseParameters(raw any, owner string) ([]parameter, error) {
	if raw == nil {
		return nil, nil
	}
	items, ok := raw.([]any)
	if !ok || len(items) > maxParameters {
		return nil, invalidSource("%s parameters must be an array with at most %d entries", owner, maxParameters)
	}
	result := make([]parameter, 0, len(items))
	seenSource := make(map[string]bool)
	seenInput := make(map[string]string)
	for index, item := range items {
		object, ok := item.(map[string]any)
		if !ok {
			return nil, invalidSource("%s parameter %d must be an object", owner, index)
		}
		if _, reference := object["$ref"]; reference {
			return nil, invalidSource("parameter references are not supported")
		}
		name, err := requiredString(object, "name", fmt.Sprintf("%s parameter %d", owner, index))
		if err != nil {
			return nil, err
		}
		location, err := requiredString(object, "in", fmt.Sprintf("%s parameter %d", owner, index))
		if err != nil {
			return nil, err
		}
		if location != "path" && location != "query" && location != "header" {
			return nil, invalidSource("parameter %q location %q is outside the portable subset", name, location)
		}
		if location == "header" && !safePlainHeader(name) {
			return nil, invalidSource("header parameter %q is reserved, sensitive, or invalid", name)
		}
		if location == "query" && !safeQueryName(name) {
			return nil, invalidSource("query parameter %q is sensitive or invalid for http@v1", name)
		}
		required := false
		if value, exists := object["required"]; exists {
			required, ok = value.(bool)
			if !ok {
				return nil, invalidSource("parameter %q required must be boolean", name)
			}
		}
		if location == "path" && !required {
			return nil, invalidSource("path parameter %q must be required", name)
		}
		if _, content := object["content"]; content {
			return nil, invalidSource("parameter content is not supported")
		}
		rawSchema, ok := object["schema"]
		if !ok {
			return nil, invalidSource("parameter %q requires a schema", name)
		}
		schema, err := g.resolver.resolve(rawSchema)
		if err != nil {
			return nil, invalidSource("parameter %q: %v", name, err)
		}
		array, err := primitiveSchema(schema, location == "query")
		if err != nil {
			return nil, invalidSource("parameter %q: %v", name, err)
		}
		if err := validateParameterSerialization(object, location, array); err != nil {
			return nil, invalidSource("parameter %q: %v", name, err)
		}
		normalized := graph.NormalizeID(name)
		if err := graph.ValidateID(normalized); err != nil {
			return nil, invalidSource("parameter %q cannot produce a stable input name", name)
		}
		inputName := location + "." + normalized
		sourceKey := parameterKey(location, name)
		if seenSource[sourceKey] {
			return nil, invalidSource("duplicate parameter %s %q", location, name)
		}
		if prior, collision := seenInput[inputName]; collision {
			return nil, invalidSource("parameters %q and %q collide as input %q", prior, name, inputName)
		}
		seenSource[sourceKey], seenInput[inputName] = true, name
		result = append(result, parameter{
			SourceName: name, InputName: inputName, Location: location,
			Required: required, Array: array, Schema: schema,
		})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].InputName < result[right].InputName })
	return result, nil
}

func validateParameterSerialization(raw map[string]any, location string, array bool) error {
	wantStyle := "simple"
	wantExplode := false
	if location == "query" {
		wantStyle, wantExplode = "form", true
	}
	if value, exists := raw["style"]; exists {
		style, ok := value.(string)
		if !ok || style != wantStyle {
			return fmt.Errorf("only %s style is supported", wantStyle)
		}
	}
	if value, exists := raw["explode"]; exists {
		explode, ok := value.(bool)
		if !ok || explode != wantExplode {
			return fmt.Errorf("explode must be %t", wantExplode)
		}
	}
	if value, exists := raw["allowReserved"]; exists && value != false {
		return fmt.Errorf("allowReserved is not supported")
	}
	if array && location != "query" {
		return fmt.Errorf("array serialization is supported only for query parameters")
	}
	return nil
}

func mergeParameters(inherited, operation []parameter) ([]parameter, error) {
	merged := make(map[string]parameter, len(inherited)+len(operation))
	for _, parameter := range inherited {
		merged[parameterKey(parameter.Location, parameter.SourceName)] = parameter
	}
	for _, parameter := range operation {
		merged[parameterKey(parameter.Location, parameter.SourceName)] = parameter
	}
	result := make([]parameter, 0, len(merged))
	inputs := make(map[string]string)
	for key, parameter := range merged {
		if prior, collision := inputs[parameter.InputName]; collision && prior != key {
			return nil, fmt.Errorf("merged parameters collide as input %q", parameter.InputName)
		}
		inputs[parameter.InputName] = key
		result = append(result, parameter)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].InputName < result[right].InputName })
	return result, nil
}

func parameterKey(location, name string) string {
	if location == "header" {
		name = strings.ToLower(name)
	}
	return location + "\x00" + name
}

func (g *generator) parseRequestBody(raw any) (string, graph.Schema, bool, error) {
	if raw == nil {
		return "", nil, false, nil
	}
	object, ok := raw.(map[string]any)
	if !ok {
		return "", nil, false, invalidSource("requestBody must be an object")
	}
	if _, reference := object["$ref"]; reference {
		return "", nil, false, invalidSource("request-body references are not supported")
	}
	required := false
	if value, exists := object["required"]; exists {
		required, ok = value.(bool)
		if !ok {
			return "", nil, false, invalidSource("requestBody.required must be boolean")
		}
	}
	content, err := objectField(object, "content")
	if err != nil || len(content) != 1 {
		return "", nil, false, invalidSource("requestBody content must contain exactly application/json")
	}
	media, ok := content["application/json"].(map[string]any)
	if !ok {
		return "", nil, false, invalidSource("requestBody application/json must be an object")
	}
	rawSchema, ok := media["schema"]
	if !ok {
		return "", nil, false, invalidSource("requestBody application/json requires a schema")
	}
	schema, err := g.resolver.resolve(rawSchema)
	if err != nil {
		return "", nil, false, err
	}
	return "body", schema, required, nil
}

func (g *generator) parseResponses(raw any) ([]int, graph.Schema, error) {
	responses, ok := raw.(map[string]any)
	if !ok || len(responses) == 0 || len(responses) > 100 {
		return nil, nil, invalidSource("responses must be a bounded non-empty object")
	}
	var statuses []int
	var selected graph.Schema
	selectedDigest := ""
	for rawStatus, rawResponse := range responses {
		if len(rawStatus) != 3 {
			continue
		}
		status, err := strconv.Atoi(rawStatus)
		if err != nil || status < 200 || status > 299 {
			continue
		}
		if status == http.StatusNoContent || status == http.StatusResetContent {
			return nil, nil, invalidSource("response %s cannot satisfy a JSON body contract", rawStatus)
		}
		response, ok := rawResponse.(map[string]any)
		if !ok {
			return nil, nil, invalidSource("response %s must be an object", rawStatus)
		}
		if _, reference := response["$ref"]; reference {
			return nil, nil, invalidSource("response references are not supported")
		}
		content, err := objectField(response, "content")
		if err != nil || len(content) != 1 {
			return nil, nil, invalidSource("response %s content must contain exactly application/json", rawStatus)
		}
		media, ok := content["application/json"].(map[string]any)
		if !ok {
			return nil, nil, invalidSource("response %s application/json must be an object", rawStatus)
		}
		rawSchema, ok := media["schema"]
		if !ok {
			return nil, nil, invalidSource("response %s application/json requires a schema", rawStatus)
		}
		schema, err := g.resolver.resolve(rawSchema)
		if err != nil {
			return nil, nil, err
		}
		digest, err := values.DigestInline(map[string]any(schema))
		if err != nil {
			return nil, nil, invalidSource("response %s schema cannot be digested", rawStatus)
		}
		if selectedDigest != "" && digest != selectedDigest {
			return nil, nil, invalidSource("successful response schemas must be identical")
		}
		selected, selectedDigest = schema, digest
		statuses = append(statuses, status)
	}
	if len(statuses) == 0 {
		return nil, nil, invalidSource("at least one exact 2xx JSON response is required")
	}
	sort.Ints(statuses)
	return statuses, selected, nil
}

func validatePathTemplate(path string) error {
	if !utf8.ValidString(path) || len(path) > 4_096 || !strings.HasPrefix(path, "/") || path == "/" {
		return invalidSource("path %q must be a bounded absolute non-root template", path)
	}
	if strings.ContainsAny(path, "\\?#%\x00\r\n\t") {
		return invalidSource("path %q contains an escape, authority, query, fragment, or control marker", path)
	}
	seen := make(map[string]bool)
	for _, segment := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if segment == "" || segment == "." || segment == ".." {
			return invalidSource("path %q contains an empty or traversal segment", path)
		}
		if strings.HasPrefix(segment, "{") || strings.HasSuffix(segment, "}") {
			if len(segment) < 3 || segment[0] != '{' || segment[len(segment)-1] != '}' || strings.Count(segment, "{") != 1 || strings.Count(segment, "}") != 1 {
				return invalidSource("path %q has an invalid parameter segment", path)
			}
			name := segment[1 : len(segment)-1]
			if !stableText(name) || seen[name] {
				return invalidSource("path %q has an invalid or duplicate parameter", path)
			}
			seen[name] = true
		} else if strings.ContainsAny(segment, "{}") {
			return invalidSource("path %q parameters must occupy a complete segment", path)
		} else if !validLiteralPathSegment(segment) {
			return invalidSource("path %q contains a non-portable literal segment", path)
		}
	}
	return nil
}

func validLiteralPathSegment(segment string) bool {
	for _, character := range segment {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || strings.ContainsRune("-._~", character) {
			continue
		}
		return false
	}
	return true
}

func requirePathParameters(path string, parameters []parameter) error {
	declared := make(map[string]bool)
	for _, parameter := range parameters {
		if parameter.Location == "path" {
			declared[parameter.SourceName] = true
		}
	}
	used := make(map[string]bool)
	for _, segment := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if strings.HasPrefix(segment, "{") {
			name := segment[1 : len(segment)-1]
			if !declared[name] {
				return fmt.Errorf("path placeholder %q has no required path parameter", name)
			}
			used[name] = true
		}
	}
	for name := range declared {
		if !used[name] {
			return fmt.Errorf("path parameter %q has no placeholder", name)
		}
	}
	return nil
}

func safePlainHeader(name string) bool {
	if !validHeaderToken(name) || credentialLikeName(name) {
		return false
	}
	canonical := http.CanonicalHeaderKey(name)
	lower := strings.ToLower(canonical)
	reserved := map[string]bool{
		"authorization": true, "proxy-authorization": true, "cookie": true,
		"x-api-key": true, "api-key": true, "idempotency-key": true,
		"connection": true, "content-length": true, "host": true, "proxy-connection": true,
		"te": true, "trailer": true, "transfer-encoding": true, "upgrade": true,
	}
	if reserved[lower] {
		return false
	}
	for _, character := range name {
		if unicode.IsControl(character) || unicode.IsSpace(character) {
			return false
		}
	}
	return true
}

func safeQueryName(name string) bool {
	if !stableText(name) || credentialLikeName(name) {
		return false
	}
	return true
}

func credentialLikeName(name string) bool {
	normalized := strings.ToLower(strings.NewReplacer("-", "", "_", "", ".", "").Replace(name))
	if normalized == "auth" || normalized == "authorization" || normalized == "sig" {
		return true
	}
	for _, marker := range []string{"token", "secret", "password", "passwd", "credential", "apikey", "accesskey", "signature"} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func validHeaderToken(name string) bool {
	if name == "" {
		return false
	}
	for index := 0; index < len(name); index++ {
		character := name[index]
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(character)) {
			continue
		}
		return false
	}
	return true
}

func rejectCredentialHeaderCollision(parameters []parameter, credential *credential) error {
	if credential == nil {
		return nil
	}
	header := credential.Header
	if credential.Kind == "bearer" || credential.Kind == "basic" {
		header = "Authorization"
	}
	for _, parameter := range parameters {
		if parameter.Location == "header" && strings.EqualFold(parameter.SourceName, header) {
			return fmt.Errorf("header parameter %q collides with credential scheme %q", parameter.SourceName, credential.SourceName)
		}
	}
	return nil
}

func escapePathValue(value string) (string, error) {
	if value == "" || value == "." || value == ".." || !utf8.ValidString(value) || strings.ContainsAny(value, "/\\\x00\r\n\t") {
		return "", fmt.Errorf("path value is empty or invalid")
	}
	return url.PathEscape(value), nil
}
