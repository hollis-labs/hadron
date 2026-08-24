package http

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	nethttp "net/http"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/values"
)

var forbiddenRequestHeaders = map[string]bool{
	"connection": true, "content-length": true, "host": true, "proxy-connection": true,
	"te": true, "trailer": true, "transfer-encoding": true, "upgrade": true, "idempotency-key": true,
}

var sensitiveHeaders = map[string]bool{
	"authorization": true, "proxy-authorization": true, "cookie": true,
	"set-cookie": true, "www-authenticate": true, "proxy-authenticate": true,
	"authentication-info": true, "x-api-key": true, "api-key": true, "idempotency-key": true,
}

type resolvedRequest struct {
	Headers       nethttp.Header
	Body          []byte
	Secrets       []*values.ResolvedSecret
	Redactor      *values.Redactor
	SecretHeaders map[string]struct{}
}

func validateHeader(name, value string) (string, bool, error) {
	canonical := nethttp.CanonicalHeaderKey(name)
	if canonical == "" || !validHeaderName(name) || forbiddenRequestHeaders[strings.ToLower(canonical)] || !validHeaderValue(value) {
		return "", false, fmt.Errorf("%w: invalid or forbidden header %q", ErrInvalidConfig, name)
	}
	ref, refErr := values.ParseSecretRef(value)
	isSecret := refErr == nil
	if strings.HasPrefix(value, "secret://") && !isSecret {
		return "", false, fmt.Errorf("%w: header %q contains an invalid secret reference", ErrInvalidConfig, name)
	}
	if isSensitiveHeader(strings.ToLower(canonical)) && !isSecret {
		return "", false, fmt.Errorf("%w: sensitive header %q requires an opaque secret reference", ErrInvalidConfig, name)
	}
	_ = ref
	return canonical, isSecret, nil
}

func parseAuth(value any) (*authConfig, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: auth must be an object", ErrInvalidConfig)
	}
	allowed := map[string]bool{"type": true, "secret_ref": true, "username": true, "header": true}
	for _, key := range sortedKeys(object) {
		if !allowed[key] {
			return nil, fmt.Errorf("%w: unknown auth field %q", ErrInvalidConfig, key)
		}
	}
	kind, kindOK := object["type"].(string)
	refText, refOK := object["secret_ref"].(string)
	ref, err := values.ParseSecretRef(refText)
	if !kindOK || !refOK || err != nil {
		return nil, fmt.Errorf("%w: auth requires type and a valid secret_ref", ErrInvalidConfig)
	}
	result := &authConfig{Kind: kind, SecretRef: ref}
	switch kind {
	case "bearer":
		if len(object) != 2 {
			return nil, fmt.Errorf("%w: bearer auth permits only type and secret_ref", ErrInvalidConfig)
		}
	case "basic":
		result.Username, kindOK = object["username"].(string)
		if !kindOK || validateStableText("auth username", result.Username) != nil || len(object) != 3 {
			return nil, fmt.Errorf("%w: basic auth requires a valid username", ErrInvalidConfig)
		}
	case "header":
		result.Header, kindOK = object["header"].(string)
		if !kindOK || len(object) != 3 {
			return nil, fmt.Errorf("%w: header auth requires a header name", ErrInvalidConfig)
		}
		canonical, _, headerErr := validateHeader(result.Header, string(result.SecretRef))
		if headerErr != nil {
			return nil, headerErr
		}
		result.Header = canonical
	default:
		return nil, fmt.Errorf("%w: unsupported auth type", ErrInvalidConfig)
	}
	return result, nil
}

func authHeader(auth *authConfig) string {
	if auth != nil && auth.Kind == "header" {
		return auth.Header
	}
	return "Authorization"
}

func validateBodySecretRefs(value any) (bool, error) {
	switch typed := value.(type) {
	case nil, bool, json.Number:
		return false, nil
	case string:
		_, err := values.ParseSecretRef(typed)
		if err == nil {
			return true, nil
		}
		if strings.HasPrefix(typed, "secret://") {
			return false, fmt.Errorf("%w: body contains an invalid secret reference", ErrInvalidConfig)
		}
		return false, nil
	case []any:
		found := false
		for _, child := range typed {
			current, err := validateBodySecretRefs(child)
			if err != nil {
				return false, err
			}
			found = found || current
		}
		return found, nil
	case map[string]any:
		found := false
		for _, key := range sortedKeys(typed) {
			if _, err := values.ParseSecretRef(key); err == nil || strings.HasPrefix(key, "secret://") {
				return false, fmt.Errorf("%w: body object keys cannot be secret references", ErrInvalidConfig)
			}
			current, err := validateBodySecretRefs(typed[key])
			if err != nil {
				return false, err
			}
			found = found || current
		}
		return found, nil
	default:
		return false, fmt.Errorf("%w: body must be JSON-compatible", ErrInvalidConfig)
	}
}

func resolveRequestWith(ctx context.Context, resolver values.SecretResolver, parsed config, maxHeaderBytes, maxRequestBytes int64) (resolvedRequest, error) {
	result := resolvedRequest{
		Headers: make(nethttp.Header), SecretHeaders: make(map[string]struct{}, len(parsed.SecretRequestHeaders)),
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	for name := range parsed.SecretRequestHeaders {
		result.SecretHeaders[name] = struct{}{}
	}
	if parsed.HasSecretReferences && nilInterface(resolver) {
		return result, fmt.Errorf("secret resolver is required")
	}
	resolve := func(ref values.SecretRef) (string, error) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		secret, err := resolver.ResolveSecret(ctx, ref)
		if err != nil || secret == nil || secret.Reference() != ref {
			if err != nil {
				return "", fmt.Errorf("resolve secret reference: %w", err)
			}
			return "", fmt.Errorf("resolve secret reference returned mismatched material")
		}
		if contextErr := ctx.Err(); contextErr != nil {
			secret.Forget()
			return "", contextErr
		}
		material := secret.Bytes()
		if !utf8.Valid(material) || len(material) == 0 {
			secret.Forget()
			return "", fmt.Errorf("resolved secret is not valid HTTP text")
		}
		result.Secrets = append(result.Secrets, secret)
		return string(material), nil
	}

	for _, name := range sortedStringMapKeys(parsed.Headers) {
		value := parsed.Headers[name]
		if ref, err := values.ParseSecretRef(value); err == nil {
			value, err = resolve(ref)
			if err != nil {
				forgetSecrets(result.Secrets)
				return resolvedRequest{}, err
			}
		}
		if !validHeaderValue(value) {
			forgetSecrets(result.Secrets)
			return resolvedRequest{}, fmt.Errorf("resolved header is invalid")
		}
		result.Headers.Set(name, value)
	}
	if parsed.Auth != nil {
		material, err := resolve(parsed.Auth.SecretRef)
		if err != nil {
			forgetSecrets(result.Secrets)
			return resolvedRequest{}, err
		}
		switch parsed.Auth.Kind {
		case "bearer":
			value := "Bearer " + material
			if !validHeaderValue(value) {
				forgetSecrets(result.Secrets)
				return resolvedRequest{}, fmt.Errorf("resolved bearer credential is invalid")
			}
			result.Headers.Set("Authorization", value)
		case "basic":
			encoded := base64.StdEncoding.EncodeToString([]byte(parsed.Auth.Username + ":" + material))
			result.Headers.Set("Authorization", "Basic "+encoded)
			derived, derivedErr := values.NewResolvedSecret(parsed.Auth.SecretRef, []byte(encoded))
			if derivedErr != nil {
				forgetSecrets(result.Secrets)
				return resolvedRequest{}, fmt.Errorf("create derived credential redactor")
			}
			result.Secrets = append(result.Secrets, derived)
		case "header":
			if !validHeaderValue(material) {
				forgetSecrets(result.Secrets)
				return resolvedRequest{}, fmt.Errorf("resolved header credential is invalid")
			}
			result.Headers.Set(parsed.Auth.Header, material)
		}
	}
	if parsed.IdempotencyKey != "" {
		if result.Headers.Get("Idempotency-Key") != "" {
			forgetSecrets(result.Secrets)
			return resolvedRequest{}, fmt.Errorf("idempotency header conflicts with configuration")
		}
		result.Headers.Set("Idempotency-Key", parsed.IdempotencyKey)
	}
	if parsed.HasBody {
		body, err := resolveBodyValue(parsed.Body, resolve)
		if err != nil {
			forgetSecrets(result.Secrets)
			return resolvedRequest{}, err
		}
		result.Body, err = json.Marshal(body)
		if err != nil {
			forgetSecrets(result.Secrets)
			return resolvedRequest{}, fmt.Errorf("encode request body")
		}
		if int64(len(result.Body)) > maxRequestBytes {
			zeroBytes(result.Body)
			forgetSecrets(result.Secrets)
			return resolvedRequest{}, fmt.Errorf("resolved request body exceeds the request bound")
		}
		if result.Headers.Get("Content-Type") == "" {
			result.Headers.Set("Content-Type", "application/json")
		}
	}
	if err := validateHeaderBound(result.Headers, maxHeaderBytes); err != nil {
		zeroBytes(result.Body)
		forgetSecrets(result.Secrets)
		return resolvedRequest{}, fmt.Errorf("resolved request headers exceed the request bound")
	}
	redactor, err := values.NewRedactor(result.Secrets...)
	if err != nil {
		forgetSecrets(result.Secrets)
		return resolvedRequest{}, fmt.Errorf("create secret redactor")
	}
	result.Redactor = redactor
	if err := ctx.Err(); err != nil {
		forgetSecrets(result.Secrets)
		return resolvedRequest{}, err
	}
	return result, nil
}

func resolveBodyValue(value any, resolve func(values.SecretRef) (string, error)) (any, error) {
	switch typed := value.(type) {
	case string:
		if ref, err := values.ParseSecretRef(typed); err == nil {
			return resolve(ref)
		}
		return typed, nil
	case []any:
		result := make([]any, len(typed))
		for index, child := range typed {
			resolved, err := resolveBodyValue(child, resolve)
			if err != nil {
				return nil, err
			}
			result[index] = resolved
		}
		return result, nil
	case map[string]any:
		result := make(map[string]any, len(typed))
		for _, key := range sortedKeys(typed) {
			resolved, err := resolveBodyValue(typed[key], resolve)
			if err != nil {
				return nil, err
			}
			result[key] = resolved
		}
		return result, nil
	default:
		return typed, nil
	}
}

func forgetSecrets(secrets []*values.ResolvedSecret) {
	for _, secret := range secrets {
		secret.Forget()
	}
}

func sortedStringMapKeys(input map[string]string) []string {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func validHeaderValue(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for index := 0; index < len(value); index++ {
		current := value[index]
		if current == '\t' || current >= 0x20 && current != 0x7f {
			continue
		}
		return false
	}
	return true
}

func validHeaderName(value string) bool {
	if value == "" {
		return false
	}
	for index := 0; index < len(value); index++ {
		current := value[index]
		if current >= 'a' && current <= 'z' || current >= 'A' && current <= 'Z' ||
			current >= '0' && current <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(current)) {
			continue
		}
		return false
	}
	return true
}

func sanitizeRequestHeaders(headers nethttp.Header, redactor *values.Redactor, secretHeaders map[string]struct{}) map[string][]string {
	result := make(map[string][]string, len(headers))
	for name, entries := range headers {
		lower := strings.ToLower(name)
		masked := make([]string, len(entries))
		for index, entry := range entries {
			if isSensitiveHeader(lower) {
				masked[index] = values.RedactedMarker
			} else if _, secret := secretHeaders[lower]; secret {
				masked[index] = values.RedactedMarker
			} else {
				masked[index] = redactor.MaskString(entry)
			}
		}
		sort.Strings(masked)
		result[lower] = masked
	}
	return result
}

func isSensitiveHeader(name string) bool {
	if sensitiveHeaders[name] {
		return true
	}
	normalized := strings.ReplaceAll(strings.ToLower(name), "_", "-")
	for _, segment := range strings.Split(normalized, "-") {
		switch segment {
		case "auth", "authorization", "token", "secret", "key", "password", "passwd", "credential", "cookie", "signature":
			return true
		}
	}
	return false
}
