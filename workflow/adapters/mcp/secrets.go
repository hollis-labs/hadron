package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/values"
)

func validateSecretReferences(arguments map[string]any) error {
	return walkJSON(arguments, func(text string) (any, error) {
		if !strings.HasPrefix(text, "secret://") {
			return text, nil
		}
		_, err := values.ParseSecretRef(text)
		return text, err
	})
}

func resolveArguments(ctx context.Context, resolver values.SecretResolver, arguments map[string]any) (map[string]any, []*values.ResolvedSecret, error) {
	var resolved []*values.ResolvedSecret
	output, err := transformMap(arguments, func(text string) (any, error) {
		if !strings.HasPrefix(text, "secret://") {
			return text, nil
		}
		ref, err := values.ParseSecretRef(text)
		if err != nil {
			return nil, err
		}
		if nilInterface(resolver) {
			return nil, fmt.Errorf("secret resolver is required for opaque secret references")
		}
		secret, err := resolver.ResolveSecret(ctx, ref)
		if err != nil {
			return nil, err
		}
		if secret == nil || secret.Reference() != ref || len(secret.Bytes()) == 0 {
			if secret != nil {
				secret.Forget()
			}
			return nil, fmt.Errorf("secret resolver returned mismatched material")
		}
		resolved = append(resolved, secret)
		material := secret.Bytes()
		if !utf8.Valid(material) {
			return nil, fmt.Errorf("resolved secret material is not valid UTF-8")
		}
		return string(material), nil
	})
	if err != nil {
		forgetSecrets(resolved)
		return nil, nil, err
	}
	return output, resolved, nil
}

func forgetSecrets(resolved []*values.ResolvedSecret) {
	for _, secret := range resolved {
		secret.Forget()
	}
}

func forgetArgumentSecrets(arguments map[string]any) {
	for key := range arguments {
		forgetJSON(arguments[key])
		arguments[key] = values.RedactedMarker
	}
}

func forgetJSON(input any) {
	switch current := input.(type) {
	case []any:
		for index := range current {
			forgetJSON(current[index])
			current[index] = values.RedactedMarker
		}
	case map[string]any:
		for key := range current {
			forgetJSON(current[key])
			current[key] = values.RedactedMarker
		}
	}
}

func transformMap(input map[string]any, transform func(string) (any, error)) (map[string]any, error) {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	output := make(map[string]any, len(input))
	for _, key := range keys {
		value, err := transformJSON(input[key], transform)
		if err != nil {
			return nil, fmt.Errorf("argument %q: %w", key, err)
		}
		output[key] = value
	}
	return output, nil
}

func transformJSON(input any, transform func(string) (any, error)) (any, error) {
	switch current := input.(type) {
	case string:
		return transform(current)
	case json.Number, bool, nil:
		return current, nil
	case []any:
		output := make([]any, len(current))
		for index, item := range current {
			value, err := transformJSON(item, transform)
			if err != nil {
				return nil, fmt.Errorf("array[%d]: %w", index, err)
			}
			output[index] = value
		}
		return output, nil
	case map[string]any:
		return transformMap(current, transform)
	default:
		return nil, fmt.Errorf("unsupported JSON value %T", input)
	}
}

func walkJSON(input map[string]any, transform func(string) (any, error)) error {
	_, err := transformMap(input, transform)
	return err
}

func maskJSON(input any, redactor *values.Redactor) (any, error) {
	switch current := input.(type) {
	case string:
		return redactor.MaskString(current), nil
	case json.Number, bool, nil:
		return current, nil
	case []any:
		output := make([]any, len(current))
		for index, item := range current {
			masked, err := maskJSON(item, redactor)
			if err != nil {
				return nil, fmt.Errorf("array[%d]: %w", index, err)
			}
			output[index] = masked
		}
		return output, nil
	case map[string]any:
		keys := make([]string, 0, len(current))
		for key := range current {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		output := make(map[string]any, len(current))
		for _, key := range keys {
			maskedKey := redactor.MaskString(key)
			if _, duplicate := output[maskedKey]; duplicate {
				return nil, fmt.Errorf("redaction collapses distinct object keys")
			}
			masked, err := maskJSON(current[key], redactor)
			if err != nil {
				return nil, fmt.Errorf("object[%q]: %w", key, err)
			}
			output[maskedKey] = masked
		}
		return output, nil
	default:
		return nil, fmt.Errorf("unsupported JSON value %T", input)
	}
}
