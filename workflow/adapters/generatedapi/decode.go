package generatedapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	maxDocumentDepth = 128
	maxDocumentNodes = 100_000
	maxScalarBytes   = 1 << 20
)

var jsonNumberPattern = regexp.MustCompile(`^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$`)

func decodeSource(source []byte, limit int64) (map[string]any, error) {
	if len(source) == 0 {
		return nil, invalidSource("document is empty")
	}
	if int64(len(source)) > limit {
		return nil, invalidSource("document exceeds %d-byte bound", limit)
	}
	decoder := yaml.NewDecoder(strings.NewReader(string(source)))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return nil, invalidSource("decode document: %v", err)
	}
	var trailing yaml.Node
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, invalidSource("multiple documents are not allowed")
		}
		return nil, invalidSource("decode trailing document: %v", err)
	}
	if len(document.Content) != 1 {
		return nil, invalidSource("document root is missing")
	}
	count := 0
	decoded, err := decodeYAMLNode(document.Content[0], 0, &count)
	if err != nil {
		return nil, err
	}
	object, ok := decoded.(map[string]any)
	if !ok {
		return nil, invalidSource("document root must be an object")
	}
	return object, nil
}

func decodeYAMLNode(node *yaml.Node, depth int, count *int) (any, error) {
	if node == nil || depth > maxDocumentDepth {
		return nil, invalidSource("document exceeds structural depth bound")
	}
	(*count)++
	if *count > maxDocumentNodes {
		return nil, invalidSource("document exceeds structural node bound")
	}
	if node.Alias != nil || node.Kind == yaml.AliasNode {
		return nil, invalidSource("YAML aliases are not supported")
	}
	switch node.Kind {
	case yaml.MappingNode:
		if node.Tag != "!!map" || len(node.Content)%2 != 0 {
			return nil, invalidSource("invalid object node")
		}
		result := make(map[string]any, len(node.Content)/2)
		for index := 0; index < len(node.Content); index += 2 {
			keyNode := node.Content[index]
			if keyNode.Kind != yaml.ScalarNode || keyNode.Tag != "!!str" || keyNode.Value == "" {
				return nil, invalidSource("object keys must be non-empty strings")
			}
			if keyNode.Value == "<<" {
				return nil, invalidSource("YAML merge keys are not supported")
			}
			if _, duplicate := result[keyNode.Value]; duplicate {
				return nil, invalidSource("duplicate object key %q", keyNode.Value)
			}
			value, err := decodeYAMLNode(node.Content[index+1], depth+1, count)
			if err != nil {
				return nil, err
			}
			result[keyNode.Value] = value
		}
		return result, nil
	case yaml.SequenceNode:
		if node.Tag != "!!seq" {
			return nil, invalidSource("invalid array node")
		}
		result := make([]any, len(node.Content))
		for index, child := range node.Content {
			value, err := decodeYAMLNode(child, depth+1, count)
			if err != nil {
				return nil, err
			}
			result[index] = value
		}
		return result, nil
	case yaml.ScalarNode:
		if len(node.Value) > maxScalarBytes {
			return nil, invalidSource("scalar exceeds %d-byte bound", maxScalarBytes)
		}
		switch node.Tag {
		case "!!str":
			return node.Value, nil
		case "!!null":
			return nil, nil
		case "!!bool":
			value, err := strconv.ParseBool(node.Value)
			if err != nil {
				return nil, invalidSource("invalid boolean %q", node.Value)
			}
			return value, nil
		case "!!int", "!!float":
			if !jsonNumberPattern.MatchString(node.Value) {
				return nil, invalidSource("number %q is not canonical JSON", node.Value)
			}
			return json.Number(node.Value), nil
		default:
			return nil, invalidSource("unsupported YAML tag %q", node.Tag)
		}
	default:
		return nil, invalidSource("unsupported YAML node kind %d", node.Kind)
	}
}

func objectField(object map[string]any, name string) (map[string]any, error) {
	value, ok := object[name]
	if !ok {
		return nil, invalidSource("%s is required", name)
	}
	result, ok := value.(map[string]any)
	if !ok {
		return nil, invalidSource("%s must be an object", name)
	}
	return result, nil
}

func requiredString(object map[string]any, name, path string) (string, error) {
	value, ok := object[name].(string)
	if !ok || !stableText(value) {
		return "", invalidSource("%s.%s must be a non-empty stable string", path, name)
	}
	return value, nil
}

func canonicalDigest(document map[string]any) (string, error) {
	encoded, err := json.Marshal(document)
	if err != nil {
		return "", fmt.Errorf("%w: canonicalize source: %w", ErrInvalidSource, err)
	}
	var normalized any
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.UseNumber()
	if err := decoder.Decode(&normalized); err != nil {
		return "", fmt.Errorf("%w: canonicalize source: %w", ErrInvalidSource, err)
	}
	return digestNative(normalized)
}
