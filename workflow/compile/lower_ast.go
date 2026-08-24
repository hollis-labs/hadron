package compile

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"gopkg.in/yaml.v3"
)

type lowerer struct {
	source      *Source
	diagnostics []diagnostic.Diagnostic
}

type sourceField struct {
	key   *yaml.Node
	value *yaml.Node
	path  []string
}

func (l *lowerer) mapping(node *yaml.Node, path []string, allowed ...string) map[string]sourceField {
	if node == nil || node.Kind != yaml.MappingNode {
		l.invalidShape(node, path, "expected a mapping")
		return nil
	}
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		allowedSet[name] = struct{}{}
	}
	fields := make(map[string]sourceField, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		key, value := node.Content[i], node.Content[i+1]
		fieldPath := appendPath(path, key.Value)
		if _, ok := allowedSet[key.Value]; !ok {
			l.addDiagnostic(CodeUnsupportedSourceField, key, fieldPath,
				fmt.Sprintf("source field %q is not supported at %s", key.Value, displayPath(path)),
				"Remove the field or express it through a supported graph-native field.")
			continue
		}
		fields[key.Value] = sourceField{key: key, value: value, path: fieldPath}
	}
	return fields
}

func (l *lowerer) sequence(node *yaml.Node, path []string) []*yaml.Node {
	if node == nil || node.Kind != yaml.SequenceNode {
		l.invalidShape(node, path, "expected a sequence")
		return nil
	}
	return node.Content
}

func (l *lowerer) string(node *yaml.Node, path []string) string {
	if node == nil || node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		l.invalidShape(node, path, "expected a string")
		return ""
	}
	return strings.TrimSpace(node.Value)
}

func (l *lowerer) literalString(node *yaml.Node, path []string) string {
	if node == nil || node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		l.invalidShape(node, path, "expected a string")
		return ""
	}
	return node.Value
}

func (l *lowerer) boolean(node *yaml.Node, path []string) bool {
	if node == nil || node.Kind != yaml.ScalarNode || node.Tag != "!!bool" {
		l.invalidShape(node, path, "expected a boolean")
		return false
	}
	value, err := strconv.ParseBool(node.Value)
	if err != nil {
		l.invalidShape(node, path, "expected a boolean")
	}
	return value
}

func (l *lowerer) integer(node *yaml.Node, path []string) int {
	if node == nil || node.Kind != yaml.ScalarNode || node.Tag != "!!int" {
		l.invalidShape(node, path, "expected an integer")
		return 0
	}
	value, err := strconv.Atoi(node.Value)
	if err != nil {
		l.invalidShape(node, path, "integer is outside the supported range")
		return 0
	}
	return value
}

func (l *lowerer) number(node *yaml.Node, path []string) float64 {
	if node == nil || node.Kind != yaml.ScalarNode || (node.Tag != "!!float" && node.Tag != "!!int") {
		l.invalidShape(node, path, "expected a number")
		return 0
	}
	value, err := strconv.ParseFloat(node.Value, 64)
	if err != nil {
		l.invalidShape(node, path, "number is outside the supported range")
		return 0
	}
	return value
}

func (l *lowerer) strings(node *yaml.Node, path []string) []string {
	items := l.sequence(node, path)
	values := make([]string, 0, len(items))
	for i, item := range items {
		values = append(values, l.string(item, appendPath(path, strconv.Itoa(i))))
	}
	return values
}

func (l *lowerer) jsonValue(node *yaml.Node, path []string) any {
	var decoded any
	if err := node.Decode(&decoded); err != nil {
		l.invalidShape(node, path, fmt.Sprintf("value is not decodable YAML: %v", err))
		return nil
	}
	encoded, err := json.Marshal(decoded)
	if err != nil {
		l.invalidShape(node, path, fmt.Sprintf("value is not JSON-compatible: %v", err))
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var normalized any
	if err := decoder.Decode(&normalized); err != nil {
		l.invalidShape(node, path, fmt.Sprintf("value is not JSON-compatible: %v", err))
		return nil
	}
	if err := requireJSONEnd(decoder); err != nil {
		l.invalidShape(node, path, err.Error())
		return nil
	}
	return normalized
}

func requireJSONEnd(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	return fmt.Errorf("value contains multiple JSON documents")
}

func (l *lowerer) normalizeID(node *yaml.Node, path []string) string {
	raw := l.string(node, path)
	normalized := graph.NormalizeID(raw)
	if err := graph.ValidateID(normalized); err != nil {
		l.addDiagnostic(CodeInvalidWorkflowID, node, path,
			fmt.Sprintf("identity %q cannot normalize to a valid workflow ID", raw),
			"Use a non-empty identity of at most 128 ASCII letters, digits, and separators.")
		return ""
	}
	return normalized
}

func (l *lowerer) invalidShape(node *yaml.Node, path []string, message string) {
	l.addDiagnostic(CodeInvalidWorkflowShape, node, path,
		fmt.Sprintf("%s: %s", displayPath(path), message),
		"Use the documented graph-native workflow source shape.")
}

func (l *lowerer) invalidBinding(node *yaml.Node, path []string, message string) {
	l.addDiagnostic(CodeInvalidBindingSource, node, path,
		fmt.Sprintf("%s: %s", displayPath(path), message),
		"Use exactly one of literal, expression, or interpolation for an explicit binding.")
}

func (l *lowerer) addDiagnostic(code diagnostic.Code, node *yaml.Node, path []string, message, remediation string) {
	locator := "<source>"
	if l.source != nil && strings.TrimSpace(l.source.Locator) != "" {
		locator = l.source.Locator
	}
	ref := sourceRef(locator, graph.SourceWorkflow, node, path)
	l.diagnostics = append(l.diagnostics, diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError,
		Code:     code,
		Message:  message,
		Source:   &ref,
		Remediation: &diagnostic.Remediation{
			Message:       remediation,
			Documentation: sourceFormatDocumentation,
		},
	})
}

func displayPath(path []string) string {
	if len(path) == 0 {
		return "workflow source root"
	}
	return strings.Join(path, ".")
}
