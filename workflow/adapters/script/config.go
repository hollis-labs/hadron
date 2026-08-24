package script

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/dop251/goja"
	"github.com/dop251/goja/file"
	"github.com/dop251/goja/parser"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const scriptFilename = "script.js"

var (
	entrypointPattern  = regexp.MustCompile(`^[A-Za-z_$][A-Za-z0-9_$]*$`)
	exportDefaultStart = regexp.MustCompile(`(?s)^(\s*)export[ \t]+default[ \t]+function\b`)
)

type config struct {
	program      *goja.Program
	entrypoint   string
	inputSchema  graph.Schema
	outputSchema graph.Schema
}

// Spec returns immutable metadata for the deterministic local script kind.
func (e *Executor) Spec() stepkind.StepKindSpec {
	return stepkind.StepKindSpec{
		Name:    Name,
		Version: Version,
		ConfigSchema: graph.Schema{
			"type":                 "object",
			"additionalProperties": false,
			"required":             []any{"runtime", "code", "input_schema", "output_schema"},
			"properties": map[string]any{
				"runtime":       map[string]any{"const": RuntimeGoja},
				"code":          map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number(strconv.Itoa(MaximumSourceBytes))},
				"entrypoint":    map[string]any{"type": "string", "pattern": entrypointPattern.String(), "maxLength": json.Number("128")},
				"input_schema":  map[string]any{"type": "object"},
				"output_schema": map[string]any{"type": "object"},
			},
		},
		InputSchema:           graph.Schema{"type": "object"},
		OutputSchema:          graph.Schema{"type": "object"},
		Effects:               graph.EffectSet{graph.EffectCompute},
		Idempotency:           graph.IdempotencyIntrinsic,
		RetrySafety:           stepkind.RetrySafe,
		Cancellation:          stepkind.CancellationSpec{Mode: stepkind.CancellationContext},
		Observation:           stepkind.ObservationSpec{Mode: stepkind.ObservationNone},
		CanSuspend:            false,
		EmbeddedModeSupported: true,
	}
}

// ValidateConfig validates schemas, syntax, the entrypoint declaration, and
// fail-closed sandbox policy without executing author code.
func (e *Executor) ValidateConfig(_ context.Context, input graph.Config) []diagnostic.Diagnostic {
	_, finding := e.parseConfig(input)
	if finding == nil {
		return nil
	}
	return []diagnostic.Diagnostic{*finding}
}

func (e *Executor) parseConfig(input graph.Config) (config, *diagnostic.Diagnostic) {
	if e == nil || e.limits.Validate() != nil {
		return config{}, configFinding("script executor resource limits are unavailable", "config", file.Position{})
	}
	normalized, err := normalizeConfig(input)
	if err != nil {
		return config{}, configFinding("script config must be an unambiguous JSON object", "config", file.Position{})
	}
	allowed := map[string]bool{"runtime": true, "code": true, "entrypoint": true, "input_schema": true, "output_schema": true}
	keys := sortedKeys(normalized)
	for _, key := range keys {
		if !allowed[key] {
			return config{}, configFinding(fmt.Sprintf("script config contains unsupported field %q", key), "config."+key, file.Position{})
		}
	}

	runtimeName, ok := normalized["runtime"].(string)
	if !ok || runtimeName != RuntimeGoja {
		return config{}, configFinding("script config runtime must be goja", "config.runtime", file.Position{})
	}
	code, ok := normalized["code"].(string)
	if !ok || strings.TrimSpace(code) == "" || !utf8.ValidString(code) {
		return config{}, configFinding("script config code must be non-empty valid UTF-8", "config.code", file.Position{})
	}
	if len(code) > e.limits.MaxSourceBytes {
		return config{}, resourceFinding("script source exceeds max_source_bytes", "config.code", file.Position{})
	}
	if strings.Contains(code, "__hadron_") {
		return config{}, configFinding("script source uses a reserved adapter identifier", "config.code", file.Position{})
	}

	entrypoint := "main"
	entrypointExplicit := false
	if raw, exists := normalized["entrypoint"]; exists {
		entrypoint, ok = raw.(string)
		entrypointExplicit = true
		if !ok || len(entrypoint) > 128 || !entrypointPattern.MatchString(entrypoint) {
			return config{}, configFinding("script entrypoint must be a JavaScript identifier", "config.entrypoint", file.Position{})
		}
	}

	inputSchema, finding := parseDeclaredSchema(normalized, "input_schema")
	if finding != nil {
		return config{}, finding
	}
	outputSchema, finding := parseDeclaredSchema(normalized, "output_schema")
	if finding != nil {
		return config{}, finding
	}

	normalizedCode, exported := normalizeExportDefault(code)
	if exported {
		if entrypointExplicit && entrypoint != "default" {
			return config{}, configFinding("export default scripts may only use entrypoint default", "config.entrypoint", file.Position{})
		}
		entrypoint = "__hadron_default_export"
	}

	fileSet := new(file.FileSet)
	programAST, parseErr := parser.ParseFile(fileSet, scriptFilename, normalizedCode, 0, parser.WithDisableSourceMaps)
	if parseErr != nil {
		return config{}, configFinding("script source contains invalid JavaScript syntax", "config.code", sourceErrorPosition(parseErr))
	}
	if violation := validateSandbox(programAST); violation != nil {
		return config{}, capabilityFinding(violation.capability, programAST.File.Position(int(violation.idx)-programAST.File.Base()))
	}
	program, compileErr := goja.Compile(scriptFilename, normalizedCode, true)
	if compileErr != nil {
		return config{}, configFinding("script source could not be compiled", "config.code", sourceErrorPosition(compileErr))
	}
	return config{program: program, entrypoint: entrypoint, inputSchema: inputSchema, outputSchema: outputSchema}, nil
}

func normalizeConfig(input graph.Config) (map[string]any, error) {
	if input == nil {
		return nil, errors.New("null config")
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var normalized map[string]any
	if err := decoder.Decode(&normalized); err != nil || normalized == nil {
		return nil, errors.New("config is not an object")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("config contains trailing JSON")
	}
	return normalized, nil
}

func parseDeclaredSchema(input map[string]any, name string) (graph.Schema, *diagnostic.Diagnostic) {
	raw, exists := input[name]
	if !exists {
		return nil, configFinding("script config must declare "+name, "config."+name, file.Position{})
	}
	object, ok := raw.(map[string]any)
	if !ok {
		return nil, configFinding("script "+name+" must be a JSON Schema object", "config."+name, file.Position{})
	}
	schema := graph.Schema(object)
	if err := values.ValidateSchema(schema); err != nil {
		return nil, configFinding("script "+name+" is not a valid local JSON Schema", "config."+name, file.Position{})
	}
	return schema, nil
}

func normalizeExportDefault(code string) (string, bool) {
	location := exportDefaultStart.FindStringSubmatchIndex(code)
	if location == nil {
		return code, false
	}
	functionStart := strings.LastIndex(code[location[0]:location[1]], "function") + location[0]
	return code[:location[0]] + code[location[2]:location[3]] + "var __hadron_default_export = " + code[functionStart:], true
}

func sourceErrorPosition(err error) file.Position {
	var parseErrors parser.ErrorList
	if errors.As(err, &parseErrors) && len(parseErrors) != 0 && parseErrors[0] != nil {
		return parseErrors[0].Position
	}
	var syntax *goja.CompilerSyntaxError
	if errors.As(err, &syntax) && syntax.File != nil {
		return syntax.File.Position(syntax.Offset)
	}
	var reference *goja.CompilerReferenceError
	if errors.As(err, &reference) && reference.File != nil {
		return reference.File.Position(reference.Offset)
	}
	return file.Position{}
}

func configFinding(message, path string, position file.Position) *diagnostic.Diagnostic {
	return newFinding(stepkind.CodeInvalidConfig, message, path, position)
}

func resourceFinding(message, path string, position file.Position) *diagnostic.Diagnostic {
	return newFinding(stepkind.CodeInvalidConfig, message, path, position)
}

func capabilityFinding(capability string, position file.Position) *diagnostic.Diagnostic {
	return newFinding(
		stepkind.CodeInvalidConfig,
		fmt.Sprintf("script source uses denied capability %q", capability),
		"config.code",
		position,
	)
}

func newFinding(code diagnostic.Code, message, path string, position file.Position) *diagnostic.Diagnostic {
	finding := &diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError,
		Code:     code,
		Message:  message,
		Remediation: &diagnostic.Remediation{
			Message: "Use deterministic local JavaScript over declared inline inputs and outputs within the configured limits.",
		},
	}
	if position.Line > 0 {
		finding.Source = &graph.SourceRef{
			Format: graph.SourceWorkflow, Locator: scriptFilename,
			StartLine: position.Line, StartColumn: position.Column,
			EndLine: position.Line, EndColumn: position.Column,
			Path: strings.Split(path, "."),
		}
	}
	return finding
}

func sortedKeys(input map[string]any) []string {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
