package offline

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	MaximumCLIArguments = 512
	MaximumInputBytes   = 4 << 20
	MaximumRPCBytes     = 8 << 20
)

// ParseCLIInputs parses generated --<input> flags without consulting ambient
// environment or configuration. Non-string inputs are exact single JSON
// values decoded with json.Number preservation.
func ParseCLIInputs(manifest Manifest, arguments []string) (map[string]any, error) {
	if len(arguments) > MaximumCLIArguments {
		return nil, fmt.Errorf("too many offline input arguments")
	}
	fields := make(map[string]SchemaField, len(manifest.Inputs))
	for _, field := range manifest.Inputs {
		fields[field.Name] = field
	}
	result := make(map[string]any)
	totalBytes := 0
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		if !strings.HasPrefix(argument, "--") || argument == "--" {
			return nil, fmt.Errorf("offline input %q must use --<name> <value>", argument)
		}
		nameValue := strings.TrimPrefix(argument, "--")
		name, raw, hasValue := strings.Cut(nameValue, "=")
		field, ok := fields[name]
		if !ok {
			return nil, fmt.Errorf("unknown offline input flag --%s", name)
		}
		if _, duplicate := result[name]; duplicate {
			return nil, fmt.Errorf("offline input --%s is duplicated", name)
		}
		if !hasValue {
			index++
			if index >= len(arguments) {
				return nil, fmt.Errorf("offline input --%s requires a value", name)
			}
			raw = arguments[index]
		}
		if len(raw) > MaximumInputBytes {
			return nil, fmt.Errorf("offline input --%s exceeds the size bound", name)
		}
		totalBytes += len(raw)
		if totalBytes > MaximumInputBytes {
			return nil, fmt.Errorf("offline inputs exceed the aggregate size bound")
		}
		value, err := parseFlagValue(field, raw)
		if err != nil {
			return nil, fmt.Errorf("offline input --%s: %w", name, err)
		}
		result[name] = value
	}
	return result, nil
}

func parseFlagValue(field SchemaField, raw string) (any, error) {
	if kind, _ := field.Schema["type"].(string); kind == "string" {
		return raw, nil
	}
	var value any
	if err := decodeJSON([]byte(raw), &value); err != nil {
		return nil, fmt.Errorf("value must be one JSON value: %w", err)
	}
	return value, nil
}

// OutputObject returns the declared workflow data projection. Inline values
// become their native JSON payloads; artifact and secret references remain
// opaque typed envelopes and never resolve content or credentials.
func OutputObject(set values.ValueSet) (map[string]any, error) {
	if err := set.Validate(); err != nil {
		return nil, err
	}
	result := make(map[string]any, len(set))
	for name, value := range set {
		switch value.Type {
		case values.TypeNull, values.TypeBoolean, values.TypeNumber, values.TypeString, values.TypeArray, values.TypeObject:
			cloned, err := cloneJSON(value.Inline)
			if err != nil {
				return nil, err
			}
			result[name] = cloned
		case values.TypeArtifact, values.TypeSecretRef:
			var envelope any
			encoded, err := json.Marshal(value)
			if err != nil {
				return nil, err
			}
			if err := decodeJSON(encoded, &envelope); err != nil {
				return nil, err
			}
			result[name] = envelope
		default:
			return nil, fmt.Errorf("unsupported output value type %q", value.Type)
		}
	}
	return result, nil
}

func RunCLI(ctx context.Context, manifest Manifest, registry stepkind.Registry, arguments []string, stdout io.Writer) error {
	return RunCLIWithOptions(ctx, manifest, ExecuteOptions{Registry: registry}, arguments, stdout)
}

// RunCLIWithOptions runs the generated CLI using the supplied exact runtime
// composition options. Parsed CLI inputs replace any Inputs in options.
func RunCLIWithOptions(ctx context.Context, manifest Manifest, options ExecuteOptions, arguments []string, stdout io.Writer) error {
	if stdout == nil {
		return fmt.Errorf("offline stdout is required")
	}
	if len(arguments) == 1 && (arguments[0] == "--help" || arguments[0] == "-h") {
		return WriteCLIHelp(manifest, stdout)
	}
	inputs, err := ParseCLIInputs(manifest, arguments)
	if err != nil {
		return err
	}
	options.Inputs = inputs
	result, err := Execute(ctx, manifest, options)
	if err != nil {
		return err
	}
	output, err := OutputObject(result.Outputs)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(output)
}

// WriteCLIHelp renders the exact declared input surface embedded in the
// artifact. It never consults environment variables or external config.
func WriteCLIHelp(manifest Manifest, output io.Writer) error {
	if output == nil {
		return fmt.Errorf("offline help output is required")
	}
	if _, err := fmt.Fprintf(output, "Usage: %s", manifest.Plan.ID); err != nil {
		return err
	}
	for _, field := range manifest.Inputs {
		required := ""
		if field.Required {
			required = " (required)"
		}
		kind, _ := field.Schema["type"].(string)
		if _, err := fmt.Fprintf(output, "\n  --%s <%s>%s\t%s", field.Name, kind, required, field.Description); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintln(output)
	return err
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ServeMCP exposes exactly one generated workflow tool over newline-delimited
// MCP JSON-RPC on stdio. It implements only initialize, tools/list, and
// tools/call plus notification tolerance; no Hadron daemon tools are exposed.
func ServeMCP(ctx context.Context, manifest Manifest, registry stepkind.Registry, input io.Reader, output io.Writer) error {
	return ServeMCPWithOptions(ctx, manifest, ExecuteOptions{Registry: registry}, input, output)
}

// ServeMCPWithOptions exposes the generated tool using the supplied exact
// runtime composition options. Each tool call supplies its own typed inputs.
func ServeMCPWithOptions(ctx context.Context, manifest Manifest, options ExecuteOptions, input io.Reader, output io.Writer) error {
	if manifest.Mode != ModeMCPServer || input == nil || output == nil {
		return fmt.Errorf("invalid offline MCP server options")
	}
	reader := bufio.NewReader(input)
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	for {
		line, err := readRPCFrame(reader)
		if errors.Is(err, io.EOF) && len(bytes.TrimSpace(line)) == 0 {
			return nil
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		var request rpcRequest
		if decodeErr := decodeStrictJSON(bytes.TrimSpace(line), &request); decodeErr != nil {
			if encodeErr := encoder.Encode(rpcResponse{JSONRPC: "2.0", Error: &rpcError{Code: -32700, Message: "parse error"}}); encodeErr != nil {
				return encodeErr
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			continue
		}
		if len(request.ID) == 0 {
			if errors.Is(err, io.EOF) {
				return nil
			}
			continue
		}
		response := rpcResponse{JSONRPC: "2.0", ID: append(json.RawMessage(nil), request.ID...)}
		switch request.Method {
		case "initialize":
			response.Result = map[string]any{"protocolVersion": "2025-03-26", "capabilities": map[string]any{"tools": map[string]any{"listChanged": false}}, "serverInfo": map[string]any{"name": manifest.ToolName, "version": manifest.BuildDigest}}
		case "tools/list":
			response.Result = map[string]any{"tools": []any{generatedTool(manifest)}}
		case "tools/call":
			response.Result, response.Error = callGeneratedTool(ctx, manifest, options, request.Params)
		default:
			response.Error = &rpcError{Code: -32601, Message: "method not found"}
		}
		if encodeErr := encoder.Encode(response); encodeErr != nil {
			return encodeErr
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
	}
}

func readRPCFrame(reader *bufio.Reader) ([]byte, error) {
	var frame []byte
	for {
		fragment, more, err := reader.ReadLine()
		if len(frame)+len(fragment) > MaximumRPCBytes {
			for more && err == nil {
				_, more, err = reader.ReadLine()
			}
			return nil, fmt.Errorf("offline MCP request exceeds the size bound")
		}
		frame = append(frame, fragment...)
		if !more {
			return frame, err
		}
		if err != nil {
			return frame, err
		}
	}
}

func generatedTool(manifest Manifest) map[string]any {
	properties := make(map[string]any, len(manifest.Inputs))
	var required []string
	for _, input := range manifest.Inputs {
		properties[input.Name] = input.Schema
		if input.Required {
			required = append(required, input.Name)
		}
	}
	sort.Strings(required)
	schema := map[string]any{"type": "object", "additionalProperties": false, "properties": properties}
	if len(required) != 0 {
		schema["required"] = required
	}
	outputProperties := make(map[string]any, len(manifest.Outputs))
	outputRequired := make([]string, 0, len(manifest.Outputs))
	for _, output := range manifest.Outputs {
		outputProperties[output.Name] = output.Schema
		if output.Required {
			outputRequired = append(outputRequired, output.Name)
		}
	}
	sort.Strings(outputRequired)
	outputSchema := map[string]any{"type": "object", "additionalProperties": false, "properties": outputProperties}
	if len(outputRequired) != 0 {
		outputSchema["required"] = outputRequired
	}
	return map[string]any{"name": manifest.ToolName, "description": "Execute compiled workflow " + manifest.Plan.ID + " (" + manifest.PlanDigest + ")", "inputSchema": schema, "outputSchema": outputSchema}
}

func callGeneratedTool(ctx context.Context, manifest Manifest, options ExecuteOptions, raw json.RawMessage) (any, *rpcError) {
	var params struct {
		Name      string         `json:"name"`
		Arguments map[string]any `json:"arguments"`
	}
	if err := decodeJSON(raw, &params); err != nil || params.Name != manifest.ToolName {
		return nil, &rpcError{Code: -32602, Message: "invalid tool call"}
	}
	options.Inputs = params.Arguments
	result, err := Execute(ctx, manifest, options)
	if err != nil {
		message := "workflow execution failed"
		var diagnostics *DiagnosticError
		if errors.As(err, &diagnostics) {
			message = "workflow input validation failed"
		}
		return map[string]any{"isError": true, "content": []any{map[string]any{"type": "text", "text": message}}}, nil
	}
	object, err := OutputObject(result.Outputs)
	if err != nil {
		return nil, &rpcError{Code: -32603, Message: "output mapping failed"}
	}
	encoded, err := json.Marshal(object)
	if err != nil {
		return nil, &rpcError{Code: -32603, Message: "output encoding failed"}
	}
	return map[string]any{"isError": false, "structuredContent": object, "content": []any{map[string]any{"type": "text", "text": string(encoded)}}}, nil
}
