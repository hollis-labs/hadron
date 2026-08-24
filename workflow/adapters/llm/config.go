package llm

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	defaultTimeout          = 2 * time.Minute
	maximumTimeout          = time.Hour
	defaultMaxInputBytes    = int64(1 << 20)
	defaultMaxOutputBytes   = int64(1 << 20)
	defaultMaxTotalTokens   = int64(128_000)
	defaultMaxToolCalls     = 16
	maximumConfiguredBytes  = int64(16 << 20)
	maximumConfiguredTokens = int64(2_000_000)
	maximumToolCalls        = 128
	maximumMessages         = 256
	maximumContextInputs    = 128
	maximumDeclaredTools    = 128
)

// Budget is the aggregate limit shared by initial, tool-loop, and repair
// provider calls. A zero CostMicrounits means cost is not independently capped.
type Budget struct {
	MaxInputBytes     int64 `json:"max_input_bytes"`
	MaxOutputBytes    int64 `json:"max_output_bytes"`
	MaxTotalTokens    int64 `json:"max_total_tokens"`
	MaxCostMicrounits int64 `json:"max_cost_microunits,omitempty"`
	MaxToolCalls      int   `json:"max_tool_calls"`
}

type repairMode string

const (
	repairFail repairMode = "fail"
	repairOnce repairMode = "once"
)

type messageConfig struct {
	Role    string
	Content string
	Input   string
}

type config struct {
	Profile           string
	Provider          string
	Model             string
	System            string
	Messages          []messageConfig
	ContextInputs     []string
	Tools             []string
	MaxToolIterations int
	OutputSchema      graph.Schema
	Repair            repairMode
	Budget            Budget
	Timeout           time.Duration
	Streaming         bool
}

func defaultBudget() Budget {
	return Budget{MaxInputBytes: defaultMaxInputBytes, MaxOutputBytes: defaultMaxOutputBytes, MaxTotalTokens: defaultMaxTotalTokens, MaxToolCalls: defaultMaxToolCalls}
}

func parseConfig(input graph.Config) (config, error) {
	normalized, normalizeErr := cloneObject(input)
	if normalizeErr != nil {
		return config{}, fmt.Errorf("%w: config must be an unambiguous JSON object", ErrInvalidConfig)
	}
	allowed := stringSet("profile", "provider", "model", "system", "messages", "context_inputs", "tools", "max_tool_iterations", "output_schema", "repair", "budget", "timeout", "stream")
	if err := rejectUnknown(normalized, allowed, "config"); err != nil {
		return config{}, err
	}

	result := config{Repair: repairFail, Budget: defaultBudget(), Timeout: defaultTimeout}
	if result.Profile, normalizeErr = requiredText(normalized, "profile", 256); normalizeErr != nil {
		return config{}, normalizeErr
	}
	if result.Provider, normalizeErr = optionalText(normalized, "provider", 256); normalizeErr != nil {
		return config{}, normalizeErr
	}
	if result.Model, normalizeErr = optionalText(normalized, "model", 256); normalizeErr != nil {
		return config{}, normalizeErr
	}
	if result.System, normalizeErr = optionalPromptText(normalized, "system", int(defaultMaxInputBytes)); normalizeErr != nil {
		return config{}, normalizeErr
	}

	rawMessages, messagesOK := normalized["messages"].([]any)
	if !messagesOK || len(rawMessages) == 0 || len(rawMessages) > maximumMessages {
		return config{}, fmt.Errorf("%w: messages must contain 1..%d entries", ErrInvalidConfig, maximumMessages)
	}
	for index, raw := range rawMessages {
		object, ok := raw.(map[string]any)
		if !ok {
			return config{}, fmt.Errorf("%w: messages[%d] must be an object", ErrInvalidConfig, index)
		}
		if err := rejectUnknown(object, stringSet("role", "content", "input"), fmt.Sprintf("messages[%d]", index)); err != nil {
			return config{}, err
		}
		role, err := requiredText(object, "role", 32)
		if err != nil || (role != "user" && role != "assistant") {
			return config{}, fmt.Errorf("%w: messages[%d].role must be user or assistant", ErrInvalidConfig, index)
		}
		content, contentErr := optionalPromptText(object, "content", int(defaultMaxInputBytes))
		inputName, inputErr := optionalText(object, "input", 256)
		if contentErr != nil || inputErr != nil || (content == "") == (inputName == "") || (inputName != "" && role != "user") {
			return config{}, fmt.Errorf("%w: messages[%d] requires exactly one of content or user input", ErrInvalidConfig, index)
		}
		if inputName != "" && graph.ValidateID(inputName) != nil {
			return config{}, fmt.Errorf("%w: messages[%d].input must be a graph ID", ErrInvalidConfig, index)
		}
		result.Messages = append(result.Messages, messageConfig{Role: role, Content: content, Input: inputName})
	}
	if result.ContextInputs, normalizeErr = optionalNames(normalized, "context_inputs", maximumContextInputs); normalizeErr != nil {
		return config{}, normalizeErr
	}
	for _, name := range result.ContextInputs {
		if graph.ValidateID(name) != nil {
			return config{}, fmt.Errorf("%w: context input %q must be a graph ID", ErrInvalidConfig, name)
		}
	}
	if result.Tools, normalizeErr = optionalNames(normalized, "tools", maximumDeclaredTools); normalizeErr != nil {
		return config{}, normalizeErr
	}
	for _, tool := range result.Tools {
		if err := validateToolName(tool); err != nil {
			return config{}, fmt.Errorf("%w: invalid tool %q", ErrInvalidConfig, tool)
		}
	}
	if raw, exists := normalized["max_tool_iterations"]; exists {
		value, err := jsonInteger(raw)
		if err != nil || value < 0 || value > maximumToolCalls {
			return config{}, fmt.Errorf("%w: max_tool_iterations must be an integer from 0 to %d", ErrInvalidConfig, maximumToolCalls)
		}
		result.MaxToolIterations = int(value)
	} else if len(result.Tools) != 0 {
		result.MaxToolIterations = 8
	}
	if len(result.Tools) == 0 && result.MaxToolIterations != 0 {
		return config{}, fmt.Errorf("%w: tool iterations require tools", ErrInvalidConfig)
	}
	rawSchema, ok := normalized["output_schema"].(map[string]any)
	if !ok {
		return config{}, fmt.Errorf("%w: output_schema must be an object", ErrInvalidConfig)
	}
	result.OutputSchema = graph.Schema(rawSchema)
	if err := values.ValidateSchema(result.OutputSchema); err != nil {
		return config{}, fmt.Errorf("%w: output_schema: %w", ErrInvalidConfig, err)
	}
	if raw, exists := normalized["repair"]; exists {
		text, ok := raw.(string)
		result.Repair = repairMode(text)
		if !ok || (result.Repair != repairFail && result.Repair != repairOnce) {
			return config{}, fmt.Errorf("%w: repair must be fail or once", ErrInvalidConfig)
		}
	}
	if raw, exists := normalized["budget"]; exists {
		object, ok := raw.(map[string]any)
		if !ok {
			return config{}, fmt.Errorf("%w: budget must be an object", ErrInvalidConfig)
		}
		if err := rejectUnknown(object, stringSet("max_input_bytes", "max_output_bytes", "max_total_tokens", "max_cost_microunits", "max_tool_calls"), "budget"); err != nil {
			return config{}, err
		}
		for _, field := range []struct {
			name        string
			destination *int64
		}{{"max_input_bytes", &result.Budget.MaxInputBytes}, {"max_output_bytes", &result.Budget.MaxOutputBytes}, {"max_total_tokens", &result.Budget.MaxTotalTokens}, {"max_cost_microunits", &result.Budget.MaxCostMicrounits}} {
			name, destination := field.name, field.destination
			if value, exists := object[name]; exists {
				parsed, parseErr := jsonInteger(value)
				if parseErr != nil {
					return config{}, fmt.Errorf("%w: budget.%s must be an integer", ErrInvalidConfig, name)
				}
				*destination = parsed
			}
		}
		if value, exists := object["max_tool_calls"]; exists {
			parsed, parseErr := jsonInteger(value)
			if parseErr != nil {
				return config{}, fmt.Errorf("%w: budget.max_tool_calls must be an integer", ErrInvalidConfig)
			}
			result.Budget.MaxToolCalls = int(parsed)
		}
	}
	if err := result.Budget.validate(); err != nil {
		return config{}, err
	}
	if raw, exists := normalized["timeout"]; exists {
		text, ok := raw.(string)
		parsed, parseErr := time.ParseDuration(text)
		if !ok || parseErr != nil || parsed <= 0 || parsed > maximumTimeout {
			return config{}, fmt.Errorf("%w: timeout must be positive and no greater than %s", ErrInvalidConfig, maximumTimeout)
		}
		result.Timeout = parsed
	}
	if raw, exists := normalized["stream"]; exists {
		var streamOK bool
		result.Streaming, streamOK = raw.(bool)
		if !streamOK {
			return config{}, fmt.Errorf("%w: stream must be boolean", ErrInvalidConfig)
		}
	}
	return result, nil
}

func (b Budget) validate() error {
	if b.MaxInputBytes < 1 || b.MaxInputBytes > maximumConfiguredBytes || b.MaxOutputBytes < 1 || b.MaxOutputBytes > maximumConfiguredBytes || b.MaxTotalTokens < 1 || b.MaxTotalTokens > maximumConfiguredTokens || b.MaxCostMicrounits < 0 || b.MaxToolCalls < 0 || b.MaxToolCalls > maximumToolCalls {
		return fmt.Errorf("%w: budget limits are outside supported bounds", ErrInvalidConfig)
	}
	return nil
}

func cloneObject(input map[string]any) (map[string]any, error) {
	if input == nil {
		return nil, errors.New("nil object")
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var result map[string]any
	if err := decoder.Decode(&result); err != nil || result == nil {
		return nil, errors.New("not object")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("trailing JSON")
	}
	return result, nil
}

func rejectUnknown(input map[string]any, allowed map[string]struct{}, prefix string) error {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("%w: %s contains unsupported field %q", ErrInvalidConfig, prefix, key)
		}
	}
	return nil
}

func stringSet(values ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func requiredText(input map[string]any, name string, limit int) (string, error) {
	value, err := optionalText(input, name, limit)
	if err != nil || value == "" {
		return "", fmt.Errorf("%w: %s must be non-empty stable text", ErrInvalidConfig, name)
	}
	return value, nil
}
func optionalText(input map[string]any, name string, limit int) (string, error) {
	raw, exists := input[name]
	if !exists {
		return "", nil
	}
	value, ok := raw.(string)
	if !ok || !stableText(value, false) || len(value) > limit {
		return "", fmt.Errorf("%w: %s must be stable UTF-8 text of at most %d bytes", ErrInvalidConfig, name, limit)
	}
	return value, nil
}

func optionalPromptText(input map[string]any, name string, limit int) (string, error) {
	raw, exists := input[name]
	if !exists {
		return "", nil
	}
	value, ok := raw.(string)
	if !ok || !utf8.ValidString(value) || len(value) > limit {
		return "", fmt.Errorf("%w: %s must be valid UTF-8 text of at most %d bytes", ErrInvalidConfig, name, limit)
	}
	for _, current := range value {
		if unicode.IsControl(current) && current != '\n' && current != '\r' && current != '\t' {
			return "", fmt.Errorf("%w: %s contains control text", ErrInvalidConfig, name)
		}
	}
	return value, nil
}

func stableText(value string, required bool) bool {
	if !utf8.ValidString(value) || strings.TrimSpace(value) != value || (required && value == "") {
		return false
	}
	for _, current := range value {
		if unicode.IsControl(current) {
			return false
		}
	}
	return true
}

func optionalNames(input map[string]any, field string, maximum int) ([]string, error) {
	raw, exists := input[field]
	if !exists {
		return nil, nil
	}
	array, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: %s must be an array", ErrInvalidConfig, field)
	}
	if len(array) > maximum {
		return nil, fmt.Errorf("%w: %s must contain no more than %d names", ErrInvalidConfig, field, maximum)
	}
	seen := make(map[string]struct{}, len(array))
	result := make([]string, 0, len(array))
	for _, item := range array {
		name, ok := item.(string)
		if !ok || !stableText(name, true) || len(name) > 256 {
			return nil, fmt.Errorf("%w: %s contains an invalid name", ErrInvalidConfig, field)
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf("%w: %s contains duplicate %q", ErrInvalidConfig, field, name)
		}
		seen[name] = struct{}{}
		result = append(result, name)
	}
	return result, nil
}

func validateToolName(value string) error {
	if len(value) < 1 || len(value) > 128 {
		return ErrInvalidConfig
	}
	for _, current := range value {
		if (current >= 'a' && current <= 'z') || (current >= 'A' && current <= 'Z') || (current >= '0' && current <= '9') || current == '_' || current == '-' || current == '.' {
			continue
		}
		return ErrInvalidConfig
	}
	return nil
}

func jsonInteger(value any) (int64, error) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, errors.New("not JSON number")
	}
	return number.Int64()
}
