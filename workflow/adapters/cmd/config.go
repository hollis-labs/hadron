package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	// DefaultCaptureBytes is the retained/emitted stream ceiling when omitted.
	DefaultCaptureBytes int64 = 64 << 10
	// MaximumCaptureBytes is the largest artifact stream accepted by cmd@v1.
	MaximumCaptureBytes int64 = 1 << 30
	// MaximumTimeout prevents accidentally unbounded command lifetimes.
	MaximumTimeout = 24 * time.Hour
)

type CaptureMode string

const (
	CaptureOutput   CaptureMode = "output"
	CaptureArtifact CaptureMode = "artifact"
	CaptureEvent    CaptureMode = "event_stream"
)

type ParseMode string

const (
	ParseText      ParseMode = "text"
	ParseJSON      ParseMode = "json"
	ParseLines     ParseMode = "lines"
	ParseKV        ParseMode = "kv"
	ParseSetOutput ParseMode = "set-output"
)

// CaptureConfig is the typed cmd@v1 stream-capture contract. MaxBytes is a
// hard raw and redacted byte bound for retained output, artifacts, or emitted
// operational events; event streams retain no bytes.
type CaptureConfig struct {
	Mode          CaptureMode `json:"as"`
	Name          string      `json:"name,omitempty"`
	Parse         ParseMode   `json:"parse,omitempty"`
	MaxBytes      int64       `json:"max_bytes,omitempty"`
	MediaType     string      `json:"media_type,omitempty"`
	Compatibility bool        `json:"compatibility,omitempty"`
}

type streamCaptures struct {
	stdout *CaptureConfig
	stderr *CaptureConfig
}

type commandConfig struct {
	executable   string
	arguments    []string
	cwd          string
	environment  map[string]values.SecretRef
	timeout      time.Duration
	effects      graph.EffectSet
	capabilities []string
	sandbox      SandboxSpec
	captures     streamCaptures
}

var topLevelFields = map[string]struct{}{
	"executable": {}, "command": {}, "argv": {}, "arguments": {}, "cwd": {},
	"env": {}, "timeout": {}, "effects": {}, "capabilities": {}, "sandbox": {}, "capture": {},
}

// DescribeConfig validates config and returns a defensive, deterministic
// description. ConservativeEffects and RequiredCapabilities are authoritative
// adapter metadata; declared fields are only author expectations.
func DescribeConfig(config graph.Config) (ConfigDescription, []diagnostic.Diagnostic) {
	parsed, findings := parseConfig(config)
	if hasErrors(findings) {
		return ConfigDescription{}, findings
	}
	return parsed.description(), findings
}

func (c commandConfig) description() ConfigDescription {
	names := make([]string, 0, len(c.environment))
	for name := range c.environment {
		names = append(names, name)
	}
	sort.Strings(names)
	return ConfigDescription{
		ConfiguredExecutable: c.executable,
		Arguments:            append([]string(nil), c.arguments...),
		ConfiguredCWD:        c.cwd,
		EnvironmentNames:     names,
		DeclaredEffects:      append(graph.EffectSet(nil), c.effects...),
		DeclaredCapabilities: append([]string(nil), c.capabilities...),
		SandboxExpectation:   c.sandbox,
		ConservativeEffects:  graph.EffectSet{graph.EffectDestructive},
		RequiredCapabilities: []string{CapabilityProcessExecute},
		Idempotency:          graph.IdempotencyNone,
		RetrySafety:          stepkind.RetryUnsupported,
	}
}

func parseConfig(config graph.Config) (commandConfig, []diagnostic.Diagnostic) {
	object, err := cloneConfig(config)
	if err != nil {
		return commandConfig{}, []diagnostic.Diagnostic{configError("config", "must be a JSON-compatible object")}
	}
	var findings []diagnostic.Diagnostic
	keys := sortedKeys(object)
	for _, key := range keys {
		if _, ok := topLevelFields[key]; !ok {
			findings = append(findings, configError("config."+key, "is not supported by cmd@v1"))
		}
	}

	parsed := commandConfig{environment: map[string]values.SecretRef{}}
	parsed.executable = exclusiveString(object, "executable", "command", &findings)
	if parsed.executable != "" && !stableText(parsed.executable, false) {
		findings = append(findings, configError("config.executable", "must be stable UTF-8 without control bytes"))
	}
	parsed.arguments = exclusiveStringList(object, "argv", "arguments", &findings)
	for index, argument := range parsed.arguments {
		if !stableText(argument, true) {
			findings = append(findings, configError(fmt.Sprintf("config.argv[%d]", index), "must be stable UTF-8 without control bytes"))
		}
	}
	parsed.cwd = requiredString(object, "cwd", &findings)
	if parsed.cwd != "" && !stableText(parsed.cwd, false) {
		findings = append(findings, configError("config.cwd", "must be stable UTF-8 without control bytes"))
	}
	parsed.environment = parseEnvironment(object["env"], &findings)
	parsed.timeout = parseTimeout(object["timeout"], &findings)
	parsed.effects = parseEffects(object["effects"], &findings)
	parsed.capabilities = parseCapabilities(object["capabilities"], &findings)
	if !containsString(parsed.capabilities, CapabilityProcessExecute) {
		findings = append(findings, configError("config.capabilities", "must explicitly include process.execute"))
	}
	parsed.sandbox = parseSandbox(object["sandbox"], &findings)
	parsed.captures = parseCaptures(object["capture"], &findings)
	validateCaptureCollisions(parsed.captures, &findings)
	return parsed, findings
}

func cloneConfig(config graph.Config) (map[string]any, error) {
	if config == nil {
		return nil, fmt.Errorf("nil config")
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var cloned map[string]any
	if err := decoder.Decode(&cloned); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("multiple config documents")
	}
	return cloned, nil
}

func exclusiveString(object map[string]any, primary, alias string, findings *[]diagnostic.Diagnostic) string {
	primaryValue, primaryOK := object[primary]
	aliasValue, aliasOK := object[alias]
	if primaryOK == aliasOK {
		*findings = append(*findings, configError("config", fmt.Sprintf("must declare exactly one of %s or %s", primary, alias)))
		return ""
	}
	key, value := primary, primaryValue
	if aliasOK {
		key, value = alias, aliasValue
	}
	text, ok := value.(string)
	if !ok || strings.TrimSpace(text) == "" {
		*findings = append(*findings, configError("config."+key, "must be a non-empty string"))
		return ""
	}
	return text
}

func requiredString(object map[string]any, key string, findings *[]diagnostic.Diagnostic) string {
	value, ok := object[key]
	if !ok {
		*findings = append(*findings, configError("config."+key, "is required"))
		return ""
	}
	text, ok := value.(string)
	if !ok || strings.TrimSpace(text) == "" {
		*findings = append(*findings, configError("config."+key, "must be a non-empty string"))
		return ""
	}
	return text
}

func exclusiveStringList(object map[string]any, primary, alias string, findings *[]diagnostic.Diagnostic) []string {
	primaryValue, primaryOK := object[primary]
	aliasValue, aliasOK := object[alias]
	if primaryOK && aliasOK {
		*findings = append(*findings, configError("config", fmt.Sprintf("must not declare both %s and %s", primary, alias)))
		return nil
	}
	if !primaryOK && !aliasOK {
		return []string{}
	}
	key, value := primary, primaryValue
	if aliasOK {
		key, value = alias, aliasValue
	}
	items, ok := value.([]any)
	if !ok {
		*findings = append(*findings, configError("config."+key, "must be an array of strings"))
		return nil
	}
	result := make([]string, 0, len(items))
	for index, item := range items {
		text, ok := item.(string)
		if !ok {
			*findings = append(*findings, configError(fmt.Sprintf("config.%s[%d]", key, index), "must be a string"))
			continue
		}
		result = append(result, text)
	}
	return result
}

func parseEnvironment(raw any, findings *[]diagnostic.Diagnostic) map[string]values.SecretRef {
	result := map[string]values.SecretRef{}
	if raw == nil {
		return result
	}
	object, ok := raw.(map[string]any)
	if !ok {
		*findings = append(*findings, configError("config.env", "must be an object of secret references"))
		return result
	}
	for _, name := range sortedKeys(object) {
		if !validEnvironmentName(name) {
			*findings = append(*findings, configError("config.env."+name, "uses an invalid portable environment name"))
			continue
		}
		rawRef, ok := object[name].(string)
		if !ok {
			*findings = append(*findings, configError("config.env."+name, "must be an opaque secret:// reference"))
			continue
		}
		ref, err := values.ParseSecretRef(rawRef)
		if err != nil {
			*findings = append(*findings, configError("config.env."+name, "must be a canonical opaque secret:// reference"))
			continue
		}
		result[name] = ref
	}
	return result
}

func parseTimeout(raw any, findings *[]diagnostic.Diagnostic) time.Duration {
	text, ok := raw.(string)
	if !ok || text == "" {
		*findings = append(*findings, configError("config.timeout", "must be a positive duration string"))
		return 0
	}
	duration, err := time.ParseDuration(text)
	if err != nil || duration <= 0 || duration > MaximumTimeout {
		*findings = append(*findings, configError("config.timeout", "must be greater than zero and no more than 24h"))
		return 0
	}
	return duration
}

func parseEffects(raw any, findings *[]diagnostic.Diagnostic) graph.EffectSet {
	items, ok := raw.([]any)
	if !ok || len(items) == 0 {
		*findings = append(*findings, configError("config.effects", "must declare at least one effect expectation"))
		return nil
	}
	set := map[graph.Effect]struct{}{}
	for index, item := range items {
		text, ok := item.(string)
		effect := graph.Effect(text)
		if !ok || !effect.Valid() {
			*findings = append(*findings, configError(fmt.Sprintf("config.effects[%d]", index), "must be a supported effect"))
			continue
		}
		if _, exists := set[effect]; exists {
			*findings = append(*findings, configError(fmt.Sprintf("config.effects[%d]", index), "duplicates an earlier effect"))
			continue
		}
		set[effect] = struct{}{}
	}
	result := make(graph.EffectSet, 0, len(set))
	for effect := range set {
		result = append(result, effect)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func parseCapabilities(raw any, findings *[]diagnostic.Diagnostic) []string {
	items, ok := raw.([]any)
	if !ok || len(items) == 0 {
		*findings = append(*findings, configError("config.capabilities", "must declare capability expectations"))
		return nil
	}
	set := map[string]struct{}{}
	for index, item := range items {
		text, ok := item.(string)
		if !ok || !validCapability(text) {
			*findings = append(*findings, configError(fmt.Sprintf("config.capabilities[%d]", index), "must be a normalized capability name"))
			continue
		}
		if _, exists := set[text]; exists {
			*findings = append(*findings, configError(fmt.Sprintf("config.capabilities[%d]", index), "duplicates an earlier capability"))
			continue
		}
		set[text] = struct{}{}
	}
	result := make([]string, 0, len(set))
	for capability := range set {
		result = append(result, capability)
	}
	sort.Strings(result)
	return result
}

func parseSandbox(raw any, findings *[]diagnostic.Diagnostic) SandboxSpec {
	object, ok := raw.(map[string]any)
	if !ok {
		*findings = append(*findings, configError("config.sandbox", "must be an object with a profile"))
		return SandboxSpec{}
	}
	for _, key := range sortedKeys(object) {
		if key != "profile" {
			*findings = append(*findings, configError("config.sandbox."+key, "is not supported by cmd@v1"))
		}
	}
	profile, ok := object["profile"].(string)
	if !ok || !validCapability(profile) {
		*findings = append(*findings, configError("config.sandbox.profile", "must be a normalized non-empty profile name"))
		return SandboxSpec{}
	}
	return SandboxSpec{Profile: profile}
}

func parseCaptures(raw any, findings *[]diagnostic.Diagnostic) streamCaptures {
	if raw == nil {
		return streamCaptures{}
	}
	object, ok := raw.(map[string]any)
	if !ok {
		*findings = append(*findings, configError("config.capture", "must be an object"))
		return streamCaptures{}
	}
	for _, key := range sortedKeys(object) {
		if key != string(StreamStdout) && key != string(StreamStderr) {
			*findings = append(*findings, configError("config.capture."+key, "must select stdout or stderr"))
		}
	}
	return streamCaptures{
		stdout: parseCapture(StreamStdout, object[string(StreamStdout)], findings),
		stderr: parseCapture(StreamStderr, object[string(StreamStderr)], findings),
	}
}

func parseCapture(stream Stream, raw any, findings *[]diagnostic.Diagnostic) *CaptureConfig {
	if raw == nil {
		return nil
	}
	path := "config.capture." + string(stream)
	object, ok := raw.(map[string]any)
	if !ok {
		*findings = append(*findings, configError(path, "must be an object"))
		return nil
	}
	allowed := map[string]struct{}{"as": {}, "mode": {}, "name": {}, "parse": {}, "max_bytes": {}, "media_type": {}, "compatibility": {}}
	for _, key := range sortedKeys(object) {
		if _, exists := allowed[key]; !exists {
			*findings = append(*findings, configError(path+"."+key, "is not supported by cmd@v1"))
		}
	}
	mode := CaptureOutput
	asValue, hasAs := object["as"]
	modeValue, hasMode := object["mode"]
	if hasAs && hasMode {
		*findings = append(*findings, configError(path, "must not declare both as and mode"))
	} else if hasAs || hasMode {
		value := asValue
		key := "as"
		if hasMode {
			value, key = modeValue, "mode"
		}
		text, ok := value.(string)
		if !ok {
			*findings = append(*findings, configError(path+"."+key, "must be output, artifact, or event_stream"))
		} else {
			mode = CaptureMode(text)
		}
	}
	if mode != CaptureOutput && mode != CaptureArtifact && mode != CaptureEvent {
		*findings = append(*findings, configError(path+".as", "must be output, artifact, or event_stream"))
	}
	name, nameOK := object["name"].(string)
	if _, exists := object["name"]; exists && !nameOK {
		*findings = append(*findings, configError(path+".name", "must be a string"))
	}
	parseText, parseOK := object["parse"].(string)
	if _, exists := object["parse"]; exists && !parseOK {
		*findings = append(*findings, configError(path+".parse", "must be a string"))
	}
	parse := ParseMode(parseText)
	compatibility, compatibilityOK := object["compatibility"].(bool)
	if _, exists := object["compatibility"]; exists && !compatibilityOK {
		*findings = append(*findings, configError(path+".compatibility", "must be a boolean"))
	}
	mediaType, mediaOK := object["media_type"].(string)
	if _, exists := object["media_type"]; exists && !mediaOK {
		*findings = append(*findings, configError(path+".media_type", "must be a string"))
	}
	if mediaType != "" {
		metadata := values.Metadata{
			Producer: values.Producer{Kind: "cmd-config", Reference: "validation"}, MediaType: mediaType,
			Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		}
		if err := metadata.Validate(); err != nil {
			*findings = append(*findings, configError(path+".media_type", "must be a valid media type"))
		}
	}
	maxBytes := int64(0)
	if rawMax, exists := object["max_bytes"]; exists {
		maxBytes = parsePositiveInt64(rawMax, path+".max_bytes", findings)
	}

	capture := &CaptureConfig{Mode: mode, Name: name, Parse: parse, MaxBytes: maxBytes, MediaType: mediaType, Compatibility: compatibility}
	switch mode {
	case CaptureOutput:
		if capture.MaxBytes == 0 {
			capture.MaxBytes = DefaultCaptureBytes
		}
		if capture.MaxBytes > values.MaximumInlineLimit {
			*findings = append(*findings, configError(path+".max_bytes", "output capture must not exceed the 1 MiB inline ceiling"))
		}
		if capture.Parse == "" {
			capture.Parse = ParseText
		}
		if !validOutputParser(capture.Parse) {
			*findings = append(*findings, configError(path+".parse", "must be text, json, lines, kv, or set-output"))
		}
		if capture.Parse == ParseSetOutput {
			if !capture.Compatibility || capture.Name != "" {
				*findings = append(*findings, configError(path, "set-output requires compatibility: true and no fixed name"))
			}
			*findings = append(*findings, compatibilityWarning(path))
		} else {
			if capture.Compatibility {
				*findings = append(*findings, configError(path+".compatibility", "is allowed only with parse: set-output"))
			}
			if err := graph.ValidateID(capture.Name); err != nil {
				*findings = append(*findings, configError(path+".name", "must be a normalized output identifier"))
			}
		}
	case CaptureArtifact:
		if capture.MaxBytes == 0 {
			capture.MaxBytes = MaximumCaptureBytes
		}
		if capture.MaxBytes > MaximumCaptureBytes {
			*findings = append(*findings, configError(path+".max_bytes", "artifact capture exceeds the cmd@v1 hard ceiling"))
		}
		if capture.Parse != "" || capture.Compatibility {
			*findings = append(*findings, configError(path, "artifact capture stores opaque bytes and cannot parse compatibility output"))
		}
		if err := graph.ValidateID(capture.Name); err != nil {
			*findings = append(*findings, configError(path+".name", "must be a normalized output identifier"))
		}
	case CaptureEvent:
		if capture.MaxBytes == 0 {
			capture.MaxBytes = DefaultCaptureBytes
		}
		if capture.MaxBytes > values.MaximumInlineLimit {
			*findings = append(*findings, configError(path+".max_bytes", "event_stream emission must not exceed the 1 MiB operational ceiling"))
		}
		if capture.Name != "" || capture.Parse != "" || capture.MediaType != "" || capture.Compatibility {
			*findings = append(*findings, configError(path, "event_stream cannot declare an output, parser, media type, or compatibility"))
		}
	}
	return capture
}

func validateCaptureCollisions(captures streamCaptures, findings *[]diagnostic.Diagnostic) {
	seen := map[string]string{"exit_code": "reserved output"}
	compatibility := 0
	for _, item := range []struct {
		stream  Stream
		capture *CaptureConfig
	}{{StreamStdout, captures.stdout}, {StreamStderr, captures.stderr}} {
		capture := item.capture
		if capture == nil {
			continue
		}
		if capture.Parse == ParseSetOutput && capture.Compatibility {
			compatibility++
		}
		if capture.Name == "" {
			continue
		}
		if earlier, exists := seen[capture.Name]; exists {
			*findings = append(*findings, configError("config.capture."+string(item.stream)+".name", fmt.Sprintf("collides with %s %q", earlier, capture.Name)))
			continue
		}
		seen[capture.Name] = string(item.stream) + " capture"
	}
	if compatibility > 1 {
		*findings = append(*findings, configError("config.capture", "may enable set-output compatibility for exactly one stream"))
	}
}

func parsePositiveInt64(raw any, path string, findings *[]diagnostic.Diagnostic) int64 {
	number, ok := raw.(json.Number)
	if !ok {
		*findings = append(*findings, configError(path, "must be a positive integer"))
		return 0
	}
	value, err := strconv.ParseInt(string(number), 10, 64)
	if err != nil || value <= 0 || value > math.MaxInt64-1 {
		*findings = append(*findings, configError(path, "must be a positive bounded integer"))
		return 0
	}
	return value
}

func validOutputParser(parser ParseMode) bool {
	return parser == ParseText || parser == ParseJSON || parser == ParseLines || parser == ParseKV || parser == ParseSetOutput
}

func validEnvironmentName(name string) bool {
	if name == "" || !utf8.ValidString(name) {
		return false
	}
	for index, character := range name {
		if (character >= 'A' && character <= 'Z') || character == '_' || (index > 0 && character >= '0' && character <= '9') {
			continue
		}
		return false
	}
	return true
}

func validCapability(value string) bool {
	if value == "" || strings.TrimSpace(value) != value || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') || character == '.' || character == '-' || character == '_' {
			continue
		}
		return false
	}
	return true
}

func stableText(value string, allowEmpty bool) bool {
	if !utf8.ValidString(value) || (!allowEmpty && strings.TrimSpace(value) == "") || strings.ContainsRune(value, 0) {
		return false
	}
	return !strings.ContainsFunc(value, unicode.IsControl)
}

func containsString(values []string, sought string) bool {
	index := sort.SearchStrings(values, sought)
	return index < len(values) && values[index] == sought
}

func sortedKeys(object map[string]any) []string {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func configError(path, message string) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{
		Severity:    diagnostic.SeverityError,
		Code:        stepkind.CodeInvalidConfig,
		Message:     fmt.Sprintf("cmd %s %s", path, message),
		Remediation: &diagnostic.Remediation{Message: "Use the documented cmd@v1 direct-execution configuration."},
	}
}

func compatibilityWarning(path string) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{
		Severity:    diagnostic.SeverityWarning,
		Code:        stepkind.CodeInvalidConfig,
		Message:     fmt.Sprintf("cmd %s enables deprecated set-output compatibility parsing", path),
		Remediation: &diagnostic.Remediation{Message: "Return a named typed output instead of emitting ::set-output records."},
	}
}

func hasErrors(findings []diagnostic.Diagnostic) bool {
	for _, finding := range findings {
		if finding.Severity == diagnostic.SeverityError {
			return true
		}
	}
	return false
}

func validateResolved(resolved ResolvedCommand) error {
	if !stableText(resolved.Executable, false) || !filepath.IsAbs(resolved.Executable) || filepath.Clean(resolved.Executable) != resolved.Executable {
		return fmt.Errorf("resolved executable must be an absolute clean path")
	}
	if !stableText(resolved.CWD, false) || !filepath.IsAbs(resolved.CWD) || filepath.Clean(resolved.CWD) != resolved.CWD {
		return fmt.Errorf("resolved cwd must be an absolute clean path")
	}
	for _, argument := range resolved.Arguments {
		if !stableText(argument, true) {
			return fmt.Errorf("resolved argument is invalid")
		}
	}
	if len(resolved.EffectiveEffects) == 0 {
		return fmt.Errorf("resolved effects are required")
	}
	seenEffects := map[graph.Effect]struct{}{}
	for _, effect := range resolved.EffectiveEffects {
		if !effect.Valid() {
			return fmt.Errorf("resolved effect is invalid")
		}
		if _, exists := seenEffects[effect]; exists {
			return fmt.Errorf("resolved effects contain duplicates")
		}
		seenEffects[effect] = struct{}{}
	}
	capabilities := append([]string(nil), resolved.EffectiveCapabilities...)
	sort.Strings(capabilities)
	if !containsString(capabilities, CapabilityProcessExecute) {
		return fmt.Errorf("resolved capabilities omit process.execute")
	}
	for index, capability := range capabilities {
		if !validCapability(capability) || (index > 0 && capability == capabilities[index-1]) {
			return fmt.Errorf("resolved capabilities are invalid")
		}
	}
	if !validCapability(resolved.Sandbox.Profile) {
		return fmt.Errorf("resolved sandbox profile is invalid")
	}
	return nil
}

func captureSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"properties": map[string]any{
			"as":            map[string]any{"enum": []any{"output", "artifact", "event_stream"}},
			"mode":          map[string]any{"enum": []any{"output", "artifact", "event_stream"}},
			"name":          map[string]any{"type": "string"},
			"parse":         map[string]any{"enum": []any{"text", "json", "lines", "kv", "set-output"}},
			"max_bytes":     map[string]any{"type": "integer", "minimum": json.Number("1"), "maximum": json.Number(strconv.FormatInt(MaximumCaptureBytes, 10))},
			"media_type":    map[string]any{"type": "string"},
			"compatibility": map[string]any{"type": "boolean"},
		},
	}
}

func configSchema() graph.Schema {
	return graph.Schema{
		"type": "object", "additionalProperties": false,
		"properties": map[string]any{
			"executable":   map[string]any{"type": "string", "minLength": json.Number("1")},
			"command":      map[string]any{"type": "string", "minLength": json.Number("1")},
			"argv":         map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"arguments":    map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"cwd":          map[string]any{"type": "string", "minLength": json.Number("1")},
			"env":          map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string", "pattern": `^secret://`}},
			"timeout":      map[string]any{"type": "string", "minLength": json.Number("1")},
			"effects":      map[string]any{"type": "array", "minItems": json.Number("1"), "uniqueItems": true, "items": map[string]any{"enum": []any{"read", "compute", "materialize", "mutate", "destructive"}}},
			"capabilities": map[string]any{"type": "array", "minItems": json.Number("1"), "uniqueItems": true, "items": map[string]any{"type": "string", "minLength": json.Number("1")}},
			"sandbox":      map[string]any{"type": "object", "required": []any{"profile"}, "additionalProperties": false, "properties": map[string]any{"profile": map[string]any{"type": "string", "minLength": json.Number("1")}}},
			"capture":      map[string]any{"type": "object", "additionalProperties": false, "properties": map[string]any{"stdout": captureSchema(), "stderr": captureSchema()}},
		},
		"required": []any{"cwd", "timeout", "effects", "capabilities", "sandbox"},
		"oneOf": []any{
			map[string]any{"required": []any{"executable"}, "not": map[string]any{"required": []any{"command"}}},
			map[string]any{"required": []any{"command"}, "not": map[string]any{"required": []any{"executable"}}},
		},
	}
}

func outputSchema() graph.Schema {
	return graph.Schema{
		"type":                 "object",
		"required":             []any{"exit_code"},
		"properties":           map[string]any{"exit_code": map[string]any{"type": "integer"}},
		"additionalProperties": true,
	}
}
