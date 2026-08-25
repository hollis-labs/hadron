package generatedapi

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	httpadapter "github.com/hollis-labs/hadron/workflow/adapters/http"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
)

const (
	// SourceFamilyOpenAPI is the stable source-family identifier.
	SourceFamilyOpenAPI = "openapi"
	// DefaultMaxSpecBytes is the default source-document bound.
	DefaultMaxSpecBytes int64 = 2 << 20
	maximumSpecBytes    int64 = 16 << 20
	defaultMaxResponse        = int64(1 << 20)
)

var (
	// ErrInvalidOptions identifies an invalid generator dependency or bound.
	ErrInvalidOptions = errors.New("invalid generated API options")
	// ErrInvalidSource identifies an unsupported or malformed source document.
	ErrInvalidSource = errors.New("invalid generated API source")
	// ErrInvalidInvocation identifies input or runtime config that does not
	// satisfy the generated operation contract.
	ErrInvalidInvocation = errors.New("invalid generated API invocation")
)

// Options supplies the stable catalog namespace and the only execution
// implementation used by generated HTTP operations.
type Options struct {
	Namespace    string
	HTTP         *httpadapter.Kind
	MaxSpecBytes int64
}

// CredentialDescription is the non-secret generated credential contract.
// Input names a typed secret_ref input; it never contains credential material
// or the reference selected by a workflow invocation.
type CredentialDescription struct {
	Input    string `json:"input"`
	Scheme   string `json:"scheme"`
	Kind     string `json:"kind"`
	Header   string `json:"header,omitempty"`
	Username string `json:"username,omitempty"`
}

// OperationDescription is the immutable generated catalog projection used by
// authoring and policy surfaces before an operation executes.
type OperationDescription struct {
	SourceFamily         string                  `json:"source_family"`
	SourceDigest         string                  `json:"source_digest"`
	SourceTitle          string                  `json:"source_title"`
	SourceVersion        string                  `json:"source_version"`
	Name                 string                  `json:"name"`
	Version              string                  `json:"version"`
	OperationID          string                  `json:"operation_id"`
	Method               string                  `json:"method"`
	Origin               string                  `json:"origin"`
	PathTemplate         string                  `json:"path_template"`
	ConfigSchema         graph.Schema            `json:"config_schema"`
	InputSchema          graph.Schema            `json:"input_schema"`
	OutputSchema         graph.Schema            `json:"output_schema"`
	Effects              graph.EffectSet         `json:"effects"`
	RequiredCapabilities []string                `json:"required_capabilities"`
	Credentials          []CredentialDescription `json:"credentials,omitempty"`
	SuccessStatuses      []int                   `json:"success_statuses"`
}

// Family is one immutable generated catalog. Its step kinds are ordinary
// registry entries and do not introduce an alternate execution path.
type Family struct {
	digest     string
	operations []*operation
}

// SourceDigest returns the digest of the canonical source document.
func (f *Family) SourceDigest() string {
	if f == nil {
		return ""
	}
	return f.digest
}

// Operations returns deterministic defensive catalog projections.
func (f *Family) Operations() []OperationDescription {
	if f == nil {
		return nil
	}
	result := make([]OperationDescription, len(f.operations))
	for index, operation := range f.operations {
		result[index] = cloneDescription(operation.description)
	}
	return result
}

// Kinds returns generated kinds ordered by stable name and version.
func (f *Family) Kinds() []stepkind.StepKind {
	if f == nil {
		return nil
	}
	result := make([]stepkind.StepKind, len(f.operations))
	for index, operation := range f.operations {
		result[index] = &Kind{operation: operation}
	}
	return result
}

// Register adds every generated kind to registry in deterministic order.
func (f *Family) Register(registry stepkind.Registry) error {
	if f == nil {
		return fmt.Errorf("%w: family is required", ErrInvalidOptions)
	}
	if nilInterface(registry) {
		return fmt.Errorf("%w: registry is required", ErrInvalidOptions)
	}
	for _, kind := range f.Kinds() {
		if err := registry.Register(kind); err != nil {
			return fmt.Errorf("register generated kind %s@%s: %w", kind.Spec().Name, kind.Spec().Version, err)
		}
	}
	return nil
}

// Kind is one generated operation implemented through http@v1.
type Kind struct {
	operation *operation
}

// Description returns a defensive non-secret operation projection.
func (k *Kind) Description() OperationDescription {
	if k == nil || k.operation == nil {
		return OperationDescription{}
	}
	return cloneDescription(k.operation.description)
}

type operation struct {
	http           *httpadapter.Kind
	description    OperationDescription
	server         string
	parameters     []parameter
	bodyInput      string
	credential     *credential
	responseSchema graph.Schema
}

type parameter struct {
	SourceName string
	InputName  string
	Location   string
	Required   bool
	Array      bool
	Schema     graph.Schema
}

type credential struct {
	SourceName string
	InputName  string
	Kind       string
	Header     string
	Username   string
}

func validateOptions(options Options) (Options, error) {
	if err := graph.ValidateID(options.Namespace); err != nil {
		return Options{}, fmt.Errorf("%w: namespace: %w", ErrInvalidOptions, err)
	}
	if options.HTTP == nil {
		return Options{}, fmt.Errorf("%w: HTTP adapter is required", ErrInvalidOptions)
	}
	if options.MaxSpecBytes == 0 {
		options.MaxSpecBytes = DefaultMaxSpecBytes
	}
	if options.MaxSpecBytes < 1 || options.MaxSpecBytes > maximumSpecBytes {
		return Options{}, fmt.Errorf("%w: max spec bytes must be between 1 and %d", ErrInvalidOptions, maximumSpecBytes)
	}
	return options, nil
}

func cloneDescription(input OperationDescription) OperationDescription {
	result := input
	result.ConfigSchema = cloneSchema(input.ConfigSchema)
	result.InputSchema = cloneSchema(input.InputSchema)
	result.OutputSchema = cloneSchema(input.OutputSchema)
	result.Effects = append(graph.EffectSet(nil), input.Effects...)
	result.RequiredCapabilities = append([]string(nil), input.RequiredCapabilities...)
	result.Credentials = append([]CredentialDescription(nil), input.Credentials...)
	result.SuccessStatuses = append([]int(nil), input.SuccessStatuses...)
	return result
}

func cloneSchema(input graph.Schema) graph.Schema {
	if input == nil {
		return nil
	}
	return cloneValue(input).(map[string]any)
}

func cloneValue(input any) any {
	switch typed := input.(type) {
	case graph.Schema:
		result := make(map[string]any, len(typed))
		for key, value := range typed {
			result[key] = cloneValue(value)
		}
		return result
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, value := range typed {
			result[key] = cloneValue(value)
		}
		return result
	case []any:
		result := make([]any, len(typed))
		for index, value := range typed {
			result[index] = cloneValue(value)
		}
		return result
	default:
		return typed
	}
}

func sortedStrings(input []string) []string {
	result := append([]string(nil), input...)
	sort.Strings(result)
	return result
}

func containsEffect(haystack graph.EffectSet, needle graph.Effect) bool {
	for _, effect := range haystack {
		if effect == needle {
			return true
		}
	}
	return false
}

func effectsContained(actual, declared graph.EffectSet) bool {
	for _, effect := range actual {
		if !containsEffect(declared, effect) {
			return false
		}
	}
	return true
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func invalidSource(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidSource, fmt.Sprintf(format, args...))
}

func invalidInvocation(code, message string, cause error) error {
	return &stepkind.ExecutionError{
		Code: code, Message: message, Classification: stepkind.RetryPermanent, Cause: cause,
	}
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return invalidSource("context is required")
	}
	return ctx.Err()
}

func stableText(value string) bool {
	if value == "" || !utf8.ValidString(value) || value != strings.TrimSpace(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}
