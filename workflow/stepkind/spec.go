package stepkind

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	// CodeInvalidSpec identifies missing or invalid step-kind metadata.
	CodeInvalidSpec diagnostic.Code = "HADR-HOST-001"
	// CodeLifecycleMismatch identifies disagreement between advertised metadata
	// and implemented optional interfaces.
	CodeLifecycleMismatch diagnostic.Code = "HADR-HOST-002"
	// CodeInvalidConfig is available to step kinds for config diagnostics that
	// do not need a more specific source-validation assignment.
	CodeInvalidConfig diagnostic.Code = "HADR-SOURCE-100"
)

const maxIdentityLength = 128

// SpecError contains structured diagnostics for an invalid registration spec.
type SpecError struct {
	Diagnostics []diagnostic.Diagnostic
}

// Error implements error while retaining Diagnostics for callers that must not
// parse presentation strings.
func (e *SpecError) Error() string {
	if e == nil || len(e.Diagnostics) == 0 {
		return "invalid step-kind spec"
	}
	var message strings.Builder
	message.WriteString("invalid step-kind spec")
	for _, finding := range e.Diagnostics {
		fmt.Fprintf(&message, "; %s: %s", finding.Code, finding.Message)
	}
	return message.String()
}

// ValidateSpec validates metadata independently of an executor implementation.
// Schemas must compile under the core's local-only JSON Schema contract;
// adapter-specific config validation remains owned by ValidateConfig.
func ValidateSpec(spec StepKindSpec) error {
	diagnostics := validateSpec(spec)
	if len(diagnostics) == 0 {
		return nil
	}
	return &SpecError{Diagnostics: diagnostics}
}

func validateSpec(spec StepKindSpec) []diagnostic.Diagnostic {
	var findings []diagnostic.Diagnostic
	add := func(field, reason string) {
		findings = append(findings, diagnostic.Diagnostic{
			Severity: diagnostic.SeverityError,
			Code:     CodeInvalidSpec,
			Message:  fmt.Sprintf("step kind %s %s", field, reason),
		})
	}

	validateIdentity("name", spec.Name, add)
	validateIdentity("version", spec.Version, add)
	if spec.ConfigSchema == nil {
		add("config_schema", "is required")
	} else {
		validateSchema("config_schema", spec.ConfigSchema, add)
	}
	if spec.InputSchema == nil {
		add("input_schema", "is required")
	} else {
		validateSchema("input_schema", spec.InputSchema, add)
	}
	if spec.OutputSchema == nil {
		add("output_schema", "is required")
	} else {
		validateSchema("output_schema", spec.OutputSchema, add)
	}
	if len(spec.Effects) == 0 {
		add("effects", "must declare at least one effect")
	}
	seenEffects := make(map[graph.Effect]struct{}, len(spec.Effects))
	for _, effect := range spec.Effects {
		if !effect.Valid() {
			add("effects", fmt.Sprintf("contains unsupported effect %q", effect))
		}
		if _, duplicate := seenEffects[effect]; duplicate {
			add("effects", fmt.Sprintf("contains duplicate effect %q", effect))
		}
		seenEffects[effect] = struct{}{}
	}
	if !spec.Idempotency.Valid() {
		add("idempotency", fmt.Sprintf("has unsupported mode %q", spec.Idempotency))
	}
	if !spec.RetrySafety.Valid() {
		add("retry_safety", fmt.Sprintf("has unsupported value %q", spec.RetrySafety))
	}
	if !spec.Memoization.Valid() {
		add("memoization", fmt.Sprintf("has unsupported value %q", spec.Memoization))
	}
	if !spec.Compensation.Valid() {
		add("compensation", fmt.Sprintf("has unsupported value %q", spec.Compensation))
	}
	if !spec.Cancellation.Mode.Valid() {
		add("cancellation.mode", fmt.Sprintf("has unsupported value %q", spec.Cancellation.Mode))
	}
	if !spec.Observation.Mode.Valid() {
		add("observation.mode", fmt.Sprintf("has unsupported value %q", spec.Observation.Mode))
	}
	if spec.Observation.Heartbeat && spec.Observation.Mode == ObservationNone {
		add("observation.heartbeat", "requires polling observation")
	}
	if spec.Lifecycle.Service {
		if spec.Observation.Mode != ObservationPoll || !spec.Observation.Heartbeat {
			add("lifecycle.service", "requires polling observation with heartbeat")
		}
		if spec.Cancellation.Mode != CancellationExplicit {
			add("lifecycle.service", "requires explicit durable stop cancellation")
		}
		if spec.CanSuspend {
			add("lifecycle.service", "uses the service coordinator and cannot advertise generic wait suspension")
		}
	}

	seenCapabilities := make(map[string]struct{}, len(spec.RequiredCapabilities))
	for _, capability := range spec.RequiredCapabilities {
		if !utf8.ValidString(capability) {
			add("required_capabilities", "contains invalid UTF-8")
			continue
		}
		if strings.TrimSpace(capability) == "" || capability != strings.TrimSpace(capability) {
			add("required_capabilities", "contains an empty or untrimmed capability")
			continue
		}
		if _, duplicate := seenCapabilities[capability]; duplicate {
			add("required_capabilities", fmt.Sprintf("contains duplicate capability %q", capability))
		}
		seenCapabilities[capability] = struct{}{}
	}
	return findings
}

func validateIdentity(field, value string, add func(string, string)) {
	if strings.TrimSpace(value) == "" {
		add(field, "is required")
		return
	}
	if value != strings.TrimSpace(value) {
		add(field, "must not contain leading or trailing whitespace")
	}
	if len(value) > maxIdentityLength {
		add(field, fmt.Sprintf("must be at most %d bytes", maxIdentityLength))
	}
	if !utf8.ValidString(value) {
		add(field, "must be valid UTF-8")
		return
	}
	for _, r := range value {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			add(field, "must not contain whitespace or control characters")
			return
		}
	}
}

func validateImplementation(kind StepKind, spec StepKindSpec) error {
	var findings []diagnostic.Diagnostic
	add := func(field, expected string) {
		findings = append(findings, diagnostic.Diagnostic{
			Severity: diagnostic.SeverityError,
			Code:     CodeLifecycleMismatch,
			Message:  fmt.Sprintf("step kind %s metadata must %s", field, expected),
		})
	}

	_, prepares := kind.(Preparer)
	if prepares != spec.Lifecycle.Prepare {
		add("lifecycle.prepare", matchOptionalInterface(spec.Lifecycle.Prepare, "Preparer"))
	}
	_, finalizes := kind.(Finalizer)
	if finalizes != spec.Lifecycle.Finalize {
		add("lifecycle.finalize", matchOptionalInterface(spec.Lifecycle.Finalize, "Finalizer"))
	}
	_, services := kind.(ServiceController)
	if services != spec.Lifecycle.Service {
		add("lifecycle.service", matchOptionalInterface(spec.Lifecycle.Service, "ServiceController"))
	}
	_, observes := kind.(Observer)
	wantObserver := spec.Observation.Mode != ObservationNone
	if spec.Observation.Mode.Valid() && !spec.Lifecycle.Service && observes != wantObserver {
		add("observation.mode", matchOptionalInterface(wantObserver, "Observer"))
	}
	_, heartbeats := kind.(Heartbeater)
	if !spec.Lifecycle.Service && heartbeats != spec.Observation.Heartbeat {
		add("observation.heartbeat", matchOptionalInterface(spec.Observation.Heartbeat, "Heartbeater"))
	}
	_, cancels := kind.(Canceler)
	wantCanceler := spec.Cancellation.Mode == CancellationExplicit
	if spec.Cancellation.Mode.Valid() && !spec.Lifecycle.Service && cancels != wantCanceler {
		add("cancellation.mode", matchOptionalInterface(wantCanceler, "Canceler"))
	}
	_, reversible := kind.(ReversibilityProvider)
	wantReversible := spec.Compensation == CompensationReceiptRequired
	if spec.Compensation.Valid() && reversible != wantReversible {
		add("compensation", matchOptionalInterface(wantReversible, "ReversibilityProvider"))
	}

	if len(findings) == 0 {
		return nil
	}
	return &SpecError{Diagnostics: findings}
}

func matchOptionalInterface(advertised bool, name string) string {
	if advertised {
		return "be backed by the " + name + " interface"
	}
	return "advertise the implemented " + name + " interface"
}

func cloneSpec(spec StepKindSpec) StepKindSpec {
	cloned := spec
	cloned.ConfigSchema = cloneSchema(spec.ConfigSchema)
	cloned.InputSchema = cloneSchema(spec.InputSchema)
	cloned.OutputSchema = cloneSchema(spec.OutputSchema)
	cloned.Effects = append(graph.EffectSet(nil), spec.Effects...)
	sort.Slice(cloned.Effects, func(i, j int) bool { return cloned.Effects[i] < cloned.Effects[j] })
	cloned.RequiredCapabilities = append([]string(nil), spec.RequiredCapabilities...)
	sort.Strings(cloned.RequiredCapabilities)
	return cloned
}

func cloneSchema(schema graph.Schema) graph.Schema {
	if schema == nil {
		return nil
	}
	return cloneSchemaReflect(reflect.ValueOf(schema)).Interface().(graph.Schema)
}

func cloneSchemaReflect(value reflect.Value) reflect.Value {
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := reflect.New(value.Type()).Elem()
		cloned.Set(cloneSchemaReflect(value.Elem()))
		return cloned
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := reflect.MakeMapWithSize(value.Type(), value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			cloned.SetMapIndex(iterator.Key(), cloneSchemaReflect(iterator.Value()))
		}
		return cloned
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := range value.Len() {
			cloned.Index(i).Set(cloneSchemaReflect(value.Index(i)))
		}
		return cloned
	case reflect.Array:
		cloned := reflect.New(value.Type()).Elem()
		for i := range value.Len() {
			cloned.Index(i).Set(cloneSchemaReflect(value.Index(i)))
		}
		return cloned
	default:
		return value
	}
}

type schemaVisit struct {
	typeOf  reflect.Type
	pointer uintptr
}

type schemaValidator struct {
	active map[schemaVisit]string
	add    func(string, string)
}

func validateSchema(field string, schema graph.Schema, add func(string, string)) {
	validator := schemaValidator{
		active: make(map[schemaVisit]string),
		add:    add,
	}
	validator.validate(reflect.ValueOf(schema), field)
	if err := values.ValidateSchema(schema); err != nil {
		add(field, fmt.Sprintf("is not a valid local JSON Schema: %v", err))
	}
}

func (v *schemaValidator) validate(value reflect.Value, path string) {
	if !value.IsValid() {
		return
	}
	if value.Type() == reflect.TypeFor[json.RawMessage]() {
		if !value.IsNil() && !json.Valid(value.Bytes()) {
			v.add(path, "contains invalid raw JSON")
		}
		return
	}

	switch value.Kind() {
	case reflect.Interface:
		if !value.IsNil() {
			v.validate(value.Elem(), path)
		}
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return
	case reflect.Float32, reflect.Float64:
		if number := value.Float(); math.IsNaN(number) || math.IsInf(number, 0) {
			v.add(path, "contains a non-finite number")
		}
	case reflect.String:
		text := value.String()
		if !utf8.ValidString(text) {
			v.add(path, "contains invalid UTF-8")
			return
		}
		if value.Type() == reflect.TypeFor[json.Number]() {
			if _, err := json.Marshal(value.Interface()); err != nil {
				v.add(path, "contains an invalid JSON number")
			}
		}
	case reflect.Map:
		v.validateMap(value, path)
	case reflect.Slice:
		v.validateSlice(value, path)
	case reflect.Array:
		for i := range value.Len() {
			v.validate(value.Index(i), fmt.Sprintf("%s[%d]", path, i))
		}
	default:
		v.add(path, fmt.Sprintf("contains non-JSON-compatible type %s", value.Type()))
	}
}

func (v *schemaValidator) validateMap(value reflect.Value, path string) {
	if value.IsNil() {
		return
	}
	if value.Type().Key().Kind() != reflect.String {
		v.add(path, fmt.Sprintf("contains map with non-string key type %s", value.Type().Key()))
		return
	}
	if !v.enter(value, path) {
		return
	}
	defer v.leave(value)

	keys := value.MapKeys()
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	for _, key := range keys {
		name := key.String()
		if !utf8.ValidString(name) {
			v.add(path, "contains an object key with invalid UTF-8")
			continue
		}
		v.validate(value.MapIndex(key), path+"."+name)
	}
}

func (v *schemaValidator) validateSlice(value reflect.Value, path string) {
	if value.IsNil() {
		return
	}
	if !v.enter(value, path) {
		return
	}
	defer v.leave(value)

	for i := range value.Len() {
		v.validate(value.Index(i), fmt.Sprintf("%s[%d]", path, i))
	}
}

func (v *schemaValidator) enter(value reflect.Value, path string) bool {
	visit := schemaVisit{typeOf: value.Type(), pointer: value.Pointer()}
	if earlier, exists := v.active[visit]; exists {
		v.add(path, "contains a cycle to "+earlier)
		return false
	}
	v.active[visit] = path
	return true
}

func (v *schemaValidator) leave(value reflect.Value) {
	delete(v.active, schemaVisit{typeOf: value.Type(), pointer: value.Pointer()})
}

func joinSpecErrors(errs ...error) error {
	var findings []diagnostic.Diagnostic
	for _, err := range errs {
		var specErr *SpecError
		if errors.As(err, &specErr) {
			findings = append(findings, specErr.Diagnostics...)
		}
	}
	if len(findings) == 0 {
		return nil
	}
	return &SpecError{Diagnostics: findings}
}
