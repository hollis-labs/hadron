package script

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"

	"github.com/dop251/goja"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const maximumSafeInteger = int64(1<<53 - 1)

type structuralBudget struct {
	limits ResourceLimits
	items  int
}

func inputPayload(input values.ValueSet, limits ResourceLimits) ([]byte, error) {
	if err := input.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidInput, err)
	}
	payload := make(map[string]any, len(input))
	names := make([]string, 0, len(input))
	for name := range input {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value := input[name]
		if value.Type == values.TypeSecretRef || value.Redaction == values.RedactionSecret {
			return nil, fmt.Errorf("%w: input %q is secret-classified and no secret capability is installed", ErrCapabilityDenied, name)
		}
		if value.Type == values.TypeArtifact {
			return nil, fmt.Errorf("%w: input %q is an artifact and no artifact resolver capability is installed", ErrCapabilityDenied, name)
		}
		payload[name] = value.Inline
	}
	budget := structuralBudget{limits: limits}
	if err := budget.check(payload, 1, "inputs"); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidInput, err)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: encode canonical input: %w", ErrInvalidInput, err)
	}
	if len(encoded) > limits.MaxInputBytes {
		return nil, fmt.Errorf("%w: input canonical JSON exceeds max_input_bytes", ErrResourceLimit)
	}
	return encoded, nil
}

func (b *structuralBudget) check(value any, depth int, path string) error {
	if depth > b.limits.MaxDepth {
		return fmt.Errorf("%w: %s exceeds max_depth", ErrResourceLimit, path)
	}
	switch typed := value.(type) {
	case nil, bool:
		return nil
	case string:
		if len(typed) > b.limits.MaxStringBytes {
			return fmt.Errorf("%w: %s exceeds max_string_bytes", ErrResourceLimit, path)
		}
		return nil
	case json.Number:
		if err := validateJavaScriptNumber(typed); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		return nil
	case []any:
		if err := b.addItems(len(typed), path); err != nil {
			return err
		}
		for index, item := range typed {
			if err := b.check(item, depth+1, fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
		return nil
	case map[string]any:
		if err := b.addItems(len(typed), path); err != nil {
			return err
		}
		keys := sortedKeys(typed)
		for _, key := range keys {
			if len(key) > b.limits.MaxStringBytes {
				return fmt.Errorf("%w: %s property name exceeds max_string_bytes", ErrResourceLimit, path)
			}
			if err := b.check(typed[key], depth+1, path+"."+key); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("%s contains unsupported JSON value %T", path, value)
	}
}

func (b *structuralBudget) addItems(count int, path string) error {
	if count > b.limits.MaxItems-b.items {
		return fmt.Errorf("%w: %s exceeds max_items", ErrResourceLimit, path)
	}
	b.items += count
	return nil
}

func validateJavaScriptNumber(number json.Number) error {
	raw := number.String()
	if len(raw) > 128 {
		return fmt.Errorf("%w: numeric lexeme exceeds the exact-conversion bound", ErrUnsafeJSONNumber)
	}
	parsed, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return fmt.Errorf("%w: invalid finite number", ErrUnsafeJSONNumber)
	}
	if parsed == 0 && decimalMantissaNonzero(raw) {
		return fmt.Errorf("%w: nonzero number underflows JavaScript float64", ErrUnsafeJSONNumber)
	}
	if !strings.ContainsAny(raw, ".eE") {
		integer, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || integer < -maximumSafeInteger || integer > maximumSafeInteger {
			return fmt.Errorf("%w: integer is outside the safe JavaScript range", ErrUnsafeJSONNumber)
		}
	}
	original, ok := exactDecimalRat(raw)
	converted := new(big.Rat).SetFloat64(parsed)
	if !ok || converted == nil || original.Cmp(converted) != 0 {
		return fmt.Errorf("%w: number is not exactly representable as a JavaScript float64", ErrUnsafeJSONNumber)
	}
	return nil
}

func exactDecimalRat(raw string) (*big.Rat, bool) {
	mantissa := raw
	exponent := 0
	if index := strings.IndexAny(mantissa, "eE"); index >= 0 {
		parsedExponent, err := strconv.Atoi(mantissa[index+1:])
		if err != nil || parsedExponent < -400 || parsedExponent > 400 {
			return nil, false
		}
		exponent = parsedExponent
		mantissa = mantissa[:index]
	}
	negative := strings.HasPrefix(mantissa, "-")
	mantissa = strings.TrimPrefix(strings.TrimPrefix(mantissa, "-"), "+")
	scale := 0
	if index := strings.IndexByte(mantissa, '.'); index >= 0 {
		scale = len(mantissa) - index - 1
		mantissa = mantissa[:index] + mantissa[index+1:]
	}
	if mantissa == "" {
		return nil, false
	}
	numerator := new(big.Int)
	if _, ok := numerator.SetString(mantissa, 10); !ok {
		return nil, false
	}
	if negative {
		numerator.Neg(numerator)
	}
	decimalScale := scale - exponent
	if decimalScale < -400 || decimalScale > 400 {
		return nil, false
	}
	if decimalScale <= 0 {
		numerator.Mul(numerator, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-decimalScale)), nil))
		return new(big.Rat).SetInt(numerator), true
	}
	denominator := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimalScale)), nil)
	return new(big.Rat).SetFrac(numerator, denominator), true
}

func decimalMantissaNonzero(raw string) bool {
	if index := strings.IndexAny(raw, "eE"); index >= 0 {
		raw = raw[:index]
	}
	for _, character := range raw {
		if character >= '1' && character <= '9' {
			return true
		}
	}
	return false
}

type gojaExporter struct {
	limits          ResourceLimits
	items           int
	visiting        map[*goja.Object]bool
	objectPrototype *goja.Object
}

func exportResult(vm *goja.Runtime, value goja.Value, limits ResourceLimits, objectPrototype *goja.Object) (map[string]any, []byte, error) {
	exporter := gojaExporter{limits: limits, visiting: make(map[*goja.Object]bool), objectPrototype: objectPrototype}
	var exported any
	var exportErr error
	if exception := vm.Try(func() {
		exported, exportErr = exporter.value(value, 1, "outputs")
	}); exception != nil {
		return nil, nil, exception
	}
	if exportErr != nil {
		return nil, nil, exportErr
	}
	object, ok := exported.(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("%w: entrypoint must return a plain object", ErrInvalidOutput)
	}
	encoded, err := json.Marshal(object)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: encode canonical output: %w", ErrInvalidOutput, err)
	}
	if len(encoded) > limits.MaxOutputBytes {
		return nil, nil, fmt.Errorf("%w: output canonical JSON exceeds max_output_bytes", ErrResourceLimit)
	}
	cloned, err := decodeJSONObject(encoded)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: clone canonical output: %w", ErrInvalidOutput, err)
	}
	return cloned, encoded, nil
}

func (e *gojaExporter) value(value goja.Value, depth int, path string) (any, error) {
	if depth > e.limits.MaxDepth {
		return nil, fmt.Errorf("%w: %s exceeds max_depth", ErrResourceLimit, path)
	}
	if goja.IsUndefined(value) {
		return nil, fmt.Errorf("%w: %s is undefined", ErrInvalidOutput, path)
	}
	if goja.IsNull(value) {
		return nil, nil
	}
	if goja.IsNaN(value) || goja.IsInfinity(value) {
		return nil, fmt.Errorf("%w: %s is not a finite JSON number", ErrInvalidOutput, path)
	}
	if object, ok := value.(*goja.Object); ok {
		return e.object(object, depth, path)
	}
	switch exported := value.Export().(type) {
	case nil:
		return nil, nil
	case bool:
		return exported, nil
	case string:
		if len(exported) > e.limits.MaxStringBytes {
			return nil, fmt.Errorf("%w: %s exceeds max_string_bytes", ErrResourceLimit, path)
		}
		return exported, nil
	case int64:
		if exported < -maximumSafeInteger || exported > maximumSafeInteger {
			return nil, fmt.Errorf("%w: %s integer is outside the safe JavaScript range", ErrInvalidOutput, path)
		}
		return json.Number(strconv.FormatInt(exported, 10)), nil
	case float64:
		if math.IsNaN(exported) || math.IsInf(exported, 0) {
			return nil, fmt.Errorf("%w: %s is not finite", ErrInvalidOutput, path)
		}
		if math.Trunc(exported) == exported && (exported < -float64(maximumSafeInteger) || exported > float64(maximumSafeInteger)) {
			return nil, fmt.Errorf("%w: %s integer is outside the safe JavaScript range", ErrInvalidOutput, path)
		}
		return json.Number(strconv.FormatFloat(exported, 'g', -1, 64)), nil
	default:
		return nil, fmt.Errorf("%w: %s contains unsupported JavaScript value %T", ErrInvalidOutput, path, exported)
	}
}

func (e *gojaExporter) object(object *goja.Object, depth int, path string) (any, error) {
	if e.visiting[object] {
		return nil, fmt.Errorf("%w: %s contains a cycle", ErrInvalidOutput, path)
	}
	e.visiting[object] = true
	defer delete(e.visiting, object)
	if len(object.Symbols()) != 0 {
		return nil, fmt.Errorf("%w: %s contains symbol properties", ErrInvalidOutput, path)
	}

	switch object.ClassName() {
	case "Array":
		length := object.Get("length").ToInteger()
		if length < 0 || length > int64(e.limits.MaxItems-e.items) {
			return nil, fmt.Errorf("%w: %s exceeds max_items", ErrResourceLimit, path)
		}
		e.items += int(length)
		keys := object.Keys()
		if int64(len(keys)) != length {
			return nil, fmt.Errorf("%w: %s arrays must be dense and contain no named properties", ErrInvalidOutput, path)
		}
		items := make([]any, int(length))
		for index := range items {
			name := strconv.Itoa(index)
			if keys[index] != name {
				return nil, fmt.Errorf("%w: %s arrays must use canonical indexes", ErrInvalidOutput, path)
			}
			item, err := e.value(object.Get(name), depth+1, fmt.Sprintf("%s[%d]", path, index))
			if err != nil {
				return nil, err
			}
			items[index] = item
		}
		return items, nil
	case "Object":
		prototype := object.Prototype()
		if prototype != nil && prototype != e.objectPrototype {
			return nil, fmt.Errorf("%w: %s must have the ordinary object prototype", ErrInvalidOutput, path)
		}
		keys := object.Keys()
		if len(keys) != len(object.GetOwnPropertyNames()) {
			return nil, fmt.Errorf("%w: %s must contain only enumerable data properties", ErrInvalidOutput, path)
		}
		if len(keys) > e.limits.MaxItems-e.items {
			return nil, fmt.Errorf("%w: %s exceeds max_items", ErrResourceLimit, path)
		}
		e.items += len(keys)
		sort.Strings(keys)
		result := make(map[string]any, len(keys))
		for _, key := range keys {
			if len(key) > e.limits.MaxStringBytes {
				return nil, fmt.Errorf("%w: %s property name exceeds max_string_bytes", ErrResourceLimit, path)
			}
			item, err := e.value(object.Get(key), depth+1, path+"."+key)
			if err != nil {
				return nil, err
			}
			result[key] = item
		}
		return result, nil
	default:
		return nil, fmt.Errorf("%w: %s contains unsupported object class %s", ErrInvalidOutput, path, object.ClassName())
	}
}

func outputValueSet(payload map[string]any, identity stepkind.InvocationIdentity) (values.ValueSet, error) {
	result := make(values.ValueSet, len(payload))
	names := sortedKeys(payload)
	for _, name := range names {
		if err := graph.ValidateID(name); err != nil {
			return nil, fmt.Errorf("%w: output %q is not a canonical graph identifier: %w", ErrInvalidOutput, name, err)
		}
		metadata := values.Metadata{
			Producer:  values.Producer{Kind: "node_output", Reference: outputReference(identity), Output: name},
			MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		}
		value, err := values.NewInline(payload[name], metadata)
		if err != nil {
			return nil, fmt.Errorf("%w: output %q: %w", ErrInvalidOutput, name, err)
		}
		if err := values.ValidatePersistable(value); err != nil {
			return nil, fmt.Errorf("%w: output %q: %w", ErrInvalidOutput, name, err)
		}
		result[name] = value
	}
	return result, nil
}

func outputReference(identity stepkind.InvocationIdentity) string {
	reference := identity.RunID + "/" + identity.NodeID
	if identity.Iteration != "" {
		reference += "/" + identity.Iteration
	}
	return reference
}

func decodeJSONObject(encoded []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil || object == nil {
		return nil, errors.New("expected JSON object")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("multiple JSON values")
	}
	return object, nil
}
