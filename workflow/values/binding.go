package values

import (
	"encoding/json"
	"fmt"

	"github.com/hollis-labs/hadron/workflow/graph"
)

// EvaluateBinding evaluates one graph binding into a typed Value. Exact
// passthrough references to an input or step output preserve the complete
// existing envelope, including producer, redaction, retention, media type,
// digest, and artifact identity. Computed, literal, and interpolated results
// receive metadata supplied by the binding owner.
func (e *ExpressionEngine) EvaluateBinding(
	binding graph.Binding,
	context ExpressionContext,
	options ExpressionOptions,
	metadata Metadata,
) (Value, error) {
	switch binding.Kind {
	case graph.BindingLiteral:
		if binding.Expression != nil || binding.Interpolation != "" {
			return Value{}, invalidBindingShape(binding, "literal binding must not also contain expression or interpolation data")
		}
		return NewInline(binding.Literal, metadata)
	case graph.BindingInterpolation:
		if binding.Expression != nil || binding.Literal != nil {
			return Value{}, invalidBindingShape(binding, "interpolation binding must not also contain literal or expression data")
		}
		result, err := e.Interpolate(binding.Interpolation, binding.Source, context, options)
		if err != nil {
			return Value{}, err
		}
		return NewInline(result, metadata)
	case graph.BindingExpression:
		if binding.Expression == nil {
			return Value{}, expressionError(
				CodeExpressionValue,
				"expression binding is missing its expression",
				binding.Source,
				fmt.Errorf("binding.expression is required"),
			)
		}
		if binding.Literal != nil || binding.Interpolation != "" {
			return Value{}, invalidBindingShape(binding, "expression binding must not also contain literal or interpolation data")
		}
		result, err := e.EvaluateRaw(*binding.Expression, context, options)
		if err != nil {
			return Value{}, err
		}
		if passthrough, ok, resolveErr := exactValuePassthrough(*binding.Expression, context); resolveErr != nil {
			return Value{}, expressionError(
				CodeExpressionValue,
				"direct value passthrough is invalid",
				binding.Expression.Source,
				resolveErr,
			)
		} else if ok {
			return passthrough, nil
		}
		return NewInline(result, metadata)
	default:
		return Value{}, expressionError(
			CodeExpressionValue,
			"binding kind is not supported",
			binding.Source,
			fmt.Errorf("unsupported binding kind %q", binding.Kind),
		)
	}
}

func invalidBindingShape(binding graph.Binding, message string) error {
	return expressionError(CodeExpressionValue, message, binding.Source, nil)
}

func exactValuePassthrough(expression graph.Expression, context ExpressionContext) (Value, bool, error) {
	reference, exact, err := exactReference(expression)
	if err != nil || !exact {
		return Value{}, false, err
	}
	var value Value
	switch {
	case reference.Root == "inputs" && len(reference.Path) == 1:
		var ok bool
		value, ok = context.Inputs[reference.Path[0]]
		if !ok {
			return Value{}, false, nil
		}
	case reference.Root == "steps" && len(reference.Path) == 3 && reference.Path[1] == "outputs":
		step, ok := context.Steps[reference.Path[0]]
		if !ok {
			return Value{}, false, nil
		}
		value, ok = step.Outputs[reference.Path[2]]
		if !ok {
			return Value{}, false, nil
		}
	default:
		return Value{}, false, nil
	}
	cloned, err := cloneValue(value)
	if err != nil {
		return Value{}, false, err
	}
	return cloned, true, nil
}

func cloneValue(value Value) (Value, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return Value{}, err
	}
	var cloned Value
	if err := json.Unmarshal(encoded, &cloned); err != nil {
		return Value{}, err
	}
	return cloned, nil
}
