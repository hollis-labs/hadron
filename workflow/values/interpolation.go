package values

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/hollis-labs/hadron/workflow/graph"
)

// Interpolate evaluates every {{ expression }} segment in a string. Implicit
// interpolation accepts strings and JSON scalar numbers/booleans. Composite
// values and null require the explicit deterministic string(...) conversion.
func (e *ExpressionEngine) Interpolate(
	template string,
	source *graph.SourceRef,
	context ExpressionContext,
	options ExpressionOptions,
) (string, error) {
	segments, err := parseInterpolation(template)
	if err != nil {
		return "", expressionError(CodeInterpolation, "string interpolation is malformed", source, err)
	}
	var output strings.Builder
	output.Grow(len(template))
	for _, segment := range segments {
		if !segment.expression {
			output.WriteString(segment.text)
			continue
		}
		value, err := e.EvaluateRaw(
			graph.Expression{Text: segment.text, Source: cloneSourceRef(source)},
			context,
			options,
		)
		if err != nil {
			return "", err
		}
		text, err := implicitInterpolationString(value)
		if err != nil {
			return "", expressionError(
				CodeInterpolation,
				"interpolation result requires an explicit deterministic string(...) conversion",
				source,
				err,
			)
		}
		output.WriteString(text)
	}
	return output.String(), nil
}

type interpolationSegment struct {
	text       string
	expression bool
}

func parseInterpolation(template string) ([]interpolationSegment, error) {
	segments := make([]interpolationSegment, 0, 3)
	position := 0
	for position < len(template) {
		openOffset := strings.Index(template[position:], "{{")
		closeOffset := strings.Index(template[position:], "}}")
		if closeOffset >= 0 && (openOffset < 0 || closeOffset < openOffset) {
			return nil, fmt.Errorf("unmatched closing interpolation marker at byte %d", position+closeOffset)
		}
		if openOffset < 0 {
			segments = append(segments, interpolationSegment{text: template[position:]})
			break
		}
		open := position + openOffset
		if open > position {
			segments = append(segments, interpolationSegment{text: template[position:open]})
		}
		closingMarker, err := interpolationClose(template, open+2)
		if err != nil {
			return nil, err
		}
		expression := strings.TrimSpace(template[open+2 : closingMarker])
		if expression == "" {
			return nil, fmt.Errorf("empty interpolation marker at byte %d", open)
		}
		segments = append(segments, interpolationSegment{text: expression, expression: true})
		position = closingMarker + 2
	}
	if len(template) == 0 {
		return []interpolationSegment{{}}, nil
	}
	return segments, nil
}

func interpolationClose(template string, start int) (int, error) {
	var quote byte
	escaped := false
	depth := 0
	for index := start; index < len(template); index++ {
		character := template[index]
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if character == '\\' && quote != '`' {
				escaped = true
				continue
			}
			if character == quote {
				quote = 0
			}
			continue
		}
		switch character {
		case '\'', '"', '`':
			quote = character
		case '(', '[', '{':
			if character == '{' && depth == 0 && index+1 < len(template) && template[index+1] == '{' {
				return 0, fmt.Errorf("nested interpolation marker at byte %d", index)
			}
			depth++
		case ')', ']':
			if depth > 0 {
				depth--
			}
		case '}':
			if depth > 0 {
				depth--
				continue
			}
			if index+1 < len(template) && template[index+1] == '}' {
				return index, nil
			}
		}
	}
	return 0, fmt.Errorf("unmatched opening interpolation marker at byte %d", start-2)
}

func implicitInterpolationString(value any) (string, error) {
	switch value := value.(type) {
	case string:
		return value, nil
	case bool:
		return strconv.FormatBool(value), nil
	case json.Number:
		return value.String(), nil
	default:
		return "", fmt.Errorf("cannot implicitly interpolate %T", value)
	}
}
