package verification

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// ReviewerDecision is the strict provider-neutral result expected from future
// LLM/reviewer adapters. Passing requires an explicit boolean plus stable code
// and message; unknown fields, duplicate fields, and trailing documents fail.
type ReviewerDecision struct {
	Passed  bool              `json:"passed"`
	Code    string            `json:"code"`
	Message string            `json:"message"`
	Details map[string]string `json:"details,omitempty"`
}

func (d ReviewerDecision) Validate() error {
	if err := validateText("reviewer decision code", d.Code, true); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidDecision, err)
	}
	if err := validateText("reviewer decision message", d.Message, true); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidDecision, err)
	}
	if err := validateStringMap("reviewer decision details", d.Details); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidDecision, err)
	}
	return nil
}

func ParseReviewerDecision(data []byte) (ReviewerDecision, error) {
	if err := rejectDuplicateJSONFields(data); err != nil {
		return ReviewerDecision{}, fmt.Errorf("%w: %w", ErrInvalidDecision, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	var wire struct {
		Passed  *bool             `json:"passed"`
		Code    string            `json:"code"`
		Message string            `json:"message"`
		Details map[string]string `json:"details,omitempty"`
	}
	if err := decoder.Decode(&wire); err != nil {
		return ReviewerDecision{}, fmt.Errorf("%w: %w", ErrInvalidDecision, err)
	}
	if err := requireEOF(decoder); err != nil {
		return ReviewerDecision{}, fmt.Errorf("%w: %w", ErrInvalidDecision, err)
	}
	if wire.Passed == nil {
		return ReviewerDecision{}, fmt.Errorf("%w: passed is required", ErrInvalidDecision)
	}
	decision := ReviewerDecision{Passed: *wire.Passed, Code: wire.Code, Message: wire.Message, Details: wire.Details}
	if err := decision.Validate(); err != nil {
		return ReviewerDecision{}, err
	}
	return decision, nil
}

func rejectDuplicateJSONFields(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, composite := token.(json.Delim)
		if !composite {
			return nil
		}
		switch delimiter {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				field, err := decoder.Token()
				if err != nil {
					return err
				}
				name, ok := field.(string)
				if !ok {
					return errorsf("object field name is not a string")
				}
				if _, duplicate := seen[name]; duplicate {
					return errorsf("duplicate JSON field %q", name)
				}
				seen[name] = struct{}{}
				if err := walk(); err != nil {
					return err
				}
			}
			closing, err := decoder.Token()
			if err != nil || closing != json.Delim('}') {
				return errorsf("malformed JSON object")
			}
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			closing, err := decoder.Token()
			if err != nil || closing != json.Delim(']') {
				return errorsf("malformed JSON array")
			}
		default:
			return errorsf("unexpected JSON delimiter %q", delimiter)
		}
		return nil
	}
	if err := walk(); err != nil {
		return err
	}
	if decoder.More() {
		return errorsf("multiple JSON values")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return errorsf("multiple JSON values")
	}
	return nil
}
