package values

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
)

// RedactedMarker is the stable replacement for secret or policy-hidden data.
const RedactedMarker = "[REDACTED]"

// Observation is an application-neutral adapter-boundary payload. Channel is
// an open classifier such as command, prompt, message, http, or mcp.
type Observation struct {
	Channel    string
	Attributes map[string]string
	Payload    []byte
}

// Redactor masks a fixed set of known resolved secret byte strings. It is
// immutable and safe for concurrent stateless masking calls. A streaming
// writer created from it owns its own bounded pending buffer.
type Redactor struct {
	secrets [][]byte
	maxLen  int
}

// NewRedactor defensively copies, deduplicates, and deterministically orders
// resolved material. Longest matches win; equal-length matches use byte order.
func NewRedactor(secrets ...*ResolvedSecret) (*Redactor, error) {
	unique := make(map[string][]byte, len(secrets))
	for _, secret := range secrets {
		if secret == nil || len(secret.material) == 0 {
			return nil, fmt.Errorf("%w: redactor secrets must contain material", ErrSecretMaterial)
		}
		key := string(secret.material)
		if _, exists := unique[key]; !exists {
			unique[key] = append([]byte(nil), secret.material...)
		}
	}
	ordered := make([][]byte, 0, len(unique))
	for _, secret := range unique {
		ordered = append(ordered, secret)
	}
	sort.Slice(ordered, func(left, right int) bool {
		if len(ordered[left]) != len(ordered[right]) {
			return len(ordered[left]) > len(ordered[right])
		}
		return bytes.Compare(ordered[left], ordered[right]) < 0
	})
	maxLen := 0
	if len(ordered) != 0 {
		maxLen = len(ordered[0])
	}
	return &Redactor{secrets: ordered, maxLen: maxLen}, nil
}

// MaskBytes replaces every known occurrence without mutating input.
func (r *Redactor) MaskBytes(input []byte) []byte {
	if r == nil || len(r.secrets) == 0 {
		return append([]byte(nil), input...)
	}
	masked, _ := r.maskPrefix(input, len(input))
	return masked
}

// MaskString replaces every known occurrence in text.
func (r *Redactor) MaskString(input string) string {
	return string(r.MaskBytes([]byte(input)))
}

// MaskObservation returns a defensive copy whose attribute values and payload
// have been masked. Attribute keys and the channel are non-payload metadata.
func (r *Redactor) MaskObservation(input Observation) Observation {
	result := Observation{Channel: input.Channel, Payload: r.MaskBytes(input.Payload)}
	if input.Attributes != nil {
		result.Attributes = make(map[string]string, len(input.Attributes))
		for key, value := range input.Attributes {
			result.Attributes[key] = r.MaskString(value)
		}
	}
	return result
}

// Writer returns a streaming masker. It may delay at most maxSecretLen-1 input
// bytes so matches spanning arbitrary Write boundaries are masked. Close flushes
// pending bytes but does not close the destination.
func (r *Redactor) Writer(destination io.Writer) io.WriteCloser {
	return &redactingWriter{redactor: r, destination: destination}
}

func (r *Redactor) maskPrefix(input []byte, limit int) ([]byte, int) {
	if r == nil || len(r.secrets) == 0 {
		return append([]byte(nil), input[:limit]...), limit
	}
	var output bytes.Buffer
	position := 0
	for position < limit {
		matched := 0
		for _, secret := range r.secrets {
			if len(secret) <= len(input)-position && bytes.Equal(input[position:position+len(secret)], secret) {
				matched = len(secret)
				break
			}
		}
		if matched != 0 {
			output.WriteString(RedactedMarker)
			position += matched
			continue
		}
		output.WriteByte(input[position])
		position++
	}
	return output.Bytes(), position
}

type redactingWriter struct {
	redactor    *Redactor
	destination io.Writer
	pending     []byte
	closed      bool
}

func (w *redactingWriter) Write(input []byte) (int, error) {
	if w.closed {
		return 0, errors.New("write to closed workflow redacting writer")
	}
	if w.destination == nil {
		return 0, errors.New("workflow redacting writer destination is nil")
	}
	w.pending = append(w.pending, input...)
	maxLen := 0
	if w.redactor != nil {
		maxLen = w.redactor.maxLen
	}
	limit := len(w.pending)
	if maxLen > 0 {
		limit = len(w.pending) - maxLen + 1
		if limit < 0 {
			limit = 0
		}
	}
	if limit == 0 {
		return len(input), nil
	}
	masked, consumed := w.redactor.maskPrefix(w.pending, limit)
	if err := writeAll(w.destination, masked); err != nil {
		return 0, err
	}
	w.pending = append(w.pending[:0], w.pending[consumed:]...)
	return len(input), nil
}

func (w *redactingWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if w.destination == nil {
		return errors.New("workflow redacting writer destination is nil")
	}
	masked := append([]byte(nil), w.pending...)
	if w.redactor != nil {
		masked = w.redactor.MaskBytes(w.pending)
	}
	w.pending = nil
	return writeAll(w.destination, masked)
}

func writeAll(destination io.Writer, content []byte) error {
	for len(content) != 0 {
		written, err := destination.Write(content)
		if written < 0 || written > len(content) {
			return io.ErrShortWrite
		}
		content = content[written:]
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
