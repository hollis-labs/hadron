package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const operationalChunkBytes = 16 << 10

type artifactResult struct {
	value values.Value
	err   error
}

type streamCollector struct {
	ctx      context.Context
	identity stepkind.InvocationIdentity
	stream   Stream
	capture  *CaptureConfig
	artifact ArtifactSink
	events   EventSink

	buffer    bytes.Buffer
	written   int64
	overflow  bool
	sinkErr   error
	sequence  uint64
	pipe      *io.PipeWriter
	artifactC chan artifactResult
	closeOnce sync.Once
	closed    collectedStream
}

func newStreamCollector(
	ctx context.Context,
	identity stepkind.InvocationIdentity,
	stream Stream,
	capture *CaptureConfig,
	artifact ArtifactSink,
	events EventSink,
) (*streamCollector, error) {
	collector := &streamCollector{
		ctx: ctx, identity: identity, stream: stream, capture: cloneCapture(capture),
		artifact: artifact, events: events,
	}
	if capture == nil {
		return collector, nil
	}
	switch capture.Mode {
	case CaptureOutput:
		// Output capture retains bytes in the bounded in-memory buffer.
	case CaptureArtifact:
		if nilInterface(artifact) {
			return nil, errors.New("artifact sink is required")
		}
		reader, writer := io.Pipe()
		collector.pipe = writer
		collector.artifactC = make(chan artifactResult, 1)
		request := ArtifactCapture{
			Identity: identity, Stream: stream, Name: capture.Name,
			MaxBytes: capture.MaxBytes, Metadata: outputMetadata(identity, capture.Name, capture.MediaType, "node_output"),
		}
		go func() {
			value, err := artifact.CaptureArtifact(ctx, request, reader)
			_ = reader.CloseWithError(err)
			collector.artifactC <- artifactResult{value: value, err: err}
		}()
	case CaptureEvent:
		if nilInterface(events) {
			return nil, errors.New("event sink is required")
		}
	}
	return collector, nil
}

func cloneCapture(input *CaptureConfig) *CaptureConfig {
	if input == nil {
		return nil
	}
	copyValue := *input
	return &copyValue
}

// Write never propagates a capture failure into the process pipe. It records
// the first failure and continues draining so a hostile child cannot block on
// a full stdout or stderr pipe.
func (c *streamCollector) Write(content []byte) (int, error) {
	originalLength := len(content)
	if len(content) == 0 || c.capture == nil {
		return originalLength, nil
	}
	if c.overflow {
		return originalLength, nil
	}
	switch c.capture.Mode {
	case CaptureOutput:
		c.writeBounded(content, func(part []byte) error {
			_, err := c.buffer.Write(part)
			return err
		})
	case CaptureArtifact:
		c.writeBounded(content, func(part []byte) error {
			if c.pipe == nil {
				return errors.New("artifact stream is closed")
			}
			_, err := c.pipe.Write(part)
			return err
		})
	case CaptureEvent:
		remaining := c.capture.MaxBytes - c.written
		if remaining <= 0 {
			c.markOverflow()
			return originalLength, nil
		}
		emitted := content
		if int64(len(emitted)) > remaining {
			emitted = emitted[:remaining]
		}
		for len(emitted) != 0 && c.sinkErr == nil {
			length := len(emitted)
			if length > operationalChunkBytes {
				length = operationalChunkBytes
			}
			c.sequence++
			event := OperationalEvent{
				Identity: c.identity, Stream: c.stream, Sequence: c.sequence,
				Payload: append([]byte(nil), emitted[:length]...),
			}
			if err := c.events.EmitCommandEvent(c.ctx, event); err != nil {
				c.sinkErr = err
			}
			c.written += int64(length)
			emitted = emitted[length:]
		}
		if int64(originalLength) > remaining {
			c.markOverflow()
		}
	}
	return originalLength, nil
}

func (c *streamCollector) writeBounded(content []byte, write func([]byte) error) {
	if c.overflow || c.sinkErr != nil {
		return
	}
	remaining := c.capture.MaxBytes - c.written
	if remaining <= 0 {
		c.markOverflow()
		return
	}
	part := content
	if int64(len(part)) > remaining {
		part = part[:remaining]
	}
	if len(part) != 0 {
		if err := write(part); err != nil {
			c.sinkErr = err
			return
		}
		c.written += int64(len(part))
	}
	if len(part) != len(content) {
		c.markOverflow()
	}
}

func (c *streamCollector) markOverflow() {
	c.overflow = true
	if c.pipe != nil {
		_ = c.pipe.CloseWithError(ErrOutputTruncated)
		c.pipe = nil
	}
}

type collectedStream struct {
	content  []byte
	artifact values.Value
	overflow bool
	err      error
}

type boundedRedactingWriter struct {
	destination io.WriteCloser
	collector   *streamCollector
	limit       int64
	written     int64
	overflow    bool
}

func newBoundedRedactingWriter(destination io.WriteCloser, collector *streamCollector) io.WriteCloser {
	limit := int64(0)
	if collector != nil && collector.capture != nil {
		limit = collector.capture.MaxBytes
	}
	return &boundedRedactingWriter{destination: destination, collector: collector, limit: limit}
}

// Write accounts raw process bytes before redaction. After overflow it still
// feeds all bytes through the redactor while the collector discards them. This
// lets the redactor recognize a secret spanning the bound without ever
// releasing an unmatched secret prefix.
func (w *boundedRedactingWriter) Write(content []byte) (int, error) {
	if w.destination == nil {
		return 0, errors.New("bounded redacting destination is nil")
	}
	if w.limit > 0 && !w.overflow {
		remaining := w.limit - w.written
		if remaining < 0 || int64(len(content)) > remaining {
			w.overflow = true
			if w.collector != nil {
				w.collector.markOverflow()
			}
		} else {
			w.written += int64(len(content))
		}
	}
	return w.destination.Write(content)
}

func (w *boundedRedactingWriter) Close() error {
	if w.destination == nil {
		return errors.New("bounded redacting destination is nil")
	}
	return w.destination.Close()
}

func (c *streamCollector) close() collectedStream {
	c.closeOnce.Do(func() {
		if c.pipe != nil {
			_ = c.pipe.Close()
			c.pipe = nil
		}
		if c.artifactC != nil {
			artifact := <-c.artifactC
			c.closed.artifact, c.closed.err = artifact.value, artifact.err
		}
		if c.closed.err == nil {
			c.closed.err = c.sinkErr
		}
		c.closed.overflow = c.overflow
		c.closed.content = append([]byte(nil), c.buffer.Bytes()...)
	})
	result := c.closed
	result.content = append([]byte(nil), c.closed.content...)
	return result
}

func validateArtifactValue(value values.Value, identity stepkind.InvocationIdentity, capture *CaptureConfig) error {
	if err := values.ValidatePersistable(value); err != nil {
		return err
	}
	if value.Type != values.TypeArtifact || value.Artifact == nil {
		return fmt.Errorf("artifact sink returned a non-artifact value")
	}
	expected := outputMetadata(identity, capture.Name, capture.MediaType, "node_output")
	if value.Producer != expected.Producer || value.MediaType != expected.MediaType ||
		value.Redaction != expected.Redaction || value.Retention != expected.Retention {
		return fmt.Errorf("artifact sink changed immutable capture metadata")
	}
	if value.Artifact.SizeBytes > capture.MaxBytes {
		return fmt.Errorf("artifact sink exceeded the configured byte bound")
	}
	return nil
}
