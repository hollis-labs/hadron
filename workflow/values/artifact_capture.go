package values

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

const (
	DefaultInlineLimit int64 = 64 << 10
	MaximumInlineLimit int64 = 1 << 20
)

// CaptureMode selects the only interpretation applied before an inline value
// is constructed. Promoted bytes remain opaque artifact content.
type CaptureMode string

const (
	CaptureJSON         CaptureMode = "json"
	CaptureText         CaptureMode = "text"
	CaptureArtifactOnly CaptureMode = "artifact_only"
)

// CapturePolicy bounds the only buffer used while deciding inline versus
// artifact storage. InlineLimit must be positive and no larger than 1 MiB.
type CapturePolicy struct {
	Mode        CaptureMode
	InlineLimit int64
}

// Validate rejects configurations that could accidentally become unbounded.
func (p CapturePolicy) Validate() error {
	if p.InlineLimit <= 0 || p.InlineLimit > MaximumInlineLimit {
		return NewArtifactError(ArtifactOperationPut, ArtifactFailureSize, nil, ErrArtifactSizeLimit)
	}
	switch p.Mode {
	case CaptureJSON, CaptureText, CaptureArtifactOnly:
		return nil
	default:
		return NewArtifactError(ArtifactOperationPut, ArtifactFailureInvalid, nil, ErrArtifactInvalid)
	}
}

// CaptureValue returns a small JSON/text value inline or promotes it to an
// immutable ArtifactRef. It buffers at most InlineLimit+1 bytes. Oversized JSON
// is deliberately not parsed: the original opaque bytes are streamed to the
// store and the result is TypeArtifact.
func CaptureValue(ctx context.Context, store ArtifactStore, request ArtifactPutRequest, source io.Reader, policy CapturePolicy) (Value, error) {
	request = request.snapshot()
	if ctx == nil {
		return Value{}, NewArtifactError(ArtifactOperationPut, ArtifactFailureInvalid, nil, ErrArtifactInvalid)
	}
	if err := ctx.Err(); err != nil {
		return Value{}, NewArtifactError(ArtifactOperationPut, ArtifactFailureInvalid, nil, err)
	}
	if err := policy.Validate(); err != nil {
		return Value{}, err
	}
	if err := request.validate(true); err != nil {
		return Value{}, err
	}
	if store == nil || source == nil {
		return Value{}, NewArtifactError(ArtifactOperationPut, ArtifactFailureInvalid, nil, ErrArtifactInvalid)
	}
	if policy.Mode == CaptureArtifactOnly || request.Metadata.Redaction == RedactionSecret {
		return putCapturedArtifact(ctx, store, request, source)
	}

	prefix, err := io.ReadAll(io.LimitReader(captureContextReader{ctx: ctx, reader: source}, policy.InlineLimit+1))
	if err != nil {
		return Value{}, NewArtifactError(ArtifactOperationPut, ArtifactFailureInvalid, nil, err)
	}
	if err := ctx.Err(); err != nil {
		return Value{}, NewArtifactError(ArtifactOperationPut, ArtifactFailureInvalid, nil, err)
	}
	if int64(len(prefix)) > policy.InlineLimit {
		return putCapturedArtifact(ctx, store, request, io.MultiReader(bytes.NewReader(prefix), source))
	}
	if int64(len(prefix)) > request.MaxBytes {
		return Value{}, NewArtifactError(ArtifactOperationPut, ArtifactFailureSize, nil, ErrArtifactSizeLimit)
	}
	if request.ExpectedSize != nil && *request.ExpectedSize != int64(len(prefix)) {
		return Value{}, NewArtifactError(ArtifactOperationPut, ArtifactFailureDigest, nil, ErrArtifactDigest)
	}
	if request.ExpectedDigest != "" && request.ExpectedDigest != SHA256Digest(prefix) {
		return Value{}, NewArtifactError(ArtifactOperationPut, ArtifactFailureDigest, nil, ErrArtifactDigest)
	}

	switch policy.Mode {
	case CaptureJSON:
		inline, decodeErr := decodeCapturedJSON(prefix)
		if decodeErr != nil {
			return Value{}, NewArtifactError(ArtifactOperationPut, ArtifactFailureInvalid, nil, decodeErr)
		}
		return NewInline(inline, request.Metadata)
	case CaptureText:
		if !utf8.Valid(prefix) {
			return putCapturedArtifact(ctx, store, request, bytes.NewReader(prefix))
		}
		return NewInline(string(prefix), request.Metadata)
	default:
		return Value{}, NewArtifactError(ArtifactOperationPut, ArtifactFailureInvalid, nil, ErrArtifactInvalid)
	}
}

type captureContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r captureContextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(buffer)
	if contextErr := r.ctx.Err(); contextErr != nil {
		return 0, contextErr
	}
	return n, err
}

func putCapturedArtifact(ctx context.Context, store ArtifactStore, request ArtifactPutRequest, source io.Reader) (Value, error) {
	metadata, err := store.Put(ctx, request, source)
	if err != nil {
		return Value{}, err
	}
	return NewArtifact(metadata.Ref)
}

func decodeCapturedJSON(content []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, artifactInvariant("captured JSON contains multiple documents")
		}
		return nil, err
	}
	return decoded, nil
}
