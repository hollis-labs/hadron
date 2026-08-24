package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	nethttp "net/http"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

func (k *Kind) mapResponse(ctx context.Context, identity stepkind.InvocationIdentity, parsed config, response *nethttp.Response, redactor *values.Redactor, hops int, finalMethod, finalOrigin string) (values.ValueSet, error) {
	defer func() { _ = response.Body.Close() }()
	if response.ContentLength > parsed.MaxResponseBytes {
		return nil, fmt.Errorf("%w: response body exceeds configured bound", ErrInvalidResult)
	}
	raw, err := io.ReadAll(io.LimitReader(contextReader{ctx: ctx, reader: response.Body}, parsed.MaxResponseBytes+1))
	defer zeroBytes(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: read response body: %w", ErrInvalidResult, err)
	}
	if int64(len(raw)) > parsed.MaxResponseBytes {
		return nil, fmt.Errorf("%w: response body exceeds configured bound", ErrInvalidResult)
	}
	mediaType := response.Header.Get("Content-Type")
	baseMediaType := "application/octet-stream"
	if mediaType != "" {
		parsedType, _, parseErr := mime.ParseMediaType(mediaType)
		if parseErr != nil {
			return nil, fmt.Errorf("%w: invalid response content type", ErrInvalidResult)
		}
		baseMediaType = strings.ToLower(parsedType)
	}
	if len(parsed.ExpectedContentTypes) != 0 && !containsString(parsed.ExpectedContentTypes, baseMediaType) {
		return nil, fmt.Errorf("%w: unexpected response content type", ErrInvalidResult)
	}
	if encoding := strings.TrimSpace(strings.ToLower(response.Header.Get("Content-Encoding"))); encoding != "" && encoding != "identity" {
		baseMediaType = "application/octet-stream"
	}

	maskedHeaders := sanitizeResponseHeaders(response.Header, redactor)
	status, err := values.NewInline(json.Number(strconv.Itoa(response.StatusCode)), outputMetadata(identity, OutputStatus, "application/json", values.RedactionPublic))
	if err != nil {
		return nil, err
	}
	headers, err := values.NewInline(stringSliceMapToAny(maskedHeaders), outputMetadata(identity, OutputHeaders, "application/json", values.RedactionPrivate))
	if err != nil {
		return nil, err
	}
	metadata, err := values.NewInline(map[string]any{
		"method": finalMethod, "origin": finalOrigin, "redirect_hops": json.Number(strconv.Itoa(hops)),
		"content_type": baseMediaType, "body_bytes": json.Number(strconv.Itoa(len(raw))),
	}, outputMetadata(identity, OutputMetadata, "application/json", values.RedactionPublic))
	if err != nil {
		return nil, err
	}

	outputs := values.ValueSet{OutputStatus: status, OutputHeaders: headers, OutputMetadata: metadata}
	maskedRaw := redactor.MaskBytes(raw)
	defer zeroBytes(maskedRaw)
	if int64(len(maskedRaw)) > parsed.MaxResponseBytes {
		return nil, fmt.Errorf("%w: redacted response exceeds configured bound", ErrInvalidResult)
	}
	if isJSONMediaType(baseMediaType) {
		decoded, decodeErr := decodeSingleJSON(raw)
		if decodeErr != nil {
			return nil, fmt.Errorf("%w: response is not one valid JSON document", ErrInvalidResult)
		}
		if parsed.HasExpectedJSONSchema {
			actualValue, valueErr := values.NewInline(decoded, outputMetadata(identity, OutputBodyJSON, baseMediaType, values.RedactionPrivate))
			if valueErr != nil {
				return nil, fmt.Errorf("%w: response JSON is invalid", ErrInvalidResult)
			}
			if schemaErr := values.ValidateValueSchema(parsed.ExpectedJSONSchema, actualValue); schemaErr != nil {
				return nil, fmt.Errorf("%w: JSON schema mismatch", ErrInvalidResult)
			}
		}
		maskedJSON, maskErr := maskJSON(decoded, redactor)
		if maskErr != nil {
			return nil, fmt.Errorf("%w: response JSON could not be redacted", ErrInvalidResult)
		}
		canonical, marshalErr := json.Marshal(maskedJSON)
		if marshalErr != nil || int64(len(canonical)) > parsed.MaxResponseBytes {
			return nil, fmt.Errorf("%w: normalized JSON exceeds configured bound", ErrInvalidResult)
		}
		jsonValue, valueErr := values.NewInline(maskedJSON, outputMetadata(identity, OutputBodyJSON, baseMediaType, values.RedactionPrivate))
		if valueErr != nil {
			return nil, valueErr
		}
		if int64(len(canonical)) <= parsed.InlineLimit {
			bodyValue, bodyErr := values.NewInline(string(canonical), outputMetadata(identity, OutputBody, baseMediaType, values.RedactionPrivate))
			if bodyErr != nil {
				return nil, bodyErr
			}
			outputs[OutputBody] = bodyValue
			outputs[OutputBodyJSON] = jsonValue
		} else {
			artifact, artifactErr := k.captureArtifact(ctx, identity, parsed, canonical, baseMediaType, redactor)
			if artifactErr != nil {
				return nil, artifactErr
			}
			outputs[OutputBody] = artifact
		}
	} else if isTextMediaType(baseMediaType) && utf8.Valid(maskedRaw) {
		if parsed.HasExpectedJSONSchema {
			return nil, fmt.Errorf("%w: JSON schema requires a JSON response", ErrInvalidResult)
		}
		if int64(len(maskedRaw)) <= parsed.InlineLimit {
			body, bodyErr := values.NewInline(string(maskedRaw), outputMetadata(identity, OutputBody, baseMediaType, values.RedactionPrivate))
			if bodyErr != nil {
				return nil, bodyErr
			}
			outputs[OutputBody] = body
		} else {
			artifact, artifactErr := k.captureArtifact(ctx, identity, parsed, maskedRaw, baseMediaType, redactor)
			if artifactErr != nil {
				return nil, artifactErr
			}
			outputs[OutputBody] = artifact
		}
	} else {
		if parsed.HasExpectedJSONSchema {
			return nil, fmt.Errorf("%w: JSON schema requires a JSON response", ErrInvalidResult)
		}
		artifact, artifactErr := k.captureArtifact(ctx, identity, parsed, maskedRaw, baseMediaType, redactor)
		if artifactErr != nil {
			return nil, artifactErr
		}
		outputs[OutputBody] = artifact
	}
	if err := values.ValidatePersistableSet(outputs); err != nil {
		return nil, fmt.Errorf("%w: output set is not persistable", ErrInvalidResult)
	}
	return outputs, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(buffer)
	if contextErr := r.ctx.Err(); contextErr != nil {
		return n, contextErr
	}
	return n, err
}

func (k *Kind) captureArtifact(ctx context.Context, identity stepkind.InvocationIdentity, parsed config, content []byte, mediaType string, redactor *values.Redactor) (values.Value, error) {
	if nilInterface(k.artifacts) {
		return values.Value{}, fmt.Errorf("%w: artifact sink is required for this response", ErrInvalidResult)
	}
	metadata := outputMetadata(identity, OutputBody, mediaType, values.RedactionPrivate)
	value, err := k.artifacts.CaptureArtifact(ctx, ArtifactRequest{
		Name: "http-response", Content: append([]byte(nil), content...), Metadata: metadata,
		RunID: identity.RunID, MaxBytes: parsed.MaxResponseBytes,
	})
	if err != nil {
		return values.Value{}, fmt.Errorf("%w: artifact capture failed: %w", ErrInvalidResult, err)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return values.Value{}, contextErr
	}
	if err := values.ValidatePersistable(value); err != nil || value.Type != values.TypeArtifact || value.Artifact == nil {
		return values.Value{}, fmt.Errorf("%w: artifact sink returned an invalid value", ErrInvalidResult)
	}
	if value.Producer != metadata.Producer || value.MediaType != metadata.MediaType || value.Redaction != metadata.Redaction || value.Retention != metadata.Retention ||
		value.Artifact.SizeBytes != int64(len(content)) || value.Digest != values.SHA256Digest(content) || value.Artifact.SizeBytes > parsed.MaxResponseBytes {
		return values.Value{}, fmt.Errorf("%w: artifact sink result does not match captured content", ErrInvalidResult)
	}
	if redactor.MaskString(value.Artifact.Store) != value.Artifact.Store || redactor.MaskString(value.Artifact.URI) != value.Artifact.URI {
		return values.Value{}, fmt.Errorf("%w: artifact sink result contains secret material", ErrInvalidResult)
	}
	return value, nil
}

func outputMetadata(identity stepkind.InvocationIdentity, output, mediaType string, redaction values.RedactionClass) values.Metadata {
	return values.Metadata{
		Producer:  values.Producer{Kind: KindName, Reference: identity.RunID + "/" + identity.NodeID + "/" + strconv.Itoa(identity.Attempt), Output: output},
		MediaType: mediaType, Redaction: redaction, Retention: values.RetentionRun,
	}
}

func decodeSingleJSON(content []byte) (any, error) {
	if !utf8.Valid(content) {
		return nil, fmt.Errorf("JSON must use valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("trailing JSON content")
	}
	return value, nil
}

func maskJSON(input any, redactor *values.Redactor) (any, error) {
	switch typed := input.(type) {
	case nil, bool, json.Number:
		return typed, nil
	case string:
		return redactor.MaskString(typed), nil
	case []any:
		result := make([]any, len(typed))
		for index, child := range typed {
			masked, err := maskJSON(child, redactor)
			if err != nil {
				return nil, err
			}
			result[index] = masked
		}
		return result, nil
	case map[string]any:
		result := make(map[string]any, len(typed))
		keys := sortedKeys(typed)
		for _, key := range keys {
			maskedKey := redactor.MaskString(key)
			if _, exists := result[maskedKey]; exists {
				return nil, fmt.Errorf("redaction collapses distinct object keys")
			}
			masked, err := maskJSON(typed[key], redactor)
			if err != nil {
				return nil, err
			}
			result[maskedKey] = masked
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported JSON value %T", input)
	}
}

func isJSONMediaType(mediaType string) bool {
	return mediaType == "application/json" || strings.HasSuffix(mediaType, "+json")
}

func isTextMediaType(mediaType string) bool {
	return strings.HasPrefix(mediaType, "text/") || isJSONMediaType(mediaType) ||
		mediaType == "application/xml" || strings.HasSuffix(mediaType, "+xml") ||
		mediaType == "application/x-www-form-urlencoded"
}

func containsString(values []string, target string) bool {
	index := sort.SearchStrings(values, target)
	return index < len(values) && values[index] == target
}

func stringSliceMapToAny(input map[string][]string) map[string]any {
	result := make(map[string]any, len(input))
	for key, entries := range input {
		array := make([]any, len(entries))
		for index, entry := range entries {
			array[index] = entry
		}
		result[key] = array
	}
	return result
}
