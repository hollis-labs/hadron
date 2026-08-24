package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

type resultCounts struct {
	structured int
	text       int
	resource   int
	artifact   int
}

func (k *Kind) mapResult(
	ctx context.Context,
	identity stepkind.InvocationIdentity,
	parsed config,
	description ConfigDescription,
	result CallResult,
	redactor *values.Redactor,
) (values.ValueSet, resultCounts, error) {
	transport, maskErr := maskTransportMetadata(result.Transport, redactor)
	if maskErr != nil {
		return nil, resultCounts{}, maskErr
	}
	if validationErr := validateTransportMetadata(transport); validationErr != nil {
		return nil, resultCounts{}, validationErr
	}
	outputs := values.ValueSet{}
	counts := resultCounts{}
	producerReference := invocationReference(identity)
	if result.HasStructured {
		masked, structuredErr := maskJSON(result.Structured, redactor)
		if structuredErr != nil {
			return nil, counts, fmt.Errorf("structured content: %w", structuredErr)
		}
		encoded, encodeErr := json.Marshal(masked)
		if encodeErr != nil {
			return nil, counts, fmt.Errorf("structured content: %w", encodeErr)
		}
		if int64(len(encoded)) > k.inlineLimit && counts.artifact >= k.maxArtifacts {
			return nil, counts, fmt.Errorf("tool result exceeds maximum artifact count %d", k.maxArtifacts)
		}
		value, artifact, captureErr := k.captureOrInline(ctx, identity, OutputStructured, encoded, masked, "application/json", false)
		if captureErr != nil {
			return nil, counts, captureErr
		}
		outputs[OutputStructured] = value
		counts.structured++
		if artifact {
			counts.artifact++
		}
	}

	texts := make([]any, 0)
	resources := make([]any, 0)
	artifactIndex := 0
	for index, content := range result.Content {
		content = maskContent(content, redactor)
		switch content.Kind {
		case ContentText:
			texts = append(texts, redactor.MaskString(content.Text))
			counts.text++
		case ContentImage, ContentAudio:
			if len(content.Data) == 0 || content.MediaType == "" {
				return nil, counts, fmt.Errorf("content[%d]: binary content requires data and media type", index)
			}
			name := artifactOutputName(artifactIndex)
			artifactIndex++
			if counts.artifact >= k.maxArtifacts {
				return nil, counts, fmt.Errorf("tool result exceeds maximum artifact count %d", k.maxArtifacts)
			}
			value, captureErr := k.captureArtifact(ctx, identity, name, content.Data, content.MediaType)
			if captureErr != nil {
				return nil, counts, fmt.Errorf("content[%d]: %w", index, captureErr)
			}
			outputs[name] = value
			resources = append(resources, resourceDescriptor(content, name))
			counts.artifact++
		case ContentResourceLink:
			if uriErr := validateStableText("resource URI", content.URI, true); uriErr != nil {
				return nil, counts, fmt.Errorf("content[%d]: %w", index, uriErr)
			}
			resources = append(resources, resourceDescriptor(content, ""))
			counts.resource++
		case ContentResourceText:
			if uriErr := validateStableText("resource URI", content.URI, true); uriErr != nil {
				return nil, counts, fmt.Errorf("content[%d]: %w", index, uriErr)
			}
			name := resourceOutputName(index)
			mediaType := content.MediaType
			if mediaType == "" {
				mediaType = "text/plain; charset=utf-8"
			}
			masked := redactor.MaskString(content.Text)
			if int64(len(masked)) > k.inlineLimit && counts.artifact >= k.maxArtifacts {
				return nil, counts, fmt.Errorf("tool result exceeds maximum artifact count %d", k.maxArtifacts)
			}
			value, artifact, captureErr := k.captureOrInline(ctx, identity, name, []byte(masked), masked, mediaType, false)
			if captureErr != nil {
				return nil, counts, fmt.Errorf("content[%d]: %w", index, captureErr)
			}
			outputs[name] = value
			resources = append(resources, resourceDescriptor(content, name))
			counts.resource++
			if artifact {
				counts.artifact++
			}
		case ContentResourceBlob:
			if uriErr := validateStableText("resource URI", content.URI, true); uriErr != nil {
				return nil, counts, fmt.Errorf("content[%d]: %w", index, uriErr)
			}
			if len(content.Data) == 0 || content.MediaType == "" {
				return nil, counts, fmt.Errorf("content[%d]: resource blob requires data and media type", index)
			}
			name := artifactOutputName(artifactIndex)
			artifactIndex++
			if counts.artifact >= k.maxArtifacts {
				return nil, counts, fmt.Errorf("tool result exceeds maximum artifact count %d", k.maxArtifacts)
			}
			value, captureErr := k.captureArtifact(ctx, identity, name, content.Data, content.MediaType)
			if captureErr != nil {
				return nil, counts, fmt.Errorf("content[%d]: %w", index, captureErr)
			}
			outputs[name] = value
			resources = append(resources, resourceDescriptor(content, name))
			counts.resource++
			counts.artifact++
		default:
			return nil, counts, fmt.Errorf("content[%d]: unsupported kind %q", index, content.Kind)
		}
	}

	if len(texts) != 0 {
		encoded, encodeErr := json.Marshal(texts)
		if encodeErr != nil {
			return nil, counts, encodeErr
		}
		if int64(len(encoded)) > k.inlineLimit && counts.artifact >= k.maxArtifacts {
			return nil, counts, fmt.Errorf("tool result exceeds maximum artifact count %d", k.maxArtifacts)
		}
		value, artifact, captureErr := k.captureOrInline(ctx, identity, OutputText, encoded, texts, "application/json", false)
		if captureErr != nil {
			return nil, counts, captureErr
		}
		outputs[OutputText] = value
		if artifact {
			counts.artifact++
		}
	}
	if len(resources) != 0 {
		value, inlineErr := values.NewInline(resources, outputMetadata(producerReference, OutputResources, "application/json", values.RedactionPrivate))
		if inlineErr != nil {
			return nil, counts, inlineErr
		}
		outputs[OutputResources] = value
	}
	metadata := map[string]any{
		"server":              parsed.Server,
		"tool":                parsed.Tool,
		"annotations":         annotationsMap(description.Annotations, redactor),
		"annotations_trusted": description.AnnotationsTrusted,
		"effects":             effectStrings(description.Effects),
		"idempotency":         string(description.Idempotency),
		"retry_safety":        string(description.RetrySafety),
		"transport":           transportMap(transport),
	}
	metadataValue, err := values.NewInline(metadata, outputMetadata(producerReference, OutputMetadata, "application/json", values.RedactionPublic))
	if err != nil {
		return nil, counts, err
	}
	outputs[OutputMetadata] = metadataValue
	if err := values.ValidatePersistableSet(outputs); err != nil {
		return nil, counts, err
	}
	return outputs, counts, nil
}

func (k *Kind) captureOrInline(ctx context.Context, identity stepkind.InvocationIdentity, name string, encoded []byte, inline any, mediaType string, forceArtifact bool) (values.Value, bool, error) {
	if !forceArtifact && int64(len(encoded)) <= k.inlineLimit {
		value, err := values.NewInline(inline, outputMetadata(invocationReference(identity), name, mediaType, values.RedactionPrivate))
		return value, false, err
	}
	value, err := k.captureArtifact(ctx, identity, name, encoded, mediaType)
	return value, true, err
}

func (k *Kind) captureArtifact(ctx context.Context, identity stepkind.InvocationIdentity, name string, content []byte, mediaType string) (values.Value, error) {
	if nilInterface(k.artifacts) {
		return values.Value{}, fmt.Errorf("artifact sink is required for large or binary MCP result content")
	}
	metadata := outputMetadata(invocationReference(identity), name, mediaType, values.RedactionPrivate)
	value, err := k.artifacts.CaptureArtifact(ctx, ArtifactRequest{
		Name: name, Content: append([]byte(nil), content...), Metadata: metadata, RunID: identity.RunID,
	})
	if err != nil {
		return values.Value{}, err
	}
	if err := values.ValidatePersistable(value); err != nil {
		return values.Value{}, fmt.Errorf("artifact sink returned invalid value: %w", err)
	}
	if value.Type != values.TypeArtifact || value.Producer != metadata.Producer || value.MediaType != metadata.MediaType ||
		value.Redaction != metadata.Redaction || value.Retention != metadata.Retention {
		return values.Value{}, fmt.Errorf("artifact sink returned mismatched value metadata")
	}
	return value, nil
}

func validateExpected(expected ExpectedResult, counts resultCounts) error {
	matched := expected == ExpectedAny ||
		(expected == ExpectedStructured && counts.structured > 0) ||
		(expected == ExpectedText && counts.text > 0) ||
		(expected == ExpectedResource && counts.resource > 0) ||
		(expected == ExpectedArtifact && counts.artifact > 0)
	if !matched {
		return fmt.Errorf("expected %s content", expected)
	}
	return nil
}

func validateTransportMetadata(metadata TransportMetadata) error {
	if metadata.AttemptCount < 0 || metadata.RetryCount < 0 ||
		(metadata.RetryCount > 0 && metadata.RetryCount >= metadata.AttemptCount) {
		return fmt.Errorf("%w: invalid transport attempt counts", ErrInvalidResult)
	}
	if err := validateStableText("transport", metadata.Transport, false); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidResult, err)
	}
	keys := make([]string, 0, len(metadata.Attributes))
	for key := range metadata.Attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := metadata.Attributes[key]
		if err := validateStableText("transport attribute key", key, true); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidResult, err)
		}
		if err := validateStableText("transport attribute value", value, false); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidResult, err)
		}
	}
	return nil
}

func outputMetadata(reference, output, mediaType string, redaction values.RedactionClass) values.Metadata {
	return values.Metadata{
		Producer:  values.Producer{Kind: KindName, Reference: reference, Output: output},
		MediaType: mediaType, Redaction: redaction, Retention: values.RetentionRun,
	}
}

func invocationReference(identity stepkind.InvocationIdentity) string {
	return identity.RunID + ":" + identity.NodeID + ":" + identity.Iteration + ":" + strconv.Itoa(identity.Attempt)
}

func artifactOutputName(index int) string { return fmt.Sprintf("artifact_%03d", index) }
func resourceOutputName(index int) string { return fmt.Sprintf("resource_%03d", index) }

func resourceDescriptor(content Content, output string) map[string]any {
	result := map[string]any{"kind": string(content.Kind)}
	if content.URI != "" {
		result["uri"] = content.URI
	}
	if content.Name != "" {
		result["name"] = content.Name
	}
	if content.Description != "" {
		result["description"] = content.Description
	}
	if content.MediaType != "" {
		result["media_type"] = content.MediaType
	}
	if output != "" {
		result["output"] = output
	}
	return result
}

func maskContent(content Content, redactor *values.Redactor) Content {
	content.Text = redactor.MaskString(content.Text)
	content.Data = redactor.MaskBytes(content.Data)
	content.URI = redactor.MaskString(content.URI)
	content.Name = redactor.MaskString(content.Name)
	content.Description = redactor.MaskString(content.Description)
	content.MediaType = redactor.MaskString(content.MediaType)
	return content
}

func maskTransportMetadata(metadata TransportMetadata, redactor *values.Redactor) (TransportMetadata, error) {
	result := metadata
	result.Transport = redactor.MaskString(metadata.Transport)
	if metadata.Attributes != nil {
		result.Attributes = make(map[string]string, len(metadata.Attributes))
		keys := make([]string, 0, len(metadata.Attributes))
		for key := range metadata.Attributes {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			maskedKey := redactor.MaskString(key)
			if _, duplicate := result.Attributes[maskedKey]; duplicate {
				return TransportMetadata{}, fmt.Errorf("transport metadata redaction collapses distinct keys")
			}
			result.Attributes[maskedKey] = redactor.MaskString(metadata.Attributes[key])
		}
	}
	return result, nil
}

func annotationsMap(annotations ToolAnnotations, redactor *values.Redactor) map[string]any {
	result := map[string]any{}
	if annotations.Title != "" {
		result["title"] = redactor.MaskString(annotations.Title)
	}
	if annotations.ReadOnlyHint != nil {
		result["read_only_hint"] = *annotations.ReadOnlyHint
	}
	if annotations.DestructiveHint != nil {
		result["destructive_hint"] = *annotations.DestructiveHint
	}
	if annotations.IdempotentHint != nil {
		result["idempotent_hint"] = *annotations.IdempotentHint
	}
	if annotations.OpenWorldHint != nil {
		result["open_world_hint"] = *annotations.OpenWorldHint
	}
	return result
}

func effectStrings(effects graph.EffectSet) []any {
	result := make([]any, len(effects))
	for index, effect := range effects {
		result[index] = string(effect)
	}
	return result
}

func transportMap(metadata TransportMetadata) map[string]any {
	result := map[string]any{
		"attempt_count": json.Number(strconv.Itoa(metadata.AttemptCount)),
		"retry_count":   json.Number(strconv.Itoa(metadata.RetryCount)),
		"reconnected":   metadata.Reconnected,
	}
	if metadata.Transport != "" {
		result["name"] = metadata.Transport
	}
	if len(metadata.Attributes) != 0 {
		attributes := make(map[string]any, len(metadata.Attributes))
		for key, value := range metadata.Attributes {
			attributes[key] = value
		}
		result["attributes"] = attributes
	}
	return result
}
