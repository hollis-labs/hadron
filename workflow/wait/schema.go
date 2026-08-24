package wait

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

// SchemaRef is an immutable digest-bound inline JSON Schema declaration.
type SchemaRef struct {
	Digest string       `json:"digest"`
	Schema graph.Schema `json:"schema"`
}

// NewSchemaRef validates and defensively copies a schema.
func NewSchemaRef(schema graph.Schema) (SchemaRef, error) {
	if schema == nil {
		schema = graph.Schema{}
	}
	if err := values.ValidateSchema(schema); err != nil {
		return SchemaRef{}, err
	}
	encoded, err := json.Marshal(schema)
	if err != nil {
		return SchemaRef{}, fmt.Errorf("marshal wait resume schema: %w", err)
	}
	copySchema, err := decodeSchema(encoded)
	if err != nil {
		return SchemaRef{}, err
	}
	return SchemaRef{Digest: values.SHA256Digest(encoded), Schema: copySchema}, nil
}

// Validate verifies the schema declaration and its deterministic digest.
func (r SchemaRef) Validate() error {
	if err := values.ValidateDigest(r.Digest); err != nil {
		return err
	}
	if r.Schema == nil {
		return fmt.Errorf("resume schema must not be null")
	}
	if err := values.ValidateSchema(r.Schema); err != nil {
		return err
	}
	encoded, err := json.Marshal(r.Schema)
	if err != nil {
		return fmt.Errorf("marshal wait resume schema: %w", err)
	}
	if digest := values.SHA256Digest(encoded); digest != r.Digest {
		return fmt.Errorf("resume schema digest mismatch: recorded %q, computed %q", r.Digest, digest)
	}
	return nil
}

func decodeSchema(encoded []byte) (graph.Schema, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var schema graph.Schema
	if err := decoder.Decode(&schema); err != nil {
		return nil, fmt.Errorf("decode wait resume schema: %w", err)
	}
	return schema, nil
}
