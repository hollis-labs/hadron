// Package schema exposes the committed graph IR JSON Schema as a strict,
// extraction-safe validation boundary for SDK, UI, and agent front ends.
package schema

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	// ID is the immutable identifier carried by graph IR authoring envelopes.
	ID = "https://schemas.hollis-labs.dev/workflow/graph/v1/workflow.schema.json"
	// Version is the compact negotiation version for ID.
	Version = "1"
)

//go:embed workflow.schema.json
var document []byte

var (
	compileOnce sync.Once
	compiled    *jsonschema.Schema
	compileErr  error
)

// Document returns a defensive copy of the committed schema bytes.
func Document() []byte { return append([]byte(nil), document...) }

// Validate validates one JSON graph value against the committed schema.
func Validate(data []byte) error {
	compileOnce.Do(func() {
		decoder := json.NewDecoder(bytes.NewReader(document))
		decoder.UseNumber()
		var schemaDocument any
		if err := decoder.Decode(&schemaDocument); err != nil {
			compileErr = err
			return
		}
		compiler := jsonschema.NewCompiler()
		if err := compiler.AddResource(ID, schemaDocument); err != nil {
			compileErr = err
			return
		}
		compiled, compileErr = compiler.Compile(ID)
	})
	if compileErr != nil {
		return fmt.Errorf("compile workflow graph schema: %w", compileErr)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("decode workflow graph JSON: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("decode workflow graph JSON: trailing value")
	}
	if err := compiled.Validate(value); err != nil {
		return fmt.Errorf("validate workflow graph schema: %w", err)
	}
	return nil
}
