package schema_test

import (
	"bytes"
	"testing"

	graphschema "github.com/hollis-labs/hadron/workflow/graph/schema"
)

func TestDocumentIsDefensiveAndValidatesJSON(t *testing.T) {
	first := graphschema.Document()
	second := graphschema.Document()
	if len(first) == 0 || !bytes.Equal(first, second) {
		t.Fatal("embedded graph schema is unavailable or unstable")
	}
	first[0] = 'x'
	if bytes.Equal(first, graphschema.Document()) {
		t.Fatal("Document returned mutable shared bytes")
	}
	valid := []byte(`{"id":"schema-test","version":"v1","digest":"","nodes":[]}`)
	if err := graphschema.Validate(valid); err != nil {
		t.Fatalf("Validate(valid) = %v", err)
	}
	if err := graphschema.Validate([]byte(`{"id":"schema-test","version":"v1","digest":"","nodes":[],"unknown":true}`)); err == nil {
		t.Fatal("Validate accepted an unknown graph field")
	}
}
