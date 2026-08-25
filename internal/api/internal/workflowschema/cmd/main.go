package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hollis-labs/hadron/internal/api/internal/workflowschema"
	graphschema "github.com/hollis-labs/hadron/workflow/graph/schema"
)

func main() {
	schemaPath := flag.String("schema", "", "workflow API schema output")
	typescriptPath := flag.String("typescript", "", "generated TypeScript output")
	flag.Parse()
	if *schemaPath == "" || *typescriptPath == "" {
		fatalf("schema and typescript outputs are required")
	}
	apiSchema, err := workflowschema.Generate()
	if err != nil {
		fatalf("generate schema: %v", err)
	}
	typescript, err := workflowschema.GenerateTypeScript(graphschema.Document(), apiSchema)
	if err != nil {
		fatalf("generate TypeScript: %v", err)
	}
	for path, data := range map[string][]byte{*schemaPath: apiSchema, *typescriptPath: typescript} {
		// #nosec G301 -- generated committed schema/client directories use conventional repository permissions.
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			fatalf("create output directory: %v", err)
		}
		// #nosec G306 -- generated committed schema/client files must be readable by repository tooling.
		if err := os.WriteFile(path, data, 0o644); err != nil {
			fatalf("write %s: %v", path, err)
		}
	}
}

func fatalf(format string, values ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", values...)
	os.Exit(1)
}
