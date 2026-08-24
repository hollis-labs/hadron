package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hollis-labs/hadron/workflow/compile/internal/planschema"
)

func main() {
	output := flag.String("output", "schema/execution-plan.schema.json", "generated schema output path")
	flag.Parse()

	data, err := planschema.Generate()
	if err != nil {
		fail(err)
	}
	if err := os.MkdirAll(filepath.Dir(*output), 0o755); err != nil { //nolint:gosec // Generated schema directories use repository-standard permissions.
		fail(fmt.Errorf("create schema output directory: %w", err))
	}
	if err := os.WriteFile(*output, data, 0o644); err != nil { //nolint:gosec // Generated schemas are non-sensitive repository artifacts.
		fail(fmt.Errorf("write schema: %w", err))
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
