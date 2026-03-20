package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/hollis-labs/hadron/internal/blueprint"
	"github.com/hollis-labs/hadron/internal/testgen"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func buildTestGenCmd() *cobra.Command {
	var (
		stdout    bool
		format    string
		outputDir string
	)

	cmd := &cobra.Command{
		Use:   "test-gen <blueprint-path>",
		Short: "Generate test input fixtures from a blueprint's input definitions",
		Long: `Generates valid, boundary, and invalid test input fixtures based on
a blueprint's input definitions. Output files are suitable for use with
hadron run --inputs-file.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bpPath := args[0]

			bp, err := blueprint.ParseFile(bpPath)
			if err != nil {
				return fmt.Errorf("parse blueprint: %w", err)
			}

			fs, err := testgen.GenerateFixtures(bp)
			if err != nil {
				return fmt.Errorf("generate fixtures: %w", err)
			}

			if stdout {
				return writeToStdout(fs, format)
			}

			return writeToFiles(fs, bpPath, outputDir, format)
		},
	}

	cmd.Flags().BoolVar(&stdout, "stdout", false, "print fixtures to stdout instead of writing files")
	cmd.Flags().StringVar(&format, "format", "yaml", "output format: yaml or json")
	cmd.Flags().StringVar(&outputDir, "output-dir", "testdata/generated", "base directory for output files")
	return cmd
}

func writeToStdout(fs *testgen.FixtureSet, format string) error {
	type output struct {
		Valid    map[string]any        `json:"valid" yaml:"valid"`
		Boundary []map[string]any      `json:"boundary" yaml:"boundary"`
		Invalid  []testgen.InvalidCase `json:"invalid" yaml:"invalid"`
	}

	data := output{
		Valid:    fs.Valid,
		Boundary: fs.Boundary,
		Invalid:  fs.Invalid,
	}

	if format == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(data)
	}

	return yaml.NewEncoder(os.Stdout).Encode(data)
}

func writeToFiles(fs *testgen.FixtureSet, bpPath, outputDir, format string) error {
	// Derive subdirectory name from blueprint filename.
	base := filepath.Base(bpPath)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)

	dir := filepath.Join(outputDir, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	fileExt := ".yaml"
	if format == "json" {
		fileExt = ".json"
	}

	marshal := marshalYAML
	if format == "json" {
		marshal = marshalJSON
	}

	// Write valid fixture.
	if err := writeFixtureFile(filepath.Join(dir, "valid"+fileExt), fs.Valid, "", marshal); err != nil {
		return err
	}
	fmt.Printf("wrote %s\n", filepath.Join(dir, "valid"+fileExt))

	// Write boundary fixtures.
	for i, bfix := range fs.Boundary {
		fname := fmt.Sprintf("boundary-%d%s", i, fileExt)
		if err := writeFixtureFile(filepath.Join(dir, fname), bfix, "", marshal); err != nil {
			return err
		}
		fmt.Printf("wrote %s\n", filepath.Join(dir, fname))
	}

	// Write invalid fixtures.
	for i, ic := range fs.Invalid {
		fname := fmt.Sprintf("invalid-%d%s", i, fileExt)
		comment := fmt.Sprintf("# Invalid: %s\n", ic.Reason)
		if format == "json" {
			comment = "" // JSON doesn't support comments; reason is in the file content.
		}
		if err := writeFixtureFile(filepath.Join(dir, fname), ic.Inputs, comment, marshal); err != nil {
			return err
		}
		fmt.Printf("wrote %s\n", filepath.Join(dir, fname))
	}

	return nil
}

type marshalFunc func(v any) ([]byte, error)

func marshalYAML(v any) ([]byte, error) {
	return yaml.Marshal(v)
}

func marshalJSON(v any) ([]byte, error) {
	return json.MarshalIndent(v, "", "  ")
}

func writeFixtureFile(path string, data any, prefix string, marshal marshalFunc) error {
	b, err := marshal(data)
	if err != nil {
		return fmt.Errorf("marshal fixture: %w", err)
	}

	content := prefix + string(b)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}
