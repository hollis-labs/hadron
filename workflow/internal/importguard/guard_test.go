package importguard

import (
	"fmt"
	"go/build"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const (
	hadronImportPath           = "github.com/hollis-labs/hadron"
	workflowImportPath         = hadronImportPath + "/workflow"
	workflowAdaptersImportPath = workflowImportPath + "/adapters"
)

var allowedExternalImports = []string{
	"github.com/expr-lang/expr",
	"github.com/santhosh-tekuri/jsonschema/v6",
	"gopkg.in/yaml.v3",
}

// allowedTransitiveDependencyImports applies only to the resolved production
// dependency graph. Workflow source files still must import an adopted root
// from allowedExternalImports rather than importing its implementation
// dependencies directly.
var allowedTransitiveDependencyImports = []string{
	"golang.org/x/text", // github.com/santhosh-tekuri/jsonschema/v6 closure
}

var siblingApplicationImports = []string{
	"github.com/hollis-labs/cerberus",
	"github.com/hollis-labs/nanite",
	"github.com/hollis-labs/tether",
	"github.com/hollis-labs/torque",
}

type violation struct {
	file     string
	line     int
	importer string
	imported string
	reason   string
}

func (v violation) String() string {
	return fmt.Sprintf("%s:%d: %s imports %q: %s", v.file, v.line, v.importer, v.imported, v.reason)
}

func TestWorkflowCoreImports(t *testing.T) {
	root := moduleRoot(t)
	err := validateImports(filepath.Join(root, "workflow"), workflowImportPath, skipNonCoreDirectory)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWorkflowCoreDependencyGraph(t *testing.T) {
	root := moduleRoot(t)
	packages := goList(t, root, "-f", "{{.ImportPath}}", "./workflow/...")
	corePackages := packages[:0]
	for _, packagePath := range packages {
		if pathWithin(packagePath, workflowImportPath) && !pathWithin(packagePath, workflowAdaptersImportPath) {
			corePackages = append(corePackages, packagePath)
		}
	}
	if len(corePackages) == 0 {
		t.Fatal("workflow core dependency guard found no core packages")
	}

	args := []string{"-deps", "-f", "{{.ImportPath}}"}
	dependencies := goList(t, root, append(args, corePackages...)...)
	if err := validateDependencies(dependencies); err != nil {
		t.Fatal(err)
	}
}

func TestEveryWorkflowPackageHasNoHadronInternalDependency(t *testing.T) {
	root := moduleRoot(t)
	dependencies := goList(t, root, "-deps", "-f", "{{.ImportPath}}", "./workflow/...")
	var rejected []string
	for _, dependency := range dependencies {
		if pathWithin(dependency, hadronImportPath+"/internal") {
			rejected = append(rejected, dependency)
		}
	}
	if len(rejected) != 0 {
		sort.Strings(rejected)
		t.Fatalf("workflow public packages depend on Hadron internal packages: %s", strings.Join(rejected, ", "))
	}
}

func TestForbiddenImportFixture(t *testing.T) {
	root := moduleRoot(t)
	fixture := filepath.Join(root, "workflow", "internal", "importguard", "testdata", "forbidden")
	err := validateImports(fixture, workflowImportPath+"/forbiddenfixture", nil)
	if err == nil {
		t.Fatal("expected the forbidden-import fixture to fail the workflow core import guard")
	}

	const want = "workflow core import guard failed:\n" +
		"- forbidden.go:5: github.com/hollis-labs/hadron/workflow/forbiddenfixture imports " +
		"\"github.com/hollis-labs/hadron/internal/persistence\": " +
		"Hadron packages outside workflow core are host-owned"
	if err.Error() != want {
		t.Fatalf("guard failure = %q, want %q", err, want)
	}
	t.Log(err)
}

func TestImportPolicy(t *testing.T) {
	t.Run("allowed", func(t *testing.T) {
		cases := []struct {
			name     string
			importer string
			imported string
		}{
			{name: "standard library", importer: workflowImportPath + "/runtime", imported: "context"},
			{name: "workflow core", importer: workflowImportPath + "/runtime", imported: workflowImportPath + "/graph"},
			{name: "yaml source parser", importer: workflowImportPath + "/compile", imported: "gopkg.in/yaml.v3"},
			{name: "schema validator", importer: workflowImportPath + "/compile", imported: "github.com/santhosh-tekuri/jsonschema/v6"},
			{name: "expression engine", importer: workflowImportPath + "/values", imported: "github.com/expr-lang/expr/vm"},
			{name: "adapter dependency", importer: workflowAdaptersImportPath + "/mcp", imported: "github.com/mark3labs/mcp-go/mcp"},
			{name: "Hadron host dependency", importer: hadronImportPath + "/internal/appworkflow", imported: "modernc.org/sqlite"},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				if reason := forbiddenImportReason(tc.importer, tc.imported); reason != "" {
					t.Fatalf("expected import to be allowed, got: %s", reason)
				}
			})
		}
	})

	t.Run("forbidden", func(t *testing.T) {
		cases := []struct {
			name     string
			imported string
			want     string
		}{
			{name: "Hadron internal", imported: hadronImportPath + "/internal/persistence", want: "host-owned"},
			{name: "Hadron command", imported: hadronImportPath + "/cmd/hadrond", want: "host-owned"},
			{name: "workflow adapter", imported: workflowAdaptersImportPath + "/http", want: "adapter packages"},
			{name: "Wails", imported: "github.com/wailsapp/wails/v2/pkg/runtime", want: "concrete UI"},
			{name: "HTTP server", imported: "github.com/labstack/echo/v4", want: "concrete transport"},
			{name: "MCP server", imported: "github.com/mark3labs/mcp-go/server", want: "concrete MCP"},
			{name: "Hollis MCP SDK", imported: "github.com/hollis-labs/go-mcp", want: "concrete MCP"},
			{name: "SQLite", imported: "modernc.org/sqlite", want: "concrete persistence"},
			{name: "model provider", imported: "github.com/hollis-labs/go-providers", want: "provider or agent SDK"},
			{name: "LLM types", imported: "github.com/hollis-labs/go-llm-types", want: "provider or agent SDK"},
			{name: "agent SDK", imported: "github.com/hollis-labs/agentkit", want: "provider or agent SDK"},
			{name: "Nanite", imported: "github.com/hollis-labs/nanite/internal/workflow", want: "sibling application"},
			{name: "Torque", imported: "github.com/hollis-labs/torque/internal/api", want: "sibling application"},
			{name: "Tether", imported: "github.com/hollis-labs/tether/client", want: "sibling application"},
			{name: "Cerberus", imported: "github.com/hollis-labs/cerberus/pkg/auth", want: "sibling application"},
			{name: "dotless non-standard", imported: "example/library", want: "not on the workflow core allowlist"},
			{name: "unapproved external", imported: "example.com/library", want: "not on the workflow core allowlist"},
			{name: "schema validator transitive", imported: "golang.org/x/text/message", want: "not on the workflow core allowlist"},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				reason := forbiddenImportReason(workflowImportPath+"/runtime", tc.imported)
				if !strings.Contains(reason, tc.want) {
					t.Fatalf("forbiddenImportReason() = %q, want substring %q", reason, tc.want)
				}
			})
		}
	})
}

func TestDependencyGraphPolicyAllowsAdoptedTransitiveClosureOnly(t *testing.T) {
	if reason := forbiddenDependencyReason("golang.org/x/text/message"); reason != "" {
		t.Fatalf("schema-validator transitive dependency rejected: %s", reason)
	}
	if reason := forbiddenImportReason(workflowImportPath+"/compile", "golang.org/x/text/message"); reason == "" {
		t.Fatal("direct workflow import of a transitive dependency was allowed")
	}
	if reason := forbiddenDependencyReason("example.com/transitive"); reason == "" {
		t.Fatal("unapproved transitive dependency was allowed")
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate import guard source file")
	}

	for dir := filepath.Dir(filename); ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		} else if !os.IsNotExist(err) {
			t.Fatalf("locate module root: %v", err)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("locate module root: go.mod not found")
		}
	}
}

func goList(t *testing.T, root string, args ...string) []string {
	t.Helper()

	args = append([]string{"list", "-mod=readonly"}, args...)
	cmd := exec.CommandContext(t.Context(), "go", args...)
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return strings.Fields(string(output))
}

func validateImports(root, importRoot string, skipDir func(string, fs.DirEntry) bool) error {
	violations, err := scanImports(root, importRoot, skipDir)
	if err != nil {
		return fmt.Errorf("scan workflow core imports: %w", err)
	}
	if len(violations) == 0 {
		return nil
	}

	sort.Slice(violations, func(i, j int) bool {
		if violations[i].file == violations[j].file {
			return violations[i].line < violations[j].line
		}
		return violations[i].file < violations[j].file
	})

	var findings strings.Builder
	for _, finding := range violations {
		fmt.Fprintf(&findings, "\n- %s", finding)
	}
	return fmt.Errorf("workflow core import guard failed:%s", findings.String())
}

func validateDependencies(dependencies []string) error {
	rejected := make(map[string]string)
	for _, dependency := range dependencies {
		if reason := forbiddenDependencyReason(dependency); reason != "" {
			rejected[dependency] = reason
		}
	}
	if len(rejected) == 0 {
		return nil
	}

	paths := make([]string, 0, len(rejected))
	for path := range rejected {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	var findings strings.Builder
	for _, path := range paths {
		fmt.Fprintf(&findings, "\n- %s: %s", path, rejected[path])
	}
	return fmt.Errorf("workflow core dependency guard failed:%s", findings.String())
}

func forbiddenDependencyReason(imported string) string {
	for _, allowed := range allowedTransitiveDependencyImports {
		if pathWithin(imported, allowed) {
			return ""
		}
	}
	return forbiddenImportReason(workflowImportPath+"/dependencygraph", imported)
}

func scanImports(root, importRoot string, skipDir func(string, fs.DirEntry) bool) ([]violation, error) {
	fset := token.NewFileSet()
	var violations []violation

	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if rel != "." && skipDir != nil && skipDir(filepath.ToSlash(rel), entry) {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}

		file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		importer := importRoot
		if relDir := filepath.ToSlash(filepath.Dir(rel)); relDir != "." {
			importer += "/" + relDir
		}
		for _, spec := range file.Imports {
			imported, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return fmt.Errorf("parse import in %s: %w", rel, err)
			}
			reason := forbiddenImportReason(importer, imported)
			if reason == "" {
				continue
			}
			violations = append(violations, violation{
				file:     filepath.ToSlash(rel),
				line:     fset.Position(spec.Pos()).Line,
				importer: importer,
				imported: imported,
				reason:   reason,
			})
		}
		return nil
	})
	return violations, err
}

func skipNonCoreDirectory(rel string, entry fs.DirEntry) bool {
	return entry.Name() == "testdata" || rel == "adapters" || strings.HasPrefix(rel, "adapters/")
}

func forbiddenImportReason(importer, imported string) string {
	if !pathWithin(importer, workflowImportPath) || pathWithin(importer, workflowAdaptersImportPath) {
		return ""
	}
	if isStandardLibrary(imported) {
		return ""
	}
	if pathWithin(imported, workflowAdaptersImportPath) {
		return "workflow core must not depend on adapter packages"
	}
	if pathWithin(imported, workflowImportPath) {
		return ""
	}
	for _, allowed := range allowedExternalImports {
		if pathWithin(imported, allowed) {
			return ""
		}
	}
	if pathWithin(imported, hadronImportPath) {
		return "Hadron packages outside workflow core are host-owned"
	}
	for _, sibling := range siblingApplicationImports {
		if pathWithin(imported, sibling) {
			return "sibling application packages are not workflow core dependencies"
		}
	}

	switch {
	case pathWithin(imported, "github.com/wailsapp/wails"):
		return "concrete UI dependencies belong in a host or adapter"
	case pathWithin(imported, "github.com/labstack/echo"), pathWithin(imported, "github.com/gorilla/websocket"):
		return "concrete transport dependencies belong in a host or adapter"
	case pathWithin(imported, "github.com/mark3labs/mcp-go"), pathWithin(imported, "github.com/hollis-labs/go-mcp"):
		return "concrete MCP dependencies belong in an adapter"
	case pathWithin(imported, "modernc.org/sqlite"), pathWithin(imported, "github.com/mattn/go-sqlite3"):
		return "concrete persistence dependencies belong in a host or adapter"
	case pathWithin(imported, "github.com/hollis-labs/go-providers"),
		pathWithin(imported, "github.com/hollis-labs/go-llm-types"),
		pathWithin(imported, "github.com/hollis-labs/go-llm-contracts"),
		pathWithin(imported, "github.com/hollis-labs/agentkit"):
		return "provider or agent SDK dependencies belong in an adapter"
	default:
		return "external dependency is not on the workflow core allowlist"
	}
}

func pathWithin(path, root string) bool {
	return path == root || strings.HasPrefix(path, root+"/")
}

func isStandardLibrary(importPath string) bool {
	pkg, err := build.Default.Import(importPath, ".", build.FindOnly)
	return err == nil && pkg.Goroot
}
