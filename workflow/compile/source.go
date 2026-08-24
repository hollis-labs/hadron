package compile

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"gopkg.in/yaml.v3"
)

const (
	// CodeUnsupportedSourceName identifies a source locator that is not a
	// preferred graph-native workflow filename.
	CodeUnsupportedSourceName diagnostic.Code = "HADR-SOURCE-001"
	// CodeMalformedSource identifies YAML that cannot form one source document.
	CodeMalformedSource diagnostic.Code = "HADR-SOURCE-002"
	// CodeMultipleSourceDocuments identifies a YAML stream with more than one
	// document.
	CodeMultipleSourceDocuments diagnostic.Code = "HADR-SOURCE-003"
	// CodeAmbiguousSourceShape identifies YAML mapping structure for which a
	// unique semantic source path cannot be constructed.
	CodeAmbiguousSourceShape diagnostic.Code = "HADR-SOURCE-004"
	// CodeLegacySource identifies an archived blueprint or pipeline root used as
	// graph-native workflow input.
	CodeLegacySource diagnostic.Code = "HADR-LEGACY-001"
)

const (
	sourceFormatDocumentation = "docs/architecture/workflow-engine-future-state/02-graph-ir-source-formats.md"
	migrationDocumentation    = "docs/architecture/workflow-engine-future-state/10-migration-safety-compatibility.md"
)

var yamlErrorLine = regexp.MustCompile(`(?:^|\s)line ([0-9]+)(?::|\s|$)`)

// Result is the outcome of loading workflow source. Source is present only
// when Diagnostics is empty.
type Result struct {
	Source      *Source
	Diagnostics []diagnostic.Diagnostic
}

// Source preserves a single YAML document and its graph-native source
// locations. Document is the unlowered yaml.v3 AST, including node tags,
// styles, anchors, aliases, and comments retained by the YAML parser.
//
// Semantic paths append a mapping key or a zero-based sequence index at each
// level. The empty path identifies the document root. YAML mappings must use
// unique string keys so every semantic path has exactly one meaning.
type Source struct {
	Locator  string
	Document *yaml.Node

	raw       []byte
	locations []sourceLocation
}

type sourceLocation struct {
	node      *yaml.Node
	reference graph.SourceRef
}

// Bytes returns a copy of the original source bytes.
func (s *Source) Bytes() []byte {
	if s == nil {
		return nil
	}
	return bytes.Clone(s.raw)
}

// Location returns the canonical source reference for a semantic YAML path.
func (s *Source) Location(path ...string) (graph.SourceRef, bool) {
	if s == nil {
		return graph.SourceRef{}, false
	}
	for _, location := range s.locations {
		if slices.Equal(location.reference.Path, path) {
			return cloneSourceRef(location.reference), true
		}
	}
	return graph.SourceRef{}, false
}

// Node returns the YAML AST node at a semantic path. Callers that mutate the
// returned node also mutate Document; source locations remain those recorded
// when the source was loaded.
func (s *Source) Node(path ...string) (*yaml.Node, bool) {
	if s == nil {
		return nil, false
	}
	for _, location := range s.locations {
		if slices.Equal(location.reference.Path, path) {
			return location.node, true
		}
	}
	return nil, false
}

// Locations returns all semantic source references in deterministic YAML
// preorder, beginning with the document root.
func (s *Source) Locations() []graph.SourceRef {
	if s == nil {
		return nil
	}
	locations := make([]graph.SourceRef, len(s.locations))
	for i, location := range s.locations {
		locations[i] = cloneSourceRef(location.reference)
	}
	return locations
}

// LoadFile loads a preferred workflow source file. Unsupported names and
// authoring failures are returned as diagnostics. Filesystem read failures are
// returned as ordinary errors.
func LoadFile(locator string) (Result, error) {
	if issue := unsupportedNameDiagnostic(locator); issue != nil {
		return Result{Diagnostics: []diagnostic.Diagnostic{*issue}}, nil
	}

	// #nosec G304 -- the caller-selected source path is this API's explicit input.
	data, err := os.ReadFile(locator)
	if err != nil {
		return Result{}, fmt.Errorf("read workflow source %q: %w", locator, err)
	}
	return loadBytes(locator, data), nil
}

// LoadBytes loads in-memory source identified by locator. The locator must use
// a preferred workflow filename so file and in-memory loading have identical
// source identity and diagnostic behavior.
func LoadBytes(locator string, data []byte) Result {
	if issue := unsupportedNameDiagnostic(locator); issue != nil {
		return Result{Diagnostics: []diagnostic.Diagnostic{*issue}}
	}
	return loadBytes(locator, data)
}

func loadBytes(locator string, data []byte) Result {
	document, issue := decodeDocument(locator, data)
	if issue != nil {
		return Result{Diagnostics: []diagnostic.Diagnostic{*issue}}
	}

	source := &Source{
		Locator:  locator,
		Document: document,
		raw:      bytes.Clone(data),
	}
	locations, sourceIssues := indexLocations(locator, document.Content[0])
	source.locations = locations
	if len(sourceIssues) != 0 {
		return Result{Diagnostics: sourceIssues}
	}
	if issue := legacySourceDiagnostic(source); issue != nil {
		return Result{Diagnostics: []diagnostic.Diagnostic{*issue}}
	}
	return Result{Source: source}
}

func decodeDocument(locator string, data []byte) (*yaml.Node, *diagnostic.Diagnostic) {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, malformedDiagnostic(locator, errors.New("source is empty"))
		}
		return nil, malformedDiagnostic(locator, err)
	}
	if document.Kind != yaml.DocumentNode || len(document.Content) != 1 {
		return nil, malformedDiagnostic(locator, errors.New("source has no YAML document root"))
	}

	var trailing yaml.Node
	if err := decoder.Decode(&trailing); err == nil {
		node := &trailing
		if len(trailing.Content) != 0 {
			node = trailing.Content[0]
		}
		ref := sourceRef(locator, graph.SourceWorkflow, node, nil)
		return nil, &diagnostic.Diagnostic{
			Severity: diagnostic.SeverityError,
			Code:     CodeMultipleSourceDocuments,
			Message:  "workflow source must contain exactly one YAML document",
			Source:   &ref,
			Remediation: &diagnostic.Remediation{
				Message:       "Move each additional document into its own *.workflow.yaml file.",
				Documentation: sourceFormatDocumentation,
			},
		}
	} else if !errors.Is(err, io.EOF) {
		return nil, malformedDiagnostic(locator, err)
	}

	return &document, nil
}

func unsupportedNameDiagnostic(locator string) *diagnostic.Diagnostic {
	name := filepath.Base(locator)
	if name == "workflow.yaml" || (strings.HasSuffix(name, ".workflow.yaml") && len(name) > len(".workflow.yaml")) {
		return nil
	}

	ref := sourceRef(diagnosticLocator(locator), graph.SourceWorkflow, nil, nil)
	return &diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError,
		Code:     CodeUnsupportedSourceName,
		Message:  fmt.Sprintf("source %q is not a supported workflow filename", locator),
		Source:   &ref,
		Remediation: &diagnostic.Remediation{
			Message:         "Rename the source to <name>.workflow.yaml or workflow.yaml.",
			SuggestedSyntax: "<name>.workflow.yaml",
			Documentation:   sourceFormatDocumentation,
		},
	}
}

func malformedDiagnostic(locator string, cause error) *diagnostic.Diagnostic {
	ref := sourceRef(locator, graph.SourceWorkflow, nil, nil)
	if matches := yamlErrorLine.FindStringSubmatch(cause.Error()); len(matches) == 2 {
		ref.StartLine, _ = strconv.Atoi(matches[1])
	}
	return &diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError,
		Code:     CodeMalformedSource,
		Message:  fmt.Sprintf("workflow source is not valid YAML: %v", cause),
		Source:   &ref,
		Remediation: &diagnostic.Remediation{
			Message:       "Correct the YAML syntax and load the source again.",
			Documentation: sourceFormatDocumentation,
		},
	}
}

func indexLocations(locator string, root *yaml.Node) ([]sourceLocation, []diagnostic.Diagnostic) {
	var locations []sourceLocation
	var issues []diagnostic.Diagnostic
	var visit func(*yaml.Node, []string)
	visit = func(node *yaml.Node, path []string) {
		ref := sourceRef(locator, graph.SourceWorkflow, node, path)
		locations = append(locations, sourceLocation{node: node, reference: ref})

		switch node.Kind {
		case yaml.MappingNode:
			seen := make(map[string]*yaml.Node, len(node.Content)/2)
			for i := 0; i+1 < len(node.Content); i += 2 {
				key, value := node.Content[i], node.Content[i+1]
				if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
					issues = append(issues, ambiguousKeyDiagnostic(locator, key, path))
					continue
				}
				childPath := appendPath(path, key.Value)
				if first, exists := seen[key.Value]; exists {
					issues = append(issues, duplicateKeyDiagnostic(locator, key, first, childPath))
				} else {
					seen[key.Value] = key
				}
				visit(value, childPath)
			}
		case yaml.SequenceNode:
			for i, child := range node.Content {
				visit(child, appendPath(path, strconv.Itoa(i)))
			}
		case yaml.DocumentNode, yaml.ScalarNode, yaml.AliasNode:
			// The loader indexes the document root directly. Scalar and alias
			// nodes are leaves whose own location was recorded above.
		}
	}
	visit(root, nil)
	return locations, issues
}

func ambiguousKeyDiagnostic(locator string, key *yaml.Node, path []string) diagnostic.Diagnostic {
	ref := sourceRef(locator, graph.SourceWorkflow, key, path)
	return diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError,
		Code:     CodeAmbiguousSourceShape,
		Message:  "workflow mappings must use string keys",
		Source:   &ref,
		Remediation: &diagnostic.Remediation{
			Message:       "Replace the mapping key with a unique string key.",
			Documentation: sourceFormatDocumentation,
		},
	}
}

func duplicateKeyDiagnostic(locator string, duplicate, first *yaml.Node, path []string) diagnostic.Diagnostic {
	ref := sourceRef(locator, graph.SourceWorkflow, duplicate, path)
	firstRef := sourceRef(locator, graph.SourceWorkflow, first, path)
	return diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError,
		Code:     CodeAmbiguousSourceShape,
		Message:  fmt.Sprintf("workflow mapping key %q is declared more than once", duplicate.Value),
		Source:   &ref,
		Related: []diagnostic.RelatedReference{{
			Message: "first declaration",
			Source:  firstRef,
		}},
		Remediation: &diagnostic.Remediation{
			Message:       "Remove or rename the duplicate mapping key.",
			Documentation: sourceFormatDocumentation,
		},
	}
}

func legacySourceDiagnostic(source *Source) *diagnostic.Diagnostic {
	root := source.Document.Content[0]
	if root.Kind != yaml.MappingNode {
		return nil
	}

	fields := mappingFields(root)
	if fields["workflow"] != nil {
		return nil
	}
	if blueprint := fields["blueprint"]; blueprint != nil && blueprint.value.Kind == yaml.MappingNode {
		return legacyDiagnostic(source.Locator, "blueprint", graph.SourceArchivedBlueprint, blueprint.key)
	}
	stages := fields["stages"]
	if stages != nil && stages.value.Kind == yaml.SequenceNode {
		return legacyDiagnostic(source.Locator, "pipeline", graph.SourceArchivedPipeline, stages.key)
	}
	return nil
}

type mappingField struct {
	key   *yaml.Node
	value *yaml.Node
}

func mappingFields(mapping *yaml.Node) map[string]*mappingField {
	fields := make(map[string]*mappingField, len(mapping.Content)/2)
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		key, value := mapping.Content[i], mapping.Content[i+1]
		if key.Kind == yaml.ScalarNode && key.Tag == "!!str" {
			fields[key.Value] = &mappingField{key: key, value: value}
		}
	}
	return fields
}

func legacyDiagnostic(locator, kind string, format graph.SourceFormat, key *yaml.Node) *diagnostic.Diagnostic {
	ref := sourceRef(locator, format, key, []string{key.Value})
	return &diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError,
		Code:     CodeLegacySource,
		Message:  fmt.Sprintf("archived %s source is not graph-native workflow input", kind),
		Source:   &ref,
		Remediation: &diagnostic.Remediation{
			Message:         fmt.Sprintf("Use the archived %s only as rewrite reference and author a graph-native workflow source.", kind),
			SuggestedSyntax: "workflow:\n  name: <workflow-name>\nsteps: []",
			Documentation:   migrationDocumentation,
		},
	}
}

func sourceRef(locator string, format graph.SourceFormat, node *yaml.Node, path []string) graph.SourceRef {
	ref := graph.SourceRef{
		Format:  format,
		Locator: locator,
		Path:    slices.Clone(path),
	}
	if node != nil {
		ref.StartLine = node.Line
		ref.StartColumn = node.Column
	}
	return ref
}

func cloneSourceRef(ref graph.SourceRef) graph.SourceRef {
	ref.Path = slices.Clone(ref.Path)
	return ref
}

func appendPath(path []string, part string) []string {
	child := make([]string, len(path)+1)
	copy(child, path)
	child[len(path)] = part
	return child
}

func diagnosticLocator(locator string) string {
	if strings.TrimSpace(locator) == "" {
		return "<memory>"
	}
	return locator
}
