package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/libs/workflow/engine/diagnostic"
	"github.com/hollis-labs/libs/workflow/engine/graph"
	"github.com/hollis-labs/libs/workflow/engine/values"
	"github.com/spf13/cobra"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
)

const (
	maximumWorkflowInputBytes   = 1 << 20
	maximumWorkflowRequestBytes = 2 << 20
)

type workflowIdentityFlags struct {
	principal, scopeKind, scopeID string
	targetID                      string
	targetKinds                   []string
	targetCapabilities            []string
	targetLabels                  []string
	targetSandboxes               []string
}

func (flags *workflowIdentityFlags) bind(command *cobra.Command) {
	command.Flags().StringVar(&flags.principal, "principal", "", "caller identity hint (authentication remains daemon-owned)")
	command.Flags().StringVar(&flags.scopeKind, "scope-kind", "", "run scope kind: project, account, session, team, or user")
	command.Flags().StringVar(&flags.scopeID, "scope-id", "", "run scope identity")
	command.Flags().StringVar(&flags.targetID, "target-id", "", "exact execution target identity")
	command.Flags().StringSliceVar(&flags.targetKinds, "target-kind", nil, "allowed execution target kind (repeatable)")
	command.Flags().StringSliceVar(&flags.targetCapabilities, "target-capability", nil, "required execution capability (repeatable)")
	command.Flags().StringArrayVar(&flags.targetLabels, "target-label", nil, "required execution target label key=value (repeatable)")
	command.Flags().StringSliceVar(&flags.targetSandboxes, "target-sandbox", nil, "allowed sandbox mode (repeatable)")
}

func (flags workflowIdentityFlags) request() (appworkflow.IdentityRequest, error) {
	request := appworkflow.IdentityRequest{PrincipalHint: flags.principal, SourceAuthority: "cli"}
	if (flags.scopeKind == "") != (flags.scopeID == "") {
		return appworkflow.IdentityRequest{}, errors.New("--scope-kind and --scope-id must be supplied together")
	}
	if flags.scopeKind != "" {
		request.RunScope = &hoststate.RunScopeSelector{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeKind(flags.scopeKind), ID: flags.scopeID}
		if err := request.RunScope.Validate(); err != nil {
			return appworkflow.IdentityRequest{}, fmt.Errorf("run scope: %w", err)
		}
	}
	if flags.targetID != "" || len(flags.targetKinds) != 0 || len(flags.targetCapabilities) != 0 || len(flags.targetLabels) != 0 || len(flags.targetSandboxes) != 0 {
		target := hoststate.ExecutionTargetSelector{Version: hoststate.ScopeTargetVersionV1, ID: flags.targetID, RequiredLabels: make(map[string]string)}
		for _, value := range flags.targetKinds {
			target.Kinds = append(target.Kinds, hoststate.ExecutionTargetKind(value))
		}
		target.RequiredCapabilities = append(target.RequiredCapabilities, flags.targetCapabilities...)
		for _, value := range flags.targetSandboxes {
			target.SandboxModes = append(target.SandboxModes, hoststate.SandboxMode(value))
		}
		for _, label := range flags.targetLabels {
			key, value, ok := strings.Cut(label, "=")
			if !ok || strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
				return appworkflow.IdentityRequest{}, fmt.Errorf("invalid --target-label %q", label)
			}
			if _, duplicate := target.RequiredLabels[key]; duplicate {
				return appworkflow.IdentityRequest{}, fmt.Errorf("duplicate target label %q", key)
			}
			target.RequiredLabels[key] = value
		}
		if len(target.RequiredLabels) == 0 {
			target.RequiredLabels = nil
		}
		sort.Slice(target.Kinds, func(i, j int) bool { return target.Kinds[i] < target.Kinds[j] })
		sort.Strings(target.RequiredCapabilities)
		sort.Slice(target.SandboxModes, func(i, j int) bool { return target.SandboxModes[i] < target.SandboxModes[j] })
		if duplicateStringers(target.Kinds) || duplicateStrings(target.RequiredCapabilities) || duplicateStringers(target.SandboxModes) {
			return appworkflow.IdentityRequest{}, errors.New("execution target selectors must not contain duplicates")
		}
		if err := target.Validate(); err != nil {
			return appworkflow.IdentityRequest{}, fmt.Errorf("execution target: %w", err)
		}
		request.ExecutionTarget = &target
	}
	return request, nil
}

func parseWorkflowDefinitionRef(raw string) (graph.DefinitionRef, error) {
	if !utf8.ValidString(raw) || strings.TrimSpace(raw) != raw || raw == "" || len(raw) > 4096 || strings.IndexFunc(raw, unicode.IsControl) >= 0 {
		return graph.DefinitionRef{}, errors.New("workflow reference is invalid or too large")
	}
	if info, err := os.Stat(raw); err == nil {
		if info.Mode().IsRegular() || info.IsDir() {
			return graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, Locator: filepath.Clean(raw)}, nil
		}
		return graph.DefinitionRef{}, errors.New("workflow file reference is not a regular file or directory")
	}
	if strings.HasSuffix(raw, ".workflow.yaml") || strings.HasSuffix(raw, ".workflow.yml") || raw == "workflow.yaml" || raw == "workflow.yml" || filepath.IsAbs(raw) || strings.HasPrefix(raw, "."+string(filepath.Separator)) {
		return graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, Locator: filepath.Clean(raw)}, nil
	}
	separator := strings.LastIndexByte(raw, '@')
	if separator <= 0 || separator == len(raw)-1 || strings.Count(raw, "@") != 1 {
		return graph.DefinitionRef{}, errors.New("registry workflow reference must be namespace/name@version or namespace/name@sha256:digest")
	}
	name, selector := raw[:separator], raw[separator+1:]
	segments := strings.Split(name, "/")
	if len(segments) < 2 || strings.ContainsAny(name, "\\") {
		return graph.DefinitionRef{}, errors.New("registry workflow identity must be an unambiguous namespace/name")
	}
	for _, segment := range segments {
		if segment == "" || segment == "." || segment == ".." {
			return graph.DefinitionRef{}, errors.New("registry workflow identity must be an unambiguous namespace/name")
		}
	}
	ref := graph.DefinitionRef{Kind: appworkflow.DefinitionKindRegistry, ID: name}
	if strings.HasPrefix(selector, "sha256:") {
		if err := values.ValidateDigest(selector); err != nil {
			return graph.DefinitionRef{}, fmt.Errorf("registry workflow digest: %w", err)
		}
		ref.Digest = selector
	} else {
		if strings.TrimSpace(selector) != selector || strings.IndexFunc(selector, unicode.IsControl) >= 0 {
			return graph.DefinitionRef{}, errors.New("registry workflow version is invalid")
		}
		ref.Version = selector
	}
	return ref, nil
}

func readWorkflowInputs(file, inline string) (map[string]any, error) {
	if file != "" && inline != "" {
		return nil, errors.New("--input and --input-json are mutually exclusive")
	}
	if file == "" && inline == "" {
		return map[string]any{}, nil
	}
	var data []byte
	var err error
	if inline != "" {
		if len(inline) > maximumWorkflowInputBytes {
			return nil, errors.New("inline workflow input exceeds 1 MiB")
		}
		data = []byte(inline)
	} else {
		data, err = readBoundedWorkflowFile(file, maximumWorkflowInputBytes, true)
		if err != nil {
			return nil, fmt.Errorf("read workflow input: %w", err)
		}
	}
	value, err := decodeUniqueWorkflowJSON(data)
	if err != nil {
		return nil, fmt.Errorf("decode workflow input: %w", err)
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("workflow input must be a JSON object")
	}
	return object, nil
}

func readTypedWorkflowValue(file, inline string) (values.Value, error) {
	if file != "" && inline != "" {
		return values.Value{}, errors.New("--payload and --payload-json are mutually exclusive")
	}
	if file == "" && inline == "" {
		return values.Value{}, errors.New("--payload or --payload-json is required")
	}
	var data []byte
	var err error
	if inline != "" {
		if len(inline) > maximumWorkflowInputBytes {
			return values.Value{}, errors.New("inline resume payload exceeds 1 MiB")
		}
		data = []byte(inline)
	} else {
		data, err = readBoundedWorkflowFile(file, maximumWorkflowInputBytes, true)
		if err != nil {
			return values.Value{}, fmt.Errorf("read resume payload: %w", err)
		}
	}
	var value values.Value
	if err := decodeUniqueTypedWorkflowJSON(data, &value); err != nil {
		return values.Value{}, fmt.Errorf("decode typed resume payload: %w", err)
	}
	if err := value.Validate(); err != nil {
		return values.Value{}, fmt.Errorf("validate typed resume payload: %w", err)
	}
	return value, nil
}

func readBoundedWorkflowFile(path string, limit int64, stdin bool) ([]byte, error) {
	var reader io.Reader
	var closeFile func() error
	if path == "-" {
		if !stdin {
			return nil, errors.New("stdin is not accepted here")
		}
		reader = os.Stdin
	} else {
		// #nosec G304 -- the CLI intentionally reads the exact user-selected input or token file.
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		reader, closeFile = file, file.Close
	}
	if closeFile != nil {
		defer func() { _ = closeFile() }()
	}
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("input exceeds %d bytes", limit)
	}
	if !utf8.Valid(data) {
		return nil, errors.New("input is not valid UTF-8")
	}
	return data, nil
}

func decodeUniqueWorkflowJSON(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	value, err := decodeUniqueWorkflowJSONValue(decoder, 0)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("JSON contains trailing data")
		}
		return nil, err
	}
	return value, nil
}

func decodeUniqueTypedWorkflowJSON(data []byte, output any) error {
	decoded, err := decodeUniqueWorkflowJSON(data)
	if err != nil {
		return err
	}
	canonical, err := json.Marshal(decoded)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("typed JSON contains trailing data")
		}
		return err
	}
	return nil
}

func parseWorkflowPins(raw []string) ([]hoststate.StartPin, error) {
	result := make([]hoststate.StartPin, 0, len(raw))
	seen := map[string]struct{}{}
	totalBytes := 0
	for _, item := range raw {
		totalBytes += len(item)
		if totalBytes > maximumWorkflowInputBytes {
			return nil, errors.New("pinned value references exceed 1 MiB")
		}
		node, encoded, ok := strings.Cut(item, "=")
		if !ok || node == "" || encoded == "" {
			return nil, fmt.Errorf("invalid --pin %q", item)
		}
		if _, duplicate := seen[node]; duplicate {
			return nil, fmt.Errorf("duplicate pin for node %q", node)
		}
		var ref values.ValueSetRef
		if err := decodeUniqueTypedWorkflowJSON([]byte(encoded), &ref); err != nil {
			return nil, fmt.Errorf("pin %s value reference: %w", node, err)
		}
		pin := hoststate.StartPin{NodeID: node, Outputs: ref}
		if err := pin.Validate(); err != nil {
			return nil, err
		}
		seen[node] = struct{}{}
		result = append(result, pin)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].NodeID < result[j].NodeID })
	return result, nil
}

func duplicateStrings(values []string) bool {
	for index := 1; index < len(values); index++ {
		if values[index-1] == values[index] {
			return true
		}
	}
	return false
}

func duplicateStringers[T ~string](values []T) bool {
	for index := 1; index < len(values); index++ {
		if values[index-1] == values[index] {
			return true
		}
	}
	return false
}

func writeWorkflowDiagnostics(writer io.Writer, findings []diagnostic.Diagnostic) error {
	for _, finding := range findings {
		location := ""
		if finding.Source != nil {
			location = fmt.Sprintf(" %s:%d:%d", finding.Source.Locator, finding.Source.StartLine, finding.Source.StartColumn)
		}
		if _, err := fmt.Fprintf(writer, "%s %s%s: %s\n", finding.Severity, finding.Code, location, finding.Message); err != nil {
			return err
		}
	}
	return nil
}

func writeWorkflowExplanation(writer io.Writer, result appworkflow.StartRunResult) error {
	if _, err := fmt.Fprintf(writer, "policy: %s (%s)\neffects: %s\ncapabilities: %s\npreview-record: %s\nwork-admitted: false\ndry-run-supported: %t\n", result.Decision.Outcome, result.Decision.Reason, strings.Join(effectStrings(result.Facts.Effects), ","), strings.Join(result.Facts.RequiredCapabilities, ","), result.Phase, result.Facts.DryRunAvailable); err != nil {
		return err
	}
	keys := make([]string, 0, len(result.Facts.BlastRadius))
	for key := range result.Facts.BlastRadius {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, err := fmt.Fprintf(writer, "blast-radius %s=%d\n", key, result.Facts.BlastRadius[key]); err != nil {
			return err
		}
	}
	return nil
}

func writeWorkflowDryRunUnsupported(writer io.Writer, result appworkflow.StartRunResult) error {
	if _, err := fmt.Fprintf(writer, "policy: %s (%s)\neffects: %s\ncapabilities: %s\nwork-admitted: false\ndry-run-supported: false\n", result.Decision.Outcome, result.Decision.Reason, strings.Join(effectStrings(result.Facts.Effects), ","), strings.Join(result.Facts.RequiredCapabilities, ",")); err != nil {
		return err
	}
	return writeWorkflowDiagnostics(writer, result.Diagnostics)
}

func writeWorkflowRejectedStart(writer io.Writer, result appworkflow.StartRunResult) error {
	_, err := fmt.Fprintf(writer, "run %s phase=%s status=%s\nwork-admitted: false\n", result.Run.ID, result.Phase, result.Run.Status)
	return err
}

func validateWorkflowPreview(result appworkflow.StartRunResult) error {
	if !result.DryRun || result.Phase != hoststate.StartDryRunComplete || result.Bound == nil || result.Run != nil || !result.Facts.DryRunAvailable {
		return errors.New("workflow daemon returned an invalid non-effecting preview")
	}
	return nil
}

func effectStrings(effects graph.EffectSet) []string {
	result := make([]string, len(effects))
	for index, effect := range effects {
		result[index] = string(effect)
	}
	return result
}

func writeWorkflowInspection(writer io.Writer, result rundiagnostics.Result) error {
	if _, err := fmt.Fprintf(writer, "run %s status=%s plan=%s\n", result.Run.ID, result.Run.Status, result.Plan.Digest); err != nil {
		return err
	}
	for _, node := range result.Nodes {
		if _, err := fmt.Fprintf(writer, "%s %s origin=%s\n", node.ID.NodeID, node.Status, node.Origin); err != nil {
			return err
		}
	}
	if len(result.Omissions) != 0 {
		if _, err := fmt.Fprintf(writer, "omissions: %s\n", strings.Join(result.Omissions, ", ")); err != nil {
			return err
		}
	}
	return nil
}

func parseExactWorkflowRegistryRef(input string) (graph.DefinitionRef, error) {
	if strings.Count(input, "#") != 1 {
		return graph.DefinitionRef{}, errors.New("exact registry reference must be namespace/name@version#sha256:digest")
	}
	separator := strings.LastIndexByte(input, '#')
	if separator <= 0 || separator == len(input)-1 {
		return graph.DefinitionRef{}, errors.New("exact registry reference must be namespace/name@version#sha256:digest")
	}
	digest := input[separator+1:]
	if err := values.ValidateDigest(digest); err != nil {
		return graph.DefinitionRef{}, fmt.Errorf("exact registry reference digest: %w", err)
	}
	ref, err := parseWorkflowDefinitionRef(input[:separator])
	if err != nil {
		return graph.DefinitionRef{}, err
	}
	if ref.Kind != appworkflow.DefinitionKindRegistry || ref.ID == "" || ref.Version == "" || strings.LastIndexByte(ref.ID, '/') <= 0 {
		return graph.DefinitionRef{}, errors.New("exact registry reference must be namespace/name@version#sha256:digest")
	}
	ref.Digest = digest
	return ref, nil
}

func decodeUniqueWorkflowJSONValue(decoder *json.Decoder, depth int) (any, error) {
	if depth > 128 {
		return nil, errors.New("JSON nesting exceeds 128 levels")
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch token := token.(type) {
	case json.Delim:
		switch token {
		case '{':
			object := map[string]any{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, errors.New("JSON object key is not a string")
				}
				if _, exists := object[key]; exists {
					return nil, fmt.Errorf("duplicate JSON key %q", key)
				}
				value, err := decodeUniqueWorkflowJSONValue(decoder, depth+1)
				if err != nil {
					return nil, err
				}
				object[key] = value
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
				return nil, errors.New("unterminated JSON object")
			}
			return object, nil
		case '[':
			array := []any{}
			for decoder.More() {
				value, err := decodeUniqueWorkflowJSONValue(decoder, depth+1)
				if err != nil {
					return nil, err
				}
				array = append(array, value)
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim(']') {
				return nil, errors.New("unterminated JSON array")
			}
			return array, nil
		}
	}
	return token, nil
}
