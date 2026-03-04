package blueprint

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"

	"github.com/hollis-labs/hadron/internal/specparse"
	"gopkg.in/yaml.v3"
)

// ─── Structs ──────────────────────────────────────────────────────────────────

type Blueprint struct {
	Version   string            `yaml:"version" json:"version"`
	Blueprint BlueprintInfo     `yaml:"blueprint" json:"blueprint"`
	Project   Project           `yaml:"project" json:"project"`
	Env       map[string]string `yaml:"env" json:"env"`
	Inputs    []Input           `yaml:"inputs" json:"inputs"`
	Packages  Packages          `yaml:"packages" json:"packages"`
	Git       Git               `yaml:"git" json:"git"`
	Stubs     Stubs             `yaml:"stubs" json:"stubs"`
	Tools     Tools             `yaml:"tools" json:"tools"`
	Imports   []Import          `yaml:"imports" json:"imports"`
	Hooks     Hooks             `yaml:"hooks" json:"hooks"`
	Steps     []Section         `yaml:"steps" json:"steps"`
}

type BlueprintInfo struct {
	Name        string   `yaml:"name" json:"name"`
	Slug        string   `yaml:"slug" json:"slug"`
	Title       string   `yaml:"title" json:"title"`
	Description string   `yaml:"description" json:"description"`
	Author      string   `yaml:"author" json:"author"`
	License     string   `yaml:"license" json:"license"`
	Tags        []string `yaml:"tags" json:"tags"`
	Homepage    string   `yaml:"homepage" json:"homepage"`
}

type Project struct {
	Type       string         `yaml:"type" json:"type"`
	Name       string         `yaml:"name" json:"name"`
	Dir        string         `yaml:"dir" json:"dir"`
	Path       string         `yaml:"path" json:"path"`
	PHPVersion string         `yaml:"php_version" json:"php_version"`
	Node       bool           `yaml:"node" json:"node"`
	Vars       map[string]any `yaml:"vars" json:"vars"`
}

type Input struct {
	Name        string   `yaml:"name" json:"name"`
	Label       string   `yaml:"label" json:"label"`
	Description string   `yaml:"description" json:"description"`
	Type        string   `yaml:"type" json:"type"`
	Required    bool     `yaml:"required" json:"required"`
	Default     any      `yaml:"default" json:"default"`
	Enum        []any    `yaml:"enum" json:"enum"`
	Prompt      string   `yaml:"prompt" json:"prompt"`
	ShortFlag   string   `yaml:"short_flag" json:"short_flag"`
	Pattern     string   `yaml:"pattern" json:"pattern"`
	MinLength   *int     `yaml:"min_length" json:"min_length"`
	MaxLength   *int     `yaml:"max_length" json:"max_length"`
	Min         *float64 `yaml:"min" json:"min"`
	Max         *float64 `yaml:"max" json:"max"`
	ItemsType   string   `yaml:"items_type" json:"items_type"`
}

// ─── Packages ─────────────────────────────────────────────────────────────────

type ComposerPackages struct {
	Require    []string `yaml:"require" json:"require"`
	RequireDev []string `yaml:"require_dev" json:"require_dev"`
}

func (c *ComposerPackages) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.SequenceNode {
		var arr []string
		if err := value.Decode(&arr); err != nil {
			return err
		}
		c.Require = arr
		return nil
	}
	type rawComposer ComposerPackages
	return value.Decode((*rawComposer)(c))
}

func (c *ComposerPackages) UnmarshalJSON(data []byte) error {
	var arr []string
	if err := json.Unmarshal(data, &arr); err == nil {
		c.Require = arr
		return nil
	}
	type rawComposer ComposerPackages
	return json.Unmarshal(data, (*rawComposer)(c))
}

type NPMPackages struct {
	Deps []string `yaml:"deps" json:"deps"`
	Dev  []string `yaml:"dev" json:"dev"`
}

func (n *NPMPackages) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.SequenceNode {
		var arr []string
		if err := value.Decode(&arr); err != nil {
			return err
		}
		n.Deps = arr
		return nil
	}
	type rawNPM NPMPackages
	return value.Decode((*rawNPM)(n))
}

func (n *NPMPackages) UnmarshalJSON(data []byte) error {
	var arr []string
	if err := json.Unmarshal(data, &arr); err == nil {
		n.Deps = arr
		return nil
	}
	type rawNPM NPMPackages
	return json.Unmarshal(data, (*rawNPM)(n))
}

type PipPackages struct {
	Deps []string `yaml:"deps" json:"deps"`
	Dev  []string `yaml:"dev" json:"dev"`
}

type GoPackages struct {
	Tools []string `yaml:"tools" json:"tools"`
}

type BrewPackages struct {
	Formulae []string `yaml:"formulae" json:"formulae"`
	Casks    []string `yaml:"casks" json:"casks"`
	Taps     []string `yaml:"taps" json:"taps"`
}

func (b *BrewPackages) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.SequenceNode {
		var arr []string
		if err := value.Decode(&arr); err != nil {
			return err
		}
		b.Formulae = arr
		return nil
	}
	type rawBrew BrewPackages
	return value.Decode((*rawBrew)(b))
}

func (b *BrewPackages) UnmarshalJSON(data []byte) error {
	var arr []string
	if err := json.Unmarshal(data, &arr); err == nil {
		b.Formulae = arr
		return nil
	}
	type rawBrew BrewPackages
	return json.Unmarshal(data, (*rawBrew)(b))
}

// Packages holds all package manager declarations.
// Supports compat shorthands: composerDev → composer.require_dev, npmDev → npm.dev.
type Packages struct {
	Composer    *ComposerPackages `yaml:"composer" json:"composer"`
	ComposerDev []string          `yaml:"composerDev" json:"composerDev"` // compat
	NPM         *NPMPackages      `yaml:"npm" json:"npm"`
	NPMDev      []string          `yaml:"npmDev" json:"npmDev"` // compat
	Pip         *PipPackages      `yaml:"pip" json:"pip"`
	Brew        *BrewPackages     `yaml:"brew" json:"brew"`
	Go          *GoPackages       `yaml:"go" json:"go"`
}

// normalizePackages merges compat fields into canonical locations.
func normalizePackages(p *Packages) {
	if len(p.ComposerDev) > 0 {
		if p.Composer == nil {
			p.Composer = &ComposerPackages{}
		}
		if len(p.Composer.RequireDev) == 0 {
			p.Composer.RequireDev = p.ComposerDev
		}
		p.ComposerDev = nil
	}
	if len(p.NPMDev) > 0 {
		if p.NPM == nil {
			p.NPM = &NPMPackages{}
		}
		if len(p.NPM.Dev) == 0 {
			p.NPM.Dev = p.NPMDev
		}
		p.NPMDev = nil
	}
}

// ─── Git / Stubs / Tools ──────────────────────────────────────────────────────

type Git struct {
	Init             bool   `yaml:"init" json:"init"`
	CreateGithubRepo bool   `yaml:"create_github_repo" json:"create_github_repo"`
	Visibility       string `yaml:"visibility" json:"visibility"`
	Remote           string `yaml:"remote" json:"remote"`
	Branch           string `yaml:"branch" json:"branch"`
}

type Stubs struct {
	Enabled     bool     `yaml:"enabled" json:"enabled"`
	SearchPaths []string `yaml:"search_paths" json:"search_paths"`
	StrictMatch bool     `yaml:"strict_match" json:"strict_match"`
}

type ToolInstall struct {
	Homebrew []string `yaml:"homebrew" json:"homebrew"`
	Apt      []string `yaml:"apt" json:"apt"`
	Custom   []string `yaml:"custom" json:"custom"`
}

type Tools struct {
	Install ToolInstall `yaml:"install" json:"install"`
}

// ─── Import ───────────────────────────────────────────────────────────────────

type Import struct {
	Path  string         `yaml:"path" json:"path"`
	Alias string         `yaml:"alias" json:"alias"`
	With  map[string]any `yaml:"with" json:"with"`
}

// ─── Hooks ────────────────────────────────────────────────────────────────────

type Hooks struct {
	BeforeRun []Hook `yaml:"before_run" json:"before_run"`
	AfterRun  []Hook `yaml:"after_run" json:"after_run"`
	OnError   []Hook `yaml:"on_error" json:"on_error"`
}

type Hook struct {
	Name string `yaml:"name" json:"name"`
	Cmd  string `yaml:"cmd" json:"cmd"`
	If   string `yaml:"if" json:"if"`
}

// ─── Section / Task ───────────────────────────────────────────────────────────

type Section struct {
	Section string `yaml:"section" json:"section"`
	Tasks   []Task `yaml:"tasks" json:"tasks"`
}

type ActionHook struct {
	Type  string `yaml:"type" json:"type"`
	Value string `yaml:"value" json:"value"`
}

// Task is the v0.4 task model. Custom unmarshalers normalize v0.2 compat aliases.
type Task struct {
	Name            string            `yaml:"name" json:"name"`
	Cmd             string            `yaml:"cmd" json:"cmd"`
	Run             string            `yaml:"run" json:"run"`
	Call            string            `yaml:"call" json:"call"`
	If              string            `yaml:"if" json:"if"`
	With            map[string]any    `yaml:"with" json:"with"`
	Dir             string            `yaml:"dir" json:"dir"`
	Env             map[string]string `yaml:"env" json:"env"`
	Retry           int               `yaml:"retry" json:"retry"`
	RetryDelaySecs  int               `yaml:"retry_delay_seconds" json:"retry_delay_seconds"`
	TimeoutSeconds  int               `yaml:"timeout_seconds" json:"timeout_seconds"`
	ContinueOnError bool              `yaml:"continue_on_error" json:"continue_on_error"`
	Enabled         *bool             `yaml:"enabled" json:"enabled"`
	OnSuccess       []ActionHook      `yaml:"on_success" json:"on_success"`
	OnFail          []ActionHook      `yaml:"on_fail" json:"on_fail"`
}

// rawTaskYAML captures both canonical and compat field names for YAML unmarshal.
type rawTaskYAML struct {
	Name                 string            `yaml:"name"`
	Cmd                  string            `yaml:"cmd"`
	Run                  string            `yaml:"run"`
	Call                 string            `yaml:"call"`
	If                   string            `yaml:"if"`
	Condition            string            `yaml:"condition"`     // compat → if
	With                 map[string]any    `yaml:"with"`
	Dir                  string            `yaml:"dir"`
	Env                  map[string]string `yaml:"env"`
	Retry                int               `yaml:"retry"`
	RetryDelaySecs       int               `yaml:"retry_delay_seconds"`
	RetryDelay           int               `yaml:"retry_delay"`   // compat
	RetryDelayCamel      int               `yaml:"retryDelay"`    // compat camelCase
	TimeoutSeconds       int               `yaml:"timeout_seconds"`
	Timeout              int               `yaml:"timeout"`       // compat
	ContinueOnError      bool              `yaml:"continue_on_error"`
	ContinueOnErrorCamel bool              `yaml:"continueOnError"` // compat camelCase
	Enabled              *bool             `yaml:"enabled"`
	OnSuccess            []ActionHook      `yaml:"on_success"`
	OnSuccessCamel       []ActionHook      `yaml:"onSuccess"` // compat camelCase
	OnFail               []ActionHook      `yaml:"on_fail"`
	OnFailCamel          []ActionHook      `yaml:"onFail"` // compat camelCase
}

func (t *Task) UnmarshalYAML(value *yaml.Node) error {
	var raw rawTaskYAML
	if err := value.Decode(&raw); err != nil {
		return err
	}
	t.Name = raw.Name
	t.Cmd = raw.Cmd
	t.Run = raw.Run
	t.Call = raw.Call
	t.If = raw.If
	if t.If == "" {
		t.If = raw.Condition
	}
	t.With = raw.With
	t.Dir = raw.Dir
	t.Env = raw.Env
	t.Retry = raw.Retry
	t.RetryDelaySecs = raw.RetryDelaySecs
	if t.RetryDelaySecs == 0 && raw.RetryDelay != 0 {
		t.RetryDelaySecs = raw.RetryDelay
	}
	if t.RetryDelaySecs == 0 && raw.RetryDelayCamel != 0 {
		t.RetryDelaySecs = raw.RetryDelayCamel
	}
	t.TimeoutSeconds = raw.TimeoutSeconds
	if t.TimeoutSeconds == 0 && raw.Timeout != 0 {
		t.TimeoutSeconds = raw.Timeout
	}
	t.ContinueOnError = raw.ContinueOnError || raw.ContinueOnErrorCamel
	t.Enabled = raw.Enabled
	t.OnSuccess = raw.OnSuccess
	if len(t.OnSuccess) == 0 {
		t.OnSuccess = raw.OnSuccessCamel
	}
	t.OnFail = raw.OnFail
	if len(t.OnFail) == 0 {
		t.OnFail = raw.OnFailCamel
	}
	return nil
}

// rawTaskJSON captures compat field names for JSON unmarshal.
type rawTaskJSON struct {
	Name                 string            `json:"name"`
	Cmd                  string            `json:"cmd"`
	Run                  string            `json:"run"`
	Call                 string            `json:"call"`
	If                   string            `json:"if"`
	Condition            string            `json:"condition"`
	With                 map[string]any    `json:"with"`
	Dir                  string            `json:"dir"`
	Env                  map[string]string `json:"env"`
	Retry                int               `json:"retry"`
	RetryDelaySecs       int               `json:"retry_delay_seconds"`
	RetryDelay           int               `json:"retry_delay"`
	RetryDelayCamel      int               `json:"retryDelay"`
	TimeoutSeconds       int               `json:"timeout_seconds"`
	Timeout              int               `json:"timeout"`
	ContinueOnError      bool              `json:"continue_on_error"`
	ContinueOnErrorCamel bool              `json:"continueOnError"`
	Enabled              *bool             `json:"enabled"`
	OnSuccess            []ActionHook      `json:"on_success"`
	OnSuccessCamel       []ActionHook      `json:"onSuccess"`
	OnFail               []ActionHook      `json:"on_fail"`
	OnFailCamel          []ActionHook      `json:"onFail"`
}

func (t *Task) UnmarshalJSON(data []byte) error {
	var raw rawTaskJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	t.Name = raw.Name
	t.Cmd = raw.Cmd
	t.Run = raw.Run
	t.Call = raw.Call
	t.If = raw.If
	if t.If == "" {
		t.If = raw.Condition
	}
	t.With = raw.With
	t.Dir = raw.Dir
	t.Env = raw.Env
	t.Retry = raw.Retry
	t.RetryDelaySecs = raw.RetryDelaySecs
	if t.RetryDelaySecs == 0 && raw.RetryDelay != 0 {
		t.RetryDelaySecs = raw.RetryDelay
	}
	if t.RetryDelaySecs == 0 && raw.RetryDelayCamel != 0 {
		t.RetryDelaySecs = raw.RetryDelayCamel
	}
	t.TimeoutSeconds = raw.TimeoutSeconds
	if t.TimeoutSeconds == 0 && raw.Timeout != 0 {
		t.TimeoutSeconds = raw.Timeout
	}
	t.ContinueOnError = raw.ContinueOnError || raw.ContinueOnErrorCamel
	t.Enabled = raw.Enabled
	t.OnSuccess = raw.OnSuccess
	if len(t.OnSuccess) == 0 {
		t.OnSuccess = raw.OnSuccessCamel
	}
	t.OnFail = raw.OnFail
	if len(t.OnFail) == 0 {
		t.OnFail = raw.OnFailCamel
	}
	return nil
}

// ─── Parse ────────────────────────────────────────────────────────────────────

func ParseFile(path string) (*Blueprint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	var bp Blueprint
	if err := specparse.Unmarshal(path, data, &bp); err != nil {
		return nil, err
	}
	normalizePackages(&bp.Packages)
	if err := Validate(&bp); err != nil {
		return nil, err
	}
	return &bp, nil
}

func ParseBytes(data []byte) (*Blueprint, error) {
	var bp Blueprint
	if err := specparse.Unmarshal("", data, &bp); err != nil {
		return nil, err
	}
	normalizePackages(&bp.Packages)
	if err := Validate(&bp); err != nil {
		return nil, err
	}
	return &bp, nil
}

// ─── Validate ─────────────────────────────────────────────────────────────────

var allowedInputTypes = map[string]struct{}{
	"string":  {},
	"number":  {},
	"boolean": {},
	"array":   {},
}

var inputNamePattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_\-]*$`)

func Validate(bp *Blueprint) error {
	if bp == nil {
		return errors.New("blueprint: nil")
	}
	if strings.TrimSpace(bp.Version) == "" {
		bp.Version = "0.4"
	}
	if strings.TrimSpace(resolveBlueprintName(bp)) == "" {
		return errors.New("blueprint.name: required")
	}
	if len(bp.Steps) == 0 {
		return errors.New("blueprint.steps: must contain at least one section")
	}
	for i, sec := range bp.Steps {
		if len(sec.Tasks) == 0 {
			return fmt.Errorf("blueprint.steps[%d] (%q): section must have at least one task", i, sec.Section)
		}
	}

	seenInputs := map[string]struct{}{}
	for i, in := range bp.Inputs {
		path := fmt.Sprintf("blueprint.inputs[%d]", i)
		if strings.TrimSpace(in.Name) == "" {
			return fmt.Errorf("%s.name: required", path)
		}
		if !inputNamePattern.MatchString(in.Name) {
			return fmt.Errorf("%s.name: invalid format %q", path, in.Name)
		}
		if _, exists := seenInputs[in.Name]; exists {
			return fmt.Errorf("%s: duplicate input name %q", path, in.Name)
		}
		seenInputs[in.Name] = struct{}{}
		if _, ok := allowedInputTypes[in.Type]; !ok {
			return fmt.Errorf("%s.type: unsupported type %q", path, in.Type)
		}
		if in.Pattern != "" {
			if _, err := regexp.Compile(in.Pattern); err != nil {
				return fmt.Errorf("%s.pattern: invalid regex: %w", path, err)
			}
		}
		if in.Min != nil && in.Max != nil && *in.Min > *in.Max {
			return fmt.Errorf("%s: min must be <= max", path)
		}
		if in.MinLength != nil && in.MaxLength != nil && *in.MinLength > *in.MaxLength {
			return fmt.Errorf("%s: min_length must be <= max_length", path)
		}
	}

	seenAliases := map[string]struct{}{}
	for i, imp := range bp.Imports {
		if strings.TrimSpace(imp.Path) == "" {
			return fmt.Errorf("blueprint.imports[%d].path: required", i)
		}
		alias := strings.TrimSpace(imp.Alias)
		if alias == "" {
			continue
		}
		if _, exists := seenAliases[alias]; exists {
			return fmt.Errorf("blueprint.imports[%d].alias: duplicate alias %q", i, alias)
		}
		seenAliases[alias] = struct{}{}
	}

	for si, sec := range bp.Steps {
		for ti, task := range sec.Tasks {
			path := fmt.Sprintf("blueprint.steps[%d].tasks[%d]", si, ti)
			cmd := strings.TrimSpace(task.Cmd)
			run := strings.TrimSpace(task.Run)
			call := strings.TrimSpace(task.Call)
			// normalize run → cmd
			if cmd == "" && run != "" {
				cmd = run
				bp.Steps[si].Tasks[ti].Cmd = run
			}
			if cmd == "" && call == "" {
				return fmt.Errorf("%s (%q): task must have cmd or call", path, task.Name)
			}
			if task.Retry < 0 {
				return fmt.Errorf("%s.retry: must be >= 0", path)
			}
			if task.RetryDelaySecs < 0 {
				return fmt.Errorf("%s.retry_delay_seconds: must be >= 0", path)
			}
			if task.TimeoutSeconds < 0 {
				return fmt.Errorf("%s.timeout_seconds: must be >= 0", path)
			}
		}
	}

	return nil
}

// ─── NormalizeInputs ──────────────────────────────────────────────────────────

func NormalizeInputs(bp *Blueprint, values map[string]any) (map[string]any, error) {
	if bp == nil {
		return nil, errors.New("blueprint: nil")
	}
	out := make(map[string]any, len(values))
	for k, v := range values {
		out[k] = v
	}

	inputDefs := make(map[string]Input, len(bp.Inputs))
	for _, in := range bp.Inputs {
		inputDefs[in.Name] = in
	}

	for _, in := range bp.Inputs {
		val, exists := out[in.Name]
		if !exists {
			if in.Default != nil {
				out[in.Name] = in.Default
				continue
			}
			if in.Required {
				return nil, fmt.Errorf("blueprint.inputs.%s: required input missing", in.Name)
			}
			continue
		}
		if err := validateInputType(in, val); err != nil {
			return nil, err
		}
	}

	for k := range out {
		if _, ok := inputDefs[k]; !ok {
			return nil, fmt.Errorf("blueprint.inputs.%s: unknown input", k)
		}
	}

	return out, nil
}

func validateInputType(in Input, v any) error {
	if len(in.Enum) > 0 {
		found := false
		for _, candidate := range in.Enum {
			if fmt.Sprintf("%v", candidate) == fmt.Sprintf("%v", v) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("blueprint.inputs.%s: value not in enum", in.Name)
		}
	}

	switch in.Type {
	case "string":
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("blueprint.inputs.%s: must be string", in.Name)
		}
		if in.MinLength != nil && len(s) < *in.MinLength {
			return fmt.Errorf("blueprint.inputs.%s: min_length=%d violated", in.Name, *in.MinLength)
		}
		if in.MaxLength != nil && len(s) > *in.MaxLength {
			return fmt.Errorf("blueprint.inputs.%s: max_length=%d violated", in.Name, *in.MaxLength)
		}
		if in.Pattern != "" {
			p, err := regexp.Compile(in.Pattern)
			if err != nil {
				return fmt.Errorf("blueprint.inputs.%s: invalid pattern", in.Name)
			}
			if !p.MatchString(s) {
				return fmt.Errorf("blueprint.inputs.%s: pattern mismatch", in.Name)
			}
		}
	case "boolean":
		if _, ok := v.(bool); !ok {
			return fmt.Errorf("blueprint.inputs.%s: must be boolean", in.Name)
		}
	case "number":
		n, ok := asFloat64(v)
		if !ok {
			return fmt.Errorf("blueprint.inputs.%s: must be number", in.Name)
		}
		if in.Min != nil && n < *in.Min {
			return fmt.Errorf("blueprint.inputs.%s: below min=%v", in.Name, *in.Min)
		}
		if in.Max != nil && n > *in.Max {
			return fmt.Errorf("blueprint.inputs.%s: above max=%v", in.Name, *in.Max)
		}
	case "array":
		items, ok := asAnySlice(v)
		if !ok {
			return fmt.Errorf("blueprint.inputs.%s: must be array", in.Name)
		}
		if in.ItemsType != "" {
			itemIn := Input{Name: in.Name + "[]", Type: in.ItemsType}
			for _, item := range items {
				if err := validateInputType(itemIn, item); err != nil {
					return err
				}
			}
		}
	default:
		return fmt.Errorf("blueprint.inputs.%s: unsupported type %q", in.Name, in.Type)
	}
	return nil
}

// ─── BuildTemplateContext ─────────────────────────────────────────────────────

func BuildTemplateContext(bp *Blueprint, blueprintPath, workspaceID string, inputs map[string]any) map[string]any {
	if workspaceID == "" {
		workspaceID = "default"
	}
	blueprintDir := filepath.Dir(blueprintPath)

	ctx := map[string]any{
		"inputs":  map[string]any{},
		"env":     map[string]any{},
		"project": map[string]any{},
		"packages": map[string]any{
			"composer":    []string{},
			"composerDev": []string{},
			"npm":         []string{},
			"npmDev":      []string{},
		},
		"git":   map[string]any{},
		"stubs": map[string]any{},
		"workspace": map[string]any{
			"id":            workspaceID,
			"blueprint_dir": blueprintDir,
			"root":          blueprintDir,
		},
		"blueprint": map[string]any{
			"name":    resolveBlueprintName(bp),
			"slug":    bp.Blueprint.Slug,
			"title":   bp.Blueprint.Title,
			"version": bp.Version,
			"path":    blueprintPath,
		},
	}

	in := ctx["inputs"].(map[string]any)
	for k, v := range inputs {
		in[k] = v
	}

	env := ctx["env"].(map[string]any)
	for k, v := range bp.Env {
		env[k] = v
	}

	project := ctx["project"].(map[string]any)
	project["type"] = bp.Project.Type
	project["name"] = bp.Project.Name
	project["dir"] = bp.Project.Dir
	project["path"] = bp.Project.Path
	project["php_version"] = bp.Project.PHPVersion
	project["node"] = bp.Project.Node
	project["vars"] = bp.Project.Vars

	pkgs := ctx["packages"].(map[string]any)
	if bp.Packages.Composer != nil {
		pkgs["composer"] = bp.Packages.Composer.Require
		pkgs["composerDev"] = bp.Packages.Composer.RequireDev
	}
	if bp.Packages.NPM != nil {
		pkgs["npm"] = bp.Packages.NPM.Deps
		pkgs["npmDev"] = bp.Packages.NPM.Dev
	}

	git := ctx["git"].(map[string]any)
	git["init"] = bp.Git.Init
	git["create_github_repo"] = bp.Git.CreateGithubRepo
	git["visibility"] = bp.Git.Visibility
	git["remote"] = bp.Git.Remote
	git["branch"] = bp.Git.Branch

	stubs := ctx["stubs"].(map[string]any)
	stubs["enabled"] = bp.Stubs.Enabled
	stubs["search_paths"] = bp.Stubs.SearchPaths
	stubs["strict_match"] = bp.Stubs.StrictMatch

	return ctx
}

// ─── RenderForExecution ───────────────────────────────────────────────────────

func RenderForExecution(bp *Blueprint, ctx map[string]any) (*Blueprint, error) {
	if bp == nil {
		return nil, errors.New("blueprint: nil")
	}

	// Deep-copy via JSON round-trip.
	raw, err := json.Marshal(bp)
	if err != nil {
		return nil, fmt.Errorf("marshal blueprint for render: %w", err)
	}
	var out Blueprint
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("unmarshal blueprint for render: %w", err)
	}

	// Render project fields.
	if out.Project.Name, err = renderString(out.Project.Name, ctx); err != nil {
		return nil, fmt.Errorf("render project.name: %w", err)
	}
	if out.Project.Dir, err = renderString(out.Project.Dir, ctx); err != nil {
		return nil, fmt.Errorf("render project.dir: %w", err)
	}
	if out.Project.Path, err = renderString(out.Project.Path, ctx); err != nil {
		return nil, fmt.Errorf("render project.path: %w", err)
	}

	// Render env values.
	for k, v := range out.Env {
		rendered, rerr := renderString(v, ctx)
		if rerr != nil {
			return nil, fmt.Errorf("render env.%s: %w", k, rerr)
		}
		out.Env[k] = rendered
	}

	// Render blueprint-level hooks.
	renderedHooks, err := renderHooks(out.Hooks, ctx)
	if err != nil {
		return nil, err
	}
	out.Hooks = renderedHooks

	// Render steps.
	for si := range out.Steps {
		for ti := range out.Steps[si].Tasks {
			t := &out.Steps[si].Tasks[ti]
			if t.Cmd, err = renderString(t.Cmd, ctx); err != nil {
				return nil, fmt.Errorf("render steps[%d].tasks[%d].cmd: %w", si, ti, err)
			}
			if t.Run, err = renderString(t.Run, ctx); err != nil {
				return nil, fmt.Errorf("render steps[%d].tasks[%d].run: %w", si, ti, err)
			}
			if t.Call, err = renderString(t.Call, ctx); err != nil {
				return nil, fmt.Errorf("render steps[%d].tasks[%d].call: %w", si, ti, err)
			}
			if t.If, err = renderString(t.If, ctx); err != nil {
				return nil, fmt.Errorf("render steps[%d].tasks[%d].if: %w", si, ti, err)
			}
			if t.Dir, err = renderString(t.Dir, ctx); err != nil {
				return nil, fmt.Errorf("render steps[%d].tasks[%d].dir: %w", si, ti, err)
			}
			if t.Name, err = renderString(t.Name, ctx); err != nil {
				return nil, fmt.Errorf("render steps[%d].tasks[%d].name: %w", si, ti, err)
			}
			for k, v := range t.Env {
				rendered, rerr := renderString(v, ctx)
				if rerr != nil {
					return nil, fmt.Errorf("render steps[%d].tasks[%d].env.%s: %w", si, ti, k, rerr)
				}
				t.Env[k] = rendered
			}
			with, rerr := renderMapValues(t.With, ctx)
			if rerr != nil {
				return nil, fmt.Errorf("render steps[%d].tasks[%d].with: %w", si, ti, rerr)
			}
			t.With = with
		}
	}

	return &out, nil
}

// ─── LoadWithImports ──────────────────────────────────────────────────────────

// LoadWithImports parses a blueprint and recursively resolves all imports,
// appending imported sections to the end of the blueprint's steps.
func LoadWithImports(path string) (*Blueprint, error) {
	bp, err := ParseFile(path)
	if err != nil {
		return nil, err
	}

	baseDir := filepath.Dir(path)
	for _, imp := range bp.Imports {
		if strings.TrimSpace(imp.Path) == "" {
			continue
		}
		importPath := imp.Path
		if !filepath.IsAbs(importPath) {
			importPath = filepath.Join(baseDir, importPath)
		}
		imported, ierr := LoadWithImports(importPath)
		if ierr != nil {
			return nil, fmt.Errorf("import %q: %w", imp.Path, ierr)
		}
		bp.Steps = append(bp.Steps, imported.Steps...)
	}

	return bp, nil
}

// ─── Template Engine ──────────────────────────────────────────────────────────

const maxTemplateFileSize = 1024 * 1024 // 1MB

var templateFuncMap = template.FuncMap{
	"upper": strings.ToUpper,
	"lower": strings.ToLower,
	"trim":  strings.TrimSpace,
	"replace": func(old, new, s string) string {
		return strings.ReplaceAll(s, old, new)
	},
	"split": strings.Split,
	"join":  strings.Join,

	"basename": filepath.Base,
	"dirname":  filepath.Dir,
	"ext":      filepath.Ext,

	"env": func(key string, defaultVal ...string) string {
		val := os.Getenv(key)
		if val == "" && len(defaultVal) > 0 {
			return defaultVal[0]
		}
		return val
	},

	"readFile": func(path string) (string, error) {
		info, err := os.Stat(path)
		if err != nil {
			return "", fmt.Errorf("cannot stat file %s: %w", path, err)
		}
		if info.Size() > maxTemplateFileSize {
			return "", fmt.Errorf("file too large: %s (max 1MB, got %d bytes)", path, info.Size())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("cannot read file %s: %w", path, err)
		}
		return string(data), nil
	},

	"default": func(def, val any) any {
		if val == nil {
			return def
		}
		if s, ok := val.(string); ok && strings.TrimSpace(s) == "" {
			return def
		}
		return val
	},

	"ternary": func(cond bool, ifTrue, ifFalse any) any {
		if cond {
			return ifTrue
		}
		return ifFalse
	},

	"json": func(v any) string {
		b, _ := json.Marshal(v)
		return string(b)
	},
}

func renderString(in string, ctx map[string]any) (string, error) {
	if !strings.Contains(in, "{{") {
		return in, nil
	}
	tpl, err := template.New("bp").Funcs(templateFuncMap).Parse(in)
	if err != nil {
		return "", err
	}
	var b bytes.Buffer
	if err := tpl.Execute(&b, ctx); err != nil {
		return "", err
	}
	return b.String(), nil
}

func renderMapValues(in map[string]any, ctx map[string]any) (map[string]any, error) {
	if len(in) == 0 {
		return in, nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		s, ok := v.(string)
		if !ok {
			out[k] = v
			continue
		}
		rendered, err := renderString(s, ctx)
		if err != nil {
			return nil, err
		}
		out[k] = rendered
	}
	return out, nil
}

func renderHooks(in Hooks, ctx map[string]any) (Hooks, error) {
	out := in
	var err error
	out.BeforeRun, err = renderHookList(out.BeforeRun, ctx, "before_run")
	if err != nil {
		return Hooks{}, err
	}
	out.AfterRun, err = renderHookList(out.AfterRun, ctx, "after_run")
	if err != nil {
		return Hooks{}, err
	}
	out.OnError, err = renderHookList(out.OnError, ctx, "on_error")
	if err != nil {
		return Hooks{}, err
	}
	return out, nil
}

func renderHookList(in []Hook, ctx map[string]any, bucket string) ([]Hook, error) {
	for i := range in {
		var err error
		if in[i].Name, err = renderString(in[i].Name, ctx); err != nil {
			return nil, fmt.Errorf("render hooks.%s[%d].name: %w", bucket, i, err)
		}
		if in[i].Cmd, err = renderString(in[i].Cmd, ctx); err != nil {
			return nil, fmt.Errorf("render hooks.%s[%d].cmd: %w", bucket, i, err)
		}
		if in[i].If, err = renderString(in[i].If, ctx); err != nil {
			return nil, fmt.Errorf("render hooks.%s[%d].if: %w", bucket, i, err)
		}
	}
	return in, nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func resolveBlueprintName(bp *Blueprint) string {
	name := strings.TrimSpace(bp.Blueprint.Name)
	if name != "" {
		return name
	}
	return strings.TrimSpace(bp.Blueprint.Slug)
}

func asFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

func asAnySlice(v any) ([]any, bool) {
	switch s := v.(type) {
	case []any:
		return s, true
	case []string:
		out := make([]any, len(s))
		for i, item := range s {
			out[i] = item
		}
		return out, true
	case []int:
		out := make([]any, len(s))
		for i, item := range s {
			out[i] = item
		}
		return out, true
	case []float64:
		out := make([]any, len(s))
		for i, item := range s {
			out[i] = item
		}
		return out, true
	case []bool:
		out := make([]any, len(s))
		for i, item := range s {
			out[i] = item
		}
		return out, true
	default:
		return nil, false
	}
}
