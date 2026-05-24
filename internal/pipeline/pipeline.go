package pipeline

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/hollis-labs/hadron/internal/specparse"
)

var outputKeyPattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_\-]*$`)

type Spec struct {
	Meta       Meta           `yaml:"meta" json:"meta"`
	StopOnFail *bool          `yaml:"stop_on_fail,omitempty" json:"stop_on_fail,omitempty"`
	Defaults   Defaults       `yaml:"defaults,omitempty" json:"defaults,omitempty"`
	Stages     []Stage        `yaml:"stages" json:"stages"`
	Inputs     map[string]any `yaml:"inputs,omitempty" json:"inputs,omitempty"`
}

type Defaults struct {
	StageWaitTimeoutSeconds *int `yaml:"stage_wait_timeout_seconds,omitempty" json:"stage_wait_timeout_seconds,omitempty"`
}

type Meta struct {
	Name string `yaml:"name" json:"name"`
}

type Position struct {
	X float64 `yaml:"x" json:"x"`
	Y float64 `yaml:"y" json:"y"`
}

type Stage struct {
	Name               string            `yaml:"name" json:"name"`
	BlueprintPath      string            `yaml:"blueprint_path" json:"blueprint_path"`
	Inputs             map[string]any    `yaml:"inputs,omitempty" json:"inputs,omitempty"`
	If                 string            `yaml:"if,omitempty" json:"if,omitempty"`
	DependsOn          []string          `yaml:"depends_on,omitempty" json:"depends_on,omitempty"`
	Position           *Position         `yaml:"position,omitempty" json:"position,omitempty"`
	Outputs            map[string]string `yaml:"outputs,omitempty" json:"outputs,omitempty"`
	WaitTimeoutSeconds *int              `yaml:"wait_timeout_seconds,omitempty" json:"wait_timeout_seconds,omitempty"`
	Async              bool              `yaml:"async,omitempty" json:"async,omitempty"`
}

func ParseFile(path string) (*Spec, error) {
	b, err := os.ReadFile(path) // #nosec G304 -- ParseFile intentionally reads the caller-selected pipeline path.
	if err != nil {
		return nil, fmt.Errorf("read pipeline: %w", err)
	}
	var s Spec
	if err := specparse.Unmarshal(path, b, &s); err != nil {
		return nil, err
	}
	if err := Validate(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

func ParseBytes(b []byte) (*Spec, error) {
	var s Spec
	if err := specparse.Unmarshal("", b, &s); err != nil {
		return nil, err
	}
	if err := Validate(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

func Validate(s *Spec) error {
	if s == nil {
		return errors.New("pipeline: nil")
	}
	if len(s.Stages) == 0 {
		return errors.New("pipeline.stages: must contain at least one stage")
	}
	if s.Defaults.StageWaitTimeoutSeconds != nil && *s.Defaults.StageWaitTimeoutSeconds <= 0 {
		return errors.New("pipeline.defaults.stage_wait_timeout_seconds: must be greater than 0")
	}
	for i, st := range s.Stages {
		path := fmt.Sprintf("pipeline.stages[%d]", i)
		if strings.TrimSpace(st.Name) == "" {
			return fmt.Errorf("%s.name: required", path)
		}
		if strings.TrimSpace(st.BlueprintPath) == "" {
			return fmt.Errorf("%s.blueprint_path: required", path)
		}
		if st.WaitTimeoutSeconds != nil && *st.WaitTimeoutSeconds <= 0 {
			return fmt.Errorf("%s.wait_timeout_seconds: must be greater than 0", path)
		}
		for key := range st.Outputs {
			if !outputKeyPattern.MatchString(key) {
				return fmt.Errorf("%s.outputs: invalid key %q", path, key)
			}
		}
	}

	// DAG validation: build name→index map, check references, detect cycles.
	nameIndex := make(map[string]int, len(s.Stages))
	for i, st := range s.Stages {
		nameIndex[st.Name] = i
	}

	hasDeps := false
	for _, st := range s.Stages {
		if len(st.DependsOn) > 0 {
			hasDeps = true
			break
		}
	}
	if !hasDeps {
		return nil
	}

	// Validate references: unknown stages and self-references.
	for _, st := range s.Stages {
		for _, dep := range st.DependsOn {
			if dep == st.Name {
				return fmt.Errorf("stage %q depends on itself", st.Name)
			}
			if _, ok := nameIndex[dep]; !ok {
				return fmt.Errorf("stage %q depends on unknown stage %q", st.Name, dep)
			}
		}
	}

	// Cycle detection using 3-color DFS (0=white, 1=gray, 2=black).
	color := make([]int, len(s.Stages))
	var dfs func(idx int) error
	dfs = func(idx int) error {
		color[idx] = 1 // gray
		for _, dep := range s.Stages[idx].DependsOn {
			di := nameIndex[dep]
			if color[di] == 1 {
				return fmt.Errorf("cycle detected involving stage %q", dep)
			}
			if color[di] == 0 {
				if err := dfs(di); err != nil {
					return err
				}
			}
		}
		color[idx] = 2 // black
		return nil
	}

	for i := range s.Stages {
		if color[i] == 0 {
			if err := dfs(i); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Spec) ShouldStopOnFail() bool {
	if s.StopOnFail == nil {
		return true
	}
	return *s.StopOnFail
}

func (s *Spec) StageWaitTimeout(st Stage) int {
	if st.WaitTimeoutSeconds != nil {
		return *st.WaitTimeoutSeconds
	}
	if s != nil && s.Defaults.StageWaitTimeoutSeconds != nil {
		return *s.Defaults.StageWaitTimeoutSeconds
	}
	return 60
}
