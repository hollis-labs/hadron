package pipeline

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/hollis-labs/hadron/internal/specparse"
)

type Spec struct {
	Meta       Meta           `yaml:"meta" json:"meta"`
	StopOnFail *bool          `yaml:"stop_on_fail,omitempty" json:"stop_on_fail,omitempty"`
	Stages     []Stage        `yaml:"stages" json:"stages"`
	Inputs     map[string]any `yaml:"inputs,omitempty" json:"inputs,omitempty"`
}

type Meta struct {
	Name string `yaml:"name" json:"name"`
}

type Stage struct {
	Name          string         `yaml:"name" json:"name"`
	BlueprintPath string         `yaml:"blueprint_path" json:"blueprint_path"`
	Inputs        map[string]any `yaml:"inputs,omitempty" json:"inputs,omitempty"`
	If            string         `yaml:"if,omitempty" json:"if,omitempty"`
}

func ParseFile(path string) (*Spec, error) {
	b, err := os.ReadFile(path)
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
	for i, st := range s.Stages {
		path := fmt.Sprintf("pipeline.stages[%d]", i)
		if strings.TrimSpace(st.Name) == "" {
			return fmt.Errorf("%s.name: required", path)
		}
		if strings.TrimSpace(st.BlueprintPath) == "" {
			return fmt.Errorf("%s.blueprint_path: required", path)
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
