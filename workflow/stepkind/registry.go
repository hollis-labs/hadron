package stepkind

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
)

var (
	// ErrUnknownStepKind identifies an unavailable exact registration.
	ErrUnknownStepKind = errors.New("unknown step kind")
	// ErrAmbiguousStepKind identifies an unpinned name with multiple versions.
	ErrAmbiguousStepKind = errors.New("ambiguous step kind version")
)

// ResolutionError reports an unknown or ambiguous kind/version request.
type ResolutionError struct {
	Name      string
	Version   string
	Available []string
	Cause     error
}

// Error implements error.
func (e *ResolutionError) Error() string {
	if errors.Is(e.Cause, ErrAmbiguousStepKind) {
		return fmt.Sprintf("%s: step kind %q requires an exact version; available: %v", e.Cause, e.Name, e.Available)
	}
	return fmt.Sprintf("%s: step kind %q version %q", e.Cause, e.Name, e.Version)
}

// Unwrap supports errors.Is against the resolution cause.
func (e *ResolutionError) Unwrap() error { return e.Cause }

// DuplicateRegistrationError identifies an already registered name/version.
type DuplicateRegistrationError struct {
	Name    string
	Version string
}

// Error implements error.
func (e *DuplicateRegistrationError) Error() string {
	return fmt.Sprintf("step kind %q version %q is already registered", e.Name, e.Version)
}

type registryKey struct {
	name    string
	version string
}

type registration struct {
	kind StepKind
	spec StepKindSpec
}

// MemoryRegistry is a concurrency-safe in-memory Registry implementation.
type MemoryRegistry struct {
	mu            sync.RWMutex
	registrations map[registryKey]registration
}

// NewRegistry returns an empty registry.
func NewRegistry() *MemoryRegistry {
	return &MemoryRegistry{registrations: make(map[registryKey]registration)}
}

// Register validates and registers kind under its immutable name and version.
func (r *MemoryRegistry) Register(kind StepKind) error {
	if isNilStepKind(kind) {
		return &SpecError{Diagnostics: []diagnostic.Diagnostic{{
			Severity: diagnostic.SeverityError,
			Code:     CodeInvalidSpec,
			Message:  "step kind implementation is nil",
		}}}
	}

	spec := kind.Spec()
	if err := joinSpecErrors(ValidateSpec(spec), validateImplementation(kind, spec)); err != nil {
		return err
	}
	spec = cloneSpec(spec)
	key := registryKey{name: spec.Name, version: spec.Version}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.registrations == nil {
		r.registrations = make(map[registryKey]registration)
	}
	if _, exists := r.registrations[key]; exists {
		return &DuplicateRegistrationError{Name: spec.Name, Version: spec.Version}
	}
	r.registrations[key] = registration{kind: kind, spec: spec}
	return nil
}

// Lookup returns the exact name/version registration. It does not select a
// latest version or apply version ranges.
func (r *MemoryRegistry) Lookup(name, version string) (StepKind, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	registered, ok := r.registrations[registryKey{name: name, version: version}]
	return registered.kind, ok
}

// List returns defensive spec copies ordered by name and then version.
func (r *MemoryRegistry) List() []StepKindSpec {
	r.mu.RLock()
	specs := make([]StepKindSpec, 0, len(r.registrations))
	for _, registered := range r.registrations {
		specs = append(specs, cloneSpec(registered.spec))
	}
	r.mu.RUnlock()

	sort.Slice(specs, func(i, j int) bool {
		if specs[i].Name == specs[j].Name {
			return specs[i].Version < specs[j].Version
		}
		return specs[i].Name < specs[j].Name
	})
	return specs
}

// Resolve returns a validated registration and defensive spec for an exact
// name/version. When version is empty, exactly one registered version may be
// selected; Resolve never chooses a latest version from multiple candidates.
func Resolve(registry Registry, name, version string) (StepKind, StepKindSpec, error) {
	if isNilRegistry(registry) {
		return nil, StepKindSpec{}, &ResolutionError{Name: name, Version: version, Cause: ErrUnknownStepKind}
	}
	if version != "" {
		kind, ok := registry.Lookup(name, version)
		if !ok || isNilStepKind(kind) {
			return nil, StepKindSpec{}, &ResolutionError{Name: name, Version: version, Cause: ErrUnknownStepKind}
		}
		var matches []StepKindSpec
		for _, candidate := range registry.List() {
			if candidate.Name == name && candidate.Version == version {
				matches = append(matches, candidate)
			}
		}
		if len(matches) != 1 {
			return nil, StepKindSpec{}, fmt.Errorf("step-kind registry lookup/list mismatch for %s@%s", name, version)
		}
		spec := matches[0]
		if err := joinSpecErrors(ValidateSpec(spec), validateImplementation(kind, spec)); err != nil {
			return nil, StepKindSpec{}, err
		}
		return kind, cloneSpec(spec), nil
	}

	var matches []StepKindSpec
	for _, spec := range registry.List() {
		if spec.Name == name {
			matches = append(matches, spec)
		}
	}
	if len(matches) == 0 {
		return nil, StepKindSpec{}, &ResolutionError{Name: name, Cause: ErrUnknownStepKind}
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].Version < matches[j].Version })
	if len(matches) != 1 {
		versions := make([]string, len(matches))
		for i := range matches {
			versions[i] = matches[i].Version
		}
		return nil, StepKindSpec{}, &ResolutionError{
			Name: name, Available: versions, Cause: ErrAmbiguousStepKind,
		}
	}
	return Resolve(registry, name, matches[0].Version)
}

func isNilRegistry(registry Registry) bool {
	if registry == nil {
		return true
	}
	value := reflect.ValueOf(registry)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func isNilStepKind(kind StepKind) bool {
	if kind == nil {
		return true
	}
	value := reflect.ValueOf(kind)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
