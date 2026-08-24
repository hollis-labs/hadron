package stepkind

import (
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
)

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
