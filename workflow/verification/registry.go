package verification

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
)

type registeredVerifier struct {
	verifier Verifier
	spec     VerifierSpec
}

// MemoryRegistry is a concurrency-safe exact-kind verifier registry. Verifier
// kinds are intentionally unversioned in graph checks; their immutable version
// remains visible in the registry result and durable report.
type MemoryRegistry struct {
	mu            sync.RWMutex
	registrations map[string]registeredVerifier
	frozen        bool
}

func NewRegistry() *MemoryRegistry {
	return &MemoryRegistry{registrations: make(map[string]registeredVerifier)}
}

func (r *MemoryRegistry) Register(verifier Verifier) error {
	if nilVerifier(verifier) {
		return fmt.Errorf("%w: verifier implementation is nil", ErrInvalidSpec)
	}
	spec := verifier.Spec()
	if err := spec.Validate(); err != nil {
		return err
	}
	cloned, err := cloneJSON(spec)
	if err != nil {
		return fmt.Errorf("%w: clone spec: %w", ErrInvalidSpec, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return fmt.Errorf("%w: verifier registry is frozen", ErrInvalidSpec)
	}
	if r.registrations == nil {
		r.registrations = make(map[string]registeredVerifier)
	}
	if _, exists := r.registrations[cloned.Kind]; exists {
		return fmt.Errorf("%w: verifier %q is already registered", ErrInvalidSpec, cloned.Kind)
	}
	r.registrations[cloned.Kind] = registeredVerifier{verifier: verifier, spec: cloned}
	return nil
}

// SnapshotRegistry freezes an exact verifier implementation/spec catalog for
// a compiler or runtime consumer. Subsequent registrations in the source
// registry cannot change validation or execution semantics mid-lifecycle.
func SnapshotRegistry(source Registry) (*MemoryRegistry, error) {
	if nilRegistry(source) {
		return nil, fmt.Errorf("%w: verifier registry is nil", ErrInvalidSpec)
	}
	listed := source.List()
	snapshot := &MemoryRegistry{registrations: make(map[string]registeredVerifier, len(listed)), frozen: true}
	for _, spec := range listed {
		if err := spec.Validate(); err != nil {
			return nil, err
		}
		if _, duplicate := snapshot.registrations[spec.Kind]; duplicate {
			return nil, fmt.Errorf("%w: duplicate verifier spec %q", ErrInvalidSpec, spec.Kind)
		}
		implementation, ok := source.Lookup(spec.Kind)
		if !ok || nilVerifier(implementation) {
			return nil, fmt.Errorf("%w: verifier %q is absent from lookup", ErrInvalidSpec, spec.Kind)
		}
		cloned, err := cloneJSON(spec)
		if err != nil {
			return nil, fmt.Errorf("%w: clone verifier %q: %w", ErrInvalidSpec, spec.Kind, err)
		}
		implementationSpec, err := cloneJSON(implementation.Spec())
		if err != nil || !reflect.DeepEqual(implementationSpec, cloned) {
			return nil, fmt.Errorf("%w: verifier %q lookup/list specs differ", ErrInvalidSpec, spec.Kind)
		}
		snapshot.registrations[spec.Kind] = registeredVerifier{verifier: implementation, spec: cloned}
	}
	return snapshot, nil
}

func (r *MemoryRegistry) Lookup(kind string) (Verifier, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	registered, ok := r.registrations[kind]
	r.mu.RUnlock()
	return registered.verifier, ok
}

func (r *MemoryRegistry) List() []VerifierSpec {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	result := make([]VerifierSpec, 0, len(r.registrations))
	for _, item := range r.registrations {
		cloned, _ := cloneJSON(item.spec)
		result = append(result, cloned)
	}
	r.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool { return result[i].Kind < result[j].Kind })
	return result
}

func Resolve(registry Registry, kind string) (Verifier, VerifierSpec, error) {
	if nilRegistry(registry) {
		return nil, VerifierSpec{}, fmt.Errorf("%w: %q", ErrUnknownVerifier, kind)
	}
	verifier, ok := registry.Lookup(kind)
	if !ok || nilVerifier(verifier) {
		return nil, VerifierSpec{}, fmt.Errorf("%w: %q", ErrUnknownVerifier, kind)
	}
	var matches []VerifierSpec
	for _, spec := range registry.List() {
		if spec.Kind == kind {
			matches = append(matches, spec)
		}
	}
	if len(matches) != 1 {
		return nil, VerifierSpec{}, fmt.Errorf("verifier registry lookup/list mismatch for %q", kind)
	}
	if err := matches[0].Validate(); err != nil {
		return nil, VerifierSpec{}, err
	}
	return verifier, matches[0], nil
}

func nilRegistry(registry Registry) bool { return nilInterface(registry) }
func nilVerifier(verifier Verifier) bool { return nilInterface(verifier) }
func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	ref := reflect.ValueOf(value)
	switch ref.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return ref.IsNil()
	default:
		return false
	}
}
