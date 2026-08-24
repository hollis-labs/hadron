package values

import (
	"errors"
	"fmt"
	"sort"
)

// ErrRetentionViolation marks an attempt to durably store a no-retain value.
var ErrRetentionViolation = errors.New("workflow value retention violation")

// ValidatePersistable applies data-plane invariants specific to durable state.
// RetentionNone values are valid ephemeral envelopes but cannot be written.
// SecretRef and ArtifactRef envelopes carry references only and may be stored;
// secret-classified inline material is rejected by Value.Validate itself.
func ValidatePersistable(value Value) error {
	if err := value.Validate(); err != nil {
		return err
	}
	if value.Retention == RetentionNone {
		return fmt.Errorf("%w: retention none forbids durable storage", ErrRetentionViolation)
	}
	return nil
}

// ValidatePersistableSet checks named values in deterministic order.
func ValidatePersistableSet(set ValueSet) error {
	if err := set.Validate(); err != nil {
		return err
	}
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := ValidatePersistable(set[name]); err != nil {
			return fmt.Errorf("value-set[%q]: %w", name, err)
		}
	}
	return nil
}
