package diagnostic

import (
	"fmt"
	"strconv"
	"strings"
)

// Domain groups diagnostic prefixes by the workflow contract they describe.
type Domain string

const (
	DomainSourceValidation Domain = "source-validation"
	DomainValues           Domain = "values"
	DomainPolicy           Domain = "policy"
	DomainEffects          Domain = "effects"
	DomainWaits            Domain = "waits"
	DomainPersistence      Domain = "persistence"
	DomainHostIntegration  Domain = "host-integration"
)

// Prefix is the reserved middle segment of a diagnostic code.
type Prefix string

const (
	PrefixSource      Prefix = "SOURCE"
	PrefixReference   Prefix = "REF"
	PrefixLegacy      Prefix = "LEGACY"
	PrefixOutput      Prefix = "OUTPUT"
	PrefixValue       Prefix = "VALUE"
	PrefixPolicy      Prefix = "POLICY"
	PrefixEffect      Prefix = "EFFECT"
	PrefixWait        Prefix = "WAIT"
	PrefixPersistence Prefix = "PERSIST"
	PrefixHost        Prefix = "HOST"
)

var prefixDomains = map[Prefix]Domain{
	PrefixSource:      DomainSourceValidation,
	PrefixReference:   DomainSourceValidation,
	PrefixLegacy:      DomainSourceValidation,
	PrefixOutput:      DomainSourceValidation,
	PrefixValue:       DomainValues,
	PrefixPolicy:      DomainPolicy,
	PrefixEffect:      DomainEffects,
	PrefixWait:        DomainWaits,
	PrefixPersistence: DomainPersistence,
	PrefixHost:        DomainHostIntegration,
}

// Valid reports whether p is reserved by this package.
func (p Prefix) Valid() bool {
	_, ok := prefixDomains[p]
	return ok
}

// Domain returns the contract domain reserved for p.
func (p Prefix) Domain() (Domain, bool) {
	domain, ok := prefixDomains[p]
	return domain, ok
}

// Code is a stable, searchable diagnostic identifier.
type Code string

// NewCode constructs a code using a reserved prefix and a three-digit number.
// Numbers start at one; zero is not assigned.
func NewCode(prefix Prefix, number int) (Code, error) {
	if !prefix.Valid() {
		return "", fmt.Errorf("diagnostic prefix %q is not reserved", prefix)
	}
	if number < 1 || number > 999 {
		return "", fmt.Errorf("diagnostic number %d must be between 1 and 999", number)
	}
	return Code(fmt.Sprintf("HADR-%s-%03d", prefix, number)), nil
}

// ParseCode validates raw and returns its prefix and numeric assignment.
func ParseCode(raw string) (Prefix, int, error) {
	parts := strings.Split(raw, "-")
	if len(parts) != 3 || parts[0] != "HADR" || len(parts[2]) != 3 {
		return "", 0, fmt.Errorf("diagnostic code %q must match HADR-<PREFIX>-<NNN>", raw)
	}

	prefix := Prefix(parts[1])
	if !prefix.Valid() {
		return "", 0, fmt.Errorf("diagnostic code %q uses unreserved prefix %q", raw, prefix)
	}

	for _, digit := range []byte(parts[2]) {
		if digit < '0' || digit > '9' {
			return "", 0, fmt.Errorf("diagnostic code %q has an invalid numeric assignment", raw)
		}
	}
	number, err := strconv.Atoi(parts[2])
	if err != nil || number < 1 || number > 999 {
		return "", 0, fmt.Errorf("diagnostic code %q has an invalid numeric assignment", raw)
	}
	return prefix, number, nil
}

// Validate reports whether c follows the reserved code convention.
func (c Code) Validate() error {
	_, _, err := ParseCode(string(c))
	return err
}

// Prefix returns the reserved prefix encoded in c.
func (c Code) Prefix() (Prefix, error) {
	prefix, _, err := ParseCode(string(c))
	return prefix, err
}

// Domain returns the contract domain encoded in c.
func (c Code) Domain() (Domain, error) {
	prefix, err := c.Prefix()
	if err != nil {
		return "", err
	}
	domain, _ := prefix.Domain()
	return domain, nil
}
