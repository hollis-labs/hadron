package values

// Type identifies which payload a Value carries.
type Type string

const (
	TypeNull     Type = "null"
	TypeString   Type = "string"
	TypeNumber   Type = "number"
	TypeBoolean  Type = "boolean"
	TypeArray    Type = "array"
	TypeObject   Type = "object"
	TypeArtifact Type = "artifact"
)

// Valid reports whether t is a supported value type.
func (t Type) Valid() bool {
	switch t {
	case TypeNull, TypeString, TypeNumber, TypeBoolean, TypeArray, TypeObject, TypeArtifact:
		return true
	default:
		return false
	}
}

// RedactionClass controls whether a value may be displayed or recorded by a
// consumer. Enforcement belongs to later policy and transport work.
type RedactionClass string

const (
	RedactionPublic  RedactionClass = "public"
	RedactionPrivate RedactionClass = "private"
	RedactionSecret  RedactionClass = "secret"
)

// Valid reports whether c is a supported redaction class.
func (c RedactionClass) Valid() bool {
	switch c {
	case RedactionPublic, RedactionPrivate, RedactionSecret:
		return true
	default:
		return false
	}
}

// RetentionClass describes the intended lifetime and ownership of value data.
// Cleanup and enforcement belong to artifact-store and host adapters.
type RetentionClass string

const (
	RetentionNone     RetentionClass = "none"
	RetentionRun      RetentionClass = "run"
	RetentionProject  RetentionClass = "project"
	RetentionExternal RetentionClass = "external"
)

// Valid reports whether c is a supported retention class.
func (c RetentionClass) Valid() bool {
	switch c {
	case RetentionNone, RetentionRun, RetentionProject, RetentionExternal:
		return true
	default:
		return false
	}
}
