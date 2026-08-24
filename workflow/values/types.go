package values

// Producer identifies where a value originated without depending on a host's
// run, persistence, principal, or registry record types. Kind is intentionally
// open-ended; Reference is an opaque engine- or adapter-owned identifier, and
// Output optionally identifies a named output within that producer.
type Producer struct {
	Kind      string `json:"kind"`
	Reference string `json:"reference"`
	Output    string `json:"output,omitempty"`
}

// Metadata is the classification and provenance common to every Value.
type Metadata struct {
	Producer  Producer       `json:"producer"`
	MediaType string         `json:"media_type"`
	Redaction RedactionClass `json:"redaction"`
	Retention RetentionClass `json:"retention"`
}

// ArtifactRef describes content held outside the workflow data envelope. It
// carries no artifact bytes and does not prescribe a storage implementation.
type ArtifactRef struct {
	Store     string         `json:"store"`
	URI       string         `json:"uri"`
	Digest    string         `json:"digest"`
	MediaType string         `json:"media_type"`
	SizeBytes int64          `json:"size_bytes"`
	Producer  Producer       `json:"producer"`
	Redaction RedactionClass `json:"redaction"`
	Retention RetentionClass `json:"retention"`
}

// Value is the workflow data-plane envelope. An inline value has exactly one
// JSON-compatible payload in Inline and no Artifact. An artifact value has an
// Artifact and no Inline payload. Type makes inline null distinct from a
// missing payload.
type Value struct {
	Type      Type
	Inline    any
	Artifact  *ArtifactRef
	Producer  Producer
	MediaType string
	Digest    string
	Redaction RedactionClass
	Retention RetentionClass
}

// ValueSet is a named collection of workflow values, such as run inputs or a
// node invocation's outputs.
type ValueSet map[string]Value

// ValueSetRef is an opaque, digest-bound reference to a persisted ValueSet.
// The same shape can be embedded by run, node invocation, wait, and event
// records without importing a host persistence identifier type.
type ValueSetRef struct {
	ID     string `json:"id"`
	Digest string `json:"digest"`
}
