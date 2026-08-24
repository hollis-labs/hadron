// Package values owns the typed workflow data plane: JSON-compatible inline
// values, opaque artifact references, producer and classification metadata,
// deterministic digests, and value-set references.
//
// Logs and compatibility set-output records are operational concerns, not
// Value fields. Secret references are validated and persistable, while resolved
// material remains an ephemeral adapter-boundary type that cannot enter Value.
// Shared renderers and streaming redactors fail closed for secret data;
// concrete secret authorities, artifact storage, and cleanup remain outside
// this package. Expressions evaluate only from explicitly supplied typed
// contexts and cannot derive from secret-classified values.
//
// Inline schemas are validated locally without resolving network or file
// references. The package is extraction-ready engine core. It imports only the
// standard library plus the adopted expression and JSON Schema dependencies
// and does not depend on Hadron application or persistence types.
package values
