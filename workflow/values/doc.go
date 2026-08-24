// Package values owns the typed workflow data plane: JSON-compatible inline
// values, opaque artifact references, producer and classification metadata,
// deterministic digests, and value-set references.
//
// Logs and compatibility set-output records are operational concerns, not
// Value fields. Artifact storage, secret resolution, and retention enforcement
// remain outside this package. Expressions evaluate only from explicitly
// supplied typed contexts; ambient environment and log streams are never roots.
//
// The package is extraction-ready engine core. It imports only the standard
// library plus expr-lang/expr and does not depend on Hadron application or
// persistence types.
package values
