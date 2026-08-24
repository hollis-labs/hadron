// Package values owns the typed workflow data plane: JSON-compatible inline
// values, opaque artifact references, producer and classification metadata,
// deterministic digests, and value-set references.
//
// Logs and compatibility set-output records are operational concerns, not
// Value fields. Artifact storage, expression evaluation, secret resolution,
// and retention enforcement remain outside this package.
//
// The package is extraction-ready engine core. It imports only the standard
// library and does not depend on Hadron application or persistence types.
package values
