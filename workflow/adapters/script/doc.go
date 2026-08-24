// Package script implements the extraction-ready Goja JavaScript step kind.
//
// The adapter is intentionally a local, synchronous data-transform sandbox.
// A script receives only validated inline workflow inputs and returns a plain
// object that becomes typed workflow outputs. It does not install filesystem,
// network, environment, secret-resolution, module-loading, clock, random,
// console, timer, or ambient Hadron APIs. Artifact and secret-reference inputs
// are rejected because this package has no resolver or policy capability seam.
//
// ResourceLimits impose deterministic structural memory bounds over source,
// canonical input/output JSON, nesting, aggregate items, strings, and call
// stack, while Goja interruption enforces wall time and caller cancellation.
// Goja does not expose a per-runtime heap quota. Therefore this in-process
// adapter does not claim byte-accurate arbitrary working-heap isolation; a host
// that runs hostile multi-tenant scripts needs a separately designed isolation
// runner in addition to these fail-closed structural limits.
// Input and output schemas are explicit config declarations rather than
// inferred from JavaScript syntax, so the contract remains stable across Goja
// parser releases and can be validated before execution.
//
// Python support remains separate because it requires an explicit subprocess,
// filesystem, environment, package, signal, and operating-system sandbox
// contract. Those authorities cannot be inferred from this in-process Goja
// adapter without weakening its boundary.
package script
