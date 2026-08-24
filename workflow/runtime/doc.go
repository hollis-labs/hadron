// Package runtime owns application-neutral workflow execution mechanics,
// including node state, readiness, retries, cancellation, and replay.
//
// It is extraction-ready engine core. Imports are limited to the standard
// library and explicitly adopted extraction-safe dependencies; Hadron internal
// packages are forbidden. Its public API is unstable until the runtime
// contracts are defined.
package runtime
