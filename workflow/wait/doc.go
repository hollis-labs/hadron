// Package wait owns application-neutral contracts for suspending, correlating,
// and resuming workflow execution.
//
// It is extraction-ready engine core. Imports are limited to the standard
// library and explicitly adopted extraction-safe dependencies; Hadron internal
// packages are forbidden. Its public API is unstable until the wait contracts
// are defined.
package wait
