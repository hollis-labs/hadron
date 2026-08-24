// Package conformance owns reusable black-box suites for workflow compilers,
// stores, runtimes, waits, and step-kind registries.
//
// It is extraction-ready test support. Imports are limited to the standard
// library and explicitly adopted extraction-safe dependencies; Hadron internal
// packages are forbidden. Its public API is unstable until the conformance
// contracts are defined.
package conformance
