// Package conformance owns reusable black-box suites for workflow compilers,
// stores, runtimes, waits, and step-kind registries.
//
// It is extraction-ready test support. Imports are limited to the standard
// library and explicitly adopted extraction-safe dependencies; Hadron internal
// packages are forbidden. Suite entry points and fixture names are stable;
// semantic assertions expand as their production contracts land.
package conformance
