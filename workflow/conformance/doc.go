// Package conformance owns reusable black-box suites for workflow compilers,
// stores, runtimes, waits, step-kind registries, verification, and memoization.
//
// It is extraction-ready test support. Imports are limited to the standard
// library and explicitly adopted extraction-safe dependencies; Hadron internal
// packages are forbidden. RunRequired preserves the original suite set;
// RunComplete is the exhaustive current entry point. Suite entry points and
// fixture names are stable; semantic assertions expand as their production
// contracts land.
package conformance
