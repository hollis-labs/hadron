// Package graph owns the application-neutral workflow graph representation,
// source references, source maps, and schema-bearing contracts shared by
// compilers, runtimes, transports, user interfaces, and conformance tests.
//
// It is extraction-ready engine core. Imports are limited to the standard
// library and explicitly adopted extraction-safe dependencies; Hadron internal
// packages are forbidden. Adapter-specific configuration remains opaque to this
// package and is validated by step-kind implementations.
package graph
