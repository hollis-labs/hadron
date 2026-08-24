// Package compile owns loading, validation, import resolution, and compilation
// of workflow sources into executable graph plans.
//
// It is extraction-ready engine core. Imports are limited to the standard
// library and explicitly adopted extraction-safe dependencies; Hadron internal
// packages are forbidden. Its public API is unstable until the compiler
// contracts are defined.
package compile
