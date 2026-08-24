// Package compile owns loading, validation, import resolution, and compilation
// of workflow sources into executable graph plans.
//
// It is extraction-ready engine core. Imports are limited to the standard
// library and explicitly adopted extraction-safe dependencies; Hadron internal
// packages are forbidden.
//
// The source loader preserves YAML authoring shape and locations without
// evaluating expressions, resolving references, or lowering source into the
// graph IR. Compiler contracts beyond source loading remain unstable.
package compile
