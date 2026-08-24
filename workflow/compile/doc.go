// Package compile owns loading, validation, import resolution, and compilation
// of workflow sources into executable graph plans.
//
// It is extraction-ready engine core. Imports are limited to the standard
// library and explicitly adopted extraction-safe dependencies; Hadron internal
// packages are forbidden.
//
// The source loader preserves YAML authoring shape and locations. Compilation
// lowers that shape to an immutable ExecutionPlan. Value-dependency inference
// parses structural expression references into data edges and a direct
// invocation visibility plan. Validation checks graph structure plus
// caller-supplied step-kind, policy, and definition seams. None of these phases
// executes expressions or starts runtime work.
package compile
