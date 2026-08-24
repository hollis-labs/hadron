// Package values owns typed workflow values, artifact references, expressions,
// and input and output binding.
//
// It is extraction-ready engine core. Imports are limited to the standard
// library and explicitly adopted extraction-safe dependencies; Hadron internal
// packages are forbidden. Its public API is unstable until the value contracts
// are defined.
package values
