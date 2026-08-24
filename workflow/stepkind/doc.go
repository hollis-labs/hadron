// Package stepkind owns application-neutral step-kind metadata, registry, and
// executor contracts.
//
// It is extraction-ready engine core. Imports are limited to the standard
// library and other workflow core packages; Hadron internal packages and
// concrete adapters are forbidden. Executors advertise immutable metadata at
// registration and implement only the lifecycle capabilities they declare.
// Memoization metadata is a fail-closed capability declaration: it cannot make
// mutate/destructive effects reusable, and materialize still requires host
// policy approval.
package stepkind
