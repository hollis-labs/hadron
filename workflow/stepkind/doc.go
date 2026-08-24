// Package stepkind owns application-neutral step-kind metadata, registry, and
// executor contracts.
//
// It is extraction-ready engine core. Imports are limited to the standard
// library and other workflow core packages; Hadron internal packages and
// concrete adapters are forbidden. Executors advertise immutable metadata at
// registration and implement only the lifecycle capabilities they declare.
package stepkind
