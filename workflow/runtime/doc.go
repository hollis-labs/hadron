// Package runtime owns application-neutral workflow execution mechanics,
// including node lifecycle state, readiness, retries, cancellation, and replay.
//
// The StateStore contract persists workflow facts and compare-and-swap claims
// without exposing SQL or host records. Lifecycle, named readiness rules,
// durable ready-queue coordination, and the provisional atomic wait-timeout
// contract are core-owned; host scheduling policy, binding, persisted generic
// wait scheduling, and concrete storage remain separate concerns.
//
// It is extraction-ready engine core. Hadron internal packages and concrete
// persistence implementations are forbidden.
package runtime
