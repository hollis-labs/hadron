// Package runtime owns application-neutral workflow execution mechanics,
// including node lifecycle state, readiness, retries, cancellation, and replay.
//
// The StateStore contract persists workflow facts and compare-and-swap claims
// without exposing SQL or host records. Lifecycle and durable ready-queue
// coordination are core-owned; host scheduling policy, binding, and concrete
// storage remain separate concerns.
//
// It is extraction-ready engine core. Hadron internal packages and concrete
// persistence implementations are forbidden.
package runtime
