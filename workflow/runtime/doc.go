// Package runtime owns application-neutral workflow execution mechanics,
// including node lifecycle state, readiness, retries, cancellation, and replay.
//
// The StateStore contract persists workflow facts and compare-and-swap claims
// without exposing SQL or host records. Lifecycle, named readiness rules,
// durable ready-queue coordination, plan-to-run input binding, completed
// workflow output binding, classification-safe rendering/event masking,
// retention hook coordination, generic durable wait coordination, and atomic
// wait timeout are core-owned. Host scheduling policy, principal and grant
// resolution, scheduler adapters, secret authorities, cleanup execution, and
// concrete storage remain separate concerns.
//
// It is extraction-ready engine core. Hadron internal packages and concrete
// persistence implementations are forbidden.
package runtime
