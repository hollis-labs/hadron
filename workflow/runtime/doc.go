// Package runtime owns application-neutral workflow execution mechanics,
// including node lifecycle state, readiness, retries, cancellation, and replay.
//
// The StateStore contract persists workflow facts and compare-and-swap claims
// without exposing SQL or host records. Lifecycle, named readiness rules,
// durable ready-queue coordination, plan-to-run input binding, completed
// workflow output binding, classification-safe rendering/event masking,
// retention hook coordination, generic durable wait coordination, and atomic
// wait timeout are core-owned. Memoized and caller-pinned outputs use the same
// typed ValueSetRef data plane as executed/replayed outcomes: memo keys are
// evaluated under compiler visibility, pins are installed before admission,
// and reuse completes a claimed node atomically without a synthetic attempt.
// Host scheduling policy, principal and grant
// resolution, scheduler adapters, secret authorities, cleanup execution, and
// concrete storage remain separate concerns.
//
// Package inmemory provides a concurrency-safe process-lifetime implementation
// for embedded execution and qualification. Hosts that promise crash recovery
// must bind a durable StateStore implementation instead.
//
// It is extraction-ready engine core. Hadron internal packages and concrete
// persistence implementations are forbidden.
package runtime
