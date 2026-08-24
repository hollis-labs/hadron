// Package offline defines extraction-ready build manifests and an embedded
// workflow runner for the conservative daemon-less execution subset.
//
// Execution deliberately uses the ordinary runtime binding, recovery,
// ready-queue, dispatch, and output-finalization contracts. The current
// in-memory durable implementation is provided by runtime/runtimetest; it is
// encapsulated here so generated artifacts do not acquire a second scheduler
// or reimplement graph semantics.
package offline
