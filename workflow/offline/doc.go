// Package offline defines extraction-ready build manifests and an embedded
// workflow runner for the conservative daemon-less execution subset.
//
// Execution deliberately uses the ordinary runtime binding, recovery,
// ready-queue, dispatch, and output-finalization contracts. The current
// process-lifetime implementation is provided by runtime/inmemory; it is
// encapsulated here so generated artifacts do not acquire a second scheduler
// or reimplement graph semantics. Hosts that promise restart recovery must
// inject durable storage through ExecuteWithStore.
package offline
