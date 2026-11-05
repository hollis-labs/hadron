// Package rundiagnostics provides Hadron-owned operator projections for both
// historical legacy runs and graph-native workflow runs.
//
// Graph-native diagnostics treat persisted lifecycle snapshots, immutable
// events, typed value references, and the exact pinned ExecutionPlan as the
// source of truth. They never reconstruct workflow outcomes from application
// logs. Optional durable histories advertise their availability explicitly,
// and all returned collections are bounded and defensively owned.
package rundiagnostics
