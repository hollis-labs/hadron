// Package call implements the graph-native call@v1 step kind.
//
// Definition discovery remains host-owned through compile.DefinitionResolver.
// Durable call resolution, inline graph driving, and child-run creation are
// explicit ports so this extraction-safe adapter imports no Hadron host,
// registry, filesystem, transport, or concrete persistence package.
//
// Prepare evaluates child declaration defaults, then resolver/import partial
// bindings, and finally overlays the caller's already-typed node-local inputs.
// It records the resolved digest, provenance, effective-input digest, and full
// active lineage before child work begins. Resolution and operation identities
// use the logical call site rather than an execution-attempt number, so a keyed
// retry cannot fork an inline operation or child run.
//
// Inline mode returns exactly the child's declared typed outputs. Run mode
// returns typed run-id, status, events-ref, cancellation, and outputs-ref
// handles. Hosts implement parent closure through the persisted
// runtime.ChildRunLink policy carried by ChildRunRequest.
package call
