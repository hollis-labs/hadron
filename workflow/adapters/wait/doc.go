// Package waitadapter implements durable external wait step kinds over the
// canonical workflow/wait record and runtime continuation contract.
//
// The executors never poll or retain a worker. Initial execution returns a
// StepWaiting handoff; accepted resumes re-drive the same attempt with a typed
// continuation. Host identity, callback materialization, and responder policy
// are injected. This package owns neither persistence nor transports.
// Callback-capable wait_for is conservatively unavailable in embedded mode.
// Callback issuers must honor the stable issuance key and own TTL cleanup if
// issuance succeeds but durable suspension later fails.
package waitadapter
