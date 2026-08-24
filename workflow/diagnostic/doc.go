// Package diagnostic defines application-neutral, source-mapped diagnostics for
// workflow authors and transports.
//
// Diagnostic codes have the stable form HADR-<PREFIX>-<NNN>. The reserved
// prefixes and their domains are:
//
//	SOURCE  source syntax and structural validation
//	REF     source reference and dependency validation
//	LEGACY  archived source migration
//	OUTPUT  archived output-shim migration
//	VALUE   expression, binding, and value evaluation
//	ARTIFACT artifact storage, authorization, verification, and retention
//	POLICY  capability and policy enforcement
//	EFFECT  effect, retry, and idempotency safety
//	WAIT    wait and external-event handling
//	PERSIST persistence and recovery
//	HOST    host integration and capability negotiation
//
// REF, LEGACY, and OUTPUT are source-validation subdomains retained for the
// migration codes established by the workflow architecture.
package diagnostic
