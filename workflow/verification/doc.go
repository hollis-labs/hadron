// Package verification defines application-neutral post-execution checks,
// literal activity evidence, verifier registration, and durable reports.
//
// The package deliberately contains no provider, adapter, runtime-store, or
// host imports. Runtimes issue an ActivityRecorder for one attempt; trusted
// adapters record only activity observed at the actual client/process boundary.
// Model text and executor outputs are never interpreted as activity evidence.
//
// Verification runs only after an executor has returned a valid completed
// result. The no_error check therefore attests that this post-execution
// boundary was reached; an executor/provider error remains authoritative and
// does not create a verification report. A generic wait is verified after its
// resumed execution completes. Durable external work is verified from its
// terminal observer result, and cannot carry process-local pre-suspension
// activity across that boundary.
package verification
