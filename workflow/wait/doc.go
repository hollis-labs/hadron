// Package wait defines application-neutral durable wait, resume, and timed
// activation contracts.
//
// A wait is durable state rather than a blocked worker. Gate, message, timer,
// callback, child-run, and signal adapters all converge on the same resume
// operation. Host-specific endpoint materialization and scheduler bindings sit
// behind the narrow interfaces in this package.
package wait
