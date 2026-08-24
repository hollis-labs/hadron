// Package llm implements the provider-neutral llm@v1 workflow step kind.
//
// The package deliberately contains no provider SDK. Hadron or another host
// supplies Policy, Provider, and ToolHost implementations. ProviderBinding and
// all audit fields cross a durable boundary and therefore contain stable,
// non-secret identifiers only; provider credentials are resolved by the host
// behind Provider and never enter workflow configuration, results, or errors.
// Direct SDK, AgentKit-style, and Nanite bridges implement the same Provider
// seam without changing the graph or typed output contract.
//
// Model tool requests are untrusted intent. Literal tool evidence is created
// only after the adapter invokes ToolHost, and is recorded both in typed output
// and in the runtime-issued verification ActivityRecorder. ToolHost receives
// the complete exact allowlist and must independently enforce it.
// Prompt and typed context contents remain process-local provider input and are
// never copied into durable audit/error metadata. SecretRef inputs are rejected;
// known resolved material can be masked from streamed and final output through
// Options.Redactor. Every durable output is private and run-retained.
package llm
