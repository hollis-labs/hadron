// Package llmprovider binds Hadron's provider-neutral workflow LLM contract to
// the go-providers registry and go-llm-contracts provider API.
//
// The bridge is deliberately Hadron-owned. Provider credentials, endpoint
// selection, SDK clients, and provider-specific retries stay behind the
// registered go-llm-contracts Provider. The workflow package sees only stable,
// non-secret binding and audit metadata.
//
// Stock go-providers PTY/Subprocess bridges execute CLI-managed tools inside the
// provider process, so their advertised tool capability is not sufficient for
// workflow tool use. Bridge defaults to denying tools. A host may opt in a
// provider name only after establishing that implementation is proposal-only
// and all execution returns through workflow/adapters/llm.ToolHost.
//
// A downstream Nanite host does not need this package or a Hadron provider
// registry. It can implement workflow/adapters/llm.Provider directly, supply
// that implementation to llm.New, and keep Nanite-owned context assembly,
// permission checks, credentials, and tool brokerage outside workflow core.
package llmprovider
