// Package agent implements the extraction-ready agent-session executor and
// the deterministic agent_launch graph composition.
//
// Agent sessions remain ordinary external operations inside separately
// identified child workflow runs. The parent composition uses call@v1 for the
// child lifecycle and wait_for@v1 for optional child-run completion. Provider,
// product, team, and presentation policy belong to injected host adapters.
//
// SourceExpander accepts substrate, logical_agent_id, optional launch_id,
// prompt_append, parent_close, and wait. wait:false is fire-and-forget.
// wait:true requires the node's timeout.wait; wait:{timeout:<duration>} is the
// self-contained form. Fire-and-forget retains the authored ID on call@v1 and
// exposes its run handle. Wait mode retains the authored ID on wait_for@v1 and
// exposes payload/resume/timed_out, where payload is the typed
// handle/status/result object.
//
// The wait contract accepts the canonical child-run terminal envelope, but
// producing that envelope from child terminal transitions is a Hadron host
// integration boundary owned by W07-T08. This adapter does not imply that a
// host lacking that producer completes wait mode automatically.
//
// SessionHost is intentionally stronger than a process-local launcher: exact
// launch replay and observe/heartbeat/cancel must survive host restart. A host
// that cannot fence ambiguous launch success does not satisfy the production
// contract.
package agent
