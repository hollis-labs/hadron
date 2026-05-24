# AI First-Class Steps Audit

**Date:** 2026-05-24  
**Scope:** `http_call`, `mcp_call`, `message_wait`, `agent_launch`, `human_gate`, related diagnostics, daemon wiring, and user/operator docs.

## Summary

Hadron's AI-first-class step surface is now real in the stock daemon:

- `http_call`
- `mcp_call`
- `message_wait`
- `agent_launch`
- `human_gate`

All five are present in:

- blueprint parsing and validation
- schema docs
- execution
- daemon wiring

The remaining gaps are no longer about "does the primitive exist". They are
now mostly about:

- deeper interop semantics
- end-to-end realism
- operator-safe message inspection breadth
- launch/profile sophistication
- doc drift around what is actually complete

## Confirmed Complete

### Blueprint / Schema

The structured task kinds are modeled and validated in:

- [internal/blueprint/blueprint.go](/Users/chrispian/dev/hollis-labs/apps/hadron/internal/blueprint/blueprint.go:274)
- [schemas/blueprint-v0.4.schema.json](/Users/chrispian/dev/hollis-labs/apps/hadron/schemas/blueprint-v0.4.schema.json:509)

Pipeline wait controls are also present:

- [schemas/pipeline-v2.schema.json](/Users/chrispian/dev/hollis-labs/apps/hadron/schemas/pipeline-v2.schema.json:56)

### Runtime / Daemon

Execution paths exist for all step kinds:

- [http_call.go](/Users/chrispian/dev/hollis-labs/apps/hadron/internal/execution/http_call.go:1)
- [mcp_call.go](/Users/chrispian/dev/hollis-labs/apps/hadron/internal/execution/mcp_call.go:1)
- [message_wait.go](/Users/chrispian/dev/hollis-labs/apps/hadron/internal/execution/message_wait.go:1)
- [agent_launch.go](/Users/chrispian/dev/hollis-labs/apps/hadron/internal/execution/agent_launch.go:1)
- [human_gate.go](/Users/chrispian/dev/hollis-labs/apps/hadron/internal/execution/human_gate.go:1)

The stock daemon wires the needed adapters in:

- [cmd/hadrond/main.go](/Users/chrispian/dev/hollis-labs/apps/hadron/cmd/hadrond/main.go:142)

### Diagnostics / UI

Operation summaries exist for:

- `mcp_call`
- `http_call`
- `message_wait`
- `agent_launch`

via:

- [internal/rundiagnostics/operations.go](/Users/chrispian/dev/hollis-labs/apps/hadron/internal/rundiagnostics/operations.go:1)
- [RunOperationsPanel.tsx](/Users/chrispian/dev/hollis-labs/apps/hadron/cmd/hadron-app/frontend/src/components/runs/RunOperationsPanel.tsx:1)

### Current Tests

There is meaningful execution coverage for:

- local and external `mcp_call`
- local and remote `message_wait`
- timeout handling for `message_wait` and `agent_launch`
- local runtime-backed `agent_launch`
- `human_gate`

The core execution integration surface is in:

- [internal/execution/integration_test.go](/Users/chrispian/dev/hollis-labs/apps/hadron/internal/execution/integration_test.go:1)

## Remaining Gaps

### 1. Autonomous launch-to-reply is still only partially proven

There is now a launch-then-wait integration proving the callback contract path,
but there is still no test where the launched agent process itself authors and
sends the reply without external test help.

Impact:

- the orchestration contract is proven
- the fully autonomous agent-reply path is still only indirectly covered

### 2. Message get/consume semantics are better, but still soft

The message API surface is:

- `GET /v1/messages/{id}`
- `POST /v1/messages/{id}/consume`

and the backing interface is also id-only:

- [internal/api/types.go](/Users/chrispian/dev/hollis-labs/apps/hadron/internal/api/types.go:62)

Optional substrate scoping now exists, but the id-only path is still supported
for backward compatibility.

Impact:

- backward-compatible lookups can still be ambiguous
- duplicate message ids across peers are still theoretically possible

### 3. Remote message support is broader, but still not full-surface

Remote message support now covers:

- send
- get
- pull inbox
- non-destructive list
- thread listing
- consume

Missing or partial compared to the local/Tether prior art:

- explicit notify/wake result surface in Hadron's own REST/MCP APIs
- richer filter semantics beyond the current thread/correlation projection

Relevant code:

- [internal/messagesubstrate/http_client.go](/Users/chrispian/dev/hollis-labs/apps/hadron/internal/messagesubstrate/http_client.go:1)

Impact:

- good enough for workflow waits and basic inspection
- not yet a full operator-grade or substrate-neutral messaging abstraction

### 4. Boot profile support is real, but intentionally narrow

`agent_substrates[*].boot.profile` and `callbacks_profile` now do real work,
but the implementation is still a simple file/template renderer:

- [internal/agentsubstrate/boot.go](/Users/chrispian/dev/hollis-labs/apps/hadron/internal/agentsubstrate/boot.go:1)

It is **not** yet:

- a full shared catalog/profile compiler
- a slot-based bootgen pipeline
- a resolver for richer profile sources

Impact:

- good enough for now
- but this is still a local minimum, not an end-state

### 5. Config validation is present, but only at settings load

Substrate validation now fails early during settings load, which is much better
than per-run failure. The remaining gap is operator ergonomics:

- no dedicated `hadron settings validate`
- no richer per-field remediation hints

Impact:

- correctness is better
- usability can still improve

## Provisional Priority Order

This is the current engineering order before any product steering:

1. stronger autonomous launch-to-reply proof
2. firmer substrate-scoped message identity rules
3. richer remote messaging semantics
4. broader boot profile/compiler evolution
5. settings validation ergonomics

## Recommendation

Treat the AI-first-class step implementation as **functionally landed but not
fully hardened**.

The next decisions should be product-driven:

- do we want better operator safety first
- better inspection parity first
- stronger interop first
- or deeper agent boot/profile sophistication first

That choice now matters more than adding another primitive.
