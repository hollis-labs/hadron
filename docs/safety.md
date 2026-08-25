# Safety and trust boundaries

Hadron authorizes graph-native operations at the shared application-host
boundary. A CLI flag, HTTP body, MCP argument, task/run ID, wait ID, or
activation registration ID is never accepted as proof of authority.

## Identity and exposure

- HTTP loopback operation uses an explicit local-operator binding and rejects
  cross-origin or DNS-rebinding-shaped requests. Durable bearer credentials
  resolve to principal/profile records; unknown credentials fail closed.
- MCP stores only token digests and binds each session to one durable principal
  and exposure profile. Raw tokens are not persisted or logged.
- A2A rewrites the already-authenticated source authority at its trusted ingress
  and persists a full owner binding for task/run correlation.
- Background activations execute as their immutable durable registration
  identity, not as the delivery caller or an ambient background context.
- Hidden definitions and runs return the same safe not-found shape as missing
  resources. Ordinary policy denial remains a distinct safe denial.

Profiles are additive restrictions. Exact pins, search scope, namespace scope,
denied effects, private display, collision checks, and direct-tool budgets are
enforced before exposure changes. Session mounts reauthorize on every operation
and reconcile when profile/catalog generations change.

## Effects, capabilities, and confirmation

Each frozen step-kind contract declares its effects, required capabilities,
idempotency, retry safety, cancellation, and suspension behavior. Validation
compares a graph node to that registered contract; source or runtime config
cannot hide effects or widen the host's capability set.

`hadron workflow explain` reports the exact policy-visible facts without
admitting work. The production policy allows authenticated non-advised work and
requires confirmation when the immutable facts advise it, including mutating,
destructive, or unresolved-call effects. Invalid or unbound facts are denied.
Execution targets are checked against the requested ID/kinds/capabilities,
labels, and sandbox constraints.

Dry-run is truthful. If an executor cannot provide a non-effecting preview,
Hadron fails closed and preserves the safe structured rejection. A dry-run may
create the documented durable audit/start binding, but it admits no running
work, nodes, or effects.

## Secrets and redaction

Credentials enter executor contracts as `SecretRef`, never plain workflow
input fields. The host resolves them only at a narrow adapter boundary. Secret
references and material are masked by the value renderer, diagnostics, events,
HTTP/MCP/A2A responses, and transport errors.

Stream-producing adapters wrap output with the workflow secret redactor before
retention or observation. Masking handles a resolved secret split across
multiple writes. Bounded observations and artifacts retain only the already
redacted bytes. `--reveal-private` can reveal authorized private values, but it
does not reveal secrets.

## Durable admission and replay

- Start intent binds definition, typed inputs, scope, target, and value pins to
  one digest and idempotency identity.
- Pins are authorized and converged before a run becomes runnable; permanent
  rejection terminalizes the durable run with no claimable work.
- Workers execute nodes from the pinned recovery plan, not a mutable registry
  locator, and resource-aware admission enforces worker and fan-out occupancy.
- Wait resume binds the current responder and delegates token/correlation/schema
  authority to the durable wait coordinator.
- Source-derived activation admission is fenced against the movable registry
  `current` alias, including reactor delivery, so stale rows cannot start a
  superseded version.
- Cancellation, resume, signal, task correlation, and activation delivery use
  their documented idempotency contracts and reject changed intent.

## Output compatibility boundary

Global `::set-output` scraping is removed from graph-native execution paths.
The retained shim exists only in the embeddable `workflow/adapters/cmd`
executor: one explicitly selected stream may use `parse: set-output` together
with `compatibility: true`, and validation emits a deprecation warning. It is
not enabled by the stock daemon because `cmd@v1` is outside the frozen six-kind
production capability profile.

Legacy blueprint/pipeline code remains only for archive/rewrite support behind
an internal compatibility switch. Production CLI roots, HTTP routes, MCP
instructions, and UI navigation do not expose it, and it carries no public
compatibility promise.

## Operational checks

- Treat `/v1/health` and `hadron daemon` as authoritative for Host startup,
  recovery, and readiness; do not accept a hard-coded process “OK.”
- Review `workflow explain` before confirming advised work.
- Prefer immutable published registry refs for operational runs.
- Inspect redacted events/values rather than raw database rows.
- Keep data, database, attestor keys, catalog locks, and token material on
  private host-owned paths.
- Stop the daemon gracefully so workers and timer callbacks quiesce before
  durable stores close.
