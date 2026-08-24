# Execution Decisions

## Decisions

- W03-T03 implements the named readiness rules with this exact progression
  contract: `all_success` skips on any unhandled non-success terminal outcome;
  `all_done` waits for all terminals; `one_failed` becomes ready on the first
  unhandled hard failure; `all_failed` requires a non-empty all-hard-failure
  set; `none_failed` accepts succeeded, skipped, and handled failures; and
  `always` is immediately ready. `failed`, `timed_out`, `canceled`, and
  `crashed` are hard failures. `FailureHandled` is only a narrow
  success-equivalent propagation signal; W03-T08 still owns catch/error-route
  mechanics.

- Readiness skip explanations are stored in the atomic terminal transition
  event, not the terminal node snapshot. Exact same-time/same-explanation
  retries are no-ops; different intent conflicts. A blocked node may refresh a
  changed structured reason only at a strictly later timestamp, producing a
  new generation and event. Hadron SQLite and the in-memory conformance store
  implement the same contract.

- W03-T03 exposes wait timeout as an optional atomic persistence capability
  while W03-T05 owns the complete generic wait record, persisted deadline,
  activation scheduling, and concrete SQLite timeout adapter.

  Vanta revision: `01M0S9GZTKZGG5F1MS7WJJV3DY`.

- W03-T05 places the canonical semantic `Record` in extraction-ready
  `workflow/wait` and embeds it as one flat JSON envelope in the runtime
  `WaitSnapshot`. Generic wait mutation is exposed only through atomic
  suspend/resume/timeout operations; the former low-level wait CRUD surface is
  removed so every wake source shares the same fenced state transition.

- Resume idempotency is wait-centered: an exact caller-key replay returns the
  accepted durable result, a different non-empty key after acceptance
  conflicts, and a keyless duplicate returns the already-accepted result with
  an `already_resumed` outcome. Host authorization is re-evaluated before any
  replay result or payload-schema detail is returned.

  Vanta revision for both W03-T05 decisions: `01M0SDEDRSK370D6NDFXHPXZJH`.

- W02-T08 sets the inline capture default to 64 KiB and the maximum
  configurable inline threshold to 1 MiB. The artifact stream limit is a
  separate required bound and may be larger. Secret values always promote;
  oversized JSON remains byte-exact opaque artifact content rather than being
  parsed or represented as validated JSON.

- The Hadron adapter owns only durable `run` and `project` artifacts. `none`
  may remain inline but cannot promote to local durable storage, while
  `external` references are routed opaquely to explicitly approved read/stat
  delegates and are never deleted by Hadron. Every resolution is authorized
  before lookup/delegation and, when verified owner metadata is available,
  authorized again with that owner context.

  Vanta revision for both W02-T08 decisions: `01M0SEA3HYG29FQ1X92SJRS4M1`.

- W04-T01 closes executor handoff into exactly one of `completed`, `waiting`,
  or `external`. Registry resolution is exact by kind and version and uses the
  immutable metadata snapshot captured at registration. Required lifecycle is
  `Spec`, `ValidateConfig`, and `Execute`; optional lifecycle interfaces must
  agree with advertised prepare, observe, heartbeat, cancel, and finalize
  metadata.

- A resumed generic wait continues the same unfinished attempt through an
  exact durable attempt-to-wait binding and digest-checked typed continuation;
  the raw resume token never enters the invocation. External work likewise
  binds an immutable non-secret reference and invocation to the exact attempt.
  Transient heartbeat, observe, and cancel I/O errors keep that operation
  pending for recovery rather than re-entering `Execute`; only adapter-reported
  terminal outcomes or irrecoverable contract mismatches close the attempt.

  Vanta revision for both W04-T01 decisions: `01M0SG27S7X00HFQ74ADDPP20X`.

- W04-T02 keeps the transform contract as a flat normalized output-name to raw
  expression map. The default context exposes only invocation inputs; a
  concurrency-safe adapter-owned `ContextProvider` may supply already-scoped
  steps, item, index, run, and target roots, but ambient environment access is
  always denied and invocation inputs remain authoritative.

- Transform output names are configuration-defined, so the immutable kind
  metadata declares an honestly open object schema. The runtime validates each
  returned name and typed value against the graph node's `OutputSpec`
  declarations instead of freezing config-specific names into registry
  metadata.

  Vanta revision for both W04-T02 decisions: `01M0SH6C50DB0QPBQ13ZYD52DZ`.

- W04-T05 keeps `mcp@v1` SDK-neutral and declares the conservative possible
  effect set, keyed idempotency capability, and retry-requires-idempotency in
  immutable registry metadata. Its exported pre-execution `DescribeConfig`
  projection may narrow effects or retry hints only for independently trusted,
  coherent tool annotations; missing, untrusted, partial, or conflicting hints
  stay fail-closed.

- The Hadron `InternalCaller` bridge treats server-self-asserted annotations as
  untrusted. Workflow transport reconnect may repeat `CallTool` only when the
  invocation carries a non-empty idempotency key; the pre-existing legacy
  `CallTool` retry behavior remains unchanged for compatibility.

  Vanta revision for both W04-T05 decisions: `01M0SHQ1FXVFMQTN6YQ8XHR2QB`.

- W04-T03 keeps `cmd@v1` static metadata fail-closed: arbitrary command
  execution is destructive, non-idempotent, retry-unsupported, and requires
  `process.execute`. Configured effects, capabilities, paths, and sandbox are
  author expectations only; the injected host policy returns the authoritative
  clean executable, arguments, working directory, effective metadata, and
  sandbox before any secret resolution or process launch.

- The default OS process runner executes one argv vector without a shell,
  ambient environment, or ambient working directory. It truthfully supports
  only `direct`/`none`; stronger isolation and PTY behavior require an injected
  runner that attests the exact policy-authorized sandbox and sends operational
  bytes only through the adapter's bounded redacted writers.

  Vanta revision for both W04-T03 decisions: `01M0SKA8ZHADZ9C97RG90GM6R1`.

- W04-T04 keeps `http@v1` SDK-neutral and fail-closed. A host destination
  policy is mandatory; every DNS answer at every hop is authorized before one
  deterministic approved address is pinned for the actual dial. The secure
  default ignores ambient proxies, cookies, credential stores, keepalive,
  decompression, transparent redirects, and transport retries while preserving
  the logical Host and TLS SNI.

- HTTP configuration effects, capabilities, and idempotency are author claims.
  Immutable kind metadata stays conservative; only a coherent independently
  trusted policy description for a safe method may narrow effects or retry
  behavior. Secret references resolve only at execution boundaries, and all
  persisted outputs and observations are bounded and masked before crossing
  their storage boundary.

  Vanta revision for both W04-T04 decisions: `01M0SM4422SRHQ8AM84WXWFKMX`.

- W03-T04 admits another attempt only after a retry activation is persisted.
  Retry policy uses the union of trusted executor and graph-declared effects;
  graph declarations may constrain but cannot upgrade executor idempotency or
  narrow trusted effects. Mutating and destructive retries therefore require
  the exact keyed/explicit policy guarantees defined by the executor contract.

- Run cancellation commits the terminal run state before any adapter or host
  cleanup. Every later attempt, retry, claim, external-operation, and fan-out
  mutation is fenced by that durable terminal state; only the exact canceled
  external-operation resolution may finish its already-recorded cancellation
  intent. Child runs use their declared `cancel`, `request_cancel`, or
  `abandon` policy, and already-terminal children remain unchanged.

- Fan-out item identities use stable, delimiter-safe attempt encoding and
  zero-padded iteration indexes. `max_concurrency` counts started nonterminal
  logical items, including durable waits and retry delays, rather than worker
  leases, so suspension cannot accidentally release a fan-out slot.

  Vanta revision for all W03-T04 decisions: `01M0SMKS6V2TETG1R8AMGSP7JH`.

- W04-T07 models a successful durable timer with immutable `WakeAt`, distinct
  from an optional failure `Deadline`. When both are due, an earlier wake wins
  deterministically regardless of competing compare-and-swap order. Core
  derives the timer activation and resume identity; generic callers cannot
  forge a timer resume.

- Wait timeout is a typed failed attempt that follows normal catch routing;
  successful wait-adapter outputs always report `timed_out: false`. Event waits
  lower to the canonical signal mechanism with durable authority attributes.
  The shared gate vocabulary admits optional/nonblocking author intent, but
  `human_gate@v1` rejects that mode until W07-T09 supplies graph lowering with
  an explicit skip path.

- Callback issuance uses an immutable, retry-stable idempotency key and must
  reuse and extend the same live callback credential or fail. Human-gate
  payload storage is likewise idempotent for the same immutable request;
  presentation, approval policy, and payload authority remain application
  responsibilities.

  Vanta revision for all W04-T07 decisions: `01M0SPW0EQ04GBNVH5YXH0Q21E`.

- W07-T02 keeps `script@v1` as a synchronous, capability-free Goja data
  transform with required input/output schemas. It admits only cloned inline
  non-secret values, returns private run-retained typed outputs, rejects
  numbers that JavaScript cannot represent exactly, and exposes no ambient
  filesystem, network, module, secret, clock, random, async, or Hadron helper
  surface.

- Upstream Goja has no truthful per-runtime heap quota. The in-process adapter
  therefore defines its deterministic memory boundary over source, canonical
  input/output bytes, nesting, aggregate items, strings, and call stack, with
  separate wall-time/context interruption. It does not expose a misleading
  heap-limit setting or claim hostile-tenant process isolation.

  Vanta revision for both W07-T02 decisions: `01M0SRTJX81SHA01FAG7E1C7G4`.

- W04-T06 pins a logical call site by parent run, node, iteration, and the
  requested definition reference, excluding both attempt number and the
  resolver's answer. The first durable record fixes the exact child digest,
  provenance, effective-input digest, and lineage; retries or recovery must
  replay that record exactly and reject resolver drift. Inline and child-run
  operation identities are therefore stable across ambiguous retries.

- Child input precedence is declaration default, then resolver/import partial
  binding, then the node-local already-evaluated input. Inline calls remain in
  the parent run and return only declared typed outputs. Run-mode calls create
  a separate stable child identity and return typed status, events,
  cancellation, and output references while the existing `ChildRunLink`
  remains authoritative for `cancel`, `abandon`, and `request_cancel` parent
  closure behavior.

  Vanta revision for both W04-T06 decisions: `01M0SSDA2YJ5JJ919KTS8DX7J2`.

- W05-T01 makes Hadron's host journal the durable application boundary around
  the extraction-safe runtime. Host start uses immutable intent and policy
  facts plus a separate CAS phase checkpoint; readiness is granted only after
  recovery drains, and concurrent materializers converge on the durable
  winner even when it is several phases ahead. Every exact start replay
  re-authenticates the current caller before returning prior state or conflict
  detail.

- Confirmation is an acknowledgment of the first append-only policy decision,
  not a different start intent. Denied and confirmation-required evaluations
  are retained even when no Run exists. Caller identity, resolved plan digest,
  effects, capabilities, target requirements, dry-run truth, and blast radius
  remain immutable policy evidence keyed by the logical start.

- Host cancellation records an immutable caller intent and stable time lower
  bound, but resolves the current Run generation for each application attempt.
  CAS churn is context-aware and bounded; exhaustion leaves the durable intent
  pending for startup/periodic recovery rather than freezing a stale
  generation or spinning indefinitely.

- Hadron SQLite owns call-resolution and child-run materialization journals.
  Call resolution, its canonical event, and exact replay are atomic; child-run
  inputs, pending Run, parent link, creation event, cancellation handle, and
  idempotency result commit together before the injected definition
  materializer advances the child.

  Vanta revision for all W05-T01 decisions: `01M0SXD68BJD9N18SPV307T6FM`.

- W03-T08 records switch and catch selection as immutable control decisions and
  records a terminal intent before admitting finalizers. Finalizers execute
  inner-to-outer within a scope while disjoint cleanup chains may progress
  independently. A cleanup failure determines the final failed run status, but
  the original intended terminal status and typed error remain durable context.

- `ParentCloseCancel` is one exact atomic cancellation operation over the
  reachable locally owned descendant tree, including explicit descendants
  with zero finalizers. The host resolves the root and every child from pinned
  stored definitions, refreshes the whole tree on compare-and-swap contention,
  and materializes pending child starts before cancellation recovery so child
  cleanup cannot be stranded.

- Pre-attempt catch/finalizer expression context exposes an error only from an
  exact persisted catch decision or terminal intent. Runtime status alone does
  not fabricate typed failure data. When several unhandled failures compete,
  terminal selection is deterministic by severity (`crashed`, `timed_out`,
  `failed`, then `canceled`) and graph order.

  Vanta revision for all W03-T08 decisions: `01M0T34JA05A46A75VDGJ4HX6W`.

- W05-T03 makes one Hadron-owned resolver the source boundary for file,
  registry, package, compiler, host-start, and child-call paths. Exact-source
  cache identity is only the canonical kind, authority, id, locator, version,
  and digest tuple; caller-supplied provenance is deliberately excluded from
  identity but remains visible to requested and resolved authorization on
  every access. Movable aliases are re-resolved, while version/digest pins
  remain immutable for the resolver lifetime.

- Host resolution replaces authored provenance rather than merging untrusted
  claims. The selected source-byte digest remains distinct from the compiled
  graph digest, and package `MaxSourceBytes` bounds the entire decompressed tar
  stream—including metadata and padding—so compressed or hidden archive data
  cannot bypass admission limits.

- Child-run recovery materializes only the exact graph and inputs already
  carried by the durable child request; it never re-resolves a movable alias.
  Node creation converges with cancellation and terminal-intent fences, and
  catch, switch, and finally targets remain pending until ordinary durable
  control-flow progression admits them.

  Vanta revision for all W05-T03 decisions: `01M0T44S9EZ9MSW3VVHVTWBCQH`.

- W07-T03 keeps `agent_launch` out of the runtime primitive vocabulary. A pure,
  named compiler expander lowers it to an exact generated child definition,
  ordinary `call@v1`, and optional `wait_for@v1`; generated definitions are
  serialized into the parent plan and participate in its semantic digest.

- Hadron resolves generated children from the current compiled-plan cache or
  immutable persisted host start plans. Requested authorization runs before
  durable lookup, and resolved authorization includes the exact containing
  `runtime.PlanRef` plus child identity and trust class; conflicts are assessed
  only among candidates the current caller may access.

- The existing agentkit launcher is exposed only through
  `LegacyWorkflowBridge`, an explicitly compatibility-only, process-local
  adapter. Production implementations must satisfy the durable `SessionHost`
  launch/replay/observe/heartbeat/cancel contract rather than inheriting the
  legacy bridge's weaker restart behavior.

  Vanta revision for all W07-T03 decisions: `01M0T66FYW6HCM98J3XZ0W1NZK`.
