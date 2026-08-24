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
