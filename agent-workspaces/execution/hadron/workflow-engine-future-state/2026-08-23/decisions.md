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
