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
