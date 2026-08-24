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
