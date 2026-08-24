# Deferred Findings Register

No execution findings have been deferred.

## Known limitations

- W00-T07 intentionally leaves stale-claim recovery policy outside
  `go-scheduler`: a process failure after `ClaimFire` and before a terminal or
  retry transition can leave a fire in `claimed`. Hadron's later scheduler and
  recovery binding must define lease/reclaim policy without weakening stable
  fire identity or per-attempt CAS guarantees. This does not block W00-T07's
  accepted contract, but must be reconciled before W05-T04/W03-T06 integration
  is considered durable.

  Vanta revision: `01M0RY570D54BHGAMK62VXGMW4`.

- W02-T05 keeps `PRAGMA foreign_keys=OFF` on Hadron's shared SQLite store
  because enabling it breaks supported legacy pipeline-stage CRUD. The
  graph-native adapter enforces its owned parent relationships inside the same
  `BEGIN IMMEDIATE` transaction; opaque value/event/cache/pin/future-run
  references intentionally retain the application-neutral StateStore
  semantics. Independent handles can surface SQLite busy or context errors
  when a writer holds the lock beyond the five-second busy timeout. Reassess
  this boundary before globally enabling foreign keys or changing legacy
  persistence relationships.

  Vanta revision: `01M0S892FPWHRP5CC3ZS6CFTWB`.

- W02-T03 and W02-T06 support graph-native `artifact` and `secret_ref`
  admission through direct root types, root-local references, and root
  `allOf`/`anyOf`/`oneOf` composition. Conditional admission for these custom
  envelope types through `if`/`then`/`else` is intentionally unsupported.
  Inline JSON Schema conditionals remain available; expand the graph-native
  permission vocabulary only if a concrete authoring case requires it,
  without confusing ordinary strings/objects with classified envelopes.

  Vanta revision: `01M0SBFTFVZW4FMK5JRSRJ1FEQ` (supersedes
  `01M0S9GZ6QCBXJ8402PD44C4S0`).

- W03-T05 migration 0015 keeps migration-0014 wait rows readable, but any
  legacy row still open at upgrade is deliberately marked
  `legacy_unresumable`: the older schema never persisted the correlation,
  authority, or raw resume credential needed to recreate a safe wake path.
  Such rows remain observable and recoverable for timeout/history purposes but
  require application-level reconciliation rather than synthesized authority.

  Vanta revision: `01M0SDEDBDN3P39TT03W5A1XSZ`.

- W02-T08 checks context cancellation before and after every adapter read, but
  Go's `io.Reader` interface cannot interrupt a source that blocks forever
  inside its own `Read` method. Callers supplying potentially indefinite
  streams must use a cancellation-aware reader. This is an interface-level
  limitation rather than permission for artifact capture to become unbounded.

  Vanta revision: `01M0SEA420A728STDHY9GRR83X`.

- W04-T01 permits exactly one durable generic wait per logical attempt. This
  makes continuation identity unambiguous and avoids unsafe timestamp-based
  inference, but an executor that genuinely needs multiple suspension stages
  must use a future explicit durable sequence contract; repeatedly returning a
  second wait from the same attempt is intentionally rejected. The initial
  wait-backed executors require only one suspension stage.

  Vanta revision: `01M0SG27G6MHHC3Q89CV1E0JWE`.

- W04-T02 evaluates named transform outputs independently against one immutable
  invocation-scoped context. The current expression vocabulary has no local
  transform-output root, so one named output cannot reference a sibling output;
  workflows needing staged derivation use a second transform node. This avoids
  order-dependent semantics until a first-class immutable local-bindings
  contract is justified.

  Vanta revision: `01M0SH6BPGHX161GR17XA43W5B`.
