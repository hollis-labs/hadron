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

- W02-T03 supports graph-native `artifact` admission through a direct root
  type, root-local references, and root `allOf`/`anyOf`/`oneOf` composition.
  Conditional artifact admission through `if`/`then`/`else` is intentionally
  unsupported. Inline JSON Schema conditionals remain available; expand the
  artifact-specific permission vocabulary only if a concrete authoring case
  requires it, without confusing ordinary object schemas with artifact
  envelopes.

  Vanta revision: `01M0S9GZ6QCBXJ8402PD44C4S0`.
