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

- W04-T05 exposes MCP tool annotations before execution but the built-in
  Hadron bridge intentionally does not trust server-self-asserted hints. Until
  W05 host composition supplies an independently approved descriptor wrapper,
  validation and policy may conservatively overstate a tool's blast radius;
  execution remains safe and functional.

  Vanta revision: `01M0SHQ180CB53GWHWCG4FAHPK`.

- W04-T03 streams artifact captures directly into a run-owned sink so child
  pipes remain bounded and drain safely. If the process later exits nonzero or
  parsing fails, that completed artifact can be left unreferenced; the W02-T08
  owner/retention cleanup path is responsible for sweeping it. The adapter
  never exposes the partial artifact as a successful typed node output.

  Vanta revision: `01M0SKA9AZ6Y6KZ93N7V1VN31F`.

- W04-T04 checks the effective operation context before and after every
  policy, resolver, secret, transport, response, and artifact boundary, so a
  collaborator cannot return a late success after deadline. A Go interface
  cannot forcibly interrupt an injected collaborator that ignores context and
  blocks forever; host implementations must honor cancellation promptly.
  Cleanup of an artifact committed by a sink before a later failed/canceled
  outcome remains the host retention layer's responsibility.

  Vanta revision: `01M0SM44GF1ASJZKC8TVX2CXZ9`.

- W03-T04 stores the aggregate fan-out `items` value with stable item status,
  structured error, and immutable output references. Lossless typed child
  values are recovered through `LoadFanOutItemResults`; the current `ValueSet`
  model does not provide a nested typed-envelope container for embedding them
  directly in the aggregate JSON value.

- Fan-out input and aggregate output value sets are persisted before the final
  atomic expansion/completion CAS. A losing concurrent coordinator can
  therefore leave an unreferenced immutable value set, but cannot create
  partial children or alter the winning aggregate; normal retention cleanup is
  responsible for sweeping unreferenced values.

  Vanta revision for both W03-T04 limitations: `01M0SMKSNPYZX8AG7Z4XDR2RWQ`.
