# Deferred Findings Register

No execution findings have been deferred.

## Known limitations

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

- W04-T07 callback credentials and human-gate payload references are created
  before the runtime durably suspends the attempt. A later suspension failure
  can therefore leave an orphan; host implementations own TTL and retention
  cleanup, while the immutable idempotency contracts prevent retry-created
  duplicates.

- Optional nonblocking gate execution remains deferred to W07-T09. The shared
  contract reserves the vocabulary, but `human_gate@v1` fails closed until the
  graph compiler/runtime can lower a gate to explicit decision and skip paths.

- SQLite wait recovery currently decodes every open wait row before ordering
  the earliest `WakeAt` or `Deadline`, because `WakeAt` is carried in immutable
  `record_json` and this task intentionally adds no migration or index. This is
  acceptable for the current scale; a future store optimization can project
  and index wake time without changing the public contract.

  Vanta revision for all W04-T07 limitations: `01M0SPW0E19F8XQEN30DZVE80H`.

- W07-T02 cannot provide a byte-accurate per-VM heap ceiling because upstream
  Goja exposes interruption and call-stack limits but no runtime allocator
  quota. Its deterministic structural bounds prevent oversized data from
  crossing the adapter boundary and wall-time interruption stops ordinary
  runaway code, but a host that exposes scripts to hostile multi-tenant input
  still needs a future process-level isolation runner. The adapter makes no
  stronger sandbox claim.

  Vanta revision: `01M0SRTJXJ4Q1FKHD6CENSK6V0`.

- W04-T06 defines extraction-safe ports and a restart-serializable reference
  journal for exact call-resolution replay, but intentionally does not bind
  them to Hadron's production SQLite store or application child-run launcher.
  W05-T01 and W05-T03 own those host bindings and must preserve atomic parent
  resolution events plus exact Run/ChildRunLink/input/event replay semantics.

  Vanta revision: `01M0SSDAAJ7T6Q6FJ6Z9VENBRG`.

- W05-T03's compiled-plan/source caches and graph-native `WorkflowIndex` are
  process-local. Durable source/plan snapshots and publishing remain W05-T05
  and W05-T07 responsibilities; callers cannot treat these caches as restart
  storage.

- Child materialization saves root input values before creating the node
  because `StateStore` has no atomic value-set-plus-node operation. A lost
  `SaveValues` response can leave an immutable run-owned orphan, but recovery
  remains digest-bound and normal run retention can sweep it.

- Package `MaxSourceBytes` counts tar headers, metadata, padding, and end blocks
  as well as the selected source. The usable workflow payload is therefore
  necessarily smaller than that whole-stream bound.

  Vanta revision for all W05-T03 limitations: `01M0T44SS8Z3G7JYR7YMV7R7EC`.

- W07-T03 defines the crash-durable `SessionHost` port and restart fixtures but
  does not ship a default production local agent substrate. The existing
  agentkit bridge is process-local and fails closed after restart instead of
  claiming durability it cannot provide.

- The agent wait adapter accepts the canonical child-terminal envelope, but
  Hadron does not yet produce that wake automatically from child terminal
  transitions. W07-T08 owns the common child completion/resume producer; the
  heavy agent-launch coverage row remains planned until that integration lands.

- Durable generated-child lookup currently scans immutable host start-plan
  records. This preserves correctness without a schema migration; a future
  indexed projection may optimize large journals without changing the public
  bundle-source contract.

  Vanta revision for all W07-T03 limitations: `01M0T66GDMA1FBR5Z5HG7PK398`.

- W03-T07 resource limits are immutable after first admission in one database.
  Safe live resizing remains a future versioned, quiescence-aware host
  configuration contract; silently changing a limit while holders exist would
  break exact cross-process fencing.

- Fail-fast creates durable cancellation intents for request-cancel child
  boundaries, but exact direct-cancel propagation to a separately hosted child
  remains a Hadron host recovery responsibility because the extraction-safe
  runtime store does not own a child-definition/graph resolver port.

  Vanta revision for all W03-T07 limitations: `01M0T70VG9WK9EA81SPMXZM35Q`.

- W04-T08 keeps executor failure authoritative: failed execution activity is
  process-local and no synthetic verification report is emitted after the
  executor/provider boundary has already failed.

- Terminal external verification currently has no durable pre-suspension
  activity stream, so evidence-dependent checks fail closed after external
  recovery. A future versioned durable-evidence contract is required if an
  external adapter needs to carry literal activity across suspension.

- Competing terminal external observers and partial report persistence may
  leave unlinked immutable verification or output value sets. They never
  create duplicate applied verification histories and remain discoverable for
  retention or repair.

- W04-T08 provides the strict, provider-neutral reviewer parser and registry
  seam but no concrete LLM/reviewer provider. W07-T01 owns that provider-facing
  executor integration.

  Vanta revision for all W04-T08 limitations: `01M0T99MRAE1RFFEYBZZK2FEEJ`.

- W03-T06 does not fabricate exact pre-attempt error payloads from aggregate
  node status. Recovery and explain surfaces expose structured error context
  only when an immutable catch decision or terminal-intent error fact exists.

- Pinned recovery requires every node to carry the exact `KindVersion` needed
  to resolve frozen executor metadata. Older shorthand-authored persisted
  plans without that version fail closed; compatibility belongs in the owning
  authoring/compiler task rather than weakening recovery identity.

  Vanta revision for both W03-T06 limitations: `01M0TF0X79CJAD6S3VNNJ65P1V`.

- W07-T01 intentionally rejects all query-bearing URI provenance, fragments,
  userinfo, and credential-shaped markers or assignments, including benign
  query-bearing endpoint metadata. The stock CLI provider bridges remain
  tool-disabled because their internal tool behavior cannot prove the
  proposal-only contract; the `go-llm-contracts` bridge also rejects a positive
  cost cap because its upstream protocol does not report exact cost.

  Vanta revision: `01M0TMBNX9G3GPV39W9SAPZJ5N`.

- W07-T05's generated executable uses the validated in-memory StateStore
  implementation that currently lives in `workflow/runtime/runtimetest`.
  Behavior is covered by parity, crash/restart, fan-out, retry, and finalizer
  fixtures, and produced binaries are self-contained. Before the public engine
  boundary is frozen or extracted in W07-T06, move that implementation to a
  production-named package so generated hosts do not depend on a test-support
  namespace. Native artifact construction also intentionally requires the
  Hadron module source and Go toolchain at build time; execution does not.

  Vanta revision: `01M0TSSF037DPN0K2SJYYNRNDQ`.

- W05-T04 consumes the authorized go-scheduler contract through a repository-
  local replacement pinned to sibling commit `51ebe8a`. This is consistent
  with Hadron's existing monorepo-local dependency model, but a standalone
  checkout cannot build that activation path until the scheduler commit is
  published and the replacement is advanced to an immutable module version.

  Vanta revision: `01M0TTMZ8WJWQYXPCXBD39VA8D`.

- The legacy file-watch tests in `internal/trigger/trigger_test.go` read their
  asynchronous mock runner/store state without synchronization. The race is
  reproducible on integration revisions before W05-T04 and is unrelated to the
  new credential-free activation trigger manager; activation-specific race
  suites pass. Repair the legacy test doubles when that package's archived
  trigger path is next maintained.

  Vanta revision: `01M0TTMZH7BNKCS8JW98QQJ7SZ`.

- `internal/agentsubstrate`'s explicit-reply outbox kickoff fixture remains
  timing-sensitive under the repository-wide suite: several full runs have
  timed out while its isolated package immediately passed three consecutive
  executions. Workflow task-focused suites are unaffected. Stabilize the
  fixture's asynchronous close/outbox coordination when agent-substrate tests
  are next maintained; keep using an immediate isolated rerun to distinguish
  this known fixture failure from workflow regressions.

  Vanta revision: `01M0TVDTR35JQ92KMGBRN6K841`.

## Resolved follow-ups

- W05-T02 resolved the temporary W05-T01 identity strings with durable,
  validated, defensively owned `RunScope` and `ExecutionTarget` models in
  `300257b`; graph-native public and journal JSON contains no `workspace_id`.
  W05-T03 had already resolved the same deferred record's production
  definition resolution and child graph materialization in `c950291`.
  W05-T04 now resolves its remaining production scheduler-registration
  boundary in `c68a38a`; scope and target facts remain non-authorizing inputs
  to the registration's exact principal/exposure binding.

  Vanta resolution revision: `01M0TTNJV2YE5D3K5RZR0ZSK7Y` (supersedes
  `01M0TP5RZS4YTEVT69023297S1`).

- W05-T04 resolves W00-T07's deliberately host-owned stale-claim policy with
  durable claim expiry, transactional reclaim, immutable per-attempt results,
  stable fire identity, and CAS that rejects the old claimant both before and
  after another worker reclaims the fire.

  Vanta resolution revision: `01M0TTNJTXN9PKN2BKF1KB5FFS` (supersedes
  `01M0RY570D54BHGAMK62VXGMW4`).

- W05-T07's canonical runner exposed an ordinary-runtime completion gap for a
  workflow that has both declared outputs and finalizers. W03-T08-H1 resolved
  it in `77b65c0`: terminal intent records whether success requires outputs,
  finalizer reconciliation hands off through `ErrRunOutputsPending`, and the
  exact durable output reference is published atomically with successful
  intent completion. Cleanup failure publishes no success outputs. Nested
  finalizers, failure, replay/contention, recovery, SQLite reopen, exact-ref
  validation, and the canonical contract runner are covered.

  Vanta resolution revision: `01M0TN9MVXSZAK11EV833S8MP0` (supersedes
  `01M0TGGDCRE6Z6GQ9ZKFM1ZK5Y`).
