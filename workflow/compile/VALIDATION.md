# Execution-Plan Validation

`ValidatePlan` and `ValidateGraph` run before runtime binding and require no
Hadron daemon or application packages. They validate explicit graph structure,
registered step-kind config, opt-in policy hooks, and (when supplied) resolved
call structure. Findings are stable `diagnostic.Diagnostic` values ordered by
source location, code, and message.

`ValidationOptions.StepKinds` is read-only. A node with `kind_version` uses an
exact registry lookup. An unversioned node resolves only when exactly one
version of that kind appears in `List`; zero versions is unknown, while
multiple versions require the author to pin `kind_version`. Validation does not
invent a latest-version selection rule. A nil or partial registry therefore
still produces structured unknown-kind findings.

Registered config is checked against `StepKindSpec.ConfigSchema` and then
passed to `StepKind.ValidateConfig`. JSON Schema validation uses structured
instance and keyword locations rather than parsing presentation errors.
Adapter diagnostics retain their codes and messages and receive the node source
and remediation when omitted.

Effect/retry decisions remain host-selectable through `PolicyHook`; validation
does not import a concrete policy engine. Definition resolution is similarly
optional through `DefinitionResolver`. Resolved immutable digests identify
call-cycle nodes when present; otherwise the full authority, kind, ID, locator,
and version tuple is used. Resolver failures are diagnostics at the call node,
with the referenced definition locator included when available.

Structural validation consumes only explicit `needs` and normalized `edges`.
It never parses or executes expressions, infers dependencies from `steps.*`
references, or enforces output visibility. Those typed-reference rules belong
to the value-binding pass.

## Value dependency inference

`InferValueDependencies` is the separate value-binding pass. It clones a
compiled plan, parses typed expression and interpolation carriers, adds stable
`data` edges, re-runs only topology and node-shape validation, and recomputes
the graph and plan digests. Existing `needs` and edges are preserved. A
control and data edge may share endpoints; only duplicate data edges are
coalesced.

The returned `ValueVisibilityPlan` contains direct explicit and inferred
producers, never transitive or merely completed nodes. `ScopeNodeContext`
filters an available `values.ExpressionContext` to that set and preserves the
caller's base expression policy. Fan-out `item` and `index` survive only for a
fan-out node invocation; they are unavailable while `for_each.items` is being
selected.

Transform config is the one core node config whose string leaves are defined
as expressions. Verification config remains verifier-owned and is validated
against the exact frozen verifier registry; unknown kinds fail closed with the
check source. A verifier kind must separately supply a
`VerificationExpressionExtractor` to expose typed expressions to dependency
inference; otherwise non-empty config is retained as an
`opaque_verification` deferred dependency. Core never guesses by scanning
arbitrary verifier strings. Because Graph config has only a carrier-level
source, transform and verifier findings use that nearest honest carrier plus a
deterministic semantic surface path.

Static `steps.<id>` references add data edges. Computed `steps[...]` lookups and
root-only `steps` map access do not guess producers; they are deferred and at
runtime may observe only steps already present in the direct visibility scope.
Conditional, branch-selected, and fan-out producers also carry deferred
availability metadata so absence is handled as a structured runtime value
failure rather than silently dropping the reference. Workflow outputs may read
any statically named producer without adding execution edges. Activation
expressions run before node results exist and therefore reject every `steps`
reference.
