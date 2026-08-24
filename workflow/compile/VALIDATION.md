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
