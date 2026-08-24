# Verification Evidence

## Baseline

| Repository | Commit | Command | Result |
| --- | --- | --- | --- |
| Hadron | `68c435f` | `go test ./...` | pass |
| go-scheduler | `962e415` | `go test ./...` | pass |
| go-scheduler | `962e415` | `go test -race ./...` | pass |

Task-specific and wave-gate reruns are appended only after execution from the
applicable integration revision.

## W00-T02

Reviewed the complete source commit `497bdbb` and integrated it as `2b9f287`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `497bdbb` | `git diff --check b5f640b..497bdbb` | pass |
| source worktree `497bdbb` | `go test ./workflow/...` | pass |
| source worktree `497bdbb` | `go list -deps ./workflow/...` | pass; only the seven new workflow packages |
| source worktree `497bdbb` | `go test ./...` | pass |
| integration `2b9f287` | `go test ./workflow/...` | pass |
| integration `2b9f287` | `go list -deps ./workflow/...` | pass; no forbidden dependencies |
| integration `2b9f287` | `go test ./...` | pass |

## W00-T01

Reviewed the complete source commit `bb8a16f` with rename detection and
integrated it as `65a7aeb`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `bb8a16f` | `git diff --check b5f640b..bb8a16f` | pass |
| source worktree `bb8a16f` | byte-compare all `R100` moves against `b5f640b` | pass; all 17 YAML files unchanged |
| source worktree `bb8a16f` | `rg -n "examples/" docs internal examples` | reviewed; matches are archive, future workflow, planning, or historical references |
| source worktree `bb8a16f` | `go test ./...` | pass |
| integration `65a7aeb` | `git diff --check HEAD^..HEAD` | pass |
| integration `65a7aeb` | old active-path `rg` check | pass; no matches |
| integration `65a7aeb` | `go test ./...` | pass |

## W00-T03

Reviewed the complete source commit, rejected its initial heuristic stdlib
classification, and accepted amended source commit `54c7c85` as integration
commit `1fc2dbc` after exact GOROOT membership and a dotless-path regression
case were added.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `54c7c85` | `git diff --check a3a993d..54c7c85` | pass |
| source worktree `54c7c85` | `go test -v ./workflow/internal/importguard` | pass; negative fixture produced the expected boundary failure |
| source worktree `54c7c85` | `go test ./workflow/...` | pass |
| source worktree `54c7c85` | `make test` | pass |
| source worktree `54c7c85` | `go test ./...` | pass |
| integration `1fc2dbc` | `git diff --check HEAD^..HEAD` | pass |
| integration `1fc2dbc` | `go test -v ./workflow/internal/importguard` | pass |
| integration `1fc2dbc` | `make test` | pass |
| integration `1fc2dbc` | `go test ./...` | pass |

## W00-T07

Reviewed the complete public contract and state transition implementation in
source commit `51ebe8a`, including a review-requested regression expansion,
then fast-forwarded go-scheduler local `main` from `962e415` to `51ebe8a`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `51ebe8a` | `git diff --check 962e415..51ebe8a` | pass |
| source worktree `51ebe8a` | `go test ./...` | pass |
| source worktree `51ebe8a` | `go test -race ./...` | pass |
| source worktree `51ebe8a` | focused CAS/failure/configuration tests, `-count=100` | pass |
| source worktree `51ebe8a` | `go vet ./...` | pass |
| source worktree `51ebe8a` | `go list -deps ./...` | pass; no application imports |
| go-scheduler integration `51ebe8a` | `go test ./...` | pass |
| go-scheduler integration `51ebe8a` | `go test -race ./...` | pass |
| go-scheduler integration `51ebe8a` | focused CAS/exhaustion/transition tests, `-count=100` | pass |
| go-scheduler integration `51ebe8a` | `go vet ./...` | pass |
| go-scheduler integration `51ebe8a` | `go list -deps ./...` | pass; only stdlib, robfig/cron, and go-scheduler |

## W00-T04

Reviewed the complete conformance harness source commit `a2e02ca` and
integrated it as `8a2ec65`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `a2e02ca` | `git diff --check b9ca2c6..a2e02ca` | pass |
| source worktree `a2e02ca` | `go test -v ./workflow/conformance/...` | pass; all external-host suites and 12 fixtures exercised |
| source worktree `a2e02ca` | `go test ./workflow/...` | pass |
| source worktree `a2e02ca` | `make test` | pass |
| source worktree `a2e02ca` | `go test ./...` | pass |
| integration `8a2ec65` | `git diff --check HEAD^..HEAD` | pass |
| integration `8a2ec65` | `go test -v ./workflow/conformance/...` | pass |
| integration `8a2ec65` | `go test ./workflow/...` | pass |
| integration `8a2ec65` | `make test` | pass |
| integration `8a2ec65` | `go test ./...` | pass |

## W01-T01

Reviewed the complete Graph IR source commit and requested a hardening pass for
nested `SourceRef` validation and deterministic map-backed error ordering. The
amended source commit `3a0c0c4` was integrated as `7b6e712`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `3a0c0c4` | `git diff --check 873d1fc..3a0c0c4` | pass |
| source worktree `3a0c0c4` | `go test ./workflow/graph/...` | pass |
| source worktree `3a0c0c4` | `go test -race ./workflow/graph/...` | pass |
| source worktree `3a0c0c4` | `go test ./workflow/...` | pass |
| source worktree `3a0c0c4` | `go vet ./workflow/...` | pass |
| source worktree `3a0c0c4` | `go test ./...` | pass |
| source worktree `3a0c0c4` | `go list -deps ./workflow/graph/...` | pass; standard library only |
| source worktree `3a0c0c4` | W00-T03 import-guard package | not present on the task's declared dependency base; rerun after integration |
| integration `7b6e712` | `git diff --check HEAD^..HEAD` | pass |
| integration `7b6e712` | `go test ./workflow/graph/...` | pass |
| integration `7b6e712` | `go test -race ./workflow/graph/...` | pass |
| integration `7b6e712` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `7b6e712` | `go test ./workflow/...` | pass |
| integration `7b6e712` | `go vet ./workflow/...` | pass |
| integration `7b6e712` | `make test` | pass |
| integration `7b6e712` | `go test ./...` | pass |

## W00-T05

Reviewed the complete diagnostic contract and requested a hardening pass for
orphaned source columns. The amended source commit `f56ce04` was integrated as
`f0ceef1`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `f56ce04` | `git diff --check 63b8246..f56ce04` | pass |
| source worktree `f56ce04` | `go test ./workflow/diagnostic/...` | pass |
| source worktree `f56ce04` | `go test -race ./workflow/diagnostic/...` | pass |
| source worktree `f56ce04` | `go test ./workflow/...` | pass |
| source worktree `f56ce04` | `go vet ./workflow/...` | pass |
| source worktree `f56ce04` | `go test ./...` | pass |
| integration `f0ceef1` | `git diff --check HEAD^..HEAD` | pass |
| integration `f0ceef1` | `go test ./workflow/diagnostic/...` | pass |
| integration `f0ceef1` | `go test -race ./workflow/diagnostic/...` | pass |
| integration `f0ceef1` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `f0ceef1` | `go test ./workflow/...` | pass |
| integration `f0ceef1` | `go vet ./workflow/...` | pass |
| integration `f0ceef1` | `make test` | pass |
| integration `f0ceef1` | `go test ./...` | pass |

## W01-T02

Reviewed the stdlib-only schema generator and generated artifact, then
requested negative behavior coverage for closed enums and structural fields.
The amended source commit `3babe00` was integrated as `0fd8245`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `3babe00` | `git diff --check 63b8246..3babe00` | pass |
| source worktree `3babe00` | two `go generate ./workflow/graph` runs plus `git diff --exit-code` | pass; byte-stable |
| source worktree `3babe00` | `go test ./workflow/graph/...` | pass |
| source worktree `3babe00` | `go test -race ./workflow/graph/...` | pass |
| source worktree `3babe00` | `go test ./workflow/...` | pass |
| source worktree `3babe00` | `go vet ./workflow/...` | pass |
| source worktree `3babe00` | `go test ./...` | pass |
| source worktree `3babe00` | generated schema SHA-256 | `7206ac855c365166ceec176f7198f343a06824b050fb1838b36a49d2ae342b68` |
| integration `0fd8245` | `git diff --check HEAD^..HEAD` | pass |
| integration `0fd8245` | two `go generate ./workflow/graph` runs plus `git diff --exit-code` | pass; no generated drift |
| integration `0fd8245` | `go test ./workflow/graph/...` | pass |
| integration `0fd8245` | `go test -race ./workflow/graph/...` | pass |
| integration `0fd8245` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `0fd8245` | `go test ./workflow/...` | pass |
| integration `0fd8245` | `go vet ./workflow/...` | pass |
| integration `0fd8245` | `make test` | pass |
| integration `0fd8245` | `go test ./...` | pass |

## W02-T01

Reviewed the typed value, artifact, value-set, JSON, validation, and digest
contracts, then requested invalid UTF-8 rejection to preserve exact transport
round trips and object-key identity. The amended source commit `9776847` was
integrated as `10e0068`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `9776847` | `git diff --check 63b8246..9776847` | pass |
| source worktree `9776847` | `go test ./workflow/values/...` | pass |
| source worktree `9776847` | `go test -race ./workflow/values/...` | pass |
| source worktree `9776847` | `go test ./workflow/...` | pass |
| source worktree `9776847` | `go vet ./workflow/...` | pass |
| source worktree `9776847` | `go test ./...` | pass |
| integration `10e0068` | `git diff --check HEAD^..HEAD` | pass |
| integration `10e0068` | `go test ./workflow/values/...` | pass |
| integration `10e0068` | `go test -race ./workflow/values/...` | pass |
| integration `10e0068` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `10e0068` | `go test ./workflow/...` | pass |
| integration `10e0068` | `go vet ./workflow/...` | pass |
| integration `10e0068` | `make test` | pass |
| integration `10e0068` | `go test ./...` | pass |

## W01-T03

Reviewed the source loader, raw AST boundary, location indexing, diagnostic
ordering, and legacy-shape rejection in source commit `353c45d`, then
integrated it as `e598e5f`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `353c45d` | `git diff --check b2e1a85..353c45d` | pass |
| source worktree `353c45d` | `go test ./workflow/compile/...` | pass |
| source worktree `353c45d` | `go test -race ./workflow/compile/...` | pass |
| source worktree `353c45d` | `go test ./workflow/...` | pass |
| source worktree `353c45d` | `go vet ./workflow/...` | pass |
| source worktree `353c45d` | `go test ./...` | pass |
| integration `e598e5f` | `git diff --check HEAD^..HEAD` | pass |
| integration `e598e5f` | `go test ./workflow/compile/...` | pass |
| integration `e598e5f` | `go test -race ./workflow/compile/...` | pass |
| integration `e598e5f` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `e598e5f` | `go test ./workflow/...` | pass |
| integration `e598e5f` | `go vet ./workflow/...` | pass |
| integration `e598e5f` | `make test` | pass |
| integration `e598e5f` | `go test ./...` | pass |

## W00-T06

Reviewed the complete step-kind contract and registry, then requested an
amendment to avoid a diagnostic-code collision and recursively validate and
clone every accepted JSON-schema shape. The amended source commit `c8349c1`
was integrated as `0983086`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `c8349c1` | `git diff --check 71ab61d..c8349c1` | pass |
| source worktree `c8349c1` | `go test ./workflow/stepkind/... ./workflow/conformance/...` | pass |
| source worktree `c8349c1` | `go test -race ./workflow/stepkind/...` | pass |
| source worktree `c8349c1` | `go test ./workflow/...` | pass |
| source worktree `c8349c1` | `go vet ./workflow/...` | pass |
| source worktree `c8349c1` | `go test ./...` | pass |
| integration `0983086` | `git diff --check HEAD^..HEAD` | pass |
| integration `0983086` | `go test ./workflow/stepkind/... ./workflow/conformance/...` | pass |
| integration `0983086` | `go test -race ./workflow/stepkind/... ./workflow/conformance/...` | pass |
| integration `0983086` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `0983086` | `go generate ./workflow/graph` plus `git diff --exit-code` | pass; no generated drift |
| integration `0983086` | `go test ./workflow/...` | pass |
| integration `0983086` | `go vet ./workflow/...` | pass |
| integration `0983086` | `make test` | pass |
| integration `0983086` | `go test ./...` | pass |

## Wave 00 gate

All Wave 00 tasks are integrated. The gate was rerun from Hadron `0983086`
and go-scheduler `51ebe8a`.

| Repository | Command | Result |
| --- | --- | --- |
| Hadron | `go test ./workflow/stepkind/... ./workflow/conformance/...` | pass |
| Hadron | `go test -race ./workflow/stepkind/... ./workflow/conformance/...` | pass |
| Hadron | `go test -v ./workflow/internal/importguard/...` | pass |
| Hadron | `go generate ./workflow/graph` plus `git diff --exit-code` | pass; no generated drift |
| Hadron | `go test ./workflow/...` | pass |
| Hadron | `go vet ./workflow/...` | pass |
| Hadron | `make test` | pass |
| Hadron | `go test ./...` | pass |
| go-scheduler | `go test ./...` | pass |
| go-scheduler | `go test -race ./...` | pass |
| go-scheduler | `go vet ./...` | pass |

## W02-T02

Reviewed the complete expression, interpolation, reference-parsing, policy, and
cache implementation. A cache-schema mismatch for structurally heterogeneous
arrays was found during review; amended source commit `5fee6ec` makes such
arrays explicitly dynamic and adds a reversed-order shared-cache regression.
The amended commit was integrated as `985243b`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `5fee6ec` | `git diff --check 3372b64..5fee6ec` | pass |
| source worktree `5fee6ec` | `go test ./workflow/values/... ./workflow/compile/...` | pass |
| source worktree `5fee6ec` | `go test -race ./workflow/values/...` | pass |
| source worktree `5fee6ec` | focused cache, env-policy, and interpolation tests, `-count=50` | pass |
| source worktree `5fee6ec` | `go test -v ./workflow/internal/importguard/...` | pass |
| source worktree `5fee6ec` | `go test ./workflow/...` | pass |
| source worktree `5fee6ec` | `go vet ./workflow/...` | pass |
| source worktree `5fee6ec` | `make test` | pass |
| source worktree `5fee6ec` | `go test ./...` | pass |
| integration `985243b` | `git diff --check HEAD^..HEAD` | pass |
| integration `985243b` | `go test ./workflow/values/... ./workflow/compile/...` | pass |
| integration `985243b` | `go test -race ./workflow/values/...` | pass |
| integration `985243b` | focused cache, env-policy, and interpolation tests, `-count=50` | pass |
| integration `985243b` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `985243b` | `go test ./workflow/...` | pass |
| integration `985243b` | `go vet ./workflow/...` | pass |
| integration `985243b` | `make test` | pass |
| integration `985243b` | `go test ./...` | pass |

## W01-T04

Reviewed the complete compiler, plan envelope, source maps, snapshots, and
generated schema boundary. Review first required exact nested source carriers
and relocation-safe recursive digest canonicalization, then found and corrected
large-number coercion and explicit-interpolation whitespace loss. Amended source
commit `785121b` was integrated as `ee38867`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `785121b` | `git diff --check 9c4e4cc..785121b` | pass |
| source worktree `785121b` | `go generate ./workflow/graph ./workflow/compile` twice plus `git diff --exit-code` | pass; byte-stable with no generated drift |
| source worktree `785121b` | `go test -count=1 ./workflow/compile/... ./workflow/graph/...` | pass |
| source worktree `785121b` | `go test -race ./workflow/compile/...` | pass |
| source worktree `785121b` | focused digest, exact-number, and interpolation tests, `-count=50` | pass |
| source worktree `785121b` | `go test -v ./workflow/internal/importguard/...` | pass |
| source worktree `785121b` | `go test ./workflow/...` | pass |
| source worktree `785121b` | `go vet ./workflow/...` | pass |
| source worktree `785121b` | `make test` | pass |
| source worktree `785121b` | `go test ./...` | pass |
| integration `ee38867` | `git diff --check HEAD^..HEAD` | pass |
| integration `ee38867` | `go generate ./workflow/graph ./workflow/compile` plus `git diff --exit-code` | pass; no generated drift |
| integration `ee38867` | `go test -count=1 ./workflow/compile/... ./workflow/graph/...` | pass |
| integration `ee38867` | `go test -race ./workflow/compile/...` | pass |
| integration `ee38867` | focused digest, exact-number, and interpolation tests, `-count=50` | pass |
| integration `ee38867` | `go test ./workflow/values/...` | pass |
| integration `ee38867` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `ee38867` | `go test ./workflow/...` | pass |
| integration `ee38867` | `go vet ./workflow/...` | pass |
| integration `ee38867` | `make test` | pass |
| integration `ee38867` | `go test ./...` | pass |

## W01-T05

Reviewed the validation API, structural topology checks, step-kind schema and
policy seams, definition traversal, diagnostics ordering, and real graph
conformance fixtures. Production schema validation exposed the adopted JSON
Schema library's `golang.org/x/text` closure; the import guard now permits that
prefix only in the resolved dependency graph while continuing to reject direct
workflow imports. Source commit `29121b6` was integrated as `7eea197`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `29121b6` | `git diff --check fd75e5c..29121b6` | pass |
| source worktree `29121b6` | `go test -count=1 ./workflow/compile/... ./workflow/stepkind/... ./workflow/conformance/...` | pass |
| source worktree `29121b6` | `go test -race ./workflow/compile/...` | pass |
| source worktree `29121b6` | `go test -count=50 ./workflow/compile` | pass |
| source worktree `29121b6` | `go test -v ./workflow/internal/importguard/...` | pass |
| source worktree `29121b6` | `go test ./workflow/...` | pass |
| source worktree `29121b6` | `go vet ./workflow/...` | pass |
| source worktree `29121b6` | `make test` | pass |
| source worktree `29121b6` | `go test ./...` | pass |
| integration `7eea197` | `git diff --check HEAD^..HEAD` | pass |
| integration `7eea197` | `go test -count=1 ./workflow/compile/... ./workflow/stepkind/... ./workflow/conformance/...` | pass |
| integration `7eea197` | `go test -race ./workflow/compile/...` | pass |
| integration `7eea197` | `go test -count=10 ./workflow/compile` | pass |
| integration `7eea197` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `7eea197` | `go test -count=1 ./workflow/...` | pass |
| integration `7eea197` | `go vet ./workflow/...` | pass |

## W02-T04

Reviewed the runtime persistence records, high-level store interface, and
concurrent in-memory conformance fake. Review required immutable run-plan and
wait-invocation identities plus semantic `time.Time` equality for idempotent
replays and lease preservation. Amended source commit `1bc4de0` was integrated
as `2caeb82`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `1bc4de0` | `git diff --check fd75e5c..1bc4de0` | pass |
| source worktree `1bc4de0` | `go test ./workflow/runtime/... ./workflow/conformance/...` | pass |
| source worktree `1bc4de0` | `go test -race ./workflow/runtime/...` | pass |
| source worktree `1bc4de0` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `2caeb82` | `go test ./workflow/...` | pass |
| integration `2caeb82` | `go test -race ./workflow/runtime/...` | pass |
| integration `2caeb82` | `go vet ./workflow/...` | pass |
| integration `2caeb82` | `go test ./...` | pass |
