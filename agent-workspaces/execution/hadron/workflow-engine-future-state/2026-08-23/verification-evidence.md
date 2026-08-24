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

## W02-T07

Reviewed dependency inference across every typed expression carrier, direct
visibility scoping, deferred runtime references, source maps, relocation-safe
digests, and production conformance fixtures. Review corrected root-only
`steps` access to remain safely scoped instead of inventing a whole-map ban,
and added explicit proof that control and data edges can share endpoints.
Amended source commit `b579713` was integrated as `f990807`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `b579713` | `git diff --check 25c92aa..b579713` | pass |
| source worktree `b579713` | `go test -count=1 ./workflow/compile/... ./workflow/values/... ./workflow/conformance/...` | pass |
| source worktree `b579713` | `go test -race ./workflow/compile/... ./workflow/values/... ./workflow/conformance/...` | pass |
| source worktree `b579713` | focused compile and values tests, `-count=50` | pass |
| source worktree `b579713` | `go test -v ./workflow/internal/importguard/...` | pass |
| source worktree `b579713` | `go vet ./workflow/...` | pass |
| integration `f990807` | `go test -count=1 ./workflow/compile/... ./workflow/values/... ./workflow/conformance/...` | pass |
| integration `f990807` | `go test -race ./workflow/compile/... ./workflow/values/... ./workflow/conformance/...` | pass |
| integration `f990807` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `f990807` | `go test ./workflow/...` | pass |
| integration `f990807` | `go vet ./workflow/...` | pass |
| integration `f990807` | `go test ./...` | pass |

## W03-T01

Reviewed the complete run/node transition model, atomic attempt lifecycle,
claim fencing, append-only lifecycle events, lifecycle-bypass protection, and
recovery classification. Review required waiting nodes to release leases,
resumed attempts to acquire a fresh live claim, and attempt completion to occur
only after the node returns to `running`. Amended source commit `fe9de95` was
integrated as `67f97c8`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `fe9de95` | focused wait/resume and attempt-history tests, `-count=30` | pass |
| source worktree `fe9de95` | `go test ./workflow/runtime/... ./workflow/conformance/...` | pass |
| source worktree `fe9de95` | `go test -race ./workflow/runtime/...` | pass |
| source worktree `fe9de95` | `go test -v ./workflow/internal/importguard/...` | pass |
| source worktree `fe9de95` | `go test ./workflow/...` | pass |
| source worktree `fe9de95` | `go vet ./workflow/...` | pass |
| source worktree `fe9de95` | `go test ./...` | pass |
| integration `67f97c8` | focused wait/resume and attempt-history tests, `-count=30` | pass |
| integration `67f97c8` | `go test ./workflow/runtime/... ./workflow/conformance/...` | pass |
| integration `67f97c8` | `go test -race ./workflow/runtime/...` | pass |
| integration `67f97c8` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `67f97c8` | `go test ./workflow/...` | pass |
| integration `67f97c8` | `go vet ./workflow/...` | pass |
| integration `67f97c8` | `go test ./...` | pass |
| integration `67f97c8` | `git diff --check` | pass |

## W01-T07

Reviewed activation lowering for webhook, schedule, message, file/event, and
one-shot declarations; immutable provenance and source maps; policy vocabulary;
closed operational fields; relocation-safe digests; generated schemas; and
external-package behavior. Review aligned `deduplication_key` and lower-snake
`run_id_reuse` with the established Graph contract and rejected URI-shaped
file activation paths. Source commit `eb6d4e9` was integrated as `1285639`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `eb6d4e9` | `go generate ./workflow/graph ./workflow/compile` twice plus `git diff --exit-code` | pass; byte-stable with no generated drift |
| source worktree `eb6d4e9` | `go test -count=10 ./workflow/graph/... ./workflow/compile/...` | pass |
| source worktree `eb6d4e9` | `go test -race ./workflow/graph/... ./workflow/compile/...` | pass |
| source worktree `eb6d4e9` | `go test -v ./workflow/internal/importguard/...` | pass |
| source worktree `eb6d4e9` | `go test ./workflow/...` | pass |
| source worktree `eb6d4e9` | `go vet ./workflow/...` | pass |
| source worktree `eb6d4e9` | `go test ./...` | pass |
| integration `1285639` | `go generate ./workflow/graph ./workflow/compile` plus `git diff --exit-code` | pass; no generated drift |
| integration `1285639` | `go test -count=10 ./workflow/graph/... ./workflow/compile/...` | pass |
| integration `1285639` | `go test -race ./workflow/graph/... ./workflow/compile/...` | pass |
| integration `1285639` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `1285639` | `go test ./workflow/...` | pass |
| integration `1285639` | `go vet ./workflow/...` | pass |
| integration `1285639` | `go test ./...` | pass |
| integration `1285639` | `git diff --check` | pass |

## W03-T02

Reviewed the stateless durable-ready coordinator, deterministic FIFO ordering,
policy reordering and subsetting, claim-scoped idempotency, live replay,
generation fencing, expired-lease reclamation, and recovery behavior. Review
required claimed ready snapshots to remain visible to recovery so exact replay
survives coordinator restart and policy changes. Source commit `f0a8abb` was
integrated as `cb96ee1`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `f0a8abb` | focused FIFO, replay, restart, and lease tests, `-count=20` | pass |
| source worktree `f0a8abb` | high-contention claim test with 64 workers, `-count=50` | pass; exactly one owner |
| source worktree `f0a8abb` | `go test ./workflow/runtime/... ./workflow/conformance/...` | pass |
| source worktree `f0a8abb` | `go test -race ./workflow/runtime/...` | pass |
| source worktree `f0a8abb` | `go test -v ./workflow/internal/importguard/...` | pass |
| source worktree `f0a8abb` | `go test ./workflow/...` | pass |
| source worktree `f0a8abb` | `go vet ./workflow/...` | pass |
| source worktree `f0a8abb` | `go test ./...` | pass |
| integration `cb96ee1` | focused FIFO, replay, restart, and lease tests, `-count=20` | pass |
| integration `cb96ee1` | high-contention claim test with 64 workers, `-count=50` | pass; exactly one owner |
| integration `cb96ee1` | `go test ./workflow/runtime/... ./workflow/conformance/...` | pass |
| integration `cb96ee1` | `go test -race ./workflow/runtime/...` | pass |
| integration `cb96ee1` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `cb96ee1` | `go test ./workflow/...` | pass |
| integration `cb96ee1` | `go vet ./workflow/...` | pass |
| integration `cb96ee1` | `go test ./...` | pass |
| integration `cb96ee1` | `git diff --check` | pass |

## W01-T06

Reviewed all active graph-native examples, production compiler/inference/
validation execution, deterministic execution-plan and source-map snapshots,
and exact structured invalid-source diagnostics. The fixtures use registered
test doubles only for executor kinds and make no claim of later-wave runtime
support. Source commit `e5d3c38` was integrated as `88fb4cf`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `e5d3c38` | focused five-test acceptance suite, `-count=30` | pass |
| source worktree `e5d3c38` | `UPDATE_WORKFLOW_SNAPSHOTS=1 go test -count=1 ./workflow/compile/...` plus `git diff --exit-code` | pass; snapshots byte-stable |
| source worktree `e5d3c38` | `go test -count=1 ./workflow/compile/... ./workflow/conformance/...` | pass |
| source worktree `e5d3c38` | `go test -race ./workflow/compile/... ./workflow/conformance/...` | pass |
| source worktree `e5d3c38` | `go test -v ./workflow/internal/importguard/...` | pass |
| source worktree `e5d3c38` | `go test -count=1 ./workflow/...` | pass |
| source worktree `e5d3c38` | `go vet ./workflow/...` | pass |
| source worktree `e5d3c38` | `go test ./...` | pass |
| source worktree `e5d3c38` | `git diff --check 1285639..e5d3c38` | pass |
| integration `88fb4cf` | focused five-test acceptance suite, `-count=30` | pass |
| integration `88fb4cf` | `UPDATE_WORKFLOW_SNAPSHOTS=1 go test -count=1 ./workflow/compile/...` plus `git diff --exit-code` | pass; snapshots byte-stable |
| integration `88fb4cf` | `go test -count=1 ./workflow/compile/... ./workflow/conformance/...` | pass |
| integration `88fb4cf` | `go test -race ./workflow/compile/... ./workflow/conformance/...` | pass |
| integration `88fb4cf` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `88fb4cf` | `go test -count=1 ./workflow/...` | pass |
| integration `88fb4cf` | `go vet ./workflow/...` | pass |
| integration `88fb4cf` | `go test ./...` | pass |
| integration `88fb4cf` | `git diff --check HEAD^..HEAD` | pass |

## W02-T05

Reviewed migration 0014 and the complete Hadron-owned SQLite StateStore
adapter: workflow-specific tables, BEGIN IMMEDIATE transaction boundaries,
typed errors, lifecycle/attempt/wait fidelity, idempotency, append-only events,
recovery, values, cache/pins, and repeatable activations. Review added explicit
manual-parent-integrity proof while legacy foreign-key enforcement remains off,
plan-metadata collision protection, event immutability assertions, waiting-run
reopen fidelity, and 16-way contention across two independent handles. Source
commit `c9497ae` was integrated as `b811d23`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `c9497ae` | focused workflow-state tests, `-count=30` | pass |
| source worktree `c9497ae` | focused workflow-state tests, `-race -shuffle=on -count=5` | pass |
| source worktree `c9497ae` | `go test -count=1 ./internal/persistence/... ./workflow/conformance/...` | pass |
| source worktree `c9497ae` | `go test -race ./internal/persistence/... ./workflow/runtime/... ./workflow/conformance/...` | pass |
| source worktree `c9497ae` | `go test -v ./workflow/internal/importguard/...` | pass |
| source worktree `c9497ae` | `go test -count=1 ./workflow/...` | pass |
| source worktree `c9497ae` | `go vet ./workflow/... ./internal/persistence/...` | pass |
| source worktree `c9497ae` | `golangci-lint run --new --timeout 30s` | pass; zero issues |
| source worktree `c9497ae` | `go test ./...` | pass |
| source worktree `c9497ae` | `git diff --check 67f97c8..c9497ae` | pass |
| integration `b811d23` | focused workflow-state tests, `-count=20` | pass |
| integration `b811d23` | focused workflow-state tests, `-race -shuffle=on -count=3` | pass |
| integration `b811d23` | `go test -count=1 ./internal/persistence/... ./workflow/runtime/... ./workflow/conformance/...` | pass |
| integration `b811d23` | `go test -race ./internal/persistence/... ./workflow/runtime/... ./workflow/conformance/...` | pass |
| integration `b811d23` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `b811d23` | `go vet ./workflow/... ./internal/persistence/...` | pass |
| integration `b811d23` | `golangci-lint run --new --timeout 30s` | pass; zero issues |
| integration `b811d23` | `go test ./...` | pass |
| integration `b811d23` | `git diff --check HEAD^..HEAD` | pass |

## W03-T03

Reviewed the six readiness truth tables, dependency identity and handled-route
semantics, predicate ordering, durable skip explanations, blocked diagnostic
refreshes, scheduler conformance fixtures, and atomic wait-timeout seam. Source
commit `3321fed` was integrated as `a588285` after the Hadron SQLite adapter was
hardened to match the in-memory lifecycle contract.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `3321fed` | focused readiness/progression/timeout tests, `-count=30` | pass |
| source worktree `3321fed` | `go test -shuffle=on -count=20 ./workflow/runtime/... ./workflow/conformance/...` | pass |
| source worktree `3321fed` | `go test -race ./workflow/runtime/... ./workflow/conformance/...` | pass |
| source worktree `3321fed` | `go test -v ./workflow/internal/importguard/...` | pass |
| source worktree `3321fed` | `go vet ./workflow/...` | pass |
| source worktree `3321fed` | `go test ./...` | pass |
| integration `a588285` | `go test -shuffle=on -count=10 ./internal/persistence/... ./workflow/runtime/... ./workflow/conformance/...` | pass |
| integration `a588285` | `go test -race ./internal/persistence/... ./workflow/runtime/... ./workflow/conformance/...` | pass |
| integration `a588285` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `a588285` | `go vet ./workflow/... ./internal/persistence/...` | pass |
| integration `a588285` | `golangci-lint run --new --timeout 30s` | pass; zero issues |
| integration `a588285` | `go test ./...` | pass |
| integration `a588285` | failed agent-substrate test rerun, `-count=10`, then isolated `make test` | pass; initial parallel timeout was resource contention |
| integration `a588285` | `git diff --check` | pass |

## W02-T03

Reviewed immutable bound-run identity, input/default/schema handling, lossless
number rules, artifact and pre-enveloped inputs, expression visibility, output
envelope passthrough, complete-set publication, replay/conflict behavior, and
sanitized source-mapped diagnostics. Source commit `9dce984` was integrated as
`257a584` with the W03-T03 package documentation preserved.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `9dce984` | focused binding/schema tests, `-count=30` | pass |
| source worktree `9dce984` | `go test -race ./workflow/runtime/... ./workflow/values/...` | pass |
| source worktree `9dce984` | `go test -v ./workflow/internal/importguard/...` | pass |
| source worktree `9dce984` | `go test ./workflow/...` | pass |
| source worktree `9dce984` | `go vet ./workflow/...` | pass |
| source worktree `9dce984` | `golangci-lint run --max-issues-per-linter=0 --max-same-issues=0 ./workflow/...` | pass; zero issues |
| source worktree `9dce984` | `make test` and `go test ./...` | pass |
| integration `257a584` | `go test -shuffle=on -count=20 ./workflow/runtime/... ./workflow/values/...` | pass |
| integration `257a584` | `go test -race ./workflow/runtime/... ./workflow/values/...` | pass |
| integration `257a584` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `257a584` | `go vet ./workflow/... ./internal/persistence/...` | pass |
| integration `257a584` | `go test ./...` and `make test` | pass |
| integration `257a584` | `golangci-lint run --new --timeout 30s` | pass; zero issues |
| integration `257a584` | `git diff --check` | pass |

## W02-T06

Reviewed canonical typed secret references, ephemeral adapter resolution,
stream-safe masking, fail-closed rendering, retention planning, binding and
expression taint, and prewrite persistence enforcement. Source commit
`c8a951f` was integrated as `4648e0b` after secret-classified ArtifactRef
passthrough/derivation coverage and canonical URI identity were hardened.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `c8a951f` | focused classification tests, `-count=30` | pass |
| source worktree `c8a951f` | `go test -race ./workflow/values/... ./workflow/runtime/... ./internal/persistence/...` | pass |
| source worktree `c8a951f` | `go test -v ./workflow/internal/importguard/...` | pass; negative fixture produced the expected boundary failure |
| source worktree `c8a951f` | `go test ./workflow/... ./internal/...` | pass |
| source worktree `c8a951f` | `go vet ./workflow/... ./internal/...` | pass |
| source worktree `c8a951f` | `golangci-lint` over workflow and internal packages | pass; zero issues |
| source worktree `c8a951f` | `make test` and `go test ./...` | pass |
| integration `4648e0b` | `go test -count=1 ./workflow/values/... ./workflow/runtime/... ./internal/persistence/...` | pass |
| integration `4648e0b` | `go test -race ./workflow/values/... ./workflow/runtime/... ./internal/persistence/...` | pass |
| integration `4648e0b` | `go test -count=1 -v ./workflow/internal/importguard/...` | pass; negative fixture produced the expected boundary failure |
| integration `4648e0b` | `go vet ./workflow/... ./internal/...` | pass |
| integration `4648e0b` | `golangci-lint run --new --timeout 60s` | pass; zero issues |
| integration `4648e0b` | `go test -count=1 ./...` | pass |
| integration `4648e0b` | `git diff --check` | pass |

## W03-T05

Reviewed the generic wait model, atomic suspend/resume/timeout mutations,
activation and materialization seams, SQLite migration and restart recovery,
all six wake-source fixtures, credential handling, authorization ordering,
idempotency, timeout races, and durable event/value outcomes. Source commit
`53748d1` was integrated as `5d9fbfc` after URL credential containment,
deterministic metadata-map validation, and authorization-before-schema
hardening.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `53748d1` | focused wait idempotency/timeout and SQLite atomic/race suites, repeated | pass |
| source worktree `53748d1` | `go test -race ./workflow/wait/... ./workflow/runtime/... ./workflow/conformance/... ./internal/persistence/...` | pass |
| source worktree `53748d1` | `go test -v ./workflow/internal/importguard/...` | pass |
| source worktree `53748d1` | `go test ./workflow/... ./internal/persistence/...` | pass |
| source worktree `53748d1` | `go vet ./workflow/... ./internal/persistence/...` | pass |
| source worktree `53748d1` | `golangci-lint run --max-issues-per-linter=0 --max-same-issues=0` | pass; zero issues |
| source worktree `53748d1` | `go test ./...` and `git diff --check` | pass |
| integration `5d9fbfc` | `go test -count=1 ./workflow/wait/... ./workflow/runtime/... ./workflow/conformance/... ./internal/persistence/...` | pass |
| integration `5d9fbfc` | `go test -race -count=1 ./workflow/wait/... ./workflow/runtime/... ./workflow/conformance/... ./internal/persistence/...` | pass |
| integration `5d9fbfc` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `5d9fbfc` | `go vet ./workflow/... ./internal/persistence/...` | pass |
| integration `5d9fbfc` | failed agent-substrate test rerun, `-count=5`, then serial `go test ./... -count=1` | pass; initial concurrent full/race run caused timing contention |
| integration `5d9fbfc` | `git diff --check HEAD^..HEAD` | pass |

## W02-T08

Reviewed the extraction-ready streaming store contract, bounded inline
capture, structured and render-safe failures, local immutable filesystem
adapter, opaque approved external passthrough, two-stage authorization,
retention cleanup, root/file identity hardening, digest verification, and
secret-data boundaries. Source commit `ee94487` was integrated as `177e0e1`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `ee94487` | `go test -count=1 ./workflow/values/... ./internal/artifacts/... ./internal/persistence/...` | pass |
| source worktree `ee94487` | `go test -race ./workflow/values/... ./internal/artifacts/...` | pass |
| source worktree `ee94487` | `go test -v ./workflow/internal/importguard/...` | pass |
| source worktree `ee94487` | `go vet ./workflow/... ./internal/artifacts/...` | pass |
| source worktree `ee94487` | `go test ./... -count=1` | pass |
| source worktree `ee94487` | `git diff --check dae5001..ee94487` | pass |
| integration `177e0e1` | `go test -count=1 ./workflow/values/... ./internal/artifacts/... ./internal/persistence/...` | pass |
| integration `177e0e1` | `go test -race ./workflow/values/... ./internal/artifacts/...` | pass |
| integration `177e0e1` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `177e0e1` | `go vet ./workflow/... ./internal/artifacts/...` | pass |
| integration `177e0e1` | `go test ./... -count=1` | pass |

## Wave 02 Gate

All eight Wave 02 tasks are integrated. The gate covers typed values and
artifacts, expression/interpolation evaluation, input/output binding, inferred
data dependencies and scoped visibility, StateStore and SQLite persistence,
redaction/retention, opaque secret references, and the Hadron artifact adapter.

| Revision | Command | Result |
| --- | --- | --- |
| integration `177e0e1` | `go test -shuffle=on -count=10 ./workflow/values/... ./workflow/compile/... ./workflow/runtime/... ./workflow/conformance/... ./internal/persistence/... ./internal/artifacts/...` | pass |
| integration `177e0e1` | `go test -race -count=1 ./workflow/values/... ./workflow/compile/... ./workflow/runtime/... ./workflow/conformance/... ./internal/persistence/... ./internal/artifacts/...` | pass |
| integration `177e0e1` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `177e0e1` | `go vet ./workflow/... ./internal/artifacts/...` | pass |
| integration `177e0e1` | `go test ./... -count=1` | pass |

## W04-T01

Reviewed the immutable exact-version registry, lifecycle metadata agreement,
typed execution and schema-validation envelopes, registry-driven dispatcher,
same-attempt generic-wait continuation, durable external-operation CAS model,
SQLite migration 0016, restart recovery, cancellation intent, transient hook
errors, finalization warnings, redaction boundaries, and exact-number fidelity.
Source commit `c19faf7` was integrated as `f1ad5fa`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `c19faf7` | `go test ./workflow/stepkind/... ./workflow/runtime/... ./internal/persistence/... ./workflow/conformance/...` | pass |
| source worktree `c19faf7` | `go test -count=20 ./workflow/stepkind/... ./workflow/runtime/...` | pass |
| source worktree `c19faf7` | `go test -race ./workflow/stepkind/... ./workflow/runtime/... ./workflow/conformance/... ./internal/persistence/...` | pass |
| source worktree `c19faf7` | import guard, workflow vet, targeted golangci, full vet/test, and diff check | pass; zero new lint issues |
| integration `f1ad5fa` | `go test -count=1 ./workflow/stepkind/... ./workflow/runtime/... ./workflow/conformance/... ./workflow/values/... ./internal/persistence/...` | pass |
| integration `f1ad5fa` | `go test -shuffle=on -count=10 ./workflow/stepkind/... ./workflow/runtime/... ./internal/persistence/...` | pass |
| integration `f1ad5fa` | `go test -race -count=1 ./workflow/stepkind/... ./workflow/runtime/... ./workflow/conformance/... ./workflow/values/... ./internal/persistence/...` | pass |
| integration `f1ad5fa` | `go test -v ./workflow/internal/importguard/...` | pass |
| integration `f1ad5fa` | `go vet ./workflow/... ./internal/persistence/...` and targeted golangci | pass; zero issues |
| integration `f1ad5fa` | `go test -count=1 ./...` | pass |
| source and integration | `make lint` | baseline-only failure: unchanged intentional nil-context regression calls at `workflow/runtime/ready_queue_external_test.go:253,257,261` trigger standalone staticcheck SA1012; golangci reports zero issues |

## W04-T02

Reviewed the transform kind's flat named-expression contract, immutable
compute-only metadata, inputs-only default context, deterministic injected
steps/item/index context, environment and nondeterminism rejection, exact JSON
number preservation, typed persistable outputs, defensive copies, source-map
paths, cancellation, and concurrency behavior. Source commit `1f5b966` was
integrated as `b8ee57d`. Dynamic per-output graph schemas remain enforced by
the runtime-owned dispatcher path rather than an untruthfully closed static
kind schema.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `1f5b966` | `go test ./workflow/adapters/transform/... ./workflow/values/...` and `go test -count=30 ./workflow/adapters/transform/...` | pass |
| source worktree `1f5b966` | `go test -race ./workflow/adapters/transform/... ./workflow/values/...` | pass |
| source worktree `1f5b966` | import guard, workflow vet, targeted staticcheck/golangci, `make test`, full `go test ./...`, and diff check | pass; zero adapter issues |
| integration `b8ee57d` | `go test -count=1 ./workflow/adapters/transform/... ./workflow/values/... ./workflow/stepkind/... ./workflow/runtime/...` | pass |
| integration `b8ee57d` | `go test -race -count=1 ./workflow/adapters/transform/...` | pass |
| integration `b8ee57d` | `go test -v ./workflow/internal/importguard/...` and `go vet ./workflow/adapters/transform/...` | pass |

## W04-T05

Reviewed the SDK-neutral MCP client, descriptor, artifact, content, transport,
and structured-error contracts; conservative registry metadata; trusted and
coherent annotation refinement; config validation; immediate-boundary secret
resolution; recursive result and metadata masking; bounded inline/artifact
mapping; expected-shape enforcement; exact-number conversion; defensive
copies; and the Hadron `InternalCaller` bridge. Workflow reconnect is fenced
by a non-empty idempotency key while legacy caller behavior is preserved.
Source commit `41a5057` was integrated as `5f0c1aa`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `41a5057` | `go test -count=30 ./workflow/adapters/mcp/... ./internal/mcpadapter/...` | pass |
| source worktree `41a5057` | `go test -race ./workflow/adapters/mcp/... ./internal/mcpadapter/...` | pass |
| source worktree `41a5057` | import guard, workflow tests/vet, targeted golangci/staticcheck, full `go test ./...`, and diff check | pass; zero adapter issues |
| integration `5f0c1aa` | `go test -count=1 ./workflow/adapters/mcp/... ./workflow/adapters/transform/... ./internal/mcpadapter/... ./workflow/stepkind/... ./workflow/runtime/...` | pass |
| integration `5f0c1aa` | `go test -race -count=1 ./workflow/adapters/mcp/... ./internal/mcpadapter/...` | pass |
| integration `5f0c1aa` | `go test -v ./workflow/internal/importguard/...`, focused vet, and commit diff check | pass |

## W04-T03

Reviewed the command kind's fail-closed configuration and conservative static
metadata; structured policy-resolved executable, arguments, working directory,
capabilities, effects, and sandbox; direct no-shell/no-ambient process runner;
process-group cancellation; boundary-only secret environment; streaming
redaction and raw byte bounds; output, artifact, and operational-event capture;
exact JSON, lines, KV, and compatibility parsers; defensive copies; safe typed
failures; and artifact-store bridge. Source commit `e508945` was integrated as
`439fafe`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `e508945` | `go test -count=30 ./workflow/adapters/cmd/...` and `go test -race ./workflow/adapters/cmd/...` | pass |
| source worktree `e508945` | adapter/value/workflow/full tests, import guard, vet, targeted golangci/staticcheck, pre-commit hooks, and diff check | pass; zero cmd issues |
| integration `439fafe` | `go test -count=10 ./workflow/adapters/cmd/...` and `go test -race -count=1 ./workflow/adapters/cmd/...` | pass |
| integration `439fafe` | `go test -count=1 ./workflow/adapters/... ./workflow/values/... ./workflow/runtime/...` | pass |
| integration `439fafe` | `go test -count=1 ./workflow/internal/importguard/...`, focused vet, commit diff check, and clean-tree check | pass |

## W04-T04

Reviewed the HTTP kind's closed config and output schemas; conservative static
metadata and trusted safe-method refinement; required destination policy;
all-answer DNS authorization and one-address pinned transport; redirect,
credential, and method-rewrite policy; boundary-only secret resolution;
raw/decoded reflected-secret rejection; bounded typed inline/artifact outputs;
exact JSON and pre-redaction schema validation; and full-operation timeout and
cancellation classification. Source commit `2e233ed` was integrated as
`4ad4e14`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `2e233ed` | `go test ./workflow/adapters/http/... ./workflow/compile/...`, `-count=30`, and focused redirect `-count=30` | pass |
| source worktree `2e233ed` | `go test -race ./workflow/adapters/http/...` | pass |
| source worktree `2e233ed` | import guard, workflow vet/tests, package golangci/staticcheck, full `go test ./...`, pre-commit hooks, and diff checks | pass; zero HTTP issues |
| source worktree `2e233ed` | `make lint-go` | baseline-only failure: unchanged intentional nil-context calls at `workflow/runtime/ready_queue_external_test.go:253,257,261` trigger standalone staticcheck SA1012 |
| integration `4ad4e14` | `go test -count=20 ./workflow/adapters/http/...` and `go test -race ./workflow/adapters/http/...` | pass |
| integration `4ad4e14` | `go test ./workflow/adapters/... ./workflow/compile/... ./workflow/values/...` | pass |
| integration `4ad4e14` | `go test -v ./workflow/internal/importguard/...`, focused vet, commit diff check, and clean-tree check | pass |

## W03-T04

Reviewed effect-, error-class-, timeout-, and idempotency-aware retry admission;
checked backoff arithmetic and durable timed activation; verified cancellation
commit ordering, child-run policies, wait/external/retry/fan-out propagation,
and terminal-run write fences; reviewed runtime fan-out expansion, stable item
identity, per-item typed result recovery, tolerance, concurrency slots, SQLite
migration 0017, in-memory parity, and atomic CAS/event mutations. Source commit
`34f1ee3` was integrated as `f103b3d`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `34f1ee3` | focused retry, cancellation, fan-out, terminal-fence, identity, SQLite, and contention suites with repeated counts | pass |
| source worktree `34f1ee3` | `go test -count=10 ./workflow/runtime/... ./internal/persistence/...` | pass |
| source worktree `34f1ee3` | `go test -race ./workflow/runtime/... ./internal/persistence/...` | pass |
| source worktree `34f1ee3` | `go test -v ./workflow/internal/importguard/...` and `go vet ./workflow/...` | pass |
| source worktree `34f1ee3` | `golangci-lint run --max-issues-per-linter=0 --max-same-issues=0`, pre-commit hooks, and diff checks | pass; zero issues |
| source worktree `34f1ee3` | `make lint` | baseline-only failure: unchanged intentional nil-context calls at `workflow/runtime/ready_queue_external_test.go:253,257,261` trigger standalone staticcheck SA1012 |
| integration `f103b3d` | `go test -count=10 ./workflow/runtime/... ./internal/persistence/...` | pass |
| integration `f103b3d` | `go test -race ./workflow/runtime/... ./internal/persistence/...` | pass |
| integration `f103b3d` | `go test -v ./workflow/internal/importguard/...` and `go vet ./workflow/...` | pass |
| integration `f103b3d` | `go test -count=1 ./...` and commit diff check | pass |

## W04-T07

Reviewed the durable `sleep`, `wait_for`, `message_wait`, and `human_gate`
executors; timer wake and timeout ordering; typed continuation and failure
paths; callback and gate host contracts; canonical message bridging; restart
recovery; scheduler cleanup; cancellation races; and in-memory/SQLite parity.
Source commit `c680634` was integrated as `2229ed8`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `c680634` | focused wait, gate, runtime, message-substrate, and persistence suites with repeated counts | pass |
| source worktree `c680634` | `go test -count=10 ./workflow/wait/... ./workflow/adapters/wait/... ./workflow/gate/... ./workflow/adapters/gate/... ./workflow/runtime/... ./internal/messagesubstrate/... ./internal/persistence/...` | pass |
| source worktree `c680634` | `go test -race ./workflow/wait/... ./workflow/adapters/wait/... ./workflow/gate/... ./workflow/adapters/gate/... ./workflow/runtime/... ./internal/messagesubstrate/... ./internal/persistence/...` | pass |
| source worktree `c680634` | import guard, focused vet, targeted golangci, hooks, `make test`, full `go test ./...`, and diff check | pass; zero new issues |
| integration `2229ed8` | `go test -count=1 ./...` | pass |
| source and integration | `make lint-go` | baseline-only failure: unchanged intentional nil-context calls at `workflow/runtime/ready_queue_external_test.go:253,257,261` trigger standalone staticcheck SA1012; golangci reports zero issues |

## W07-T02

Reviewed the `script@v1` Goja contract, explicit schemas, capability-free
sandbox, static and runtime prototype hardening, source-safe diagnostics,
exact-number admission, typed private outputs, deterministic structural limits,
wall-time interruption, cancellation, and concurrent fresh-runtime behavior.
Source commit `25dabcf` was integrated as `9e52e0a`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `25dabcf` | `go test -count=20 ./workflow/adapters/script/...` | pass |
| source worktree `25dabcf` | `go test -race -count=1 ./workflow/adapters/script/... ./workflow/runtime/...` | pass |
| source worktree `25dabcf` | import guard, workflow and adapter vet, targeted golangci, pre-commit hooks, and diff checks | pass; zero new issues |
| source worktree `25dabcf` | `go test -count=1 ./...` | pass on clean rerun; one earlier concurrent run had an unrelated `internal/agentsubstrate` timeout, whose isolated rerun passed |
| source and integration | `make lint-go` | baseline-only failure: unchanged intentional nil-context calls at `workflow/runtime/ready_queue_external_test.go:253,257,261` trigger standalone staticcheck SA1012; golangci reports zero issues |

## W04-T06

Reviewed the graph-native `call@v1` contract, definition resolution and
provenance normalization, layered typed input binding, durable resolution
journal, attempt-independent replay identity, authoritative lineage and cycle
checks, inline declared outputs, asynchronous child handles, parent-close
policy propagation, compiler shape rejection, and nested real-runtime fixtures.
Source commit `d7c312f` was integrated as `8ac8dbe`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `d7c312f` | `go test -count=20 ./workflow/adapters/call/... ./workflow/runtime/... ./workflow/compile/...` | pass |
| source worktree `d7c312f` | `go test -race -count=1 ./workflow/adapters/call/... ./workflow/runtime/... ./workflow/compile/...` | pass |
| source worktree `d7c312f` | import guard, workflow vet, targeted golangci, hooks, and diff checks | pass; zero new issues |
| source worktree `d7c312f` | `go test -count=1 ./...` | pass |
| integration `8ac8dbe` | `go test -count=1 ./workflow/adapters/call/... ./workflow/adapters/script/... ./workflow/runtime/... ./workflow/compile/...` | pass |
| integration `8ac8dbe` plus tracking | `go test -count=1 ./...` | pass |
| source | `make lint-go` | baseline-only failure: unchanged intentional nil-context calls at `workflow/runtime/ready_queue_external_test.go:253,257,261` trigger standalone staticcheck SA1012; golangci reports zero issues |
