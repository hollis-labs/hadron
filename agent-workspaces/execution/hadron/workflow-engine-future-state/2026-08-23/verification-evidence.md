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

## W05-T01

Reviewed the Hadron-owned workflow host, lifecycle and recovery state machine,
identity and policy journal, app-service methods, SQLite migration 0018, call
resolution journal, and atomic child-run start. Source commit `a5eb3bc` was
integrated as `2d14c13` after review-requested hardening for authorization
order, defensive plan ownership, exact cancellation replay, bounded CAS churn,
concurrent start convergence, definition drift, and startup/shutdown races.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `a5eb3bc` | focused appworkflow/persistence suites at `-count=20` and focused race suites | pass |
| source worktree `a5eb3bc` | `go test ./internal/appworkflow/... ./workflow/...` and race equivalent | pass |
| source worktree `a5eb3bc` | import guard, focused vet, golangci/pre-commit hooks, full `go test ./...`, and diff check | pass; zero new issues |
| independent source review `a5eb3bc` | `go test -count=10 ./internal/appworkflow/...` and `go test -count=1 ./internal/persistence/...` | pass |
| independent source review `a5eb3bc` | `go test -race -count=1 ./internal/appworkflow/... ./internal/persistence/...` | pass |
| independent source review `a5eb3bc` | `go test -count=1 ./workflow/internal/importguard/...` and `go vet ./internal/appworkflow/... ./internal/persistence/... ./workflow/...` | pass |
| integration `2d14c13` | `go test -count=1 ./internal/appworkflow/... ./internal/persistence/... ./workflow/adapters/call/... ./workflow/runtime/...` | pass |
| integration `2d14c13` plus tracking | `go test -count=1 ./...` | pass |
| source | `make lint-go` | baseline-only failure: unchanged intentional nil-context calls at `workflow/runtime/ready_queue_external_test.go:253,257,261` trigger standalone staticcheck SA1012 |

## W03-T08

Reviewed ordered switch/default and catch lowering, typed error expression
context, immutable decisions and terminal intents, nested/disjoint finalizer
progression, cleanup-failure terminal accounting, exact recursive
`ParentCloseCancel` trees, pinned child definitions, cancellation admission
fences, pending-child recovery ordering, and in-memory/SQLite parity. Source
commit `d771e44` was integrated as `61ea279` after review-requested replay,
recovery, and cancellation-tree hardening.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `d771e44` | focused runtime/compiler/conformance/values/host/persistence suites and selected suites at `-count=10` or `-count=20` | pass |
| source worktree `d771e44` | `go test -race -count=1 ./workflow/runtime/... ./workflow/compile/... ./workflow/conformance/... ./internal/appworkflow/... ./internal/persistence/...` | pass |
| source worktree `d771e44` | import guard, focused vet, targeted golangci, full `go test ./...`, full `go test -race ./...`, hooks, and diff checks | pass; zero new issues |
| independent source review `d771e44` | `go test -count=1 ./workflow/runtime/... ./workflow/compile/... ./workflow/conformance/... ./workflow/values/... ./internal/appworkflow/... ./internal/persistence/...` | pass |
| independent source review `d771e44` | focused runtime/host/SQLite replay and cancellation suites at repeated counts; focused race suite | pass |
| independent source review `d771e44` | import guard, focused vet, targeted golangci, and commit diff check | pass; zero issues |
| integration `61ea279` | `go test -count=1 ./...` | pass |

## W05-T03

Reviewed the Hadron-owned file, registry, and package definition resolver;
canonical exact-source and semantic plan caches; per-request authorization;
host-owned provenance; graph-native registry index; bounded tar selection; and
pinned child-run materializer. Source commit `c40bf7d` was integrated as
`c950291` after review-requested exact-selector, registry-provider bound,
whole-decompressed-stream, child cancellation-fence, and saturating-bound
hardening.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `c40bf7d` | focused definition, registry, package, child-materializer, and persistence suites at repeated counts | pass |
| source worktree `c40bf7d` | changed-scope and persistence race suites | pass |
| source worktree `c40bf7d` | import guard, focused vet, targeted golangci, full `go test ./...`, hooks, and diff checks | pass; zero new issues |
| independent source review `c40bf7d` | `go test -count=1 ./internal/appworkflow/... ./internal/registry/... ./internal/pack/... ./workflow/compile/... ./internal/persistence/...` | pass |
| independent source review `c40bf7d` | exact-key, registry-bound, PAX-bomb, child-fence, and maximum-bound tests at `-count=20` | pass |
| independent source review `c40bf7d` | focused race suite, import guard, focused vet, and commit diff check | pass |
| integration `c950291` | `go test -count=1 ./...` | pass |
| source | `make lint-go` | baseline-only failure: unchanged intentional nil-context calls at `workflow/runtime/control_flow_external_test.go:498` and `workflow/runtime/ready_queue_external_test.go:253,257,261` trigger standalone staticcheck SA1012 |

## W07-T03

Reviewed the extraction-ready `agent_session@v1` adapter and durable host
contract, deterministic `agent_launch` graph expansion, ordinary correlated
wait composition, serialized generated-child definitions, Hadron compiler
binding, container-scoped authorization, persisted plan-bundle lookup, and the
explicitly compatibility-only legacy agent bridge. Source commit `e5a9dc4` was
integrated as `2d95b0c` after host-path and durable bundle hardening.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `e5a9dc4` | focused appworkflow, persistence, agent, wait, compiler, and legacy-bridge suites at `-count=5` | pass |
| source worktree `e5a9dc4` | focused race suite across the same packages | pass |
| source worktree `e5a9dc4` | import guard, focused vet, generation stability, targeted golangci, hooks, and diff checks | pass; zero new issues |
| source worktree `e5a9dc4` | `go test -count=1 ./...` | pass; one earlier timing-sensitive legacy agent test passed on isolated, package, and subsequent full reruns |
| integration `2d95b0c` plus tracking | `go test -count=1 ./...` | pass |

## W03-T07

Reviewed atomic bounded admission across global worker, per-run, effect,
capability, named cross-run, and fan-out occupancy resources; lease-coupled
holder lifecycle and diagnostics; FIFO/fairness hooks; immutable SQLite
migration 0020; and fail-fast/run-to-completion coordination through the
existing terminal-intent and cancellation contracts. Source commit `cb3e80c`
was integrated as `6c59dbf` after projection-integrity and rollback-parity
hardening.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `cb3e80c` | focused runtime, conformance, and persistence suites at `-count=5`; targeted contention, rollback, reopen, and policy suites at higher repeated counts | pass |
| source worktree `cb3e80c` | `go test -race -count=1 ./workflow/runtime/... ./workflow/conformance/... ./internal/persistence/...` | pass |
| source worktree `cb3e80c` | import guard, focused vet, targeted golangci, hooks, and diff checks | pass; zero new issues |
| source worktree before tracking-only rebase | `go test -count=1 ./...` | pass |
| integration `6c59dbf` plus tracking | `go test -count=1 ./...` | pass |

## W04-T08

Reviewed the extraction-safe verifier registry and built-ins, source lowering
and diagnostics, runtime-issued literal activity recorder, post-output and
pre-success verification boundary, durable report/event binding, external
operation recovery and contention behavior, MCP evidence emission, and the
Hadron resolver/Host catalog seam. Source commit `1d670de` was integrated as
`92147ad` after registry freezing, generic config-schema enforcement,
required-evidence enforcement, suspension fencing, and atomic external
terminal verification hardening.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `1d670de` | focused verification, compile, runtime, step-kind, MCP, conformance, appworkflow, and persistence suites; selected regressions and contention cases at `-count=30` | pass |
| source worktree `1d670de` | broad relevant `go test -race -count=1` suite | pass |
| source worktree `1d670de` | import guard, `go vet ./...`, targeted golangci, full workflow and repository suites, hooks, and diff checks | pass; zero new issues |
| independent source review `1d670de` | `go test -count=5 ./workflow/verification/... ./workflow/compile/... ./workflow/runtime/... ./workflow/stepkind/... ./workflow/adapters/mcp/... ./workflow/conformance/... ./internal/appworkflow/... ./internal/persistence/...` | pass |
| independent source review `1d670de` | `go test -race -count=1 ./internal/appworkflow/... ./internal/persistence/... ./workflow/verification/... ./workflow/stepkind/... ./workflow/runtime/... ./workflow/compile/... ./workflow/conformance/... ./workflow/adapters/mcp/...` | pass |
| independent source review `1d670de` | `go test -v ./workflow/internal/importguard/...`; `go vet ./...`; `git diff --check 2953b3e..1d670de` | pass |
| integration `92147ad` | `go test -count=1 ./...` | pass |
| source | `make lint-go` | baseline-only failure: five unchanged `shadow` findings in `internal/persistence/workflow_state_control_flow.go:430,441,444,447,464` |

## W03-T06

Reviewed deterministic crash reconciliation and repeat policy, exact pinned
root/child/replay plan recovery, compiler-owned value visibility and atomic
input binding, control/finalizer/wait/retry restoration ordering, readiness
fixpoint convergence, atomic replay provenance and history reuse, dynamic
fan-out rebinding, restart-stable explain projections, and SQLite migration
0021. Source commit `4110fd9` was integrated as `8fb3ab0` after error-path,
canonicalization, semantic graph-integrity, and direct driver hardening.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `4110fd9` | focused runtime, persistence, and appworkflow suites at `-count=10`; selected recovery/replay/driver/SQLite/host cases at higher repeated counts | pass |
| source worktree `4110fd9` | `go test -race -count=1 ./workflow/runtime/... ./internal/persistence/... ./internal/appworkflow/...` | pass |
| source worktree `4110fd9` | import guard, focused and full vet, new-diff golangci, full repository tests, hooks, formatting, and diff checks | pass; zero new issues |
| independent source review `4110fd9` | focused runtime, persistence, and appworkflow tests; repeated recovery/replay/driver/SQLite/host cases; focused race suite | pass |
| independent source review `4110fd9` | import guard, `go vet ./...`, committed diff check, and complete public-contract/migration audit | pass |
| integration `8fb3ab0` | new-diff golangci and selected recovery/replay/driver tests at `-count=5` | pass; zero issues |
| integration `8fb3ab0` | `go test -count=1 ./...` | first run hit the known timing-sensitive legacy agent-launch outbox test; isolated `-count=10` and immediate full repository rerun passed |

## W03-T09

Reviewed compiler-scoped memo keys, append-only durable cache entries, safe
effect defaults and explicit materialization approval, exact plan/schema/source
provenance, authority-checked pinned outputs, attempt-free atomic reuse,
fan-out/replay behavior, inspection origins, and SQLite migration 0022. Source
commit `1ffda24` was integrated as `ca2a7d2` after cross-store ordering and
untrusted value-record integrity hardening.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `1ffda24` | focused runtime, persistence, compiler, conformance, and step-kind suites at repeated counts | pass |
| source worktree `1ffda24` | focused race suite, import guard, vet, targeted golangci, full repository suite, hooks, and diff checks | pass; zero new issues |
| independent source review `1ffda24` | `go test -count=10 ./workflow/runtime/... ./internal/persistence/... ./workflow/compile/... ./workflow/conformance/... ./workflow/stepkind/...` | pass |
| independent source review `1ffda24` | `go test -race -count=2 ./workflow/runtime/... ./internal/persistence/... ./workflow/compile/... ./workflow/conformance/...`; focused vet and committed diff check | pass |
| integration `ca2a7d2` | full repository suite, focused race suite, and import-boundary guard | pass |

## W05-T07

Reviewed graph-native editable suites and scaffold generation, controlled
executor mocks, ordinary-runtime qualification, signed bounded reports,
two-stage namespace authorization, immutable catalog versioning/pin/publish,
source authority/provenance, and deterministic packages. Source commit
`4a89c14` was integrated as `d717a58` after caller-ownership, defensive report
admission, aggregate effect, and post-attestor output hardening.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `4a89c14` | focused appworkflow, registry, and package suites at repeated counts | pass |
| source worktree `4a89c14` | focused race suite, import guard, vet, targeted golangci, full repository suite, hooks, and diff checks | pass; zero new issues |
| independent source review `4a89c14` | `go test -count=15 ./internal/appworkflow ./internal/registry ./internal/pack`; focused vet | pass |
| independent source review `4a89c14` | `go test -race -count=2 ./internal/appworkflow ./internal/registry ./internal/pack`; import-boundary guard and committed diff check | pass |
| integration `d717a58` | focused suites at `-count=10` and focused race suite | pass |
| integration `d717a58` | `go test -count=1 ./...` | first run hit the existing timing-sensitive child-start cancellation contention test; isolated `-count=50` and immediate full repository rerun passed |

## W07-T01

Reviewed the provider-neutral `llm@v1` step contract, closed configuration and
typed private output schemas, exact tool allowlists, policy/provider/tool
boundaries, aggregate token/byte/cost/tool budgets, streaming redaction,
schema repair/fail, literal activity evidence, safe failure taxonomy, and the
Hadron-owned `go-providers` bridge. Source commit `93667ef` was integrated as
`377490c` after exact provider-instance trust, audit/provenance bounds, output
budget charging, ambiguous result, and atomic tool-batch hardening.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `93667ef` | focused LLM adapter and provider bridge suites at repeated counts | pass |
| source worktree `93667ef` | focused race suite, import guard, vet, targeted golangci, module-tidy diff, full repository suite, hooks, and diff checks | pass; zero new issues |
| independent source review `93667ef` | `go test -count=10 ./workflow/adapters/llm/... ./internal/llmprovider/...` | pass |
| independent source review `93667ef` | `go test -race -count=2 ./workflow/adapters/llm/... ./internal/llmprovider/... ./workflow/runtime/...`; `go mod tidy -diff`; focused vet and committed diff check | pass |
| source worktree `93667ef` | `go test -count=1 ./...` | pass |
| integration `377490c` | focused repeated and race suites, module-tidy diff, and full repository suite | pass |

## W03-T08-H1

Reviewed the finalizer/output completion boundary exposed by W05-T07. Source
commit `04ba64b` was integrated as `77b65c0` with an explicit durable output
requirement on successful terminal intents, exact run-owned output-reference
validation, atomic success/output publication, failure-wins cleanup fencing,
pinned-plan recovery, replay/contention convergence, SQLite parity, and a
truthful canonical contract-runner regression.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `04ba64b` | focused finalizer/output, failure-fence, SQLite reopen, recovery, and contract-runner tests at repeated counts up to `-count=100` | pass |
| source worktree `04ba64b` | `go test -race ./workflow/runtime/... ./internal/persistence/... ./internal/appworkflow/...`; `go test -count=1 ./...`; import guard; vet; pre-commit lint; module-tidy and diff checks | pass; zero issues |
| independent source review `04ba64b` | `go test -count=5 ./workflow/runtime/... ./workflow/conformance/... ./internal/persistence/... ./internal/appworkflow/...` | pass |
| independent source review `04ba64b` | `go test -race -count=1 ./workflow/runtime/... ./internal/persistence/... ./internal/appworkflow/...`; import guard; committed diff check | pass |
| integration `77b65c0` | focused suites at `-count=3` followed by `go test -count=1 ./...` | pass |

## W05-T02

Reviewed Hadron-local versioned scope and target models, exact selectors,
closed compute/sandbox requirements, pre-policy target admission, separate
policy projections, immutable start/policy journals, and the clean-break public
JSON shape. Source commit `65e1bc0` was integrated as `300257b` after
public-metadata, malformed-plan, callback-mutation, and SQLite replay hardening.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `65e1bc0` | focused hoststate, appworkflow, and persistence suites at `-count=10` | pass |
| source worktree `65e1bc0` | focused race suite, import guard, vet, targeted golangci, full repository suite, hooks, and diff checks | pass; zero issues |
| independent source review `65e1bc0` | `go test -count=3 ./internal/appworkflow/hoststate ./internal/appworkflow ./internal/persistence/...` | pass |
| independent source review `65e1bc0` | `go test -race -count=1 ./internal/appworkflow/hoststate ./internal/appworkflow ./internal/persistence/...`; committed diff audit | pass |
| integration `300257b` | focused suites at `-count=3`, import guard, committed diff check, and `go test -count=1 ./...` | pass |

## W07-T07

Reviewed provider-neutral keyed event publication, typed private envelope
receipts, pre-side-effect policy, redacted observations, shared profiled gate
execution, configured checkpoint schemas, durable resume/restart/timeout and
cancellation behavior, unsafe authority/responder fences, compiler lowering,
and conformance fixtures. Source commit `aa01d1d` was integrated as `9431754`.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `aa01d1d` | focused emit, checkpoint, gate, wait, runtime, compiler, and conformance suites at `-count=10` | pass |
| source worktree `aa01d1d` | focused race suite, import guard, workflow vet, targeted golangci, full repository suite, hooks, and diff checks | pass; zero issues |
| independent source review `aa01d1d` | focused emit, checkpoint, shared-gate, wait, and conformance suites at `-count=3`; same packages under the race detector | pass |
| integration `9431754` | focused adapter/runtime/compiler/conformance suites at `-count=3`, import guard, and committed diff check | pass |
| integration `9431754` plus tracking `cb173aa` | `go test -count=1 ./...` after W05-T02 and W07-T07 integration | pass |

## W07-T05

Reviewed the conservative compiled/offline subset, canonical manifest and build
identity, exact node-scoped remote execution profiles, in-memory execution over
the ordinary runtime coordinators, typed CLI and single-tool MCP surfaces, and
native reproducible publication. Source commit `6348a06` was integrated as
`4d9c2c7` after arbitrary unsafe-source narrowing was restricted to the
accepted MCP/LLM exception and remote observation carried the bound profile.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `6348a06` | focused offline/build suites at `-count=10`; compile suite; focused race suite; import guard; vet; targeted golangci; module-tidy diff; hooks | pass; zero issues |
| source worktree `6348a06` | real generated CLI, stdio MCP, bound MCP/LLM, arbitrary-CWD atomic rebuild, and remote-wait kill/restart fixtures | pass |
| independent source review `6348a06` | `go test -count=1 ./...`; committed diff and worktree checks | pass; clean |
| integration `4d9c2c7` plus tracking/dispatch `210b0e5` | focused offline/build/compiler suite and `go test -count=1 ./...` | pass; initial full-suite attempt hit the unrelated timing-sensitive agentsubstrate outbox fixture, which passed at `-count=3` before the complete rerun passed |

## W05-T04

Reviewed exact activation registrations, scheduler/external fire dispatch,
stable retry identity, leased stale-claim recovery, overlap and run-ID reuse,
canonical source-local input materialization, callback-to-wait binding, safe
observer history, and the quarantined legacy scheduler adapter. Source commit
`bb2a55a` was integrated as `c68a38a` with migration 0023 after identity,
claim-expiry, retry-attempt, replacement-finalizer, payload-collision,
callback-replay, and projection-CAS hardening.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `bb2a55a` | focused activation/appworkflow/persistence/scheduler/trigger suites, including repeated `-count=10`; go-scheduler focused and race suites | pass |
| independent source review `bb2a55a` | focused packages at `-count=3`; activation-specific persistence/appworkflow/hoststate/trigger race tests; diff checks | pass |
| source worktree `bb2a55a` | import guard, vet, targeted golangci, hooks, and module-tidy review | pass; zero issues; intended local scheduler replace only |
| independent source review `bb2a55a` | `go test -count=1 ./...` | all task-affected and remaining packages pass; unrelated agentsubstrate outbox timing fixture failed once and passed separately at `-count=3` |
| integration environment | Go build-cache cleanup before the final branch gate | reclaimed 71 GB of disposable cache after a prior linker/SQLite temp-space failure; no repository or module-download data removed |
| integration `c68a38a` plus tracking/dispatch `15d55ac` | focused activation/appworkflow/persistence/scheduler/trigger suite at `-count=3`; `go test -count=1 ./...`; `go test -count=3 ./internal/agentsubstrate` | task-focused suite pass; full suite reached only the pre-existing timing-sensitive agentsubstrate outbox failure; immediate isolated package rerun passed three consecutive times |

## W07-T09

Reviewed deterministic matrix/join/sequential lowering, transactional fail-fast
admission, generated global service finalizers, launch intent before provider
acquisition, exact-key crash reacquisition, monotonic readiness/heartbeat/stop
state, exact generated-definition validation and authorization, and honest
optional gate proceed/skip semantics. Source commit `fbc877d` was integrated as
`94f03bb` with append-only migration 0024 after W05-T04's migration 0023.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `fbc877d` | focused advanced adapter/compiler/runtime/persistence suites at `-count=10`; focused race suite; generation stability; import guard; vet; golangci; staticcheck; module diff; full repository suite | pass; zero task-local issues; full suite passed on clean rerun after the known agentsubstrate fixture flake |
| independent source review `fbc877d` | focused changed packages at `-count=3`; focused race suite; `go generate ./workflow/graph ./workflow/compile`; import guard; vet; committed diff and migration review | pass; generated bytes and worktree unchanged |
| integration `94f03bb` | focused changed packages at `-count=3`; `go test -count=1 ./...`; `go test -count=3 ./internal/agentsubstrate` | task-focused suite pass; full suite reached only the recorded agentsubstrate outbox timing fixture; immediate isolated package rerun passed three consecutive times |

## W05-T06

Reviewed the read-only graph diagnostics service and shared DTOs, durable-state
explanations, exact pinned plan/source mapping, bounded SQL projections,
rendered values/events, safe lease/concurrency metadata, and migration-0023
activation attempt joins. Source commit `c8b57bc` was integrated as `ddc2030`
after credential-shaped lease owners were masked and proven absent from JSON.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `c8b57bc` | diagnostics at `-count=10`; appworkflow/persistence/API suites; focused race; import guard; vet; targeted golangci; module diff; full repository suite | pass; full suite passed on clean rerun after known agentsubstrate/runtime timing flakes |
| independent source review `c8b57bc` | focused diagnostics/appworkflow/persistence/API at `-count=3`; diagnostics/persistence race suite; committed diff, redaction, and activation-join audit | pass |
| integration `ddc2030` | focused diagnostics/appworkflow/persistence/API at `-count=3`; `go test -count=1 ./...`; `go test -count=3 ./internal/agentsubstrate` | task-focused suite pass; full suite reached only the recorded agentsubstrate outbox timing fixture; immediate isolated package rerun passed three consecutive times |

## Integration lint baseline before Wave 06 hardening

| Revision | Command | Result |
| --- | --- | --- |
| integration `1ef7d40` | `make lint` | `go vet`, `golangci-lint`, and `staticcheck` pass; `errcheck` fails on two ignored standard-library results and nine ignored `requireExpressionError` test-helper results, all present on the clean integration baseline; assigned to W06-T07 release hardening |

## W05-T08

Reviewed deterministic materialization of all compiled activation declaration
kinds, source-versus-operator authority, exact source/template/materialization
digests, atomic reconciliation and retirement, shared Host start dispatch,
qualified registration ingress, current-alias removal, and retained operational
history. Source commit `dcfb868` was integrated as `0a37129` without a migration.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `dcfb868` | focused activation lifecycle/reconciliation suites at `-count=10`; relevant packages at `-count=3`; focused race, vet, golangci, full repository, hooks, and diff checks | pass; zero task-local issues |
| independent source review `dcfb868` | `go test -count=3 ./internal/appworkflow/... ./internal/registry/... ./internal/persistence/... ./internal/scheduler/... ./internal/trigger/...` | pass |
| integration `0a37129` | `go test -count=1 ./...`; `go test -count=3 ./internal/agentsubstrate`; immediate `go test -count=1 ./...` rerun | first full run reached only the recorded agentsubstrate outbox timing fixture; the isolated package passed three consecutive times and the immediate complete rerun passed |

## W06-T01

Reviewed all seven graph-native workflow CLI commands, the shared authenticated
application-service facade, bounded daemon client, structured diagnostics and
redacted projections, truthful dry-run explanation, exact run authorization,
designated-responder resume, and policy-checked pinned start lifecycle. Source
commit `6ae32b2` was integrated as `93ab469` after rejection recovery proved
that partially bound pins cannot leave runnable or claimable work.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `6ae32b2` | focused CLI/appworkflow/persistence suites at repeated counts; boundary cases at `-count=10`; focused race suite | pass |
| source worktree `6ae32b2` | import guard, vet, targeted golangci, module diff, committed diff, and full repository suite | pass; zero task-local issues |
| independent source review `6ae32b2` | `go test -count=3 ./cmd/hadron ./internal/appworkflow ./internal/persistence`; committed contract, authorization, redaction, and pin-rejection audit | pass |
| source worktree `6ae32b2` | overlapping full/race execution followed by exact agentsubstrate fixture at `-count=10` and an uncontended full rerun | only the recorded agentsubstrate explicit-outbox timing fixture failed during overlap; isolation and the uncontended complete rerun passed |
| integration `93ab469` | focused packages at `-count=3`; focused CLI/appworkflow race suite; integration diff check | pass |
| integration `93ab469` plus tracking/dispatch `1e02394` | `go test -count=1 ./...` | pass |

## W06-T05

Reviewed the graph-first desktop run view, bounded graph and edge diagnostic
projection, redacted typed value flow, source/retry/start-policy facts,
fan-out invocation selection, durable waits and controls, concurrency and
control-decision presentation, and explicit unavailable/truncation states.
Source commit `d4cbb35` was integrated as `0fbea80`. Both available agent
contexts had no connected browser session after the required browser setup and
troubleshooting checks, so rendered screenshot and interaction QA is not
claimed.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `d4cbb35` | frontend unit tests (35/35), typecheck, Biome, ESLint, production build; `go test ./internal/rundiagnostics/... ./internal/api/...`; diff and pre-commit gates | pass; zero task-local issues |
| source worktree `d4cbb35` | Vite preview plus direct index and transformed `RunDetailPage` HTTP requests | pass |
| independent source review `d4cbb35` | `go test -count=3 ./internal/rundiagnostics/... ./internal/api/...`; `go test -race -count=1 ./internal/rundiagnostics/...`; frontend test/typecheck/lint/build; committed security, truncation, and endpoint-contract audit | pass |
| integration `0fbea80` | cherry-pick and committed diff check | pass |

## W06-T02

Reviewed the graph-native HTTP transport, shared application-service DTOs,
authenticated caller authority, definition/run hiding boundaries, bounded and
duplicate-safe JSON decoding, redacted read projections, exact action routing,
CORS, safe error envelopes, and idempotent mutations. Source commit `5bfc8c2`
was integrated as `cea2992` after cancel and rerun were hardened to reject
missing idempotency keys at the transport boundary instead of surfacing an
internal service error.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `5bfc8c2` | focused API/appworkflow suites at repeated counts; focused race; vet; targeted golangci; import guard; full repository suite; hooks and diff checks | pass; zero issues |
| independent source review `5bfc8c2` | `go test -count=3 ./cmd/hadron ./internal/api ./internal/appworkflow ./internal/rundiagnostics`; API/appworkflow race suite; hidden-resource, private-display, authority, JSON-bound, exact-route, response-bound, and safe-error audit | pass |
| independent idempotency hardening `5bfc8c2` | API at `-count=3`; API/appworkflow at `-count=10`; API/appworkflow race suite; pre-commit format/vet/golangci | pass; missing cancel/rerun keys now return structured `invalid_request` 400 responses |
| integration `cea2992` | focused API/appworkflow/diagnostics/CLI suite at `-count=3`; `go test -count=1 ./...` | pass |
| integration hardening `99de732` | API at `-count=10`; API race suite; pre-commit format/vet/golangci | pass; escaped opaque run IDs round-trip through exact two-segment action routes |

## W07-T04

Reviewed deterministic first-class OpenAPI 3.0/3.1 generated step kinds,
bounded source and local-schema resolution, fixed origins and operations,
typed parameter/body/response contracts, conservative policy-visible effects,
runtime-bound unsafe idempotency, and secret-backed bearer, Basic, and header
API-key credentials. Source commit `e52e041` was integrated as `3a88a51`
after ordinary credential-like parameters and cookie/authorization-shaped API
key channels were rejected.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `e52e041` | generated adapter at `-count=10`; race at `-count=3`; adapters/compiler at `-count=3`; import guard at `-count=10` | pass |
| source worktree `e52e041` | full repository test and vet; targeted golangci/staticcheck; schema generation; tidy/module and commit-hook checks | pass; zero task-local issues and stable generated bytes |
| independent source review `e52e041` | focused generated/http/all-adapters/compiler suite; source, credential, idempotency, policy, and expansion-bound audit | pass |
| integration `3a88a51` | `go test -count=10 ./workflow/adapters/generatedapi`; `go test -race -count=1 ./workflow/adapters/generatedapi` | pass |

## W07-T08

Reviewed named query/signal/tracked-update controls over the canonical wait
transaction, source-derived durable reactors and exact generations,
continue-as-new carry and bindability fences, bounded `durability:none`, global
failure-handler recursion control, and exact child-terminal-to-wait bridging.
Source commit `880241b` was integrated as `53217b3` with append-only migration
0025 after continuation validation proved every non-carried required or
defaulted input can bind in later generations.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `880241b` | focused values/runtime/compiler/wait/persistence/appworkflow suite; reactor/control/child/failure cases at `-count=10` | pass |
| source worktree `880241b` | focused race suite; W06 CLI/HTTP regressions; import guard; vet; targeted golangci; schema generation; full repository and diff/module checks | pass; zero task-local issues and stable generated bytes |
| independent source review `880241b` | contract, host composition, migration/CAS, recovery, timestamp, redaction, child identity, failure convergence, event ceiling, and continuation-bindability audit | pass after required/defaulted/optional continuation inputs were hardened |
| integration `53217b3` | seven-package focused suite at `-count=3`; same suite under race detector | pass |
| integration `53217b3` | `go test -count=1 ./...` | pass |

## W06-T10

Reviewed the transport-neutral browser cutover, same-origin workflow commands,
opaque run-ID escaping and mutation idempotency, embedded SPA route/cache/path
behavior, optional lifecycle-only launcher, retained xyflow authoring and run
inspection, explicit upstream registry/exposure gaps, release packaging, and
complete removal of Wails bindings and Go dependencies. Source commit
`c1580b41` was integrated as `2fdb665`; hygiene, dependency-security, and
generated-scan hardening landed as `e2ee5ee`, `225d113`, and `bc3b589`.

Live `WorkflowOperations`, `WorkflowRunReadOperations`, and HTTP authentication
composition remains assigned to W06-T06. Until then, the UI is served by the
real daemon and its request contracts are browser-tested, while uncomposed live
graph operations fail closed instead of falling back to legacy semantics.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `c1580b41` | frontend unit/typecheck/Biome/ESLint/build; focused embedded-web Go suites; vet/golangci; full repository; build/install/release packaging and HTTP smoke | pass; zero task-local issues |
| source worktree `c1580b41` | `npm run test:e2e` with installed Chromium | pass, 3/3 graph-edit, waiting-run resume, registry/exposure fact, and replay scenarios |
| integration `2fdb665` plus hardening through `bc3b589` | 41 frontend unit tests; typecheck; Biome; ESLint; Vite 8 production build; focused app/api/webui Go suite | pass |
| integration `225d113` | fresh `npm ci`; `npm audit --audit-level=low` | pass; zero vulnerabilities and no dependency overrides |
| integration `bc3b589` | repeated production build; exact Wails reference scan; `npm run test:e2e`; `go test -count=1 ./...`; committed diff/status checks | pass; byte-stable embedded bundle, 3/3 Chromium scenarios, clean full repository suite |

## W06-T03

Reviewed durable MCP principal and exposure-profile CAS records, canonical
credential digests, complete identity/profile reauthorization, exact pinned and
discoverable registry-plan binding, schema/effect projection, per-session lazy
mounts, stdio's single-client fallback, list-changed notifications, safe
redaction, typed resume/signal routing, honest asynchronous run-handle output
schemas, restart-unique invocation identities, and lossless JSON-number
handling. Source commit `f21e495` was integrated as `6a4f55f` with append-only
migration 0026 after the final raw MCP regressions proved unsafe integers and
fractional limits cannot reach workflow operations.

Live graph Host, catalog, operator, and read-service composition remains owned
by W06-T06. The production daemon composes the durable exposure boundary now
and fails graph-dependent MCP calls closed with typed unavailable results until
that switchover.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `f21e495` | focused exposure/appworkflow/MCP/persistence suites at repeated counts; boundary cases at `-count=10`; focused race at `-count=3`; workflow packages; full repository suite | pass |
| source worktree `f21e495` | `go vet ./...`; targeted golangci/staticcheck; generation, module, format, commit-hook, and diff stability | pass; zero task-local issues and no dependency/generated-artifact changes |
| independent source review `f21e495` | focused changed packages at `-count=3`; focused race; vet; gofmt and diff checks; migration, authorization, schema, session, stdio, replay, collision, and raw MCP numeric audit | pass |
| integration `6a4f55f` | focused appworkflow/MCP/persistence/webui suite at `-count=3`; focused race; `go test -count=1 ./...` | pass; one persistence cancellation-contention fixture failed once in the first aggregate run, then passed 20/20 in isolation, 3/3 in the complete aggregate rerun, and in the full repository run |

## W07-T11

Reviewed the immutable Go builder and public graph compiler, committed graph
and Hadron workflow API schemas, generated TypeScript DTO/client authority,
strict schema negotiation and compact diagnostics, host-bound agent authoring,
ordinary contract-test and registry admission, and source-format preservation.
Source commit `90d722f` was integrated as `d66cc39`. Integration hardening
`a02d858` reconciles W06-T03 exposure with W07-T11's source-local definition
identity and fails closed on catalog/plan source, provenance, or digest drift.

Ephemeral agent sources, plans, compilation flights, digest variants, and
generated bundled children are never retained in shared resolver caches.
Successful registration re-enters through the ordinary immutable registry
path. Semantic comparison uses a separately typed tooling fingerprint; it
cannot substitute for a runtime plan or definition digest.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `90d722f` | focused and full Go suites; vet; targeted golangci; graph/API generation byte check; frontend 44 tests, typecheck, Biome, ESLint, build, audit; Playwright E2E | pass; zero issues, zero vulnerabilities, stable generated bytes, 3/3 browser scenarios |
| independent source review `90d722f` | `go test -count=1 ./workflow/compile ./workflow/authoring ./workflow/graph/schema ./internal/registry ./internal/appworkflow ./internal/api`; focused race; frontend generated freshness, 44 tests, and typecheck | pass |
| integration `d66cc39` + `a02d858` | `go test -count=1 ./workflow/... ./internal/api/... ./internal/appworkflow/... ./internal/registry/...`; frontend generated freshness, 44 tests, and typecheck; exposure regression | pass |

## W06-T04

Reviewed graph-native A2A task/run binding, exact published-registry skill
projection, owner-scoped resource visibility, canonical intent idempotency,
confirmation escalation, cancel/resume semantics, append-only migration 0027,
restart recovery, and unconditional fail-closed API composition. Source commit
`e423176` was integrated as `05a97b5` after review hardening replaced random
task identities, separated public caller keys from globally unique host start
keys, removed confirmation from the immutable intent digest, made persistence
append-only, and kept policy-denied starts terminal instead of pending.

The same caller key may be reused by different owners without colliding in the
host store, while retries from one owner are bound to the complete canonical
workflow identity and request. Legacy helper compatibility remains exercised
without exposing legacy blueprint execution as target semantics.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `e423176` | focused A2A/appworkflow/registry/API/persistence suites; restart and two-handle persistence; concurrent retry, changed-intent, two-owner live-host, confirmation, migration mutation, HTTP, and legacy-helper cases | pass |
| independent source review `e423176` | identity/provenance, authorization, idempotency, state-transition, migration, registry projection, and fail-closed route audit | pass after required hardening |
| integration `05a97b5` | focused five-package suite; `go vet ./...`; committed diff/status checks | pass |
| integration `05a97b5` | `go test -count=1 ./...` | all packages passed except one known `internal/agentsubstrate` explicit-reply timing flake; the isolated failing test passed immediately on rerun |

## W05-T05

Reviewed exact plan/source/compiler capture, relocation-stable plan identity,
locator-sensitive snapshot identity, per-run immutable persistence, legacy and
generic-provider unavailable fallbacks, private source retention, bounded
cleanup, restart-safe inspection and rerun, and the safe metadata projection.
Source commit `bb72580` was integrated as `f0df790` after the shared locator
sanitizer was hardened for malformed, opaque, credential-bearing, secret, and
protocol-relative references. Migration 0028 follows the integrated A2A
migration 0027.

| Revision | Command | Result |
| --- | --- | --- |
| source worktree `bb72580` | focused appworkflow/persistence/diagnostics suites; repeated exact-snapshot and locator cases; focused race; full repository; vet; targeted golangci/staticcheck; tidy and diff checks | pass; zero task-local issues and no dependency/generated-artifact changes |
| independent source review `bb72580` | resolver/source atomicity, plan/snapshot digest separation, defensive copies, immutable persistence/link integrity, legacy recovery, cleanup retention, inspect/explain redaction, source mutation/deletion/reopen/rerun, and exposure-provenance audit | pass after protocol-relative userinfo redaction was added |
| integration `f0df790` | `go test -count=1 ./internal/appworkflow/... ./internal/persistence/... ./internal/rundiagnostics/...` | pass |
| integration `f0df790` | `go test -race ./internal/appworkflow/... ./internal/persistence/... ./internal/rundiagnostics/...`; `go test ./...` | pass |
