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
