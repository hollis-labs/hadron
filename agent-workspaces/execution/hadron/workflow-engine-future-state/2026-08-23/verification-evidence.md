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
