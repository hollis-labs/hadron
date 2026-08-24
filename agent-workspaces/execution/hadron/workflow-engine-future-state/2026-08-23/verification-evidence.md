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
