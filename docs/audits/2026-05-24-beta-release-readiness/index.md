# Beta Release Readiness Audit

**Date:** 2026-05-24  
**Scope:** public repo hygiene, install/build path, verification automation, and example workflow quality for Hadron's public beta posture.

## Summary

Hadron is now in much better shape for public beta than it was before the repo
cleanup and AI workflow landing, but a few release-facing issues were still
material:

1. shipped examples did not lint clean, which broke `make e2e`
2. there was no GitHub CI workflow, so verification depended on local hooks
3. hook commands used `go test ./...` and `go vet ./...`, which could traverse
   frontend `node_modules` and produce noisy or misleading results
4. the repo still contained accidental/internal artifact paths that should not
   be part of the public surface

This pass fixes those items directly.

## Changes Landed

- example blueprints now include explicit `timeout_seconds` where needed
- example metadata warnings were removed
- `hadron lint examples/` is expected to pass cleanly
- `make e2e` is unblocked by example lint noise
- `lefthook` now targets the intended Go package set instead of raw `./...`
- GitHub Actions CI now runs build, Go tests, frontend tests, typecheck, and e2e
- internal/accidental repo artifacts are removed from version control and ignored

## Remaining Risks

### 1. Source-first install is still the only supported release path

Hadron now has a clearer `make install` story, but it still lacks:

- packaged binary releases
- Homebrew packaging
- checksums/signing
- platform release notes

Impact:

- acceptable for beta
- still rough for broader adoption

### 2. Desktop app build is not yet CI-covered

Current CI verifies frontend correctness and daemon/CLI behavior, but does not
yet run a full Wails app build on macOS.

Impact:

- backend and web-layer confidence is good
- desktop packaging regressions can still slip through

### 3. Race-hardening remains a stated beta caveat

The repo guidance still calls out known `go test -race ./...` issues in trigger
handling.

Impact:

- not a blocker for open beta
- still a blocker for stronger stability claims

## Recommendation

Treat Hadron as ready for an openly developed public beta, not as a polished
binary-distributed release.

The next hardening order should be:

1. add packaged release artifacts
2. add a macOS desktop build check in CI
3. fix `go test -race` instability
