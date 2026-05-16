# Go Lint and Security Triage

**Date:** 2026-05-12
**Scope:** Go lint hardening, gosec triage, govulncheck readiness

## Summary

This pass made `make lint-go` usable as a merge gate across the Go codebase.

Completed outcomes:

- `golangci-lint` reports `0 issues`.
- `staticcheck` reports no failures.
- `errcheck` reports no failures.
- `govulncheck` reports no called vulnerabilities.
- `go test ./cmd/hadron ./cmd/hadron-app ./cmd/hadrond ./internal/... ./schemas/...` passes.

## Disposition

Fixed directly:

- Added `ReadHeaderTimeout` to the HTTP server.
- Tightened writable file permissions from `0644` to `0600` where files are local state, generated fixtures, preferences, logs, or settings.
- Tightened directory permissions from `0755` to `0750` for local state, logs, generated output, and unpacked archive directories.
- Preserved archive writer close errors so tar/gzip finalization failures are not dropped.
- Updated Go module target to `go 1.26.3` and upgraded vulnerable `golang.org/x/*` dependencies.

Documented suppressions:

- User-selected local blueprint, pipeline, pack, and settings paths are intentional local-first file operations.
- Blueprint command execution and hook execution are explicit Hadron runtime capabilities.
- Pipeline execution and file watcher cancellation are daemon-owned background lifecycles.
- Test-only gosec findings are excluded in `.golangci.yml` so the security gate focuses on production paths.

## Follow-Up

No blocking gosec or govulncheck findings remain in called code. Future changes should keep `make lint-go` green before merge.
