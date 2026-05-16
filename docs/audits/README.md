# Hadron Audits

This directory is the home for deeper review passes on Hadron.

Use it for structured audits that should stay attached to the repo over time:

- backend hardening reviews
- execution safety reviews
- API trust-boundary reviews
- frontend quality and UX audits
- coverage and verification gap reviews

## Folder format

Create one folder per audit:

`docs/audits/<YYYY-MM-DD>-<scope-slug>/`

Example:

`docs/audits/2026-04-23-execution-safety/`

Each folder should contain:

- `index.md`: scope, methodology, findings summary, recommended next steps
- one file per finding, named with sortable severity prefixes

Suggested finding filename format:

- `01-critical-<topic>.md`
- `02-high-<topic>.md`
- `03-medium-<topic>.md`
- `04-low-<topic>.md`
- `05-info-<topic>.md`

## Severity rubric

- `Critical`: unsafe or broken enough that shipping or continued work should pause
- `High`: significant correctness, safety, or maintainability risk
- `Medium`: real issue, but not immediately destabilizing
- `Low`: useful cleanup or consistency improvement
- `Info`: observations, strengths, or deferred notes

## Rules

- Keep `index.md` as the entry point; do not bury the summary in one finding file.
- One topic per finding file unless multiple observations are inseparable.
- Cross-link related findings instead of duplicating the same argument.
- Prefer evidence and exact file paths over general complaints.
- Do not use this folder for routine implementation notes; keep those in architecture docs, handoff docs, or task tracking.

## Current intent

Hadron should start using this directory for:

1. execution safety review
2. daemon/API boundary review
3. frontend quality baseline review
4. test and verification coverage review
