# ADR 0005: Wails App Keeps a Frontend/Backend Contract Boundary

**Status:** Accepted<br>
**Date:** 2026-05-12

## Context

The Wails app combines a Go backend and a React/Vite frontend. It needs desktop
capabilities such as file picking and local app lifecycle integration, while the
core Hadron state still belongs to `hadrond`.

Without a clear boundary, frontend pages can accumulate orchestration rules,
backend bindings can become a second API, and beta behavior can diverge between
the desktop app and CLI/API surfaces.

## Decision

The Wails app is a presentation layer over the daemon contract. The frontend is
responsible for view state, forms, navigation, and rendering. The Wails backend
may provide desktop-specific affordances, but durable Hadron mutations and state
reads should continue to flow through daemon-backed APIs.

Frontend architecture should separate:

- route/page orchestration
- reusable UI components
- API/data access helpers
- desktop-only Wails bindings
- pure utilities that can be tested without a browser

## Consequences

This keeps desktop beta behavior aligned with CLI and API behavior while still
allowing desktop-native UX. It also gives the frontend cleanup tasks a clear
target: page components should become thinner, async state should be
standardized, and reusable logic should move into testable helpers.

Wails backend changes should be reviewed for whether they support desktop UX or
accidentally create a competing Hadron backend contract.
