# ADR 0002: CLI and Desktop App Are Clients of Daemon State

**Status:** Accepted<br>
**Date:** 2026-05-12

## Context

The CLI and desktop app serve different workflows. The CLI is optimized for
terminal automation and scripting, while the desktop app is optimized for
interactive inspection and authoring. Both need access to the same runs,
pipelines, schedules, triggers, workspaces, settings, and telemetry.

Duplicating state management in each client would create inconsistent run
histories, competing schedule behavior, and different safety semantics depending
on how a user starts work.

## Decision

The CLI and Wails desktop app are clients of daemon state. They communicate with
`hadrond` through the daemon API contract and should treat daemon responses as
the source of truth for execution status and persisted records.

Client responsibilities are limited to:

- input collection and local path selection
- request construction and response presentation
- streaming or polling daemon state
- client-side validation that improves UX without replacing daemon validation
- desktop-only affordances such as file pickers and UI state

The clients must not maintain independent durable execution records or scheduler
state.

## Consequences

The product has one operational state model, which reduces beta support risk and
makes bugs easier to reproduce. Client tests should focus on request shape,
presentation, and error handling, while daemon tests remain responsible for
state transitions and execution semantics.

This decision increases the importance of API compatibility. Frontend and CLI
changes should be reviewed against the daemon contract rather than against local
implementation assumptions.
