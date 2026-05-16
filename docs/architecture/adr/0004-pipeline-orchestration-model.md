# ADR 0004: Pipelines Orchestrate Through the Shared Execution Manager

**Status:** Accepted<br>
**Date:** 2026-05-12

## Context

Pipelines compose multiple blueprint stages. They need orchestration state for
stage order, status, outputs, and failure handling, but each stage still runs a
blueprint under the same local execution constraints as any other run.

If pipelines execute blueprints directly, they duplicate runtime settings,
telemetry, event handling, and cancellation semantics.

## Decision

Pipeline orchestration is daemon-owned and stage execution is delegated through
the shared execution manager. The pipeline runner is responsible for resolving
pipeline definitions, determining stage order, recording pipeline/stage state,
and submitting stage runs through the common blueprint execution path.

Pipeline APIs expose pipeline-level state in addition to the underlying run
records, but they do not create a second execution engine.

## Consequences

Pipeline behavior remains compatible with direct runs, schedules, triggers, and
MCP/API usage. Users can reason about a stage as a normal blueprint run with
additional pipeline context.

Future pipeline work should improve stage-state modeling, cancellation, retries,
and output passing without bypassing the execution manager.
