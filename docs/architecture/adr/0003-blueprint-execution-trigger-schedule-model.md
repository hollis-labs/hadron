# ADR 0003: Blueprint Execution Uses One Trigger and Schedule Dispatch Path

**Status:** Accepted<br>
**Date:** 2026-05-12

## Context

Blueprint runs can be started directly by a client, by a schedule, by file or
webhook triggers, or by a pipeline stage. These entry points differ in how they
are initiated, but they all need consistent validation, settings enforcement,
run event persistence, cancellation behavior, and telemetry.

Schedules and triggers are background services that live beside the API server
inside the daemon. They should activate work, not implement separate execution
engines.

## Decision

All blueprint execution flows dispatch into the shared execution manager. Direct
run requests, schedules, triggers, and pipeline stages enqueue execution
requests that produce the same persisted run records and run events.

Schedules are responsible for claiming due work and computing next run times.
Triggers are responsible for detecting file, webhook, and TTL events. Neither
owns command execution or durable run lifecycle semantics.

## Consequences

This gives users one run history and one event model regardless of how a
blueprint was started. It also keeps safety controls centralized, which is
required for beta hardening.

The execution manager remains a critical boundary and should continue to be
split into queue/worker coordination, subprocess execution, and event emission
without changing the dispatch model.
