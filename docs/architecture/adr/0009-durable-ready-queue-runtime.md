# ADR 0009: Durable Ready-Queue Runtime and Waits

**Status:** Accepted<br>
**Date:** 2026-08-24

## Context

Blueprint execution is currently sequential. Pipeline execution has DAG
ordering but runs by topological levels, so a slow node can block unrelated
downstream work. `human_gate` and `message_wait` currently poll while holding
workers.

The target engine needs durable node state, immediate readiness, fan-out,
replay, and waits that release workers.

## Decision

Use a durable ready-queue scheduler over graph node invocations. The runtime
state-store contract exposes runs, node invocations, attempts, waits, values,
events, and compare-and-swap claims. Hadron SQLite is one adapter behind that
interface.

Scheduler fairness is host-configurable with FIFO as the default. The first
contract supports priority and per-run fairness hooks without requiring a
complex scheduler on day one.

Readiness uses named Airflow-style rules. `all_success` is the default; other
rules include `all_done`, `one_failed`, `all_failed`, `none_failed`, and
`always`. `if` is evaluated only as a data predicate after readiness is
satisfied.

Fan-out defaults to fail on unhandled item failure. Workflows may explicitly
tolerate failures by count or percentage. Fan-out always collects per-item
status, output, and error data for downstream handling.

Core owns generic wait semantics and a `WaitRecord` contract. Hadron may
materialize callback wakes as one-shot TTL triggers, but that is an app-service
implementation detail. Resume transitions are idempotent by `wait_id`, with an
optional caller-provided idempotency key.

## Consequences

Workers are not occupied by human gates, message waits, sleeps, callback waits,
or child-run waits. The daemon can recover from persisted run state after
restart.

Implementation work must replace level barriers, render-once data flow, and
worker-blocking waits with durable node invocation state.
