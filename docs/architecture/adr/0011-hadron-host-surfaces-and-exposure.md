# ADR 0011: Hadron Host Surfaces and Exposure Profiles

**Status:** Accepted<br>
**Date:** 2026-08-24

## Context

Hadron has multiple operator and agent surfaces: CLI, HTTP, MCP, A2A, and the
desktop UI. Existing ADRs keep CLI and desktop as daemon clients, with the
daemon owning orchestration and persistence.

The future engine core must stay transport-agnostic, while Hadron app/service
continues to expose and operate reusable workflows.

## Decision

Hadron app/service is the reference host over the workflow core. It owns daemon
lifecycle, registry, schedules, triggers, HTTP, MCP, A2A, CLI, UI, run
inspection, exposure profiles, and operator diagnostics. It does not define a
separate workflow semantic model.

Principal and exposure profile records are Hadron-local by default. A Tether
policy adapter may become an optional authority later. MCP token/session
resolves to a principal, and the principal resolves to an exposure profile.

Direct MCP tools come from explicit exposure profiles. Default unknown callers
receive meta-tools only. Selected workflows can be pinned as first-class tools;
other allowed workflows can be discovered and lazy-loaded.

The compiled/offline subset starts conservative:

- daemon-less builds support pure, read, compute, and materialize nodes that do
  not require daemon wait services;
- MCP and LLM nodes are allowed only with explicit external config bindings;
- gates, messages, and callback waits require a remote daemon binding or are
  rejected at build time.

## Consequences

The engine core stays embeddable and transport-neutral. Hadron remains useful
as a product: the place to discover, validate, register, expose, inspect, and
operate workflows.

Tool exposure becomes profile-driven rather than flooding every agent context
with every workflow.
