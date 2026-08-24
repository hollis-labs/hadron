# ADR 0008: Typed Values and Artifacts Are the Workflow Data Plane

**Status:** Accepted<br>
**Date:** 2026-08-24

## Context

Current Hadron blueprints have typed inputs but no typed outputs. Several step
executors emit compatibility `::set-output` log lines, and the pipeline runner
scrapes run event logs to construct string outputs.

That mixes logs, data flow, and control flow. It also makes arbitrary command
output capable of steering workflow behavior if it prints marker syntax.

## Decision

Typed executor returns are the workflow data plane. Logs are operational event
streams, not data exchange.

Use one `Value` envelope for inline values and artifact references:

- small JSON-compatible values are stored inline;
- large, binary, sensitive, or long-lived values use `ArtifactRef`;
- every value carries producer metadata, media type, retention class,
  redaction class, and digest where applicable.

Secrets are opaque references resolved at adapter boundaries, for example
`secret://authority/path#field`. Hadron records secret references and
provenance, not secret material. Values and events carry redaction and
retention metadata.

## Consequences

`::set-output` is not part of target semantics. If any rewrite aid parses it,
that parsing must be explicit, scoped to a selected node/stream, and marked as
compatibility-origin data.

Typed values unlock output schemas, workflow-as-tool exposure, replay,
contract tests, redaction, retention, and data-aware UI inspection.
