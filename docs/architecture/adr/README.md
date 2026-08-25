# Architecture Decision Records

This directory records durable Hadron architecture decisions. ADRs should capture
the decision, the context that made it necessary, and the consequences for future
implementation work.

## Index

- [ADR 0001: Daemon owns orchestration and persistence](0001-daemon-owns-orchestration-and-persistence.md)
- [ADR 0002: CLI and desktop app are clients of daemon state](0002-cli-and-desktop-are-daemon-clients.md)
- [ADR 0003: Blueprint execution uses one trigger and schedule dispatch path](0003-blueprint-execution-trigger-schedule-model.md)
- [ADR 0004: Pipelines orchestrate through the shared execution manager](0004-pipeline-orchestration-model.md)
- [ADR 0005: Wails app keeps a frontend/backend contract boundary](0005-wails-layering-frontend-backend-contract.md)
- [ADR 0006: Reusable workflow engine boundary](0006-reusable-workflow-engine-boundary.md)
- [ADR 0007: Graph-native workflow source and IR](0007-graph-native-workflow-ir.md)
- [ADR 0008: Typed values and artifacts are the workflow data plane](0008-typed-values-and-artifacts.md)
- [ADR 0009: Durable ready-queue runtime and waits](0009-durable-ready-queue-runtime.md)
- [ADR 0010: Step executor registry](0010-step-executor-registry.md)
- [ADR 0011: Hadron host surfaces and exposure profiles](0011-hadron-host-surfaces-and-exposure.md)
- [ADR 0012: RunScope and ExecutionTarget are separate concepts](0012-run-scope-and-execution-target.md)
- [ADR 0013: Durable graph-visible compensation with a per-run saga ledger](0013-durable-graph-visible-compensation.md)

## Format

New ADRs should use this structure:

- `Status`: proposed, accepted, superseded, or deprecated.
- `Date`: the date the decision was accepted or last materially changed.
- `Context`: constraints and forces behind the decision.
- `Decision`: the selected architecture rule.
- `Consequences`: tradeoffs and follow-up work created by the decision.
