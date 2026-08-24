# Workflow ExecutionPlan JSON Schema

`execution-plan.schema.json` is generated from `compile.ExecutionPlan` and
references graph-owned types in the generated workflow graph schema. Do not
edit either generated artifact by hand.

The optional `bundled_definitions` sidecar contains complete immutable child
graphs required by generated call nodes. It is serialized with the plan so
`compile.NewBundledDefinitionResolver` can resolve them after restart without
a process-local compiler registry.

From the repository root, regenerate both schema boundaries with:

```sh
go generate ./workflow/graph ./workflow/compile
```

Then run:

```sh
go test ./workflow/graph/... ./workflow/compile/...
```

The compile schema test regenerates the plan schema in memory, rejects stale
committed bytes, and compiles it together with the graph schema as JSON Schema
Draft 2020-12. External graph references keep `graph.Graph` and its nested Go
types authoritative without duplicating their definitions in the plan schema.
