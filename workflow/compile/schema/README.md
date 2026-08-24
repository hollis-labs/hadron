# Workflow ExecutionPlan JSON Schema

`execution-plan.schema.json` is generated from `compile.ExecutionPlan` and
references graph-owned types in the generated workflow graph schema. Do not
edit either generated artifact by hand.

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
