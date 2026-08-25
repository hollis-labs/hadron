# Workflow Graph JSON Schema

`workflow.schema.json` is generated from the Go types and enum declarations in
`workflow/graph`. Do not edit the generated file by hand.

From the repository root, regenerate it with:

```sh
go generate ./workflow/graph
```

Then run `go test ./workflow/graph/...`. The schema test regenerates in memory
and fails if the committed bytes are stale. It also compiles the document as
JSON Schema Draft 2020-12.

`go generate ./internal/api` consumes this committed document to produce the
workflow HTTP schema and TypeScript graph/authoring client. Frontends must use
that generated surface instead of copying graph DTOs by hand.

The root `x-workflow-boundaries` metadata identifies use at the graph-native
source-authoring boundary and as the graph component of the separately
generated serialized `ExecutionPlan` schema. The plan schema references these
definitions, so graph Go types remain authoritative without a graph-to-compiler
import cycle.
