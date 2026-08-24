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

The root `x-workflow-boundaries` metadata identifies use at the graph-native
source-authoring boundary and as the graph component of a serialized execution
plan. The later execution-plan task can point that boundary at its wrapper type
without changing the graph definitions.
