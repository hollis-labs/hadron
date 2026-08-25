# Workflow Authoring Front Ends

`workflow/authoring` is a value-style view over the canonical `graph.Graph`
contract. It does not execute plans or define a second workflow language.

Go callers build an immutable graph and then use the ordinary compiler,
dependency inference, validation, definition-resolution, and policy catalogs:

```go
built := authoring.New("release", "v1").
    Authority("project").
    Node(graph.Node{
        ID: "publish", Kind: "http", KindVersion: "v1",
        Effects: graph.EffectSet{graph.EffectMutate},
    })

result := built.Compile(ctx, authoring.CompileOptions{
    Validation: compile.ValidationOptions{StepKinds: kinds},
})
```

Every builder method returns independently owned graph data. `Compile` never
creates mutable in-flight execution state.

## Generated TypeScript

The committed graph and workflow API schemas generate
`cmd/hadron-app/frontend/src/api/generated/workflow.ts`. That file is the sole
frontend authority for workflow DTOs, schema identifiers, authoring-envelope
preflight, and daemon workflow routes. It provides:

- `createGraphAuthoringEnvelope` and `createWorkflowSourceAuthoringEnvelope`;
- `decodeAuthoringEnvelope`, with strict unknown-field, byte, depth, node, edge,
  and exact schema/version checks;
- `HadronWorkflowClient`, generated from the shared HTTP operation map.

Regenerate and verify from the repository root:

```sh
go generate ./workflow/graph ./internal/api
go test ./workflow/graph/... ./internal/api/...
```

The frontend `npm test` command includes the byte-for-byte stale-artifact gate.

## Agent ingress

`appworkflow.AgentAuthoringService` accepts a bounded raw `authoring.Envelope`.
It stages exact material only while the existing definition resolver validates
it, then uses `ContractRegistrationService` for contract tests and registry
publication. Namespace authorization, definition authorization, step-kind and
effects validation, policy hooks, provenance, and exact digests stay on those
shared paths. A request without a contract suite receives a scaffold and does
not mutate the catalog.

Schema identifiers and versions are exact, not negotiated by content sniffing.
Legacy registry records with all discriminator fields absent default only to
the historical graph-native workflow source contract. A qualified registry
name is always `<namespace>/<source-local graph ID>`; definition resolution
removes that single authority namespace before comparing `Definition.ID` and
`Graph.ID`.

`SemanticPlanFingerprint` and `SemanticPlanDocument` exist only for
cross-front-end conformance. Runtime `ExecutionPlan.Digest` and definition
digests remain bound to exact source identity.
