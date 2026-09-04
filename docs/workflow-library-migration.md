# Workflow library migration

Hadron consumes `github.com/hollis-labs/go-workflow` at exact release
`v0.1.0`. The former in-repository `workflow/` tree was extracted without
changing package names below the module root or changing graph, plan,
authoring, value, wait, runtime-event, and offline-manifest schema identifiers.

Hadron remains the product host. It owns SQLite persistence, authenticated
identity and policy, registry publication, durable workers and timers,
artifacts, application lifecycle, HTTP, MCP, A2A, CLI, and daemon composition.
The shared module owns graph/source contracts, compilation, runtime state
machines, waits, values, step-kind contracts and adapters, offline execution,
generated graph schema, public API snapshot, and conformance fixtures.

Consumer rules:

- import reusable packages from `github.com/hollis-labs/go-workflow/...`;
- pin a released version in `go.mod` without a local `replace`;
- never restore `github.com/hollis-labs/hadron/workflow/...` or a local
  `workflow/` implementation;
- generate only Hadron-owned HTTP schema and TypeScript artifacts with
  `make generate`; graph schema generation belongs to go-workflow; and
- run Hadron's production SQLite-store `conformance.RunExhaustive` adoption
  gate before upgrading the dependency.

An upgrade must first pass go-workflow's own schema, public-API, import,
conformance, race, and module-proxy checks. Then update Hadron's exact version,
run `go mod tidy`, regenerate Hadron-owned API artifacts, and verify that no
wire/schema identifier or checked-in generated artifact changed unexpectedly.
