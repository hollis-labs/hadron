# Generated Workflow API Schema

`workflow-api.schema.json` is generated from the transport-neutral Go workflow
request/result contracts. Graph IR references the committed graph schema rather
than copying its types. The generated TypeScript owns workflow DTOs, exact
schema/version constants, strict authoring-envelope preflight, and the existing
daemon workflow route client. Regenerate both artifacts with:

```sh
go generate ./internal/api
```

The `internal/api` tests regenerate both artifacts twice in memory, require
deterministic output, and fail on stale committed bytes. Frontend `npm test`
runs that same gate before unit tests.
