# Generated Workflow API Schema (v2)

`workflow-api.schema.json` is generated from the transport-neutral Go workflow
request/result contracts, and `workflow.ts` is the TypeScript client contract
derived from it. Graph IR references the committed graph schema rather than
copying its types.

v2 is the Substrate-hosted API: operations live under `/api/workflows/...`, each
carries the token scope it requires (`hadron.run` or `hadron.manage`), and every
non-2xx reply has the flat `ApiError` body (`code`, `message`, optional
`details`). It replaces v1, which described `/v1/workflows/...` and per-route
errors that were only a code.

Regenerate both artifacts with:

```sh
go generate ./internal/api
```

The `internal/api` tests regenerate both artifacts twice in memory, require
deterministic output, and fail on stale committed bytes.
