# Workflow Core Dependency Policy

The workflow core is the `workflow` package tree except for
`workflow/adapters`. Core packages are intended to move to a standalone module,
so their direct imports are deliberately narrow.

Allowed imports are:

- the Go standard library, including `testing`;
- other packages under `github.com/hollis-labs/hadron/workflow`, except
  `workflow/adapters`;
- `gopkg.in/yaml.v3` for workflow source parsing;
- `github.com/santhosh-tekuri/jsonschema/v6` for schema validation; and
- `github.com/expr-lang/expr` for the expression language selected by ADR 0007.

Direct workflow imports are limited to those adopted roots. The production
dependency-graph check separately permits `golang.org/x/text` as the active
transitive closure required by `github.com/santhosh-tekuri/jsonschema/v6`.
Workflow source files must not import `golang.org/x/text` directly; changing
that transitive-only allowance requires an intentional guard and policy update.

No third-party test helper is currently adopted. Adding one, or adding another
schema, expression, or extraction-safe primitive dependency, requires an
intentional update to this allowlist and its guard tests.

Core must not import Hadron host packages; app, daemon, transport, registry,
settings, or persistence packages; workflow adapters; concrete Wails, HTTP,
MCP, SQLite, model-provider, LLM, or agent SDKs; or sibling application
packages. Those dependencies belong in `workflow/adapters`, Hadron host
bindings, or the consuming application.

`go test ./workflow/internal/importguard` scans every core Go source file,
including tests and files selected only on other platforms. It also checks the
active build's production dependency graph so forbidden transitive packages do
not enter through an allowed import. Directories named `testdata` are ignored,
which keeps the deliberately forbidden test fixture out of normal builds. The
repository `make test` target includes all workflow packages and therefore runs
the guard.
