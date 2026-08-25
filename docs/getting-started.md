# Getting started

Hadron has three graph-native entry points over one durable application host:
the `hadron workflow` CLI, the daemon-served browser UI, and `hadrond mcp`.

## 1. Install and start the daemon

Install from Homebrew or build the repository:

```sh
brew install hollis-labs/tap/hadron

# Or, from a checkout:
make build
export PATH="$PWD/bin:$PATH"
```

Stage the runnable example inside the daemon's bounded workflow source root,
then start the HTTP daemon:

```sh
install -d "$HOME/.hadron/workflows"
install -m 0600 examples/workflow/production/hello-transform.workflow.yaml \
  "$HOME/.hadron/workflows/hello-transform.workflow.yaml"
hadrond serve
```

The defaults are `http://127.0.0.1:8095`, SQLite state under `~/.hadron`, and
file workflow definitions under `~/.hadron/workflows`. A no-token request is
accepted only through the loopback local-operator boundary; unknown remote
credentials fail closed. Check host recovery and readiness with:

```sh
hadron daemon
```

## 2. Validate without starting a run

From a source checkout:

```sh
hadron workflow validate \
  "$HOME/.hadron/workflows/hello-transform.workflow.yaml"
```

Validation compiles the graph and returns structured diagnostics. It does not
create or start a durable run. Add `--json` for the typed response. Use
`hadron workflow explain` to inspect the policy-visible effects, capabilities,
target, and blast-radius facts before admission.

## 3. Start with typed input

```sh
hadron workflow run \
  "$HOME/.hadron/workflows/hello-transform.workflow.yaml" \
  --run-id hello-1 \
  --idempotency-key hello-1 \
  --input-json '{"message":"Hello, workflow"}' \
  --json
```

`--input-json` must be one bounded JSON object. `--input <path>` reads the same
typed object from disk (`-` reads stdin). Duplicate JSON keys and trailing JSON
are rejected. Unknown workflow input names are reported by canonical
application validation rather than guessed by the CLI.

The returned run handle is asynchronous. Inspect it with:

```sh
hadron workflow inspect hello-1 --json
```

Values are typed and rendered through the diagnostics projection. Secret
references remain masked. Private values require explicit authorized display;
`--reveal-private` never reveals secret material.

## 4. Use a registry definition

File references are useful for local authoring. `workflow validate`, `explain`,
and `run` accept one registry selector, either a version or a digest:

```text
namespace/name@version
namespace/name@sha256:<digest>
```

They do not accept the combined lifecycle grammar. Catalog inspection and
registry/exposure lifecycle mutations bind both version and digest using
`namespace/name@version#sha256:<digest>`.

Search and inspect authorized records with:

```sh
hadron workflow catalog search --query release --json
hadron workflow catalog inspect \
  namespace/name@version#sha256:<digest> --json
```

The lifecycle commands under `workflow author`, `workflow registry`, and
`workflow exposure` distinguish the movable `current` alias, an exact qualified
registry-version pin, and an exact exposure-profile pin. See
[Workflow authoring and operations](workflows.md).

## 5. Resume a durable wait

`sleep`, `wait_for`, `message_wait`, and `human_gate` suspend as durable waits;
they do not hold an executor goroutine. Inspect the run to obtain the safe wait
projection, then use the wait ID and correlation returned by the host:

```sh
hadron workflow resume hello-1 \
  --wait <wait-id> \
  --correlation <correlation> \
  --source <source> \
  --token-file <path> \
  --payload-json '<typed-json>' \
  --idempotency-key <key>
```

The authenticated responder and wait coordinator own authorization. Run IDs
and wait IDs are not capabilities.

## 6. Open the UI or MCP adapter

The browser UI at `http://127.0.0.1:8095/` exposes Registry, Workflow Graph,
and Runs over the generated HTTP client. For an MCP client:

```sh
hadrond mcp -token '<secret>'
```

Continue with [MCP setup](mcp-setup.md), [CLI reference](cli-reference.md), and
[Safety](safety.md).
