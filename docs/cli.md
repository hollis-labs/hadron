# CLI reference

One binary, `hadron`. The framework supplies `version`, `config`, `health` and `completion`; Hadron adds the rest.

| Command | What it does |
|---|---|
| `hadron serve` | Run the daemon: workflow runtime, `/api/workflows`, `/mcp`, A2A |
| `hadron mcp [--token-file F]` | Serve MCP over stdio for an agent that launches Hadron as a subprocess |
| `hadron workflow ...` | Operate workflows on a running daemon (below) |
| `hadron build <workflow> -o <exe>` | Build a workflow into a daemon-less executable (needs a source checkout) |
| `hadron health` | Run the health checks in-process |
| `hadron config` | Print the effective configuration, secrets redacted |
| `hadron version` | Version and build information |

`hadron mcp` runs a full runtime in its own process on the same database as the daemon; claims and leases keep the two from running the same work twice.

## `hadron workflow`

Every subcommand is one call to the daemon's `/api/workflows` routes. Global flags:

| Flag | |
|---|---|
| `--server URL` | The daemon (default `http://<httphost.addr>`) |
| `-o, --output text\|json\|yaml` | Output format (default `text`) |

The outbound token, when the daemon enforces authentication, is the one the app is configured with (`auth.token_file` or `auth.mint_url`; see [Access](access.md)). It is sent to the `--server` host only.

### Output

- **text** prints a short form: diagnostics as `severity CODE file:line:col: message`, `valid name@version (digest)`, `run <id> phase=... status=...`, a run's nodes. Commands that have no text form print the response as indented JSON.
- **json** is the daemon's response body, indented. **yaml** is the same data.
- A failure prints the diagnostics it carries, then `code (status): message`, and the command exits non-zero. In `json` and `yaml` mode the error body is printed instead.

### References

`validate`, `explain` and `run` take one reference:

| Reference | Meaning |
|---|---|
| `name.workflow.yaml`, `./x.workflow.yaml`, an absolute path, or an existing file or directory | A file workflow under the daemon's workflow directory |
| `namespace/name@version` | A registered version |
| `namespace/name@sha256:<digest>` | A registered version by digest |

Commands that change the registry or a profile take the **exact** reference `namespace/name@version#sha256:<digest>`.

### Starting and inspecting runs

| Command | Notes |
|---|---|
| `validate <ref>` | Compile and check; changes nothing |
| `explain <ref>` | Policy-check effects, capabilities, blast radius; admits no work |
| `run <ref>` | `--input FILE\|-`, `--input-json`, `--run-id`, `--idempotency-key`, `--confirm`, `--dry-run`, `--pin node={...}` |
| `inspect <run>` | Redacted durable state: nodes, attempts, waits, values |
| `events <run>` | `--after <cursor>`, `--limit`, `--wait-ms` (long poll, up to 30000) |
| `waits <run>`, `values <run>` | Waits and values of a run |
| `cancel <run>` | `--reason`, `--idempotency-key` |
| `resume <run>` | `--wait-id`, `--correlation`, `--token`, `--wake-source callback\|gate\|message`, `--payload FILE\|-` or `--payload-json` |
| `signal <run>` | `--name`, `--correlation`, `--payload` or `--payload-json`, `--confirm` |
| `rerun <run>` | `--from-node` (required), `--run-id` |

`run`, `explain`, `validate`, `inspect`, `resume`, `signal` and `rerun` also take identity selectors that can only **narrow** what the token already grants: `--principal`, `--scope-kind` with `--scope-id`, `--target-id`, `--target-kind`, `--target-capability`, `--target-label key=value`, `--target-sandbox`. They never widen it.

Ids and keys are generated when omitted. Resume and signal payloads are typed values, not bare JSON.

### Catalog, authoring, registry

| Command | |
|---|---|
| `search [query] --namespace NS` | Ranked catalog search in one namespace (`--limit`) |
| `inspect-version <exact-ref>` | One registered version |
| `author validate\|scaffold --draft F` | Check a draft; generate a contract-test scaffold |
| `author test\|register --draft F --suite F` | Run the contract suite; register (`--make-current`) |
| `registry pin\|unpin\|publish\|clear-current <exact-ref>` | Registry state of one exact version |
| `registry package <exact-ref> --suite F` | Package a version with its suite |

### Exposure profiles

| Command | |
|---|---|
| `exposure put <profile.json> [--expected-generation N]` | Create (generation 0) or replace a profile |
| `exposure inspect <profile-id>` | Read a profile and its generation |
| `exposure pin\|unpin <profile-id> <exact-ref> --expected-generation N` | Add or remove one pinned workflow |
| `exposure delete <profile-id> --expected-generation N` | Delete a profile |

Edits take the generation you read, so two people editing a profile cannot overwrite each other. See [Access](access.md).
