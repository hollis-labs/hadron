# Access

Three questions decide what a caller can do: who is it (authentication), what may it call (scopes), and which workflows may it use (exposure profiles, decided by policy).

## Authentication

The `auth` module verifies a bearer token on every `/api/`, `/mcp` and A2A request. The mode is a dial, set per app with `HOLLIS_AUTH_HADRON` (or `HOLLIS_AUTH`, or `auth.mode`):

| Mode | Behaviour |
|---|---|
| `off` (default) | Development. A request with no verified token is the **local operator**: every scope, unrestricted. A request that does carry a valid token is that principal, with its own scopes and profile |
| `permissive` | Tokens are verified; what `enforce` would refuse is logged ("would deny") and let through |
| `enforce` | A missing, invalid or under-scoped request is refused: 401 without a valid token, 403 without the scope. Production |

`off` and `permissive` refuse to listen on anything but loopback, a tailnet address or a unix socket.

Ways to verify tokens (set under `auth` in the [configuration](configuration.md)):

```yaml
auth:
  mode: enforce
  # JWTs from your issuer (Tether):
  issuer: https://issuer.example
  jwks_url: https://issuer.example/.well-known/jwks.json
  bootstrap_key_file: /etc/hadron/issuer-key.pem      # used until the first JWKS fetch
  # and/or static service keys; the hash is the hex SHA-256 of the key, never the key
  api_keys:
    - {id: ci, hash: "<sha256 hex>", scopes: [hadron.run, hadron.mcp]}
```

`printf %s "$KEY" | shasum -a 256` produces a hash. `GET /api/me` shows the caller as the daemon sees it.

## Scopes

| Scope | Allows |
|---|---|
| `hadron.run` | Validate, explain, run, inspect, cancel, resume, signal and read runs; fire activations; the A2A card and tasks; catalog search and inspect |
| `hadron.manage` | Everything under authoring, registry and exposure; **unrestricted workflow access** (below). Give it to administrators, not to agents |
| `hadron.confirm` | Satisfy a confirmation. A person's scope: an agent never confirms, even with it |
| `hadron.mcp` | Reach the `/mcp` endpoint (over stdio the token is checked the same way) |
| `hadron.profile.<id>` | Not a permission: it names the caller's [exposure profile](#exposure-profiles) |
| `hadron.*` | Wildcard: includes `hadron.manage`, so the holder is an administrator. It names no profile |

A route or tool with no scope declared is refused in `enforce` mode, so a new endpoint is closed until someone maps it.

## Exposure profiles

A caller is either **unrestricted** or bound by a profile.

- **Unrestricted:** a principal holding `hadron.manage`, and the local operator when auth is off. They administer the catalog and can use any workflow, including file workflows.
- **Bound by a profile:** everyone else. The profile is the caller's `hadron.profile.<id>` scope. No such scope, an unknown profile, or two different ones all mean the default profile, which exposes **nothing**. A file workflow or a bare digest is never inside a profile.

A profile is a record:

```json
{
  "id": "reviewers",
  "pins": [
    {"kind": "registry", "id": "team/code-review", "version": "v3", "digest": "sha256:..."}
  ],
  "namespaces": [],
  "search_scope": "none",
  "denied_effects": ["destructive"],
  "max_direct_tools": 12
}
```

| Field | |
|---|---|
| `pins` | Exact registered versions the holder may use, and sees as typed tools |
| `search_scope` | `none`, `public` (any published workflow), `namespaces` (those in `namespaces`) or `all`. Widens what the holder may **run**; typed tools list only the pins |
| `denied_effects` | Effects the holder may not start: a start of a workflow with one of them is denied, and such workflows are not shown as tools |
| `max_direct_tools` | 1 to 256: the most typed tools the holder is shown |

Manage profiles with the CLI, MCP tools or HTTP routes:

```sh
hadron workflow exposure put reviewers.json                       # create
hadron workflow exposure inspect reviewers                        # read, with its generation
hadron workflow exposure pin reviewers 'team/code-review@v3#sha256:...' --expected-generation 1
hadron workflow exposure delete reviewers --expected-generation 2
```

Every edit names the generation it was based on, so two administrators cannot overwrite each other. Issue the holder a token whose scopes include `hadron.profile.reviewers`. Changing the profile changes what that token can do immediately; no new token is needed.

## The policy mode

Whether a profile is *applied* is a policy decision made through the framework's `policy` module, and the module's mode decides how hard:

| `policy.mode` | A denial |
|---|---|
| `shadow` (default) | Logged as would-block and **allowed** |
| `enforce` | Refused |

Run `shadow` while rolling profiles out, read the `policy` log lines for would-block decisions, then switch to `enforce`. In `enforce` a denied start fails with `policy_denied` (shown to the caller as not found, so a hidden workflow looks like a missing one), and a denied validate reports the diagnostic `HADR-HOST-021`.

> The two dials are independent. A production daemon wants `auth.mode: enforce` **and** `policy.mode: enforce`.

## Confirmation

See [Workflows](workflows.md#confirmation). In short: effects that mutate or destroy, and every `call`, need a person's `--confirm`; agents cannot confirm; unattended starts need an allow-list entry.
