# MCP credential rotation

The credential issuer rotates credentials for an **existing** durable MCP
principal. It preserves the principal, profile and grants, including empty
grants. It does not bootstrap a principal or rotate Hadron's operator token.

Credential administration requires the independently authenticated operator
bearer. A workflow credential with `workflow.manage`, a browser cookie and
anonymous loopback access cannot issue, list or revoke credentials. The CLI
reads the private operator token file, rather than `HADRON_TOKEN`. Use local
HTTP or TLS; redirects are refused.

## Inspect and issue

Read the current generation and predecessor credential ID:

```sh
hadron --addr http://127.0.0.1:8787 --token-file /private/operator.token \
  auth credential list --principal-id operator:mcp-local
```

Use the returned generation and a retained credential ID in the issue request:

```sh
hadron --addr http://127.0.0.1:8787 --token-file /private/operator.token \
  auth credential issue --principal-id operator:mcp-local \
  --credential-id cred_REPLACE_WITH_LISTED_ID --expected-generation 1 \
  --idempotency-key operator-cutover-001 --overlap 15m --ttl 720h \
  --secret-file /private/new-hadron-mcp.token
```

The output path must be absolute, absent and have an existing parent. The
issuer writes the secret once to an exclusively created `0600` file, with safe
metadata on stderr. `--secret-stdout` instead requires an actual pipe; a
terminal, regular stdout file or in-memory writer refuses before issuance.
Never put the secret in argv, a transcript or a task comment.

The overlap defaults to 900 seconds and permits 0 through 86400 seconds. Each
issue caps **all currently active predecessors** at the earlier of their
existing expiry and the overlap deadline. Later issues cannot extend a prior
cap or revive expired/revoked credentials. Zero overlap immediately invalidates
the old credentials. An omitted TTL means no expiry; a supplied TTL must be
positive, at most 31536000 seconds. CLI durations must be integral seconds.

## Cut over a Tether consumer

Live rotation, catalog changes and service restarts require the separately
authorized operator cutover. The source change itself performs none of them.
During that cutover, keep the old private file until the new credential is
verified. Configure the consumer's existing `token_file` with the privately
delivered file, or atomically replace its private token file. Do not paste a
raw value into catalog YAML, environment, a shell argument or documentation.

Hadron's MCP process also authenticates its configured `--token-file` at
startup. Keep that issuer-side configuration and the consuming Tether
credential coordinated before revoking the old credential; changing only the
consumer does not update an already configured MCP process's startup input.
The new credential resolves to the same principal and accepted profile.
Revocation is enforced by subsequent credential validation; it does not claim
to terminate previously established authenticated sessions.

After the authorized cutover and bounded consumer check, list again for the
current generation and revoke the exact old ID:

```sh
hadron --addr http://127.0.0.1:8787 --token-file /private/operator.token \
  auth credential revoke --principal-id operator:mcp-local \
  --credential-id cred_REPLACE_WITH_OLD_ID --expected-generation 2 \
  --idempotency-key operator-revoke-001
```

Revocation affects only that ID. Changes to principal/profile/grant assignment
also advance the principal generation. Last-used observations do not.

## Retries and lost delivery

Retain the operation key and exact request, including its submitted generation.
An exact authenticated retry is recognized before the stale-generation guard.
It returns current status/current generation and the original
`operation_generation`, with `replayed=true`, `secret_available=false` and no
secret. A changed actor, operation or request under the same key conflicts.
The CLI still validates its private sink before calling the issuer; use an
unused path for a metadata-only retry. The retry does not create that file.

If delivery fails after commit, the credential remains listed and revocable;
the error reports only its ID. There is no secret recovery or replay. Revoke
the outstanding ID if appropriate, then use a **new authorized operation** to
issue a new value. An expired or revoked retained member can be the source of
new issuance, but is never revived itself.

## Admin HTTP contract

All routes require a current operator bearer over confidential local/TLS
transport. Authorization is checked again inside the issuer transaction,
including on retries. Responses have `Cache-Control: no-store`.

| Route | Input |
|---|---|
| `GET /v1/auth/credentials/list` | Exact `principal_id` query |
| `GET /v1/auth/credentials/audit` | Exact `principal_id` query; newest 100 secret-free records |
| `POST /v1/auth/credentials/issue` | `principal_id`, `credential_id`, `expected_generation`, `idempotency_key`, optional `overlap_seconds`, optional `ttl_seconds` |
| `POST /v1/auth/credentials/revoke` | `principal_id`, `credential_id`, `expected_generation`, `idempotency_key` |

JSON requests are bounded and refuse duplicate/unknown fields, null durations,
trailing values and non-integral/out-of-range numbers. Public mutation inputs
cannot supply actor, identity, profile, grants or raw predecessor values.

Metadata includes `principal_id`, `credential_id`, `generation`,
`operation_generation`, creation/expiry/overlap/last-used/revocation times,
`status`, `replayed` and `secret_available`. Only the first successful issue
also has the one-time `secret`. Lists contain every retained family member and
no secret or digest.

SQLite stores digests only. Generation changes, expiry caps, new/revoked IDs,
idempotency receipts and secret-free actor/target/source/time audits commit in
one transaction. Migration backfills existing principal digests into credential
families; reopen retains revocation and retry metadata. Keep the entire SQLite
database in owner-controlled backups, rather than copying individual auth
tables. Never use a backup to recover a plaintext credential.

`hadron auth credential audit --principal-id operator:mcp-local` exposes the
same bounded audit through the operator-authenticated CLI. The durable audit
retains earlier records. A new operation against an already-revoked ID returns
HTTP 409 and current safe credential metadata; an exact revoke retry succeeds
with metadata instead of repeating the mutation.
