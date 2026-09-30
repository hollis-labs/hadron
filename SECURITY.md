# Security policy

## Supported versions

Hadron is pre-1.0 software with beta releases. Security fixes are made on
`main` and in the newest tagged release, when one exists. Older releases may
not receive backports.

## Report a vulnerability

Do not include an exploit, token, database, workflow definition containing
secrets, or other sensitive material in a public issue.

Use GitHub's private vulnerability-reporting flow when the repository's
Security tab offers it. If it is unavailable, contact a repository maintainer
privately through a contact channel published on the Hollis Labs organization
or maintainer profile. Include:

- the affected commit or version and operating system
- the transport involved (CLI, HTTP, UI, MCP, A2A, schedule, activation)
- whether the listener was loopback or remote
- reproduction steps and the security impact
- whether credentials or workflow data may have been exposed
- a safe way to contact you about coordination

Maintainers will acknowledge a private report, investigate it, and coordinate
disclosure; response times are best effort during the beta.

## Deployment boundary

The safe default is a single-user machine with `hadrond` listening on
`127.0.0.1:8095`. In that mode a request with no token is accepted only through
the loopback local-operator boundary, and cross-origin or DNS-rebinding-shaped
requests are rejected. Durable bearer credentials resolve to principal and
profile records; unknown credentials fail closed.

- Hadron does not provide TLS. A bearer token sent over plaintext HTTP can be
  stolen by anyone who can observe the connection. If you expose the listener
  beyond loopback, put it behind TLS, a trusted reverse proxy, a VPN, or an SSH
  tunnel, and restrict it with firewall rules.
- MCP stores only token digests and binds each session to one durable
  principal and exposure profile. Raw tokens are shown or supplied once; do not
  commit them to MCP configuration, shell history, issue reports, or logs.
- Every transport authorizes at the shared application-host boundary. A CLI
  flag, request body, task/run ID or wait ID is never proof of authority.
  Hidden definitions and runs return the same not-found shape as missing ones.
- `script@v1` runs bounded deterministic JavaScript in an in-process goja
  sandbox. Other effects are declared per step kind and checked against the
  host's policy; the production policy requires confirmation for mutating,
  destructive or unresolved-call effects. Still treat who may submit or approve
  workflows as a sensitive permission. The full model is in
  [`docs/safety.md`](docs/safety.md).
- Schedules, triggers and reactors have nobody to confirm, so such starts are
  refused unless the operator allow-list (`workflow-unattended.json` in the
  data dir, edited only by `hadron workflow unattended`) pins that exact plan
  digest. No network surface can read or write the list; the daemon refuses it
  if it is group- or world-writable or owned by another uid. **Its boundary is
  filesystem ownership, and agents Hadron, Tether or Torque launch run as your
  uid**: such an agent can add an entry for its own workflow, including a
  digest it just edited, and the digest pin does not prevent that. Only
  sandboxing agent launches away from the data dir does (tracked as Torque
  task CW-20260930-0237). Details in
  [`docs/workflows.md`](docs/workflows.md#confirmation-and-unattended-starts).

## Data at rest

Hadron has no built-in at-rest encryption. SQLite state under `~/.hadron`
(workflow definitions, runs, values, registry, credential digests) may
contain sensitive content. Protect it with filesystem permissions and disk
encryption, and treat backups the same way.

## External data processors

Hadron itself makes no outbound calls beyond those a workflow asks for and the
optional OpenTelemetry exporter (`OTEL_EXPORTER_OTLP_ENDPOINT`). Agents
launched by Hadron, and any workflow step that calls a network service, send
data to whatever they are configured to reach.

## Current security limitations

- no built-in TLS
- no at-rest encryption
- the script sandbox is in-process; it is not an OS-level isolation boundary
- agents Hadron launches run as the operator's uid and can write Hadron's
  data dir, including the unattended allow-list
- bearer-token authorization rather than an external identity provider
- pre-1.0 contracts; beta releases have no compatibility promise
