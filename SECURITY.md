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
`127.0.0.1:8095`. **Loopback is not a credential**: every process running as
your user can reach 127.0.0.1, including every agent Hadron, Tether or Torque
launches.

- **The operator credential is a file.** `hadrond` creates
  `~/.hadron/operator.token` (mode 0600, under `--data`) on first start. It
  refuses a token file that is group- or world-accessible, owned by another
  user, or a symlink, and keeps only the token's digest in memory. The `hadron`
  CLI sends it as a Bearer token; `hadron auth rotate` replaces it and ends
  every browser session.
- **The browser never sees the token.** `hadron ui` (or the desktop app) asks
  the daemon for a sign-in link that works once and expires after 60 seconds;
  redeeming it sets an HttpOnly, SameSite=Strict session cookie. Code requests
  and redemptions are rate-limited. A cookie-authenticated state change must
  also carry a same-origin `Origin` or `Referer`, and sessions only answer
  loopback Hosts.
- **Without a credential, loopback gets only** `/v1/health` (status and
  version) and the static UI shell. Every workflow operation, workspace
  change and read of run data needs the token or a session.
- **Only the operator can confirm.** A `confirmed: true` sent with an MCP or
  exposure token is refused (`confirmation_not_permitted`), so an agent
  cannot confirm its own effect-advised run. The start record keeps who
  confirmed.
- **Prefer the token file to `HADRON_TOKEN`.** The CLI also reads that
  variable, but an exported variable is inherited by every process started
  from that shell, agents included. Hadron removes it from the environment of
  everything it launches, but it cannot protect a shell you exported it in.
- **Agents that can read `operator.token` still get through.** The file's
  boundary is filesystem permissions, and agents run as your uid. Until agent
  sandboxes deny reads of Hadron's data dir (Torque task CW-20260930-0237),
  this removes the zero-effort path (any local process or web page calling
  loopback) but not an agent that deliberately reads the file.
- `hadrond serve --allow-unauthenticated-loopback` restores the old
  behavior (any loopback request is the operator) for the transition only. It
  is off by default, logs a warning on every use, and will be removed.

Durable bearer credentials resolve to principal and profile records; unknown
credentials fail closed. Cross-origin and DNS-rebinding-shaped requests are
rejected.

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
  data dir, edited only by `hadron workflow unattended`) pins that exact
  workflow digest. Call-started child runs need their own entries. No network surface can read or write the list; the daemon refuses it
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
- agents Hadron launches run as the operator's uid and can read or write
  Hadron's data dir, including `operator.token` and the unattended allow-list,
  until agent sandboxing lands (CW-20260930-0237)
- bearer-token authorization rather than an external identity provider
- pre-1.0 contracts; beta releases have no compatibility promise
