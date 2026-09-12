# Workflow Diagnostics And Recovery

This guide is for graph-native workflow runs admitted through `hadron workflow`,
`/v1/workflows`, or the graph-native MCP tools. It does not describe legacy
blueprint JSONL telemetry; that package remains archive/compatibility material
and is not the current production run path.

## Sources Of Truth

Use these sources in order:

1. `hadron workflow inspect <run-id> --json`
   returns the redacted graph-native diagnostic projection. It is built from
   durable run snapshots, node invocations, waits, events, values, resources,
   activation attempts, replay provenance, and the exact pinned execution plan.
2. `POST /v1/workflows/runs/{run_id}/inspect`,
   `/events`, `/waits`, and `/values` expose the same authenticated application
   boundary to HTTP clients. The request body must include the same `run_id`.
3. MCP tools use the same boundary:
   `hadron_workflow_run_inspect`, `hadron_workflow_run_events`,
   `hadron_workflow_run_cancel`, `hadron_workflow_run_resume`,
   `hadron_workflow_signal`, and direct mounted workflow run tools.
4. `/v1/health` and `hadron daemon` are the readiness/recovery checks. Do not
   infer workflow host readiness from a process being alive.
5. The daemon process log on stderr is for host startup/shutdown warnings and
   trace-correlated `slog` output. It is not the run ledger.

Current implementation references:

- `cmd/hadron/workflow_cmd.go` defines the CLI inspect/cancel/resume/rerun
  commands.
- `internal/api/workflows.go` defines the authenticated graph-native HTTP run
  and read routes.
- `internal/mcpadapter/workflow_surface.go` defines the graph-native MCP tools.
- `internal/rundiagnostics/service.go` defines the diagnostic projection and
  its durable inputs.
- `internal/appworkflow/operator.go` defines the shared application operations
  used by CLI, HTTP, MCP, A2A, and UI-facing clients.
- `internal/persistence/workflow_state_events.go` persists graph-native events
  in ascending per-run sequence.

## Telemetry Export

`hadrond serve` initializes OpenTelemetry through the pinned
`github.com/hollis-labs/go-otel` module:

- service name: `hadron`
- service namespace: `hollis`
- service version: the built daemon version
- deployment environment: first non-empty `HOLLIS_ENV`, `APP_ENV`, or `ENV`,
  otherwise `development`
- OTLP endpoint: `OTEL_EXPORTER_OTLP_ENDPOINT`, default `localhost:4318`

The current daemon call enables trace export only. The pinned `go-otel v0.6.1`
supports metrics and logs behind `WithMetricsEnabled` and `WithLogsEnabled`,
but Hadron does not pass those options today. The daemon also installs a
trace-correlated `slog` handler, so future `slog` records emitted with a traced
context can include `trace_id` and `span_id` fields on stderr.

Finding: there is no Hadron-specific CLI command that prints telemetry exporter
configuration at runtime, and graph-native workflow events are not exported as
OTel logs or metrics by default. Operators should inspect run state through the
authenticated run surfaces and configure an OTLP collector separately for
process traces.

Example local trace export wiring:

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=127.0.0.1:4318
export HOLLIS_ENV=local
hadrond serve -addr 127.0.0.1:8095
```

## Inspect First

Start every incident with a redacted diagnostic snapshot:

```sh
hadron workflow inspect <run-id> --json > /tmp/hadron-run.json
jq '.run.status, .truncated, .omissions' /tmp/hadron-run.json
jq '.nodes[] | {id: .id, status: .status, wait: .wait, failure: .failure}' /tmp/hadron-run.json
jq '.events[] | {sequence, type, occurred_at, invocation, attempt, masked}' /tmp/hadron-run.json
```

Use `--reveal-private` only when the caller is authorized and the private value
is necessary. Secret material remains masked.

For HTTP clients, use the authenticated workflow routes:

```sh
curl -sS -X POST "$HADRON_ADDR/v1/workflows/runs/$RUN_ID/inspect" \
  -H "Content-Type: application/json" \
  -d "{\"run_id\":\"$RUN_ID\",\"event_limit\":200,\"node_limit\":200}" |
  jq '.run.status, .nodes[]?.status'

curl -sS -X POST "$HADRON_ADDR/v1/workflows/runs/$RUN_ID/events" \
  -H "Content-Type: application/json" \
  -d "{\"run_id\":\"$RUN_ID\",\"event_limit\":200}" |
  jq '.events[] | {sequence,type,redaction,masked}'
```

For MCP clients:

```text
hadron_workflow_run_inspect({"run_id":"<run-id>"})
hadron_workflow_run_events({"run_id":"<run-id>","limit":200})
```

Finding: the CLI has no separate `hadron workflow events` command today; use
`inspect --json` or the authenticated HTTP/MCP event read surface.

## Failed Runs

1. Inspect the run and record the run status, failing node, attempt, failure
   code, retryability, and the last few events.
2. If the start result was rejected before admission, rerun
   `hadron workflow explain <file|registry-ref> --input-json ... --json` and
   review diagnostics, effects, target, and policy outcome before using
   `--confirm`.
3. If the run is terminal and a downstream slice should be retried, use rerun
   from the first node whose outputs must be recomputed:

```sh
hadron workflow rerun <source-run-id> \
  --from <node-id> \
  --run-id <new-run-id> \
  --idempotency-key <stable-key> \
  --json
```

`rerun` is policy-authorized and uses replay provenance. It is not a mutable
in-place retry of the old run.

## Waiting Runs

If a run is waiting, inspect the wait descriptor first:

```sh
jq '.nodes[] | select(.wait != null) | {node: .id, wait: .wait}' /tmp/hadron-run.json
```

Resume only with the exact wait ID, correlation, source, token, typed payload,
and an idempotency key:

```sh
hadron workflow resume <run-id> \
  --wait <wait-id> \
  --correlation <correlation> \
  --source gate \
  --token-file /path/to/one-time-token \
  --payload-json '{"type":"object","inline":{"approved":true},"producer":{"kind":"operator","reference":"manual-resume"},"media_type":"application/json","digest":"sha256:20ec2f38fc96bc7d416a9af537493ae0bae26c5ad4c5a0f2d94f56f9c3e04958","redaction":"private","retention":"run"}' \
  --idempotency-key <stable-key> \
  --json
```

Valid wake sources are `gate`, `message`, `callback`, and `signal`. A run ID or
wait ID is not a bearer credential; the application host still binds the current
responder and delegates token, correlation, schema, wake-source, and authority
checks to the wait coordinator.

For named signal waits, use the optional signal surface rather than inventing a
transport-specific resume path:

```text
hadron_workflow_signal({
  "run_id": "<run-id>",
  "name": "<signal-name>",
  "correlation": "<correlation>",
  "payload": {"type":"object","inline":{"approved":true},"producer":{"kind":"operator","reference":"manual-resume"},"media_type":"application/json","digest":"sha256:20ec2f38fc96bc7d416a9af537493ae0bae26c5ad4c5a0f2d94f56f9c3e04958","redaction":"private","retention":"run"},
  "idempotency_key": "<stable-key>"
})
```

## Recovering Or Unavailable Host

If `hadron daemon` or `/v1/health` reports unavailable or recovering:

1. Leave the store untouched; do not repair by editing SQLite rows.
2. Wait for the host to complete startup recovery. The production runtime
   configures periodic recovery at a 2 second interval with a batch limit of
   100.
3. Inspect the run again. Recovery reconstructs from immutable start records,
   child-start records, replay provenance, durable waits, cancellation/update
   intents, and pinned recovery plans.
4. If a run remains blocked after readiness returns, use the failed or waiting
   sequence above. The supported recovery actions are inspect, cancel, resume,
   signal, and rerun through the authenticated application surface.

Cancel is supported for policy-authorized aborts:

```sh
hadron workflow cancel <run-id> \
  --idempotency-key <stable-key> \
  --reason "operator requested stop after inspection" \
  --json
```

## Bounded Local Measurement

The project does not publish measured beta baselines for throughput, event
volume, or scheduling precision. Treat any expectations as unmeasured until
you run a bounded local measurement on your machine.

Use temporary local state and a small graph-native production example:

```sh
tmp="$(mktemp -d)"
install -d "$tmp/workflows"
install -m 0600 examples/workflow/production/hello-transform.workflow.yaml \
  "$tmp/workflows/hello-transform.workflow.yaml"

hadrond serve \
  -addr 127.0.0.1:8095 \
  -db "$tmp/state/hadron.db" \
  -data "$tmp/data" \
  -logs "$tmp/logs" >"$tmp/hadrond.log" 2>&1 &
daemon_pid=$!
trap 'kill "$daemon_pid" 2>/dev/null || true; rm -rf "$tmp"' EXIT

hadron workflow validate "$tmp/workflows/hello-transform.workflow.yaml" --json

for i in $(seq 1 10); do
  run_id="measure-$i"
  started="$(perl -MTime::HiRes=time -e 'printf "%.0f", time*1000')"
  hadron workflow run "$tmp/workflows/hello-transform.workflow.yaml" \
    --run-id "$run_id" \
    --idempotency-key "measure-start-$i" \
    --input-json '{"name":"operator"}' \
    --json >/dev/null
  until hadron workflow inspect "$run_id" --json |
    jq -e '.run.status == "succeeded" or .run.status == "failed" or .run.status == "canceled"' >/dev/null; do
    sleep 0.2
  done
  ended="$(perl -MTime::HiRes=time -e 'printf "%.0f", time*1000')"
  hadron workflow inspect "$run_id" --json >"$tmp/$run_id.json"
  events="$(jq '.events | length' "$tmp/$run_id.json")"
  printf '%s latency_ms=%s events=%s\n' "$run_id" "$((ended-started))" "$events"
done
```

This method measures local end-to-end admission-to-terminal latency and
rendered diagnostic event volume for one small workflow. It is not a load test,
does not measure production collector export latency, and should not be used to
publish system-wide capacity claims.
