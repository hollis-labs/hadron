# Graph-native workflow examples

## Runnable with the stock daemon

The files under [`production/`](production/) use only step kinds registered by
the production Hadron host. File definitions are confined to the daemon's
workflow source root, so stage the examples there before starting `hadrond`:

```sh
install -d "$HOME/.hadron/workflows"
install -m 0600 examples/workflow/production/*.workflow.yaml \
  "$HOME/.hadron/workflows/"
hadrond serve

hadron workflow validate "$HOME/.hadron/workflows/hello-transform.workflow.yaml"
hadron workflow run "$HOME/.hadron/workflows/hello-transform.workflow.yaml" \
  --run-id example-hello-1 \
  --idempotency-key example-hello-1 \
  --input-json '{"message":"Hello, workflow"}' \
  --json

hadron workflow validate "$HOME/.hadron/workflows/normalize-script.workflow.yaml"
hadron workflow run "$HOME/.hadron/workflows/normalize-script.workflow.yaml" \
  --run-id example-normalize-1 \
  --idempotency-key example-normalize-1 \
  --input-json '{"message":"  Release READY  "}' \
  --json
```

The stock production host exposes exactly `transform@v1`, `script@v1`,
`sleep@v1`, `wait_for@v1`, `message_wait@v1`, and `human_gate@v1`. This is a
host capability boundary, not a limitation of the embeddable workflow engine.

## Compiler and adapter conformance fixtures

The files immediately below this README exercise broader graph source and
adapter contracts in focused tests. They are graph-native, but they are not all
runnable by the stock daemon:

- [`release-approval-gate.workflow.yaml`](release-approval-gate.workflow.yaml)
  uses only production kinds and documents a durable human gate.
- [`http-cmd-transform.workflow.yaml`](http-cmd-transform.workflow.yaml) is a
  compiler snapshot fixture. The stock daemon does not register `http@v1` or
  `cmd@v1`.
- [`torque-task-bulk-create.workflow.yaml`](torque-task-bulk-create.workflow.yaml)
  is a fake-MCP integration fixture used by
  `internal/mcpadapter/torque_bulk_create_e2e_test.go`. The stock daemon does
  not register `mcp@v1`.

The wider adapter set remains available to embedders that construct and bind a
host with an explicit capability and policy profile. It is never inferred from
the presence of these fixtures.

Beta-era blueprint and pipeline samples remain under
[`../archive/legacy-blueprints-pipelines/`](../archive/legacy-blueprints-pipelines/)
for rewrite/reference use only. They are not accepted by the active public CLI.
