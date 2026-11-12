# Workflow examples

## Runnable with the stock daemon

The files under [`production/`](production/) use only step kinds the Hadron host
registers. File definitions are confined to the daemon's workflow source root
(`<data_dir>/workflows`, `hadron-data/workflows` by default), so stage them there
and start the daemon:

```sh
install -d hadron-data/workflows
install -m 0600 examples/workflow/production/*.workflow.yaml hadron-data/workflows/
hadron serve &

hadron workflow validate hello-transform.workflow.yaml
hadron workflow run hello-transform.workflow.yaml \
  --run-id example-hello-1 --input-json '{"message":"Hello, workflow"}'
hadron workflow inspect example-hello-1

hadron workflow run normalize-script.workflow.yaml \
  --run-id example-normalize-1 --input-json '{"message":"  Release READY  "}' -o json
```

Add `-o json` or `-o yaml` to any `hadron workflow` command for the daemon's
response instead of the text form. A workflow can also be built into a
daemon-less executable from a source checkout:

```sh
hadron build examples/workflow/production/hello-transform.workflow.yaml -o ./hello
./hello --message "Hello, workflow"
```

The host registers `transform@v1`, `script@v1`, `sleep@v1`, `wait_for@v1`,
`message_wait@v1`, `human_gate@v1`, `call@v1`, and `agent_session@v1` when a
`tether_session` substrate is configured. This is a host capability boundary, not
a limitation of the embeddable workflow engine.

## A durable gate

[`release-approval-gate.workflow.yaml`](release-approval-gate.workflow.yaml) uses
only host kinds and documents a durable human gate: the run waits at `approval`
until someone resumes it (`hadron workflow resume`). Its `environment` is a static policy subject: the engine rejects a templated one.

Not carried over from the previous app: the `http-cmd-transform` compiler
snapshot fixture and the `torque-task-bulk-create` fake-MCP fixture. Both needed
kinds the stock host does not register (`http@v1`, `cmd@v1`, `mcp@v1`) and tests
that no longer exist.
