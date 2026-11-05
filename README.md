# hadron

Hadron rebuilt on [Substrate](https://github.com/hollis-labs/substrate). Early: the shell runs; the workflow engine host is not ported yet. See `docs/status.md`.

```sh
go build -o hadron ./cmd/hadron
./hadron serve        # daemon on 127.0.0.1:8095 (config: cmd/hadron/substrate.yaml, SUBSTRATE_CONFIG)
./hadron health       # run the health checks in-process
./hadron mcp --token-file ~/.config/hadron/token   # MCP over stdio for an agent
```
