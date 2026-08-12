# resonate-on-scylladb

A Go implementation of the [Resonate](https://resonatehq.io) server protocol on [ScyllaDB](https://www.scylladb.com/) — a drop-in alternative to the core server speaking the same HTTP/JSON protocol, so existing Resonate SDKs work unchanged.

## Why ScyllaDB

The Resonate protocol is storage-agnostic: durable storage and atomic compare-and-swap are enough to implement it. `resonate-on-scylladb` is the ScyllaDB-native implementation of that protocol.

- https://resonatehq.io/providers/scylladb
- https://docs.resonatehq.io/deploy/providers/scylladb

## Status

Active development. The implementation carries strong in-repo test coverage: an oracle-diff suite (`internal/test/test_diff_test.go`), randomized kill tests with invariant checks (`internal/test/test_kill_test.go`), and Porcupine linearizability checks (`internal/test/test_linz_test.go`).

**Known gaps and constraints:**

- `task.search` is not yet implemented; the server returns a 501 in the protocol response.
- The server performs no API authentication. The auth path exists in the codebase but is not wired into the serve path — all requests are allowed. Run this server inside a trusted network boundary; access control belongs at the infrastructure layer. If you need authentication, open an issue.

Running this in production? Open an issue — feedback on real requirements shapes prioritization.

## Requirements

- Go 1.25+
- Docker

## Quickstart

Start ScyllaDB:

```sh
docker compose up
```

**`docker compose up` starts ScyllaDB only.** The server service is behind a Compose profile. To bring up the full stack (ScyllaDB + server):

```sh
docker compose --profile server up
```

> **Warning: the compose file runs the server with `--debug`, and debug mode drops and recreates the ScyllaDB keyspace on every start.**
>
> This is the intended behavior for local development — the server calls `DROP KEYSPACE IF EXISTS` on startup when debug mode is active (`serve.go` → `internal/dbms/schema.go`). Never point a debug-mode server at a keyspace you care about.
>
> For production, run `resonate serve` without `--debug` against a keyspace you have provisioned separately.

The server listens on `:8001`.

## Configuration

`resonate serve` accepts configuration from, in priority order:

1. CLI flags
2. Environment variables
3. Optional config file (`resonate.yaml`)
4. Built-in defaults

### Config file

```sh
resonate serve                        # loads ./resonate.yaml if present
resonate serve --config ./my.yaml     # explicit file; error if missing
```

Example `resonate.yaml`:

```yaml
server:
  addr: ":8001"
  debug: false

scylladb:
  hosts:
    - localhost
  port: 0
  username: ""
  password: ""
  keyspace: ""
  replication: ""
  tls:
    enabled: false
    insecure: false

timeouts:
  bucket-width: 1h
  bucket-lookback: 1
  shards: 1

worker:
  ttl: 15s
  tick-interval: 1s
```

### Environment variables

| Variable | Description |
|---|---|
| `SERVER_ADDR` | Server listen address |
| `SERVER_DEBUG` | Enable debug mode. **Drops and recreates the keyspace on every start.** Do not set this against a keyspace you care about. |
| `SCYLLADB_HOSTS` | Comma-separated seed hosts |
| `SCYLLADB_PORT` | CQL port |
| `SCYLLADB_USERNAME` | Username |
| `SCYLLADB_PASSWORD` | Password |
| `SCYLLADB_TLS_ENABLED` | Enable TLS |
| `SCYLLADB_TLS_INSECURE` | Skip certificate verification |
| `SCYLLADB_KEYSPACE` | Keyspace name |
| `SCYLLADB_REPLICATION` | Replication clause for schema creation |
| `TIMEOUTS_BUCKET_WIDTH` | Timeout bucket width (e.g. `1h`, `30m`) |
| `TIMEOUTS_BUCKET_LOOKBACK` | Past buckets to scan |
| `TIMEOUTS_SHARDS` | Shard count |
| `WORKER_TTL` | Worker row TTL (e.g. `15s`) |
| `WORKER_TICK_INTERVAL` | Coordinator tick interval (e.g. `1s`) |

## Test

```sh
# Diff tests
docker compose -f docker-compose.test.yml -p resonate-diff --profile diff up --build --abort-on-container-exit --exit-code-from tester-diff; docker compose -p resonate-diff down

# Kill tests
docker compose -f docker-compose.test.yml -p resonate-kill --profile kill up --build --abort-on-container-exit --exit-code-from tester-kill; docker compose -p resonate-kill down

# Linearizability tests
docker compose -f docker-compose.test.yml -p resonate-linz --profile linearizability up --build --abort-on-container-exit --exit-code-from tester-linearizability; docker compose -p resonate-linz down
```

## See also

- [resonatehq/resonate](https://github.com/resonatehq/resonate) — the core Resonate server
- [resonatehq/resonate-on-nats](https://github.com/resonatehq/resonate-on-nats) — NATS-native transport implementation (not a drop-in)

## License

`resonate-on-scylladb` is licensed under the [Business Source License 1.1](LICENSE) (BUSL-1.1).

This is **not** an open-source license. Non-production use (development, testing, evaluation) is permitted. **Any production use requires a commercial license from Resonate HQ, Inc.** On the Change Date (2030-07-01), each released version converts to the Apache License, Version 2.0.

For commercial licensing, contact licensing@resonatehq.io.
