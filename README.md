<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./assets/banner-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="./assets/banner-light.png">
    <img alt="Resonate on ScyllaDB" src="./assets/banner-dark.png">
  </picture>
</p>

# Resonate on ScyllaDB

[![ci](https://github.com/resonatehq/resonate-on-scylladb/actions/workflows/ci.yml/badge.svg)](https://github.com/resonatehq/resonate-on-scylladb/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-BUSL_1.1-blue.svg)](./LICENSE)

## About this component

A Go implementation of the [Resonate](https://resonatehq.io) server protocol backed by [ScyllaDB](https://www.scylladb.com/). It speaks the same HTTP/JSON protocol as the core Resonate server, so your application code and your SDK stay as they are — you point `RESONATE_URL` at this instead. The one exception is search, which isn't implemented; see [What's not there yet](#whats-not-there-yet).

Reach for it when you already run ScyllaDB and would rather not stand up a second database for durable execution. If you don't already run ScyllaDB, run the core server on Postgres — it is the reference implementation and the default recommendation.

This repository is source-available under BUSL-1.1, not Apache-2.0. Development, testing, and evaluation are free; production use needs a commercial license until the Change Date. See [License](#license).

- [Read the docs for this provider](https://docs.resonatehq.io/deploy/providers/scylladb)
- [Evaluate Resonate for your next project](https://docs.resonatehq.io/evaluate/)
- [Example application library](https://github.com/resonatehq-examples)
- [Distributed Async Await — the concepts that power Resonate](https://www.distributed-async-await.io/)
- [Join the Discord](https://resonatehq.io/discord)
- [Subscribe to the Journal](https://journal.resonatehq.io/subscribe)
- [Follow on X](https://x.com/resonatehqio)
- [Follow on LinkedIn](https://www.linkedin.com/company/resonatehqio)
- [Subscribe on YouTube](https://www.youtube.com/@resonatehqio)

## Requirements

- A ScyllaDB cluster reachable over CQL
- Docker and Docker Compose for the local stack below
- Go 1.25.4+ to build from source

## Run

```sh
docker compose --profile server up
```

The server listens on `:8001`. The bundled stack runs the server with `--debug`, which creates the keyspace for you — and drops it again on every restart. Any other deployment provisions the schema itself; see [Configuration](#configuration).

The profile is not optional. The `server` service in `docker-compose.yaml` declares `profiles: [server]`, so a bare `docker compose up` starts ScyllaDB and nothing else.

## Connecting workers

This provider is drop-in. It serves the same HTTP/JSON protocol as the core server, so every Resonate SDK reaches it the same way and no client wiring changes:

```sh
RESONATE_URL=http://localhost:8001
```

Your workflow code doesn't change either — the only thing that moves is `RESONATE_URL`.

Protocol requests go to `/`, task delivery streams from `GET /poll/{group}/{id}` as server-sent events, and `GET /health` is the health check. The RPC route is unconstrained, so any other path or method also lands there and answers `400` rather than `404` — don't probe it for capability.

Requests carry a protocol version of `2026-04-01` in the header. The server requires the field to be present but does not check its value.

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
  log-level: info

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
| `SERVER_DEBUG` | Debug mode — **drops and recreates the keyspace on connect** |
| `SERVER_LOG_LEVEL` | `debug`, `info`, `warn`, or `error` (default `info`); any other value is a fatal startup error |
| `SCYLLADB_HOSTS` | Comma-separated seed hosts |
| `SCYLLADB_PORT` | CQL port. Set it to `9142` for a TLS cluster — putting the port in `SCYLLADB_HOSTS` isn't enough, because gossip-discovered peers are dialled on this value |
| `SCYLLADB_USERNAME` | Username |
| `SCYLLADB_PASSWORD` | Password |
| `SCYLLADB_TLS_ENABLED` | Enable TLS |
| `SCYLLADB_TLS_INSECURE` | Skip certificate verification |
| `SCYLLADB_KEYSPACE` | Keyspace name (default `resonate`) |
| `SCYLLADB_REPLICATION` | Replication clause used when the server creates the keyspace — **debug mode only**, ignored otherwise |
| `TIMEOUTS_BUCKET_WIDTH` | Timeout bucket width (e.g. `1h`, `30m`) |
| `TIMEOUTS_BUCKET_LOOKBACK` | Past buckets to scan |
| `TIMEOUTS_SHARDS` | Shard count |
| `WORKER_TTL` | Worker row TTL (e.g. `15s`) |
| `WORKER_TICK_INTERVAL` | Coordinator tick interval (e.g. `1s`) |

Two of these will hurt you if you set them casually.

**`SERVER_DEBUG` drops the keyspace.** In debug mode the server issues `DROP KEYSPACE IF EXISTS` and recreates it on every connect. That is why the bundled local stack comes up with a working schema, and why a local restart starts from empty. Pointing a debug-mode server at a populated keyspace destroys it. Debug mode is for local development and the test suites.

**`TIMEOUTS_SHARDS` is baked into the partition key.** The shard is a hash of the record id, stored in the partition key of every timeout row. Every instance must use the same value. Changing it on a populated cluster strands existing rows in partitions no server scans, and durable timers stop firing without an error.

Outside debug mode the server applies no DDL at all — it opens a keyspace-bound session and assumes the schema is there. For any real deployment you provision the keyspace yourself, with a replication strategy you chose deliberately.

## What's not there yet

Read this before you plan a production rollout.

- **No production reference deployments.** Nobody is running this in production yet.
- **Search is unimplemented.** `task.search` returns `501`. `promise.search` and `schedule.search` aren't recognized kinds at all, so they come back as `400` with `unknown kind: <kind>` rather than `501` — worth knowing if you plan to probe for capability. Anything that queries promises by tag will not work.
- **No authentication.** An auth hook exists in the code, but nothing wires it up and its check is an unimplemented stub, so every request that reaches the server is served. Put it behind your own network controls.
- **Behaviour under degraded quorum is uncharacterised.** Node loss and partitions are ScyllaDB's own problem and it handles them. What the test suites don't cover is how this server behaves while a quorum is unavailable, and a repair path exists in the code without being wired into the running server.
- **Known bug:** deleting a schedule can leave stale rows in `schedule_timeouts`.
- **The column layout is not a compatibility surface yet.** This is a young repository and the schema can still change.

## How it's tested

Three suites, all runnable from a clone under Docker Compose.

```sh
# Diff tests — every step checked against an in-memory oracle
docker compose -f docker-compose.test.yml -p resonate-diff --profile diff up --build --abort-on-container-exit --exit-code-from tester-diff; docker compose -p resonate-diff down

# Kill tests — crash mid-transaction, assert invariants on what survives
docker compose -f docker-compose.test.yml -p resonate-kill --profile kill up --build --abort-on-container-exit --exit-code-from tester-kill; docker compose -p resonate-kill down

# Linearizability — concurrent interleavings, model-checked
docker compose -f docker-compose.test.yml -p resonate-linz --profile linearizability up --build --abort-on-container-exit --exit-code-from tester-linearizability; docker compose -p resonate-linz down
```

The kill tests abort a randomly chosen fiber at a cooperative yield checkpoint, with probability 0.3 per scheduler tick, a thousand iterations by default, and check named state invariants on what is left. Linearizability is checked with the [Porcupine](https://github.com/anishathalye/porcupine) model checker against the same oracle the diff tests use.

## Community

- [Discord](https://resonatehq.io/discord)
- [X](https://x.com/resonatehqio)
- [LinkedIn](https://www.linkedin.com/company/resonatehqio)
- [YouTube](https://www.youtube.com/@resonatehqio)
- [Journal](https://journal.resonatehq.io)

## License

`resonate-on-scylladb` is licensed under the [Business Source License 1.1](LICENSE) (BUSL-1.1).

This is **not** an open-source license. Non-production use (development, testing, evaluation) is permitted. **Any production use requires a commercial license from Resonate HQ, Inc.** On the Change Date (2030-07-01), each released version converts to the Apache License, Version 2.0.

For commercial licensing, contact licensing@resonatehq.io.
