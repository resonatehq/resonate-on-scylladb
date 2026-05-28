# Resonate on ScyllaDB

A Go implementation of the [Resonate](https://resonatehq.io) server protocol backed by [ScyllaDB](https://www.scylladb.com/).

## Requirements

- Go 1.25+
- Docker

## Run

```sh
docker compose up
```

The server listens on `:8001`. ScyllaDB schema is applied automatically on startup.

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
| `SERVER_DEBUG` | Debug mode |
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
