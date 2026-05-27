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

## Test

```sh
# Diff tests
docker compose -f docker-compose.test.yml -p resonate-diff --profile diff up --build --abort-on-container-exit --exit-code-from tester-diff; docker compose -p resonate-diff down

# Kill tests
docker compose -f docker-compose.test.yml -p resonate-kill --profile kill up --build --abort-on-container-exit --exit-code-from tester-kill; docker compose -p resonate-kill down

# Linearizability tests
docker compose -f docker-compose.test.yml -p resonate-linz --profile linearizability up --build --abort-on-container-exit --exit-code-from tester-linearizability; docker compose -p resonate-linz down
```
