# Tool Use

## Running Tests

All commands run from the repo root. The `--build` flag forces a fresh image rebuild so
the Go test cache never masks a real failure.

### Diff tests

```
docker compose -f docker-compose.test.yml -p resonate-diff --profile diff up --build --abort-on-container-exit --exit-code-from tester-diff; docker compose -p resonate-diff down
```

### Kill tests

```
docker compose -f docker-compose.test.yml -p resonate-kill --profile kill up --build --abort-on-container-exit --exit-code-from tester-kill; docker compose -p resonate-kill down
```

### Linearizability tests

```
docker compose -f docker-compose.test.yml -p resonate-linz --profile linearizability up --build --abort-on-container-exit --exit-code-from tester-linearizability; docker compose -p resonate-linz down
```

## ScyllaDB Cloud (dev cluster)

Hosts (GCE us-east-1):
- node-0.gce-us-east-1.f604b92b93ac2ecec4d9.clusters.scylla.cloud
- node-1.gce-us-east-1.f604b92b93ac2ecec4d9.clusters.scylla.cloud
- node-2.gce-us-east-1.f604b92b93ac2ecec4d9.clusters.scylla.cloud

Username: `scylla`
Password: `qh0ozUfEPp3a5uV`

CQL native: port 9042 (plain), port **9142** (TLS — required from outside the cluster).
DC name: `GCE_US_EAST_1`. Use `NetworkTopologyStrategy{GCE_US_EAST_1: 3}` replication.

The cluster cert is signed by a private CA (`ScyllaDB Cloud CA for cluster 49208`)
which the system trust store doesn't know about. For dev runs, use
`SCYLLADB_TLS_INSECURE=1` (still encrypted, just doesn't verify the chain).

Use `SCYLLADB_PORT=9142` (not `:9142` in hosts) so gocql uses 9142 for both the
seeds AND nodes discovered via gossip — without it, gocql tries 9042 (plain) for
discovered peers and TLS handshake fails.

Env-var setup for the diff test against the cloud:

```
SCYLLADB_HOSTS=node-0.gce-us-east-1.f604b92b93ac2ecec4d9.clusters.scylla.cloud,node-1.gce-us-east-1.f604b92b93ac2ecec4d9.clusters.scylla.cloud,node-2.gce-us-east-1.f604b92b93ac2ecec4d9.clusters.scylla.cloud
SCYLLADB_PORT=9142
SCYLLADB_USERNAME=scylla
SCYLLADB_PASSWORD=qh0ozUfEPp3a5uV
SCYLLADB_TLS=1
SCYLLADB_TLS_INSECURE=1
SCYLLADB_REPLICATION="{'class': 'NetworkTopologyStrategy', 'GCE_US_EAST_1': 3}"
go test -v -run TestHandlerDiff ./internal/test/
```
