# Valkey (Redis) in Substrate — Overview

Redis/Valkey in this repo means one thing: **Valkey (Redis-compatible, cluster mode) is
the control plane's primary state store**, used exclusively by the `ate-api-server`
(`cmd/ateapi`). It is not a cache — it is the system of record for high-churn resources,
plus pub/sub and distributed locks. One client library, one consumer service, one
deployment manifest.

## Client & where it lives

- **Library**: `github.com/redis/go-redis/v9` (`go.mod`), with `miniredis` for tests. No
  other redis clients anywhere (many "redis" grep hits elsewhere are just the word
  "redistribute" in licenses).
- **Store implementation**: `cmd/ateapi/internal/store/ateredis/ateredis.go`,
  implementing the `store.Interface` from `cmd/ateapi/internal/store/store.go`.
- The client is always a `redis.NewClusterClient` (`cmd/ateapi/main.go`) — cluster mode
  only, go-redis default pooling, startup `PING` with 30 retries × 2s, fatal if
  unreachable.

## What it's used for

1. **Primary object storage** — Actors, Workers, Atespaces, ActorSnapshots, and
   SnapshotTags stored as protojson values. Creates use `SetNX` (no TTL — data is
   persistent), reads are `GET`, listing is per-master `SCAN` + pipelined `GET` with
   pagination tokens that encode the shard address and cursor.
2. **Optimistic concurrency** — updates and guarded deletes run inside `WATCH`/`MULTI`
   transactions; `redis.TxFailedErr` maps to `store.ErrVersionConflict`, which surfaces
   as gRPC `Aborted` ("concurrent update conflict, please retry"). Some callers retry
   in-process (e.g. `AssignWorkerStep` in `internal/controlapi/workflow_resume.go` with
   `wait.Backoff`), and the WorkerPoolSyncer requeues via a rate-limited workqueue.
3. **Pub/sub** — worker change events on the `worker-changes` channel. The in-memory
   `workercache` subscribes to it, and heals with exponential-backoff resubscribe
   (1s→30s) plus an unconditional full relist every 5 minutes.
4. **Distributed locks** — TTL leases via `SetNX key uuid 30s`, renewed every TTL/3 by a
   Lua script (`GET`+`PEXPIRE` if owner matches), released by a matching Lua `GET`+`DEL`.
   If renewal fails past ⅔ of the TTL, the lease context cancels so workflows abort.
   Used around actor workflows (`lock:actor:<atespace>:<name>`), atespace deletion, and
   snapshot operations.
5. **Debug flush** — `DebugClearAll` runs `FLUSHALL ASYNC` on every master, exposed as a
   gRPC RPC and reachable via `kubectl ate admin debug-flush-redis`.

## Keyspace

Plain colon-delimited keys, documented in `ateredis.go` as frozen ("existing databases
hold keys in this form"):

| Key pattern | Value |
|---|---|
| `actor:<atespace>:<name>` | protojson Actor |
| `worker:<namespace>:<pool>:<pod>` | protojson Worker (integer `Version` field for CAS) |
| `atespace:<name>` | protojson Atespace |
| `actor-snapshot:<atespace>:<name>` | JSON `{snapshot, location}` |
| `actor-snapshot-tag:<atespace>:<name>` | protojson ActorSnapshotTag |
| `lock:actor:<atespace>:<name>` | UUID lease value |
| `lock:atespace:<name>` | UUID lease value |
| `lock:actor-snapshot:<atespace>:<name>` | UUID lease value |
| channel `worker-changes` | JSON `{t: eventType, w: protojson Worker}` |

Only lock keys have TTLs. No hash tags yet — `docs/roadmap.md` lists hash-tag sharding
as future work for 1M+ actors. The `ateredis` package doc explains the cluster-slot
constraints (no cross-slot multi-key ops), which is why worker status is denormalized
into the Actor record.

## Configuration & deployment

- **Flags** on ateapi: `--redis-cluster-address`, `--redis-ca-certs`,
  `--redis-use-iam-auth`, `--redis-tls-server-name`, `--redis-client-cert`; a flag set
  to the sentinel `@env` resolves from `ATE_API_REDIS_*` env vars.
- **Two auth paths**:
  1. In-cluster mutual TLS using Kubernetes PodCertificate signers — the valkey server
     presents a `servicedns` cert and ateapi dials with a `podidentity` SPIFFE client
     cert.
  2. Google IAM token auth via a `CredentialsProvider` (username "default" + OAuth
     token), intended for a managed GCP endpoint, though nothing in the repo actually
     provisions one.

  TLS 1.3 minimum in both cases.
- **The deployment** is `manifests/ate-install/valkey.yaml`: a 6-replica StatefulSet
  running digest-pinned `valkey/valkey:9.1`, TLS-only (`port 0`,
  `tls-auth-clients yes`), AOF persistence, 1Gi PVC per pod, with an idempotent init
  Job that runs `valkey-cli --cluster create --cluster-replicas 1` → **3 masters +
  3 replicas**. Certs auto-reload every 600s to stay ahead of the 30-minute rotation.
  `hack/install-ate.sh` wires up the env ConfigMap and combines the two CAs into the
  `valkey-ca-certs` secret (Valkey only accepts a single `tls-ca-cert-file`, which also
  drove the multi-CA bundle logic in the cert signers). No docker-compose, helm, or
  terraform.

## Testing

Everything uses **miniredis** — the shared helper
(`cmd/ateapi/internal/store/storetest/storetest.go`) wraps a single miniredis in a
`ClusterClient`, which works because cluster-specific commands are avoided. Notable:
lock-expiry tests use `mr.FastForward` (miniredis TTLs are virtual), multi-shard
pagination tests spin up multiple miniredis instances, and a mock client injects
`SetNX`/`EvalSha` failures to exercise lock-renewal paths. The e2e suites never touch
redis directly. The code style guide explicitly says to prefer miniredis over mocks for
the store.

## Failure model & open questions

There are no circuit breakers or degraded modes by design: Valkey is the source of
truth, so when it's down, RPCs error and the process won't even start. Resilience is
targeted where it matters — lock auto-renewal, CAS retry/requeue, and pub/sub
self-healing.

Worth knowing: `docs/threat-model.md` and `docs/roadmap.md` both flag that **the choice
of Redis/Valkey as the API storage backend is under active debate**, so the store is
deliberately hidden behind `store.Interface`. PR #640 (below) adds a PostgreSQL
implementation of that interface.

## PostgreSQL backend (PR #640)

PR [#640](https://github.com/agent-substrate/substrate/pull/640) adds an experimental
**parallel PostgreSQL backend** behind the same `store.Interface` — opt-in via a flag,
with no data migration or dual-write. Switching backends means starting with fresh
state; redis remains the default.

- **Selection**: `--store-backend=redis|postgres` (default redis) plus
  `--postgres-connection-string` (a libpq DSN; TLS configured entirely through
  `sslmode`/`sslrootcert`/`sslcert`/`sslkey` params). Env vars:
  `ATE_API_STORE_BACKEND`, `ATE_API_POSTGRES_CONNECTION_STRING`. A `connectStore()`
  switch in `cmd/ateapi/main.go` builds the chosen backend; all consumers already take
  `store.Interface`, so nothing downstream changed.
- **Implementation**: `cmd/ateapi/internal/store/atepg` on `jackc/pgx/v5` + pgxpool
  (default pool settings, mirroring the untuned go-redis pool).
- **Schema** (`atepg/schema.go`): hybrid model — full binary protobuf in a `bytea`
  column, plus native SQL columns only for what SQL must operate on (primary keys,
  `version` for CAS, actor `status` for the delete precondition). Six tables:
  `atespaces`, `actors`, `actor_snapshots`, `actor_snapshot_tags`, `workers`, `leases`.
  Foreign keys with `ON DELETE RESTRICT` push the "can't delete a non-empty atespace"
  precondition into the database. The embedded schema is applied idempotently at
  startup under `pg_advisory_xact_lock`, so multiple ateapi replicas can bootstrap an
  empty database concurrently. No migration framework.

How each Redis mechanism was translated:

| Redis/Valkey | PostgreSQL |
|---|---|
| `SetNX` create | `INSERT`; unique violation → `ErrAlreadyExists` |
| `WATCH`/`MULTI` CAS | Transaction + `UPDATE ... WHERE version = $expected RETURNING`; zero rows re-reads inside the tx to distinguish conflict / not-found / immutable-field |
| Per-shard `SCAN` pagination | Keyset pagination (`ORDER BY` key cols, `WHERE (cols) > (last)`, `LIMIT n+1`); page token is base64 JSON `{version, kind, scope, last}` with no topology, validated against the list method and scope |
| Pub/sub `worker-changes` | `LISTEN`/`NOTIFY` on `worker_changes`; `pg_notify` runs **inside the write's transaction**, so delivery happens iff the write commits. `WatchWorkers` hijacks a dedicated pool connection. NOTIFY payloads cap at 8,000 bytes — oversized worker writes fail rather than silently skip the event |
| Lua lock scripts | A `leases` table with `expires_at timestamptz`; acquire is `INSERT ... ON CONFLICT DO UPDATE ... WHERE expired`, renew/release are token-guarded. Same TTL and renewal-loop constants as ateredis (30s, renew TTL/3, retry TTL/10, give up at ⅔ TTL) — deliberately not pg advisory locks, to keep lease semantics identical across backends |
| `FLUSHALL ASYNC` | `TRUNCATE` all six tables |

- **Contract tests**: the backend-neutral assertions were extracted from the redis
  tests into `cmd/ateapi/internal/store/storecontract` (~45 subtests: CRUD, CAS
  conflicts, immutable fields, pagination, locks, watches, preconditions). Both
  backends run the suite — ateredis against miniredis, atepg against a real
  `postgres:18-alpine` via **testcontainers** (one shared container per package,
  per-test isolation via `TRUNCATE`, skipped when Docker is unavailable).
- **Deployment**: `manifests/ate-install/postgres.yaml` — a single-replica
  `postgres:18-alpine` StatefulSet reusing the same PodCertificate mTLS machinery as
  Valkey: `servicedns` serving cert (copied to an emptyDir by an initContainer to get
  the 0600 perms postgres requires), client auth via
  `hostssl all all all trust clientcert=verify-ca` against the `podidentity` CA — pure
  mTLS, no passwords. `hack/install-ate.sh` grows `--store-backend` and a standalone
  `--deploy-postgres`, and always reconciles the env ConfigMap so flipping the backend
  updates an existing install. The api-server manifest also switched its store CA from
  the `valkey-ca-certs` secret to a projected `clusterTrustBundle`.
- **Caveats**: single replica, no HA; the kustomize overlays still deploy Valkey
  unconditionally (postgres is applied additionally when selected; overlay cleanup is
  deferred). Watch-loss semantics intentionally match redis — callers resubscribe and
  the workercache relist heals gaps.

## Related docs

- `docs/dev/onboarding-actor-lifecycle.md` — how actors/workers flow through the store.
- `docs/dev/valkey-direct-access.md` — opening a TLS `valkey-cli` against the cluster
  for debugging.
- `docs/architecture.md` — state store's role in the overall system.
