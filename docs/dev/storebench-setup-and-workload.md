# storebench: Storage Backend Benchmark Architecture

`storebench` is an in-cluster, open-loop benchmarking tool designed to measure the raw performance, throughput, and tail latency of Substrate's persistence backends (**PostgreSQL / Cloud SQL** via `atepg` and **Redis / Valkey** via `ateredis`).

---

## 1. Test Setup Overview

`storebench` runs as a Kubernetes Job inside the `ate-system` namespace. It calls the Go storage interface (`store.Interface`) directly in-process via native database drivers (`pgx` connection pool for PostgreSQL or `go-redis` for Valkey), bypassing all gRPC, HTTP, and Envoy routing hops.

```
                      ┌─────────────────────────────────────────┐
                      │        Kubernetes Job: storebench       │
                      │         (cmd/ateapi/storebench)         │
                      └────────────────────┬────────────────────┘
                                           │ Direct Go Driver (pgx / go-redis)
                      ┌────────────────────▼────────────────────┐
                      │          PostgreSQL / Cloud SQL         │
                      │  • atespaces          • workers         │
                      │  • actors             • leases          │
                      │  • actor_snapshots    • snapshot_tags   │
                      └─────────────────────────────────────────┘
```

### Key Differences from End-to-End Benchmarks (Locust)
* **Open-Loop Pacing:** Requests are dispatched on a strict timer (e.g., 2,500 or 5,000 RPS in 5ms batches) regardless of whether previous requests have completed, completely avoiding **coordinated omission**.
* **Microsecond Precision:** Measures wall-clock execution time for pure database statements with microsecond precision and produces quantile reports (`p50`, `p90`, `p95`, `p99`, `p99.9`, `max`), plus an `unfinished_at_cutoff` count so saturated runs self-declare that their percentiles are queue depth, not op cost.

---

## 2. How the Data Preload Works

Before starting the timed measurement window, `storebench` seeds a **deterministic standing dataset**. There are two load paths (`--load-mode`, default `auto`):

* **COPY bulk loader** (postgres; `copyLoad()` in `cmd/ateapi/storebench/copyload.go`): pgx `CopyFrom` in 100k-row atomic batches, writing atepg's exact storage format. ~9k actors/s (+1:1 snapshots) sustained against Cloud SQL — the only viable path for the L/XL tiers (100M actors ≈ 3–3.5 h). **Resumable**: each table continues from its max zero-padded name after an interruption.
* **Store-API loader** (`load()` in `bench.go`; used for redis or `--load-mode=store`): 64 concurrent `CreateActor`/`CreateActorSnapshot`/`CreateWorker` calls; `ErrAlreadyExists` counts as success.

Both produce the same dataset:

1. **Atespaces:** `sbench-%06d` (widths sized for the XL tier; changing widths orphans existing datasets since resume depends on lexicographic name order).
2. **Actors & Snapshots in lockstep:** actor $i$ lives in atespace $i \bmod \text{atespaces}$ as `actor-%09d`; one snapshot per actor (`gs://storebench-fake/snapshot`). Records are padded to realistic marshaled sizes via `--actor-bytes/--worker-bytes/--snapshot-bytes` (plan of record: 3000/1000/1000, matching the storage requirements doc).
3. **Workers:** `worker-%08d` in pool $i \bmod \text{pools}$.
4. **Restarts:** `--skip-load=true` reuses an existing dataset (its shape flags must match what was loaded — the name↔location mapping is a pure function of the tier shape).
5. **Initial State:** All preloaded actors start in **`STATUS_SUSPENDED`** with no worker assignment.

---

## 3. How `ActorUpdate` and Worker Updates Work in `storebench`

During the timed benchmark run, operations are selected according to the configured `--mix` ratios (e.g., `actorget=30,actorupdate=25,workerget=15,workerupdate=15,snapget=10,snapcreate=5`).

### A. How `ActorUpdate` is Executed

In [`opActorUpdate` in `bench.go`](file:///Users/shrutiyam/Documents/substrate/cmd/ateapi/storebench/bench.go):

1. **Random Key Selection:** `keyPicker` randomly selects an actor index from the dataset (using either `uniform` random distribution or `zipf` power-law distribution for hot-key testing).
2. **Untimed Version Read:** Reads the actor's current state and `expectedVersion` from the database.
3. **Payload Mutation:** Mutates the actor's `WorkerSelector` with a new random label:
   ```go
   updated := proto.Clone(current).(*ateapipb.Actor)
   updated.WorkerSelector = &ateapipb.Selector{
       MatchLabels: map[string]string{"bench": uuid.NewString()},
   }
   ```
4. **Timed CAS Execution:** Starts the timer and executes `st.UpdateActor(ctx, updated, expectedVersion)`: `BEGIN → SELECT → UPDATE … WHERE version = $expected → COMMIT`. (A reduced-round-trip variant was benchmarked — ~25% faster p50 over Cloud SQL — and rejected as not worth the complexity; see atepg-optimizations.md §2.)
5. **Conflict Handling:** If another concurrent goroutine updated the same actor in between, the database matches 0 rows and returns `store.ErrVersionConflict`. `storebench` logs this as a `conflict` rather than an error.

---

### B. How `WorkerUpdate` is Executed

In [`opWorkerUpdate` in `bench.go`](file:///Users/shrutiyam/Documents/substrate/cmd/ateapi/storebench/bench.go):

1. **Random Worker Selection:** `keyPicker` picks a random worker from the standing worker pool (e.g., `worker-000042` in `pool-01`).
2. **Untimed Version Read:** Fetches the worker's current row.
3. **Payload Mutation:** Updates the worker's label map with a new unique test label.
4. **Timed Execution:** Executes `st.UpdateWorker(ctx, updated, expectedVersion)` (transactional read-check-write, like actors). Unlike actor updates, **worker writes also emit a watch event inside the transaction**, and the mechanism is the decisive performance variable (`ATEPG_CHANGE_FEED`):
   * **NOTIFY (feed OFF):** `pg_notify` in the write transaction. PostgreSQL globally serializes notifying commits — measured hard ceiling ~600 worker writes/s regardless of instance size (tens of seconds p50 at the 100M tier).
   * **Change feed (feed ON):** the transaction instead inserts the event into the `worker_changes` table (transactional outbox; watcher polls every 100 ms). Parallel commits — measured ≥2k worker writes/s. See bench-results/notify-vs-feed.png.

---

### C. Actor-to-Worker Association: `storebench` vs Real Substrate Lifecycle

| Aspect | In `storebench` (Synthetic Storage Benchmark) | In Full Substrate Lifecycle (`ateapi` Control Plane) |
|---|---|---|
| **Actor State** | Actors remain `STATUS_SUSPENDED` with synthetic selector label updates. | Actors transition through `STATUS_RESUMING` $\rightarrow$ `STATUS_RUNNING` $\rightarrow$ `STATUS_SUSPENDING`. |
| **Worker Association** | Actors and Workers are updated **independently and randomly** across the dataset to measure pure database concurrency. | The Substrate Scheduler matches `actor.worker_selector` with ready workers, assigning a specific worker pod (`actor.worker_assignment = 'worker-xxx'`). |
| **Purpose** | Maximizes raw database read/write throughput and tests Optimistic Concurrency Control (OCC) under contention. | Orchestrates live container memory restore, gVisor sandboxing, and request routing. |
