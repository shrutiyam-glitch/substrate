# Cloud SQL Postgres Benchmarking — Meeting Prep (Max / Julian)

My working notes for finalizing goals, strategy, and open questions. Sources:
the Substrate Storage requirements doc, the community Postgres benchmarking doc
(Jul 27, 2026, supreme-gg-gg fork), PR #640 (`atepg`), and last meeting's notes.

## 1. Purpose (as I understand it)

Decide whether a Postgres-backed store — specifically **Cloud SQL** — can replace
Valkey as the control-plane state store, by validating the `atepg` schema against
the documented latency/volume targets. Valkey's latency is fine; the motivations
to move are **durability** (Redis loses its write-speed edge once you force
`appendfsync always`, where the community benchmark showed Postgres beating it on
every RPC) and **operability cost** of running a 6-node Valkey cluster.

The specific open design question I'm evaluating: **do the foreign keys in the
atepg schema create contention at scale, and are they worth it?**

## 2. What I know so far

### The schema under test (PR #640, `cmd/ateapi/internal/store/atepg`)

- Six tables: `atespaces`, `actors`, `actor_snapshots`, `actor_snapshot_tags`,
  `workers`, `leases`. Full binary proto in `bytea` + native columns only for
  keys, `version` (CAS), and actor `status`.
- **Foreign keys with `ON DELETE RESTRICT`**: `actors → atespaces`, and
  `actor_snapshot_tags → atespaces` + `actor_snapshots`. These enforce the
  "can't delete a non-empty atespace" constraint in the database.
- Hot-path ops: `GetActor` = single point read by PK; `UpdateActor` =
  transaction (point read → version check → conditional `UPDATE ... RETURNING`).
- Watches = `LISTEN`/`NOTIFY` (transactional, but 8,000-byte payload hard limit —
  oversized worker events *fail the write*). Leases = a `leases` table with
  `expires_at`, same TTL/renewal constants as ateredis.
- Only PK indexes exist. No secondary indexes.

### Tension between the requirements doc and the schema

The requirements doc describes the model as "**no-relational resources (i.e. no
foreign keys)**", and notes the atespace-delete constraint is "today not enforced
in the ValKey impl." PR #640's schema *chose* to add FKs to get that constraint
enforced. So the FK question isn't just performance — it's whether the schema
should follow the requirements doc's model (app-enforced, like Valkey) or upgrade
it (DB-enforced). **Mechanically, the risk**: every actor INSERT/DELETE takes a
`FOR KEY SHARE` lock on its atespace row, so high create/delete churn concentrated
in few atespaces contends on those rows. Steady-state Get/Update shouldn't touch
the FK at all — testable hypothesis.

### Targets from the requirements doc

| Dimension | Target |
|---|---|
| Cardinality | Actors O(1B), Snapshots O(1B), Workers O(1M) |
| Hot-path reads (Actor/Worker/Snapshot Get) | p99 ≤ 5 ms @ O(10K) RPS |
| Hot-path writes (Actor/Worker Update, Snapshot Create) | p99 ≤ 10 ms @ O(10K) RPS |
| Lease acquire | p99 ≤ 10 ms @ O(1K) RPS |
| Lists (≤1000/page) | p99 ≤ 250 ms, low RPS |
| Worker watch | delivery ≤ 1 s @ O(10K) events/s **per subscribed replica** |

Note the RPS column assumes O(1K) suspend/pause/resume workflows per second.

### What the community benchmark already established (so I don't redo it)

- Setup: existing Locust framework on GKE; in-cluster single-replica Postgres
  (2 CPU / 2 GB) vs 6-replica Valkey; mock atelet returning after 1 ms.
- Default-config Redis wins creates/mutations; point reads ~tied; Postgres wins
  lists. **Durable Redis (`appendfsync always`) loses to Postgres on every RPC.**
- Lifecycle at 100k actors / 1k workers / 1k users: Redis initially +24%
  throughput, but after giving Postgres 16 pool connections + 2 CPUs, Postgres
  passed it (1,019 vs 889 RPC/s). Bottleneck was resource limits, not the engine.
- Lifecycle cost (~300 ms) is dominated by checkpoint/restore, not the store.
- Reproduction: `benchmarking/automation/run.py` (pod or job mode, results to
  GCS), tests defined in YAML (`tests-lifecycle-scale-comparison.yaml`),
  summarize with `summarize_results.py`. Cleanup via
  `benchmarking/lifecycle-scale/deploy.sh --delete`.

### What the community benchmark did NOT cover (→ my job)

1. **Cloud SQL** — everything so far was an in-cluster StatefulSet. Managed
   Postgres changes network latency, connection limits, HA semantics, auth.
2. **Scale**: max tested was 100k actors. Targets go to 500k in my ladder and
   O(1B) aspirationally. Does point-read/update p99 hold as table + index grow?
3. **p99 discipline**: community doc mostly reports averages (appendix has some
   percentiles). Requirements are p99.
4. **FK contention isolation**: no FK-on vs FK-off comparison, no
   namespace-density variation, no create/delete churn focus.
5. **Watch throughput**: LISTEN/NOTIFY at O(10K) events/s per replica untested;
   payload-limit behavior untested.
6. **Lease ops at O(1K) RPS** untested.

## 3. Proposed strategy (to validate with Max/Julian)

Two layers: a **store-level Go microbenchmark** (drives `store.Interface`
directly against Cloud SQL — isolates schema + DB from gRPC/K8s; implemented
as `cmd/ateapi/storebench`, run via `benchmarking/storebench/job.yaml`) and
the **end-to-end Locust lifecycle suite** (would come from the community
fork's harness — ported, later removed unused from the branch when
storebench became the sole instrument; re-fetch with
`git fetch https://github.com/supreme-gg-gg/substrate.git benchmark/postgres`
if that layer is ever scheduled). Store-level finds the schema's limits; end-to-end confirms the system
inherits them.

### 3.1 Dataset shapes (the scale ladder)

Default density 100:1 actors:workers (community setup used this; the O(1B)
actors / O(1M) workers requirement implies ~1000:1 — density variants below).
Snapshots ≈ actors (every suspended actor has one). Workers are spread over
pools like real deployments (a pool per sandbox class).

| Rung | Actors | Snapshots | Workers | Worker pools | Atespaces | actors table size |
|---|---|---|---|---|---|---|
| S | 10k | 10k | 100 | 2 × 50 | 10 | ~30 MB |
| M | 100k | 100k | 1,000 | 4 × 250 | 100 | ~300 MB |
| L | 500k | 500k | 5,000 | 10 × 500 | 500 | ~1.5 GB |
| XL (stretch) | 10M | 10M | 100k | 20 × 5,000 | 10k | ~30 GB |

Variants applied at rung M (the pivot rung):
- **Density**: 1000:1 (100 workers — Spark-like, sandbox-heavy) and 10:1
  (10k workers — Rail-like, resource-heavy), vs the 100:1 default.
- **Atespace skew**: all actors in 1 atespace (worst case for FK row locks) /
  100 atespaces (default) / 10k atespaces (best case).
- **Schema**: FK vs no-FK (identical except the two `REFERENCES` clauses).

XL exists to answer "does point-read/update p99 survive when the table and
B-tree no longer fit hot in cache" — bulk-loaded via `COPY`, read/update-only.

### 3.1b Why the preload dataset is required

Without preloading, a CRUD test only ever has ~`users` live actors (each
iteration creates and deletes its own), so every measurement runs against an
essentially empty database — flattering and meaningless. Latency depends on
standing data volume: B-tree index depth grows with row count (point
reads/updates), list pagination scans real pages, buffer-cache hit ratio drops
once tables outgrow RAM, and autovacuum has actual work. Preloading pins the
**dataset size** (the rung: 10k → 500k) independently of the **offered load**
(users/RPS), so a latency change between rungs is attributable to data volume
alone — the load profile is held constant across rungs.

### 3.2 Load profiles

Open-loop (fixed arrival rate), not closed-loop — closed-loop user counts hide
tail latency, and our targets are p99 at a given RPS. Each measurement: ~2 min
warmup, 10 min measured, 3 repeats. Longer than the community's 90 s runs on
purpose: autovacuum/bloat effects from update churn don't show up in 90 s.

Per-op RPS steps, derived from the requirements table (targets assume O(1K)
lifecycle workflows/s):

| Workload | Mix | RPS steps |
|---|---|---|
| Steady-state hot path | ActorGet 30%, ActorUpdate 25%, WorkerGet 15%, WorkerUpdate 15%, SnapshotGet 10%, SnapshotCreate 5% | 1k → 5k → 10k → until p99 breaks |
| Churn (FK experiment) | ActorCreate 40%, ActorDelete 40%, ActorGet 20% | 200 → 1k → 5k |
| Lease | Acquire/renew/release cycles on contended + uncontended keys | 250 → 1k → 2k |
| Watch | Worker update generators + N listening replicas | 1k → 5k → 10k events/s, 1/3/5 subscribers |
| List | ListActors pages of 1000, walking full keyset | constant low rate, run against L/XL |

### 3.3 Metrics and pass/fail

Client-side (the verdict): per-op **p50 / p95 / p99 / max** latency, achieved
vs offered RPS, error rates split by type (version conflict, lock conflict,
timeout, connection). Pass = p99 within target at the target RPS:

| Op | p99 target | at RPS |
|---|---|---|
| Actor/Worker/Snapshot Get | ≤ 5 ms | 10k |
| Actor/Worker Update, Snapshot Create | ≤ 10 ms | 10k |
| Create/Delete (non-hot) | ≤ 50 ms | low |
| List page (≤1000) | ≤ 250 ms | low |
| Lease acquire | ≤ 10 ms | 1k |
| Watch delivery | ≤ 1 s | 10k events/s per replica |

DB-side (the diagnosis, scraped each run):
- `pg_stat_statements`: per-query mean/stddev, calls — which statement degrades.
- Lock contention: `pg_locks` waits sampled + `log_lock_waits=on`; specifically
  waits on `atespaces` rows during churn (the FK hypothesis is confirmed or
  killed by this one signal).
- Bloat/vacuum: dead-tuple counts, autovacuum frequency/duration, **HOT-update
  ratio** on `actors` (`pg_stat_user_tables.n_tup_hot_upd`) — every update
  rewrites a ~3 KB row; if HOT ratio is low, test `fillfactor=70`.
- Cache: `pg_stat_database` blks_hit/blks_read (buffer hit ratio), esp. at XL.
- WAL generation rate; replication lag when testing regional HA.
- Cloud SQL instance: CPU, memory, disk IOPS/throughput, connection count.

Client-pool-side: pgxpool `AcquireDuration` / `EmptyAcquireCount` (queueing in
the pool masquerades as DB latency; the community's 4-connection default did
exactly this).

### 3.4 Stages

Each stage gates the next; a failed gate means diagnose (pg_stat_statements,
locks) and either tune or report, not silently continue.

1. **Setup validation** — run the `storecontract` suite against a Cloud SQL
   instance; deploy PR #640 on GKE with `--store-backend=postgres` pointed at
   Cloud SQL; one manual actor lifecycle end-to-end. Gate: everything green.
2. **Baseline @ S** — steady-state + churn + lease profiles, store-level.
   Gate: all p99 targets met at S (if not, the schema fails before scale even
   matters).
3. **Scale ladder @ M, L** — repeat stage 2; plot p99 vs dataset size per op.
   Gate: targets hold at L; latency growth curve flat-ish (B-tree depth, not
   linear degradation).
4. **FK experiment @ M** — churn profile × {FK, no-FK} × {1, 100, 10k
   atespaces}. Deliverable: the p99 delta and `atespaces` lock-wait numbers
   that answer the FK question with data.
5. **End-to-end lifecycle** — ported Locust suite + mock atelet + lifecycle-
   scale preloader against Cloud SQL @ M (directly comparable to the community
   doc's in-cluster numbers), then @ L. Also the saturation profile (find max
   lifecycle RPC/s).
6. **Primitives** — watch/NOTIFY ladder (this is the least-proven part of the
   design; also measure payload sizes vs the 8 KB limit with realistic worker
   protos) and lease profile at 1k RPS.
7. **Stretch: XL** — 10M actors bulk-loaded, read/update only. Answers the
   "does this extrapolate toward O(1B)" question as far as budget allows.
8. **Config sensitivity (interleaved where relevant)** — zonal vs regional-HA
   (sync replication vs the 10 ms write budget), pool size (16 → 64 → 128),
   vCPU tiers. One variable at a time.

### 3.4b Optimization candidates (hypotheses each run can prove or kill)

The exercise's real deliverable: is the schema right, and what schema/code
changes would improve it. Candidates, cheapest-to-test first:

| # | Change | Layer | Evidence that decides it | Predicted impact |
|---|---|---|---|---|
| 1 | Pool size via DSN `pool_max_conns` (pgxpool default is max(4, numCPU)) | config | acquire-wait vs Cloud SQL conn count at 5k+/s | throughput at saturation (community saw exactly this) |
| 2 | `fillfactor=75` on actors/workers — updates are HOT-eligible (no updated column is indexed) but need page slack | schema | `n_tup_hot_upd/n_tup_upd`; update p99 drift over 30-min runs | sustained-update latency stability |
| 3 | Single-statement CAS update: drop atepg's BEGIN→SELECT→UPDATE→COMMIT (4 RTTs — why update p50 is ~4.6ms vs ~1.0ms gets) for one `UPDATE..WHERE version RETURNING`; classify failures after the fact | code | storebench A/B, same run config | update p50 ~4.6→~1.5-2ms; p99 8→~3ms. Likely the biggest single win |
| 4 | Drop FKs (`actors→atespaces`, tag FKs) | schema | churn mix × atespaces {1,10} × FK {on,off}; `atespaces` row lock waits | create/delete p99 under skewed churn; the standing design question |
| 5 | `UNLOGGED` leases table (lease writes are ephemeral; WAL is pure overhead) — caveat: truncated on crash recovery, semantics call for Julian | schema | `--mix=lock=100` logged vs unlogged | lease acquire p99 + WAL rate |
| 6 | NOTIFY payload redesign (send key only, watcher re-reads) if worker protojson nears the 8KB limit that today FAILS the write | code | payload sizes observed in lifecycle tests | removes a hard failure mode |

Confirm-not-change: PK-only indexing (verify no seq scans in
pg_stat_statements), bytea proto (not JSONB), native status column.
Sequence: complete the baseline sweep first, then A/B one variable at a time
against it.

### 3.5 Known risks to watch regardless of stage

- **Update-churn bloat**: ~3 KB row rewrites at 10k RPS → autovacuum pressure;
  HOT-update ratio and fillfactor are the levers.
- **NOTIFY**: global serialization of notifications at commit could cap watch
  throughput well below 10k events/s; payload limit fails writes outright.
- **Connection scaling**: Cloud SQL connection ceilings vs N ateapi replicas ×
  pool size; may force pgbouncer / managed pooling into the design.
- **Comparability trap**: the community fork's schema ≠ PR #640's schema (more
  native columns, no snapshot/tag tables). Port their harness, don't reuse
  their numbers.

## 4. Questions for the meeting

**Scope & purpose**
1. Is the deliverable a go/no-go on Postgres generally, on Cloud SQL
   specifically, or on this particular schema? What decision does this feed and
   by when?
2. Is PR #640's schema the schema of record, or the community fork's
   (`supreme-gg-gg/substrate` branch `benchmark/postgres`)? Are they identical?
3. The requirements doc says "no foreign keys" as a model property, but #640
   adds them. Is DB-enforced integrity a goal we *want* (test FK cost, keep if
   cheap), or should the benchmark assume the no-FK model and treat FKs as the
   experiment?

**Targets & workload shape**
4. The meeting notes say 10k–500k "workers" in one place and "actors" in
   another — confirm the ladder is actors, and what actor:worker density(ies)
   to test (Gemini Spark-like vs Rail-like — concrete ratios?).
5. Requirements say Actor cardinality O(1B) but we test to 500k. Is 500k
   considered sufficient signal, or do we need at least one large bulk-loaded
   dataset (say 10M–100M rows) to check index-depth/cache effects on point
   reads?
6. Are the p99 targets measured at the store API or end-to-end at the gRPC
   API? (5 ms end-to-end is a very different budget.)
7. Do we need to validate the watch path at O(10K) events/s, or is that out of
   scope for this round? (LISTEN/NOTIFY may be the weakest link and there's a
   hard 8 KB payload limit.)
8. Do we re-run Valkey baselines ourselves, or compare only against targets +
   the community doc's numbers?

**Cloud SQL setup**
9. What instance shape mirrors the intended production topology (tier, vCPUs,
   RAM, zonal vs regional HA, disk type)? HA synchronous replication directly
   affects the ≤10 ms write target — do we test both?
10. Connectivity/auth from GKE: private IP, Cloud SQL Auth Proxy /
    connector, IAM database auth? (atepg config is DSN-only today — does
    anything need code changes, e.g. IAM token refresh?)
11. Who provisions it / which project & budget do I use?

**Max's framework & logistics**
12. Where does Max's benchmarking framework live, what does it already cover,
    and what should I reuse vs build? Is it the same as
    `benchmarking/automation/` in the community fork or something newer?
13. Is the community fork's mock-atelet + preload tooling
    (`benchmarking/lifecycle-scale/`) usable as-is upstream, or does it need
    porting to PR #640's branch?
14. For the store-level Go microbenchmark: does something like this already
    exist (Max?), or do I write it? Preferred output format so results are
    comparable with the Locust runs?
15. Contacts: Jet and Eden (Solo team, fake-atelet approach) — which Slack
    channel, and what exactly should I sync with them on?

## 5. My immediate plan after the meeting

1. Pull PR #640, deploy substrate on GKE (`--store-backend=postgres` first
   in-cluster to validate the setup end-to-end).
2. Provision Cloud SQL per agreed shape; point ateapi's
   `ATE_API_POSTGRES_CONNECTION_STRING` at it; confirm the contract tests /
   basic lifecycle pass.
3. Build the bulk loader + store-level benchmark; run the 10k rung; sanity-check
   numbers against targets before scaling up.
4. Port/reuse the Locust lifecycle suite for layer 2.
5. Separately: trace the snapshot/suspend/resume flow in the API server
   (suspend = snapshot + release worker in DB; resume = pick worker + write
   actor→worker mapping + call atelet) — `internal/controlapi/workflow_*.go`.
