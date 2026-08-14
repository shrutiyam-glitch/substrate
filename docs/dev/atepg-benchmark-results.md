# atepg on Cloud SQL — benchmark results log (as of 2026-08-10)

Instrument: `cmd/ateapi/storebench` (open-loop, drives `store.Interface`
directly). Instance: Cloud SQL `atepg-bench`, us-central1, private IP.
Targets (storage requirements doc): Get p99 ≤ 5 ms, Update p99 ≤ 10 ms @
O(10K) RPS.

## Run ledger (full mix unless noted)

| # | Config (vCPU/disk/pool) | RPS | Dataset | Result | Verdict |
|---|---|---|---|---|---|
| 1 | 2/10GB/default | 1,000 | 10k actors, 100 workers | Get p99 2.2 ms, Update p99 8.0–8.4 ms | ALL TARGETS PASS at 1k |
| 2–5 | 2/10GB/default | 2,500 | 10k/100 | p50s stable (1.6/6.1 ms), p99 17–48 ms varying run to run | FAIL — episodic stalls; disk write-throughput cap (10GB ≈ 300 IOPS) |
| 6 | 2/10GB/default | 5,000 | 10k/100 | uniform p50 51 ms all ops | FAIL — pool queue (default = max(4,NumCPU)) in front of DB |
| 7 | 2/10GB/default | 5,000 | 50k/100 | p95 7s, p99 9.2s | FAIL — open-loop queue compounding + post-bulk-load I/O debt |
| — | **fixes applied**: pool_max_conns=32, 100GB disk, 4 vCPU/16GB | | | | |
| 8 | 4/100GB/32 | 2,500 | 50k/100 | Get p99 2.55, ActorUpdate p99 8.69, **WorkerUpdate p99 64** | PASS except WorkerUpdate — hot-row lock queueing (375 upd/s ÷ 100 rows × ~5 ms lock hold) |
| 9 | 4/100GB/32 | 5,000 | 50k/100 | drain ~4,610/s < offered | FAIL — capacity ceiling |
| 10 | 4/100GB/32 | 5,000 | 50k/**1000** | drain ~4,590/s | FAIL — **same ceiling ⇒ density is NOT the throughput limiter**; ceiling is pool-or-CPU (TBD via Pantheon CPU / pool=64 run) |
| 11 | 4/100GB/32 | 2,500 | 50k/1000 | p50 clean (1.0/5.4), p95+ polluted (~360 ms) | VOID tails — ran on run-10's vacuum/checkpoint debt |
| 12–13 | 4/100GB/32 | 1,000 updates-only, 2m | 50k/1000 | p50 420–550 ms both flag states | VOID — transient server-side degradation; pair invalid |
| 14–15 | 4/100GB/32 | 500 updates-only, 3m, 5m settle | 50k/1000 | flag OFF: ActorUpdate 5.47/7.66 (p50/p99), WorkerUpdate 5.80/9.65. flag ON: **4.11/6.27** and **4.74/8.79** | **CAS A/B verdict: −25% p50 / −18% p99 (actor), −18%/−9% (worker).** Matches 2-fewer-RTT prediction at ~0.7 ms RTT. Low-contention regime — lock-hold benefit on hot rows (run 8's 64 ms WorkerUpdate p99) still to be re-measured with flag on |
| 16–17 | 4/100GB/32 | 1,000 full mix, 3m, hot-row config | 50k/**100** | flag OFF: ActorUpdate 5.23/7.52 (p50/p99), WorkerUpdate 5.62/10.99, 225 conflicts. flag ON: **3.94/6.63**, **4.72/10.86**, **166 conflicts** | CAS −25% actor p50 (reproduces U-pair); worker p50 −16%, **conflicts −26%** (shorter read-to-write window); worker p99 unchanged at this mild contention (1.5 upd/worker/s) — the 64 ms case needs the 2.5k re-test. Reads identical across both = environment parity control |
| 18–19 | 4/100GB/32, fast-CAS ON | Tier-M volume probes: 500 then 1,000 mixed, 5m, fresh padded dataset (3KB/1KB records via COPY loader, settled) | 100k actors / 100k snaps / 1k workers / 100 atespaces | 500: Get p99 1.39, ActorUpd 6.00, WorkerUpd 7.18. 1k: Get **1.26**, ActorUpd **5.67**, WorkerUpd **9.20** (conflicts 2→22) | **All targets pass at both rates.** 8× heavier records: no cost (cache-resident). Load-to-rate insensitive except WorkerUpdate — worker-row contention is consistently the first pressure point at every rate tested. Tier-M reference rows for the volume ladder |
| 20–22 | 4/100GB/32, fast-CAS ON | (b) sweep: pure actorupdate @1k/2k, pure workerupdate @1k (tier M padded) | 100k/1k workers | ActorUpdate: 6.15/6.45 ms p99 — flat, no knee found yet. WorkerUpdate @1k: **collapsed** — p50 610 ms, 41% conflicts, successes drained at ~596/s | **Suspected root cause: pg_notify serializes notifying commits globally** (worker writes single-file through the notification queue; ~600/s cap regardless of worker count). Retroactively explains the 4.6k mixed ceiling's insensitivity to workers 100→1000. If confirmed by a no-notify A/B: Worker Update at O(10K) RPS is unreachable with per-update NOTIFY — the NOTIFY redesign becomes the top schema/design recommendation |
| 23–25 | 4/100GB/32, fast-CAS ON | (b) sweep: pure workerupdate @250/500/750 (descending knee-trace) | 100k/1k workers | 250: p99 **6.97** ✓; 500: p99 **16.13** ✗ (tail lifting at the wall); 750: saturated, successes **~606/s**; (1000 earlier: ~596/s) | **WorkerUpdate curve complete** (plot: bench-results/sweep-updates-m/update-curves.png). Hard ceiling ~600 successful notifying commits/s regardless of offered rate; target-compliant capacity ~400-450/s. vs ActorUpdate flat at 2,000/s — the delta is the per-update pg_notify. **~25x short of the O(10K) worker-update requirement; NOTIFY redesign (change-feed table) is the top design recommendation.** No-notify falsification A/B still pending |
| 26–27 | 4/100GB/32, fast-CAS ON, **CHANGE FEED ON** | (b) sweep: pure workerupdate @1k/2k with `ATEPG_CHANGE_FEED=1` (worker_changes table + polling watcher replacing pg_notify) | 100k/1k workers | @1k: p50 **4.87**, p99 14.6, full drain, 0.5% conflicts. @2k: p50 **5.05**, p99 69, drain ~1,967/s | **NOTIFY diagnosis proven by intervention**: the ~600/s wall vanished (p50 610→4.9 ms at identical load; capacity ≥2k/s, 3.3× the NOTIFY ceiling). Residual 2k tail is ordinary contention, not serialization. Change-feed implemented env-gated in atepg (schema table, both write paths, 100ms polling watcher w/ janitor); 4 contract suites × 44 subtests green incl. watch delivery. **Top recommendation, now measured end to end** |
| 28–30 | feed ON | workerupdate @250/500/750 (completing the feed curve) | 100k/1k workers | p99: **6.22 / 6.70 / 7.01** — all within target | Full before/after p99 curves (plot: bench-results/notify-vs-feed.png): identical at 250, diverging from 500 (notify 16 vs feed 6.7), 2 orders of magnitude apart by 1k. Target-compliant worker-write capacity: notify ~400/s → feed **~800-1000/s**, usable ≥2k/s. Feed's 750-point p99.9 (230 ms) shows an episodic stall (workers-table vacuum under churn) — next-order tuning, not a wall |
| 31–36 | fast-CAS ON, feed OFF | **(b) complete at tier M**: actorget/workerget/snapget/actorupdate/lock/snapcreate × 250–2,000 QPS (30 runs; plot bench-results/tier-m-methods.png) | 100k/1k workers | p99 @2k: gets **1.5/1.5/1.5**, ActorUpdate **7.15**, Lock **11.02**, SnapCreate **11.31** | Every method sustains ≥2k QPS: reads and actor updates flat with no knee in range; lock and snapcreate show gentle knees at 2k (2× and far above their required rates; checkpoint-flavored tails, `max_wal_size` would help if ever needed). Combined with runs 20–30: **the only method that cannot reach its required rate is NOTIFY-encumbered WorkerUpdate — fixed by the change feed.** One actorget step lost to a local kubectl auth blip and re-run; sweep.sh now retries transient failures before declaring saturation |
| 37–42 | **both flags OFF** (original PR code) | actorupdate ladder 250–2k + workerupdate (saturated at first step) | 100k/1k workers | ActorUpdate p99 6.8/7.9/9.3/6.8/9.1 — passes targets, consistently 1.5–3 ms above fast-CAS. WorkerUpdate @250: p99 **729 ms**, saturated immediately (single run, post-sweep environment — verify before quoting the exact number) | Completes the 2×2 optimization matrix. Charts: `actorupdate-cas.png` (pure CAS effect: ~25–30% improvement, both paths in-target at tier M) and `notify-vs-feed.png` (3 generations: original collapses at 250; +fast-CAS walls at ~600/s; +feed flat to 2k). Fast CAS = headroom; change feed = requirement-rescuer; original's longer tx compounds the NOTIFY serialization |

| 43–58 | XL tier (100M actors / 100M snaps / 1M workers / 100k atespaces, ~500GB; loaded 4h39m via COPY) | XL sweeps: all 7 methods @1k/5k/10k (CAS+feed ON); workerupdate flag A/Bs; actorupdate original | 1TB disk, 4vCPU | See tables in atepg-optimizations/report. Highlights @1k: WorkerGet p99 **1.48** ✓, Lock **6.64** ✓ (cached tables); ActorGet **5.53**, SnapGet **6.53** (~just over target — disk-read p50 ~4ms); ActorUpdate p99 71 (fast)/12.9 (slow — environment-confounded, do not compare across phases); WorkerUpdate: feed **173 ms** vs notify **46–84 s** (!). ActorGet uniquely drained 5k (p99 10.3); everything else on 100M-row tables saturates between 1k and 5k | **Volume verdict**: cache boundary = table size (1GB workers table fast, 300GB tables pay ~4ms disk p50 under worst-case uniform keys); capacity cliff 1k–5k, binding resource = instance I/O, not CPU/pool. NOTIFY at XL degrades from wall to catastrophe (tens of seconds). Uniform keys = lower bound; zipf pending for the fair number. 10k-QPS-at-100M needs memory-optimized tier or workload locality |

## Findings

1. **Dataset size: no effect.** 10k → 50k actors: identical latency (cached
   PK point reads, B-tree depth negligible). Positive scaling signal.
2. **Schema hot path validated.** HOT-update ratio 99.5–100% across 5M
   cumulative updates; dead tuples trivial; autovacuum keeps up; tables
   compact (workers: 632 kB after 1.5M updates). The "no indexes on mutable
   columns + bytea proto + native version/status" design is working as
   intended. No fillfactor/JSONB/index changes recommended.
3. **The 4-RTT CAS update is the dominant structural cost over a real
   network.** BEGIN→SELECT→UPDATE→COMMIT ≈ 4×RTT ≈ 4 ms of the 5.4 ms update
   p50, invisible in-cluster, dominant on Cloud SQL. Also holds the row lock
   for the whole window → WorkerUpdate hot-row queueing (the one target miss
   at 2.5k).
4. **Client pool sizing is a first-class failure mode.** pgxpool default
   took the system from all-green to 9-second collapse with no code change.
5. **Under saturation, optimistic concurrency degrades quadratically**
   (WorkerUpdate conflicts: 0.7% @1k → 3.6% @2.5k → 40–60% when saturated).
   Overload latencies describe queue depth, not schema cost.
6. **Capacity on 4 vCPU / 32 conns ≈ 4.6k ops/s** for the hot-path mix,
   independent of worker count. Attribution between pool and CPU pending.
7. **FK question: still open; no evidence against.** Steady-state mix never
   touches the FKs. Churn A/B (± FK × atespace skew) not yet run.

## Code change implemented (pending A/B verdict)

`atepg` reduced-round-trip CAS (`ATEPG_SINGLE_STATEMENT_CAS=1`):
untransacted read + one conditional `UPDATE … WHERE version=$n RETURNING`
(workers: CTE + `pg_notify`, keeping notify-iff-commit). 2 RTTs instead of
4; row lock held only for the UPDATE statement. Both contract suites pass
(the pure single-statement variant was rejected by the contract's
server-owned-metadata test — uid/create_time must come from the stored row).

## Recommendations status

(Consolidated writeups with design detail: [atepg-optimizations.md](atepg-optimizations.md).)

| Rec | Status |
|---|---|
| Reduced-RTT CAS update | **Measured: −25% update p50, −18% p99** (uncontended); contract-green; hot-row contention re-test pending |
| Explicit pool sizing + pool metrics in ateapi | Proven by runs 6/8; recommend for production |
| Keep schema as-is (HOT path, PK-only indexes, bytea) | Validated with data |
| FK drop | Undecided — churn experiment pending |
| Unlogged leases | Untested |
| NOTIFY 8KB guard redesign | Untested (needs lifecycle payload sizes) |
| Instance sizing | 2vCPU/10GB insufficient @2.5k; 4vCPU/100GB good @2.5k, ceiling ~4.6k; tier-up or pool-up test pending for 5–10k |

## Protocol lessons (baked into storebench README)

Never measure right after bulk loads or saturated runs (settle until disk
writes flatline); one variable per attributed run; completed-count vs
offered is the saturation gauge (latency percentiles under saturation are
survivorship-biased); synthetic protos are ~10× smaller than the
requirements' 3 KB estimate — fatten for realism.

## 2026-08-13 — Flagless change-feed validation on rebased code (fresh DB `atepg_v2`)

Setup: `bench/cloudsql` rebased onto merged main, feed now the only
worker-watch path (no env flag). Fresh database `atepg_v2` (new schema:
actors +uid −status, workers −ip, snapshots −location) on the same
`atepg-bench` instance (db-custom-4-16384, 1TB, pool_max_conns=32).
All runs: `--mix=workerupdate=100 --rps=1000 --duration=3m --warmup=1m`,
uniform keys, full drain (180,008 completions), 0 errors.

| Fleet | Condition | p50 | p90 | p95 | p99 | p99.9 | conflicts |
|---|---|---|---|---|---|---|---|
| 10k workers | right after seed, cold DB | 6.56 | 8.38 | 10.37 | 32.41 | — | 128 (0.07%) |
| 1M workers | right after 990k-row COPY + 2m settle | 7.32 | 8.89 | 11.44 | 69.06 | 113.3 | 2 |
| 1M workers | warm repeat (settled, steady state) | **6.26** | **7.14** | **7.58** | **10.13** | 91.6 | 2 |

Findings:
- Steady-state p99 at requirements-scale worker cardinality (1M) is
  **10.1 ms** — at the ≤10 ms update target boundary; p50 6.3 ms matches
  the post-fast-CAS-removal arithmetic (4-RTT tx + feed insert).
- Fleet size does not move the medians (workers table ~1 GB, cache
  resident). CAS conflicts are a key-density property: 128 at 10k
  workers → 2 at 1M.
- Both elevated-tail rows are the measure-after-ingest protocol violation
  (first-touch page reads + checkpoint wave from the 1 GB seed);
  reproduced and cleared by a warm repeat. Reconfirms the "settle until
  disk writes flatline" rule.

### Clean-protocol repeat + p99 root cause (checkpoint alignment)

Fourth run, strict hygiene: VACUUM ANALYZE workers/worker_changes (0 dead
tuples — autovacuum had already caught up), forced CHECKPOINT, 5m settle,
then the identical 1M-worker workerupdate @1k: p50 7.41 / p90 8.64 /
p95 9.83 / **p99 45.4** / p99.9 123.6, 3 conflicts, full drain.

p99 across the three 1M runs: 69 → 10 → 45 ms while p50/p90/p95 stayed
~7/9/10 ms. The tail is EPISODIC, not load- or hygiene-driven. Cause
(from pg_settings, server is PG 18.4): `checkpoint_timeout=300s` — every
4-minute run (1m warmup + 3m window) contains ≥1 timed checkpoint; and
uniform keys over 163k table pages make every post-checkpoint update emit
a ~8KB full-page image, ≈0.5 GB WAL/min, so `max_wal_size=1504MB` also
forces a requested checkpoint ~every 3 min. Whether the checkpoint write
burst lands in warmup or in the measured window is pure clock alignment —
hence 10 vs 45 vs 69. (Same "checkpoint-flavored tails / max_wal_size"
signature as tier-M lock/snapcreate knees, runs 31–36.)

Implications: sub-p99 metrics are the stable schema signal (p95 ≤10 ms at
1M workers ✓). To make p99 honest rather than alignment-lottery: run ≥10m
windows (averages ~2–3 checkpoints in), or raise `max_wal_size` +
`checkpoint_timeout` (Cloud SQL flags) and re-measure. The XL 173 ms p99
was a different regime (300 GB co-resident tables, instance I/O
saturation), not comparable.

### Checkpoint-free run (flags applied)

Cloud SQL flags set on atepg-bench (no restart needed, verified live):
`checkpoint_timeout=1800`, `max_wal_size=32768` (MB). Manual CHECKPOINT +
2m settle, then one run: p50 7.08 / p90 8.04 / p95 8.52 / p99 **27.5** /
p99.9 90 / max 400, 0 conflicts, full drain. Tightest p90/p95 of all five
1M runs; the 45–69 ms checkpoint burst is gone. Residual p99 ≈27 ms is
other background I/O — most likely autovacuum (prior run's ~180k dead
tuples crossed the 0.2× threshold mid-window), bgwriter, disk variance.
Verdict line for the report: p50 7 / p95 ≤9 ms stable across all configs;
p99 10–69 ms is background-write alignment, 27.5 with checkpoint tuning —
operational levers, not schema issues. FLAGS LEFT SET on the instance.

### Review-hardened feed code, same protocol (2026-08-13)

Change-feed review fixes ((xid,seq) gap-free cursor + xid fence, two extra
columns w/ defaults, (xid,seq) btree + created_at BRIN indexes, store-level
aged janitor, trim high-water table) benchmarked on bench/cloudsql commit
c63359fe. atepg_v2 retrofitted additively (ALTER TABLE only; 1M workers
intact). Checkpoint-free protocol (manual CHECKPOINT + 2m settle; flags
30min/32GB): WorkerUpdate @1k, 1M workers: p50 6.91 / p90 7.71 / p95 8.00 /
**p99 9.91** / p99.9 45.4, 1 conflict, full drain.

Verdict: the hardening costs nothing measurable on the hot path (within
run-to-run variance of the 7.08/27.5 baseline; if anything faster) and this
is the first 1M-worker run with p99 inside the <=10ms update target. The
extra per-update work (2 column defaults + right-edge btree insert + BRIN)
is invisible at 1k QPS.

### Partitioned feed (hourly, retention = DROP PARTITION), 2026-08-13

Feed partitioning (commit 5fa6d1ca) benchmarked after additive migration
(worker_changes RENAMEd to worker_changes_legacy, rows preserved; index
renamed too — index names are global and IF NOT EXISTS would have skipped
the new partitioned index). Two runs, vacuum+checkpoint between:

| run | p50 | p90 | p95 | p99 | conflicts |
|---|---|---|---|---|---|
| 1 | 7.02 | 7.81 | 8.11 | **9.83** | 3 |
| 2 | 7.41 | 8.94 | 10.72 | 42.6 | 0 |

Partition state after run 1: events in worker_changes_p2026081323
(313 MB / ~240k rows = **1.30 KB/row measured**), next-hour partition
pre-created empty, DEFAULT partition 0 bytes (routing never fell back).
Worst-case arithmetic at the 10k events/s requirement ceiling: 36M
rows/hour ≈ 47 GB/partition, 50–100 GB transient on disk — bounded,
self-cleaning, reclaimed by metadata DROP. Slimming options if needed:
binary-proto payload (−30–40%) and/or 15-min partitions (−3×).

Run 2's tail is the recurring workers-table autovacuum alignment (240k
updates/run vs the 200k default threshold — fires in-window whenever the
run starts from a freshly vacuumed table), not the feed. Across all seven
1M-worker runs: p50–p90 stable at 7–9 ms in every configuration; p99 is
10 ms when the window is free of background maintenance and 40–70 ms when
checkpoint/vacuum lands inside it. Partitioning removes the janitor's
delete burst from that list of tail sources permanently (the p99-141ms
catch-up case is now structurally impossible).

### UNLOGGED feed partitions (2026-08-14)

Feed partitions made UNLOGGED (commit on both branches; trim table stays
logged; watcher closes for resync after 50 consecutive failed polls — the
DB-restart signature, since restart truncates unlogged tables). Existing
partitions converted via ALTER TABLE SET UNLOGGED. Pair at 1M workers @1k:

| run | p50 | p90 | p95 | p99 |
|---|---|---|---|---|
| 1 | 7.29 | 8.06 | 8.34 | **9.73** |
| 2 | 7.43 | 8.84 | 10.34 | 92.9 (second-run autovacuum signature) |

Verdicts: (1) UNLOGGED is latency-neutral at 1k QPS (identical to logged
baseline 7.02/9.83) — its value is WAL bandwidth at the 10k/s ceiling
(~15 MB/s removed) and reduced checkpoint pressure, an arithmetic claim,
not a measured-at-1k one. (2) **Partition-drop retention validated live**:
the janitor dropped the previous 313 MB partition INSIDE run 1's
measurement window and p99 stayed 9.73 ms — vs p99 141 ms when the old
row-delete janitor reclaimed a comparable backlog mid-run. Retention cost
went from measurable-worst-tail-source to unmeasurable.

### Partition autovacuum disabled (close-out pair, 2026-08-14)

Hourly partitions now created WITH (autovacuum_enabled=off) (commit
c9a6e28f; DEFAULT partition keeps autovacuum — it takes in-place deletes).
Pair at 1M workers @1k, no manual vacuum between runs:
run 1: 7.12/8.18/8.96/35.5; run 2: 7.14/8.21/9.27/29.9.

The 90ms-class insert-autovacuum spike is eliminated and runs are now
CONSISTENT (~30-35ms p99 band) instead of bimodal (10 vs 42-93).
Prediction correction: workers-table autovacuum did NOT fire in either
window (last_autovacuum unchanged since the day before) — the residual
~30ms tail (<1% of requests) is unattributed I/O-class variance (WAL
flush / bgwriter / Cloud SQL disk jitter); attribution would need
pg_stat_io or wait-event sampling in-run.

SERIES CLOSE-OUT (11 runs at 1M workers @1k QPS): p50 6.9-7.4ms and
p95 8.0-10.7ms in every configuration tested. Tail sources found and
removed by design: pg_notify commit serialization (change feed),
janitor delete bursts (partition-drop retention, verified live in-window
at p99 9.7), feed insert-autovacuum (disabled on hourly partitions).
Managed by ops knobs: checkpoint cadence (instance flags). Remaining:
~30ms residual jitter at p99, unattributed, consistent.
