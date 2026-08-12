# atepg optimizations: findings & recommendations

What the Cloud SQL benchmarking found worth changing in the PostgreSQL
store (PR #640), what was implemented (all env-flag-gated on branch
`bench/cloudsql`, off by default = original PR behavior), and what was
measured. Evidence: [atepg-benchmark-results.md](atepg-benchmark-results.md)
(run ledger) and `bench-results/*.png` (charts). All optimized paths pass
the full `storecontract` suite (5 suite configurations × 44 subtests).

| # | Optimization | Flag | Status | Measured effect |
|---|---|---|---|---|
| 1 | Worker change feed (replace per-update `pg_notify`) | `ATEPG_CHANGE_FEED=1` | implemented, contract-green | worker-write ceiling **~600/s → ≥2k/s**; p50 at 1k QPS **610 ms → 4.9 ms**; **required** to ever reach the O(10K) worker-write target |
| 2 | Reduced-RTT CAS update | — | **measured, then REJECTED — code removed** | update p50 −25% was real but marginal in absolute terms (~1.3 ms) and rescued no requirement; simplicity won |
| 3 | Explicit pool sizing + pool metrics | DSN `pool_max_conns` | config + storebench metrics | default pool collapsed the system at 5k mixed QPS; 32 conns restored it |
| 4 | Actor change feed (write side) | — | **removed 2026-08-12** | no current consumer existed; deleted to keep the PR minimal (§4) |
| — | Schema itself | — | **validated as-is** | HOT updates 99.5–100%, no volume effect S→M, all reads 3–4× inside target |

## 1. Worker change feed (the critical one)

**Problem.** Every worker create/update/delete carries a `pg_notify` for the
`WatchWorkers` channel. PostgreSQL serializes the commits of *all* notifying
transactions through a global queue lock, held through the commit (fsync
included). Measured: a hard ceiling of ~600 successful worker writes/s —
independent of instance size, pool size, or worker count — versus the
requirements' O(10K) RPS. The original code (with the slower CAS path
holding the lock longer) saturated at ≤250/s. This also explains the
earlier mixed-workload ceiling (~4.6k ops/s) that was insensitive to worker
count. Chart: `bench-results/notify-vs-feed.png`.

**Design (transactional outbox).** A `worker_changes(seq, payload)` table;
the write statement inserts the event in the same transaction/CTE as the
worker write (delivery-iff-commit preserved exactly). `WatchWorkers` polls
past a cursor every 100 ms instead of LISTEN and trims old rows (janitor).
Plain inserts commit in parallel — the ceiling disappears. Bonus: the 8 KB
NOTIFY payload limit (which *fails writes* today) disappears too.

**Measured.** At identical load (1k pure worker updates/s): p50 610 → 4.9 ms,
p99 1163 → 14.6 ms, conflicts 41% → 0.5%, full drain. Sustains ≥2k/s.
Target-compliant capacity roughly 800–1,000/s on 4 vCPU (tail-limited by
ordinary contention, which responds to normal tuning — unlike the wall).

**Caveats / production hardening.** Polling adds ≤100 ms delivery latency
(target is ≤1 s). The naive cursor can skip a row whose older transaction
commits late — mitigate with a snapshot-xmin guard (few lines) and note the
watch contract is already best-effort + healed by workercache's periodic
relist (LISTEN/NOTIFY also drops events on disconnect). Janitor ownership
(which replica trims) needs a decision for multi-replica ateapi.

**Recommendation: adopt.** Without it the worker-write requirement is
structurally unreachable. Interim mitigations if adoption is deferred:
notify only on create/delete, or per-transaction `synchronous_commit=off`
for worker writes (defensible — workers are largely derived state).

**Prior art / references.** The design is the [Transactional Outbox
pattern](https://microservices.io/patterns/data/transactional-outbox.html)
combined with a [Polling Publisher](https://microservices.io/patterns/data/polling-publisher.html)
(Chris Richardson's catalog). The NOTIFY pathology is independently
documented: [Recall.ai, "Postgres LISTEN/NOTIFY does not
scale"](https://www.recall.ai/blog/postgres-listen-notify-does-not-scale)
(production outages from the same AccessExclusiveLock we hit) and the
[DBOS follow-up](https://www.dbos.dev/blog/postgres-listen-notify-scalability)
(explains the commit-ordering rationale; benchmarked ≤2.9k notifying
writes/s even on large instances — same order as our ~600/s on 4 vCPU).
Notably, an upstream fix (commit 282b1cde) is slated for **PostgreSQL 19**
(~Sept 2026), but current releases — and Cloud SQL for the foreseeable
future — retain the global lock, so the outbox remains the correct design
now. Heavier-weight alternative for large fan-out: transaction-log
tailing / CDC ([pattern](https://microservices.io/patterns/data/transaction-log-tailing.html),
[Debezium outbox](https://debezium.io/blog/2019/02/19/reliable-microservices-data-exchange-with-the-outbox-pattern/)).

## 2. Reduced-RTT CAS update — measured, then rejected (code removed)

Decision 2026-08-11: after full measurement, the team removed this change to
keep the PR minimal. The record below is retained because the measurements
inform future work (the RTT anatomy of updates is now known).

**Problem.** `UpdateActor`/`UpdateWorker` ran BEGIN → SELECT → UPDATE →
COMMIT: four network round trips (~4 ms of the ~5.4 ms update p50 on Cloud
SQL — invisible on an in-cluster DB, dominant over a real network), with
the row lock held for the whole window (queueing concurrent updates to hot
worker rows).

**Design.** Untransacted point read (server-owned metadata + immutability
checks) + one conditional `UPDATE … WHERE version = $expected RETURNING`
(workers: CTE that also emits the watch event). Two round trips; row lock
held only for the UPDATE statement. Failure classification moves to the
miss path (one extra read there — deliberately trades the rare case for
the common one). A pure single-statement variant was rejected by the
contract suite: uid/create_time are server-owned and must come from the
stored row.

**Measured.** Update p50 −25% (reproduced in two independent A/Bs), p99
−12–18%, hot-row conflicts −26%. Both paths pass targets at tier-M rates —
this is headroom, not a rescue. Chart: `bench-results/actorupdate-cas.png`.

**Outcome: not adopted.** The ~1.3 ms / 25% p50 saving is real and
reproduced, but the original path also met every target at every measured
in-cache rate, at XL the disk term dwarfs the RTT term, and the predicted
hot-row lock-hold benefit did not materialize at realistic contention. The
code (flag, fast paths, dedicated contract suite) was deleted; the ledger
runs and `bench-results/actorupdate-cas.png` remain the record. Reopen if a
future latency budget is ever ~1–2 ms short on updates.

## 3. Pool sizing & observability

pgxpool's default (`max(4, NumCPU)`) turned a passing system into a
9-second-latency collapse at 5k mixed QPS — pool queueing is invisible in
DB metrics and masquerades as backend latency. Recommendation: set
`pool_max_conns` explicitly (≥32 per replica for hot-path load), and export
pgxpool stats (acquired/max conns, empty-acquire count, acquire-wait time)
from ateapi the way storebench now does. Watch Cloud SQL `max_connections`
(~400 at 16 GB) vs replicas × pool size.

## 4. Actor change feed — built, then removed (2026-08-12)

A write-side twin of #1 was implemented behind ATEPG_ACTOR_CHANGE_FEED and
deleted once confirmed that **nothing consumes actor events**: there is no
WatchActors in store.Interface and no component needs one (actors are
request-scoped, CAS forces fresh reads, leases coordinate workflows). If
actor watches are ever proposed, the worker feed (#1) is the pattern to
copy — transactional outbox insert per mutation + polling watcher.

## Validated as-is (no change recommended)

- **Hot-path schema design**: no indexes on mutated columns ⇒ 99.5–100%
  HOT updates across 5M+ writes, no bloat, autovacuum keeps up. Do not add
  fillfactor tuning, JSONB, or secondary indexes.
- **bytea proto + native key/version/status columns**: point reads 1.0 ms
  p50 / 1.3–1.6 ms p99 through 2k QPS; 8× record-size increase had no cost
  while cache-resident.
- **Leases table**: acquire p99 flat to 1k/s (2× requirement); unlogged
  variant demoted to optional.
- **Data volume**: no latency effect 10k → 100k actors (expected —
  cache-resident); the XL (100M) tier tests the cache-breakdown regime.

## Open questions (not optimization calls yet)

- **Foreign keys**: churn A/B (± FK × atespace skew) still pending; no
  evidence against them so far — steady-state traffic never touches them.
- **IAM database auth** (cloudsqlconn dialer) and **DSN in a Secret**:
  production security posture, orthogonal to performance.
- **Instance tier for 5–10k mixed QPS**: retest the mixed sweep with feed
  ON before sizing — the old 4.6k ceiling was #1's bug, not CPU.
