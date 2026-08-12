# atepg Cloud SQL test plan (plan of record)

Supersedes §3 of [atepg-benchmarking-prep.md](atepg-benchmarking-prep.md)
after Julian's 2026-08-11 guidance. Results ledger:
[atepg-benchmark-results.md](atepg-benchmark-results.md). Findings &
recommendations: [atepg-optimizations.md](atepg-optimizations.md). Instrument:
`cmd/ateapi/storebench` (open-loop, drives `store.Interface`; Job in
`benchmarking/storebench/`).

## Two benchmark types (Julian's framing)

**(a) Volume ladder** — does latency degrade as data volume grows?
Fixed, gentle probe load; per-store-method p50/p90/p99 at each tier.

**(b) Latency vs throughput** — what QPS can each method sustain within its
latency target? Per-method open-loop QPS sweep until p99 breaks or drain
falls below offered; deliverable is the classic latency/throughput curve
per method.

## Tiers (dataset shapes)

Density 100:1 actors:workers, snapshots 1:1 with actors (every actor has at
least one), workers spread over pools, ~1k actors per atespace (to confirm
with Julian).

| Tier | Actors | Snapshots | Workers | Pools | Atespaces | Est. data (3KB/1KB records) |
|---|---|---|---|---|---|---|
| S | 10k | 10k | 100 | 2 | 10 | ~40 MB |
| M | 100k | 100k | 1k | 4 | 100 | ~400 MB |
| L | 1M | 1M | 10k | 10 | 1k | ~4 GB |
| XL | 100M | 100M | 1M | 20 | 100k | ~400–600 GB incl. indexes |

Record sizes padded to the requirements doc's estimates (actor ~3 KB,
worker/snapshot ~1 KB) via storebench `--actor-bytes/--worker-bytes/
--snapshot-bytes` — at XL, cache-fit is the question, so sizes must be
honest. XL requires the Cloud SQL disk grown to 1 TB first, and its load
runs as a dedicated `--load-only` Job (COPY mode, ~4.5–5 h measured — see the XL
load procedure below).

Instance config of record: `db-custom-4-16384`, 100 GB (1 TB for XL),
`pool_max_conns=32`. (The fast-CAS flag was measured, rejected, and its
code removed 2026-08-11 — see atepg-optimizations.md §2; runs 14–42 that
reference it predate the removal.) `ATEPG_CHANGE_FEED` defaults OFF in
runs (the current-PR baseline) but is the measured fix for the worker-write
NOTIFY ceiling and the recommended production configuration; worker-write
runs must state which mode they used.

## XL load procedure (what it actually took, 2026-08-11)

1. **Disk**: `gcloud sql instances patch atepg-bench --storage-size=1000GB`
   (online, grow-only; ~$170/mo prorated while it exists). 100M×3KB actors +
   100M×1KB snapshots + PK indexes + WAL headroom ≈ 450–550 GB.
2. **Clean the previous tier first — the layouts are incompatible.** Dataset
   names are pure functions of index AND tier shape: actor i lives in
   atespace `i % atespaces` and worker i in pool `i % pools`, so changing
   `--atespaces` (100 → 100k) or `--worker-pools` (4 → 20) re-homes
   overlapping indices. Loading XL over tier M leaves ~100k rows stranded
   under old keys → phantom NotFound errors. Targeted deletes of
   storebench-owned rows (atespace LIKE 'sbench-%', worker_namespace
   'storebench', both feed tables) + `VACUUM ANALYZE`.
3. **Load**: storebench Job with `--load-only=true --load-mode=copy` (pgx
   `CopyFrom`, 100k-row atomic batches, padded records 3000/1000/1000
   bytes). The 100k atespaces are created first via the store API
   (sequential, few minutes), then actors+snapshots in lockstep, then
   workers. **Resumable**: each table continues from its max
   zero-padded name, so a died pod just redeploys — this is also why name
   widths were sized for the tier (`actor-%09d`; changing widths orphans
   datasets).
4. **Observed rate**: ~9k actors/s (+1:1 snapshots ⇒ ~18k rows/s, ~36 MB/s)
   at steady state, over private-IP VPC into WAL-logged COPY with the
   actors→atespaces FK live — **measured 4h39m for 100M** (avg ~6k
   actors/s; an early ~9k/s burst does not hold). The first few minutes
   run slower still (checkpoint ramp); don't extrapolate from either end.
   The ~80k rows/s local-smoke number doesn't survive a real network +
   4-vCPU WAL budget. Progress prints every 15 s in the Job log.
5. **After the load, settle before measuring** — hundreds of GB of ingest
   leave a checkpoint/vacuum wave; wait ≥15–30 min or until Pantheon disk
   writes flatline.

Not needed: schema changes, pg tuning, manual SQL — the loader writes
atepg's exact storage format and applies the idempotent schema itself.

## (a) Volume-ladder profile

**Status: S/M measured (flat, cache-resident, as expected); XL load in
progress 2026-08-11; L pending.**

At each tier, after load + settle:
1. Mixed probe: 500 RPS, standard hot-path mix, 5 min — the cross-tier
   comparable.
2. Per-method probes: 200 RPS each, 3 min — ActorGet, ActorUpdate,
   WorkerGet, WorkerUpdate, SnapshotGet, SnapshotCreate, lease (`lock`),
   ListActors page.
Report p50/p90/p99 per method per tier; the deliverable is the
latency-vs-volume table/plot. Pass = flat-ish curve (B-tree depth only);
fail = super-log growth (cache breakdown — expected at XL, the finding is
*where* it starts).

## (b) Latency-vs-throughput profile

**Status: tier M complete** (runs 20–42 in the ledger; plots
`bench-results/tier-m-methods.png`, `notify-vs-feed.png`,
`actorupdate-cas.png`). L spot-check pending.

At tier M (primary) and L (spot-check), per hot method:
- QPS steps: 250 → 500 → 1k → 2k → 4k → 8k → … until p99 > target or
  completed/duration < offered (saturation).
- Each step: 1 min warmup + 3 min measured, one storebench Job.
- Curve per method from the `--json-out` files via
  `benchmarking/storebench/plot.py`.
Targets: Get ≤ 5 ms, Update/SnapshotCreate ≤ 10 ms, lease ≤ 10 ms (p99).
Also record the knee (max sustainable QPS) per method — the capacity
number that prices the O(10K) RPS requirement.

## Secondary experiments (unchanged from prep doc, still pending)

- FK churn A/B: `actorchurn` mix × atespace skew {1, N} × FK on/off schema.
- Zipf key-skew variants of (b) at tier M.
- Unlogged `leases` A/B — demoted to optional: the lock curve is flat to
  1k/s (double the requirement) and only crests its target at 2k.
- Redis/Valkey baseline for (a) and (b) at tiers S–L only — 100M resources
  do not fit the 6×1 GB in-cluster Valkey (itself a comparison finding).
- ~~Pool-vs-CPU attribution of the ~4.6k ops/s mixed ceiling~~ RESOLVED:
  the ceiling was pg_notify commit serialization (proven by the change-feed
  intervention; ledger runs 20–30). Instance-tier scaling for a 5–10k QPS
  mixed load remains untested (re-run the mixed sweep with feed ON).

## Measurement protocol (hard rules, learned)

1. Never measure right after a bulk load or a saturated run — settle until
   Pantheon disk writes flatline (≥5 min), or warmup ≥ 2 min.
2. One variable per attributed run; the run log line prints the feed
   flags so every result is self-identifying.
3. Saturation gauge is completed/duration vs offered — under saturation,
   percentiles are queue measurements with survivorship bias (report the
   knee, not the latencies).
4. Percentiles of record come from storebench's exact-sample report;
   Grafana/Pantheon (`storebench_*`, GMP) is the time-series lens.
5. Dataset drifts across runs (snapcreate accumulates; updates churn) —
   reset/reload between phases, and never compare runs across different
   standing datasets.
