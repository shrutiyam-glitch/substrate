# storebench — open-loop store-level benchmark

`cmd/ateapi/storebench` drives `store.Interface` (the exact code path ateapi
uses: `atepg` or `ateredis`, same serialization and transactions) at a fixed
arrival rate. Requests launch on schedule whether or not earlier ones have
returned, so latency percentiles are free of coordinated omission. This is
the instrument for "p99 <= X ms at Y RPS" verdicts against the storage
requirements doc; the Locust suite (closed loop, via gRPC) remains the
end-to-end comparison layer.

## Why in-cluster

Cloud SQL is private-IP-only and Valkey requires the in-cluster mTLS
certificates, so storebench runs as a Job in `ate-system` with the same
projected certificates as ate-api-server (see job.yaml).

## Usage

```bash
# Postgres (Cloud SQL): put the DSN in a secret once
kubectl create secret generic storebench-dsn -n ate-system \
  --from-literal=dsn="postgresql://postgres:<PW>@10.25.0.3:5432/atepg?sslmode=require"

# Edit job.yaml args (backend, dataset rung, rps, mix), then:
hack/run-tool.sh ko apply -f benchmarking/storebench/job.yaml
kubectl logs -n ate-system job/storebench -f     # report prints at the end
kubectl delete job -n ate-system storebench      # before re-running
```

## Flags that matter

| Flag | Meaning |
|---|---|
| `--backend=redis\|postgres` | which store implementation to drive |
| `--actors/--workers/--atespaces` | standing dataset (the scale-ladder rung); loaded idempotently, `--skip-load` to reuse, `--load-only` to just load |
| `--rps` | offered arrival rate (open loop) — step 1k -> 5k -> 10k per run |
| `--duration/--warmup` | measured window / discarded warmup |
| `--mix` | weighted ops: `actorget, actorupdate, workerget, workerupdate, snapget, snapcreate, actorchurn, lock, list` |
| `--key-dist=uniform\|zipf` | which rows get hit: uniform, or zipf (hot-key skew; expect `conflicts` > 0) |
| `--clear-first` | DebugClearAll before load — DESTRUCTIVE, wipes the store |

Notes:
- CAS updates time only the `UpdateActor`/`UpdateWorker` call; the read that
  obtains the version is untimed setup. Version/lock conflicts are counted
  separately (`conflicts`), not as errors or latency samples.
- `actorchurn` (create+delete pairs) is the op that exercises atepg's
  `actors -> atespaces` foreign key; run it with `--atespaces=1` vs `=10`
  vs a no-FK schema variant for the FK experiment.
- If achieved RPS < offered RPS the report warns: the backend is saturated
  and latencies describe an unbounded queue, not steady state. Step down.
- `--json-out` writes the machine-readable report (job.yaml points it at
  /dev/termination-log so it also survives in the pod's status).

## Measurement protocol (learned the hard way)

- **Never measure right after a bulk load.** Loading leaves WAL, checkpoint,
  and autovacuum debt that poisons the first minutes of a run. Load with
  `--load-only` as its own Job, let disk writes flatline (Pantheon), then
  measure with `--skip-load=true`. If load and run must share a Job, use
  `--warmup=2m` or more.
- **One configuration change per run** when attributing effects (pool size,
  disk, tier, schema variant). Batch changes only after each has been
  individually diagnosed.
- **Read `count` (completed), not the achieved-launch line, as the
  saturation gauge**: completed/duration < offered RPS means the queue grew
  the whole run and latencies describe queue depth, not operation cost.
- Instance restarts (tier changes) leave a cold buffer cache — first run
  after needs a long warmup.
- The dataset drifts: snapcreate adds ~15 rows/s and churn leaves tombstone
  debt. Reset + reload (`--clear-first` + `--load-only`) between sweep
  stages for clean comparisons.

## Suggested Rung S sweep (per backend)

```
--actors=10000 --workers=100 --atespaces=10
1. steady state: --rps=1000 / 2500 / 5000, default mix
2. churn/FK:     --rps=500, --mix=actorchurn=80,actorget=20, --atespaces=10 then 1
3. skew:         repeat 1 with --key-dist=zipf
4. lease:        --rps=1000, --mix=lock=100
```
