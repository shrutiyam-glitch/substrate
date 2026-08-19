# Fake-atelet lifecycle benchmarking (control plane in isolation)

How to run **real** ResumeActor/SuspendActor/PauseActor flows through the
**real** ateapi — gRPC, auth, leases, CAS, store, workercache, watch — at
scale, without any worker pods or container runtime: atelet is replaced by a
simulator, and the worker fleet exists only as store rows. (The "fake atelet"
methodology from the Solo team.)

## Architecture

```
Locust (ate_api.py, N users)          benchmark-workloads namespace:
        │ gRPC                          ActorTemplate  (real CR, Ready)
        ▼                               WorkerPool     (real CR, 0 replicas)
   ate-api-server ──────────────┐
   (real: workflows, leases,    │ AteomHerder RPCs
    CAS, store, workercache)    ▼ (Run/Checkpoint/Restore)
        │                 atelet-simulator ──► MintCert callback to ateapi
        ▼                 (returns OK after ~1ms)   (replays atunnel's boot mint)
   store (PostgreSQL / Valkey) ← synthetic worker rows (COPY-seeded);
                                  actors reference the real ActorTemplate
```

What stays real: everything in ateapi and the store — including actor
credential minting, which the simulator replays after every Run/Restore.
What is fake: the three atelet RPCs (success stubs) and the worker fleet
(rows without pods).

## Components (all on this branch)

| Piece | Where |
|---|---|
| `WorkerRuntimeDialer` interface + `StaticAteletDialer` | `cmd/ateapi/internal/controlapi/dialer.go` |
| ateapi flag `--atelet-simulator-address` (empty = production dialing) | `cmd/ateapi/main.go` |
| Syncer guard env `ATE_BENCH_KEEP_SYNTHETIC_WORKERS=1` (see below) | `cmd/ateapi/internal/controlapi/syncer.go` |
| Simulator (AteomHerder: Run/Checkpoint/Restore → OK after `--response-delay`) | `cmd/benchmarking/atelet-simulator/` + `benchmarking/atelet-simulator/manifests.yaml` |
| MintCert callback (`--mint-target`; fires after every Run/Restore) | `cmd/benchmarking/atelet-simulator/mint.go` |
| Seeder flags (workers/actors that reference real CRs) | storebench `--seed-*` (see Seed fidelity below) |

## The syncer WILL delete synthetic workers — set the guard

An earlier revision of this doc claimed the syncer was purely
pod-event-driven and needed no changes. **Disproven in practice**:
`enqueueStoredWorkers` sweeps every store row at startup so records whose
pods are gone get cleaned up; synthetic rows have no pods, so the sweep
reconciles each one as dead — releasing its actor and deleting the row
(~7k rows/min observed; it ate 347k of a 1M fleet before being stopped).

Set `ATE_BENCH_KEEP_SYNTHETIC_WORKERS=1` on ateapi (alongside
`--atelet-simulator-address`) to disable dead-worker cleanup for the run.
Never set it in production: it also disables legitimate orphan cleanup.

## Seed fidelity — every field the lifecycle path checks

Seeded rows must satisfy the same checks real ones do; each miss below was
found as a distinct smoke-test failure:

1. **Seed with a storebench built from THIS branch.** Old-schema seeders
   marshal protos the current server decodes as `STATE_UNSPECIFIED` —
   resume then fails its state precondition.
2. **Actors point at their own snapshot** (`Status.LatestSnapshot` + a
   parseable `SnapshotUri` on the snapshot row — the seeder does both).
   Without the ref, resume falls back to the ActorTemplate golden
   snapshot, which benchmark stores do not carry → `DataLoss`.
3. **Workers carry scheduling fields**: `--seed-worker-active`,
   `--seed-worker-sandbox-class` matching the template, and
   `--seed-worker-labels` satisfying the template's `workerSelector`.
4. **Workers carry a NodeName** (`--seed-worker-node-name`): resume's
   AttachVolumes step requires it, and MintCert authorizes only workers on
   the caller's node — set it to the simulator pod's node (the manifest
   pins the simulator to one node for exactly this reason).
5. **Worker identity travels in the pod UID**: the seeder writes
   `WorkerPodUid = "sim://<namespace>/<pod>"`. Run/Restore carry only the
   pod UID, so this is how the simulator learns which worker to mint for;
   echoing the stored UID also satisfies the pod-UID match.
6. **COPY seeding bypasses the change feed**: caches see new rows only at
   the next periodic relist (5 min) — or restart ateapi after seeding.

## MintCert callback — measuring the credential race

With `--mint-target=dns:///api.ate-system.svc:443`, the simulator calls
`MintCert` synchronously after every Run/Restore — the same call atunnel
makes when a real sandbox boots, at the same point in the flow (right
after the assignment commit). Requirements, all in the manifest: the
simulator runs under the `atelet` ServiceAccount with a podidentity
certificate (MintCert authenticates the caller as atelet by SPIFFE ID) and
is node-pinned (see Seed fidelity #4).

Counters (`ok / denied / failed / skipped`) log every 10s. On the ateapi
side, the log line `"authorized via store read-through; worker cache was
stale"` counts the mints where the worker cache lost the race to the
assignment commit — the per-mechanism freshness metric the watch design
doc asks for (see worker-change-feed-alternatives.md, "Freshness vs
correctness").

## Setup

1. **Simulator** (edit the nodeSelector pin for your cluster first):
   `hack/run-tool.sh ko apply -f benchmarking/atelet-simulator/manifests.yaml`
2. **Point ateapi at it** and set the syncer guard:
   ```bash
   export ATE_API_ATELET_SIMULATOR_ADDRESS="atelet-simulator.ate-system.svc:9090"
   # ensure the envvars ConfigMap also carries ATE_BENCH_KEEP_SYNTHETIC_WORKERS=1
   ./hack/install-ate.sh --create-api-server-env-vars --store-backend=postgres
   kubectl rollout restart deployment/ate-api-server -n ate-system
   ```
   ateapi logs a "BENCHMARK MODE" warning at startup. Unset the env vars
   and repeat to return to production dialing.
3. **Real CRs, zero pods** (the bootstrap-then-drain trick): create the
   ActorTemplate + WorkerPool CRs in a benchmark namespace with 1 replica;
   wait for the template to become Ready (the controller bootstraps its
   golden actor through that one real worker); then scale the WorkerPool to
   0. The CRs stay, the template stays Ready, no pods remain.
4. **Seed the fleet + actors** (COPY loader; names must match the CRs):
   ```
   storebench --load-only --actors=<N> --workers=<M> \
     --seed-worker-namespace=<CR namespace> --seed-pool-prefix=<CR name> \
     --seed-template-namespace=<CR namespace> --seed-template-name=<template> \
     --seed-worker-sandbox-class=<template class> --seed-worker-active \
     --seed-worker-labels=<selector labels> \
     --seed-worker-node-name=<simulator pod's node>
   ```
5. **Drive it**, two workload shapes:
   * `ate_api.py` (`AteAPIUser`) — each user creates + cycles its OWN actor:
     cache-warm actors; measures rate scaling and contention.
   * `lifecycle_cold.py` (`ColdLifecycleUser`) — each cycle resumes a
     UNIFORMLY RANDOM actor from a large pre-seeded population. Seed the
     population additively with a distinct prefix (resume-from-max is
     prefix-scoped) and pass matching `--cold-actor-*` flags to locust.

## Caveats / open items

- Smoke-tested 2026-08-19, synthetic-worker variant: resume → RUNNING →
  suspend → SUSPENDED → resume, zero worker pods, with the MintCert
  callback issuing a real actor certificate per resume.
- Actors suspended through the simulator record snapshot references with
  no real data behind them — harmless in benchmark mode (Restore is
  stubbed), but such actors must not be resumed after switching back to
  production dialing.
- Latency results measure the control plane only: real lifecycle cost is
  dominated by checkpoint/restore, which the simulator replaces with
  `--response-delay`. Set the delay to a realistic value if end-to-end
  realism matters; leave it at 1 ms to isolate control-plane cost.
- The simulator's AteomHerder listener is insecure gRPC — acceptable
  in-cluster for benchmarks; never deploy alongside production traffic.
  (The MintCert callback itself uses real mTLS.)
- Worker rows seeded this way are never cleaned by Kubernetes; remove them
  with targeted deletes when done.
