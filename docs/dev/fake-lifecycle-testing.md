# Fake-atelet lifecycle benchmarking (control plane in isolation)

How to run **real** ResumeActor/SuspendActor/PauseActor flows through the
**real** ateapi — gRPC, auth, leases, CAS, store, workercache, watch — at
scale, without any worker pods or container runtime: atelet is replaced by a
simulator, and the worker fleet exists only as store rows. (The "fake atelet"
methodology from the Solo team, rebuilt on this branch.)

## Architecture

```
Locust (ate_api.py, N users)          benchmark-workloads namespace:
        │ gRPC                          ActorTemplate  (real CR, Ready)
        ▼                               WorkerPool     (real CR, 0 replicas)
   ate-api-server ──────────────┐
   (real: workflows, leases,    │ AteomHerder RPCs
    CAS, store, workercache)    ▼ (Run/Checkpoint/Restore)
        │                 atelet-simulator (returns OK after ~1ms)
        ▼
   store (Cloud SQL / Valkey)  ← 1M synthetic worker rows (COPY-seeded);
                                  actors reference the real ActorTemplate
```

What stays real: everything in ateapi and the store. What is fake: the three
atelet RPCs (success stubs) and the worker fleet (rows without pods).

## Components (all on this branch)

| Piece | Where |
|---|---|
| `WorkerRuntimeDialer` interface + `StaticAteletDialer` | `cmd/ateapi/internal/controlapi/dialer.go` |
| ateapi flag `--atelet-simulator-address` (env `ATE_API_ATELET_SIMULATOR_ADDRESS`; empty = production dialing) | `cmd/ateapi/main.go`, plumbed via `ate-api-server.yaml` + `install-ate.sh` |
| Simulator (AteomHerder: Run/Checkpoint/Restore → OK after `--response-delay`, default 1ms) | `cmd/benchmarking/atelet-simulator/` + `benchmarking/atelet-simulator/manifests.yaml` |
| Seeder flags (workers/actors that reference real CRs) | storebench `--seed-worker-namespace/--seed-pool-prefix/--seed-template-namespace/--seed-template-name` |

## Why the syncer needs no changes

`WorkerPoolSyncer` is pod-event-driven: its queue is fed only by the pod
informer, and it deletes a store worker only in reaction to that worker's
pod disappearing. Synthetic worker rows in a pool that never had pods are
invisible to it. (Verified against `syncer.go`; if a store-sweeping
reconcile is ever added, this assumption breaks — re-check then.)

## Setup

1. **Simulator**:
   `hack/run-tool.sh ko apply -f benchmarking/atelet-simulator/manifests.yaml`
2. **Point ateapi at it** (reconcile + restart, same pattern as backend
   switches):
   ```bash
   export ATE_API_ATELET_SIMULATOR_ADDRESS="atelet-simulator.ate-system.svc:9090"
   ./hack/install-ate.sh --create-api-server-env-vars --store-backend=postgres
   kubectl rollout restart deployment/ate-api-server -n ate-system
   ```
   ateapi logs a "BENCHMARK MODE" warning at startup. Unset the env var and
   repeat to return to production dialing.
3. **Real CRs, zero pods** (the bootstrap-then-drain trick): create the
   ActorTemplate + WorkerPool CRs in a benchmark namespace with 1 replica;
   wait for the template to become Ready (the controller bootstraps its
   golden actor through that one real worker); then scale the WorkerPool to
   0. The CRs stay, the template stays Ready, no pods remain.
4. **Seed the fleet + actors** (COPY loader; names must match the CRs):
   ```
   storebench --load-only --actors=<N> --workers=<M> \
     --seed-worker-namespace=<CR namespace> --seed-pool-prefix=<CR name prefix> \
     --seed-template-namespace=<CR namespace> --seed-template-name=<template>
   ```
5. **Drive it**, two workload shapes:
   * `ate_api.py` (`AteAPIUser`) — each user creates + cycles its OWN actor:
     cache-warm actors; measures rate scaling and contention.
   * `lifecycle_cold.py` (`ColdLifecycleUser`) — each cycle resumes a
     UNIFORMLY RANDOM actor from a large pre-seeded population: at XL
     volume, actor reads pay real cold-page costs. Seed the population
     additively with a distinct prefix (coexists with other datasets;
     resume-from-max is prefix-scoped), e.g. 10M resumable actors:
     ```
     storebench --load-only --actors=10000000 --atespaces=100000 --workers=0        --seed-actor-prefix=cactor-        --seed-template-namespace=benchmark-workloads --seed-template-name=sleep
     ```
     and pass matching `--cold-actor-count/--cold-actor-prefix/
     --cold-atespace-count/--cold-atespace-prefix` to locust.

## Caveats / open items

- **Smoke-tested 2026-08-11 (real-worker variant)**: one actor on
  `benchmark-workloads/sleep`, resume → RUNNING → suspend → SUSPENDED,
  zero errors, through a real worker's store row with the simulator
  answering all atelet RPCs. **Still untested: the synthetic-worker
  variant** (rows without pods) — two open items: (a) whether
  `AssignWorkerStep`'s cache filter needs specific State/SandboxClass
  fields on seeded rows. (The former item (b) — resume-from-max scanning
  globally and blocking additive seeds — is FIXED: resume is now scoped by
  `--seed-actor-prefix`/`--seed-worker-prefix`, so populations with
  distinct prefixes coexist.)
- Actors suspended through the simulator record snapshot references with
  no real data behind them — harmless in benchmark mode (Restore is
  stubbed), but such actors must not be resumed after switching back to
  production dialing.
- Latency results measure the control plane only: real lifecycle cost is
  dominated by checkpoint/restore (~300 ms in the community's measurements),
  which the simulator replaces with `--response-delay`. Set the delay to a
  realistic value if end-to-end realism matters; leave it at 1 ms to
  isolate control-plane cost.
- The simulator is insecure gRPC (no mTLS) — acceptable in-cluster for
  benchmarks; never deploy alongside production traffic.
- Worker rows seeded this way are never cleaned by Kubernetes; remove them
  with the storebench cleanup (targeted deletes) when done.
