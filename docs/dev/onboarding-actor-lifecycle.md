# Onboarding: The Decoupled Actor Lifecycle

A code-first guide for engineers new to Agent Substrate. It assumes you know the
concepts from the demo — an **actor** is the workload, a **worker** is the pod —
and focuses on *how the decoupled lifecycle actually works in the code*.

For the conceptual background, read [architecture.md](../architecture.md) and
[glossary.md](../glossary.md) first. This doc points you at the real types,
files, and functions so you can jump straight in.

## The one big idea: two independent lifecycles

An actor and a worker have **independent lifecycles**. An actor can outlive any
worker, migrate between workers, and spend most of its life as a blob in object
storage with *no* worker at all.

The decoupling in one sentence: **the `Actor` record and the `Worker` record are
separate rows in Redis, joined only transiently by a pointer
(`actor.ateom_pod_*` ↔ `worker.assignment.actor`) that exists only while the
actor is running.** Suspend severs the pointer; the actor lives on as a snapshot
URI.

### The two-layer data model

This is the core architectural bet (`docs/architecture.md`, §"API Resource
Models"):

| Layer | What | Stored where | Why |
|---|---|---|---|
| **Declarative** | `ActorTemplate`, `WorkerPool`, `SandboxConfig` (CRDs) | Kubernetes etcd | Low churn, needs RBAC/policy |
| **Dynamic** | `Actor`, `Worker` (records) | ValKey/Redis (the "store") | High churn (many writes/sec), needs low latency |

Actors and Workers are **deliberately not Kubernetes objects** — etcd can't
handle millions of them changing many times per second. `ateapi` (the control
plane) owns the Redis store; `atecontroller` reconciles the CRDs into a
`Deployment` of pods.

## The actor state machine — start here

Defined as an enum in the proto. **Read this first** — it is the spine of
everything.

`pkg/proto/ateapipb/ateapi.proto` — `Actor.Status`:

```
STATUS_UNSPECIFIED, RESUMING, RUNNING, SUSPENDING, SUSPENDED,
PAUSING, PAUSED, CRASHED, DELETING
```

```
(none) ──CreateActor──▶ SUSPENDED ──ResumeActor──▶ RESUMING ──▶ RUNNING
                            ▲                                       │
                            │                                       ├─SuspendActor─▶ SUSPENDING ─▶ SUSPENDED
          FinalizeSuspended─┘                                       └─PauseActor───▶ PAUSING ────▶ PAUSED
                                                                    (any broken) ──▶ CRASHED
   SUSPENDED / CRASHED ──DeleteActor──▶ DELETING ──▶ gone
```

- `SUSPENDED` — state lives in object storage (GCS/S3), no worker assigned. The
  resting state. Actors are **born suspended** (`CreateActor` — no worker).
- `PAUSED` — a *cheap* suspend: snapshot kept **on the node VM**, next resume is
  steered back to that node (`LocalSnapshotInfo.node_vms_with_local_snapshots`).
  Contrast with `SUSPENDED`, which uploads to external storage.
- `CRASHED` — the worker pod vanished while the actor was running (set by the
  syncer, see below).

Two orthogonal concepts also matter:

- `Actor.SnapshotInfo` — a oneof of `ExternalSnapshotInfo` (object storage,
  survives node loss) or `LocalSnapshotInfo` (pinned to specific node VMs).
- `Worker.State` — `STATE_ACTIVE` or `STATE_DRAINING`.

The same file defines the `Control` gRPC service (`CreateActor`, `ResumeActor`,
`SuspendActor`, `PauseActor`, `DeleteActor`, plus atespace CRUD). That service is
the entire public API surface — everything `kubectl-ate` and the `atenet` router
do goes through these RPCs.

RPC entrypoints are thin handlers that validate then call `s.actorWorkflow.*`:
`create_actor.go`, `resume_actor.go`, `suspend_actor.go`, `pause_actor.go`,
`delete_actor.go` (all under `cmd/ateapi/internal/controlapi/`).

## The workflow engine — how transitions actually happen

The nicest bit of the codebase. Every lifecycle transition is a **workflow**: an
ordered list of idempotent steps run by a generic engine.

**Read `cmd/ateapi/internal/controlapi/workflow.go` in full — it's ~290 lines
and it's the backbone.**

The `WorkflowStep[Params, Context]` interface (`workflow.go`) — every step
implements four methods:

- `IsComplete()` — already done? → skip. (This is what makes retries safe.)
- `CheckPrerequisite()` — is the current actor status a valid edge for this
  step? → else abort with `FailedPrecondition`.
- `Execute()` — do the work, persist state to Redis.
- `RetryBackoff()` — optional auto-retry on version conflicts.

`RunWorkflow[Params, Context]()` just loops the steps: for each, skip if
`IsComplete`, validate `CheckPrerequisite`, then `Execute`. This is called
**"Client-Driven Forward Recovery"**: if a workflow dies halfway, the *client
retries the same RPC*, `IsComplete` fast-forwards past finished steps, and it
picks up where it left off. There is **no separate async reconciler** for actor
state — the RPC call itself drives the state machine forward, under a per-actor
lock (`acquireActorLock`, a Redis lock so only one transition touches an actor at
a time). The persisted `Status` is the durable checkpoint.

`ActorWorkflow` composes the four workflows:

**Resume** (`workflow_resume.go`) — the one to study first:

```
LoadActorForResume → CreateVolumes → AssignWorker → AttachVolumes → CallAteletRestore → FinalizeRunning
```

The two steps that matter most:

- **`AssignWorkerStep`** — *the scheduler in action*. Asks the `scheduler` for a
  free worker matching the actor's constraints, writes `worker.Assignment =
  {this actor}` into Redis, then flips the actor to `RESUMING` and stamps
  `AteomPodName/Ip/Uid/WorkerPoolName` onto it. The claim is an **atomic
  optimistic-concurrency write** (`UpdateWorker`/`UpdateActor` with a version
  check; see the `ErrVersionConflict` handling). That's how two concurrent
  resumes can't grab the same worker.
- **`CallAteletRestoreStep`** — dials the node's `atelet` and issues the restore.
  Three branches:
  1. Has `LatestSnapshotInfo` → `client.Restore(...)` (local or external config).
  2. No snapshot but template has `GoldenSnapshot` and `!boot` → `Restore` from
     the golden snapshot.
  3. Otherwise → `client.Run(...)` — cold boot from spec, with `SandboxAssets`
     resolved from the pool's `SandboxConfig`.

Then `FinalizeRunningStep` flips `RESUMING → RUNNING`.

**Suspend** (`workflow_suspend.go`) — the mirror image, external snapshot to
object storage:

```
LoadActorForSuspend → MarkSuspending → CallAteletSuspend → DetachVolumes → FinalizeSuspended
```

`MarkSuspendingStep` computes an `InProgressSnapshot` URI;
`CallAteletSuspendStep` calls atelet's `Checkpoint` with
`CHECKPOINT_TYPE_EXTERNAL`; `FinalizeSuspendedStep` frees the worker (clears
`Assignment`), promotes `InProgressSnapshot` to `LatestSnapshotInfo.External`,
clears pod pointers, and sets `SUSPENDED`.

**Pause** (`workflow_pause.go`) — same shape as Suspend but
`CHECKPOINT_TYPE_LOCAL`; `FinalizePausedStep` records `LocalSnapshotInfo` with
`NodeVmsWithLocalSnapshots=[nodeName]`. Pause = "hibernate but keep the snapshot
on the node VM for a fast local resume." If the node name is lost it crashes the
actor (it could never be resumed).

**Delete** (`workflow_delete.go`) — only allowed from `SUSPENDED`/`CRASHED`:

```
LoadActorForDelete → MarkDeleting → DeleteVolumes → FinalizeDeleted
```

> **Tip:** open `workflow.go`, `workflow_resume.go`, and `workflow_suspend.go`
> side by side. Once you see one workflow you understand all four — same pattern,
> different steps.

## The gRPC chain: crossing the machine boundary

The control plane never touches a sandbox directly. There are **two gRPC hops**
to the node:

```
client ──Control──▶ ateapi ──AteomHerder──▶ atelet ──Ateom──▶ ateom ──▶ runsc / cloud-hypervisor
                      │        (per node)              (in pod)
                      └── watches k8s worker pods (informer)
```

| Service | Proto | RPCs | Direction |
|---|---|---|---|
| `Control` | `pkg/proto/ateapipb/ateapi.proto` | `CreateActor`, `ResumeActor`, `SuspendActor`, `PauseActor`, `DeleteActor`, `ListActors`, `ListWorkers`, atespace CRUD | client → ateapi |
| `AteomHerder` | `internal/proto/ateletpb/atelet.proto` | `Run`, `Checkpoint`, `Restore` | ateapi → atelet |
| `Ateom` | `internal/proto/ateompb/ateom.proto` | `RunWorkload`, `CheckpointWorkload`, `RestoreWorkload` | atelet → ateom |

- **`atelet`** (`cmd/atelet`) = the node-level "herder" and the **storage mover**.
  `AteomHerder.Checkpoint` calls ateom's `CheckpointWorkload` then
  `uploadExternalCheckpoint`/`moveLocalCheckpoint`; `Restore` does
  `downloadExternalCheckpoint` → `prepareOCIBundles` → ateom `RestoreWorkload`.
  GCS/S3 code lives under `cmd/atelet/internal/ategcs/`. It dials workers via
  `AteletDialer.DialForWorker` (`controlapi/dialer.go`). On SIGTERM it drains
  gracefully (`drainOnShutdown` in `cmd/atelet/main.go`): in-flight
  `Checkpoint`/`Restore` RPCs finish — bounded by `--drain-timeout`, sized for
  multi-GiB snapshot transfers — so a DaemonSet rollout or node drain no
  longer aborts a checkpoint mid-upload and crashes the actor.
- **`ateom`** (`cmd/ateom-gvisor`, `cmd/ateom-microvm`) = the in-pod helper that
  drives the sandbox runtime. Split out so the *pod's* lifecycle is decoupled
  from the *sandbox's* lifecycle.
  - gVisor: `cmd/ateom-gvisor/runsc.go` shells out to `runsc checkpoint
    --image-path` / `runsc restore`.
  - micro-VM: `cmd/ateom-microvm/checkpoint.go` / `restore.go` drive
    cloud-hypervisor over its REST API with `userfaultfd` demand-paging.

**Snapshot scopes** flow through both protos as `SnapshotScope`:
`SNAPSHOT_SCOPE_FULL` (memory + rootfs delta) vs `SNAPSHOT_SCOPE_DATA` (only
durable-dir volumes), chosen per template via
`SnapshotsConfig.OnCommit`/`OnPause` (`toAteletSnapshotScope`).

There is also an **`ActorIdentity`** service (`ateapi.proto`, `MintJWT` /
`MintCert`) that lets a running workload swap its Kubernetes SA token for a
*stable actor-level* credential that survives migration between workers.

## The background machinery — three distinct loops

The workflow engine above is **not** a background loop (it's client-driven
forward recovery). The genuinely background machinery is separate, and there are
**three** distinct mechanisms:

| # | Loop | File / wiring | Keeps in sync |
|---|---|---|---|
| a | **WorkerPoolSyncer** (k8s informer on worker pods) | `controlapi/syncer.go`, wired `cmd/ateapi/main.go` | pod → `Worker` record in Redis; draining; dead/orphan cleanup |
| b | **workercache** (store watch + ~5-min relist) | `cmd/ateapi/internal/workercache/workercache.go`, wired `cmd/ateapi/main.go` | in-memory worker snapshot the scheduler reads for O(1) |
| c | **atecontroller reconcilers** (controller-runtime) | `cmd/atecontroller/...` | CRDs → k8s objects + golden snapshots |

### (a) WorkerPoolSyncer — keeping the fleet accurate

`cmd/ateapi/internal/controlapi/syncer.go`, type `WorkerPoolSyncer`. It
reconciles Kubernetes worker *pods* into `Worker` records in the store, driven by
a `SharedIndexInformer`. This is where actor and worker lifecycles get
*re-coupled* on failure.

- **Pod Add/Update** → `syncWorkerToStore` creates/updates the `Worker` record.
  It checks `pod.DeletionTimestamp != nil` and marks the worker `DRAINING`
  **before** the `isWorkerEligible` (has-IP) check — a Terminating pod may have
  already lost its IP, and gating on IP first would drop the draining
  transition.
- **Pod entering Terminating** → `markWorkerDraining` (→ `STATE_DRAINING`) so the
  scheduler stops routing to it, but *deliberately does not touch the actor* —
  inside the pod, ateom got SIGTERM and is gracefully suspending the actor.
- **Pod Deleted** → `reconcileDeadWorker` → `releaseActorOnDeadWorker`: a
  still-`RUNNING` actor is moved to `CRASHED` and its pod pointers cleared; an
  already-`SUSPENDED` actor is left resumable.
- **On startup** → `reconcileOrphanedWorkers` sweeps the store for worker records
  whose pods no longer exist — recovers delete events missed while `ateapi` was
  down (the informer cache starts empty on restart and can't replay them).

### (b) workercache — fast scheduling reads

`AssignWorkerStep` doesn't scan Redis directly — it reads `workercache`, an
in-memory snapshot fed by a `WatchWorkers` stream plus a periodic relist. That's
the O(1) scheduling read that keeps assignment fast.

### (c) atecontroller — pools and golden snapshots

`cmd/atecontroller/...`, controller-runtime reconcilers. **The
`ActorTemplateReconciler` will confuse you if you don't know it exists**, because
it is a reconciler that *calls the control-plane RPCs itself*.

- `WorkerPoolReconciler` — `For(WorkerPool).Owns(Deployment)`: turns a
  `WorkerPool` CR into the actual `Deployment` of worker pods. This *materializes
  the pod fleet* that the syncer then discovers.
- `ActorTemplateReconciler` — a **phase state machine** (`PhaseInitial →
  PhaseResumeGoldenActor → PhaseWaitGoldenActor → PhaseReady`). When you create
  an `ActorTemplate`, this controller: creates a throwaway **"golden actor"** in
  a reserved `ate-golden` atespace → `ResumeActor(boot=true)` to cold-boot it →
  waits a warmup period (`RequeueAfter`) → `SuspendActor` → stores the resulting
  snapshot URI as `Status.GoldenSnapshot`.
- `NetworkPolicyReconciler` — applies NetworkPolicy at the pool boundary.

This closes the loop: the `CallAteletRestoreStep` "restore from golden snapshot"
branch is restoring the snapshot *this reconciler* produced. **Golden snapshot =
the pre-booted template, captured once, so every new actor's first resume is a
fast restore instead of a cold boot.** If `kubectl ate create actor` seems to
hang, the usual cause is the `ActorTemplate` not yet being `PhaseReady`.

## The scheduler

`cmd/ateapi/internal/scheduling/scheduling.go` — interface `Scheduler`.

- `Schedule()` — filters free workers (`Assignment == nil`) through `Applies()`
  and picks one at random.
- `Applies()` — checks `SandboxClass` match, `STATE_ACTIVE` (skips DRAINING),
  template + actor label selectors, and `RequiredNodes` (for local snapshots
  pinned to node VMs).
- `Constraints` is built by `schedulingConstraints()` in `workflow_resume.go`,
  ANDing the template `WorkerSelector`, the actor `WorkerSelector`, and snapshot
  node affinity.

## Suggested reading order

1. `pkg/proto/ateapipb/ateapi.proto` — the `Actor` status enum + `Control`
   service. The contract.
2. `cmd/ateapi/internal/controlapi/workflow.go` — the step engine +
   `ActorWorkflow`. The keystone file.
3. `.../workflow_resume.go` → `.../workflow_suspend.go` →
   `.../workflow_pause.go` → `.../workflow_delete.go` — one full transition each
   way.
4. `.../scheduling/scheduling.go` + `.../syncer.go` — placement + fleet
   reconciliation.
5. `cmd/atelet/main.go` (`AteomHerder`) → `cmd/ateom-gvisor/main.go` +
   `runsc.go` — the checkpoint/restore mechanics.
6. `cmd/atecontroller/internal/controllers/actortemplate_controller.go` — how
   pools and golden snapshots get created in the background (ties the whole
   picture together).

## Run it locally to watch it happen

The kind quickstart (see [README.md](../../README.md)) gets you a live loop in a
few minutes:

```bash
hack/create-kind-cluster.sh
hack/install-ate-kind.sh --deploy-ate-system
hack/install-ate-kind.sh --deploy-demo-counter
go install ./cmd/kubectl-ate
kubectl ate create atespace demo
kubectl ate create actor my-counter-1 -a demo --template=ate-demo-counter/counter
kubectl port-forward -n ate-system svc/atenet-router 8000:80
# then, in another terminal:
curl -X POST -H "Host: my-counter-1.demo.actors.resources.substrate.ate.dev" -i http://localhost:8000/
```

That `curl` is the whole lifecycle in miniature: the `atenet` router extracts the
actor from the `Host` header, calls `ResumeActor` on `ateapi`, which runs the
resume workflow (assign worker → atelet → ateom → runsc restore), routes the
request in, and — because the counter is stateful — the count survives across
suspends. Watch it with `kubectl ate list actors -a demo` and `kubectl ate list
workers` to see the status transitions and the actor→worker binding appear and
disappear.
