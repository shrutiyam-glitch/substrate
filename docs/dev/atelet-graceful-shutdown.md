# Design: atelet SIGTERM graceful shutdown (issue #719)

Status: **decided and implemented** (branch `issue-719`). Decision 1 → Option B
(shared `serverboot.DrainGRPCOnShutdown` helper used by both atelet and
ateapi; the package keeps its `serverboot` name — its scope is boot-time
wiring of the whole lifecycle, shutdown hooks included). Decision 2 →
Option 3 (configurable flags), with `--drain-delay` defaulting to `0` and
`--drain-timeout` to `5m`. The options and their pros/cons are kept below for
the record. See "As implemented", "Verification results", and "Production
failure modes" at the end.

## Context

Today `cmd/atelet/main.go` ends with a bare `svr.Serve(lis)` and has **no signal
handling**. When Kubernetes sends SIGTERM (node drain, DaemonSet rollout, pod
eviction), the Go runtime's default SIGTERM behavior terminates the process
immediately — any in-flight `Run`/`Checkpoint`/`Restore` RPC is killed
mid-execution.

Issue #719 asks atelet to adopt **the same shutdown pattern ate-api already
uses**: on SIGTERM, stop accepting new RPCs, let in-flight ones finish, and
force-stop after a timeout.

**Explicitly out of scope (tracked in #517):** what happens to the *running
actor* inside the pod when the node is evicted — i.e. ateom capturing the signal
and forwarding it to actors for graceful suspend. This doc only covers atelet's
own process shutdown and its own in-flight RPCs.

### Relationship to #517

The two tracks are complementary, not redundant. #719 is *passive*: atelet must
not abort a `Checkpoint`/`Restore` RPC it is already executing. #517 is
*active*: the orchestration that suspends running actors when their node/worker
goes away (ateom catching SIGTERM in the worker pod; the control plane driving
`SuspendActor`). #719 is effectively a prerequisite for #517 on a draining
node: when #517 fires `SuspendActor` → `atelet.Checkpoint` during a drain, that
RPC only succeeds if atelet's own shutdown (this work) doesn't force-stop it
partway. Without #719, a node drain could kill atelet mid-checkpoint → aborted
upload → actor CRASHED (the `TODO(#362)` path).

### The reference pattern (ate-api)

`cmd/ateapi/main.go` already implements this:

- `shutdownCtx, stopSignals := signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)`
  (`cmd/ateapi/main.go:104`) — kept **separate** from the work `ctx` so
  in-progress work isn't cancelled the instant SIGTERM arrives.
- `drainOnShutdown(shutdownCtx, mux, readiness)` (`main.go:236`, defined at
  `main.go:245`) runs a goroutine that, on signal:
  1. `readiness.MarkNotReady()` → `/readyz` starts returning 503.
  2. `time.Sleep(*drainDelay)` → keep accepting work while load balancers /
     clients observe NotReady and stop routing.
  3. `srv.GracefulStop()` in a goroutine (stops accepting new conns/RPCs, waits
     for in-flight to finish).
  4. `select` on drain-complete vs `time.After(*drainTimeout)` → on timeout,
     `srv.Stop()` (hard cancel).
- `main` then blocks on `mux.Serve(lis)` and, after it returns, `<-drainDone`.
- Flags `--drain-delay` (13s) and `--drain-timeout` (15s), plus manifest
  `terminationGracePeriodSeconds: 40` and `/readyz` + `/healthz` probes
  (`serverboot.Readiness`, `EnableHealthz`).

### atelet's serving setup before this change

- `cmd/atelet/main.go` (`main`): built `svr := grpc.NewServer(...)`, registered
  `AteomHerderServer`, then a bare `svr.Serve(lis)` — no signal handling at all.
- The metrics server was started fire-and-forget with **no Readiness and no
  healthz** — `serverboot.StartMetricsServer(ctx, {Addr: ...})`.
- Manifest `manifests/ate-install/atelet.yaml` (DaemonSet): **no
  `terminationGracePeriodSeconds`** (defaulted to 30s) and **no
  readiness/liveness probes** (only prometheus scrape annotations).

### The atelet-specific crux

ateapi's RPCs are short state transitions, so a ~13s + ~15s drain fits inside the
default 30s grace. **atelet's `Checkpoint` uploads and `Restore` downloads
multi-GiB snapshots to/from GCS/S3 and can take minutes.** If the force-stop
timeout fires mid-checkpoint, the upload is aborted → per the existing
`TODO(#362)` path in `cmd/atelet/main.go` (`Checkpoint`, upload-failure
handling), the actor is marked **CRASHED**. So the two
design decisions below (where the code lives, and the timeout policy) are the
substance of this work — the mechanism itself is a direct copy of ateapi.

---

## Decision 1 — Where the drain logic lives

### Option A: Copy `drainOnShutdown` into `cmd/atelet/main.go` (local func)

Mirror ateapi exactly — a private `drainOnShutdown(ctx, srv, readiness)` in
atelet's `main.go`, plus `--drain-delay` / `--drain-timeout` flags.

**Pros**
- Smallest change; **zero blast radius on ateapi** (no need to re-verify the
  control plane).
- Literally "the same pattern ate-api uses" — trivial to review side-by-side.
- No new shared API to design or name.

**Cons**
- ~25 lines of drain logic now duplicated across two `main.go` files → drift
  risk (a future fix to one is easily missed in the other).
- atenet already has its own ad-hoc `GracefulStop` calls
  (`cmd/atenet/internal/router/extproc.go:75`, `xds.go:278`) — this is the third
  copy-ish of the same idea, reinforcing that it *wants* to be shared.

### Option B: Extract a helper into `internal/serverboot`

Add e.g. `serverboot.DrainGRPCOnShutdown(ctx, srv, readiness, delay, timeout)`
(and/or a small `serverboot.GracefulServe` wrapper), then call it from **both**
atelet and ateapi. `serverboot`'s package doc already declares it the home for
"startup boilerplate shared by the long-running substrate server binaries
(ateapi, atelet, ateom-gvisor, ateom-microvm)".

**Pros**
- DRY — one implementation, one place to fix/test the drain sequence.
- Fits `serverboot`'s stated purpose; `Readiness`/`StartMetricsServer` already
  live there, so the drain helper is a natural neighbor.
- Reusable by atenet and future binaries; a single unit test covers everyone.

**Cons**
- Touches ateapi → must re-verify the control plane's shutdown still behaves
  (larger review/test surface, higher risk for a "just add it to atelet" issue).
- Requires designing the helper's API (param list vs options struct; does it own
  the `signal.NotifyContext`, the `Serve` call, both?).
- Slightly larger PR; couples the #719 fix to an ateapi refactor.

**Recommendation:** Option A for this issue (keeps #719 focused and low-risk),
with a fast-follow to Option B if we want to consolidate atelet + ateapi + atenet
onto one helper. If the team prefers to pay the refactor cost once, B is the
cleaner end state.

**Decided: Option B** (after first shipping Option A on this branch, then
consolidating). The drain lives in `internal/serverboot/drain.go` as
`DrainGRPCOnShutdown(ctx, srv, readiness, delay, timeout)`, taking a small
`GRPCServer` interface (`GracefulStop`/`Stop`) and explicit delay/timeout
parameters (each binary passes its own flag values). Both `cmd/atelet/main.go`
and `cmd/ateapi/main.go` call it; their local `drainOnShutdown` copies are
deleted. The package keeps the `serverboot` name (a rename to `serverutil` was
tried and reverted): everything in it — including the shutdown hooks — is
wired from `main()` during boot, and it already owned the shutdown-side
primitives (`Readiness`, `ShutdownProvider`). atenet's ad-hoc `GracefulStop`
calls (`cmd/atenet/internal/router/extproc.go`, `xds.go`) have different
semantics (no readiness, no force-timeout) and were deliberately left alone.

---

## Decision 2 — Force-stop timeout policy

### Option 1: Long, checkpoint-sized timeout

Set `drain-timeout` to minutes (sized to a worst-case checkpoint/restore) and
bump the DaemonSet `terminationGracePeriodSeconds` to cover
`drain-delay + drain-timeout`.

**Pros**
- An in-flight `Checkpoint`/`Restore` **completes** before force-stop → no
  spurious CRASHED actor, no aborted multi-GiB upload (avoids the `TODO(#362)`
  data-loss path).
- Safest for actor durability, which is atelet's whole reason to exist.

**Cons**
- Slow node drain: a terminating atelet can hold the node for minutes.
- Must keep `terminationGracePeriodSeconds` ≥ the sum, or k8s SIGKILLs anyway
  and the long timeout buys nothing.
- Picking "worst case" is guesswork; too-long stalls autoscaling/upgrades.

### Option 2: Short timeout, like ateapi (~15s)

Reuse ateapi's ~13s delay + ~15s timeout; fits the default 30s grace with no
manifest grace change.

**Pros**
- Fast, predictable shutdown; consistent numbers with ateapi.
- No `terminationGracePeriodSeconds` change needed.

**Cons**
- **Force-cancels any in-flight checkpoint mid-upload → actor marked CRASHED**
  (`main.go:400`, `TODO(#362)`). Directly harmful for atelet's long RPCs.
- "Let in-flight ones finish" (the issue's own wording) becomes false for
  exactly the RPCs that most need to finish.

### Option 3: Configurable flags, long default (hybrid)

Add `--drain-delay` / `--drain-timeout` flags (matching ateapi's surface) but
default `--drain-timeout` to a long, checkpoint-appropriate value; set the
manifest `terminationGracePeriodSeconds` to match the default.

**Pros**
- Operators tune per environment without a rebuild; same flag surface as ateapi.
- Safe default (long) while allowing a short override where fast drain matters.
- Avoids hardcoding a single worst-case guess.

**Cons**
- Flag/grace-period coupling is a footgun: a long `--drain-timeout` with an
  unchanged `terminationGracePeriodSeconds` is silently defeated by SIGKILL.
  (ate-api's manifest comment already warns "the sum must fit within
  terminationGracePeriodSeconds" — replicate that comment.)

**Recommendation:** Option 3 — it *is* the ateapi pattern (configurable flags),
just with atelet-appropriate defaults, and it keeps the safe behavior by default.
Whatever the chosen default, the manifest `terminationGracePeriodSeconds` must be
set to `drain-delay + drain-timeout` with the same warning comment as ateapi.

**Decided: Option 3**, with defaults `--drain-delay=0` (see the note below —
atelet is not behind a Service, so there is no route-drain window to cover) and
`--drain-timeout=5m` (checkpoint-sized). The manifest sets
`terminationGracePeriodSeconds: 330` (0 + 5m + slack for the force `Stop()` and
the tracer/meter flush on exit).

### Note on `drain-delay` for atelet specifically

ateapi's `drain-delay` exists so round-robin gRPC clients re-resolve DNS and stop
routing before the drain starts. atelet is **not behind a Service** — ateapi
dials it directly by pod IP via a pod informer (`AteletDialer`,
`controlapi/dialer.go`). So atelet's `drain-delay` really only needs to cover the
window for ateapi's informer to observe the pod's `DeletionTimestamp` and stop
issuing new dials. A small delay (or even 0) may suffice; keep it configurable
and document the different rationale.

---

## Implementation outline (independent of the two choices above)

1. **Signals** — in `main()`, replace the plain `ctx` serving path with
   `shutdownCtx, stopSignals := signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)`;
   `defer stopSignals()`. Add `os/signal` + `syscall` imports.
2. **Readiness + healthz** — build a `readiness := &serverboot.Readiness{}` and
   pass it to `serverboot.StartMetricsServer` with `Readiness: readiness,
   EnableHealthz: true` (`main.go:110`). Reuses existing `serverboot` types.
3. **Drain wiring** — capture `svr`, start the drain (local `drainOnShutdown`
   for Option A, or `serverboot.DrainGRPCOnShutdown` for Option B) returning a
   `drainDone` channel; keep `svr.Serve(lis)`; after it returns, `<-drainDone`
   then log "Shutdown complete". Same control flow as `ateapi/main.go:233–239`.
4. **Flags** — add `--drain-delay` / `--drain-timeout` (Option 3), or hardcode
   (Options 1/2), following the chosen timeout policy.
5. **Manifest** — `manifests/ate-install/atelet.yaml`: add
   `terminationGracePeriodSeconds` sized to `delay + timeout` with the ateapi
   warning comment; optionally add `/readyz` readiness + `/healthz` liveness
   probes on port 9090 (mirroring `ate-api-server.yaml:155–170`). Note: atelet
   currently has no probes, so probes are additive/forward-looking, not required
   for the drain itself to work.

## Files to touch

- `cmd/atelet/main.go` — signals, readiness, drain wiring, flags (and local
  `drainOnShutdown` if Option A).
- `internal/serverboot/serverboot.go` — only if Option B (new helper) is chosen;
  then `cmd/ateapi/main.go` switches to it too.
- `manifests/ate-install/atelet.yaml` — grace period (+ optional probes).

## As implemented (branch `issue-719`)

- `internal/serverboot/drain.go` (new): `DrainGRPCOnShutdown(ctx, srv,
  readiness, delay, timeout) <-chan struct{}` — marks not-ready, sleeps
  `delay`, `GracefulStop()`s, force-`Stop()`s after `timeout`. `srv` is a
  small `GRPCServer` interface (`GracefulStop`/`Stop`) satisfied by
  `*grpc.Server`. The package doc now says "lifecycle boilerplate … and the
  graceful gRPC drain used on shutdown".
- `cmd/atelet/main.go`:
  - `--drain-delay` (default `0`) and `--drain-timeout` (default `5m`) flags,
    with a comment explaining why atelet's defaults differ from ateapi's.
  - `shutdownCtx` via `signal.NotifyContext(SIGTERM, os.Interrupt)`, kept
    separate from the work `ctx`.
  - `readiness := &serverboot.Readiness{}` wired into `StartMetricsServer`
    with `EnableHealthz: true`, so `/readyz` flips to 503 on drain while
    `/healthz` stays 200.
  - `drainDone := serverboot.DrainGRPCOnShutdown(shutdownCtx, svr, readiness,
    *drainDelay, *drainTimeout)`; `main` blocks on `<-drainDone` after `Serve`
    returns so the deferred tracer/meter flushes still run.
- `cmd/ateapi/main.go`: its local `drainOnShutdown` is replaced by the same
  one-line `serverboot.DrainGRPCOnShutdown` call with its own flag values
  (13s/15s defaults unchanged). Behavior is byte-for-byte identical (same log
  lines, same sequence).
- `internal/serverboot/drain_test.go` (new): a loopback-TCP gRPC server with a
  blocking handler (bufconn is not vendored).
  `TestDrainGRPCOnShutdownInFlightFinishes` covers the graceful path,
  `TestDrainGRPCOnShutdownForceStopsAfterTimeout` the force-stop path; both
  assert the readiness flip. One suite covers both binaries' drain.
- `manifests/ate-install/atelet.yaml`: `terminationGracePeriodSeconds: 330`,
  `--drain-delay=0s` / `--drain-timeout=5m` in args, `/readyz` readiness +
  `/healthz` liveness probes on port 9090. The kind overlay
  (`manifests/ate-install/kind/atelet/`) replaces the container args wholesale,
  so kind runs on the flag defaults (which match), and inherits the grace
  period and probes from the base.

## Verification

- **Unit** — there is currently **no** drain test for ateapi to copy. Add a
  focused test on the drain function: a stub `*grpc.Server` / fake, cancel the
  context, assert `MarkNotReady` fires, `GracefulStop` is attempted, and
  force-`Stop` triggers after `drain-timeout`. (For Option B, the test lives in
  `serverboot` and covers both binaries at once.)
- **Build/lint** — `go build ./cmd/atelet/...` and the repo's usual `go vet` /
  lint.
- **Manual (kind)** — bring up the kind quickstart (README), start a
  checkpoint/long RPC against atelet, `kubectl delete pod -n ate-system -l
  app=atelet` (or `kubectl drain` the node), and observe atelet logs:
  "Shutdown signal received; draining" → NotReady → in-flight RPC finishes →
  "Drain completed within deadline" (or "Drain deadline exceeded; forcing stop"
  if you force a short timeout). Confirm `/readyz` returns 503 during drain while
  `/healthz` stays 200.
- Confirm the actor is **not** left CRASHED when an in-flight checkpoint is
  allowed to finish under the chosen timeout.

### Verification results

- **Unit**: both drain tests pass (`go test ./cmd/atelet/... -run Drain`),
  covering the graceful path (in-flight RPC completes, readiness flips) and the
  force-stop path (stuck RPC cancelled at `drain-timeout`).
- **Live (kind)**: deployed via `hack/install-ate-kind.sh --deploy-atelet`,
  temporarily set `--drain-delay=25s` to make the window observable, then
  `kubectl delete pod` (SIGTERM). Observed `/readyz=503` while `/healthz=200`
  for the whole window, and the exact log sequence with the 25s delay honored
  to the second: `Shutdown signal received; draining` → (+25s) `Starting gRPC
  drain` → `Drain completed within deadline` → `Shutdown complete`. The
  temporary flag was reverted.
- **Not exercised live**: force-stop with a real in-flight Checkpoint. The demo
  workload's checkpoint is sub-millisecond (no window to race), so this path is
  covered by the unit test only. A reliable live repro needs a long RPC — e.g.
  a large-memory actor's suspend on a real cluster, or a cold-boot
  `resume --boot` with a large uncached image (whose abort is recoverable,
  unlike a checkpoint's).

## Production failure modes (pressure tests)

Findings from walking the design through real-world failure scenarios.

### 1. ateapi → atelet race: RPC arrives just after SIGTERM

`AteletDialer.DialForWorker` resolves atelet by node + pod IP and **never
consults pod readiness**, so no `drain-delay` value closes this race — the
readiness probe is observability, not routing. Mechanics: `GracefulStop()`
closes the listener and GOAWAYs existing conns; a new RPC on ateapi's cached
conn fails fast with `Unavailable`. The dialer configures no gRPC retry policy,
so the error propagates to the workflow step; `Unavailable` carries no
`ateerrors.Reason` tag → treated as transient → **actor not crashed**, and the
error surfaces to the caller per client-driven forward recovery (the client
retries; `IsComplete` fast-forwards). Sharp edge: on retry,
`AssignWorkerStep.IsComplete` keeps the same worker, so retries target the same
node's atelet — fine during a DaemonSet rollout (new pod, new UID → fresh
dial), but during a node drain retries hammer a dead node until the syncer
reconciles. Transient user-visible errors during the restart window are
by-design; zero-error rollouts would need client-side retry/reassignment, not
more drain-delay.

### 2. Long-lived streams vs `GracefulStop()`

Non-issue today: `AteomHerder` has exactly three **unary** RPCs (no streams, no
watches; liveness is HTTP `/healthz` outside the gRPC server), so nothing
open-ended can pin the drain. The residual risk is a *stuck unary* RPC (hung
ateom socket, stalled GCS stream) — which is precisely what the force-stop
backstop is for. **Forward constraint**: if streaming RPCs are ever added (e.g.
actor log streaming), their handlers must select on a shutdown-aware context
and close early, so only transactional work holds the drain open.

### 3. Spot VMs / short-notice preemption (30s vs 5m)

`terminationGracePeriodSeconds` is a *request*; a hypervisor ACPI kill (spot
preemption, some node upgrades) caps the real budget at ~30s regardless of the
manifest. If a multi-GiB upload is severed: **no corrupt snapshot is possible**
(GCS/S3 writes are atomic on finalize, and `InProgressSnapshot` is only
promoted to `LatestSnapshotInfo` in `FinalizeSuspendedStep` after atelet
returns success — the previous good snapshot stays authoritative), **but the
actor record is lost**: `releaseActorOnDeadWorker`
(`cmd/ateapi/internal/controlapi/syncer.go`) moves any actor not yet
`SUSPENDED` — including one mid-`SUSPENDING` — to `CRASHED`, and `CRASHED` is
terminal today (resume requires `SUSPENDED`/`PAUSED`; only delete is allowed).
The last good snapshot object survives in the bucket with no API path to
resurrect the actor from it. **Operational guidance**: worker pools on
spot/preemptible nodes should size `drain-timeout` to the real notice (≤25s),
accept that in-flight checkpoints there crash actors, and prefer frequent
commit/pause snapshots to bound the loss window. Durable fix tracked as the
"resume-from-CRASHED" follow-up below.

### 4. Second SIGTERM is swallowed during the drain

`signal.NotifyContext` keeps the signals registered until `stopSignals()` runs
(deferred to main's exit), so after the first SIGTERM cancels the context,
subsequent SIGTERM/Ctrl+C are delivered and discarded — an operator watching a
5-minute drain can only escalate with SIGKILL (losing the force-`Stop()`
bookkeeping and OTel flushes). In-cluster this barely matters (the kubelet
sends one SIGTERM then SIGKILL at the grace deadline). ateapi has the identical
behavior, and #719's mandate was a faithful mirror, so this was left as-is. The
one-line fix — call `stopSignals()` right after `<-ctx.Done()` in
`drainOnShutdown`, restoring the default disposition so a second signal
terminates immediately — is tracked as a follow-up for **both** binaries.

### 5. atelet restart vs running sandboxes on the node

Running ateom sandboxes are **undisturbed by design** — they are pods of the
WorkerPool Deployment, not children of atelet; the `runsc` processes live in
the ateom pod's namespaces. atelet is stateless between RPCs: all durable
per-actor state lives on the shared hostPath (`/var/lib/ateom-gvisor`). There
is no re-adoption step — the new atelet pod starts with an empty conn cache and
lazily reconnects (reads the on-disk `sandboxAssetsRecord`, dials the ateom
per-pod-UID unix socket fresh) on the next RPC for that actor. Caveats are
about *disk state*, not processes: an RPC killed mid-flight leaves partial
artifacts (half-written bundles, in-progress checkpoint files) cleaned lazily
by `resetActorDirs` on the actor's next Run/Restore, and the known orphaned
external-volume-mount TODOs in `main.go` remain. A future atelet startup sweep
(reconcile actor dirs against live ateom sockets, GC orphans) would close both.

## Follow-ups

1. **Resume-from-CRASHED** (control plane): allow resuming a `CRASHED` actor
   from its intact `LatestSnapshotInfo`, closing the spot-preemption dead end
   in failure mode 3.
2. **Second-signal fast exit** (atelet + ateapi): `stopSignals()` after the
   first signal in `drainOnShutdown` so a repeat SIGTERM/Ctrl+C terminates
   immediately (failure mode 4).
3. **Option B refactor**: ~~extract the drain helper into `serverboot` and
   adopt it in atelet and ateapi~~ — **done on this branch** (see "As
   implemented"). Remaining optional piece: adopt it in atenet, whose ad-hoc
   `GracefulStop` calls have different semantics today.
4. **atelet startup sweep** (optional): GC partial bundles/checkpoint files and
   orphaned volume mounts left by an interrupted RPC (failure mode 5).
