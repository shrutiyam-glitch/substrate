# Follow-up issues from #719 (atelet graceful shutdown)

Draft texts for the two follow-up issues identified while pressure-testing the
shutdown design (see `atelet-graceful-shutdown.md`, "Production failure modes"
§3 and §4). Copy each into a GitHub issue.

---

## Issue 1: Implement revert: CRASHED → SUSPENDED from the last good external snapshot

> Suggested as a **sub-issue of #119** (it implements the `revert` recovery
> path that #119's state-machine design already specifies for CRASHED).

### Problem

#119 designs `CRASHED` recovery via `ate actor revert` — "discards failed
local data and returns to SUSPENDED at the last stable commit" — and #292
defines `CRASHED` as requiring manual intervention "to revert to a known good
state." But the revert path is not implemented: today `CRASHED` is a hard
dead end in code. The resume workflow requires `SUSPENDED` or `PAUSED`
(`LoadActorForResume` prerequisite), there is no revert API, and
`DeleteActor` is the only transition out.

Yet in the most common crash scenario the actor's durable state is
**provably intact**:

- When a worker pod vanishes mid-suspend (node drain force-stop, spot VM
  preemption, node crash), `releaseActorOnDeadWorker`
  (`cmd/ateapi/internal/controlapi/syncer.go`) moves the actor to
  `STATUS_CRASHED` and clears `InProgressSnapshot` — but leaves
  `LatestSnapshotInfo` untouched.
- The severed upload can never produce a corrupt object (GCS/S3 writes are
  atomic on finalize), and `InProgressSnapshot` is only promoted to
  `LatestSnapshotInfo` in `FinalizeSuspendedStep` *after* atelet reports
  success. So after any severed checkpoint, `LatestSnapshotInfo` still points
  at the previous good external snapshot — revert is safe by construction for
  the external case.

Result: the snapshot object sits in the bucket, recoverable in principle,
with no API path to use it — only delete (losing the actor's
identity/history) or manual store surgery.

This is sharpest on spot/preemptible worker-node pools, where the hypervisor
caps shutdown at ~30s regardless of `terminationGracePeriodSeconds` — an
in-flight multi-GiB checkpoint there is *guaranteed* to be severed, so on
spot pools this is a fleet-level need, not an edge case. See "Production
failure modes" §3 in `docs/dev/atelet-graceful-shutdown.md`.

### Proposal

Implement the #119 revert shape — **CRASHED → SUSPENDED**, then the existing
resume path runs unchanged (rather than resuming directly from CRASHED):

- `RevertActor` (new RPC / `kubectl ate revert actor`, per #119's
  `ate actor revert`): allowed from `CRASHED`; validates
  `LatestSnapshotInfo`, clears any stale pod pointers, sets `SUSPENDED`.
  The subsequent `ResumeActor` needs no changes.
- `ExternalSnapshotInfo` → always revertible (survives node loss; the main
  case, and the safe-by-construction one).
- `LocalSnapshotInfo` → revertible to *resumable* state only if a node in
  `node_vms_with_local_snapshots` still exists; otherwise reject with a clear
  error (the local snapshot died with the node — #643's force-delete is the
  escape hatch there).
- No snapshot info at all → reject (nothing to restore; candidate for the
  golden-snapshot reset-to-template variant, or #643's force-delete).

Semantics to decide:

- Whether revert should be blocked while a concurrent transition holds the
  actor lock (it should follow the same per-actor locking as the other
  workflows; #554 reentrancy applies).
- Whether a golden-snapshot fallback (reset-to-template for crashed actors
  with no per-actor snapshot) belongs here or in a follow-up.

### Prior art / relationship to existing issues

- **#119** (state machine design, P0): specifies `revert` from CRASHED to
  SUSPENDED at the last stable commit, plus `dump`/`commit` variants. This
  issue is the implementation of that revert path for the external-snapshot
  case; adopt its vocabulary (`revert`, not `resume --from-crashed`).
- **#292** (implement transitions *to* CRASHED): the entry side of the same
  state; presumes revert as the exit. Cross-link.
- **#643** (recovery from failed resume / force-delete escape hatch): handles
  stuck-RESUMING and the case where revert itself fails or nothing is
  recoverable — complementary, not overlapping. Cross-link.
- **#660** (resume-from-pause capacity failures): adjacent local-snapshot
  concern, unrelated mechanism.
- **#362** (checkpoint upload failure forces CRASHED because snapshot files
  aren't cached on failure): one of the producers of revertible CRASHED
  actors.
- **#719 / #517**: graceful atelet shutdown reduces how often force-stops
  crash actors, but cannot help under a 30s hypervisor deadline — revert is
  the durable fix.

### Acceptance criteria

- [ ] A `CRASHED` actor with intact `ExternalSnapshotInfo` can be reverted to
      `SUSPENDED` and then resumed to `RUNNING`, retaining state from the
      last good snapshot.
- [ ] A `CRASHED` actor with no usable snapshot is rejected with a clear
      error (not `FailedPrecondition` boilerplate), pointing at the
      force-delete path.
- [ ] The local-snapshot case is handled (node still present → revert
      succeeds; node gone → clear rejection).
- [ ] Behavior is covered by workflow tests (revert happy path,
      local-snapshot-node-gone path, no-snapshot path).
- [ ] State machine docs (`docs/dev/onboarding-actor-lifecycle.md`) and
      `Actor.Status` enum comments updated with the new edge.

---

## Issue 2: Graceful drain: second SIGTERM should force immediate exit (atelet + ateapi)

### Problem

Both ateapi and atelet use the same graceful-shutdown pattern:

```go
shutdownCtx, stopSignals := signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)
defer stopSignals()
...
drainDone := drainOnShutdown(shutdownCtx, srv, readiness)
```

`signal.NotifyContext` keeps SIGTERM/SIGINT registered until `stopSignals()`
runs — which is deferred to the end of `main`. So after the first signal
cancels the context and the drain begins, **every subsequent SIGTERM or
Ctrl+C is silently discarded**. An operator watching atelet's drain (up to
`--drain-timeout=5m`) or ateapi's (~28s) has no escalation short of SIGKILL,
which skips the force-`Stop()` bookkeeping and the deferred OTel
tracer/meter flushes.

In-cluster impact is minimal (the kubelet sends exactly one SIGTERM, then
SIGKILL at the grace deadline), but it hurts anyone running the binaries
directly, in dev loops, or intervening on a wedged drain.

### Proposal

Adopt the standard "first signal graceful, second signal immediate" pattern.
The minimal version is one line in each binary's `drainOnShutdown`:

```go
go func() {
    defer close(done)
    <-ctx.Done()
    stopSignals() // restore default disposition: next SIGTERM/SIGINT kills the process
    slog.InfoContext(ctx, "Shutdown signal received; draining (send again to exit immediately)")
    ...
```

(`stopSignals` needs to be passed in or captured; today it's only deferred in
`main`.) After `stopSignals()`, Go's default disposition applies, so a second
signal terminates the process immediately — no extra channels or goroutines.

A slightly richer variant — second signal triggers `srv.Stop()` + orderly
exit instead of process death — requires a raw `signal.Notify` channel and is
probably not worth the extra machinery; the deferred flushes are best-effort
anyway once an operator is force-killing.

Apply to both `cmd/ateapi/main.go` and `cmd/atelet/main.go` (they
deliberately mirror each other; see #719). If the Option-B refactor from
`docs/dev/atelet-graceful-shutdown.md` (shared `serverboot` drain helper)
lands first, implement it there once.

### Acceptance criteria

- [ ] During a drain, a second SIGTERM/SIGINT terminates the process
      immediately (verify with a stuck in-flight RPC).
- [ ] Single-signal behavior is unchanged: full graceful drain, force-stop at
      `drain-timeout`, clean `Shutdown complete`.
- [ ] The drain log line tells the operator a second signal exits immediately.
- [ ] Both binaries behave identically.

---

## Issue 3: atenet-router: shutdown-outcome metric (count unclean drains)

> Requested in review on #774
> (https://github.com/agent-substrate/substrate/pull/774#discussion_r3732192560).

### Problem

The router's graceful drain (`cmd/atenet/internal/router/drain.go`) degrades
loudly-but-log-only on its two unclean paths:

- **dataplane drain incomplete** — downstream connections still active when
  the dataplane drain window expires (idle keep-alives that ignore GOAWAY are
  benign; in-flight requests cut at the subsequent dataplane SIGTERM are not);
- **ext_proc force-stop** — in-flight ext_proc streams (parked requests
  included) still open at `--drain-timeout`, cancelled by `Stop()`.

Operators have no aggregate view of how many shutdowns were unclean, which
flavor, or whether a rollout regression made them more frequent. `TODO`
markers sit at both sites in `drain.go`.

### Proposal

Add an OTel counter on the `atenet-router` meter, recorded once per shutdown:

- `atenet.router.shutdown.outcome` (counter), attribute `outcome`:
  `clean` | `dataplane_drain_incomplete` | `extproc_force_stopped`
  (record both attributes when both occur).
- Optionally a `atenet.router.shutdown.duration` histogram (SIGTERM →
  sequence complete) to watch drift toward `terminationGracePeriodSeconds`.

Caveat to solve: the meter provider is flushed by `ShutdownProvider` after
the drain completes — recording must happen before the final flush (it does:
`stopRest` runs before `Run` returns), but verify the periodic reader exports
a last batch on shutdown, or force-flush.

### Acceptance criteria

- [ ] A clean drain records `outcome=clean`; a force-stopped drain records
      `outcome=extproc_force_stopped` (unit-testable via the drain fakes).
- [ ] The metric is documented in `docs/observability.md` alongside the other
      `atenet.router.*` instruments.
- [ ] The `TODO`s in `drain.go` are replaced by the recording calls.
