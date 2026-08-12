# Design: atenet router graceful shutdown

Status: **decided and implemented** (branch `issue-721`). Decisions:
1 → Option B (atenet drives the Envoy admin API; a `preStop` sleep on the
Envoy container turned out to be **required**, not optional — see below),
2 → Option A (`serverboot.Readiness` + `/readyz` on :9090),
3 → Option A (`--drain-delay=13s`),
4 → Option B (derived `--drain-timeout`, validated ≥ park budget),
5 → Option B (atenet-specific orchestrator, `drain.go`).
The options and their pros/cons are kept below for the record. Implementation
notes: an Envoy `preStop` hook is required because the kubelet SIGTERMs both
containers simultaneously and Envoy fast-exits — without it, Envoy is gone
before the drain sequence starts. After review, the initial fixed
`sleep 35` was replaced by a **file handshake**: the router writes a
drain-complete marker (`--drain-complete-file`, default
`/var/run/atenet/drain-complete` on a pod-shared emptyDir) at the end of its
sequence, and Envoy's preStop polls for it — so Envoy exits seconds after an
idle drain yet is covered up to the grace period on a busy one. The marker is
removed at router startup (emptyDir survives container restarts; a stale
marker would release the hook instantly on a later drain), and a router crash
before writing it degrades to the kubelet killing the hook at
`terminationGracePeriodSeconds` — slower cleanup, never a wedge. The xDS
server now hard-`Stop()`s instead of `GracefulStop()` (ADS streams are
open-ended, so the old graceful stop could never complete while Envoy was
connected — a pre-existing wedge this work fixes). Companion to
`atelet-graceful-shutdown.md` (#719).

## Context

The issue: *"Atenet needs to properly handle termination. It needs to shut
down the exposed services (e.g. ext_proc) but more importantly, it needs to
coordinate how Envoy will stop accepting requests, drain existing, etc."*

Target behavior — the ate-api pattern, extended for a two-container data
plane: on SIGTERM, flip readiness so the Service stops sending new
connections, let in-flight requests finish, **drain the Envoy sidecar instead
of letting it die mid-connection**, and size `terminationGracePeriodSeconds`
to also outlive the parking budget (`--parked-request-budget`) so parked
requests finish normally instead of resetting.

### What exists today (verified)

- **atenet already catches SIGTERM — but crudely.** `RouterServer.Run`
  (`cmd/atenet/internal/router/router.go:123-128`) does a raw
  `signal.Notify(SIGINT, SIGTERM)` → `cancel()` on the root context. Every
  goroutine (health checker, controller, statusz, both gRPC servers) stops
  simultaneously.
- Each gRPC server reacts to that cancellation with a bare `GracefulStop()`:
  ext_proc (`extproc.go`, `Serve`: `<-ctx.Done() → grpcServer.GracefulStop()`)
  and xDS (`xds.go`, same shape). **No readiness flip, no delay, no force-stop
  timeout** — a wedged stream stalls exit until the kubelet SIGKILLs.
- **The manifest has nothing**: `manifests/ate-install/atenet-router.yaml` has
  no readiness/liveness probes, no `terminationGracePeriodSeconds` (default
  30s), no `preStop` hooks.
- **The Envoy sidecar is the real victim.** Kubernetes sends SIGTERM to both
  containers at the same instant; Envoy's default SIGTERM behavior is a fast
  exit. Today a pod delete resets every established client connection and
  every in-flight proxied request immediately, regardless of what the atenet
  container does.
- The metrics server is started without Readiness/healthz
  (`router.go:165` — `StartMetricsServer(ctx, {Addr: ...})` only).

### Why atenet is harder than atelet (#719)

| Dimension | atelet | atenet router |
|---|---|---|
| Behind a Service? | No (dialed by pod IP) → `drain-delay=0` | **Yes** (`svc/atenet-router` 80/443 → Envoy) → a route-drain window is load-bearing, like ateapi |
| Processes | one gRPC server | **two containers** (atenet + Envoy), and atenet runs ext_proc + xDS + statusz + health + controller |
| In-flight work | its own RPCs (minutes: snapshots) | split: **header-phase ext_proc streams** (bounded by the park budget, ≤5s default) live in atenet; **proxied request bodies/responses** (bounded by the 10s route timeout) live in Envoy |
| Order sensitivity | none | ext_proc is `failClosed` for Envoy — **atenet must outlive Envoy's drain**, or every request Envoy still accepts fails 5xx |
| Data-plane variant | n/a | agentgateway mode has no Envoy admin API and a static config |

Key simplification discovered in the code: the ext_proc `ProcessingMode` only
SENDs request headers (everything else SKIP), so an ext_proc stream is held
open only for the header-processing phase — i.e. **parked requests are the
only long-lived ext_proc streams, and the park budget already bounds them**
(`--parked-request-budget`, default 5s; Envoy's ext_proc `MessageTimeout` is
budget+5s). atenet's own drain is therefore cheap and bounded; the genuinely
new work is **sequencing Envoy**.

### The required shutdown order

```
SIGTERM (both containers)
  1. readiness → NotReady            (Service endpoint removal begins)
  2. wait drain-delay                (endpoints propagate; no NEW connections)
  3. drain Envoy                     (existing downstream conns/requests finish;
                                      ext_proc still alive → requests accepted
                                      during the window still route)
  4. GracefulStop ext_proc           (parked streams finish ≤ park budget,
                                      force-Stop after drain-timeout)
  5. stop xDS / controller / statusz (Envoy is gone or on last-known config;
                                      order no longer matters)
  6. exit — before terminationGracePeriodSeconds
```

Steps 1–2 are exactly ateapi's pattern. Steps 3–4 are the atenet-specific
substance, and their relative order is forced by `failClosed`.

### Architectural Review

This is an exceptionally well-thought-out design. Networking control-planes that sit in front of Envoy proxies (especially with custom `ext_proc` filters and request parking) are notoriously difficult to drain gracefully. This implementation identifies and solves the core traps that usually break systems like this in production:

- **The `ext_proc` `failClosed` Dependency**: Envoy’s `ext_proc` filter is `failClosed`. If the `extproc` gRPC server shuts down before Envoy finishes draining its downstream connections, Envoy will fail all remaining in-flight requests with HTTP 500s. The ordering in `drain.go` (`Readiness 503` → `Drain Delay` → `Envoy Admin Drain` → `extproc GracefulStop` → `stopRest`) is mathematically and operationally exact.
- **Kubelet Dual-SIGTERM Synchronization**: In Kubernetes multi-container pods, both `atenet-router` and `envoy` receive `SIGTERM` simultaneously. By adding `lifecycle.preStop.exec.command: ["sleep", "35"]` to Envoy, Envoy is prevented from fast-exiting on `SIGTERM` while the Go control-plane orchestrates the graceful drain via Envoy's admin API (`/drain_listeners`).
- **Rolling Update Safety**: Setting `maxSurge: 1` and `maxUnavailable: 0` ensures a replacement router pod is `Ready` before the terminating pod begins its drain sequence, avoiding total routing outages during deployments. *(Update: the explicit strategy block was later dropped — upgrades are whole-system swaps per #473, so per-Deployment rollout tuning is not part of the design; Kubernetes' defaults for a 1-replica Deployment compute to the same values anyway.)*
---

## Decision 1 — Who coordinates Envoy's drain?

### Option A: `preStop` hook on the Envoy container (Kubernetes-native)

Give the Envoy container a `preStop` that calls the admin API
(`POST /drain_listeners?graceful&skip_exit`) and then sleeps a fixed window,
so Envoy keeps serving established connections while atenet drains.

**Pros**
- No atenet code at all; pure manifest change.
- Works even if the atenet container is wedged or crashed.
- Standard, widely-documented Envoy pattern (Istio does a variant of this).

**Cons**
- Fixed sleep is guesswork — too short resets connections, too long stalls
  every rollout by the full window.
- `preStop` `httpGet` only issues GET; the drain endpoints need POST → an
  `exec` hook needing a shell + curl/wget **in the Envoy image** (must be
  validated for the pinned `envoyproxy/envoy:v1.30-latest` image), or a
  `sleep`-only hook that relies on Envoy's `--drain-time-s` behavior.
- Sequencing is implicit (two containers counting down independently), not
  observable from atenet's logs/metrics.

### Option B: atenet drives the Envoy admin API from its drain sequence

atenet's shutdown orchestrator (in `RouterServer.Run`) calls the sidecar's
admin endpoint (`127.0.0.1:9901`, same pod): `POST /healthcheck/fail` +
`POST /drain_listeners?graceful&skip_exit`, then **polls**
`/stats?filter=downstream_cx_active` until connections hit zero or a deadline
elapses, then proceeds to stop its own servers.

**Pros**
- One owner of the whole sequence — ordering is explicit, logged, and
  metric-able (mirrors the #719 log line pattern:
  "draining Envoy" → "Envoy drained; stopping ext_proc" → …).
- **Adaptive**: exits the wait as soon as connections actually reach zero
  instead of always sleeping a worst-case window.
- No dependency on shell/curl inside the Envoy image; plain Go HTTP client.
- Works identically in managed and standalone modes (admin is always
  localhost within the pod).

**Cons**
- More code (an `envoyDrainer` with poll loop, error tolerance, and a fake
  admin server for tests).
- Helps nothing if the atenet container itself crashed — but then there is no
  drain of any kind, so a *small* belt-and-braces `preStop` sleep on Envoy
  (a few seconds, no shell needed on k8s ≥1.30 via the native `sleep` action)
  still pairs well with this option.
- atenet must tolerate admin-API failure (Envoy already gone) and continue
  its own drain — degrade, never wedge.

### Option C: passive — readiness flip + grace period only

No active Envoy drain; rely on endpoint removal to stop new traffic and hope
in-flight requests finish before Envoy exits.

**Pros**
- Cheapest; identical to what ateapi does (which has no sidecar).

**Cons**
- **Does not solve the stated problem**: Envoy still receives its own SIGTERM
  at t=0 and fast-exits, resetting established connections mid-response. The
  issue explicitly calls out "drain the Envoy sidecar instead of letting it
  die mid-connection" — C fails that requirement unless combined with at
  least a preStop sleep, at which point it becomes a weaker Option A.

**Note on native sidecars (k8s ≥1.28):** making Envoy an init-container
sidecar (`restartPolicy: Always`) gives reverse-order termination — Envoy
would outlive the atenet container. That is the **wrong order here**: ext_proc
is `failClosed`, so an Envoy that outlives atenet fails every request it still
accepts. Keep both as regular containers with atenet sequencing the drain.

**Recommendation: Option B**, with an optional small native-`sleep` preStop on
the Envoy container as a safety margin so Envoy cannot exit before atenet's
sequence has at least started. Option A is the fallback if we want a
manifest-only first step.

---

## Decision 2 — Readiness signaling

### Option A: `serverboot.Readiness` + `/readyz` on the atenet container

Wire `Readiness`/`EnableHealthz` into the existing metrics server
(`router.go:165`), add a pod `readinessProbe` against `:9090/readyz` on the
atenet container. Pod readiness gates Service endpoints for the whole pod —
it does not matter that client traffic targets the Envoy container's ports.

**Pros**
- Verbatim reuse of the #719/ateapi machinery (`serverboot.Readiness`,
  `MetricsServerOptions{Readiness, EnableHealthz}`); zero new concepts.
- The readiness flip is step 1 of the same orchestrator that runs the rest of
  the sequence — one place, one log stream.

**Cons**
- The probe reflects the atenet container's intent, not the data plane's
  actual state (an unhealthy Envoy with a healthy atenet still reads Ready —
  though that is a pre-existing gap, not a regression).

### Option B: probe the Envoy admin (`/ready`) and flip it via `/healthcheck/fail`

**Pros**
- The probe reflects the actual data plane; `healthcheck/fail` is the
  idiomatic Envoy drain-start signal and pairs with Option B of Decision 1
  anyway.

**Cons**
- Readiness state then lives in a different container from the orchestrator;
  two things must agree.
- `atenet-router` also serves in agentgateway mode, which has a different
  health surface (`:15021/healthz/ready`) — Option A is dataplane-agnostic.

**Recommendation: Option A** for the Service gate (dataplane-agnostic,
mirrors the siblings), and — since Decision 1 Option B calls
`/healthcheck/fail` anyway — Envoy's own health signal flips as a side
effect. Add a `livenessProbe` on `/healthz` at the same time (diverges
correctly during drain, as in ateapi/atelet).

---

## Decision 3 — drain-delay (route-drain window)

Unlike atelet (`drain-delay=0`, dialed by pod IP), **atenet is behind a
Service**, so ateapi's original rationale applies fully: after NotReady, the
endpoint controller and kube-proxy/dataplane need time to observe the change
before the listener stops accepting.

- **Option A: mirror ateapi — `--drain-delay=13s`** with a `readinessProbe` of
  `periodSeconds: 2, failureThreshold: 3` (probe detection ≈6s + propagation
  margin). Consistent numbers across the fleet; proven.
- **Option B: shorter delay (~5s) with a faster probe** (`periodSeconds: 1,
  failureThreshold: 2`). Faster rollouts; tighter margins on slow dataplanes.

**Recommendation: Option A.** Rollout latency is dominated by the Envoy drain
window anyway; consistency wins.

---

## Decision 4 — timeout sizing (the parking connection)

The issue's explicit requirement: parked requests must **finish normally**
across a shutdown. The bounded quantities at defaults:

| Quantity | Value | Source |
|---|---|---|
| Park budget | 5s (configurable) | `--parked-request-budget` |
| ext_proc MessageTimeout | budget + 5s | `dataplane.go` |
| Envoy route timeout (proxied request) | 10s | `xds.go` route config |
| drain-delay | 13s (per Decision 3) | flag |

### Option A: fixed `--drain-timeout` flag (like ateapi/atelet)

Operator sets it; manifest comment warns it must cover the park budget.

**Pros:** consistent flag surface. **Cons:** silently wrong if someone raises
`--parked-request-budget` past it — exactly the class of coupling bug the
parking config already guards against elsewhere.

### Option B: derive the default from the parking config

Default `drain-timeout = parkBudget + routeTimeout + margin` (≈20s at
defaults), overridable by flag. Precedent: `config.go` already derives the
ext_proc circuit breaker from `--parked-request-max` and validates
`extproc-max-requests ≥ parked-request-max` at startup — deriving and
**validating** `drain-timeout ≥ parkBudget` fits the same philosophy.

**Pros:** can't be misconfigured below the parking budget without an explicit,
validated override. **Cons:** slightly magic; needs a clear `--help` string.

**Recommendation: Option B** — derived default + startup validation
(`drain-timeout ≥ park budget`, error out otherwise), keeping the flag for
override. Then:

```
terminationGracePeriodSeconds ≥ drain-delay (13s)
                              + Envoy drain window (≤ route timeout, 10s)
                              + drain-timeout (≈20s)
                              + slack (OTel flush, force-Stop)   → 60s
```

with the same "the sum must fit" manifest comment as ateapi/atelet.

---

## Decision 5 — code shape

### Option A: per-server reuse of `serverboot.DrainGRPCOnShutdown`

Call the shared helper for each gRPC server.

**Pros:** maximum reuse. **Cons:** the helper couples readiness-flip + delay +
one server; calling it twice double-flips readiness (harmless) and
double-sleeps the delay (harmful), and it cannot express the Envoy step or
the ext_proc-before-xDS ordering. It was designed for the one-server case.

### Option B: a small atenet-specific orchestrator that reuses the pieces

A `drainOnShutdown`-style sequencer in `router.go` owning the order
(readiness → delay → Envoy drain → ext_proc drain with timeout → xDS stop),
reusing `serverboot.Readiness` directly and reusing
`serverboot.DrainGRPCOnShutdown(ctx, extprocSrv, readiness, 0, timeout)`
semantics for the ext_proc step (or a thin variant without the readiness
param). The existing `<-ctx.Done() → GracefulStop` blocks in
`extproc.go`/`xds.go` are replaced by orchestrator-driven stops; the raw
`signal.Notify` in `Run` becomes `signal.NotifyContext` with the shutdown ctx
kept separate from the work ctx (the #719 pattern — today's shared-ctx cancel
kills the health checker and controller at t=0, which is fine, but the
ext_proc server must NOT be stopped at t=0).

**Pros:** expresses the real sequence; keeps the shared helper's contract
clean; each piece unit-testable. **Cons:** a second drain "shape" in the tree
(justified — the doc for #719 already noted atenet's semantics differ).

**Recommendation: Option B.**

---

## Out of scope

- **`atenet dns`**: its controller loop already exits on SIGTERM; CoreDNS has
  its own lifecycle. No user-visible in-flight work to drain (DNS answers are
  synthesized instantly). Separate, trivial track if ever needed.
- **agentgateway mode**: no Envoy admin API; drain becomes
  readiness → delay → ext_proc drain only, and the agentgateway container's
  own termination behavior is undocumented here. Flag as an open question in
  the PR; do not block on it (the mode is experimental).
- **Rollout availability / multi-replica HA**: out of scope by decision —
  upgrades are whole-system swaps (#473): a new system comes up and the old
  one terminates, so per-Deployment rolling-update tuning (`maxSurge` etc.)
  is not part of the design. The drain's job is making the old system's
  termination lossless. True HA (N replicas) remains its own track.
- **Second-SIGTERM fast exit**: same follow-up as ateapi/atelet (see
  `issue.md` draft 2); adopt here whenever that lands.

## Test scenarios

**The axis, as with atelet: what is in flight when SIGTERM lands.**

| # | In-flight at SIGTERM | Expected with this design | Today |
|---|---|---|---|
| 1 | Nothing (idle) | readiness flips, Envoy drains empty, fast clean exit | Envoy resets nothing; OK by luck |
| 2 | Active proxied request (response streaming from actor) | Envoy drain lets it complete (≤10s route timeout); client sees 200 | **connection reset** |
| 3 | **Parked request** (actor resuming, pool saturated) | ext_proc stream survives: park → resume → route → 200, within park budget; drain-timeout > budget guarantees it | reset or 5xx |
| 4 | New connection during drain window | endpoint removal steers it to other replicas / next pod; none arrive after delay | routed until Envoy dies mid-flight |
| 5 | New request on an **established** connection during drain | Envoy graceful drain sends GOAWAY/`Connection: close`; request completes then conn closes | reset |
| 6 | Wedged ext_proc stream (bug) | force-Stop at drain-timeout; exit before SIGKILL | hangs until SIGKILL |
| 7 | Envoy admin unreachable during drain | orchestrator logs, skips Envoy step, still drains ext_proc and exits | n/a |
| 8 | Grace-period misconfig (sum > grace) | startup validation catches drain-timeout vs budget; manifest comment covers the rest | silent SIGKILL |

**Unit tests** (patterned on `internal/serverboot/drain_test.go` + existing
router tests):
- Orchestrator ordering: readiness flips before the Envoy drainer is invoked,
  which returns before ext_proc `GracefulStop` is called (mock drainer +
  blocking ext_proc stream).
- Parked-request survival: park a request (resumer returning
  `FailedPrecondition` then success, as in `resumer_test.go`), fire the
  shutdown ctx, assert the stream completes with the routing mutation — not
  an error.
- Force-stop: never-completing stream + tiny drain-timeout → force stop,
  orchestrator returns.
- `envoyDrainer` against an `httptest` fake admin: POSTs
  `/healthcheck/fail` + `/drain_listeners?graceful&skip_exit`, polls stats
  until zero / deadline, tolerates connection-refused.
- Config: derived drain-timeout default; validation error when overridden
  below park budget.

**Integration (kind)** — automated as `hack/verify-atenet-drain.sh` (lives in
hack/ rather than the e2e suites because it deletes the shared router pod and
the e2e runner executes suites in parallel; run it alone against a cluster
with the counter demo installed). It covers the parked-request survival,
readiness divergence, termination window, log sequence, and replacement-pod
checks below. Manual variants:
- Pod delete under continuous load → in-flight and parked requests finish;
  new connections resume once the replacement pod is Ready. (A zero-non-2xx
  rollout target was dropped along with per-Deployment rollout tuning — see
  #473; upgrades are whole-system swaps.)
- Pod delete during a long-running response (a slow actor endpoint) →
  response completes.
- Pod delete while a request is parked (suspend the actor first, saturate the
  pool as in the parking e2e suite) → request completes with 200 after
  resume; `atenet.router.parking.wait.duration{outcome=served}` recorded, not
  `canceled`.
- Observe the sequence in logs and `/readyz` (503 during drain, `/healthz`
  200), and Envoy admin `downstream_cx_active` reaching 0 before Envoy exits.
- Reuse/extend `internal/e2e/suites/parking` with a router-restart injection.

## Files to touch (per the recommendations)

- `cmd/atenet/internal/router/router.go` — `signal.NotifyContext`, readiness
  wiring into `StartMetricsServer`, the drain orchestrator, flag plumbing.
- `cmd/atenet/internal/router/extproc.go`, `xds.go` — accept
  orchestrator-driven stop instead of the inline `<-ctx.Done() →
  GracefulStop` (expose the `*grpc.Server` or a stop func).
- `cmd/atenet/internal/router/envoydrain.go` (new) — admin-API drainer +
  poll loop.
- `cmd/atenet/internal/router/cmd.go` / `config.go` — `--drain-delay`,
  `--drain-timeout` (derived default), validation vs park budget.
- `manifests/ate-install/atenet-router.yaml` — readiness/liveness probes,
  `terminationGracePeriodSeconds` (+ comment), Envoy `preStop` marker-poll
  hook + shared emptyDir. (No explicit rollout strategy: whole-system
  upgrades per #473.)
- Docs: this file's status flip; `atenet-overview.md` limitation note.
