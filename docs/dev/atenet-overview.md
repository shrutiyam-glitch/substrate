# atenet — Component Overview

`atenet` is Substrate's **data-plane front door**: a single Go binary
(`cmd/atenet`) with two subcommands — `atenet dns` (makes every actor
addressable at a stable name) and `atenet router` (an Envoy control plane +
per-request "brain" that resumes suspended actors on demand and steers traffic
to the right worker). It is the component that makes suspend/resume
**invisible to clients** — scale-from-zero per actor with sub-second
activation.

> Glossary: "the networking stack. It provides a DNS server for actor
> resolution and a router that resumes suspended Actors on demand and routes
> traffic to the right worker pod." (`docs/glossary.md`)

## The big picture

The architecture's core need: to resume actors on demand, something must
**trap and inspect traffic** (`docs/architecture.md`, "Agent-Aware Routing").
atenet is that trap. The end-to-end request flow:

```
client ──DNS──▶ CoreDNS (synthetic A) ──▶ atenet-router Service IP
client ──HTTP, Host: my-counter-1.demo.actors.resources.substrate.ate.dev──▶
   Envoy (:8080/:8443) ──ext_proc──▶ atenet router
      │ parse actor from Host ─▶ park if pool full ─▶ ateapi.ResumeActor
      │ (resume workflow: assign worker → atelet Restore → ateom → runsc)
      ▼ sets x-ate-original-dst = <worker-ip>:443
   Envoy ORIGINAL_DST cluster ──mTLS──▶ atunnel (in ateom pod, :443) ──▶ actor :80
```

Two framing facts:

- **Every actor has a uniform address**:
  `<actor>.<atespace>.actors.resources.substrate.ate.dev` (the "Uniform DNS
  Mesh"). Location-transparent — the name never changes as the actor migrates
  between workers or sleeps in object storage.
- **atenet only talks to ateapi's public `Control` API** — the same RPCs
  `kubectl-ate` uses. It never touches Redis, atelet, or ateom directly; per
  request it asks ateapi "resume this actor and tell me where it lives."

## `atenet dns`

It does **not answer DNS itself** — it is a small controller
(`cmd/atenet/internal/dns/`) orchestrating a **CoreDNS sidecar**:

- Every `--interval` (10s) it reads the `atenet-router` Service ClusterIP and
  rewrites the CoreDNS Corefile with a `template IN A` plugin: any name
  matching `<label>.<label>.actors.resources.substrate.ate.dev` synthesizes an
  A record pointing at **the router's single IP**, TTL 60
  (`dns/corefile.go`).
- Reloads CoreDNS by finding its PID via `/proc/*/comm` and sending SIGUSR1
  (`dns/dns.go` — Linux-only, requires `shareProcessNamespace`).
- Patches the `kube-system/kube-dns` ConfigMap with a **stub domain** so
  regular cluster pods resolve actor names (GKE path; skipped on kind).

Key insight: **DNS does zero per-actor work**. Every actor name resolves to
the same router IP; actor→worker resolution happens per-request in the
router. The data source is Kubernetes Services, not ateapi.

## `atenet router`

Runs up to three servers (`internal/router/router.go`):

### a) ext_proc server (:50051) — the per-request brain (`extproc.go`)

Envoy streams every request's headers to it; for each request:

1. Parse the actor `(atespace, name)` from Host/`:authority` (port stripped;
   invalid suffix → 404 without leaking the host).
2. **Parking admission** — enter the bounded parking lot (below).
3. `ResumeActor` via the singleflight resumer.
4. Validate the returned worker IP parses (bad → 500, IP not leaked).
5. Mutate headers: `x-ate-original-dst = <worker-ip>:443` with
   `OVERWRITE_IF_EXISTS_OR_ADD` — **a client can never spoof the dialed
   address** (`extproc_out.go`) — plus the original Host so atunnel can
   authorize by actor DNS name. In agentgateway mode, also rewrites
   `:authority`.
6. Errors map gRPC→HTTP (`errors.go`): NotFound→404,
   FailedPrecondition/Aborted/Unavailable→503, DeadlineExceeded→504,
   PermissionDenied→403, ResourceExhausted→429, everything else→500 generic.

Actors are currently reachable only on port 80 behind atunnel's :443
(`extproc.go`, the tree's one TODO).

### b) xDS/ADS server (:18000) — Envoy's control plane (`xds.go`)

Serves snapshots to the Envoy sidecar:

- `ate-cluster`: static H2 cluster at the router's own ext_proc address, with
  a circuit breaker sized to 2× the parking lot (floor 1024) so parked
  requests can't starve traffic to already-running actors.
- `actor_original_dst`: the trick — an `ORIGINAL_DST` cluster keyed on the
  `x-ate-original-dst` header. **No per-actor clusters or endpoints ever
  exist in Envoy**; routing is fully dynamic via the header. Upstream mTLS
  presents the router's SPIFFE identity and validates atunnel's cert by
  SPIFFE URI SAN prefix (pod certs carry no IP SANs).
- Optional OTLP tracing cluster; HTTP (:8080) and HTTPS (:8443) listeners,
  the serving cert delivered over **SDS with directory watching** so pod-cert
  rotation is picked up live.
- A 5s-tick `Controller` pushes snapshots and (in managed, non-standalone
  mode) reconciles the Envoy Deployment itself. Its ActorTemplate list is
  currently unused for routing (`controller.go`).

### c) `/statusz` (:4040)

Flags, health, parking snapshot, last-100-queries ring buffer.

### Graceful Shutdown (`drain.go`, `envoydrain.go`)

Upon `SIGTERM`:
1. `/readyz` turns 503 while `/healthz` stays 200 for liveness.
2. Waits `--drain-delay` (13s) for K8s Service endpoint removal across the cluster.
3. Coordinates with Envoy via Admin API (`/drain_listeners`) and polls active downstream connections until zero.
4. Gracefully stops `ext_proc` gRPC server so in-flight and parked requests finish normally within `--drain-timeout`.
5. Hard-stops xDS and writes `/var/run/atenet/drain-complete` on a pod-shared `emptyDir` to release Envoy's `preStop` hook.

### Request parking — the standout operation (`docs/request-parking.md`)

When the worker pool is saturated, `AssignWorkerStep` returns
`FailedPrecondition: no free workers available`. Instead of 503ing, the router
**holds the request**:

- Retries the resume with exponential backoff (100ms × 1.1, jitter,
  deliberately **no cap and no attempt limit** — the budget alone bounds the
  wait) until success or the **park budget** (`--parked-request-budget`,
  default 5s) expires → then 503 with the real reason.
- A fixed-capacity **parking lot** (`--parked-request-max`, default 1024)
  sheds overload with "router at capacity."
- **Singleflight dedup**: N concurrent requests for the same actor hold N
  parking slots but make **one** control-plane RPC; the flight runs on a
  detached context so the leader disconnecting doesn't kill joiners. (Known
  quirk: the budget is per-flight, not per-caller — #613.)
- Envoy's ext_proc `MessageTimeout` is set to budget+5s so the router always
  sheds before Envoy times the stream out.

This is Substrate's answer to serverless cold-start queueing.

## The agentgateway alternative

`--atenet-router=agentgateway` swaps the Envoy sidecar for **agentgateway**
(same ext_proc protocol, so the router's brain is unchanged). Differences: no
xDS server (static ConfigMap config), ext_proc additionally rewrites
`:authority` (agentgateway's dynamic backend dials it), different health
endpoint, and it loses SDS cert rotation + dynamic config. Experimental.

## Security posture

- **Ingress**: TLS on :8443 via a `servicedns` pod certificate. The threat
  model wants client authz *before* resuming actors (Critical; largely future
  work).
- **Upstream**: mTLS to atunnel presenting
  `spiffe://cluster.local/ns/ate-system/sa/atenet-router`; atunnel only
  accepts that identity and only forwards to the actor currently assigned —
  the direct worker-port-80 path is closed.
- **To ateapi**: mTLS by default (client cert re-read every handshake for
  rotation) or bearer-token mode (token re-read every RPC).
- **Anti-spoofing**: overwrite-only `x-ate-original-dst` mutation.

## Observability

- **The SLI**: `atenet.router.route.duration` histogram — Envoy receipt to
  forward, labeled by template, outcome (`ok/timeout/no_capacity/…`), and
  resume state (`none/triggered/joined`).
- Parking metrics: `atenet.router.parking.active`, `.wait.duration` (by
  outcome), `.rejected`.
- Tracing: 1% sampling at the router (vs 10% control plane), parent-based so
  clients can force traces. Envoy admin stats scraped for E2E context only.

## Known limitations

- Router + Envoy combined in one single-replica Deployment — "will likely be
  split in the future" (`cmd/atenet/README.md`).
- Actors reachable only on port 80.
- Agentgateway sampling ratio must be manually kept in sync; static config.
- DNS reloader is Linux-only and swallows find-PID errors.
