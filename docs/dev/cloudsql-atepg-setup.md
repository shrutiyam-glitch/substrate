# Cloud SQL for the atepg store — setup (Option 1: private IP, direct)

How to point ateapi (branch `pr-640`, `--store-backend=postgres`) at a Cloud SQL
PostgreSQL instance over private IP. No code changes — `atepg.Connect` is just
`pgxpool.New(dsn)`, so everything is driven by the connection string in
`ATE_API_POSTGRES_CONNECTION_STRING`.

## Environment (looked up 2026-08-09)

| Thing | Value | How to find it |
|---|---|---|
| Project | `shrutiyam-gke-dev` | `gcloud config get-value project` |
| Cluster | `agent-substrate`, `us-central1` | `gcloud container clusters list` |
| Cluster VPC / subnet | `default` / `default` | `gcloud container clusters list --format="table(name,location,network,subnetwork)"` |

The Cloud SQL instance must be on the **same VPC** (`default`) and ideally the
**same region** (`us-central1`) as the cluster.

## 1. One-time project setup (private services access)

Not yet done on this project (Service Networking API was disabled as of
2026-08-09). Cloud SQL private IP works via a VPC peering to Google's service
network; set it up once per VPC:

```bash
gcloud services enable servicenetworking.googleapis.com sqladmin.googleapis.com

# Allocate an IP range on the default VPC for Google-managed services
gcloud compute addresses create google-managed-services-default \
  --global --purpose=VPC_PEERING --prefix-length=16 --network=default

# Create the peering
gcloud services vpc-peerings connect \
  --service=servicenetworking.googleapis.com \
  --ranges=google-managed-services-default \
  --network=default
```

## 2. Create the instance and database

```bash
gcloud sql instances create atepg-bench \
  --database-version=POSTGRES_18 \
  --edition=enterprise \
  --tier=db-custom-2-8192 \
  --region=us-central1 \
  --network=default \
  --no-assign-ip
# --edition=enterprise is required for db-custom-* tiers; without it Cloud SQL
# defaults to Enterprise Plus, which only accepts db-perf-optimized-N-* tiers.
# If POSTGRES_18 is rejected in enterprise edition, either use
# --tier=db-perf-optimized-N-2 (Enterprise Plus: 2 vCPU/16 GB, includes a data
# cache — note the shape in results) or POSTGRES_17 with the flags above.

gcloud sql databases create atepg --instance=atepg-bench
gcloud sql users set-password postgres --instance=atepg-bench --password='<PW>'

# Grab the private IP for the DSN (10.25.0.3 for atepg-bench).
# Works because a --no-assign-ip instance has exactly one address; `describe`
# does not support --filter.
gcloud sql instances describe atepg-bench --format="value(ipAddresses[0].ipAddress)"
```

Notes:
- `db-custom-2-8192` (2 vCPU / 8 GB) roughly matches the community benchmark's
  final Postgres shape; change deliberately when testing config sensitivity.
- This creates a **zonal** instance. Add `--availability-type=REGIONAL` only
  when explicitly benchmarking HA (synchronous replication adds write latency).
- No schema setup needed: ateapi applies the embedded schema idempotently at
  startup (`atepg.applySchema`, guarded by an advisory lock).

## 3. Point ateapi at it

There is no explicit `kubectl` command for the DSN — the install script does
it: `create_api_server_env_vars()` (hack/install-ate.sh:352) reads
`ATE_API_POSTGRES_CONNECTION_STRING` from your shell environment and writes it
into the `ate-api-server-envvars` ConfigMap via `--from-literal` (:386),
overriding the in-cluster default DSN. So the `export` below is what feeds
it, and either script invocation below is what persists it:

```bash
export ATE_API_POSTGRES_CONNECTION_STRING="postgresql://postgres:<PW>@<PRIVATE_IP>:5432/atepg?sslmode=require"
./hack/install-ate.sh --deploy-ate-system --store-backend=postgres

# If the system was already deployed, reconcile + restart instead:
./hack/install-ate.sh --create-api-server-env-vars --store-backend=postgres
kubectl rollout restart deployment/ate-api-server -n ate-system
```

Switching back to Valkey later: rerun with `--store-backend=redis` and restart
the deployment. State does not migrate between backends (workers self-heal via
the pool syncer; actors/atespaces must be re-created — fine for benchmarks,
which preload/reset anyway).

## 4. Verify

```bash
# From inside the cluster (the instance has no public IP):
kubectl run -it --rm psql --image=postgres:18-alpine --restart=Never -- \
  psql "postgresql://postgres:<PW>@<PRIVATE_IP>:5432/atepg?sslmode=require" -c '\dt'
# Expect the six atepg tables after ateapi has started once.

kubectl logs deployment/ate-api-server -n ate-system | head -30
# Expect store-backend=postgres in the flag dump and no connection errors.

kubectl ate atespace create smoke-test   # or any trivial CRUD via kubectl-ate
```

## Caveats (fine for benchmarking, not production hygiene)

- The DSN — password included — lands in the `ate-api-server-envvars`
  **ConfigMap**, not a Secret (written by `create_api_server_env_vars` in
  hack/install-ate.sh as described in step 3; verify with
  `kubectl get configmap ate-api-server-envvars -n ate-system -o yaml`).
  Production would want this in a Secret or sourced via IAM auth instead.
- `sslmode=require` encrypts but does not verify the server certificate. For
  verification, download the server CA (`gcloud sql ssl server-ca-certs list
  --instance=atepg-bench`), mount it into the pod, and use
  `sslmode=verify-ca&sslrootcert=<path>` — requires a manifest change.
- IAM database auth would need a code change (the `cloud.google.com/go/cloudsqlconn`
  dialer wired into `atepg.Connect`) — flagged as an open question for the
  production design, out of scope for benchmarking.

## Cleanup

```bash
gcloud sql instances delete atepg-bench
```
