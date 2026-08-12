# ateapi: PostgreSQL Persistence Backend

This document summarizes the changes introduced to support PostgreSQL as an alternative persistence backend for `ateapi` (originally proposed in #640).

## 1. How Storage is Changed to PostgreSQL

The system introduces PostgreSQL as an **alternative persistence backend** alongside the existing Redis/Valkey implementation. It is designed to be opt-in.

### A. New Storage Implementation (`atepg` package)
* A new package `atepg` is located under `cmd/ateapi/internal/store/atepg/`. This package implements the `store.Interface` required by the `ateapi` service.
* **Data Model:** Instead of fully normalizing every field into relational columns, it uses a **hybrid approach**:
    * It stores standard SQL columns for fields that PostgreSQL must operate on (like primary keys, versions, pagination tokens).
    * It stores the **complete protocol buffer message** (binary-encoded) in a `BYTEA` column. This allows flexibility for the protobuf schema while retaining the ability to query by metadata.
* **Driver:** It uses the modern `pgx` driver (`github.com/jackc/pgx/v5`) for efficient communication with PostgreSQL.

### B. Coordination Features
* **Pub/Sub Replacement:** Redis `SUBSCRIBE`/`PUBLISH` for worker changes is replaced in this backend by PostgreSQL's native **`LISTEN` / `NOTIFY`** feature (`pg_notify`). This allows different `ateapi` instances to communicate worker pool updates in real-time.
* **Distributed Locking:** Workflow locking (previously done via Redis locks) is implemented using a **`leases` table** with conditional `INSERT ... ON CONFLICT DO UPDATE` queries to guarantee atomic lease acquisition and renewal.

### C. Integration in API Server
* In `cmd/ateapi/main.go`, new flags are introduced:
    * `--store-backend`: Defaults to `redis` but can be set to `postgres`.
    * `--postgres-connection-string`: Takes the DSN or URI for connecting to PostgreSQL.
* The API server switches between `ateredis` and `atepg` initialization based on these flags.

---

## 2. Which PostgreSQL is used?

### A. PostgreSQL Image
The implementation uses the official **`postgres:18-alpine`** Docker image.
* This is pinned in test files (via `testcontainers`) and in deployment manifests.

### B. Deployment Model
* **Manifest:** A new manifest file `manifests/ate-install/postgres.yaml` is provided.
* **Type:** It is deployed as a single-replica **`StatefulSet`** inside the `ate-system` namespace.
* **Configuration:** It runs with a custom configuration mounted via a `ConfigMap` (`postgres-config`).
* **Security/TLS:** It is configured to run with SSL enabled (`ssl = on`), leveraging the same internal pod-identity and certificate management system as other components in Substrate to verify client certificates (mTLS).

*Note: While a StatefulSet is provided for easy installation/testing within the cluster, the system is designed to connect to any PostgreSQL instance (including managed services like Cloud SQL) via the `--postgres-connection-string` flag.*

---

## 3. Architecture & Operations Summary

### A. Database Schema Overview

| Table | Primary Key | Key Columns | Purpose |
|---|---|---|---|
| **`atespaces`** | `name` | `name`, `proto` (BYTEA) | Tenancy/namespace isolation |
| **`actors`** | `(atespace, name)` | `version`, `status`, `proto` (BYTEA) | Actor metadata, status, worker assignments |
| **`workers`** | `(worker_namespace, worker_pool, worker_pod)` | `ip`, `version`, `proto` (BYTEA) | Worker pod registrations and assigned actors |
| **`actor_snapshots`** | `(atespace, name)` | `location` (GCS URL), `proto` (BYTEA) | Durable actor memory snapshot metadata |
| **`actor_snapshot_tags`** | `(atespace, name)` | `snapshot_atespace`, `snapshot_name`, `version`, `proto` (BYTEA) | Tag aliases pointing to durable snapshots |
| **`leases`** | `key` | `token` (UUID), `expires_at` (TIMESTAMPTZ) | Distributed lock manager for workflows |

---

### B. Lifecycle Operations & SQL Mapping

| Operation | Calling Step (Control Plane) | Go Function (`atepg.go`) | Primary SQL Executed |
|---|---|---|---|
| **Acquire Lock** | `acquireActorLock()` in `workflow.go` | `p.acquireLease()` | `INSERT INTO leases (key, token, expires_at) VALUES ($1, $2, clock_timestamp() + $3) ON CONFLICT (key) DO UPDATE ... WHERE leases.expires_at <= clock_timestamp()` |
| **Release Lock** | `defer lock.Close()` in `workflow.go` | `p.releaseLease()` | `DELETE FROM leases WHERE key = $1 AND token = $2` |
| **Create Actor** | `Service.CreateActor()` in `create_actor.go` | `p.CreateActor()` | `INSERT INTO actors (atespace, name, version, status, proto) VALUES ($1, $2, $3, $4, $5)` |
| **Read Actor** | `LoadActorFor*Step` in `workflow_*.go` | `p.GetActor()` | `SELECT proto FROM actors WHERE atespace = $1 AND name = $2` |
| **Update Actor / State Change** | `Mark*Step`, `Finalize*Step`, `crashActor()` | `p.UpdateActor()` | `UPDATE actors SET version = $1, status = $2, proto = $3 WHERE atespace = $4 AND name = $5 AND version = $6 RETURNING proto` |
| **Assign / Release Worker** | `AssignWorkerStep`, `releaseWorker()` | `p.UpdateWorker()` | `UPDATE workers SET version = $1, proto = $2 WHERE worker_namespace = $3 AND worker_pool = $4 AND worker_pod = $5 AND version = $6 AND ip = $7 RETURNING proto` |
| **Record Snapshot** | `SuspendActor()` checkpoint commit | `p.CreateActorSnapshot()` | `INSERT INTO actor_snapshots (atespace, name, location, proto) VALUES ($1, $2, $3, $4)` |
| **Delete Actor** | `FinalizeDeletedStep` in `workflow_delete.go` | `p.DeleteActor()` | `DELETE FROM actors WHERE atespace = $1 AND name = $2 AND status = $3 RETURNING proto` |

---

### C. Concurrency Control Mechanisms

| Mechanism | Table / Column | Purpose | When Used |
|---|---|---|---|
| **Distributed Lock** | `leases` (`key`, `token`, `expires_at`) | Serializes multi-step asynchronous workflows and prevents split-brain. Uses auto-expiring TTLs for crash resilience. | During `Resume`, `Suspend`, `Pause`, and `Delete` workflows. |
| **Optimistic Locking (OCC)** | `actors.version`, `workers.version` | Detects concurrent row modifications and prevents stale overwrites via `WHERE version = $expected`. | During every single-row `UPDATE` on actors and workers. |
| **Unique / Foreign Constraints** | `PRIMARY KEY`, `FOREIGN KEY ... ON DELETE RESTRICT` | Enforces relational integrity and prevents duplicate records or orphaned actors/snapshots. | Table-level database constraints. |

