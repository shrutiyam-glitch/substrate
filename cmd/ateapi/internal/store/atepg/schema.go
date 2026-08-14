// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package atepg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// schema is atepg's idempotent embedded schema.
//
// Resource fields are projected into columns only when PostgreSQL needs them
// for identity, relationships, queries, ordering, or atomic concurrency
// checks. All other resource state remains authoritative in the opaque proto.
const schema = `
CREATE TABLE IF NOT EXISTS atespaces (
    name   text PRIMARY KEY,
    proto  bytea NOT NULL
);

CREATE TABLE IF NOT EXISTS actors (
    atespace  text NOT NULL
        REFERENCES atespaces(name) ON DELETE RESTRICT,
    name      text NOT NULL,
    uid       text NOT NULL UNIQUE,
    version   bigint NOT NULL,
    proto     bytea NOT NULL,
    PRIMARY KEY (atespace, name)
);

CREATE TABLE IF NOT EXISTS actor_snapshots (
    atespace  text NOT NULL,
    name      text NOT NULL,
    proto     bytea NOT NULL,
    PRIMARY KEY (atespace, name)
);

CREATE TABLE IF NOT EXISTS actor_snapshot_tags (
    atespace           text NOT NULL
        REFERENCES atespaces(name) ON DELETE RESTRICT,
    name               text NOT NULL,
    snapshot_atespace  text NOT NULL,
    snapshot_name      text NOT NULL,
    version            bigint NOT NULL,
    proto              bytea NOT NULL,
    PRIMARY KEY (atespace, name),
    FOREIGN KEY (snapshot_atespace, snapshot_name)
        REFERENCES actor_snapshots(atespace, name) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS workers (
    worker_namespace  text NOT NULL,
    worker_pool       text NOT NULL,
    worker_pod        text NOT NULL,
    version           bigint NOT NULL,
    proto             bytea NOT NULL,
    PRIMARY KEY (worker_namespace, worker_pool, worker_pod)
);

-- Transactional change feed backing WatchWorkers. Events are appended in
-- the same transaction as the worker write and delivered by polling past a
-- (xid, seq) cursor;
-- payload is a JSON envelope: {"t": <event type>, "w": <protojson Worker>}.
--
-- Partitioned hourly by created_at so retention is a partition DROP — a
-- metadata operation with no row deletes, dead tuples, or vacuum debt —
-- instead of bulk DELETEs whose I/O competes with foreground traffic. The
-- janitor (changeFeedJanitor) creates upcoming partitions and drops expired
-- ones; the DEFAULT partition only receives writes if partition creation
-- ever stalls, and is trimmed row-wise as a fallback.
--
-- seq has no PRIMARY KEY: a unique constraint on a partitioned table must
-- include the partition key, and uniqueness already holds by construction
-- (one identity sequence). Requires PostgreSQL 17+ (identity column on a
-- partitioned table).
--
-- Partitions are UNLOGGED: the feed is ephemeral by design (cursors are
-- not durable, subscriptions start "from now", and every consumer rebuilds
-- from the workers table on resync), so paying WAL on every event — inside
-- every worker-write transaction — buys nothing. Crash/failover truncates
-- unlogged tables; see WatchWorkers for how watchers recover.
-- worker_changes_trim stays logged — the trim mark must survive a crash.
CREATE TABLE IF NOT EXISTS worker_changes (
    seq         bigint GENERATED ALWAYS AS IDENTITY,
    xid         xid8 NOT NULL DEFAULT pg_current_xact_id(),
    created_at  timestamptz NOT NULL DEFAULT now(),
    payload     bytea NOT NULL
) PARTITION BY RANGE (created_at);

CREATE INDEX IF NOT EXISTS worker_changes_xid_seq ON worker_changes (xid, seq);

CREATE UNLOGGED TABLE IF NOT EXISTS worker_changes_pdefault PARTITION OF worker_changes DEFAULT;

-- Single-row high-water mark of retention: the greatest seq ever discarded
-- from worker_changes (dropped with an hourly partition, or row-trimmed
-- from the DEFAULT partition). Watchers compare it against their cursor to
-- detect (exactly, without inferring from identity gaps) that unconsumed
-- rows were discarded out from under them.
CREATE TABLE IF NOT EXISTS worker_changes_trim (
    id   boolean PRIMARY KEY DEFAULT true CHECK (id),
    seq  bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS leases (
    key         text PRIMARY KEY,
    token       text NOT NULL,
    expires_at  timestamptz NOT NULL
);
`

// applySchema idempotently creates atepg's tables.
func applySchema(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning atepg schema transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	// Multiple ateapi replicas can start against an empty database together.
	// PostgreSQL's IF NOT EXISTS does not eliminate every concurrent-DDL race,
	// so serialize schema application with a transaction-scoped advisory lock.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('agent-substrate-atepg-schema'))`); err != nil {
		return fmt.Errorf("locking atepg schema: %w", err)
	}
	if _, err := tx.Exec(ctx, schema); err != nil {
		return fmt.Errorf("applying atepg schema: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing atepg schema: %w", err)
	}
	return nil
}
