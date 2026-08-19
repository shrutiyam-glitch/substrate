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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"google.golang.org/protobuf/proto"
)

// Worker change events are delivered by logical decoding: the watch opens a
// replication connection, creates a TEMPORARY replication slot (dropped
// automatically by the server when the connection ends, so a crashed replica
// can never leave a slot behind pinning WAL), and streams pgoutput messages
// for the workers table through the publication below.
//
// Requirements on the server: PostgreSQL 14+ (for pgoutput's binary mode),
// wal_level=logical, a free max_wal_senders / max_replication_slots entry
// per ateapi replica, and the REPLICATION privilege (or equivalent) on the
// connecting role.
//
// The loss contract matches the other backends: on ANY stream error —
// disconnect, slot invalidation, decode failure — the event channel is
// closed and the consumer (workercache) recovers with a full relist. The
// slot's position dies with the connection, so there is deliberately no
// resume-from-LSN path.
const (
	// workerPublication is created idempotently by applySchema.
	workerPublication = "ate_workers_pub"

	// standbyStatusInterval bounds how often the watcher acknowledges WAL to
	// the walsender. Periodic acks (not just reply-on-request) keep
	// wal_sender_timeout at bay and advance the slot's restart_lsn so even a
	// temporary slot does not accumulate WAL across a long-lived stream.
	standbyStatusInterval = 10 * time.Second

	// consumerStallTimeout bounds how long a wedged consumer can park the
	// stream (and therefore pin WAL through this slot) before the watch is
	// closed for a loud resync. The 128-slot buffer absorbs normal jitter;
	// only a consumer stalled this long trips it. Kept below the server's
	// default wal_sender_timeout (60s) so the close is a deliberate contract
	// signal, not a server-side connection kill.
	consumerStallTimeout = 30 * time.Second
)

// WatchWorkers streams changes committed after the subscription; consumers
// obtain the base state themselves (workercache relists after subscribing).
func (p *Persistence) WatchWorkers(ctx context.Context) (*store.WorkerWatch, error) {
	return p.watchWorkers(ctx, false)
}

// WatchWorkersSnapshot additionally delivers the entire workers table as
// synthetic Created events before the live stream, read under the slot's
// exported snapshot — the state is exactly the stream's starting point, so
// there is no race between initial sync and changes (nothing missed, and
// overlap is impossible rather than merely reconciled). Consumers using this
// need no separate relist.
//
// Not wired into store.Interface: the cross-backend watch contract is
// "changes only". This is the exported-snapshot upgrade path, kept behind
// its own method until the contract moves.
func (p *Persistence) WatchWorkersSnapshot(ctx context.Context) (*store.WorkerWatch, error) {
	return p.watchWorkers(ctx, true)
}

func (p *Persistence) watchWorkers(ctx context.Context, seedFromSnapshot bool) (*store.WorkerWatch, error) {
	watchCtx, cancel := context.WithCancel(ctx)
	fail := func(conn *pgconn.PgConn, format string, args ...any) (*store.WorkerWatch, error) {
		if conn != nil {
			conn.Close(context.Background()) //nolint:errcheck
		}
		cancel()
		return nil, fmt.Errorf(format, args...)
	}

	conn, err := p.dialReplication(watchCtx)
	if err != nil {
		return fail(nil, "opening replication connection: %w", err)
	}

	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		return fail(conn, "generating slot name: %w", err)
	}
	slotName := "ateapi_workers_" + hex.EncodeToString(suffix)

	snapshotAction := "NOEXPORT_SNAPSHOT"
	if seedFromSnapshot {
		snapshotAction = "EXPORT_SNAPSHOT"
	}
	// Creating the slot waits for a consistent point: every transaction in
	// flight at this moment must finish first. A long-running transaction
	// elsewhere therefore delays subscription (not steady-state delivery,
	// which pgoutput emits per commit with no such fence).
	slot, err := pglogrepl.CreateReplicationSlot(watchCtx, conn, slotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Temporary: true, SnapshotAction: snapshotAction})
	if err != nil {
		return fail(conn, "creating temporary replication slot (requires wal_level=logical and the REPLICATION privilege): %w", err)
	}
	startLSN, err := pglogrepl.ParseLSN(slot.ConsistentPoint)
	if err != nil {
		return fail(conn, "parsing slot consistent point %q: %w", slot.ConsistentPoint, err)
	}

	// The exported snapshot stays valid only until the next command on the
	// replication connection, so the seed read happens before
	// StartReplication. The slot retains WAL from the consistent point, so
	// nothing between snapshot and stream start is lost.
	var seed []*ateapipb.Worker
	if seedFromSnapshot {
		seed, err = p.workersAtSnapshot(watchCtx, slot.SnapshotName)
		if err != nil {
			return fail(conn, "reading workers under exported snapshot: %w", err)
		}
	}

	if err := pglogrepl.StartReplication(watchCtx, conn, slotName, startLSN, pglogrepl.StartReplicationOptions{
		// binary 'true' (PG14+): bytea tuples arrive as raw bytes instead of
		// \x-hex text, so the proto column feeds proto.Unmarshal directly.
		PluginArgs: []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", workerPublication), "binary 'true'"},
	}); err != nil {
		return fail(conn, "starting replication on slot %s: %w", slotName, err)
	}

	ch := make(chan store.WorkerEvent, 128)
	go func() {
		defer close(ch)
		defer conn.Close(context.Background()) //nolint:errcheck
		for _, w := range seed {
			select {
			case ch <- store.WorkerEvent{Type: store.WorkerEventCreated, Worker: w}:
			case <-watchCtx.Done():
				return
			}
		}
		p.streamWorkerChanges(watchCtx, conn, startLSN, ch)
	}()
	return store.NewWorkerWatch(ch, cancel), nil
}

// dialReplication opens a physical connection in logical replication mode,
// reusing the pool's connection settings (host, credentials, TLS).
func (p *Persistence) dialReplication(ctx context.Context) (*pgconn.PgConn, error) {
	cfg := p.pool.Config().ConnConfig.Copy()
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	cfg.RuntimeParams["replication"] = "database"
	return pgconn.ConnectConfig(ctx, &cfg.Config)
}

// snapshotNamePattern matches PostgreSQL exported snapshot identifiers
// (e.g. "00000003-0000001B-1"). SET TRANSACTION SNAPSHOT cannot take a bind
// parameter, so the name is validated before interpolation.
var snapshotNamePattern = regexp.MustCompile(`^[0-9A-Fa-f-]+$`)

// workersAtSnapshot reads every worker as of the exported snapshot, in one
// REPEATABLE READ transaction on a regular pool connection.
func (p *Persistence) workersAtSnapshot(ctx context.Context, snapshotName string) ([]*ateapipb.Worker, error) {
	if !snapshotNamePattern.MatchString(snapshotName) {
		return nil, fmt.Errorf("unexpected exported snapshot name %q", snapshotName)
	}
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return nil, fmt.Errorf("beginning snapshot transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	if _, err := tx.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotName)); err != nil {
		return nil, fmt.Errorf("importing snapshot %s: %w", snapshotName, err)
	}
	rows, err := tx.Query(ctx, `SELECT proto FROM workers`)
	if err != nil {
		return nil, fmt.Errorf("reading workers snapshot: %w", err)
	}
	defer rows.Close()

	var workers []*ateapipb.Worker
	for rows.Next() {
		var protoBytes []byte
		if err := rows.Scan(&protoBytes); err != nil {
			return nil, fmt.Errorf("scanning worker snapshot row: %w", err)
		}
		w := &ateapipb.Worker{}
		if err := proto.Unmarshal(protoBytes, w); err != nil {
			return nil, fmt.Errorf("unmarshaling worker snapshot row: %w", err)
		}
		workers = append(workers, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading workers snapshot: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing snapshot transaction: %w", err)
	}
	return workers, nil
}

// streamWorkerChanges pumps pgoutput messages into ch until the context is
// cancelled or the stream fails. Returning (which closes ch) is the loss
// signal; it must happen on every non-cancellation error.
func (p *Persistence) streamWorkerChanges(ctx context.Context, conn *pgconn.PgConn, startLSN pglogrepl.LSN, ch chan<- store.WorkerEvent) {
	dec := &pgoutputDecoder{}
	ackedLSN := startLSN
	nextStatus := time.Now().Add(standbyStatusInterval)

	for {
		if !time.Now().Before(nextStatus) {
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: ackedLSN}); err != nil {
				if ctx.Err() == nil {
					slog.WarnContext(ctx, "worker CDC watch: standby status update failed; closing for resync", slog.Any("err", err))
				}
				return
			}
			nextStatus = time.Now().Add(standbyStatusInterval)
		}

		recvCtx, cancelRecv := context.WithDeadline(ctx, nextStatus)
		rawMsg, err := conn.ReceiveMessage(recvCtx)
		cancelRecv()
		if err != nil {
			if pgconn.Timeout(err) && ctx.Err() == nil {
				continue // deadline elapsed so a status update is due
			}
			if ctx.Err() == nil {
				slog.WarnContext(ctx, "worker CDC watch: replication stream error; closing for resync", slog.Any("err", err))
			}
			return
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			if errResp, isErr := rawMsg.(*pgproto3.ErrorResponse); isErr {
				slog.WarnContext(ctx, "worker CDC watch: server error; closing for resync",
					slog.String("code", errResp.Code), slog.String("message", errResp.Message))
				return
			}
			continue // NoticeResponse, ParameterStatus, etc.
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				slog.WarnContext(ctx, "worker CDC watch: bad keepalive; closing for resync", slog.Any("err", err))
				return
			}
			if pkm.ServerWALEnd > ackedLSN {
				ackedLSN = pkm.ServerWALEnd
			}
			if pkm.ReplyRequested {
				nextStatus = time.Now()
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				slog.WarnContext(ctx, "worker CDC watch: bad XLogData; closing for resync", slog.Any("err", err))
				return
			}
			event, ok, err := dec.decode(xld.WALData)
			if err != nil {
				// Unlike the poison-pill case in an outbox (skip one row and
				// continue), a decode failure here may desynchronize the
				// relation cache — resync rather than risk misapplied events.
				slog.ErrorContext(ctx, "worker CDC watch: decode failed; closing for resync", slog.Any("err", err))
				return
			}
			if ok {
				select {
				case ch <- event: // fast path: buffer has room
				case <-ctx.Done():
					return
				default:
					// Buffer full: wait, but not forever — a consumer
					// stalled past the timeout would otherwise pin WAL via
					// this slot indefinitely. Closing converts the stall
					// into an explicit resync (the watch contract).
					select {
					case ch <- event:
					case <-ctx.Done():
						return
					case <-time.After(consumerStallTimeout):
						slog.WarnContext(ctx, "worker CDC watch: consumer stalled; closing for resync to unpin WAL")
						return
					}
				}
			}
			if end := xld.ServerWALEnd; end > ackedLSN {
				ackedLSN = end
			}
		}
	}
}

// workerRelation holds the column indexes the decoder needs, resolved once
// per Relation message. The publication carries a single table with a fixed
// set of interesting columns, so no generic catalog mapping is required —
// but the indexes come from the Relation message rather than being assumed,
// so column reordering via ALTER TABLE cannot misroute values.
type workerRelation struct {
	id                                      uint32
	protoIdx, namespaceIdx, poolIdx, podIdx int
}

// pgoutputDecoder turns pgoutput protocol messages into WorkerEvents.
type pgoutputDecoder struct {
	rel *workerRelation // set by the Relation message preceding row messages
}

// rowRelation resolves a row message's relation: ok=false skips rows of
// other relations (a future publication member must not be misdecoded with
// the workers column mapping), while a row before any Relation message is a
// protocol violation and errors (→ resync).
func (d *pgoutputDecoder) rowRelation(relationID uint32) (ok bool, err error) {
	if d.rel == nil {
		return false, fmt.Errorf("row message before Relation message")
	}
	return d.rel.id == relationID, nil
}

func (d *pgoutputDecoder) decode(walData []byte) (store.WorkerEvent, bool, error) {
	logical, err := pglogrepl.Parse(walData)
	if err != nil {
		return store.WorkerEvent{}, false, fmt.Errorf("parsing pgoutput message: %w", err)
	}

	switch m := logical.(type) {
	case *pglogrepl.RelationMessage:
		// The publication carries only workers today, but if it ever grows,
		// other relations must not overwrite the cached column mapping.
		if m.RelationName != "workers" {
			return store.WorkerEvent{}, false, nil
		}
		rel := &workerRelation{id: m.RelationID, protoIdx: -1, namespaceIdx: -1, poolIdx: -1, podIdx: -1}
		for i, col := range m.Columns {
			switch col.Name {
			case "proto":
				rel.protoIdx = i
			case "worker_namespace":
				rel.namespaceIdx = i
			case "worker_pool":
				rel.poolIdx = i
			case "worker_pod":
				rel.podIdx = i
			}
		}
		if rel.protoIdx < 0 || rel.namespaceIdx < 0 || rel.poolIdx < 0 || rel.podIdx < 0 {
			return store.WorkerEvent{}, false, fmt.Errorf("relation %s is missing expected worker columns", m.RelationName)
		}
		d.rel = rel
		return store.WorkerEvent{}, false, nil

	case *pglogrepl.InsertMessage:
		if ok, err := d.rowRelation(m.RelationID); err != nil || !ok {
			return store.WorkerEvent{}, false, err
		}
		worker, err := d.workerFromTuple(m.Tuple)
		if err != nil {
			return store.WorkerEvent{}, false, err
		}
		return store.WorkerEvent{Type: store.WorkerEventCreated, Worker: worker}, true, nil

	case *pglogrepl.UpdateMessage:
		if ok, err := d.rowRelation(m.RelationID); err != nil || !ok {
			return store.WorkerEvent{}, false, err
		}
		worker, err := d.workerFromTuple(m.NewTuple)
		if err != nil {
			return store.WorkerEvent{}, false, err
		}
		return store.WorkerEvent{Type: store.WorkerEventUpdated, Worker: worker}, true, nil

	case *pglogrepl.DeleteMessage:
		if ok, err := d.rowRelation(m.RelationID); err != nil || !ok {
			return store.WorkerEvent{}, false, err
		}
		// With REPLICA IDENTITY DEFAULT the old tuple is full-width with
		// non-key columns null; only the primary key (namespace, pool, pod)
		// carries values — enough to evict the cache entry, matching the
		// skeleton event DeleteWorker used to publish.
		ns, err := columnValue(m.OldTuple, d.rel.namespaceIdx)
		if err != nil {
			return store.WorkerEvent{}, false, err
		}
		pool, err := columnValue(m.OldTuple, d.rel.poolIdx)
		if err != nil {
			return store.WorkerEvent{}, false, err
		}
		pod, err := columnValue(m.OldTuple, d.rel.podIdx)
		if err != nil {
			return store.WorkerEvent{}, false, err
		}
		return store.WorkerEvent{Type: store.WorkerEventDeleted, Worker: &ateapipb.Worker{
			WorkerNamespace: string(ns),
			WorkerPool:      string(pool),
			WorkerPod:       string(pod),
		}}, true, nil

	default:
		// Begin/Commit/Origin/Type/Truncate — nothing to deliver. Rows are
		// forwarded as they arrive, with no transaction buffering: pgoutput
		// streams only committed transactions (protocol v1, no in-progress
		// streaming requested), so there is never anything to roll back.
		return store.WorkerEvent{}, false, nil
	}
}

// workerFromTuple unmarshals the full worker proto from the tuple's proto
// column.
func (d *pgoutputDecoder) workerFromTuple(tuple *pglogrepl.TupleData) (*ateapipb.Worker, error) {
	protoBytes, err := columnValue(tuple, d.rel.protoIdx)
	if err != nil {
		return nil, err
	}
	worker := &ateapipb.Worker{}
	if err := proto.Unmarshal(protoBytes, worker); err != nil {
		return nil, fmt.Errorf("unmarshaling worker proto from WAL tuple: %w", err)
	}
	return worker, nil
}

// columnValue extracts one column's raw bytes from a tuple. With binary mode
// requested the data arrives raw; the text branch (hex-encoded bytea) is
// kept because pgoutput falls back to text for types without binary send
// functions.
func columnValue(tuple *pglogrepl.TupleData, idx int) ([]byte, error) {
	if tuple == nil || idx >= len(tuple.Columns) {
		return nil, fmt.Errorf("tuple is missing expected column %d", idx)
	}
	col := tuple.Columns[idx]
	switch col.DataType {
	case pglogrepl.TupleDataTypeBinary:
		return col.Data, nil
	case pglogrepl.TupleDataTypeText:
		return decodeTextColumn(col.Data)
	default:
		// Null, or unchanged TOAST. Every worker write rewrites every
		// projected column, so this indicates a write path the decoder does
		// not know about; fail so the stream resyncs.
		return nil, fmt.Errorf("column %d has no value (type %q)", idx, col.DataType)
	}
}

// decodeTextColumn converts a text-format column value to raw bytes,
// hex-decoding PostgreSQL's \x-prefixed bytea output.
func decodeTextColumn(data []byte) ([]byte, error) {
	if len(data) >= 2 && data[0] == '\\' && data[1] == 'x' {
		out := make([]byte, hex.DecodedLen(len(data)-2))
		if _, err := hex.Decode(out, data[2:]); err != nil {
			return nil, fmt.Errorf("hex-decoding bytea: %w", err)
		}
		return out, nil
	}
	return data, nil
}
