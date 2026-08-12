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

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// copyLoad bulk-loads the dataset with pgx CopyFrom, writing rows in
// atepg's exact storage format (server metadata populated, binary proto in
// the bytea column). ~100x faster than the store-API loader — the 100M-actor
// tier needs this. Resumable: each table continues from its max existing
// deterministic name, and each batch is one atomic COPY.
//
// copyBatch bounds per-COPY transaction size (WAL burst) and is the resume
// granularity.
const copyBatch = 100_000

func copyLoad(ctx context.Context, pool *pgxpool.Pool, st store.Interface, ds *dataset) error {
	// Atespaces via the store API: few rows, and it keeps FK targets correct.
	for i := 0; i < ds.atespaces; i++ {
		_, err := st.CreateAtespace(ctx, &ateapipb.Atespace{
			Metadata: &ateapipb.ResourceMetadata{Name: ds.atespaceName(i)},
		})
		if err != nil && !errors.Is(err, store.ErrAlreadyExists) {
			return fmt.Errorf("creating atespace: %w", err)
		}
	}

	if err := copyActorsAndSnapshots(ctx, pool, ds); err != nil {
		return err
	}
	return copyWorkers(ctx, pool, ds)
}

// resumeIndex returns the index after the lexicographically-max existing
// deterministic name, so an interrupted load continues where it stopped.
func resumeIndex(ctx context.Context, pool *pgxpool.Pool, table, prefix string) (int, error) {
	var maxName *string
	err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT max(name) FROM %s WHERE name LIKE $1`, table),
		prefix+"%").Scan(&maxName)
	if err != nil {
		return 0, fmt.Errorf("finding resume point in %s: %w", table, err)
	}
	if maxName == nil {
		return 0, nil
	}
	n, err := strconv.Atoi(strings.TrimPrefix(*maxName, prefix))
	if err != nil {
		return 0, fmt.Errorf("unparseable name %q in %s", *maxName, table)
	}
	return n + 1, nil
}

func serverMetadata(atespace, name string) *ateapipb.ResourceMetadata {
	now := timestamppb.Now()
	return &ateapipb.ResourceMetadata{
		Atespace: atespace, Name: name, Uid: uuid.NewString(),
		Version: 1, CreateTime: now, UpdateTime: now,
	}
}

// pad grows a message's marshaled size to ~target bytes via a filler label,
// so record sizes match the requirements doc's estimates (cache-fit at the
// XL tier depends on honest sizes). setPad installs the filler string.
func pad(msg proto.Message, target int, setPad func(string)) ([]byte, error) {
	b, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	if target <= len(b) {
		return b, nil
	}
	setPad(strings.Repeat("x", target-len(b)))
	return proto.Marshal(msg)
}

func copyActorsAndSnapshots(ctx context.Context, pool *pgxpool.Pool, ds *dataset) error {
	startIdx, err := resumeIndex(ctx, pool, "actors", actorNamePrefix)
	if err != nil {
		return err
	}
	snapStart, err := resumeIndex(ctx, pool, "actor_snapshots", actorNamePrefix)
	if err != nil {
		return err
	}
	// Snapshots are 1:1 with actors and loaded in lockstep; resume from the
	// lower watermark so neither table ends up short.
	if snapStart < startIdx {
		startIdx = snapStart
	}
	lastLog := time.Now()

	for base := startIdx; base < ds.actors; base += copyBatch {
		end := min(base+copyBatch, ds.actors)
		actorRows := make([][]any, 0, end-base)
		snapRows := make([][]any, 0, end-base)
		for i := base; i < end; i++ {
			ref := ds.actorRef(i)

			a := benchActor(ref.Atespace, ref.Name)
			a.Metadata = serverMetadata(ref.Atespace, ref.Name)
			ab, err := pad(a, ds.actorBytes, func(fill string) { a.WorkerSelector.MatchLabels["pad"] = fill })
			if err != nil {
				return fmt.Errorf("marshaling actor: %w", err)
			}
			actorRows = append(actorRows, []any{ref.Atespace, ref.Name, a.GetMetadata().GetUid(), int64(1), ab})

			s := benchSnapshot(ref.Atespace, ref.Name)
			s.Metadata = serverMetadata(ref.Atespace, ref.Name)
			sb, err := pad(s, ds.snapshotBytes, func(fill string) { s.ActorTemplateUid = fill })
			if err != nil {
				return fmt.Errorf("marshaling snapshot: %w", err)
			}
			snapRows = append(snapRows, []any{ref.Atespace, ref.Name, sb})
		}

		// ON CONFLICT is unavailable to COPY; resume-from-max plus atomic
		// batches keeps duplicates impossible unless names were tampered with.
		if _, err := pool.CopyFrom(ctx, pgx.Identifier{"actors"},
			[]string{"atespace", "name", "uid", "version", "proto"},
			pgx.CopyFromRows(actorRows)); err != nil {
			return fmt.Errorf("COPY actors [%d,%d): %w", base, end, err)
		}
		if _, err := pool.CopyFrom(ctx, pgx.Identifier{"actor_snapshots"},
			[]string{"atespace", "name", "proto"},
			pgx.CopyFromRows(snapRows)); err != nil {
			return fmt.Errorf("COPY actor_snapshots [%d,%d): %w", base, end, err)
		}
		if time.Since(lastLog) > 15*time.Second {
			slog.Info("copy load progress", "actors", end, "of", ds.actors)
			lastLog = time.Now()
		}
	}
	return nil
}

func copyWorkers(ctx context.Context, pool *pgxpool.Pool, ds *dataset) error {
	// The workers table's deterministic name lives in worker_pod.
	startIdx := 0
	var maxPod *string
	if err := pool.QueryRow(ctx, `SELECT max(worker_pod) FROM workers WHERE worker_pod LIKE $1`, workerPodPrefix+"%").Scan(&maxPod); err != nil {
		return fmt.Errorf("finding worker resume point: %w", err)
	}
	if maxPod != nil {
		n, err := strconv.Atoi(strings.TrimPrefix(*maxPod, workerPodPrefix))
		if err != nil {
			return fmt.Errorf("unparseable worker pod %q", *maxPod)
		}
		startIdx = n + 1
	}

	for base := startIdx; base < ds.workers; base += copyBatch {
		end := min(base+copyBatch, ds.workers)
		rows := make([][]any, 0, end-base)
		for i := base; i < end; i++ {
			w := benchWorker(workerNamespace, ds.poolName(i), ds.workerPod(i), i)
			w.Version = 1
			wb, err := pad(w, ds.workerBytes, func(fill string) { w.Labels["pad"] = fill })
			if err != nil {
				return fmt.Errorf("marshaling worker: %w", err)
			}
			rows = append(rows, []any{w.GetWorkerNamespace(), w.GetWorkerPool(), w.GetWorkerPod(), int64(1), wb})
		}
		if _, err := pool.CopyFrom(ctx, pgx.Identifier{"workers"},
			[]string{"worker_namespace", "worker_pool", "worker_pod", "version", "proto"},
			pgx.CopyFromRows(rows)); err != nil {
			return fmt.Errorf("COPY workers [%d,%d): %w", base, end, err)
		}
	}
	return nil
}
