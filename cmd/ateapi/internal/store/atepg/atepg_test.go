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
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// One Postgres container serves every test in this package; each test gets
// isolation via DebugClearAll rather than a fresh container, which would be
// far slower. Tests in this package are not safe to run with -parallel.
var (
	containerOnce sync.Once
	containerPool *pgxpool.Pool
	containerPG   *postgres.PostgresContainer
	containerErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if containerPool != nil {
		containerPool.Close()
	}
	if containerPG != nil {
		if err := containerPG.Terminate(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "terminating PostgreSQL testcontainer: %v\n", err)
			if code == 0 {
				code = 1
			}
		}
	}
	os.Exit(code)
}

func requirePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	containerOnce.Do(func() {
		ctx := context.Background()
		pgContainer, err := postgres.Run(ctx, "postgres:18-alpine",
			postgres.WithDatabase("atepg"),
			postgres.WithUsername("atepg"),
			postgres.WithPassword("atepg"),
		)
		if err != nil {
			containerErr = err
			return
		}
		containerPG = pgContainer
		dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			containerErr = err
			return
		}
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			containerErr = err
			return
		}
		// The official postgres image restarts its server process once after
		// initdb; the port accepts (and briefly resets) connections during
		// that window, so ping with retries rather than failing on the first
		// attempt.
		var pingErr error
		for i := 0; i < 30; i++ {
			pingErr = pool.Ping(ctx)
			if pingErr == nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if pingErr != nil {
			containerErr = fmt.Errorf("pinging PostgreSQL testcontainer after retries: %w", pingErr)
			return
		}
		containerPool = pool
	})
	if containerErr != nil {
		t.Skipf("PostgreSQL testcontainer unavailable (requires Docker): %v", containerErr)
	}
	return containerPool
}

func setupPostgresPersistence(t *testing.T) *Persistence {
	t.Helper()
	ctx := context.Background()
	p, err := NewPersistence(ctx, requirePool(t))
	if err != nil {
		t.Fatalf("NewPersistence failed: %v", err)
	}
	t.Cleanup(p.Close)
	if err := p.DebugClearAll(ctx); err != nil {
		t.Fatalf("DebugClearAll failed: %v", err)
	}
	return p
}

func setupPostgresStore(t *testing.T) store.Interface {
	t.Helper()
	return setupPostgresPersistence(t)
}

func newTestAtespace(name string) *ateapipb.Atespace {
	return &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: name}}
}

// TestCreateActor_MissingAtespace_FailedPrecondition exercises the
// foreign-key race the doc calls out: CreateActor rejects an actor whose
// atespace doesn't exist (including a concurrently-deleted one), closing the
// TOCTOU window ateredis's separate existence check leaves open.
func TestCreateActor_MissingAtespace_FailedPrecondition(t *testing.T) {
	s := setupPostgresStore(t).(*Persistence)
	ctx := context.Background()

	actor := &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Name: "id1", Atespace: "no-such-atespace"},
		ActorTemplateNamespace: "ns1",
		ActorTemplateName:      "tmpl1",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
	}
	if _, err := s.CreateActor(ctx, actor); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Errorf("CreateActor with missing atespace = %v, want ErrFailedPrecondition", err)
	}
}

// TestWorkerNotification_OnlyAfterCommit proves the doc's atomicity claim: a
// worker write's pg_notify shares the write's transaction, so a rolled-back
// write never notifies, while a committed write always does.
func TestWorkerNotification_OnlyAfterCommit(t *testing.T) {
	s := setupPostgresStore(t).(*Persistence)
	ctx := context.Background()

	watch, err := s.WatchWorkers(ctx)
	if err != nil {
		t.Fatalf("WatchWorkers failed: %v", err)
	}
	defer watch.Close()

	worker := &ateapipb.Worker{WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: "pod"}
	protoBytes, err := proto.Marshal(worker)
	if err != nil {
		t.Fatalf("marshaling worker: %v", err)
	}

	// Write the row and roll back instead of committing: no notification
	// should ever arrive, proving pg_notify's effect is undone with the rest
	// of the transaction.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO workers (worker_namespace, worker_pool, worker_pod, version, proto)
		VALUES ($1, $2, $3, $4, $5)`,
		worker.GetWorkerNamespace(), worker.GetWorkerPool(), worker.GetWorkerPod(), int64(1), protoBytes); err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO worker_changes (payload) VALUES ($1)`, []byte("rolled-back-payload")); err != nil {
		t.Fatalf("feed insert failed: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	select {
	case event := <-watch.Events:
		t.Fatalf("received event %+v from a rolled-back transaction; NOTIFY should not survive rollback", event)
	case <-time.After(500 * time.Millisecond):
		// Expected: nothing arrives.
	}

	// The equivalent committed write must notify.
	if err := s.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	select {
	case event := <-watch.Events:
		if event.Type != store.WorkerEventCreated {
			t.Errorf("expected WorkerEventCreated, got %v", event.Type)
		}
		worker.Version = 1 // CreateWorker assigns version 1 server-side.
		if diff := cmp.Diff(worker, event.Worker, protocmp.Transform()); diff != "" {
			t.Errorf("event worker mismatch (-want +got):\n%s", diff)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event from a committed write")
	}
}

// TestWatchWorkers_OutOfOrderCommitNotSkipped reproduces the seq-cursor gap:
// feed seqs are allocated at INSERT but rows appear at COMMIT, so a
// transaction holding a lower seq can commit after a higher-seq sibling. A
// watcher whose cursor chased raw seq would advance past the in-flight row
// and lose its event permanently. The (xid, seq) cursor must instead hold
// the committed sibling back until the older transaction resolves, then
// deliver both in order.
func TestWatchWorkers_OutOfOrderCommitNotSkipped(t *testing.T) {
	s := setupPostgresStore(t).(*Persistence)
	ctx := context.Background()

	watch, err := s.WatchWorkers(ctx)
	if err != nil {
		t.Fatalf("WatchWorkers failed: %v", err)
	}
	defer watch.Close()

	mkPayload := func(pod string) []byte {
		payload, err := marshalWorkerEvent(store.WorkerEventCreated,
			&ateapipb.Worker{WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: pod})
		if err != nil {
			t.Fatalf("marshaling event for %q: %v", pod, err)
		}
		return payload
	}

	// tx1 appends first (lower seq, lower xid) and stays open.
	tx1, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx1 failed: %v", err)
	}
	defer tx1.Rollback(ctx) //nolint:errcheck // no-op once committed
	if _, err := tx1.Exec(ctx, `INSERT INTO worker_changes (payload) VALUES ($1)`, mkPayload("first-seq-late-commit")); err != nil {
		t.Fatalf("tx1 feed insert failed: %v", err)
	}

	// tx2 appends second (higher seq) and commits immediately.
	tx2, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx2 failed: %v", err)
	}
	if _, err := tx2.Exec(ctx, `INSERT INTO worker_changes (payload) VALUES ($1)`, mkPayload("second-seq-early-commit")); err != nil {
		t.Fatalf("tx2 feed insert failed: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("tx2 Commit failed: %v", err)
	}

	// While tx1 is in flight, tx2's committed event must be held back by
	// the xmin fence — otherwise the cursor has already skipped tx1's row.
	select {
	case event := <-watch.Events:
		t.Fatalf("event %q delivered while an older feed transaction was still in flight; its sibling event is now unreachable", event.Worker.GetWorkerPod())
	case <-time.After(500 * time.Millisecond):
		// Expected: fence holds both events back.
	}

	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("tx1 Commit failed: %v", err)
	}

	var got []string
	for len(got) < 2 {
		select {
		case event := <-watch.Events:
			got = append(got, event.Worker.GetWorkerPod())
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for both events; delivered so far: %v", got)
		}
	}
	want := []string{"first-seq-late-commit", "second-seq-early-commit"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("event delivery order mismatch (-want +got):\n%s", diff)
	}
}

// TestTrimWorkerChanges_DeletesByAgeOnly verifies the janitor's age-based
// retention: rows older than changeFeedRetentionAge are deleted, fresh rows
// survive, independent of any watcher cursor.
func TestTrimWorkerChanges_DeletesByAgeOnly(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	if _, err := s.pool.Exec(ctx, `
		INSERT INTO worker_changes (payload, created_at) VALUES
			($1, now() - interval '1 hour'),
			($1, now() - interval '20 minutes'),
			($1, now())`, []byte("payload")); err != nil {
		t.Fatalf("seeding feed rows failed: %v", err)
	}

	n, err := s.trimWorkerChanges(ctx)
	if err != nil {
		t.Fatalf("trimWorkerChanges failed: %v", err)
	}
	if n != 2 {
		t.Errorf("trimWorkerChanges deleted %d rows, want 2 (the two older than retention)", n)
	}
	var remaining int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM worker_changes`).Scan(&remaining); err != nil {
		t.Fatalf("counting remaining rows failed: %v", err)
	}
	if remaining != 1 {
		t.Errorf("%d rows remain, want 1 (the fresh row)", remaining)
	}
}

// TestWatchWorkers_ClosesWhenTrimmedPastCursor verifies the retention
// escape hatch: when rows a watcher has not consumed are deleted out from
// under it (a janitor trim on a badly lagging watcher), the watcher must
// close its channel — the signal consumers treat as resync-and-relist —
// rather than silently skip the gap.
func TestWatchWorkers_ClosesWhenTrimmedPastCursor(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()

	watch, err := s.WatchWorkers(ctx)
	if err != nil {
		t.Fatalf("WatchWorkers failed: %v", err)
	}
	defer watch.Close()

	// Deliver one event normally so the cursor is established.
	worker := &ateapipb.Worker{WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: "pod"}
	if err := s.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	select {
	case <-watch.Events:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the first event")
	}

	// Atomically append three events and trim all but the newest — the
	// watcher never gets a chance to consume the first two, exactly as if
	// the janitor trimmed rows a lagging watcher had not reached.
	payload, err := marshalWorkerEvent(store.WorkerEventUpdated, worker)
	if err != nil {
		t.Fatalf("marshaling event: %v", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed
	if _, err := tx.Exec(ctx, `INSERT INTO worker_changes (payload) VALUES ($1), ($1), ($1)`, payload); err != nil {
		t.Fatalf("feed inserts failed: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		WITH doomed AS (
			DELETE FROM worker_changes WHERE seq < (SELECT max(seq) FROM worker_changes)
			RETURNING seq
		)
		INSERT INTO worker_changes_trim (seq)
		SELECT max(seq) FROM doomed HAVING count(*) > 0
		ON CONFLICT (id) DO UPDATE SET seq = GREATEST(worker_changes_trim.seq, EXCLUDED.seq)`); err != nil {
		t.Fatalf("trim failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// The watcher must close the channel, not deliver past the gap.
	select {
	case event, ok := <-watch.Events:
		if ok {
			t.Fatalf("received event %+v past a trimmed gap; expected the channel to close for resync", event)
		}
		// Expected: channel closed.
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the watch channel to close after a trim past the cursor")
	}
}

func TestListActors_InvalidPageToken(t *testing.T) {
	s := setupPostgresStore(t).(*Persistence)
	ctx := context.Background()

	if _, _, err := s.ListActors(ctx, "", 10, "not-valid-base64!!"); err == nil {
		t.Errorf("ListActors with malformed page token = nil error, want an error")
	}
}

func TestDecodePageTokenRejectsWrongKeyShape(t *testing.T) {
	token := encodePageToken(kindActor, "", []string{"only-an-atespace"})
	if _, err := decodePageToken(token, kindActor, "", 2); err == nil {
		t.Fatal("decodePageToken() accepted a global actor token with only one key part")
	}
}

func TestListActors_CrossScopePageToken(t *testing.T) {
	s := setupPostgresStore(t).(*Persistence)
	ctx := context.Background()

	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace(team-a) failed: %v", err)
	}
	if _, err := s.CreateAtespace(ctx, newTestAtespace("team-b")); err != nil {
		t.Fatalf("CreateAtespace(team-b) failed: %v", err)
	}
	for _, name := range []string{"a1", "a2"} {
		if _, err := s.CreateActor(ctx, &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: name, Atespace: "team-a"}, Status: ateapipb.Actor_STATUS_SUSPENDED}); err != nil {
			t.Fatalf("CreateActor failed: %v", err)
		}
	}

	_, nextToken, err := s.ListActors(ctx, "team-a", 1, "")
	if err != nil {
		t.Fatalf("ListActors(team-a) failed: %v", err)
	}
	if nextToken == "" {
		t.Fatalf("expected a next page token")
	}

	// A token minted for team-a must be rejected when replayed against team-b
	// or against the unscoped (global) listing.
	if _, _, err := s.ListActors(ctx, "team-b", 1, nextToken); err == nil {
		t.Errorf("ListActors(team-b) with team-a's token = nil error, want an error")
	}
	if _, _, err := s.ListActors(ctx, "", 1, nextToken); err == nil {
		t.Errorf("ListActors(all) with team-a's token = nil error, want an error")
	}

	// A worker-list token must be rejected by ListAtespaces (different kind).
	_, workerToken, err := s.ListWorkers(ctx, 1, "")
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}
	if workerToken != "" {
		if _, _, err := s.ListAtespaces(ctx, 1, workerToken); err == nil {
			t.Errorf("ListAtespaces with a worker page token = nil error, want an error")
		}
	}
}

func TestAcquireLock_ExpiresAfterHolderStops(t *testing.T) {
	s := setupPostgresPersistence(t)
	s.lockTTL = 200 * time.Millisecond
	holderCtx, cancelHolder := context.WithCancel(context.Background())
	lock, err := s.AcquireLock(holderCtx, "test-lock")
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}
	cancelHolder()
	select {
	case <-lock.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("lock context was not cancelled with its holder")
	}

	// Canceling the holder stops renewal without calling Close, modeling a
	// process that disappeared and left its lease to expire.
	time.Sleep(s.lockTTL + 500*time.Millisecond)

	newLock, err := s.AcquireLock(context.Background(), "test-lock")
	if err != nil {
		t.Fatalf("AcquireLock after lease expiration failed: %v", err)
	}
	newLock.Close()
}

// TestAcquireLock_ConcurrentTakeover races many goroutines to acquire an
// already-expired lease against the real database, and asserts exactly one
// wins -- the property the doc's conditional-upsert SQL is meant to
// guarantee under real concurrency, which a single-connection unit test
// can't exercise.
func TestAcquireLock_ConcurrentTakeover(t *testing.T) {
	s := setupPostgresPersistence(t)
	s.lockTTL = time.Millisecond
	holderCtx, cancelHolder := context.WithCancel(context.Background())
	initial, err := s.AcquireLock(holderCtx, "contested-lock")
	if err != nil {
		t.Fatalf("seeding initial lease failed: %v", err)
	}
	cancelHolder()
	<-initial.Context().Done()
	time.Sleep(50 * time.Millisecond) // let the 1ms lease expire.
	s.lockTTL = 10 * time.Second

	const numRacers = 20
	winners := make(chan *store.Lock, numRacers)
	var wg sync.WaitGroup
	for i := 0; i < numRacers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lock, err := s.AcquireLock(context.Background(), "contested-lock")
			if err != nil {
				if !errors.Is(err, store.ErrLockConflict) {
					t.Errorf("AcquireLock racer %d failed: %v", i, err)
				}
				return
			}
			// Keep the winning lease held until every racer has attempted
			// acquisition. Releasing it here would let later racers win
			// sequentially rather than testing concurrent takeover.
			winners <- lock
		}(i)
	}
	wg.Wait()
	close(winners)

	if got := len(winners); got != 1 {
		t.Errorf("expected exactly 1 racer to win the expired lease, got %d", got)
	}
	for lock := range winners {
		lock.Close()
	}
}
