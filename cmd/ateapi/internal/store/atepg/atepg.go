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

// Package atepg is an ate storage backend built on PostgreSQL.
//
// Each table holds native SQL columns for fields SQL must operate on
// (primary keys, versions, pagination, update/delete preconditions) plus
// the complete protobuf message, binary-encoded, in a BYTEA column.
// TLS is configured entirely through the connection string passed
// to Connect (standard libpq sslmode/sslrootcert/sslcert/sslkey parameters)
package atepg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Persistence is a service that stores ate state in PostgreSQL.
type Persistence struct {
	pool        *pgxpool.Pool
	lockTTL     time.Duration
	stopJanitor context.CancelFunc
}

var _ store.Interface = (*Persistence)(nil)

// Connect opens a pgxpool against dsn, verifies connectivity, and applies the
// embedded schema. Startup fails if the database cannot be reached.
func Connect(ctx context.Context, dsn string) (*Persistence, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("opening PostgreSQL pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging PostgreSQL: %w", err)
	}
	p, err := NewPersistence(ctx, pool)
	if err != nil {
		pool.Close()
		return nil, err
	}
	return p, nil
}

// NewPersistence wraps an already-open pool, applying the idempotent schema.
// Callers that already hold a pool (e.g. tests using
// testcontainers) use this directly instead of Connect.
func NewPersistence(ctx context.Context, pool *pgxpool.Pool) (*Persistence, error) {
	if err := applySchema(ctx, pool); err != nil {
		return nil, err
	}
	janitorCtx, stopJanitor := context.WithCancel(context.Background())
	p := &Persistence{pool: pool, lockTTL: defaultLockTTL, stopJanitor: stopJanitor}
	// Cover the current and next hour before accepting writes; from then on
	// the janitor keeps partitions ahead of the clock (and the DEFAULT
	// partition catches writes if it ever falls behind).
	now := time.Now().UTC()
	if err := p.createWorkerChangesPartitions(ctx, now, now.Add(time.Hour)); err != nil {
		stopJanitor()
		return nil, err
	}
	go p.changeFeedJanitor(janitorCtx)
	return p, nil
}

// Close stops background maintenance (the change-feed janitor). It does not
// close the pool, which the caller owns.
func (p *Persistence) Close() {
	p.stopJanitor()
}

// querier is satisfied by both *pgxpool.Pool and pgx.Tx, letting read helpers
// run either directly against the pool or inside an in-flight transaction.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func newCreateMetadata(atespace, name string) *ateapipb.ResourceMetadata {
	now := timestamppb.Now()
	return &ateapipb.ResourceMetadata{
		Atespace:   atespace,
		Name:       name,
		Uid:        uuid.NewString(),
		Version:    1,
		CreateTime: now,
		UpdateTime: now,
	}
}

func newUpdateMetadata(current *ateapipb.ResourceMetadata) *ateapipb.ResourceMetadata {
	metadata := proto.Clone(current).(*ateapipb.ResourceMetadata)
	metadata.Version++
	metadata.UpdateTime = timestamppb.Now()
	return metadata
}

func isUniqueViolation(err error) bool { return pgErrCode(err) == "23505" }

// isForeignKeyViolation matches both the insert/update-side violation
// (23503, foreign_key_violation) and the delete-side violation PostgreSQL 18
// split out into its own code (23001, restrict_violation, for ON DELETE
// RESTRICT); older PostgreSQL versions report 23503 for both cases.
func isForeignKeyViolation(err error) bool {
	switch pgErrCode(err) {
	case "23503", "23001":
		return true
	default:
		return false
	}
}

func pgErrCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// --- Atespaces ---

func (p *Persistence) CreateAtespace(ctx context.Context, atespace *ateapipb.Atespace) (*ateapipb.Atespace, error) {
	name := atespace.GetMetadata().GetName()

	dbAtespace := proto.Clone(atespace).(*ateapipb.Atespace)
	dbAtespace.Metadata = newCreateMetadata("", name)

	protoBytes, err := proto.Marshal(dbAtespace)
	if err != nil {
		return nil, fmt.Errorf("marshaling atespace: %w", err)
	}

	_, err = p.pool.Exec(ctx, `
		INSERT INTO atespaces (name, proto)
		VALUES ($1, $2)`,
		name, protoBytes)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, store.ErrAlreadyExists
		}
		return nil, fmt.Errorf("inserting atespace %q: %w", name, err)
	}
	return dbAtespace, nil
}

func getAtespaceRow(ctx context.Context, q querier, name string) (*ateapipb.Atespace, error) {
	var protoBytes []byte
	err := q.QueryRow(ctx, `SELECT proto FROM atespaces WHERE name = $1`, name).Scan(&protoBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting atespace %q: %w", name, err)
	}
	out := &ateapipb.Atespace{}
	if err := proto.Unmarshal(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling atespace: %w", err)
	}
	return out, nil
}

func (p *Persistence) GetAtespace(ctx context.Context, name string) (*ateapipb.Atespace, error) {
	return getAtespaceRow(ctx, p.pool, name)
}

func (p *Persistence) AtespaceExists(ctx context.Context, name string) (bool, error) {
	var exists bool
	if err := p.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM atespaces WHERE name = $1)`, name).Scan(&exists); err != nil {
		return false, fmt.Errorf("checking atespace existence: %w", err)
	}
	return exists, nil
}

func (p *Persistence) ListAtespaces(ctx context.Context, pageSize int32, pageTokenStr string) ([]*ateapipb.Atespace, string, error) {
	token, err := decodePageToken(pageTokenStr, kindAtespace, "", 1)
	if err != nil {
		return nil, "", err
	}
	var last *string
	if len(token.Last) > 0 {
		last = &token.Last[0]
	}

	rows, err := p.pool.Query(ctx, `
		SELECT name, proto FROM atespaces
		WHERE $1::text IS NULL OR name > $1
		ORDER BY name
		LIMIT $2`, last, int64(pageSize)+1)
	if err != nil {
		return nil, "", fmt.Errorf("listing atespaces: %w", err)
	}
	defer rows.Close()

	var names []string
	var result []*ateapipb.Atespace
	for rows.Next() {
		var name string
		var protoBytes []byte
		if err := rows.Scan(&name, &protoBytes); err != nil {
			return nil, "", fmt.Errorf("scanning atespace row: %w", err)
		}
		a := &ateapipb.Atespace{}
		if err := proto.Unmarshal(protoBytes, a); err != nil {
			return nil, "", fmt.Errorf("unmarshaling atespace: %w", err)
		}
		result = append(result, a)
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("listing atespaces: %w", err)
	}

	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		nextToken = encodePageToken(kindAtespace, "", []string{names[pageSize-1]})
	}
	return result, nextToken, nil
}

func (p *Persistence) DeleteAtespace(ctx context.Context, name string) (*ateapipb.Atespace, error) {
	var protoBytes []byte
	err := p.pool.QueryRow(ctx, `DELETE FROM atespaces WHERE name = $1 RETURNING proto`, name).Scan(&protoBytes)
	if err != nil {
		if isForeignKeyViolation(err) {
			return nil, store.ErrFailedPrecondition
		}
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("deleting atespace %q: %w", name, err)
	}
	out := &ateapipb.Atespace{}
	if err := proto.Unmarshal(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling deleted atespace: %w", err)
	}
	return out, nil
}

// --- Actors ---

func (p *Persistence) CreateActor(ctx context.Context, actor *ateapipb.Actor) (*ateapipb.Actor, error) {
	atespace := actor.GetMetadata().GetAtespace()
	name := actor.GetMetadata().GetName()

	dbActor := proto.Clone(actor).(*ateapipb.Actor)
	dbActor.Metadata = newCreateMetadata(atespace, name)

	protoBytes, err := proto.Marshal(dbActor)
	if err != nil {
		return nil, fmt.Errorf("marshaling actor: %w", err)
	}

	_, err = p.pool.Exec(ctx, `
		INSERT INTO actors (atespace, name, uid, version, proto)
		VALUES ($1, $2, $3, $4, $5)`,
		atespace, name, dbActor.GetMetadata().GetUid(), dbActor.GetMetadata().GetVersion(), protoBytes)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, store.ErrAlreadyExists
		}
		if isForeignKeyViolation(err) {
			// The atespace referenced by this actor doesn't exist (or was
			// deleted concurrently with the control API's own pre-check).
			return nil, store.ErrFailedPrecondition
		}
		return nil, fmt.Errorf("inserting actor %s/%s: %w", atespace, name, err)
	}
	return dbActor, nil
}

func getActorRow(ctx context.Context, q querier, atespace, name string) (*ateapipb.Actor, error) {
	var protoBytes []byte
	err := q.QueryRow(ctx, `SELECT proto FROM actors WHERE atespace = $1 AND name = $2`, atespace, name).Scan(&protoBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting actor %s/%s: %w", atespace, name, err)
	}
	out := &ateapipb.Actor{}
	if err := proto.Unmarshal(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling actor: %w", err)
	}
	return out, nil
}

func (p *Persistence) GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error) {
	return getActorRow(ctx, p.pool, actorRef.Atespace, actorRef.Name)
}

// validateUpdateActorMutation reports whether an actor mutation changed fields
// that are immutable for the lifetime of the stored actor.
func validateUpdateActorMutation(storedActor, mutatedActor *ateapipb.Actor) error {
	if stored, mutated := storedActor.GetMetadata().GetAtespace(), mutatedActor.GetMetadata().GetAtespace(); stored != mutated {
		return fmt.Errorf("metadata.atespace is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedActor.GetMetadata().GetName(), mutatedActor.GetMetadata().GetName(); stored != mutated {
		return fmt.Errorf("metadata.name is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedActor.GetActorTemplateNamespace(), mutatedActor.GetActorTemplateNamespace(); stored != mutated {
		return fmt.Errorf("actor_template_namespace is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedActor.GetActorTemplateName(), mutatedActor.GetActorTemplateName(); stored != mutated {
		return fmt.Errorf("actor_template_name is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	return nil
}

func (p *Persistence) UpdateActor(ctx context.Context, actorRef resources.ActorRef, mutate func(*ateapipb.Actor) error) (*ateapipb.Actor, error) {
	atespace, name := actorRef.Atespace, actorRef.Name

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning actor update: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	var protoBytes []byte
	if err := tx.QueryRow(ctx, `
		SELECT proto FROM actors
		WHERE atespace = $1 AND name = $2
		FOR UPDATE`, atespace, name).Scan(&protoBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("locking actor %s/%s for update: %w", atespace, name, err)
	}

	dbActor := &ateapipb.Actor{}
	if err := proto.Unmarshal(protoBytes, dbActor); err != nil {
		return nil, fmt.Errorf("unmarshaling actor for update: %w", err)
	}
	actorBeforeMutation := proto.Clone(dbActor).(*ateapipb.Actor)
	if err := mutate(dbActor); err != nil {
		return nil, err
	}
	if err := validateUpdateActorMutation(actorBeforeMutation, dbActor); err != nil {
		return nil, err
	}
	// Stored metadata is authoritative; discard any metadata edits made by the
	// closure and derive the next revision from the transactionally read actor.
	dbActor.Metadata = newUpdateMetadata(actorBeforeMutation.GetMetadata())

	protoBytes, err = proto.Marshal(dbActor)
	if err != nil {
		return nil, fmt.Errorf("marshaling actor: %w", err)
	}

	commandTag, err := tx.Exec(ctx, `
		UPDATE actors
		SET version = $1, proto = $2
		WHERE atespace = $3 AND name = $4`,
		dbActor.GetMetadata().GetVersion(), protoBytes, atespace, name)
	if err != nil {
		return nil, fmt.Errorf("updating actor %s/%s: %w", atespace, name, err)
	}
	if commandTag.RowsAffected() != 1 {
		return nil, fmt.Errorf("updating actor %s/%s affected %d rows, want 1", atespace, name, commandTag.RowsAffected())
	}


	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing actor update: %w", err)
	}
	return dbActor, nil
}

func (p *Persistence) DeleteActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error) {
	atespace, name := actorRef.Atespace, actorRef.Name
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning actor delete: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	var protoBytes []byte
	err = tx.QueryRow(ctx, `
		SELECT proto FROM actors
		WHERE atespace = $1 AND name = $2
		FOR UPDATE`,
		atespace, name,
	).Scan(&protoBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("locking actor %s/%s for deletion: %w", atespace, name, err)
	}

	out := &ateapipb.Actor{}
	if err := proto.Unmarshal(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling actor for deletion: %w", err)
	}
	if out.GetStatus() != ateapipb.Actor_STATUS_DELETING {
		return nil, store.ErrFailedPrecondition
	}
	if _, err := tx.Exec(ctx, `DELETE FROM actors WHERE atespace = $1 AND name = $2`, atespace, name); err != nil {
		return nil, fmt.Errorf("deleting actor %s/%s: %w", atespace, name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing actor delete: %w", err)
	}
	return out, nil
}

func (p *Persistence) ListActors(ctx context.Context, atespace string, pageSize int32, pageTokenStr string) ([]*ateapipb.Actor, string, error) {
	if atespace != "" {
		return p.listActorsScoped(ctx, atespace, pageSize, pageTokenStr)
	}
	return p.listActorsGlobal(ctx, pageSize, pageTokenStr)
}

func (p *Persistence) listActorsScoped(ctx context.Context, atespace string, pageSize int32, pageTokenStr string) ([]*ateapipb.Actor, string, error) {
	token, err := decodePageToken(pageTokenStr, kindActor, atespace, 1)
	if err != nil {
		return nil, "", err
	}
	var last *string
	if len(token.Last) > 0 {
		last = &token.Last[0]
	}

	rows, err := p.pool.Query(ctx, `
		SELECT name, proto FROM actors
		WHERE atespace = $1 AND ($2::text IS NULL OR name > $2)
		ORDER BY name
		LIMIT $3`, atespace, last, int64(pageSize)+1)
	if err != nil {
		return nil, "", fmt.Errorf("listing actors in %q: %w", atespace, err)
	}
	defer rows.Close()

	var names []string
	var result []*ateapipb.Actor
	for rows.Next() {
		var name string
		var protoBytes []byte
		if err := rows.Scan(&name, &protoBytes); err != nil {
			return nil, "", fmt.Errorf("scanning actor row: %w", err)
		}
		a := &ateapipb.Actor{}
		if err := proto.Unmarshal(protoBytes, a); err != nil {
			return nil, "", fmt.Errorf("unmarshaling actor: %w", err)
		}
		result = append(result, a)
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("listing actors in %q: %w", atespace, err)
	}

	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		nextToken = encodePageToken(kindActor, atespace, []string{names[pageSize-1]})
	}
	return result, nextToken, nil
}

func (p *Persistence) listActorsGlobal(ctx context.Context, pageSize int32, pageTokenStr string) ([]*ateapipb.Actor, string, error) {
	token, err := decodePageToken(pageTokenStr, kindActor, "", 2)
	if err != nil {
		return nil, "", err
	}
	var lastAtespace, lastName *string
	if len(token.Last) == 2 {
		lastAtespace, lastName = &token.Last[0], &token.Last[1]
	}

	rows, err := p.pool.Query(ctx, `
		SELECT atespace, name, proto FROM actors
		WHERE $1::text IS NULL OR (atespace, name) > ($1, $2)
		ORDER BY atespace, name
		LIMIT $3`, lastAtespace, lastName, int64(pageSize)+1)
	if err != nil {
		return nil, "", fmt.Errorf("listing actors: %w", err)
	}
	defer rows.Close()

	type key struct{ atespace, name string }
	var keys []key
	var result []*ateapipb.Actor
	for rows.Next() {
		var k key
		var protoBytes []byte
		if err := rows.Scan(&k.atespace, &k.name, &protoBytes); err != nil {
			return nil, "", fmt.Errorf("scanning actor row: %w", err)
		}
		a := &ateapipb.Actor{}
		if err := proto.Unmarshal(protoBytes, a); err != nil {
			return nil, "", fmt.Errorf("unmarshaling actor: %w", err)
		}
		result = append(result, a)
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("listing actors: %w", err)
	}

	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		last := keys[pageSize-1]
		nextToken = encodePageToken(kindActor, "", []string{last.atespace, last.name})
	}
	return result, nextToken, nil
}

// --- Actor snapshots ---

func (p *Persistence) CreateActorSnapshot(ctx context.Context, snapshot *ateapipb.ActorSnapshot) (*ateapipb.ActorSnapshot, error) {
	atespace := snapshot.GetMetadata().GetAtespace()
	name := snapshot.GetMetadata().GetName()
	dbSnapshot := proto.Clone(snapshot).(*ateapipb.ActorSnapshot)
	dbSnapshot.Metadata = newCreateMetadata(atespace, name)

	protoBytes, err := proto.Marshal(dbSnapshot)
	if err != nil {
		return nil, fmt.Errorf("marshaling actor snapshot: %w", err)
	}
	if _, err := p.pool.Exec(ctx, `
		INSERT INTO actor_snapshots (atespace, name, proto)
		VALUES ($1, $2, $3)`,
		atespace, name, protoBytes); err != nil {
		if isUniqueViolation(err) {
			return nil, store.ErrAlreadyExists
		}
		return nil, fmt.Errorf("inserting actor snapshot %s/%s: %w", atespace, name, err)
	}
	return dbSnapshot, nil
}

func getActorSnapshotRow(ctx context.Context, q querier, atespace, name string) (*ateapipb.ActorSnapshot, error) {
	var protoBytes []byte
	if err := q.QueryRow(ctx, `
		SELECT proto FROM actor_snapshots
		WHERE atespace = $1 AND name = $2`, atespace, name).Scan(&protoBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting actor snapshot %s/%s: %w", atespace, name, err)
	}
	out := &ateapipb.ActorSnapshot{}
	if err := proto.Unmarshal(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling actor snapshot: %w", err)
	}
	return out, nil
}

func (p *Persistence) GetActorSnapshot(ctx context.Context, atespace, name string) (*ateapipb.ActorSnapshot, error) {
	return getActorSnapshotRow(ctx, p.pool, atespace, name)
}

func (p *Persistence) GetActorSnapshotByTag(ctx context.Context, atespace, name string) (*ateapipb.ActorSnapshot, *ateapipb.ActorSnapshotTag, error) {
	return p.getActorSnapshotByTag(ctx, p.pool, atespace, name)
}

func (p *Persistence) ListActorSnapshots(ctx context.Context, atespace string, pageSize int32, pageTokenStr string) ([]*ateapipb.ActorSnapshot, string, error) {
	if atespace != "" {
		return p.listActorSnapshotsScoped(ctx, atespace, pageSize, pageTokenStr)
	}
	return p.listActorSnapshotsGlobal(ctx, pageSize, pageTokenStr)
}

func (p *Persistence) listActorSnapshotsScoped(ctx context.Context, atespace string, pageSize int32, pageTokenStr string) ([]*ateapipb.ActorSnapshot, string, error) {
	token, err := decodePageToken(pageTokenStr, kindSnapshot, atespace, 1)
	if err != nil {
		return nil, "", err
	}
	var last *string
	if len(token.Last) > 0 {
		last = &token.Last[0]
	}
	rows, err := p.pool.Query(ctx, `
		SELECT name, proto FROM actor_snapshots
		WHERE atespace = $1 AND ($2::text IS NULL OR name > $2)
		ORDER BY name
		LIMIT $3`, atespace, last, int64(pageSize)+1)
	if err != nil {
		return nil, "", fmt.Errorf("listing actor snapshots in %q: %w", atespace, err)
	}
	defer rows.Close()

	var names []string
	var result []*ateapipb.ActorSnapshot
	for rows.Next() {
		var name string
		var protoBytes []byte
		if err := rows.Scan(&name, &protoBytes); err != nil {
			return nil, "", fmt.Errorf("scanning actor snapshot row: %w", err)
		}
		snapshot := &ateapipb.ActorSnapshot{}
		if err := proto.Unmarshal(protoBytes, snapshot); err != nil {
			return nil, "", fmt.Errorf("unmarshaling actor snapshot: %w", err)
		}
		result = append(result, snapshot)
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("listing actor snapshots in %q: %w", atespace, err)
	}
	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		nextToken = encodePageToken(kindSnapshot, atespace, []string{names[pageSize-1]})
	}
	return result, nextToken, nil
}

func (p *Persistence) listActorSnapshotsGlobal(ctx context.Context, pageSize int32, pageTokenStr string) ([]*ateapipb.ActorSnapshot, string, error) {
	token, err := decodePageToken(pageTokenStr, kindSnapshot, "", 2)
	if err != nil {
		return nil, "", err
	}
	var lastAtespace, lastName *string
	if len(token.Last) == 2 {
		lastAtespace, lastName = &token.Last[0], &token.Last[1]
	}
	rows, err := p.pool.Query(ctx, `
		SELECT atespace, name, proto FROM actor_snapshots
		WHERE $1::text IS NULL OR (atespace, name) > ($1, $2)
		ORDER BY atespace, name
		LIMIT $3`, lastAtespace, lastName, int64(pageSize)+1)
	if err != nil {
		return nil, "", fmt.Errorf("listing actor snapshots: %w", err)
	}
	defer rows.Close()

	type key struct{ atespace, name string }
	var keys []key
	var result []*ateapipb.ActorSnapshot
	for rows.Next() {
		var k key
		var protoBytes []byte
		if err := rows.Scan(&k.atespace, &k.name, &protoBytes); err != nil {
			return nil, "", fmt.Errorf("scanning actor snapshot row: %w", err)
		}
		snapshot := &ateapipb.ActorSnapshot{}
		if err := proto.Unmarshal(protoBytes, snapshot); err != nil {
			return nil, "", fmt.Errorf("unmarshaling actor snapshot: %w", err)
		}
		result = append(result, snapshot)
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("listing actor snapshots: %w", err)
	}
	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		last := keys[pageSize-1]
		nextToken = encodePageToken(kindSnapshot, "", []string{last.atespace, last.name})
	}
	return result, nextToken, nil
}

func (p *Persistence) TagActorSnapshot(ctx context.Context, snapshotAtespace, snapshotName string, tag *ateapipb.ActorSnapshotTag) (*ateapipb.ActorSnapshotTag, error) {
	tagAtespace := tag.GetMetadata().GetAtespace()
	tagName := tag.GetMetadata().GetName()
	dbTag := proto.Clone(tag).(*ateapipb.ActorSnapshotTag)
	dbTag.Metadata = newCreateMetadata(tagAtespace, tagName)
	dbTag.Snapshot = &ateapipb.ObjectRef{Atespace: snapshotAtespace, Name: snapshotName}
	protoBytes, err := proto.Marshal(dbTag)
	if err != nil {
		return nil, fmt.Errorf("marshaling actor snapshot tag: %w", err)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning actor snapshot tag create: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed
	if _, err := getActorSnapshotRow(ctx, tx, snapshotAtespace, snapshotName); err != nil {
		return nil, err
	}

	var inserted []byte
	err = tx.QueryRow(ctx, `
		INSERT INTO actor_snapshot_tags
		    (atespace, name, snapshot_atespace, snapshot_name, version, proto)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (atespace, name) DO NOTHING
		RETURNING proto`, tagAtespace, tagName, snapshotAtespace, snapshotName,
		dbTag.GetMetadata().GetVersion(), protoBytes).Scan(&inserted)
	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("committing actor snapshot tag create: %w", err)
		}
		return dbTag, nil
	}
	if isForeignKeyViolation(err) {
		return nil, store.ErrFailedPrecondition
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("inserting actor snapshot tag %s/%s: %w", tagAtespace, tagName, err)
	}

	var existingBytes []byte
	if err := tx.QueryRow(ctx, `
		SELECT proto FROM actor_snapshot_tags
		WHERE atespace = $1 AND name = $2`, tagAtespace, tagName).Scan(&existingBytes); err != nil {
		return nil, fmt.Errorf("getting existing actor snapshot tag %s/%s: %w", tagAtespace, tagName, err)
	}
	existing := &ateapipb.ActorSnapshotTag{}
	if err := proto.Unmarshal(existingBytes, existing); err != nil {
		return nil, fmt.Errorf("unmarshaling actor snapshot tag: %w", err)
	}
	if existing.GetSnapshot().GetAtespace() != snapshotAtespace || existing.GetSnapshot().GetName() != snapshotName || existing.GetScope() != tag.GetScope() {
		return nil, store.ErrAlreadyExists
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing idempotent actor snapshot tag create: %w", err)
	}
	return existing, nil
}

func (p *Persistence) UpdateActorSnapshotTag(ctx context.Context, atespace, name string, scope ateapipb.ActorSnapshotTagScope, expectedVersion int64) (*ateapipb.ActorSnapshotTag, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning actor snapshot tag update: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	var currentBytes []byte
	var currentVersion int64
	if err := tx.QueryRow(ctx, `
		SELECT version, proto FROM actor_snapshot_tags
		WHERE atespace = $1 AND name = $2`, atespace, name).Scan(&currentVersion, &currentBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting actor snapshot tag %s/%s: %w", atespace, name, err)
	}
	if currentVersion != expectedVersion {
		return nil, store.ErrVersionConflict
	}
	current := &ateapipb.ActorSnapshotTag{}
	if err := proto.Unmarshal(currentBytes, current); err != nil {
		return nil, fmt.Errorf("unmarshaling actor snapshot tag: %w", err)
	}
	if current.GetScope() == scope {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("committing unchanged actor snapshot tag update: %w", err)
		}
		return current, nil
	}

	updated := proto.Clone(current).(*ateapipb.ActorSnapshotTag)
	updated.Scope = scope
	updated.Metadata = newUpdateMetadata(current.GetMetadata())
	updatedBytes, err := proto.Marshal(updated)
	if err != nil {
		return nil, fmt.Errorf("marshaling actor snapshot tag: %w", err)
	}
	var returned []byte
	err = tx.QueryRow(ctx, `
		UPDATE actor_snapshot_tags
		SET version = $1, proto = $2
		WHERE atespace = $3 AND name = $4 AND version = $5
		RETURNING proto`, updated.GetMetadata().GetVersion(), updatedBytes,
		atespace, name, expectedVersion).Scan(&returned)
	if errors.Is(err, pgx.ErrNoRows) {
		if _, _, getErr := p.getActorSnapshotByTag(ctx, tx, atespace, name); getErr != nil {
			return nil, getErr
		}
		return nil, store.ErrVersionConflict
	}
	if err != nil {
		return nil, fmt.Errorf("updating actor snapshot tag %s/%s: %w", atespace, name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing actor snapshot tag update: %w", err)
	}
	return updated, nil
}

func (p *Persistence) getActorSnapshotByTag(ctx context.Context, q querier, atespace, name string) (*ateapipb.ActorSnapshot, *ateapipb.ActorSnapshotTag, error) {
	var snapshotBytes, tagBytes []byte
	err := q.QueryRow(ctx, `
		SELECT s.proto, t.proto
		FROM actor_snapshot_tags AS t
		JOIN actor_snapshots AS s
		  ON s.atespace = t.snapshot_atespace AND s.name = t.snapshot_name
		WHERE t.atespace = $1 AND t.name = $2`, atespace, name).Scan(&snapshotBytes, &tagBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, store.ErrNotFound
		}
		return nil, nil, fmt.Errorf("resolving actor snapshot tag %s/%s: %w", atespace, name, err)
	}
	snapshot := &ateapipb.ActorSnapshot{}
	if err := proto.Unmarshal(snapshotBytes, snapshot); err != nil {
		return nil, nil, fmt.Errorf("unmarshaling actor snapshot: %w", err)
	}
	tag := &ateapipb.ActorSnapshotTag{}
	if err := proto.Unmarshal(tagBytes, tag); err != nil {
		return nil, nil, fmt.Errorf("unmarshaling actor snapshot tag: %w", err)
	}
	return snapshot, tag, nil
}

func (p *Persistence) DeleteActorSnapshotTag(ctx context.Context, atespace, name string) (*ateapipb.ActorSnapshotTag, error) {
	var protoBytes []byte
	if err := p.pool.QueryRow(ctx, `
		DELETE FROM actor_snapshot_tags
		WHERE atespace = $1 AND name = $2
		RETURNING proto`, atespace, name).Scan(&protoBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("deleting actor snapshot tag %s/%s: %w", atespace, name, err)
	}
	tag := &ateapipb.ActorSnapshotTag{}
	if err := proto.Unmarshal(protoBytes, tag); err != nil {
		return nil, fmt.Errorf("unmarshaling deleted actor snapshot tag: %w", err)
	}
	return tag, nil
}

// --- Workers ---



type workerEventEnvelope struct {
	Type   int    `json:"t"`
	Worker string `json:"w"` // protojson-encoded Worker
}

func marshalWorkerEvent(eventType store.WorkerEventType, worker *ateapipb.Worker) ([]byte, error) {
	workerJSON, err := protojson.Marshal(worker)
	if err != nil {
		return nil, fmt.Errorf("in protojson.Marshal: %w", err)
	}
	msg, err := json.Marshal(workerEventEnvelope{Type: int(eventType), Worker: string(workerJSON)})
	if err != nil {
		return nil, fmt.Errorf("in json.Marshal: %w", err)
	}
	return msg, nil
}

func unmarshalWorkerEvent(payload []byte) (store.WorkerEvent, error) {
	var env workerEventEnvelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return store.WorkerEvent{}, fmt.Errorf("in json.Unmarshal: %w", err)
	}
	worker := &ateapipb.Worker{}
	if err := protojson.Unmarshal([]byte(env.Worker), worker); err != nil {
		return store.WorkerEvent{}, fmt.Errorf("in protojson.Unmarshal: %w", err)
	}
	return store.WorkerEvent{Type: store.WorkerEventType(env.Type), Worker: worker}, nil
}

// writeAndAppendChange runs fn inside a transaction, then--only if fn
// reports a change worth publishing--appends the event to the worker_changes
// feed in the same transaction, so watchers see it if and only if the
// transaction commits.
func (p *Persistence) writeAndAppendChange(ctx context.Context, eventType store.WorkerEventType, worker *ateapipb.Worker, fn func(ctx context.Context, tx pgx.Tx) (changed bool, err error)) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	changed, err := fn(ctx, tx)
	if err != nil {
		return err
	}

	if changed {
		payload, err := marshalWorkerEvent(eventType, worker)
		if err != nil {
			return fmt.Errorf("marshaling worker event: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO worker_changes (payload) VALUES ($1)`, payload); err != nil {
			return fmt.Errorf("appending worker change feed: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

func (p *Persistence) CreateWorker(ctx context.Context, worker *ateapipb.Worker) error {
	dbWorker := proto.Clone(worker).(*ateapipb.Worker)
	dbWorker.Version = 1

	protoBytes, err := proto.Marshal(dbWorker)
	if err != nil {
		return fmt.Errorf("marshaling worker: %w", err)
	}

	err = p.writeAndAppendChange(ctx, store.WorkerEventCreated, dbWorker, func(ctx context.Context, tx pgx.Tx) (bool, error) {
		_, err := tx.Exec(ctx, `
			INSERT INTO workers (worker_namespace, worker_pool, worker_pod, version, proto)
			VALUES ($1, $2, $3, $4, $5)`,
			dbWorker.GetWorkerNamespace(), dbWorker.GetWorkerPool(), dbWorker.GetWorkerPod(), dbWorker.GetVersion(), protoBytes)
		if err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		if isUniqueViolation(err) {
			return store.ErrAlreadyExists
		}
		return fmt.Errorf("creating worker: %w", err)
	}
	return nil
}

func getWorkerRow(ctx context.Context, q querier, namespace, poolName, pod string) (*ateapipb.Worker, error) {
	var protoBytes []byte
	err := q.QueryRow(ctx, `SELECT proto FROM workers WHERE worker_namespace = $1 AND worker_pool = $2 AND worker_pod = $3`,
		namespace, poolName, pod).Scan(&protoBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting worker %s/%s/%s: %w", namespace, poolName, pod, err)
	}
	out := &ateapipb.Worker{}
	if err := proto.Unmarshal(protoBytes, out); err != nil {
		return nil, fmt.Errorf("unmarshaling worker: %w", err)
	}
	return out, nil
}

func (p *Persistence) GetWorker(ctx context.Context, namespace, poolName, pod string) (*ateapipb.Worker, error) {
	return getWorkerRow(ctx, p.pool, namespace, poolName, pod)
}

func (p *Persistence) UpdateWorker(ctx context.Context, worker *ateapipb.Worker, expectedVersion int64) error {
	namespace, poolName, pod := worker.GetWorkerNamespace(), worker.GetWorkerPool(), worker.GetWorkerPod()

	dbWorker := proto.Clone(worker).(*ateapipb.Worker)
	dbWorker.Version = expectedVersion + 1

	protoBytes, err := proto.Marshal(dbWorker)
	if err != nil {
		return fmt.Errorf("marshaling worker: %w", err)
	}

	return p.writeAndAppendChange(ctx, store.WorkerEventUpdated, dbWorker, func(ctx context.Context, tx pgx.Tx) (bool, error) {
		var returned []byte
		err := tx.QueryRow(ctx, `
			UPDATE workers
			SET version = $1, proto = $2
			WHERE worker_namespace = $3 AND worker_pool = $4 AND worker_pod = $5
			  AND version = $6
			RETURNING proto`,
			dbWorker.GetVersion(), protoBytes, namespace, poolName, pod, expectedVersion,
		).Scan(&returned)
		if err == nil {
			return true, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return false, fmt.Errorf("updating worker %s/%s/%s: %w", namespace, poolName, pod, err)
		}

		current, getErr := getWorkerRow(ctx, tx, namespace, poolName, pod)
		if getErr != nil {
			return false, getErr
		}
		if current.GetVersion() != expectedVersion {
			return false, store.ErrVersionConflict
		}
		return false, fmt.Errorf("update worker %s/%s/%s: no row matched but current state is otherwise consistent", namespace, poolName, pod)
	})
}

func (p *Persistence) DeleteWorker(ctx context.Context, namespace, poolName, pod string) error {
	deletedEvent := &ateapipb.Worker{WorkerNamespace: namespace, WorkerPod: pod}
	return p.writeAndAppendChange(ctx, store.WorkerEventDeleted, deletedEvent, func(ctx context.Context, tx pgx.Tx) (bool, error) {
		var protoBytes []byte
		err := tx.QueryRow(ctx, `
			DELETE FROM workers
			WHERE worker_namespace = $1 AND worker_pool = $2 AND worker_pod = $3
			RETURNING proto`, namespace, poolName, pod).Scan(&protoBytes)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// Idempotent: nothing existed, so nothing to notify either.
				return false, nil
			}
			return false, fmt.Errorf("deleting worker %s/%s/%s: %w", namespace, poolName, pod, err)
		}
		return true, nil
	})
}

func (p *Persistence) ListWorkers(ctx context.Context, pageSize int32, pageTokenStr string) ([]*ateapipb.Worker, string, error) {
	token, err := decodePageToken(pageTokenStr, kindWorker, "", 3)
	if err != nil {
		return nil, "", err
	}
	var lastNS, lastPool, lastPod *string
	if len(token.Last) == 3 {
		lastNS, lastPool, lastPod = &token.Last[0], &token.Last[1], &token.Last[2]
	}

	rows, err := p.pool.Query(ctx, `
		SELECT worker_namespace, worker_pool, worker_pod, proto FROM workers
		WHERE $1::text IS NULL OR (worker_namespace, worker_pool, worker_pod) > ($1, $2, $3)
		ORDER BY worker_namespace, worker_pool, worker_pod
		LIMIT $4`, lastNS, lastPool, lastPod, int64(pageSize)+1)
	if err != nil {
		return nil, "", fmt.Errorf("listing workers: %w", err)
	}
	defer rows.Close()

	type key struct{ namespace, pool, pod string }
	var keys []key
	var result []*ateapipb.Worker
	for rows.Next() {
		var k key
		var protoBytes []byte
		if err := rows.Scan(&k.namespace, &k.pool, &k.pod, &protoBytes); err != nil {
			return nil, "", fmt.Errorf("scanning worker row: %w", err)
		}
		w := &ateapipb.Worker{}
		if err := proto.Unmarshal(protoBytes, w); err != nil {
			return nil, "", fmt.Errorf("unmarshaling worker: %w", err)
		}
		result = append(result, w)
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("listing workers: %w", err)
	}

	var nextToken string
	if len(result) > int(pageSize) {
		result = result[:pageSize]
		last := keys[pageSize-1]
		nextToken = encodePageToken(kindWorker, "", []string{last.namespace, last.pool, last.pod})
	}
	return result, nextToken, nil
}

// changeFeedPollInterval bounds worker-event delivery latency (watch target
// is <=1s). changeFeedBatch caps rows fetched per poll; a burst beyond it
// carries over to the next poll (events are delayed, never dropped).
// changeFeedRetentionAge is the minimum time the janitor keeps feed rows;
// it must comfortably exceed worst-case watcher lag, because a watcher that
// falls behind it closes for resync (see WatchWorkers). Retention operates
// on whole hourly partitions, so a row actually lives between
// changeFeedRetentionAge and changeFeedRetentionAge + 1h.
// changeFeedJanitorInterval paces partition maintenance;
// changeFeedJanitorBatch bounds the row-wise fallback trim of the DEFAULT
// partition.
const (
	changeFeedPollInterval    = 100 * time.Millisecond
	changeFeedBatch           = 1024
	changeFeedRetentionAge    = 15 * time.Minute
	changeFeedJanitorInterval = time.Minute
	changeFeedJanitorBatch    = 10_000
)

// changeFeedJanitor maintains worker_changes partitions on a fixed timer,
// for the life of the Persistence. Retention is deliberately decoupled from
// watcher cursors: keying it to any one watcher's position would let a fast
// watcher discard rows a slower one has not consumed, and a janitor living
// inside a watcher goroutine never runs on a process holding no watch,
// letting the table grow without bound (the write path always appends).
// Retention is a partition DROP — a metadata operation — so reclaiming even
// a large backlog produces no delete/WAL/vacuum load competing with
// foreground traffic (bulk row DELETEs measurably degrade update p99; see
// the benchmarking ledger).
func (p *Persistence) changeFeedJanitor(ctx context.Context) {
	ticker := time.NewTicker(changeFeedJanitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := p.maintainWorkerChangesPartitions(ctx); err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "worker change feed janitor failed", slog.Any("err", err))
		}
	}
}

// maintainWorkerChangesPartitions is one janitor pass: make sure the
// current and next hourly partitions exist, drop partitions wholly past
// retention, and trim any strays that landed in the DEFAULT partition.
// Every step is idempotent, so concurrent replicas racing a pass is
// harmless.
func (p *Persistence) maintainWorkerChangesPartitions(ctx context.Context) error {
	now := time.Now().UTC()
	if err := p.createWorkerChangesPartitions(ctx, now, now.Add(time.Hour)); err != nil {
		return err
	}
	if err := p.dropExpiredWorkerChangesPartitions(ctx, now); err != nil {
		return err
	}
	for {
		n, err := p.trimWorkerChangesDefault(ctx)
		if err != nil {
			return err
		}
		if n < changeFeedJanitorBatch {
			return nil
		}
	}
}

func workerChangesPartitionName(hour time.Time) string {
	return "worker_changes_p" + hour.UTC().Format("2006010215")
}

// createWorkerChangesPartitions idempotently creates the hourly partitions
// covering the given instants. Serialized with an advisory lock: IF NOT
// EXISTS does not close every concurrent-DDL race.
func (p *Persistence) createWorkerChangesPartitions(ctx context.Context, hours ...time.Time) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning feed partition transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('agent-substrate-atepg-feed-partitions'))`); err != nil {
		return fmt.Errorf("locking feed partition DDL: %w", err)
	}
	for _, h := range hours {
		start := h.UTC().Truncate(time.Hour)
		stmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s PARTITION OF worker_changes FOR VALUES FROM ('%s') TO ('%s')`,
			workerChangesPartitionName(start), start.Format(time.RFC3339), start.Add(time.Hour).Format(time.RFC3339))
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("creating feed partition for %s: %w", start, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing feed partition transaction: %w", err)
	}
	return nil
}

// dropExpiredWorkerChangesPartitions drops every hourly partition whose
// entire range is older than retention. Each drop first records the
// partition's greatest seq in worker_changes_trim, in the same transaction,
// so watchers can detect exactly (not by inferring from identity gaps,
// which rollbacks burn) that rows were discarded past their cursor.
func (p *Persistence) dropExpiredWorkerChangesPartitions(ctx context.Context, now time.Time) error {
	rows, err := p.pool.Query(ctx, `
		SELECT c.relname FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid
		JOIN pg_class parent ON parent.oid = i.inhparent
		WHERE parent.relname = 'worker_changes'`)
	if err != nil {
		return fmt.Errorf("listing feed partitions: %w", err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return fmt.Errorf("scanning feed partition name: %w", err)
		}
		names = append(names, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("listing feed partitions: %w", err)
	}

	for _, name := range names {
		suffix, ok := strings.CutPrefix(name, "worker_changes_p")
		if !ok || suffix == "default" {
			continue
		}
		start, err := time.Parse("2006010215", suffix)
		if err != nil {
			continue // not a partition this janitor manages
		}
		if now.Sub(start.Add(time.Hour)) < changeFeedRetentionAge {
			continue
		}
		if err := p.dropWorkerChangesPartition(ctx, name); err != nil {
			return err
		}
	}
	return nil
}

func (p *Persistence) dropWorkerChangesPartition(ctx context.Context, name string) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning feed partition drop: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	// A concurrent replica may have dropped it between listing and here.
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, name).Scan(&exists); err != nil {
		return fmt.Errorf("checking feed partition %s: %w", name, err)
	}
	if !exists {
		return nil
	}
	ident := pgx.Identifier{name}.Sanitize()
	if _, err := tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO worker_changes_trim (seq)
		SELECT max(seq) FROM %s HAVING count(*) > 0
		ON CONFLICT (id) DO UPDATE SET seq = GREATEST(worker_changes_trim.seq, EXCLUDED.seq)`, ident)); err != nil {
		return fmt.Errorf("recording trim mark for feed partition %s: %w", name, err)
	}
	if _, err := tx.Exec(ctx, `DROP TABLE `+ident); err != nil {
		return fmt.Errorf("dropping feed partition %s: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing feed partition drop: %w", err)
	}
	return nil
}

// trimWorkerChangesDefault deletes one batch of aged rows from the DEFAULT
// partition — the row-wise fallback for writes that landed there because no
// hourly partition covered them (partition creation stalled). Empty in
// normal operation. Returns rows deleted; 0 without error when another
// replica holds the lock this round.
func (p *Persistence) trimWorkerChangesDefault(ctx context.Context) (int64, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("beginning feed janitor transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	var elected bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext('agent-substrate-atepg-change-feed-janitor'))`).Scan(&elected); err != nil {
		return 0, fmt.Errorf("electing feed janitor: %w", err)
	}
	if !elected {
		return 0, nil
	}
	var n int64
	if err := tx.QueryRow(ctx, `
		WITH doomed AS (
			DELETE FROM worker_changes_pdefault WHERE seq IN (
				SELECT seq FROM worker_changes_pdefault
				WHERE created_at < now() - make_interval(secs => $1)
				ORDER BY seq LIMIT $2)
			RETURNING seq
		), recorded AS (
			INSERT INTO worker_changes_trim (seq)
			SELECT max(seq) FROM doomed HAVING count(*) > 0
			ON CONFLICT (id) DO UPDATE SET seq = GREATEST(worker_changes_trim.seq, EXCLUDED.seq)
		)
		SELECT count(*) FROM doomed`,
		changeFeedRetentionAge.Seconds(), changeFeedJanitorBatch).Scan(&n); err != nil {
		return 0, fmt.Errorf("trimming feed default partition: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("committing feed janitor transaction: %w", err)
	}
	return n, nil
}

// WatchWorkers subscribes by polling the worker_changes feed table past a
// (xid, seq) cursor. Events are appended to the feed in the same transaction
// as the worker write (see writeAndAppendChange), so delivery happens iff
// the write committed.
//
// The cursor is (xid, seq), not seq alone: seq is allocated at INSERT but
// the row only becomes visible at COMMIT, so a seq cursor can move past a
// still-in-flight lower seq and lose its event permanently. Each poll only
// consumes rows whose xid is older than every in-flight transaction
// (pg_snapshot_xmin), so nothing visible can ever appear behind the cursor.
// A row is therefore held back for at most one in-flight-writer lifetime
// plus a poll interval — well inside the <=1s delivery target. Events are
// delivered in xid order, which can reorder updates to the same worker
// across concurrent transactions; consumers reconcile by worker version
// (see workercache.applyEvent).
//
// A watcher that lags the janitor's age-based retention
// (changeFeedRetentionAge) may have had unconsumed rows trimmed; rather
// than skip them silently, it closes the event channel, which consumers
// already treat as a resync-and-relist signal (see workercache.watchEvents).
func (p *Persistence) WatchWorkers(ctx context.Context) (*store.WorkerWatch, error) {
	watchCtx, cancel := context.WithCancel(ctx)

	// Start at (xmin, 0): everything committed before every in-flight
	// transaction is history; anything in flight at subscribe time has
	// xid >= xmin and is delivered once it clears the fence. baselineSeq
	// records where the feed (and any past trimming) ended at subscribe
	// time, so pre-subscribe trims are never mistaken for lost events.
	var cursorXidText string
	var baselineSeq int64
	if err := p.pool.QueryRow(watchCtx, `
		SELECT pg_snapshot_xmin(pg_current_snapshot())::text,
		       GREATEST(COALESCE(max(seq), 0), COALESCE((SELECT seq FROM worker_changes_trim), 0))
		FROM worker_changes`).Scan(&cursorXidText, &baselineSeq); err != nil {
		cancel()
		return nil, fmt.Errorf("reading worker change feed cursor: %w", err)
	}
	cursorXid, err := strconv.ParseUint(cursorXidText, 10, 64)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("parsing worker change feed cursor xid %q: %w", cursorXidText, err)
	}
	var cursorSeq int64

	ch := make(chan store.WorkerEvent, 128)
	go func() {
		defer close(ch)
		ticker := time.NewTicker(changeFeedPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-watchCtx.Done():
				return
			case <-ticker.C:
			}
			// Drain until a poll comes back short: a full batch means more
			// rows are waiting, and sleeping a tick between full batches
			// would cap delivery at changeFeedBatch per interval — no
			// headroom over the watch throughput target, so any burst
			// would open a lag the watcher could never close.
			for drained := false; !drained; {
				rows, err := p.pool.Query(watchCtx, `
					SELECT seq, xid::text, payload FROM worker_changes
					WHERE (xid, seq) > ($1::xid8, $2)
					  AND xid < pg_snapshot_xmin(pg_current_snapshot())
					ORDER BY xid, seq LIMIT $3`,
					strconv.FormatUint(cursorXid, 10), cursorSeq, changeFeedBatch)
				if err != nil {
					if watchCtx.Err() != nil {
						return
					}
					// Transient poll failure: keep the cursor, try again on
					// the next tick.
					slog.WarnContext(watchCtx, "worker change feed poll failed", slog.Any("err", err))
					break
				}
				type feedRow struct {
					seq     int64
					xid     uint64
					payload []byte
				}
				var batch []feedRow
				for rows.Next() {
					var r feedRow
					var xidText string
					if err := rows.Scan(&r.seq, &xidText, &r.payload); err != nil {
						rows.Close()
						slog.WarnContext(watchCtx, "worker change feed scan failed", slog.Any("err", err))
						batch = nil
						break
					}
					if r.xid, err = strconv.ParseUint(xidText, 10, 64); err != nil {
						rows.Close()
						slog.WarnContext(watchCtx, "worker change feed xid parse failed", slog.Any("err", err))
						batch = nil
						break
					}
					batch = append(batch, r)
				}
				rows.Close()
				drained = len(batch) < changeFeedBatch

				// Retention safety: if the janitor's recorded trim high-water
				// mark is ahead of everything this watcher has seen, a row it
				// never consumed was deleted. Close before delivering anything
				// past the gap; consumers resync with a full relist.
				var trimmed int64
				if err := p.pool.QueryRow(watchCtx, `SELECT COALESCE((SELECT seq FROM worker_changes_trim), 0)`).Scan(&trimmed); err != nil {
					if watchCtx.Err() != nil {
						return
					}
					slog.WarnContext(watchCtx, "worker change feed trim check failed", slog.Any("err", err))
					break
				}
				if trimmed > max(cursorSeq, baselineSeq) {
					slog.WarnContext(watchCtx, "worker watch fell behind change feed retention; closing for resync",
						slog.Int64("cursor_seq", cursorSeq), slog.Int64("trimmed_through", trimmed))
					return
				}

				for _, r := range batch {
					event, err := unmarshalWorkerEvent(r.payload)
					if err != nil {
						slog.ErrorContext(watchCtx, "worker event unmarshal failed", slog.Any("err", err))
						cursorXid, cursorSeq = r.xid, r.seq
						continue
					}
					select {
					case ch <- event:
						cursorXid, cursorSeq = r.xid, r.seq
					case <-watchCtx.Done():
						return
					}
				}
			}
		}
	}()
	return store.NewWorkerWatch(ch, cancel), nil
}

// --- Workflow locks ---

// defaultLockTTL is how long a lock may go unrenewed before another client
// can reclaim it.
const defaultLockTTL = 30 * time.Second

func (p *Persistence) AcquireLock(ctx context.Context, key string) (*store.Lock, error) {
	ttl := p.lockTTL
	token := uuid.NewString()

	acquired, err := p.acquireLease(ctx, key, token, ttl)
	if err != nil {
		return nil, err
	}
	if !acquired {
		return nil, store.ErrLockConflict
	}

	leaseCtx, cancel := context.WithCancel(ctx)
	renewalDone := make(chan struct{})
	go func() {
		defer close(renewalDone)
		defer cancel()
		p.renewLockLoop(leaseCtx, key, token, ttl)
	}()

	closeFn := func() {
		cancel()
		<-renewalDone

		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer releaseCancel()
		if err := p.releaseLease(releaseCtx, key, token); err != nil {
			slog.WarnContext(releaseCtx, "failed to release PostgreSQL lock, relying on TTL to reclaim it", "key", key, "error", err)
		}
	}
	return store.NewLock(leaseCtx, closeFn), nil
}

func (p *Persistence) acquireLease(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	var returnedKey string
	err := p.pool.QueryRow(ctx, `
		INSERT INTO leases (key, token, expires_at)
		VALUES ($1, $2, clock_timestamp() + make_interval(secs => $3))
		ON CONFLICT (key) DO UPDATE
		SET token = EXCLUDED.token,
		    expires_at = EXCLUDED.expires_at
		WHERE leases.expires_at <= clock_timestamp()
		RETURNING key`, key, token, ttl.Seconds()).Scan(&returnedKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("acquiring lock for %q: %w", key, err)
	}
	return true, nil
}

const (
	renewIntervalDivisor    = 3
	renewRetryPeriodDivisor = 10
	renewDeadlineFraction   = 2.0 / 3.0
)

func (p *Persistence) renewLockLoop(ctx context.Context, key, token string, ttl time.Duration) {
	interval := ttl / renewIntervalDivisor
	renewDeadline := time.Duration(float64(ttl) * renewDeadlineFraction)

	lastRenewed := time.Now()
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			renewCtx, cancel := context.WithDeadline(ctx, lastRenewed.Add(renewDeadline))
			renewed := p.tryRenewLease(renewCtx, key, token, ttl)
			cancel()
			if !renewed {
				return
			}
			lastRenewed = time.Now()
			timer.Reset(interval)
		}
	}
}

func (p *Persistence) tryRenewLease(ctx context.Context, key, token string, ttl time.Duration) bool {
	retryPeriod := ttl / renewRetryPeriodDivisor
	retry := time.NewTimer(0)
	defer retry.Stop()

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				slog.WarnContext(ctx, "failed to renew PostgreSQL lock before its deadline", "key", key)
			}
			return false
		case <-retry.C:
			renewed, err := p.renewLease(ctx, key, token, ttl)
			if ctx.Err() != nil {
				return false
			}
			switch {
			case err == nil && renewed:
				return true
			case err == nil:
				slog.WarnContext(ctx, "PostgreSQL lock renewal found lease no longer owned", "key", key)
				return false
			default:
				slog.WarnContext(ctx, "failed to renew PostgreSQL lock, retrying", "key", key, "error", err)
				retry.Reset(retryPeriod)
			}
		}
	}
}

func (p *Persistence) renewLease(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	var returnedKey string
	err := p.pool.QueryRow(ctx, `
		UPDATE leases
		SET expires_at = clock_timestamp() + make_interval(secs => $3)
		WHERE key = $1 AND token = $2 AND expires_at > clock_timestamp()
		RETURNING key`, key, token, ttl.Seconds()).Scan(&returnedKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("renewing lock for %q: %w", key, err)
	}
	return true, nil
}

func (p *Persistence) releaseLease(ctx context.Context, key, token string) error {
	if _, err := p.pool.Exec(ctx, `DELETE FROM leases WHERE key = $1 AND token = $2`, key, token); err != nil {
		return fmt.Errorf("releasing lock for %q: %w", key, err)
	}
	return nil
}

// --- Debug ---

func (p *Persistence) DebugClearAll(ctx context.Context) error {
	if _, err := p.pool.Exec(ctx, `TRUNCATE atespaces, actors, actor_snapshots, actor_snapshot_tags, workers, leases, worker_changes, worker_changes_trim`); err != nil {
		return fmt.Errorf("truncating tables: %w", err)
	}
	return nil
}
