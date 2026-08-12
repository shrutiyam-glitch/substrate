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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// dataset describes the deterministic standing dataset. Names are pure
// functions of the index so the run phase can address any row without
// listing: actor i lives in atespace i%atespaces and has one snapshot with
// the actor's name; worker i lives in pool i%pools.
type dataset struct {
	actors    int
	workers   int
	atespaces int
	pools     int

	// Marshaled-size targets for copy-mode loads (0 = natural size).
	actorBytes    int
	workerBytes   int
	snapshotBytes int
}

const (
	atespacePrefix    = "sbench"
	workerNamespace   = "storebench"
	templateNamespace = "storebench"
	templateName      = "synthetic"
)

// Name widths: fixed-width zero padding keeps names lexicographically
// ordered, which the copy loader's resume-from-max depends on. Widths cover
// the XL tier (100M actors, 1M workers, 100k atespaces) with headroom.
// Changing widths orphans previously-loaded datasets — reload after upgrades.
func (d *dataset) atespaceName(i int) string { return fmt.Sprintf("%s-%06d", atespacePrefix, i) }
func (d *dataset) actorName(i int) string    { return fmt.Sprintf("actor-%09d", i) }
func (d *dataset) actorRef(i int) resources.ActorRef {
	return resources.ActorRef{Atespace: d.atespaceName(i % d.atespaces), Name: d.actorName(i)}
}
func (d *dataset) poolName(i int) string   { return fmt.Sprintf("pool-%02d", i%d.pools) }
func (d *dataset) workerPod(i int) string  { return fmt.Sprintf("worker-%08d", i) }
func (d *dataset) snapLocation() string    { return "gs://storebench-fake/snapshot" }

func benchActor(atespace, name string) *ateapipb.Actor {
	return &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
		ActorTemplateNamespace: templateNamespace,
		ActorTemplateName:      templateName,
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"bench": "storebench"},
		},
	}
}

func benchSnapshot(atespace, name string) *ateapipb.ActorSnapshot {
	return &ateapipb.ActorSnapshot{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
		SourceActor:            &ateapipb.ObjectRef{Atespace: atespace, Name: name},
		ActorTemplateNamespace: templateNamespace,
		ActorTemplateName:      templateName,
	}
}

func benchWorker(ns, pool, pod string, i int) *ateapipb.Worker {
	return &ateapipb.Worker{
		WorkerNamespace: ns,
		WorkerPool:      pool,
		WorkerPod:       pod,
		Ip:              fmt.Sprintf("10.250.%d.%d", (i/250)%250, i%250+1),
		Labels:          map[string]string{"bench": "storebench"},
	}
}

// load idempotently creates the dataset (ErrAlreadyExists is success, so an
// interrupted load resumes where it left off).
func load(ctx context.Context, st store.Interface, ds *dataset, concurrency int) error {
	for i := 0; i < ds.atespaces; i++ {
		_, err := st.CreateAtespace(ctx, &ateapipb.Atespace{
			Metadata: &ateapipb.ResourceMetadata{Name: ds.atespaceName(i)},
		})
		if err != nil && !errors.Is(err, store.ErrAlreadyExists) {
			return fmt.Errorf("creating atespace: %w", err)
		}
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	for i := 0; i < ds.actors; i++ {
		g.Go(func() error {
			ref := ds.actorRef(i)
			if _, err := st.CreateActor(gctx, benchActor(ref.Atespace, ref.Name)); err != nil && !errors.Is(err, store.ErrAlreadyExists) {
				return fmt.Errorf("creating actor %s: %w", ref, err)
			}
			if _, err := st.CreateActorSnapshot(gctx, benchSnapshot(ref.Atespace, ref.Name)); err != nil && !errors.Is(err, store.ErrAlreadyExists) {
				return fmt.Errorf("creating snapshot %s: %w", ref, err)
			}
			return nil
		})
	}
	for i := 0; i < ds.workers; i++ {
		g.Go(func() error {
			w := benchWorker(workerNamespace, ds.poolName(i), ds.workerPod(i), i)
			if err := st.CreateWorker(gctx, w); err != nil && !errors.Is(err, store.ErrAlreadyExists) {
				return fmt.Errorf("creating worker %d: %w", i, err)
			}
			return nil
		})
	}
	return g.Wait()
}

// --- op mix ---

type opFunc func(ctx context.Context, st store.Interface, ds *dataset, keys *keyPicker, rec *recorder)

var ops = map[string]opFunc{
	"actorget":     opActorGet,
	"actorupdate":  opActorUpdate,
	"workerget":    opWorkerGet,
	"workerupdate": opWorkerUpdate,
	"snapget":      opSnapGet,
	"snapcreate":   opSnapCreate,
	"actorchurn":   opActorChurn,
	"lock":         opLock,
	"list":         opList,
}

type weightedOp struct {
	name string
	fn   opFunc
	cum  int // cumulative weight for O(#ops) picking
}

type opMix struct {
	ops   []weightedOp
	total int
}

func parseMix(spec string) (*opMix, error) {
	mix := &opMix{}
	for _, part := range strings.Split(spec, ",") {
		name, weightStr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return nil, fmt.Errorf("bad mix entry %q (want op=weight)", part)
		}
		fn, known := ops[name]
		if !known {
			return nil, fmt.Errorf("unknown op %q", name)
		}
		w, err := strconv.Atoi(weightStr)
		if err != nil || w <= 0 {
			return nil, fmt.Errorf("bad weight in %q", part)
		}
		mix.total += w
		mix.ops = append(mix.ops, weightedOp{name: name, fn: fn, cum: mix.total})
	}
	if mix.total == 0 {
		return nil, fmt.Errorf("empty mix")
	}
	return mix, nil
}

func (m *opMix) pick(r int) weightedOp {
	n := r % m.total
	for _, op := range m.ops {
		if n < op.cum {
			return op
		}
	}
	return m.ops[len(m.ops)-1]
}

// keyPicker picks dataset indices, uniformly or Zipf-skewed (s=1.1). All
// methods are safe for concurrent use.
type keyPicker struct {
	mu        sync.Mutex
	rng       *rand.Rand
	actorZipf *rand.Zipf
	workZipf  *rand.Zipf
	ds        *dataset
}

func newKeyPicker(ds *dataset, zipf bool) *keyPicker {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	kp := &keyPicker{rng: rng, ds: ds}
	if zipf {
		kp.actorZipf = rand.NewZipf(rng, 1.1, 1, uint64(ds.actors-1))
		kp.workZipf = rand.NewZipf(rng, 1.1, 1, uint64(ds.workers-1))
	}
	return kp
}

func (k *keyPicker) actor() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.actorZipf != nil {
		return int(k.actorZipf.Uint64())
	}
	return k.rng.Intn(k.ds.actors)
}

func (k *keyPicker) worker() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.workZipf != nil {
		return int(k.workZipf.Uint64())
	}
	return k.rng.Intn(k.ds.workers)
}

func (k *keyPicker) intn(n int) int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.rng.Intn(n)
}

// --- ops ---
//
// Timed sections cover exactly one store call each (the quantity with a p99
// target); any read needed to obtain a CAS version is untimed setup.

func opActorGet(ctx context.Context, st store.Interface, ds *dataset, keys *keyPicker, rec *recorder) {
	ref := ds.actorRef(keys.actor())
	start := time.Now()
	_, err := st.GetActor(ctx, ref)
	rec.record("ActorGet", start, err)
}

func opActorUpdate(ctx context.Context, st store.Interface, ds *dataset, keys *keyPicker, rec *recorder) {
	ref := ds.actorRef(keys.actor())
	// The interface is now a server-side read-modify-write: the timed call
	// includes the transactional locked read, the mutation, and the write.
	start := time.Now()
	_, err := st.UpdateActor(ctx, ref, func(dbActor *ateapipb.Actor) error {
		dbActor.WorkerSelector = &ateapipb.Selector{
			MatchLabels: map[string]string{"bench": uuid.NewString()},
		}
		return nil
	})
	rec.record("ActorUpdate", start, err)
}

func opWorkerGet(ctx context.Context, st store.Interface, ds *dataset, keys *keyPicker, rec *recorder) {
	i := keys.worker()
	start := time.Now()
	_, err := st.GetWorker(ctx, workerNamespace, ds.poolName(i), ds.workerPod(i))
	rec.record("WorkerGet", start, err)
}

func opWorkerUpdate(ctx context.Context, st store.Interface, ds *dataset, keys *keyPicker, rec *recorder) {
	i := keys.worker()
	current, err := st.GetWorker(ctx, workerNamespace, ds.poolName(i), ds.workerPod(i))
	if err != nil {
		rec.recordSetupFailure("WorkerUpdate")
		return
	}
	updated := proto.Clone(current).(*ateapipb.Worker)
	if updated.Labels == nil {
		updated.Labels = map[string]string{}
	}
	updated.Labels["bench"] = uuid.NewString()
	start := time.Now()
	err = st.UpdateWorker(ctx, updated, current.GetVersion())
	rec.record("WorkerUpdate", start, err)
}

func opSnapGet(ctx context.Context, st store.Interface, ds *dataset, keys *keyPicker, rec *recorder) {
	ref := ds.actorRef(keys.actor())
	start := time.Now()
	_, err := st.GetActorSnapshot(ctx, ref.Atespace, ref.Name)
	rec.record("SnapshotGet", start, err)
}

func opSnapCreate(ctx context.Context, st store.Interface, ds *dataset, keys *keyPicker, rec *recorder) {
	atespace := ds.atespaceName(keys.intn(ds.atespaces))
	name := "snap-" + uuid.NewString()
	start := time.Now()
	_, err := st.CreateActorSnapshot(ctx, benchSnapshot(atespace, name))
	rec.record("SnapshotCreate", start, err)
}

// opActorChurn creates a throwaway actor and deletes it, timing each half.
// Created directly in STATUS_DELETING to satisfy DeleteActor's precondition.
// This is the op that exercises atepg's actors->atespaces foreign key.
func opActorChurn(ctx context.Context, st store.Interface, ds *dataset, keys *keyPicker, rec *recorder) {
	atespace := ds.atespaceName(keys.intn(ds.atespaces))
	name := "churn-" + uuid.NewString()
	actor := benchActor(atespace, name)
	actor.Status = ateapipb.Actor_STATUS_DELETING

	start := time.Now()
	_, err := st.CreateActor(ctx, actor)
	rec.record("ActorCreate", start, err)
	if err != nil {
		return
	}
	start = time.Now()
	_, err = st.DeleteActor(ctx, resources.ActorRef{Atespace: atespace, Name: name})
	rec.record("ActorDelete", start, err)
}

func opLock(ctx context.Context, st store.Interface, ds *dataset, keys *keyPicker, rec *recorder) {
	key := "lock:storebench:" + uuid.NewString()
	start := time.Now()
	lock, err := st.AcquireLock(ctx, key)
	rec.record("LockAcquire", start, err)
	if err == nil {
		lock.Close()
	}
}

func opList(ctx context.Context, st store.Interface, ds *dataset, keys *keyPicker, rec *recorder) {
	atespace := ds.atespaceName(keys.intn(ds.atespaces))
	start := time.Now()
	_, _, err := st.ListActors(ctx, atespace, 1000, "")
	rec.record("ListActors", start, err)
}

// --- recorder ---

type opStats struct {
	mu            sync.Mutex
	latenciesUS   []int64
	errors        int64
	conflicts     int64
	setupFailures int64
}

type recorder struct {
	mu        sync.Mutex
	stats     map[string]*opStats
	measuring atomic.Bool // false during warmup
}

func newRecorder() *recorder {
	return &recorder{stats: map[string]*opStats{}}
}

func (r *recorder) statsFor(op string) *opStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.stats[op]
	if !ok {
		s = &opStats{latenciesUS: make([]int64, 0, 1<<20)}
		r.stats[op] = s
	}
	return s
}

func (r *recorder) record(op string, start time.Time, err error) {
	elapsed := time.Since(start)
	outcome := "ok"
	switch {
	case err == nil:
	case errors.Is(err, store.ErrVersionConflict), errors.Is(err, store.ErrLockConflict):
		outcome = "conflict"
	default:
		outcome = "error"
	}
	observeOp(op, elapsed, outcome) // live time series: includes warmup

	if !r.measuring.Load() {
		return
	}
	s := r.statsFor(op)
	s.mu.Lock()
	defer s.mu.Unlock()
	switch outcome {
	case "ok":
		s.latenciesUS = append(s.latenciesUS, elapsed.Microseconds())
	case "conflict":
		// Expected under contention (especially --key-dist=zipf): reported
		// separately, not as an error and not as a latency sample.
		s.conflicts++
	default:
		s.errors++
	}
}

func (r *recorder) recordSetupFailure(op string) {
	observeOp(op, 0, "setup_failure")
	if !r.measuring.Load() {
		return
	}
	s := r.statsFor(op)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setupFailures++
}

// --- open-loop pacer ---

type runConfig struct {
	rps         int
	duration    time.Duration
	warmup      time.Duration
	mix         *opMix
	zipf        bool
	maxInflight int
}

// run fires cfg.rps requests per second for warmup+duration. Arrivals are
// scheduled in 5ms batches with a fractional accumulator, launched
// regardless of how many earlier requests are still in flight (open loop).
func run(ctx context.Context, st store.Interface, ds *dataset, cfg runConfig) (*report, error) {
	keys := newKeyPicker(ds, cfg.zipf)
	rec := newRecorder()

	var inflight, launched, completed atomic.Int64
	var wg sync.WaitGroup

	const tick = 5 * time.Millisecond
	perTickNum := cfg.rps * int(tick) // fractional accumulator, denominator 1s
	accum := 0

	total := cfg.warmup + cfg.duration
	deadline := time.Now().Add(total)
	warmupEnd := time.Now().Add(cfg.warmup)
	if cfg.warmup == 0 {
		rec.measuring.Store(true)
	}

	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	var measuredLaunchStart int64

	for now := time.Now(); now.Before(deadline); {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case now = <-ticker.C:
		}
		if !rec.measuring.Load() && now.After(warmupEnd) {
			rec.measuring.Store(true)
			measuredLaunchStart = launched.Load()
		}
		backlog := inflight.Load()
		metricInflight.Set(float64(backlog))
		if backlog > int64(cfg.maxInflight) {
			return nil, fmt.Errorf("aborting: %d requests in flight (> --max-inflight=%d); the backend is saturated at %d rps", backlog, cfg.maxInflight, cfg.rps)
		}

		accum += perTickNum
		n := accum / int(time.Second)
		accum %= int(time.Second)
		for j := 0; j < n; j++ {
			op := cfg.mix.pick(keys.intn(cfg.mix.total))
			launched.Add(1)
			inflight.Add(1)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer inflight.Add(-1)
				defer completed.Add(1)
				op.fn(ctx, st, ds, keys, rec)
			}()
		}
	}

	// Let stragglers finish (bounded) so tail samples are captured.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
	case <-ctx.Done():
	}

	measuredLaunched := launched.Load() - measuredLaunchStart
	rep := buildReport(rec, cfg, ds, measuredLaunched)
	rep.Unfinished = launched.Load() - completed.Load()
	return rep, nil
}

// --- report ---

type opReport struct {
	Op            string  `json:"op"`
	Count         int     `json:"count"`
	Errors        int64   `json:"errors"`
	Conflicts     int64   `json:"conflicts"`
	SetupFailures int64   `json:"setup_failures,omitempty"`
	P50ms         float64 `json:"p50_ms"`
	P90ms         float64 `json:"p90_ms"`
	P95ms         float64 `json:"p95_ms"`
	P99ms         float64 `json:"p99_ms"`
	P999ms        float64 `json:"p999_ms"`
	MaxMs         float64 `json:"max_ms"`
}

type report struct {
	Backend     string     `json:"backend"`
	OfferedRPS  int        `json:"offered_rps"`
	AchievedRPS float64    `json:"achieved_rps"`
	DurationSec float64    `json:"duration_sec"`
	Actors      int        `json:"dataset_actors"`
	Workers     int        `json:"dataset_workers"`
	Atespaces   int        `json:"dataset_atespaces"`
	KeyDist     string     `json:"key_dist"`
	Unfinished  int64      `json:"unfinished_at_cutoff"`
	Ops         []opReport `json:"ops"`
}

func percentileMS(sorted []int64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return float64(sorted[idx]) / 1000.0
}

func buildReport(rec *recorder, cfg runConfig, ds *dataset, measuredLaunched int64) *report {
	dist := "uniform"
	if cfg.zipf {
		dist = "zipf"
	}
	rep := &report{
		Backend:     *backend,
		OfferedRPS:  cfg.rps,
		AchievedRPS: float64(measuredLaunched) / cfg.duration.Seconds(),
		DurationSec: cfg.duration.Seconds(),
		Actors:      ds.actors,
		Workers:     ds.workers,
		Atespaces:   ds.atespaces,
		KeyDist:     dist,
	}
	var names []string
	rec.mu.Lock()
	for name := range rec.stats {
		names = append(names, name)
	}
	rec.mu.Unlock()
	sort.Strings(names)
	for _, name := range names {
		s := rec.stats[name]
		s.mu.Lock()
		lat := append([]int64(nil), s.latenciesUS...)
		or := opReport{
			Op: name, Count: len(lat),
			Errors: s.errors, Conflicts: s.conflicts, SetupFailures: s.setupFailures,
		}
		s.mu.Unlock()
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		or.P50ms = percentileMS(lat, 0.50)
		or.P90ms = percentileMS(lat, 0.90)
		or.P95ms = percentileMS(lat, 0.95)
		or.P99ms = percentileMS(lat, 0.99)
		or.P999ms = percentileMS(lat, 0.999)
		if len(lat) > 0 {
			or.MaxMs = float64(lat[len(lat)-1]) / 1000.0
		}
		rep.Ops = append(rep.Ops, or)
	}
	return rep
}

func (r *report) print(w io.Writer) {
	fmt.Fprintf(w, "\nbackend=%s offered=%d rps achieved=%.0f rps (measured window %.0fs)\n",
		r.Backend, r.OfferedRPS, r.AchievedRPS, r.DurationSec)
	fmt.Fprintf(w, "dataset: %d actors / %d workers / %d atespaces, key-dist=%s\n\n",
		r.Actors, r.Workers, r.Atespaces, r.KeyDist)
	fmt.Fprintf(w, "%-15s %10s %8s %9s %9s %9s %9s %9s %9s %9s\n",
		"op", "count", "errs", "conflicts", "p50ms", "p90ms", "p95ms", "p99ms", "p99.9ms", "maxms")
	for _, o := range r.Ops {
		fmt.Fprintf(w, "%-15s %10d %8d %9d %9.2f %9.2f %9.2f %9.2f %9.2f %9.2f\n",
			o.Op, o.Count, o.Errors, o.Conflicts, o.P50ms, o.P90ms, o.P95ms, o.P99ms, o.P999ms, o.MaxMs)
	}
	if r.Unfinished > 0 {
		fmt.Fprintf(w, "\nWARNING: %d requests unfinished at cutoff — the system did not drain the offered rate; percentiles above are survivorship-biased (queue latencies excluded). Treat as saturated.\n", r.Unfinished)
	}
	if r.AchievedRPS < float64(r.OfferedRPS)*0.98 {
		fmt.Fprintf(w, "\nWARNING: achieved rate is below offered rate; treat latencies as saturated-regime numbers.\n")
	}
}

func (r *report) writeJSON(path string) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}
