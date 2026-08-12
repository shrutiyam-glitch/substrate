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

// storebench is an open-loop benchmark for the ateapi store backends.
//
// Unlike the Locust suite (closed loop, via gRPC), storebench drives
// store.Interface directly at a fixed arrival rate: requests launch on
// schedule whether or not earlier ones have returned, so a slow backend
// accumulates in-flight work and every request records its true latency
// (no coordinated omission). This is the instrument for "p99 <= X ms at
// Y RPS" verdicts; the Locust suite remains the end-to-end comparison.
//
// It lives under cmd/ateapi (not cmd/benchmarking) because the store
// packages are internal to cmd/ateapi.
//
// Typical use, as a Job in the cluster (see benchmarking/storebench/):
//
//	storebench --backend=postgres --postgres-connection-string="$DSN" \
//	  --actors=100000 --workers=1000 --atespaces=10 \
//	  --rps=5000 --duration=10m --warmup=2m \
//	  --mix=actorget=30,actorupdate=25,workerget=15,workerupdate=15,snapget=10,snapcreate=5
//
// Phases: load (idempotent bulk create; --load-only to stop there,
// --skip-load to reuse an existing dataset), then paced run, then a
// per-op latency report (text + optional --json-out).
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/pflag"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/atepg"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/ateredis"
	"github.com/agent-substrate/substrate/internal/credbundle"
)

var (
	backend = pflag.String("backend", "", "Store backend to benchmark: redis|postgres.")

	postgresConnectionString = pflag.String("postgres-connection-string", os.Getenv("STOREBENCH_POSTGRES_DSN"),
		"PostgreSQL DSN (defaults to $STOREBENCH_POSTGRES_DSN).")

	redisClusterAddress = pflag.String("redis-cluster-address", "valkey-cluster.ate-system.svc:6379", "Redis/Valkey cluster seed address.")
	redisCACerts        = pflag.String("redis-ca-certs", "", "CA bundle for verifying the Redis/Valkey server certificate.")
	redisTLSServerName  = pflag.String("redis-tls-server-name", "", "ServerName for Redis/Valkey TLS hostname verification.")
	redisClientCert     = pflag.String("redis-client-cert", "", "Client TLS credential bundle for Redis/Valkey mTLS.")

	actors     = pflag.Int("actors", 10000, "Standing dataset: number of actors (with one snapshot each).")
	workers    = pflag.Int("workers", 100, "Standing dataset: number of workers.")
	atespaces  = pflag.Int("atespaces", 10, "Number of atespaces the actors are spread across.")
	pools      = pflag.Int("worker-pools", 2, "Number of worker pools the workers are spread across.")
	loadOnly   = pflag.Bool("load-only", false, "Only (idempotently) load the dataset, then exit.")
	skipLoad   = pflag.Bool("skip-load", false, "Assume the dataset already exists; skip the load phase.")
	loadConc   = pflag.Int("load-concurrency", 64, "Concurrent creators during the load phase.")
	loadMode   = pflag.String("load-mode", "auto", "Dataset load path: store (one CreateX per row, any backend), copy (pgx CopyFrom bulk load, postgres only, ~100x faster), auto (copy for postgres, store otherwise).")

	actorBytes    = pflag.Int("actor-bytes", 0, "Pad actor protos to ~this many marshaled bytes (0 = no padding; requirements estimate ~3000).")
	workerBytes   = pflag.Int("worker-bytes", 0, "Pad worker protos to ~this many marshaled bytes (0 = no padding; requirements estimate ~1000).")
	snapshotBytes = pflag.Int("snapshot-bytes", 0, "Pad snapshot protos to ~this many marshaled bytes (0 = no padding; requirements estimate ~1000).")
	clearFirst = pflag.Bool("clear-first", false, "DebugClearAll before loading. DESTRUCTIVE: wipes the whole store.")

	seedWorkerNamespace = pflag.String("seed-worker-namespace", "storebench", "Namespace recorded on seeded workers; set to a real namespace with WorkerPool CRs for e2e lifecycle benchmarks.")
	seedPoolPrefix      = pflag.String("seed-pool-prefix", "pool-", "Worker pool name prefix for seeded workers (pools are <prefix>NN); match real WorkerPool CR names for e2e.")
	seedTemplateNS      = pflag.String("seed-template-namespace", "storebench", "ActorTemplate namespace recorded on seeded actors; match a real Ready ActorTemplate for e2e.")
	seedTemplateName    = pflag.String("seed-template-name", "synthetic", "ActorTemplate name recorded on seeded actors.")
	seedActorPrefix     = pflag.String("seed-actor-prefix", "actor-", "Actor/snapshot name prefix; distinct prefixes let populations coexist (resume-from-max is prefix-scoped).")
	seedWorkerPrefix    = pflag.String("seed-worker-prefix", "worker-", "Worker pod-name prefix; distinct prefixes let fleets coexist.")
	seedWorkerClass     = pflag.String("seed-worker-sandbox-class", "", "SandboxClass on seeded workers; must equal the ActorTemplate's class for scheduling eligibility.")
	seedWorkerActive    = pflag.Bool("seed-worker-active", false, "Mark seeded workers STATE_ACTIVE (required for scheduling eligibility).")
	seedWorkerLabels    = pflag.String("seed-worker-labels", "", "Extra labels on seeded workers, k=v[,k=v...]; must satisfy the template's workerSelector.")

	rps      = pflag.Int("rps", 1000, "Offered arrival rate, requests/second (open loop).")
	duration = pflag.Duration("duration", 5*time.Minute, "Measured run duration (after warmup).")
	warmup   = pflag.Duration("warmup", 30*time.Second, "Warmup duration; samples discarded.")
	mixSpec  = pflag.String("mix", "actorget=30,actorupdate=25,workerget=15,workerupdate=15,snapget=10,snapcreate=5",
		"Weighted op mix. Ops: actorget, actorupdate, workerget, workerupdate, snapget, snapcreate, actorchurn, lock, list.")
	keyDist     = pflag.String("key-dist", "uniform", "Key selection distribution: uniform|zipf.")
	maxInflight = pflag.Int("max-inflight", 200000, "Abort if in-flight requests exceed this (the backend is saturated far beyond recovery).")

	jsonOut = pflag.String("json-out", "", "Optional path to also write results as JSON.")

	metricsListenAddr = pflag.String("metrics-listen-addr", ":9090",
		"Prometheus /metrics listen address for live time-series during the run; empty disables.")
)

func main() {
	pflag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveMetrics(*metricsListenAddr)
	metricOffered.Set(float64(*rps))
	workerNamespace = *seedWorkerNamespace
	poolNamePrefix = *seedPoolPrefix
	templateNamespace = *seedTemplateNS
	templateName = *seedTemplateName
	actorNamePrefix = *seedActorPrefix
	workerPodPrefix = *seedWorkerPrefix
	workerSandboxClass = *seedWorkerClass
	workerActive = *seedWorkerActive
	for _, kv := range strings.Split(*seedWorkerLabels, ",") {
		if k, v, ok := strings.Cut(kv, "="); ok {
			workerExtraLabels[k] = v
		}
	}

	st, pgPool, err := connect(ctx)
	if err != nil {
		fatal("connecting to backend", err)
	}

	ds := &dataset{
		actors:        *actors,
		workers:       *workers,
		atespaces:     *atespaces,
		pools:         *pools,
		actorBytes:    *actorBytes,
		workerBytes:   *workerBytes,
		snapshotBytes: *snapshotBytes,
	}

	if *clearFirst {
		slog.Info("Clearing store (DebugClearAll)")
		if err := st.DebugClearAll(ctx); err != nil {
			fatal("clearing store", err)
		}
	}
	if !*skipLoad {
		start := time.Now()
		useCopy := *loadMode == "copy" || (*loadMode == "auto" && pgPool != nil)
		if useCopy && pgPool == nil {
			fatal("loading dataset", fmt.Errorf("--load-mode=copy requires --backend=postgres"))
		}
		if useCopy {
			if err := copyLoad(ctx, pgPool, st, ds); err != nil {
				fatal("loading dataset (copy)", err)
			}
		} else {
			if err := load(ctx, st, ds, *loadConc); err != nil {
				fatal("loading dataset", err)
			}
		}
		slog.Info("Dataset loaded", "mode", map[bool]string{true: "copy", false: "store"}[useCopy],
			"actors", ds.actors, "workers", ds.workers,
			"atespaces", ds.atespaces, "took", time.Since(start).Round(time.Second).String())
	}
	if *loadOnly {
		return
	}

	mix, err := parseMix(*mixSpec)
	if err != nil {
		fatal("parsing --mix", err)
	}
	cfg := runConfig{
		rps:         *rps,
		duration:    *duration,
		warmup:      *warmup,
		mix:         mix,
		zipf:        *keyDist == "zipf",
		maxInflight: *maxInflight,
	}
	if *keyDist != "uniform" && *keyDist != "zipf" {
		fatal("parsing --key-dist", fmt.Errorf("want uniform|zipf, got %q", *keyDist))
	}

	slog.Info("Starting run", "backend", *backend, "rps", cfg.rps,
		"duration", cfg.duration.String(), "warmup", cfg.warmup.String(),
		"mix", *mixSpec, "key-dist", *keyDist,
		"change-feed", os.Getenv("ATEPG_CHANGE_FEED") == "1",
		"actor-change-feed", os.Getenv("ATEPG_ACTOR_CHANGE_FEED") == "1")
	report, err := run(ctx, st, ds, cfg)
	if err != nil {
		fatal("run", err)
	}

	report.print(os.Stdout)
	if *jsonOut != "" {
		if err := report.writeJSON(*jsonOut); err != nil {
			fatal("writing --json-out", err)
		}
	}
}

func connect(ctx context.Context) (store.Interface, *pgxpool.Pool, error) {
	switch *backend {
	case "postgres":
		if *postgresConnectionString == "" {
			return nil, nil, fmt.Errorf("--backend=postgres requires --postgres-connection-string or $STOREBENCH_POSTGRES_DSN")
		}
		// Build the pool here (rather than atepg.Connect) so its Stat() can
		// be exported: pool exhaustion masquerades as backend latency, and
		// these metrics are how we tell the two apart.
		pool, err := pgxpool.New(ctx, *postgresConnectionString)
		if err != nil {
			return nil, nil, fmt.Errorf("opening PostgreSQL pool: %w", err)
		}
		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			return nil, nil, fmt.Errorf("pinging PostgreSQL: %w", err)
		}
		registerPoolMetrics(pool)
		p, err := atepg.NewPersistence(ctx, pool)
		if err != nil {
			return nil, nil, err
		}
		return p, pool, nil
	case "redis":
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13}
		if *redisCACerts != "" {
			ca, err := os.ReadFile(*redisCACerts)
			if err != nil {
				return nil, nil, fmt.Errorf("reading Redis CA cert: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(ca) {
				return nil, nil, fmt.Errorf("parsing Redis CA cert from %s", *redisCACerts)
			}
			tlsConfig.RootCAs = pool
		}
		if *redisTLSServerName != "" {
			tlsConfig.ServerName = *redisTLSServerName
		}
		if *redisClientCert != "" {
			cert, err := credbundle.Parse(*redisClientCert)
			if err != nil {
				return nil, nil, fmt.Errorf("parsing Redis client cert: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{*cert}
		}
		client := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:     []string{*redisClusterAddress},
			TLSConfig: tlsConfig,
		})
		if err := client.Ping(ctx).Err(); err != nil {
			return nil, nil, fmt.Errorf("pinging Redis/Valkey: %w", err)
		}
		return ateredis.NewPersistence(client), nil, nil
	default:
		return nil, nil, fmt.Errorf("--backend must be redis or postgres, got %q", *backend)
	}
}

func fatal(what string, err error) {
	slog.Error(what, "err", err)
	os.Exit(1)
}
