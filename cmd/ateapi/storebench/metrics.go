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
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Prometheus metrics for live time-series views (Grafana) during a run.
// Unlike the final report, these include warmup samples: the graphs show
// wall-clock time, and the report remains the warmup-filtered artifact
// with exact percentiles (histogram_quantile over these buckets is an
// approximation).
var (
	metricOpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "storebench_op_duration_seconds",
		Help:    "Latency of successful, timed store operations.",
		Buckets: prometheus.ExponentialBuckets(0.00025, 2, 15), // 0.25ms .. ~4s
	}, []string{"op", "backend"})

	metricOpOutcomes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "storebench_op_outcomes_total",
		Help: "Store operation outcomes: ok, error, conflict, setup_failure.",
	}, []string{"op", "backend", "outcome"})

	metricInflight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "storebench_inflight_requests",
		Help: "Requests launched but not yet completed (open-loop backlog).",
	})

	metricOffered = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "storebench_offered_rps",
		Help: "Configured open-loop arrival rate.",
	})
)

func observeOp(op string, elapsed time.Duration, outcome string) {
	if outcome == "ok" {
		metricOpDuration.WithLabelValues(op, *backend).Observe(elapsed.Seconds())
	}
	metricOpOutcomes.WithLabelValues(op, *backend, outcome).Inc()
}

// registerPoolMetrics exports pgxpool health. The tell for pool exhaustion:
// acquired_conns pinned at max_conns while empty_acquire_total climbs —
// requests are queueing in the pool, which inflates every op's tail latency
// while p50 stays clean.
func registerPoolMetrics(pool *pgxpool.Pool) {
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "storebench_pool_max_conns",
		Help: "Pool size cap (pool_max_conns; pgxpool default is max(4, NumCPU)).",
	}, func() float64 { return float64(pool.Stat().MaxConns()) })
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "storebench_pool_acquired_conns",
		Help: "Connections currently checked out.",
	}, func() float64 { return float64(pool.Stat().AcquiredConns()) })
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "storebench_pool_idle_conns",
		Help: "Idle connections in the pool.",
	}, func() float64 { return float64(pool.Stat().IdleConns()) })
	promauto.NewCounterFunc(prometheus.CounterOpts{
		Name: "storebench_pool_empty_acquire_total",
		Help: "Acquires that had to wait because the pool was empty.",
	}, func() float64 { return float64(pool.Stat().EmptyAcquireCount()) })
	promauto.NewCounterFunc(prometheus.CounterOpts{
		Name: "storebench_pool_acquire_wait_seconds_total",
		Help: "Cumulative time spent waiting to acquire a connection.",
	}, func() float64 { return pool.Stat().AcquireDuration().Seconds() })
}

// serveMetrics exposes /metrics on addr for the duration of the process.
func serveMetrics(addr string) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			slog.Warn("metrics server exited", "addr", addr, "err", err)
		}
	}()
}
