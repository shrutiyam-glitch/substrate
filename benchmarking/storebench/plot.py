# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Plot storebench results (docs/dev/atepg-test-plan.md benchmarks).

  throughput: latency-vs-throughput — one --json-out file per QPS step.
      python3 plot.py throughput sweep/*.json -o curves.png
  volume: latency-vs-volume — one file per tier, same probe load.
      python3 plot.py volume tiers/*.json -o volume.png

Options:
  --pcts p50,p99      percentile lines to draw (default p50,p90,p99)
  --label-by-dir      one series per parent directory (A/B comparisons:
                      pass files from both dirs; dir name = series label)
  --logy              log-scale latency axis (for curves spanning decades)
  --title TEXT        figure title override

Every point is annotated with its value; saturated runs (unfinished
requests at cutoff, or achieved < 98% of offered) are drawn hollow with a
'sat' tag — their latencies are queue depth, not operation cost.

Requires matplotlib.
"""

import argparse
import json
import os
import sys
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

STYLES = {"p50": "-o", "p90": "--s", "p95": "--d", "p99": ":^", "p99.9": ":v"}
KEYS = {"p50": "p50_ms", "p90": "p90_ms", "p95": "p95_ms", "p99": "p99_ms", "p99.9": "p999_ms"}


def fmt(v):
    if v < 1:
        return f"{v:.2f}"
    if v < 10:
        return f"{v:.1f}"
    return f"{v:.0f}"


def is_saturated(run):
    return (
        run.get("unfinished_at_cutoff", 0) > 0
        or run["achieved_rps"] < run["offered_rps"] * 0.98
    )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("mode", choices=["throughput", "volume"])
    p.add_argument("files", nargs="+")
    p.add_argument("-o", "--out", default="storebench.png")
    p.add_argument("--pcts", default="p50,p90,p99")
    p.add_argument("--ncols", type=int, default=0, help="panels per row (0 = all in one row)")
    p.add_argument("--label-by-dir", action="store_true")
    p.add_argument("--relabel", default="", help="rename series labels: old=new,old=new (with --label-by-dir)")
    p.add_argument("--logy", action="store_true")
    p.add_argument("--title", default=None)
    args = p.parse_args()

    pcts = [x.strip() for x in args.pcts.split(",")]
    relabel = dict(pair.split("=", 1) for pair in args.relabel.split(",") if "=" in pair)
    xkey = "achieved_rps" if args.mode == "throughput" else "dataset_actors"
    xlabel = "achieved RPS" if args.mode == "throughput" else "actors in dataset"

    # series[(op, series_label)] -> [(x, run_ops_entry, saturated)]
    series = defaultdict(list)
    ops_seen, labels_seen = [], []
    for path in args.files:
        with open(path) as f:
            run = json.load(f)
        sat = is_saturated(run)
        if sat:
            print(f"note: {os.path.basename(path)} is saturated — latencies are queue depth", file=sys.stderr)
        label = os.path.basename(os.path.dirname(os.path.abspath(path))) if args.label_by_dir else ""
        label = relabel.get(label, label)
        for op in run["ops"]:
            series[(op["op"], label)].append((run[xkey], op, sat))
            if op["op"] not in ops_seen:
                ops_seen.append(op["op"])
        if label not in labels_seen:
            labels_seen.append(label)

    ops_seen.sort()
    ncols = args.ncols if args.ncols > 0 else len(ops_seen)
    nrows = (len(ops_seen) + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(6.5 * ncols, 5.2 * nrows), squeeze=False)
    flat_axes = [ax for row in axes for ax in row]
    for ax in flat_axes[len(ops_seen):]:
        ax.set_visible(False)
    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]

    for ax, op in zip(flat_axes, ops_seen):
        for li, label in enumerate(labels_seen):
            pts = sorted(series.get((op, label), []))
            if not pts:
                continue
            xs = [pt[0] for pt in pts]
            for pi, pct in enumerate(pcts):
                ys = [pt[1][KEYS[pct]] for pt in pts]
                color = colors[(li * len(pcts) + pi) % len(colors)]
                name = f"{label} {pct}".strip()
                style = STYLES.get(pct, "-o")
                ax.plot(xs, ys, style, color=color, label=name, markersize=7, linewidth=1.8)
                # hollow markers + 'sat' tag on saturated points
                for (x, opd, sat), y in zip(pts, ys):
                    if sat:
                        ax.plot([x], [y], style[-1], color="white",
                                markeredgecolor=color, markersize=7, zorder=3)
                    tag = f"{fmt(y)}{' sat' if sat else ''}"
                    ax.annotate(tag, (x, y), textcoords="offset points",
                                xytext=(0, 7), ha="center", fontsize=8, color=color)
        ax.set_title(op, fontsize=13)
        ax.set_xlabel(xlabel)
        ax.set_ylabel("latency (ms)")
        if args.mode == "volume":
            ax.set_xscale("log")
        if args.logy:
            ax.set_yscale("log")
        ax.grid(True, which="both", alpha=0.25)
        ax.legend(fontsize=9)

    first = json.load(open(args.files[0]))
    fig.suptitle(args.title or f"backend={first['backend']}  key-dist={first['key_dist']}  "
                 f"dataset={first['dataset_actors']} actors / {first['dataset_workers']} workers",
                 fontsize=12)
    fig.tight_layout(rect=(0, 0, 1, 1 - 0.06 / max(nrows, 1)))
    fig.savefig(args.out, dpi=130)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
