#!/bin/bash
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

# Benchmark (b) of docs/dev/atepg-test-plan.md: per-method latency vs
# throughput. For each store method, run storebench with a single-op mix at
# stepped QPS until the method saturates (unfinished requests at cutoff, or
# achieved < 98% of offered) or its p99 blows far past target — then move to
# the next method. One storebench Job per (method, qps) point.
#
# Produces $OUT_DIR/<method>-<qps>.json (+ .log) — feed the JSONs to:
#   python3 benchmarking/storebench/plot.py throughput $OUT_DIR/<method>-*.json -o <method>.png
#
# ASSUMES: the target tier dataset is already loaded (job.yaml's
# --actors/--workers/--atespaces match it) and the DB has settled. Does NOT
# load or clean. snapcreate runs LAST because it grows the dataset (~rps
# rows/s); reload the tier afterwards before further volume-ladder probes.
#
# Runtime: 6 methods x <=6 steps x ~4.5 min, minus early stops — plan for
# 2.5-3.5 h. Run it in the background and watch $OUT_DIR fill up.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"
source .ate-dev-env.sh

OUT_DIR="${OUT_DIR:-bench-results/sweep-$(date +%Y%m%d-%H%M)}"
JOB_YAML=benchmarking/storebench/job.yaml
NS=ate-system

# Methods in sweep order (dataset-growing snapcreate last), with the p99
# target (ms) used for the early-stop heuristic (stop past 3x target: the
# curve is already vertical there).
# Override via env: METHODS="actorupdate workerupdate" QPS_STEPS="1000 2000" ./sweep.sh
read -r -a METHODS <<< "${METHODS:-actorget workerget actorupdate workerupdate lock snapcreate}"
read -r -a QPS_STEPS <<< "${QPS_STEPS:-250 500 1000 2000 4000 8000}"

# p99 target (ms) per method (no associative arrays: macOS ships bash 3.2).
target_p99() {
  case "$1" in
    actorget|workerget|snapget) echo 5 ;;
    *) echo 10 ;;
  esac
}

WARMUP=1m
DURATION=3m
SETTLE_BETWEEN_METHODS=120 # seconds; lets WAL/vacuum from write-heavy sweeps drain

mkdir -p "${OUT_DIR}"

# set_arg <flag> <value>: rewrite "- --flag=..." in the Job manifest,
# whatever its current value.
set_arg() {
  python3 - "$1" "$2" <<'EOF'
import re, sys
flag, value = sys.argv[1], sys.argv[2]
p = "benchmarking/storebench/job.yaml"
s = open(p).read()
s2 = re.sub(rf"- --{re.escape(flag)}=\S+", f"- --{flag}={value}", s)
assert s2 != s or f"--{flag}={value}" in s, f"flag --{flag} not found"
open(p, "w").write(s2)
EOF
}

run_one() { # run_one <method> <qps> -> writes .json/.log, echoes SATURATED|OK
  local method=$1 qps=$2
  local name="${method}-${qps}"

  set_arg mix "${method}=100"
  set_arg rps "${qps}"
  set_arg duration "${DURATION}"
  set_arg warmup "${WARMUP}"
  set_arg skip-load true

  kubectl delete job -n "${NS}" storebench --ignore-not-found >/dev/null
  hack/run-tool.sh ko apply -f "${JOB_YAML}" >/dev/null 2>&1
  local completed=""
  for attempt in 1 2 3; do
    if kubectl wait --for=condition=complete "job/storebench" -n "${NS}" --timeout=600s >/dev/null 2>&1; then
      completed=yes
      break
    fi
    # Distinguish a genuinely failed Job (max-inflight abort = saturation)
    # from transient kubectl/auth/DNS failures on this machine: only give up
    # if the cluster confirms the Job failed; otherwise retry the wait.
    failed=$(kubectl get job storebench -n "${NS}"       -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null) || failed=""
    if [[ "${failed}" == "True" ]]; then
      break
    fi
    echo "    (kubectl wait hiccup, attempt ${attempt} — retrying)" >&2
    sleep 20
  done
  if [[ "${completed}" != "yes" ]]; then
    kubectl logs -n "${NS}" job/storebench --tail=20 > "${OUT_DIR}/${name}.log" 2>&1 || true
    echo "SATURATED"
    return
  fi

  kubectl logs -n "${NS}" job/storebench > "${OUT_DIR}/${name}.log" 2>&1
  kubectl get pod -n "${NS}" -l app=storebench \
    -o jsonpath='{.items[0].status.containerStatuses[0].state.terminated.message}' \
    > "${OUT_DIR}/${name}.json"

  python3 - "${OUT_DIR}/${name}.json" "$(target_p99 "${method}")" <<'EOF'
import json, sys
r = json.load(open(sys.argv[1]))
target = float(sys.argv[2])
saturated = r.get("unfinished_at_cutoff", 0) > 0 or r["achieved_rps"] < r["offered_rps"] * 0.98
p99 = max(op["p99_ms"] for op in r["ops"])
print("SATURATED" if saturated or p99 > 3 * target else "OK")
EOF
}

for method in "${METHODS[@]}"; do
  echo "########## sweeping ${method} (p99 target $(target_p99 "${method}")ms) ##########"
  for qps in "${QPS_STEPS[@]}"; do
    echo "--- ${method} @ ${qps} qps ---"
    verdict=$(run_one "${method}" "${qps}" | tail -1)
    grep -E "^(op|Actor|Worker|Snapshot|Lock|List)" "${OUT_DIR}/${method}-${qps}.log" 2>/dev/null || true
    if [[ "${verdict}" == "SATURATED" ]]; then
      echo ">>> ${method} saturated at ${qps} qps; knee is between the previous step and here."
      break
    fi
  done
  sleep "${SETTLE_BETWEEN_METHODS}"
done

echo "Sweep complete. Results in ${OUT_DIR}/"
echo "Plot per method, e.g.:"
echo "  python3 benchmarking/storebench/plot.py throughput ${OUT_DIR}/actorupdate-*.json -o actorupdate.png"
