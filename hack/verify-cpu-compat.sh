#!/usr/bin/env bash

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

# Verifies same-architecture (amd64) gVisor snapshot compatibility across
# heterogeneous CPU generations (cpu_features subset <-> superset) on a live
# GKE cluster:
#
#   1. Discovers (or provisions with --provision-node-pools) two amd64 nodes
#      whose gVisor cpu_features bitmasks form a strict subset/superset pair:
#        - SUBSET_NODE   (e.g. Intel Broadwell)
#        - SUPERSET_NODE (e.g. Intel Cascade Lake, where
#          (FEAT_SUPERSET & FEAT_SUBSET) == FEAT_SUBSET)
#   2. Boots a counter actor on SUBSET_NODE, increments it twice
#      (memory=2, file=2, rootfs=2), and suspends it (--mode=sync), verifying
#      the snapshot is stamped with FEAT_SUBSET.
#   3. Moves the worker to SUPERSET_NODE and wakes the actor, asserting a
#      MEMORY restore (memory=3, file=3, rootfs=3).
#   4. Suspends the actor on SUPERSET_NODE (stamping the new snapshot with
#      FEAT_SUPERSET), moves the worker back to SUBSET_NODE, and wakes the
#      actor, asserting a clean ROOTFS fallback (memory=1, file=4, rootfs=4).
#
# Prerequisites:
#   - A GKE cluster with ate-system and the counter demo (ate-demo-counter)
#     installed.
#   - Pass --provision-node-pools to automatically create Intel Broadwell
#     (subset) and Intel Cascade Lake (superset) node pools if the cluster
#     does not already have two compatible CPU tiers.
#
# Environment overrides:
#   KUBECTL_CONTEXT  kubectl context to target
#   KUBECTL_ATE_BIN  path to kubectl-ate binary (if not on PATH)
#   SUBSTRATE_ROOT   path to substrate repo checkout (optional)
#   SUBSET_NODE      explicit hostname of the subset-CPU node
#   SUPERSET_NODE    explicit hostname of the superset-CPU node

set -o errexit -o nounset -o pipefail

ROOT="${SUBSTRATE_ROOT:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"

PROVISION_NODE_POOLS="false"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --provision-node-pools)
      PROVISION_NODE_POOLS="true"
      shift
      ;;
    -h|--help)
      sed -n '17,47p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

DEMO_NS="ate-demo-counter"
DEMO_POOL="counter"
ATESPACE="ate-demo-counter"
SUFFIX="$(date +%s)"
ACTOR="cpu-compat-${SUFFIX}"
LOCAL_PROXY_PORT="${LOCAL_PROXY_PORT:-18085}"

SUBSET_POOL_NAME="${SUBSET_POOL_NAME:-cpu-subset-broadwell}"
SUPERSET_POOL_NAME="${SUPERSET_POOL_NAME:-cpu-superset-cascadelake}"
SUBSET_NODE="${SUBSET_NODE:-}"
SUPERSET_NODE="${SUPERSET_NODE:-}"

run_kubectl() { kubectl ${KUBECTL_CONTEXT:+--context="${KUBECTL_CONTEXT}"} "$@"; }
run_kubectl_ate() {
  if [[ -n "${KUBECTL_ATE_BIN:-}" ]]; then
    "${KUBECTL_ATE_BIN}" ${KUBECTL_CONTEXT:+--context="${KUBECTL_CONTEXT}"} "$@"
  elif [[ -x "${ROOT}/bin/kubectl-ate" ]]; then
    "${ROOT}/bin/kubectl-ate" ${KUBECTL_CONTEXT:+--context="${KUBECTL_CONTEXT}"} "$@"
  elif command -v kubectl-ate >/dev/null 2>&1; then
    kubectl-ate ${KUBECTL_CONTEXT:+--context="${KUBECTL_CONTEXT}"} "$@"
  elif [[ -d "${ROOT}/cmd/kubectl-ate" ]]; then
    (cd "${ROOT}" && go run ./cmd/kubectl-ate ${KUBECTL_CONTEXT:+--context="${KUBECTL_CONTEXT}"} "$@")
  else
    echo "FAIL: kubectl-ate not found; put kubectl-ate on PATH, set KUBECTL_ATE_BIN, or set SUBSTRATE_ROOT" >&2
    return 1
  fi
}

log_step() { echo; echo "[cpu-compat]: $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

ORIG_REPLICAS=""
ORIG_NODE_SELECTOR=""
ORIG_NODE_AFFINITY=""
ORIG_TOLERATIONS=""
cleanup() {
  local code=$?
  set +e
  log_step "cleanup: deleting test actor ${ACTOR} and restoring ${DEMO_NS}/${DEMO_POOL}"
  run_kubectl_ate suspend actor "${ACTOR}" -a "${ATESPACE}" --mode=sync >/dev/null 2>&1
  run_kubectl_ate delete actor "${ACTOR}" -a "${ATESPACE}" >/dev/null 2>&1
  if [[ -n "${ORIG_REPLICAS}" ]]; then
    local patch
    patch=$(python3 -c '
import json, sys
replicas = int(sys.argv[1])
ns_raw = sys.argv[2]
aff_raw = sys.argv[3]
tol_raw = sys.argv[4]
ns = json.loads(ns_raw) if ns_raw else None
aff = json.loads(aff_raw) if aff_raw else None
tol = json.loads(tol_raw) if tol_raw else None
print(json.dumps({
  "spec": {
    "replicas": replicas,
    "template": {
      "nodeSelector": ns,
      "nodeAffinity": aff,
      "tolerations": tol,
    },
  },
}))
' "${ORIG_REPLICAS}" "${ORIG_NODE_SELECTOR}" "${ORIG_NODE_AFFINITY}" "${ORIG_TOLERATIONS}")
    run_kubectl patch workerpool -n "${DEMO_NS}" "${DEMO_POOL}" --type merge -p "${patch}" >/dev/null 2>&1
    run_kubectl rollout status "deploy/${DEMO_POOL}" -n "${DEMO_NS}" --timeout=180s >/dev/null 2>&1
  fi
  exit "${code}"
}
trap cleanup EXIT

# --- Optional node-pool provisioning -----------------------------------------

provision_node_pools_if_requested() {
  if [[ "${PROVISION_NODE_POOLS}" != "true" ]]; then
    return 0
  fi
  if [[ -f "${ROOT}/.ate-dev-env.sh" ]]; then
    # shellcheck disable=SC1091
    source "${ROOT}/.ate-dev-env.sh"
  fi
  local ctx="${KUBECTL_CONTEXT:-$(kubectl config current-context)}"
  local project="${PROJECT_ID:-}"
  local location="${CLUSTER_LOCATION:-}"
  local cluster="${CLUSTER_NAME:-}"
  if [[ -z "${project}" || -z "${location}" || -z "${cluster}" ]]; then
    if [[ "${ctx}" =~ ^gke_([^_]+)_([^_]+)_(.+)$ ]]; then
      project="${project:-${BASH_REMATCH[1]}}"
      location="${location:-${BASH_REMATCH[2]}}"
      cluster="${cluster:-${BASH_REMATCH[3]}}"
    else
      fail "set PROJECT_ID, CLUSTER_LOCATION, and CLUSTER_NAME (or use a gke_<project>_<location>_<cluster> context) with --provision-node-pools"
    fi
  fi

  log_step "ensuring Broadwell (subset) and Cascade Lake (superset) node pools on ${cluster}"
  if ! gcloud container node-pools describe "${SUBSET_POOL_NAME}" \
    --project="${project}" --location="${location}" --cluster="${cluster}" >/dev/null 2>&1; then
    gcloud container node-pools create "${SUBSET_POOL_NAME}" \
      --project="${project}" \
      --location="${location}" \
      --cluster="${cluster}" \
      --num-nodes=1 \
      --machine-type=n1-standard-4 \
      --min-cpu-platform="Intel Broadwell" \
      --disk-type=pd-balanced \
      --disk-size=100 \
      --workload-metadata=GKE_METADATA
  fi
  if ! gcloud container node-pools describe "${SUPERSET_POOL_NAME}" \
    --project="${project}" --location="${location}" --cluster="${cluster}" >/dev/null 2>&1; then
    gcloud container node-pools create "${SUPERSET_POOL_NAME}" \
      --project="${project}" \
      --location="${location}" \
      --cluster="${cluster}" \
      --num-nodes=1 \
      --machine-type=n2-standard-4 \
      --min-cpu-platform="Intel Cascade Lake" \
      --disk-type=pd-balanced \
      --disk-size=100 \
      --workload-metadata=GKE_METADATA
  fi
}

# --- Helpers -----------------------------------------------------------------

# curl_actor sends one HTTP request to the actor through atenet-router.
# Uses the in-cluster bugbash-client pod if present, falling back to
# `kubectl-ate proxy actor`.
curl_actor() {
  local actor="$1"
  if run_kubectl get pod client -n bugbash-client >/dev/null 2>&1; then
    run_kubectl exec -n bugbash-client client -- curl -sS --max-time 30 -X POST \
      --cert /certs/credential-bundle.pem \
      --key /certs/credential-bundle.pem \
      --cacert /certs/router-ca.pem \
      -H "ate-target-actor: ${ATESPACE}/${actor}" \
      https://atenet-router.ate-system.svc/
    return $?
  fi
  run_kubectl_ate proxy actor "${actor}" "${LOCAL_PROXY_PORT}:80" -a "${ATESPACE}" >/dev/null 2>&1 &
  local pf_pid=$!
  local out=""
  local ok="false"
  for _ in $(seq 1 20); do
    if out=$(curl -sS --max-time 30 "http://127.0.0.1:${LOCAL_PROXY_PORT}/" 2>/dev/null) && [[ -n "${out}" ]]; then
      ok="true"
      break
    fi
    sleep 0.5
  done
  kill "${pf_pid}" 2>/dev/null || true
  wait "${pf_pid}" 2>/dev/null || true
  [[ "${ok}" == "true" ]] || return 1
  printf '%s\n' "${out}"
}

# pin_pool_to_node patches workerpool/counter to 1 replica pinned to target_node
# and waits until an ACTIVE gVisor worker on target_node is registered.
LAST_WORKER_FEAT=""
CURRENT_PINNED_NODE=""
pin_pool_to_node() {
  local target_node="$1"
  if [[ "${CURRENT_PINNED_NODE}" == "${target_node}" ]]; then
    return 0
  fi
  log_step "pinning ${DEMO_NS}/${DEMO_POOL} (1 replica) to node ${target_node}"
  local patch
  patch=$(python3 -c '
import json, sys
print(json.dumps({
  "spec": {
    "replicas": 1,
    "template": {
      "nodeSelector": {"kubernetes.io/hostname": sys.argv[1]},
      "nodeAffinity": None,
      "tolerations": [
        {"key": "ate.dev/sandboxClass", "operator": "Exists", "effect": "NoSchedule"},
      ],
    },
  },
}))
' "${target_node}")
  run_kubectl patch workerpool -n "${DEMO_NS}" "${DEMO_POOL}" --type merge -p "${patch}" >/dev/null
  run_kubectl rollout status "deploy/${DEMO_POOL}" -n "${DEMO_NS}" --timeout=180s >/dev/null

  for i in $(seq 1 30); do
    local worker_feat
    worker_feat=$(run_kubectl_ate get workers -o json 2>/dev/null | python3 -c '
import json, sys
data = json.load(sys.stdin)
target = sys.argv[1]
pool = sys.argv[2]
active = [
    w for w in data.get("workers", data.get("items", []))
    if w.get("workerPool") == pool
    and w.get("status", {}).get("state") == "WORKER_STATE_ACTIVE"
]
if len(active) != 1 or active[0].get("nodeName") != target:
    sys.exit(0)
for sc in active[0].get("status", {}).get("sandboxClasses", []):
    if sc.get("sandboxClass") != "gvisor":
        continue
    attrs = sc.get("defaultRuntime", {}).get("version", {}).get("attributes", [])
    amap = {a.get("key"): a.get("value") for a in attrs}
    if amap.get("cpu_features"):
        print(amap["cpu_features"])
        sys.exit(0)
' "${target_node}" "${DEMO_POOL}" || true)
    if [[ -n "${worker_feat}" ]]; then
      LAST_WORKER_FEAT="${worker_feat}"
      CURRENT_PINNED_NODE="${target_node}"
      echo "  active worker on ${target_node} registered with cpu_features=${worker_feat}"
      return 0
    fi
    [[ "${i}" == 30 ]] && fail "timed out waiting for ACTIVE worker on ${target_node}"
    sleep 2
  done
}

# latest_snapshot_cpu_features returns the cpu_features attribute of the actor's
# latest snapshot.
latest_snapshot_cpu_features() {
  local actor="$1"
  run_kubectl_ate get actor "${actor}" -a "${ATESPACE}" -o json 2>/dev/null | python3 -c '
import json, sys
data = json.load(sys.stdin)
snaps = data.get("status", {}).get("snapshots", [])
if not snaps:
    sys.exit(1)
latest = max(snaps, key=lambda s: int(s.get("generation", 0)))
attrs = latest.get("sandboxRuntime", {}).get("version", {}).get("attributes", [])
for a in attrs:
    if a.get("key") == "cpu_features":
        print(a.get("value", ""))
        sys.exit(0)
sys.exit(1)
'
}

# --- Preflight & Node Discovery ----------------------------------------------

provision_node_pools_if_requested

log_step "preflight: checking ${DEMO_NS}/${DEMO_POOL} and counter template"
ORIG_WP_JSON="$(run_kubectl get workerpool -n "${DEMO_NS}" "${DEMO_POOL}" -o json)"
ORIG_REPLICAS="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["spec"].get("replicas", 1))' "${ORIG_WP_JSON}")"
ORIG_NODE_SELECTOR="$(python3 -c 'import json,sys; ns=json.loads(sys.argv[1])["spec"].get("template",{}).get("nodeSelector"); print(json.dumps(ns) if ns is not None else "")' "${ORIG_WP_JSON}")"
ORIG_NODE_AFFINITY="$(python3 -c 'import json,sys; aff=json.loads(sys.argv[1])["spec"].get("template",{}).get("nodeAffinity"); print(json.dumps(aff) if aff is not None else "")' "${ORIG_WP_JSON}")"
ORIG_TOLERATIONS="$(python3 -c 'import json,sys; tol=json.loads(sys.argv[1])["spec"].get("template",{}).get("tolerations"); print(json.dumps(tol) if tol is not None else "")' "${ORIG_WP_JSON}")"

if [[ -z "${SUBSET_NODE}" || -z "${SUPERSET_NODE}" ]]; then
  CANDIDATE_NODES="$(run_kubectl get nodes -o json | python3 -c '
import collections, json, sys
data = json.load(sys.stdin)
sub_pool, sup_pool = sys.argv[1], sys.argv[2]
by_pool = collections.defaultdict(dict)
for item in data.get("items", []):
    conds = {c["type"]: c["status"] for c in item.get("status", {}).get("conditions", [])}
    if conds.get("Ready") != "True":
        continue
    labels = item.get("metadata", {}).get("labels", {})
    if labels.get("kubernetes.io/arch", "amd64") != "amd64":
        continue
    name = item["metadata"]["name"]
    pool = labels.get("cloud.google.com/gke-nodepool", labels.get("node.kubernetes.io/instance-type", name))
    zone = labels.get("topology.kubernetes.io/zone", "")
    cpu = labels.get("cloud.google.com/gke-cpu-model", "")
    key = (zone, cpu)
    if key not in by_pool[pool]:
        by_pool[pool][key] = name

def pool_sort_key(p):
    if p in (sub_pool, sup_pool):
        return (0, p)
    return (1, p)

pools = [list(by_pool[p].values()) for p in sorted(by_pool, key=pool_sort_key)]
out = []
while any(pools):
    for grp in pools:
        if grp:
            out.append(grp.pop(0))
for n in out:
    print(n)
' "${SUBSET_POOL_NAME}" "${SUPERSET_POOL_NAME}")"

  log_step "discovering node cpu_features across candidate hardware groups"
  DISCOVERED=""
  PAIR=""
  while IFS= read -r cand; do
    [[ -z "${cand}" ]] && continue
    pin_pool_to_node "${cand}"
    DISCOVERED+="${cand} ${LAST_WORKER_FEAT}"$'\n'
    PAIR=$(python3 -c '
import sys
nodes = {}
for line in sys.argv[1].splitlines():
    parts = line.split()
    if len(parts) == 2:
        nodes[parts[0]] = parts[1]
for n_sub, f_sub in nodes.items():
    v_sub = int(f_sub, 16)
    for n_sup, f_sup in nodes.items():
        v_sup = int(f_sup, 16)
        if v_sub != v_sup and (v_sup & v_sub) == v_sub:
            print(f"{n_sub} {f_sub} {n_sup} {f_sup}")
            sys.exit(0)
' "${DISCOVERED}" || true)
    [[ -n "${PAIR}" ]] && break
  done <<<"${CANDIDATE_NODES}"

  if [[ -z "${PAIR}" ]]; then
    fail "could not find two amd64 nodes where one cpu_features bitmask is a strict subset of the other; re-run with --provision-node-pools"
  fi
  read -r SUBSET_NODE FEAT_SUBSET SUPERSET_NODE FEAT_SUPERSET <<<"${PAIR}"
else
  pin_pool_to_node "${SUBSET_NODE}"
  FEAT_SUBSET="${LAST_WORKER_FEAT}"
  pin_pool_to_node "${SUPERSET_NODE}"
  FEAT_SUPERSET="${LAST_WORKER_FEAT}"
fi

python3 -c '
import sys
sub, sup = int(sys.argv[1], 16), int(sys.argv[2], 16)
assert sub != sup, "SUBSET_NODE and SUPERSET_NODE have identical cpu_features"
assert (sup & sub) == sub, "SUPERSET_NODE cpu_features is not a bitwise superset of SUBSET_NODE"
' "${FEAT_SUBSET}" "${FEAT_SUPERSET}" || fail "invalid cpu_features subset/superset relationship"

echo "  SUBSET_NODE:   ${SUBSET_NODE} (cpu_features=${FEAT_SUBSET})"
echo "  SUPERSET_NODE: ${SUPERSET_NODE} (cpu_features=${FEAT_SUPERSET})"

# --- Step 1: Boot & Snapshot on SUBSET_NODE (e.g. Intel Broadwell) -----------

pin_pool_to_node "${SUBSET_NODE}"

log_step "creating actor ${ATESPACE}/${ACTOR} on SUBSET_NODE (${SUBSET_NODE})"
run_kubectl_ate create actor "${ACTOR}" -a "${ATESPACE}" --template "${DEMO_POOL}" >/dev/null

RESP1="$(curl_actor "${ACTOR}")"
echo "  request 1 on SUBSET_NODE: ${RESP1}"
grep -q "preserved memory count: 1 | preserved file counter: 1 | preserved rootfs counter: 1" <<<"${RESP1}" \
  || fail "unexpected counter output on request 1: ${RESP1}"

RESP2="$(curl_actor "${ACTOR}")"
echo "  request 2 on SUBSET_NODE: ${RESP2}"
grep -q "preserved memory count: 2 | preserved file counter: 2 | preserved rootfs counter: 2" <<<"${RESP2}" \
  || fail "unexpected counter output on request 2: ${RESP2}"

log_step "suspending ${ACTOR} on SUBSET_NODE (--mode=sync)"
run_kubectl_ate suspend actor "${ACTOR}" -a "${ATESPACE}" --mode=sync >/dev/null
SNAP1_FEAT="$(latest_snapshot_cpu_features "${ACTOR}")"
echo "  snapshot 1 cpu_features: ${SNAP1_FEAT}"
[[ "${SNAP1_FEAT}" == "${FEAT_SUBSET}" ]] \
  || fail "snapshot 1 cpu_features (${SNAP1_FEAT}) != SUBSET_NODE (${FEAT_SUBSET})"

# --- Step 2: Resume on SUPERSET_NODE (Subset -> Superset = MEMORY) -----------

pin_pool_to_node "${SUPERSET_NODE}"

log_step "waking ${ACTOR} on SUPERSET_NODE (${SUPERSET_NODE}): expecting MEMORY restore (3, 3, 3)"
RESP3="$(curl_actor "${ACTOR}")"
echo "  request 3 on SUPERSET_NODE: ${RESP3}"
grep -q "preserved memory count: 3 | preserved file counter: 3 | preserved rootfs counter: 3" <<<"${RESP3}" \
  || fail "expected MEMORY restore (3, 3, 3) on SUPERSET_NODE, got: ${RESP3}"

ASSIGNED_NODE="$(run_kubectl_ate get actor "${ACTOR}" -a "${ATESPACE}" -o json | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"]["workerAssignment"]["nodeName"])')"
[[ "${ASSIGNED_NODE}" == "${SUPERSET_NODE}" ]] \
  || fail "expected actor on SUPERSET_NODE (${SUPERSET_NODE}), got ${ASSIGNED_NODE}"
echo "  PASS: Subset -> Superset restored at MEMORY fidelity on ${SUPERSET_NODE}"

# --- Step 3: Snapshot on SUPERSET_NODE, Resume on SUBSET_NODE (ROOTFS) -------

log_step "suspending ${ACTOR} on SUPERSET_NODE (--mode=sync)"
run_kubectl_ate suspend actor "${ACTOR}" -a "${ATESPACE}" --mode=sync >/dev/null
SNAP2_FEAT="$(latest_snapshot_cpu_features "${ACTOR}")"
echo "  snapshot 2 cpu_features: ${SNAP2_FEAT}"
[[ "${SNAP2_FEAT}" == "${FEAT_SUPERSET}" ]] \
  || fail "snapshot 2 cpu_features (${SNAP2_FEAT}) != SUPERSET_NODE (${FEAT_SUPERSET})"

pin_pool_to_node "${SUBSET_NODE}"

log_step "waking ${ACTOR} on SUBSET_NODE (${SUBSET_NODE}): expecting ROOTFS fallback (1, 4, 4)"
RESP4="$(curl_actor "${ACTOR}")"
echo "  request 4 on SUBSET_NODE: ${RESP4}"
grep -q "preserved memory count: 1 | preserved file counter: 4 | preserved rootfs counter: 4" <<<"${RESP4}" \
  || fail "expected ROOTFS fallback (1, 4, 4) on SUBSET_NODE, got: ${RESP4}"

ASSIGNED_NODE="$(run_kubectl_ate get actor "${ACTOR}" -a "${ATESPACE}" -o json | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"]["workerAssignment"]["nodeName"])')"
[[ "${ASSIGNED_NODE}" == "${SUBSET_NODE}" ]] \
  || fail "expected actor on SUBSET_NODE (${SUBSET_NODE}), got ${ASSIGNED_NODE}"
echo "  PASS: Superset -> Subset degraded cleanly to ROOTFS fidelity on ${SUBSET_NODE}"

echo
echo "PASS: gVisor cross-CPU snapshot compatibility verified:"
echo "  1. Subset (${SUBSET_NODE}) -> Superset (${SUPERSET_NODE}): MEMORY (3, 3, 3)"
echo "  2. Superset (${SUPERSET_NODE}) -> Subset (${SUBSET_NODE}): ROOTFS (1, 4, 4)"
