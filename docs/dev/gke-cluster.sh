#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit -o nounset -o pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CLUSTER_NAME="${CLUSTER_NAME:-agent-substrate}"
PROJECT_ID="${PROJECT_ID:-shrutiyam-gke-dev}"
REGION="${REGION:-us-central1}"

echo "=== Step 1: Creating GKE Regional Standard cluster '${CLUSTER_NAME}' in '${PROJECT_ID}' (${REGION}) ==="
# Note: In a regional GKE cluster spanning 3 default zones, --num-nodes=1 provisions
# 1 node per zone = 3 nodes total in the cluster pool.
gcloud container clusters create "${CLUSTER_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --num-nodes=1 \
  --machine-type=e2-standard-32 \
  --workload-pool="${PROJECT_ID}.svc.id.goog" \
  --enable-kubernetes-unstable-apis=certificates.k8s.io/v1beta1/podcertificaterequests,certificates.k8s.io/v1beta1/clustertrustbundles

echo "=== Step 2: Fetching kubectl credentials ==="
gcloud container clusters get-credentials "${CLUSTER_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}"

echo "=== Step 3: Provisioning GCP resources (Bucket, Redis, IAM) ==="
cd "${ROOT}"
# Ensure .ate-dev-env.sh is configured with your PROJECT_ID, CLUSTER_NAME, CLUSTER_LOCATION=REGION, etc.
if [[ -f .ate-dev-env.sh ]]; then
  source .ate-dev-env.sh
fi
go run ./tools/setup-gcp bootstrap

echo "=== Step 4: Deploying Agent Substrate system to GKE ==="
./hack/install-ate.sh --deploy-ate-system

echo "=== Step 5: Deploying demo counter application ==="
./hack/install-ate.sh --deploy-demo-counter

echo "Done! Substrate and demo counter are deployed to '${CLUSTER_NAME}'."
