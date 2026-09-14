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

package steps

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
)

const (
	// apiServerName is the ate-api-server Deployment and the ServiceAccount it
	// runs as.
	apiServerName = "ate-api-server"

	// envHashAnnotation carries a digest of the apiserver's environment on its
	// pod template; see apiServerEnvHash.
	envHashAnnotation = "ate.dev/env-hash"
)

// applyManifest applies a rendered manifest, first adjusting the
// ate-api-server objects in it for where the store is.
//
// Both the sidecar and the annotations are written into the objects before
// they are applied, rather than patched onto the cluster afterwards as the
// shell installer does. That puts them under the installer's own field
// manager, so server-side apply takes them out again on an install that no
// longer wants them: there is no teardown path to write, and none to forget.
func (e *Env) applyManifest(ctx context.Context, manifest []byte) error {
	cloudSQL, err := e.CloudSQL(ctx)
	if err != nil {
		return err
	}
	objs, err := kube.DecodeManifestBytes(manifest)
	if err != nil {
		return err
	}
	if err := e.configureAPIServer(objs, cloudSQL); err != nil {
		return err
	}
	return e.Kube.Apply(ctx, objs)
}

// configureAPIServer applies to the ate-api-server objects what depends on the
// store: the Cloud SQL proxy sidecar and the Workload Identity annotation its
// credentials resolve through, and the digest that rolls the pods when the
// environment they read the store's address from changes.
func (e *Env) configureAPIServer(objs []*unstructured.Unstructured, cloudSQL config.CloudSQL) error {
	for _, obj := range objs {
		if obj.GetName() != apiServerName {
			continue
		}
		switch obj.GetKind() {
		case "Deployment":
			if cloudSQL.Enabled() {
				if err := e.injectCloudSQLProxy(obj); err != nil {
					return err
				}
			}
			if err := setEnvHashAnnotation(obj, e.apiServerEnvHash); err != nil {
				return err
			}
		case "ServiceAccount":
			if !cloudSQL.Enabled() {
				continue
			}
			annotations := obj.GetAnnotations()
			if annotations == nil {
				annotations = map[string]string{}
			}
			annotations[workloadIdentityAnnotation] = cloudSQL.GSA
			obj.SetAnnotations(annotations)
		}
	}
	return nil
}

// setEnvHashAnnotation puts the digest on the pod template, where a change to
// it is a change to the pod spec and so a rollout.
//
// Kubernetes restarts nothing when a ConfigMap or Secret pulled in with
// envFrom changes, so without this an install that moves the store to another
// database leaves the running pods talking to the old one. The hash is empty
// on paths that did not write the environment, which leaves whatever is on the
// running Deployment alone.
func setEnvHashAnnotation(obj *unstructured.Unstructured, hash string) error {
	if hash == "" {
		return nil
	}
	return unstructured.SetNestedField(obj.Object, hash,
		"spec", "template", "metadata", "annotations", envHashAnnotation)
}

// apiServerEnvHash digests the environment ate-api-server is about to be
// given. Sorted so that a rerun that changed nothing produces the same digest
// and rolls nothing, and delimited so that a key cannot be confused with the
// tail of a value.
func apiServerEnvHash(envs ...map[string]string) string {
	digest := sha256.New()
	for _, env := range envs {
		for _, key := range slices.Sorted(maps.Keys(env)) {
			fmt.Fprintf(digest, "%s=%s\n", key, env[key])
		}
	}
	return hex.EncodeToString(digest.Sum(nil))
}
