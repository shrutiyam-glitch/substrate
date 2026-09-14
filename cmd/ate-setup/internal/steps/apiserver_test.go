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
	"os"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
)

// Runs against the manifest that ships, so a rename in either the manifest or
// the patch surfaces here rather than as an apiserver that cannot reach its
// database.
func TestConfigureAPIServer(t *testing.T) {
	e := &Env{Cfg: &config.Config{Root: repoRoot(t)}, apiServerEnvHash: "abc123"}
	data, err := os.ReadFile(e.Cfg.Manifest("ate-api-server.yaml"))
	if err != nil {
		t.Fatalf("reading the ate-api-server manifest: %v", err)
	}
	objs, err := kube.DecodeManifestBytes(data)
	if err != nil {
		t.Fatalf("DecodeManifestBytes() = %v", err)
	}

	cs := testCloudSQL()
	if err := e.configureAPIServer(objs, cs); err != nil {
		t.Fatalf("configureAPIServer() = %v", err)
	}

	deployment := findObject(t, objs, "Deployment", apiServerName)
	initContainers, found, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "initContainers")
	if err != nil || !found {
		t.Fatalf("Deployment has no initContainers (found=%v): %v", found, err)
	}
	var names []string
	for _, c := range initContainers {
		names = append(names, c.(map[string]any)["name"].(string))
	}
	if len(names) != 1 || names[0] != "cloud-sql-proxy" {
		t.Errorf("initContainers = %v, want just cloud-sql-proxy", names)
	}

	// The patch must add to the pod, not replace it.
	containers, _, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
	if err != nil {
		t.Fatalf("reading containers: %v", err)
	}
	if len(containers) == 0 {
		t.Error("the patch left the Deployment with no containers")
	}

	annotations, _, err := unstructured.NestedStringMap(deployment.Object, "spec", "template", "metadata", "annotations")
	if err != nil {
		t.Fatalf("reading pod template annotations: %v", err)
	}
	if annotations[envHashAnnotation] != e.apiServerEnvHash {
		t.Errorf("pod template %s = %q, want %q", envHashAnnotation, annotations[envHashAnnotation], e.apiServerEnvHash)
	}

	serviceAccount := findObject(t, objs, "ServiceAccount", apiServerName)
	if got := serviceAccount.GetAnnotations()[workloadIdentityAnnotation]; got != cs.GSA {
		t.Errorf("ServiceAccount %s = %q, want %q", workloadIdentityAnnotation, got, cs.GSA)
	}
}

// An in-cluster store leaves the Deployment as the manifest ships it, apart
// from the digest that rolls the pods.
func TestConfigureAPIServerWithoutCloudSQL(t *testing.T) {
	e := &Env{Cfg: &config.Config{Root: repoRoot(t)}}
	data, err := os.ReadFile(e.Cfg.Manifest("ate-api-server.yaml"))
	if err != nil {
		t.Fatalf("reading the ate-api-server manifest: %v", err)
	}
	objs, err := kube.DecodeManifestBytes(data)
	if err != nil {
		t.Fatalf("DecodeManifestBytes() = %v", err)
	}

	if err := e.configureAPIServer(objs, config.CloudSQL{}); err != nil {
		t.Fatalf("configureAPIServer() = %v", err)
	}

	deployment := findObject(t, objs, "Deployment", apiServerName)
	if _, found, _ := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "initContainers"); found {
		t.Error("the Deployment has initContainers without a Cloud SQL instance")
	}
	serviceAccount := findObject(t, objs, "ServiceAccount", apiServerName)
	if _, ok := serviceAccount.GetAnnotations()[workloadIdentityAnnotation]; ok {
		t.Errorf("the ServiceAccount carries %s without a Cloud SQL instance", workloadIdentityAnnotation)
	}
}

// The digest has to depend on the values and not on map ordering, or a rerun
// that changed nothing would roll the pods.
func TestAPIServerEnvHash(t *testing.T) {
	first := map[string]string{"A": "1", "B": "2"}
	second := map[string]string{"B": "2", "A": "1"}
	if apiServerEnvHash(first) != apiServerEnvHash(second) {
		t.Error("apiServerEnvHash() depends on map ordering")
	}
	if apiServerEnvHash(first) == apiServerEnvHash(map[string]string{"A": "1", "B": "3"}) {
		t.Error("apiServerEnvHash() is the same for different values")
	}
	if apiServerEnvHash(map[string]string{"AB": "1"}) == apiServerEnvHash(map[string]string{"A": "B=1"}) {
		t.Error("apiServerEnvHash() confuses a key with a value")
	}
}

func findObject(t *testing.T, objs []*unstructured.Unstructured, kind, name string) *unstructured.Unstructured {
	t.Helper()
	for _, obj := range objs {
		if obj.GetKind() == kind && obj.GetName() == name {
			return obj
		}
	}
	t.Fatalf("no %s named %s in the manifest", kind, name)
	return nil
}
