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
	"fmt"
	"os"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"sigs.k8s.io/yaml"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

const (
	// workloadIdentityAnnotation links a Kubernetes ServiceAccount to the
	// Google service account it acts as. Cloud SQL needs this classic
	// Workload Identity form rather than the WIF-direct principal:// bindings
	// used elsewhere, because an IAM database user must be a service account.
	workloadIdentityAnnotation = "iam.gke.io/gcp-service-account"

	// cloudSQLInstanceKey names the instance in the apiserver's ConfigMap. It
	// is both the proxy's argument and the record a later install reads to
	// find out what this cluster runs.
	cloudSQLInstanceKey = "ATE_API_POSTGRES_CLOUDSQL_INSTANCE"
)

// cloudSQLProxyPatch is the strategic-merge patch that adds the proxy sidecar,
// shared verbatim with hack/install-ate.sh so the two installers produce the
// same pod.
var cloudSQLProxyPatch = []string{"cloudsql", "proxy-sidecar-patch.yaml"}

// CloudSQL returns the Cloud SQL configuration for this run, resolved once.
//
// A run that named one uses it. A run that did not adopts the cluster's, so
// that deploying from a shell without the environment variables leaves the
// store where it is instead of quietly moving it back to the bundled
// database — the same reasoning as resolve_cloudsql_instance in the shell
// installer, which this reads the same two places for.
func (e *Env) CloudSQL(ctx context.Context) (config.CloudSQL, error) {
	if e.cloudSQL != nil {
		return *e.cloudSQL, nil
	}

	cs := e.Cfg.CloudSQL
	if !cs.Named {
		adopted, err := e.adoptCloudSQL(ctx)
		if err != nil {
			return config.CloudSQL{}, err
		}
		cs = adopted
	}
	e.cloudSQL = &cs
	return cs, nil
}

// adoptCloudSQL reads back what the cluster is already configured for: the
// instance from the apiserver's ConfigMap, and the service account from the
// Workload Identity annotation on its ServiceAccount.
func (e *Env) adoptCloudSQL(ctx context.Context) (config.CloudSQL, error) {
	instance, err := e.Kube.ConfigMapValue(ctx, NamespaceAteSystem, ConfigMapAPIEnvVars, cloudSQLInstanceKey)
	if err != nil {
		return config.CloudSQL{}, err
	}
	if instance == "" {
		return config.CloudSQL{}, nil
	}

	gsa, err := e.Kube.ServiceAccountAnnotation(ctx, NamespaceAteSystem, apiServerName, workloadIdentityAnnotation)
	if err != nil {
		return config.CloudSQL{}, err
	}
	if gsa == "" {
		return config.CloudSQL{}, fmt.Errorf(
			"cluster is configured for Cloud SQL instance %s, but ServiceAccount %s/%s has no %s annotation naming the "+
				"service account to connect as; pass --cloudsql-instance and --cloudsql-gsa to say what this install should use",
			instance, NamespaceAteSystem, apiServerName, workloadIdentityAnnotation)
	}

	log.Infof("Cloud SQL config adopted from cluster: %s", instance)
	return config.CloudSQL{Instance: instance, GSA: gsa}, nil
}

// cloudSQLEnvVars is what the apiserver's ConfigMap carries for Cloud SQL: the
// instance the proxy connects to, and the proxy's own settings, which it reads
// from its environment. Empty when the store is in-cluster, which is what
// takes a previous install's keys back out again.
//
// Health checks listen on 9801 because ateapi's metrics own 9090.
func cloudSQLEnvVars(cs config.CloudSQL) map[string]string {
	if !cs.Enabled() {
		return nil
	}
	return map[string]string{
		cloudSQLInstanceKey:          cs.Instance,
		"CSQL_PROXY_AUTO_IAM_AUTHN":  "true",
		"CSQL_PROXY_PRIVATE_IP":      "true",
		"CSQL_PROXY_PORT":            "5432",
		"CSQL_PROXY_HEALTH_CHECK":    "true",
		"CSQL_PROXY_HTTP_ADDRESS":    "0.0.0.0",
		"CSQL_PROXY_HTTP_PORT":       "9801",
		"CSQL_PROXY_STRUCTURED_LOGS": "true",
	}
}

// injectCloudSQLProxy merges the sidecar patch into the Deployment.
func (e *Env) injectCloudSQLProxy(obj *unstructured.Unstructured) error {
	patchYAML, err := os.ReadFile(e.Cfg.Manifest(cloudSQLProxyPatch...))
	if err != nil {
		return fmt.Errorf("while reading the Cloud SQL proxy patch: %w", err)
	}
	patchJSON, err := yaml.YAMLToJSON(patchYAML)
	if err != nil {
		return fmt.Errorf("while parsing the Cloud SQL proxy patch: %w", err)
	}
	original, err := obj.MarshalJSON()
	if err != nil {
		return fmt.Errorf("while encoding the ate-api-server Deployment: %w", err)
	}
	merged, err := strategicpatch.StrategicMergePatch(original, patchJSON, appsv1.Deployment{})
	if err != nil {
		return fmt.Errorf("while adding the Cloud SQL proxy sidecar: %w", err)
	}
	if err := obj.UnmarshalJSON(merged); err != nil {
		return fmt.Errorf("while decoding the patched ate-api-server Deployment: %w", err)
	}
	return nil
}
