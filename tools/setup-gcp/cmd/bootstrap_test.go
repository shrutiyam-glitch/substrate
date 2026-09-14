// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"slices"
	"testing"
)

func stepNames(cfg *Config) []string {
	steps := bootstrapSteps(cfg)
	names := make([]string, 0, len(steps))
	for _, s := range steps {
		names = append(names, s.name)
	}
	return names
}

const cloudSQLStep = "Creating Cloud SQL instance for the ateapi store"

// Cloud SQL bills by the hour and needs private services access on the VPC, so
// a bootstrap that did not ask for it must not provision one.
func TestBootstrapStepsOmitCloudSQLByDefault(t *testing.T) {
	names := stepNames(&Config{})
	if slices.Contains(names, cloudSQLStep) {
		t.Errorf("bootstrapSteps() = %v, want no Cloud SQL step", names)
	}
	if len(names) != 7 {
		t.Errorf("bootstrapSteps() has %d steps, want 7: %v", len(names), names)
	}
}

// The Workload Identity binding names the pool the cluster step enables, so
// Cloud SQL has to follow it.
func TestBootstrapStepsRunCloudSQLAfterCluster(t *testing.T) {
	names := stepNames(&Config{CloudSQLEnabled: true})

	cluster := slices.Index(names, "Creating GKE Cluster")
	cloudSQL := slices.Index(names, cloudSQLStep)
	if cluster < 0 || cloudSQL < 0 {
		t.Fatalf("bootstrapSteps() = %v, want both the cluster and Cloud SQL steps", names)
	}
	if cloudSQL != cluster+1 {
		t.Errorf("Cloud SQL step at %d, want immediately after the cluster step at %d: %v", cloudSQL, cluster, names)
	}
}

// Enabling Cloud SQL adds a step rather than replacing one: everything a plain
// bootstrap does still runs, in the same order.
func TestBootstrapStepsPreserveDefaultOrder(t *testing.T) {
	base := stepNames(&Config{})
	withCloudSQL := stepNames(&Config{CloudSQLEnabled: true})

	got := slices.DeleteFunc(withCloudSQL, func(name string) bool { return name == cloudSQLStep })
	if !slices.Equal(got, base) {
		t.Errorf("steps without the Cloud SQL entry = %v, want %v", got, base)
	}
}
