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
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/spf13/cobra"
	servicenetworking "google.golang.org/api/servicenetworking/v1"
)

func TestCloudSQLIdentityDerivations(t *testing.T) {
	cfg := &Config{
		ProjectID:        "my-proj",
		Region:           "us-central1",
		Network:          "default",
		CloudSQLGSAName:  "ate-api-server",
		CloudSQLInstance: "atepg",
	}

	if got, want := cloudSQLGSAEmail(cfg), "ate-api-server@my-proj.iam.gserviceaccount.com"; got != want {
		t.Errorf("cloudSQLGSAEmail() = %q, want %q", got, want)
	}
	// The IAM database username is the GSA email without the
	// .gserviceaccount.com suffix — this must match what operators GRANT to
	// and what install-ate.sh synthesizes into the DSN.
	if got, want := cloudSQLDatabaseUser(cloudSQLGSAEmail(cfg)), "ate-api-server@my-proj.iam"; got != want {
		t.Errorf("cloudSQLDatabaseUser() = %q, want %q", got, want)
	}
	if got, want := workloadIdentityMember(cfg.ProjectID), "serviceAccount:my-proj.svc.id.goog[ate-system/ate-api-server]"; got != want {
		t.Errorf("workloadIdentityMember() = %q, want %q", got, want)
	}
	if got, want := privateNetworkURL(cfg), "projects/my-proj/global/networks/default"; got != want {
		t.Errorf("privateNetworkURL() = %q, want %q", got, want)
	}
}

func TestCloudSQLInstanceSpec(t *testing.T) {
	base := Config{
		ProjectID:        "my-proj",
		Region:           "us-central1",
		Network:          "default",
		CloudSQLInstance: "atepg",
		CloudSQLTier:     "db-custom-2-8192",
	}

	t.Run("enterprise defaults", func(t *testing.T) {
		cfg := base
		spec, err := cloudSQLInstanceSpec(&cfg)
		if err != nil {
			t.Fatalf("cloudSQLInstanceSpec() = %v, want nil", err)
		}
		if spec.Settings.Edition != "ENTERPRISE" {
			t.Errorf("Edition = %q, want ENTERPRISE", spec.Settings.Edition)
		}
		if spec.Settings.DataCacheConfig != nil {
			t.Error("DataCacheConfig set for enterprise edition, want nil")
		}
		if spec.Settings.DataDiskSizeGb != 0 {
			t.Errorf("DataDiskSizeGb = %d, want 0 (API default)", spec.Settings.DataDiskSizeGb)
		}
		if got := spec.Settings.DatabaseFlags[0]; got.Name != "cloudsql.iam_authentication" || got.Value != "on" {
			t.Errorf("DatabaseFlags[0] = %v, want cloudsql.iam_authentication=on", got)
		}
		// The Admin API leaves these off when omitted, so the spec must ask
		// for them explicitly: this database is the control plane's state.
		if spec.Settings.BackupConfiguration == nil || !spec.Settings.BackupConfiguration.Enabled {
			t.Error("BackupConfiguration not enabled, want automated backups on")
		} else if !spec.Settings.BackupConfiguration.PointInTimeRecoveryEnabled {
			t.Error("PointInTimeRecoveryEnabled = false, want true")
		}
		if !spec.Settings.DeletionProtectionEnabled {
			t.Error("DeletionProtectionEnabled = false, want true")
		}
	})

	t.Run("enterprise-plus enables data cache", func(t *testing.T) {
		cfg := base
		cfg.CloudSQLEdition = "enterprise-plus"
		cfg.CloudSQLTier = "db-perf-optimized-N-16"
		cfg.CloudSQLStorageGB = 5000
		spec, err := cloudSQLInstanceSpec(&cfg)
		if err != nil {
			t.Fatalf("cloudSQLInstanceSpec() = %v, want nil", err)
		}
		if spec.Settings.Edition != "ENTERPRISE_PLUS" {
			t.Errorf("Edition = %q, want ENTERPRISE_PLUS", spec.Settings.Edition)
		}
		if spec.Settings.DataCacheConfig == nil || !spec.Settings.DataCacheConfig.DataCacheEnabled {
			t.Error("DataCacheConfig not enabled for enterprise-plus")
		}
		if spec.Settings.DataDiskSizeGb != 5000 {
			t.Errorf("DataDiskSizeGb = %d, want 5000", spec.Settings.DataDiskSizeGb)
		}
	})

	t.Run("unknown edition rejected", func(t *testing.T) {
		cfg := base
		cfg.CloudSQLEdition = "hyperscale"
		if _, err := cloudSQLInstanceSpec(&cfg); err == nil {
			t.Error("cloudSQLInstanceSpec() = nil error for unknown edition, want error naming --edition")
		}
	})
}

func TestGetEnvInt64(t *testing.T) {
	const key = "TEST_ENV_INT64_VAR"
	t.Setenv(key, "1234")
	if got := getEnv(key, int64(0)); got != 1234 {
		t.Errorf("getEnv(%q, 0) = %d, want 1234", key, got)
	}
	t.Setenv(key, "not-a-number")
	if got := getEnv(key, int64(7)); got != 7 {
		t.Errorf("getEnv(%q, 7) with junk value = %d, want fallback 7", key, got)
	}
}

func TestAddProjectIamBindingCloudSQLRoles(t *testing.T) {
	// The cloudsql roles reuse addProjectIamBinding; verify idempotency for
	// the two roles this command grants.
	policy := &iampb.Policy{}
	member := "serviceAccount:ate-api-server@my-proj.iam.gserviceaccount.com"
	for _, role := range []string{"roles/cloudsql.client", "roles/cloudsql.instanceUser"} {
		if !addProjectIamBinding(policy, role, member) {
			t.Errorf("addProjectIamBinding(%q) = false on first add, want true", role)
		}
		if addProjectIamBinding(policy, role, member) {
			t.Errorf("addProjectIamBinding(%q) = true on repeat add, want false", role)
		}
	}
}

// registerCloudSQLFlags serves both spellings: `create cloudsql --tier` and
// `bootstrap --cloudsql-tier`, writing to the same Config fields either way.
func TestRegisterCloudSQLFlags(t *testing.T) {
	settings := []string{"instance", "tier", "edition", "storage-size", "gsa-name"}

	for _, prefix := range []string{"", "cloudsql-"} {
		cmd := &cobra.Command{Use: "test"}
		registerCloudSQLFlags(cmd, prefix)

		for _, name := range settings {
			if cmd.Flags().Lookup(prefix+name) == nil {
				t.Errorf("registerCloudSQLFlags(%q) did not register --%s%s", prefix, prefix, name)
			}
		}
		// Nothing shared with the cluster's own flags: bootstrap registers
		// --machine-type and --cluster-version next to these.
		if prefix != "" {
			for _, name := range settings {
				if cmd.Flags().Lookup(name) != nil {
					t.Errorf("registerCloudSQLFlags(%q) also registered the unprefixed --%s", prefix, name)
				}
			}
		}
	}

	// The defaults land in the shared Config the commands provision from.
	cmd := &cobra.Command{Use: "test"}
	registerCloudSQLFlags(cmd, "cloudsql-")
	if err := cmd.Flags().Parse([]string{"--cloudsql-instance=other", "--cloudsql-storage-size=250"}); err != nil {
		t.Fatalf("Parse() = %v", err)
	}
	if cfg.CloudSQLInstance != "other" {
		t.Errorf("cfg.CloudSQLInstance = %q, want %q", cfg.CloudSQLInstance, "other")
	}
	if cfg.CloudSQLStorageGB != 250 {
		t.Errorf("cfg.CloudSQLStorageGB = %d, want 250", cfg.CloudSQLStorageGB)
	}
}

func TestHasReservedPeeringRange(t *testing.T) {
	tests := []struct {
		name  string
		conns []*servicenetworking.Connection
		want  bool
	}{
		{"no connections", nil, false},
		{
			"connection without a range",
			[]*servicenetworking.Connection{{}},
			false,
		},
		{
			"connection with a range",
			[]*servicenetworking.Connection{{ReservedPeeringRanges: []string{"google-managed-services-default"}}},
			true,
		},
		{
			// The range is shared by every service producer on the VPC, so a
			// peering listed second satisfies Cloud SQL just as well.
			"range on a later connection",
			[]*servicenetworking.Connection{
				{},
				{ReservedPeeringRanges: []string{"google-managed-services-default"}},
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasReservedPeeringRange(tt.conns); got != tt.want {
				t.Errorf("hasReservedPeeringRange() = %v, want %v", got, tt.want)
			}
		})
	}
}
