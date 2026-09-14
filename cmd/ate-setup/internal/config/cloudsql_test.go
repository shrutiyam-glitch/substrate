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

package config

import (
	"os"
	"strings"
	"testing"
)

const (
	testInstance = "ate-dev:us-central1:ate-pg"
	testGSA      = "ate-cloudsql@ate-dev.iam.gserviceaccount.com"
)

// cloudSQLEnv isolates Load the way loadEnv does, and additionally takes the
// Cloud SQL variables out of the environment rather than blanking them.
// Presence is what loadCloudSQL reads, so a blank one says "no Cloud SQL"
// where these tests mean "nothing was said".
func cloudSQLEnv(t *testing.T) {
	t.Helper()
	loadEnv(t)
	for _, name := range []string{
		"ATE_API_POSTGRES_CLOUDSQL_INSTANCE",
		"ATE_API_POSTGRES_CLOUDSQL_GSA",
		"ATE_API_POSTGRES_CLOUDSQL_IP_TYPE",
		"ATE_API_POSTGRES_CLOUDSQL_IAM_AUTH",
	} {
		// Setenv first so the test framework restores whatever was there.
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("os.Unsetenv(%s) = %v", name, err)
		}
	}
}

func TestLoadCloudSQLFromEnvironment(t *testing.T) {
	cloudSQLEnv(t)
	t.Setenv("ATE_API_POSTGRES_CLOUDSQL_INSTANCE", testInstance)
	t.Setenv("ATE_API_POSTGRES_CLOUDSQL_GSA", testGSA)

	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.CloudSQL.Enabled() {
		t.Fatalf("CloudSQL.Enabled() = false, want true")
	}
	if cfg.CloudSQL.Instance != testInstance {
		t.Errorf("CloudSQL.Instance = %q, want %q", cfg.CloudSQL.Instance, testInstance)
	}
	if cfg.CloudSQL.GSA != testGSA {
		t.Errorf("CloudSQL.GSA = %q, want %q", cfg.CloudSQL.GSA, testGSA)
	}
}

func TestLoadCloudSQLFlagsBeatEnvironment(t *testing.T) {
	cloudSQLEnv(t)
	t.Setenv("ATE_API_POSTGRES_CLOUDSQL_INSTANCE", "other:us-central1:other-pg")
	t.Setenv("ATE_API_POSTGRES_CLOUDSQL_GSA", "other@ate-dev.iam.gserviceaccount.com")

	cfg, err := Load(Options{CloudSQLInstance: testInstance, CloudSQLGSA: testGSA, CloudSQLNamed: true})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.CloudSQL.Instance != testInstance {
		t.Errorf("CloudSQL.Instance = %q, want %q", cfg.CloudSQL.Instance, testInstance)
	}
	if cfg.CloudSQL.GSA != testGSA {
		t.Errorf("CloudSQL.GSA = %q, want %q", cfg.CloudSQL.GSA, testGSA)
	}
}

// Whether the run said anything is the difference between adopting the
// cluster's Cloud SQL configuration and moving the store off Cloud SQL, so an
// empty instance has to be distinguishable from an absent one.
func TestLoadCloudSQLNamed(t *testing.T) {
	t.Run("nothing said", func(t *testing.T) {
		cloudSQLEnv(t)
		cfg, err := Load(Options{})
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cfg.CloudSQL.Named {
			t.Errorf("CloudSQL.Named = true, want false with no instance in the environment")
		}
	})

	t.Run("named empty in the environment", func(t *testing.T) {
		cloudSQLEnv(t)
		t.Setenv("ATE_API_POSTGRES_CLOUDSQL_INSTANCE", "")
		cfg, err := Load(Options{})
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if !cfg.CloudSQL.Named {
			t.Errorf("CloudSQL.Named = false, want true for an explicitly empty instance")
		}
		if cfg.CloudSQL.Enabled() {
			t.Errorf("CloudSQL.Enabled() = true, want false for an empty instance")
		}
	})

	t.Run("named empty by flag", func(t *testing.T) {
		cloudSQLEnv(t)
		t.Setenv("ATE_API_POSTGRES_CLOUDSQL_INSTANCE", testInstance)
		cfg, err := Load(Options{CloudSQLNamed: true})
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cfg.CloudSQL.Enabled() {
			t.Errorf("CloudSQL.Enabled() = true, want false: --cloudsql-instance=\"\" overrides the environment")
		}
	})
}

func TestValidateCloudSQL(t *testing.T) {
	for _, tc := range []struct {
		name    string
		env     map[string]string
		opts    Options
		wantErr string
	}{
		{
			name: "supported configuration",
			env:  map[string]string{"ATE_API_POSTGRES_CLOUDSQL_IP_TYPE": "private", "ATE_API_POSTGRES_CLOUDSQL_IAM_AUTH": "true"},
			opts: Options{CloudSQLInstance: testInstance, CloudSQLGSA: testGSA, CloudSQLNamed: true},
		},
		{
			name:    "public ip",
			env:     map[string]string{"ATE_API_POSTGRES_CLOUDSQL_IP_TYPE": "public"},
			wantErr: "ATE_API_POSTGRES_CLOUDSQL_IP_TYPE=public",
		},
		{
			name:    "password authentication",
			env:     map[string]string{"ATE_API_POSTGRES_CLOUDSQL_IAM_AUTH": "false"},
			wantErr: "ATE_API_POSTGRES_CLOUDSQL_IAM_AUTH=false",
		},
		{
			name:    "instance is not a connection name",
			opts:    Options{CloudSQLInstance: "ate-pg", CloudSQLGSA: testGSA, CloudSQLNamed: true},
			wantErr: "instance connection name",
		},
		{
			name:    "instance connection name with an empty part",
			opts:    Options{CloudSQLInstance: "ate-dev::ate-pg", CloudSQLGSA: testGSA, CloudSQLNamed: true},
			wantErr: "instance connection name",
		},
		{
			name:    "no service account",
			opts:    Options{CloudSQLInstance: testInstance, CloudSQLNamed: true},
			wantErr: "requires --cloudsql-gsa",
		},
		{
			name:    "service account is not an email",
			opts:    Options{CloudSQLInstance: testInstance, CloudSQLGSA: "ate-cloudsql", CloudSQLNamed: true},
			wantErr: "must be a service account email",
		},
		{
			name:    "connection string as well",
			env:     map[string]string{"ATE_API_POSTGRES_CONNECTION_STRING": "postgresql://someone@db.example:5432/atepg"},
			opts:    Options{CloudSQLInstance: testInstance, CloudSQLGSA: testGSA, CloudSQLNamed: true},
			wantErr: "cannot be combined",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cloudSQLEnv(t)
			for name, value := range tc.env {
				t.Setenv(name, value)
			}

			_, err := Load(tc.opts)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Load() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Load() error = nil, want one mentioning %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("Load() error = %v, want one mentioning %q", err, tc.wantErr)
			}
		})
	}
}

// The proxy owns the TLS tunnel and the IAM token, so the DSN reaching it is
// the loopback address, no password, and no TLS.
func TestCloudSQLDSN(t *testing.T) {
	cs := CloudSQL{Instance: testInstance, GSA: testGSA}
	if want := "ate-cloudsql@ate-dev.iam"; cs.DatabaseUser() != want {
		t.Errorf("DatabaseUser() = %q, want %q", cs.DatabaseUser(), want)
	}
	want := "user=ate-cloudsql@ate-dev.iam host=127.0.0.1 port=5432 dbname=atepg sslmode=disable"
	if cs.DSN() != want {
		t.Errorf("DSN() = %q, want %q", cs.DSN(), want)
	}
}
