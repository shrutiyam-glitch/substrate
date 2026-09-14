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
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
)

func testCloudSQL() config.CloudSQL {
	return config.CloudSQL{
		Instance: "ate-dev:us-central1:ate-pg",
		GSA:      "ate-cloudsql@ate-dev.iam.gserviceaccount.com",
	}
}

// The ConfigMap is the proxy's whole configuration, and it is also the record
// a later install reads to find out what this cluster runs, so the instance
// has to be in it. An in-cluster store writes no keys at all: server-side
// apply then removes the ones a previous Cloud SQL install left.
func TestCloudSQLEnvVars(t *testing.T) {
	if got := cloudSQLEnvVars(config.CloudSQL{}); len(got) != 0 {
		t.Errorf("cloudSQLEnvVars(disabled) = %v, want no keys", got)
	}

	got := cloudSQLEnvVars(testCloudSQL())
	if got[cloudSQLInstanceKey] != testCloudSQL().Instance {
		t.Errorf("%s = %q, want %q", cloudSQLInstanceKey, got[cloudSQLInstanceKey], testCloudSQL().Instance)
	}
	if got["CSQL_PROXY_AUTO_IAM_AUTHN"] != "true" {
		t.Errorf("CSQL_PROXY_AUTO_IAM_AUTHN = %q, want true; without it the proxy expects a password",
			got["CSQL_PROXY_AUTO_IAM_AUTHN"])
	}
	if got["CSQL_PROXY_PRIVATE_IP"] != "true" {
		t.Errorf("CSQL_PROXY_PRIVATE_IP = %q, want true; the instance has no public address",
			got["CSQL_PROXY_PRIVATE_IP"])
	}
	// The probes in the sidecar patch are hard-coded to this port.
	if got["CSQL_PROXY_HTTP_PORT"] != "9801" {
		t.Errorf("CSQL_PROXY_HTTP_PORT = %q, want 9801 to match the health probes in %v",
			got["CSQL_PROXY_HTTP_PORT"], cloudSQLProxyPatch)
	}
}
