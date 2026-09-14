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
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"
)

// bootstrapStep is one unit of the bootstrap sequence, named for the progress
// log it prints.
type bootstrapStep struct {
	name string
	run  func(context.Context, *Config) error
}

// bootstrapSteps returns the sequence to run, in order.
//
// Cloud SQL is opt-in: it bills by the hour and needs private services access
// on the VPC, so a plain bootstrap leaves ateapi on the in-cluster PostgreSQL
// StatefulSet the installer bundles. When it is asked for it follows the
// cluster, because its Workload Identity binding names the pool that step
// enables, and it precedes everything else because creating an instance is the
// long pole.
func bootstrapSteps(cfg *Config) []bootstrapStep {
	steps := []bootstrapStep{
		{"Enabling required APIs", enableRequiredAPIs},
		{"Creating GKE Cluster", createClusterIdempotent},
	}
	if cfg.CloudSQLEnabled {
		steps = append(steps, bootstrapStep{"Creating Cloud SQL instance for the ateapi store", provisionCloudSQL})
	}
	return append(steps,
		bootstrapStep{"Creating GCS Bucket for snapshots", createSnapshotBucket},
		bootstrapStep{"Granting GKE Node permissions", grantGkeNodePermissions},
		bootstrapStep{"Granting Atelet permissions", grantAteletPermissions},
		bootstrapStep{"Creating IAM policy bindings for bucket", createIamPolicyBindings},
		bootstrapStep{"Creating Monitoring Dashboards", createMonitoringDashboards},
	)
}

var bootstrapCmd = &cobra.Command{
	Use:   "bootstrap",
	Short: "Fully bootstrap the GCP environment",
	Long: `Runs all setup steps in order: enable APIs, create cluster, create bucket, grant IAM permissions, and create dashboards.

Pass --cloudsql to also provision the Cloud SQL PostgreSQL instance that backs
the ateapi store, in --region. Without it, ateapi runs against the in-cluster
PostgreSQL StatefulSet the installer bundles. See cloud-sql.md.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		if err := resolveProjectID(ctx, &cfg); err != nil {
			return err
		}
		if cfg.BucketName == "" {
			return errors.New("--bucket-name is required")
		}
		// Settings are validated up front because the Cloud SQL step runs
		// after the cluster: left to the step itself, a rejected
		// --cloudsql-edition would surface once a cluster already exists.
		if cfg.CloudSQLEnabled {
			if _, err := cloudSQLInstanceSpec(&cfg); err != nil {
				return err
			}
		}

		slog.Info("Starting full bootstrap...")

		// The Cloud SQL instance is created in --region, and the cluster step
		// rejects a --cluster-location outside it, so the two cannot land in
		// different regions without an error first.
		steps := bootstrapSteps(&cfg)
		for i, step := range steps {
			slog.Info(fmt.Sprintf("Step %d/%d: %s...", i+1, len(steps), step.name))
			if err := step.run(ctx, &cfg); err != nil {
				return err
			}
		}

		slog.Info("Bootstrap completed successfully.")
		if cfg.CloudSQLEnabled {
			printCloudSQLNextSteps(&cfg)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(bootstrapCmd)

	// Register bootstrap-specific flags that map to Config fields.
	// We use distinct names to avoid confusion and match the desired design.
	bootstrapCmd.Flags().StringVar(&cfg.ClusterName, "cluster-name", getEnv("CLUSTER_NAME", "substrate-poc"), "Name of the GKE cluster [env: CLUSTER_NAME]")
	bootstrapCmd.Flags().StringVar(&cfg.ClusterLocation, "cluster-location", getEnv("CLUSTER_LOCATION", "us-west1-c"), "Zone or region for the cluster [env: CLUSTER_LOCATION]")
	bootstrapCmd.Flags().StringVar(&cfg.ClusterVersion, "cluster-version", getEnv("CLUSTER_VERSION", ""), "Kubernetes version [env: CLUSTER_VERSION]")
	bootstrapCmd.Flags().StringVar(&cfg.Network, "network", getEnv("NETWORK", "default"), "VPC network name [env: NETWORK]")
	bootstrapCmd.Flags().StringVar(&cfg.Subnetwork, "subnetwork", getEnv("SUBNETWORK", "default"), "VPC subnetwork name [env: SUBNETWORK]")
	bootstrapCmd.Flags().StringVar(&cfg.MachineType, "machine-type", getEnv("GVISOR_NODE_MACHINE_TYPE", "c3-standard-4"), "Machine type for the gVisor node pool [env: GVISOR_NODE_MACHINE_TYPE]")
	bootstrapCmd.Flags().Int32Var(&cfg.BootDiskSizeGB, "boot-disk-size", getEnv("BOOT_DISK_SIZE_GB", int32(0)), "Boot disk size in GB for the node pool; 0 = GKE default (100 GB) [env: BOOT_DISK_SIZE_GB]")
	bootstrapCmd.Flags().StringVar(&cfg.BootDiskType, "boot-disk-type", getEnv("BOOT_DISK_TYPE", ""), "Boot disk type for the node pool; empty = GKE default [env: BOOT_DISK_TYPE]")
	bootstrapCmd.Flags().StringVar(&cfg.BucketName, "bucket-name", getEnv("BUCKET_NAME", ""), "Name of the GCS bucket for snapshots [env: BUCKET_NAME]")
	bootstrapCmd.Flags().StringVar(&cfg.DashboardDir, "dashboard-dir", getEnv("DASHBOARD_DIR", "tools/setup-gcp/dashboards"), "Directory containing dashboard JSON files [env: DASHBOARD_DIR]")

	bootstrapCmd.Flags().BoolVar(&cfg.CloudSQLEnabled, "cloudsql", getEnv("CLOUDSQL_ENABLED", false), "Also provision the Cloud SQL PostgreSQL instance backing the ateapi store, in --region (see cloud-sql.md) [env: CLOUDSQL_ENABLED]")
	registerCloudSQLFlags(bootstrapCmd, "cloudsql-")
}
