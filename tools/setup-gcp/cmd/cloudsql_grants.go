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
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	container "cloud.google.com/go/container/apiv1"
	"cloud.google.com/go/container/apiv1/containerpb"
	"golang.org/x/oauth2/google"
	sqladmin "google.golang.org/api/sqladmin/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	// grantJobName names both the Job that issues the grants and the Secret
	// holding the password it connects with. They live in the default
	// namespace because ate-system belongs to the installer and need not
	// exist yet when the database is provisioned.
	grantJobName      = "ate-cloudsql-grant"
	grantJobNamespace = "default"

	// grantJobImage supplies psql. The version only has to speak the wire
	// protocol; it need not match the server.
	grantJobImage = "postgres:18-alpine"

	// grantJobTimeout bounds pulling the image, scheduling, and two grants.
	grantJobTimeout = 5 * time.Minute

	// postgresUser is the built-in Cloud SQL administrator, and the only role
	// able to grant on a database it did not create.
	postgresUser = "postgres"
)

// grantCloudSQLSchemaPrivileges gives the IAM database user the rights ateapi
// needs to create its tables.
//
// IAM database users are created with no privileges, and PostgreSQL 15+ took
// CREATE on the public schema away from PUBLIC, so ateapi cannot apply its
// migrations until someone grants it. Only the built-in postgres user can, and
// a fresh instance gives it no password; the instance has no public address,
// so the session has to originate inside the VPC. This therefore sets a
// password, runs psql as a Job on the cluster, and sets the password to
// another random value nobody keeps.
//
// Re-running is harmless: both grants are idempotent. The postgres password is
// not preserved across runs, by design -- see the warning in cloud-sql.md.
func grantCloudSQLSchemaPrivileges(ctx context.Context, svc *sqladmin.Service, cfg *Config) error {
	dbUser := cloudSQLDatabaseUser(cloudSQLGSAEmail(cfg))
	statements, err := grantStatements(dbUser)
	if err != nil {
		return err
	}

	host, err := cloudSQLPrivateIP(ctx, svc, cfg)
	if err != nil {
		return err
	}

	kube, err := clusterClient(ctx, cfg)
	if err != nil {
		// Provisioning succeeded; only the grants did not. Saying so and
		// printing the statements beats failing a command that did create
		// everything it was asked to, and `create cloudsql` is allowed to run
		// against a project with no cluster in it yet.
		slog.Warn("Cannot reach a cluster to grant schema privileges from; ateapi will fail its migrations until these are run",
			slog.String("error", err.Error()),
			slog.String("sql", statements))
		return nil
	}

	password, err := randomPassword()
	if err != nil {
		return err
	}
	slog.Info("Setting a temporary password on the built-in postgres user to grant schema privileges...")
	if err := setPostgresPassword(ctx, svc, cfg, password); err != nil {
		return err
	}
	defer func() {
		// The password exists only for the Job above. Leaving it usable would
		// add a credential to the instance that nothing needs and no one is
		// tracking.
		scrambled, err := randomPassword()
		if err == nil {
			err = setPostgresPassword(ctx, svc, cfg, scrambled)
		}
		if err != nil {
			slog.Error("Failed to scramble the temporary postgres password; reset or remove it by hand",
				slog.String("error", err.Error()))
		}
	}()

	slog.Info("Granting schema privileges to the IAM database user...", slog.String("user", dbUser))
	if err := runGrantJob(ctx, kube, host, password, statements); err != nil {
		return err
	}
	slog.Info("Schema privileges granted.")
	return nil
}

// grantStatements is the SQL the Job runs. The username is an identifier
// rather than a value, so it cannot be a bound parameter and is quoted
// instead; a quote in it would end the identifier early, so reject that
// outright rather than escape it.
func grantStatements(dbUser string) (string, error) {
	if strings.ContainsAny(dbUser, `"\`) {
		return "", fmt.Errorf("refusing to build a GRANT for database user %q: it contains a quote", dbUser)
	}
	return fmt.Sprintf(`GRANT CREATE ON DATABASE %q TO %q; GRANT USAGE, CREATE ON SCHEMA public TO %q;`,
		cloudSQLDatabase, dbUser, dbUser), nil
}

// cloudSQLPrivateIP returns the address the cluster reaches the instance on.
func cloudSQLPrivateIP(ctx context.Context, svc *sqladmin.Service, cfg *Config) (string, error) {
	instance, err := svc.Instances.Get(cfg.ProjectID, cfg.CloudSQLInstance).Context(ctx).Do()
	if err != nil {
		return "", fmt.Errorf("get Cloud SQL instance %s: %w", cfg.CloudSQLInstance, err)
	}
	for _, address := range instance.IpAddresses {
		if address.Type == "PRIVATE" {
			return address.IpAddress, nil
		}
	}
	return "", fmt.Errorf("Cloud SQL instance %s has no private IP address", cfg.CloudSQLInstance)
}

// setPostgresPassword sets the built-in administrator's password.
func setPostgresPassword(ctx context.Context, svc *sqladmin.Service, cfg *Config, password string) error {
	op, err := svc.Users.Update(cfg.ProjectID, cfg.CloudSQLInstance, &sqladmin.User{
		Name:     postgresUser,
		Password: password,
	}).Name(postgresUser).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("set the password of the %s user: %w", postgresUser, err)
	}
	return waitForSQLOperation(ctx, svc, cfg, op)
}

// randomPassword returns a password with no structure to guess.
func randomPassword() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate a password: %w", err)
	}
	return hex.EncodeToString(raw), nil
}

// clusterClient connects to the cluster's API server using application default
// credentials, the same way every other call in this tool authenticates.
//
// The token is read once. It outlives the Job below by a wide margin, and the
// client is discarded immediately after.
func clusterClient(ctx context.Context, cfg *Config) (kubernetes.Interface, error) {
	manager, err := container.NewClusterManagerClient(ctx)
	if err != nil {
		return nil, err
	}
	defer manager.Close()

	name := fmt.Sprintf("projects/%s/locations/%s/clusters/%s", cfg.ProjectID, cfg.ClusterLocation, cfg.ClusterName)
	cluster, err := manager.GetCluster(ctx, &containerpb.GetClusterRequest{Name: name})
	if err != nil {
		return nil, fmt.Errorf("get cluster %s: %w", name, err)
	}
	certificate, err := base64.StdEncoding.DecodeString(cluster.GetMasterAuth().GetClusterCaCertificate())
	if err != nil {
		return nil, fmt.Errorf("decode the cluster CA certificate: %w", err)
	}
	source, err := google.DefaultTokenSource(ctx, "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		return nil, fmt.Errorf("find application default credentials: %w", err)
	}
	token, err := source.Token()
	if err != nil {
		return nil, fmt.Errorf("get an access token: %w", err)
	}
	return kubernetes.NewForConfig(&rest.Config{
		Host:            "https://" + cluster.GetEndpoint(),
		TLSClientConfig: rest.TLSClientConfig{CAData: certificate},
		BearerToken:     token.AccessToken,
	})
}

// runGrantJob runs psql on the cluster and waits for it to finish.
func runGrantJob(ctx context.Context, kube kubernetes.Interface, host, password, statements string) error {
	secrets := kube.CoreV1().Secrets(grantJobNamespace)
	jobs := kube.BatchV1().Jobs(grantJobNamespace)

	// A leftover from an interrupted run would otherwise collide, and its
	// password no longer opens anything.
	deleteGrantJob(ctx, kube)

	if _, err := secrets.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: grantJobName, Namespace: grantJobNamespace},
		StringData: map[string]string{"password": password},
	}, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create the Secret holding the grant password: %w", err)
	}
	defer deleteGrantJob(ctx, kube)

	if _, err := jobs.Create(ctx, grantJob(host, statements), metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create the grant Job: %w", err)
	}
	return waitForGrantJob(ctx, kube)
}

// grantJob is the Job that connects to the instance and issues the grants.
//
// The password arrives as an environment variable from a Secret rather than
// inside a connection string, which would put it in the pod spec for anyone
// who can read pods. ON_ERROR_STOP makes psql exit non-zero on a failed
// statement, which a Job reports as a failure instead of a success with a
// message in the log.
func grantJob(host, statements string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: grantJobName, Namespace: grantJobNamespace},
		Spec: batchv1.JobSpec{
			BackoffLimit: ptr(int32(2)),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:    "psql",
						Image:   grantJobImage,
						Command: []string{"psql", "-v", "ON_ERROR_STOP=1", "-c", statements},
						Env: []corev1.EnvVar{
							{Name: "PGHOST", Value: host},
							{Name: "PGPORT", Value: "5432"},
							{Name: "PGUSER", Value: postgresUser},
							{Name: "PGDATABASE", Value: cloudSQLDatabase},
							{Name: "PGSSLMODE", Value: "require"},
							{Name: "PGCONNECT_TIMEOUT", Value: "30"},
							{Name: "PGPASSWORD", ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: grantJobName},
									Key:                  "password",
								},
							}},
						},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: ptr(false),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
					}},
				},
			},
		},
	}
}

// waitForGrantJob blocks until the Job succeeds or fails, reporting a failure
// with the psql output that explains it.
func waitForGrantJob(ctx context.Context, kube kubernetes.Interface) error {
	var failure error
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, grantJobTimeout, true,
		func(ctx context.Context) (bool, error) {
			job, err := kube.BatchV1().Jobs(grantJobNamespace).Get(ctx, grantJobName, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if job.Status.Succeeded > 0 {
				return true, nil
			}
			if job.Spec.BackoffLimit != nil && job.Status.Failed > *job.Spec.BackoffLimit {
				failure = fmt.Errorf("the grant Job failed: %s", grantJobLogs(ctx, kube))
				return false, failure
			}
			return false, nil
		})
	if failure != nil {
		return failure
	}
	if err != nil {
		return fmt.Errorf("waiting for the grant Job: %w (%s)", err, grantJobLogs(ctx, kube))
	}
	return nil
}

// grantJobLogs returns what psql printed, for an error message.
func grantJobLogs(ctx context.Context, kube kubernetes.Interface) string {
	pods, err := kube.CoreV1().Pods(grantJobNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "job-name=" + grantJobName,
	})
	if err != nil || len(pods.Items) == 0 {
		return "no pod logs available"
	}
	last := pods.Items[len(pods.Items)-1]
	logs, err := kube.CoreV1().Pods(grantJobNamespace).
		GetLogs(last.Name, &corev1.PodLogOptions{}).DoRaw(ctx)
	if err != nil || len(logs) == 0 {
		return fmt.Sprintf("pod %s: %s", last.Name, last.Status.Phase)
	}
	return strings.TrimSpace(string(logs))
}

// deleteGrantJob removes the Job and the password it ran with. Failures are
// logged rather than returned: this runs on the way out, including out of an
// error, and the grants themselves are what the caller asked about.
func deleteGrantJob(ctx context.Context, kube kubernetes.Interface) {
	policy := metav1.DeletePropagationBackground
	options := metav1.DeleteOptions{PropagationPolicy: &policy}
	if err := kube.BatchV1().Jobs(grantJobNamespace).Delete(ctx, grantJobName, options); err != nil && !isKubeNotFound(err) {
		slog.Warn("Failed to delete the grant Job", slog.String("error", err.Error()))
	}
	if err := kube.CoreV1().Secrets(grantJobNamespace).Delete(ctx, grantJobName, options); err != nil && !isKubeNotFound(err) {
		slog.Warn("Failed to delete the grant password Secret; delete it by hand",
			slog.String("secret", grantJobNamespace+"/"+grantJobName),
			slog.String("error", err.Error()))
	}
}

// isKubeNotFound reports whether err is the API server saying the object is
// already gone, which is a successful deletion for these purposes.
func isKubeNotFound(err error) bool { return apierrors.IsNotFound(err) }

func ptr[T any](v T) *T { return &v }
