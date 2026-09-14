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
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const testDBUser = "ate-api-server@ate-dev.iam"

// ateapi needs both: CREATE on the database to create its schema objects, and
// USAGE + CREATE on public because PostgreSQL 15+ no longer grants it.
func TestGrantStatements(t *testing.T) {
	got, err := grantStatements(testDBUser)
	if err != nil {
		t.Fatalf("grantStatements() error = %v", err)
	}
	for _, want := range []string{
		`GRANT CREATE ON DATABASE "atepg" TO "ate-api-server@ate-dev.iam";`,
		`GRANT USAGE, CREATE ON SCHEMA public TO "ate-api-server@ate-dev.iam";`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("grantStatements() = %q, want it to contain %q", got, want)
		}
	}
}

// The username is an identifier, so it is quoted rather than bound. A quote in
// it would end the identifier and leave the rest of the name as SQL.
func TestGrantStatementsRejectsQuotes(t *testing.T) {
	for _, dbUser := range []string{
		`ate"; DROP TABLE actors; --`,
		`ate\"admin`,
	} {
		if _, err := grantStatements(dbUser); err == nil {
			t.Errorf("grantStatements(%q) error = nil, want a refusal", dbUser)
		}
	}
}

// The password must not reach the pod spec, which anyone who can read pods in
// the namespace can see.
func TestGrantJobKeepsThePasswordOutOfTheSpec(t *testing.T) {
	statements, err := grantStatements(testDBUser)
	if err != nil {
		t.Fatalf("grantStatements() error = %v", err)
	}
	job := grantJob("10.1.2.3", statements)
	container := job.Spec.Template.Spec.Containers[0]

	var password *corev1.EnvVar
	env := map[string]string{}
	for i, variable := range container.Env {
		if variable.Name == "PGPASSWORD" {
			password = &container.Env[i]
			continue
		}
		env[variable.Name] = variable.Value
	}
	if password == nil {
		t.Fatal("the Job passes no PGPASSWORD")
	}
	if password.Value != "" {
		t.Errorf("PGPASSWORD has a literal value in the pod spec: %q", password.Value)
	}
	if password.ValueFrom == nil || password.ValueFrom.SecretKeyRef == nil {
		t.Error("PGPASSWORD does not come from a Secret")
	}

	if env["PGHOST"] != "10.1.2.3" {
		t.Errorf("PGHOST = %q, want the instance's private address", env["PGHOST"])
	}
	if env["PGSSLMODE"] != "require" {
		t.Errorf("PGSSLMODE = %q, want require", env["PGSSLMODE"])
	}
	if env["PGDATABASE"] != cloudSQLDatabase {
		t.Errorf("PGDATABASE = %q, want %q", env["PGDATABASE"], cloudSQLDatabase)
	}

	// Without ON_ERROR_STOP psql exits 0 after a failed statement, and the
	// Job reports success while the grants did not happen.
	if !slices.Contains(container.Command, "ON_ERROR_STOP=1") {
		t.Errorf("command = %v, want ON_ERROR_STOP=1", container.Command)
	}
	if !slices.Contains(container.Command, statements) {
		t.Errorf("command = %v, want it to run the grants", container.Command)
	}
	if job.Spec.Template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("RestartPolicy = %q, want Never", job.Spec.Template.Spec.RestartPolicy)
	}
}

// A successful run leaves nothing behind: the Secret in particular holds a
// password to the database.
func TestRunGrantJobCleansUp(t *testing.T) {
	kube := fake.NewSimpleClientset()
	succeedGrantJob(t, kube)

	if err := runGrantJob(t.Context(), kube, "10.1.2.3", "hunter2", "GRANT;"); err != nil {
		t.Fatalf("runGrantJob() = %v", err)
	}

	if _, err := kube.CoreV1().Secrets(grantJobNamespace).Get(t.Context(), grantJobName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("the password Secret survived the run (err = %v)", err)
	}
	if _, err := kube.BatchV1().Jobs(grantJobNamespace).Get(t.Context(), grantJobName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("the grant Job survived the run (err = %v)", err)
	}
}

// A failed grant has to be reported. Succeeding quietly would leave ateapi to
// fail its migrations later, a long way from the cause.
func TestRunGrantJobReportsFailure(t *testing.T) {
	kube := fake.NewSimpleClientset()
	kube.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		job := action.(k8stesting.CreateAction).GetObject().(*batchv1.Job)
		job.Status.Failed = *job.Spec.BackoffLimit + 1
		return false, job, nil
	})

	err := runGrantJob(t.Context(), kube, "10.1.2.3", "hunter2", "GRANT;")
	if err == nil {
		t.Fatal("runGrantJob() = nil, want an error")
	}
	if !strings.Contains(err.Error(), "failed") {
		t.Errorf("runGrantJob() = %v, want it to say the Job failed", err)
	}
	// Even then, the password must not be left lying around.
	if _, err := kube.CoreV1().Secrets(grantJobNamespace).Get(t.Context(), grantJobName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("the password Secret survived a failed run (err = %v)", err)
	}
}

// succeedGrantJob makes the fake API server report every created Job as done,
// which is what the real one does once psql exits 0.
func succeedGrantJob(t *testing.T, kube *fake.Clientset) {
	t.Helper()
	kube.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		job := action.(k8stesting.CreateAction).GetObject().(*batchv1.Job)
		job.Status.Succeeded = 1
		return false, job, nil
	})
}
