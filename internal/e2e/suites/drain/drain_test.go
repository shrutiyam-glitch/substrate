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

// Package drain exercises the atenet router's graceful shutdown end to end:
// a request parked on the router (1-worker pool, oversubscribed by two
// actors) must survive the router pod's deletion — park → resume → 200
// through the dying pod — and the pod must terminate via the drain sequence
// (readiness flip, Envoy drain, ext_proc drain, drain-complete marker), well
// under terminationGracePeriodSeconds rather than being SIGKILLed at it.
//
// CAUTION: this suite deletes the shared atenet-router pod. The Deployment
// recreates it, but port-forward tunnels other concurrently-running suites
// hold to the old pod break when it exits, and the Service is briefly
// unbacked while the replacement comes up. Run this suite isolated (its own
// `go test ./internal/e2e/suites/drain` invocation, or `-p 1`) rather than in
// parallel with the other suites.
package drain

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	v1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	drainAtespace   = "drain-e2e"
	routerNamespace = "ate-system"
	routerAppLabel  = "app=atenet-router"

	// The deployed router's flag values (manifest / flag defaults). The
	// timing assertions below are windows around them.
	routerParkBudget = 5 * time.Second
	routerDrainDelay = 13 * time.Second

	// The pod's shutdown budget. Terminating close to it means the SIGKILL
	// path fired; terminating well under it means the drain-complete marker
	// handshake released Envoy's preStop.
	routerGracePeriod = 60 * time.Second
)

func TestRouterGracefulDrain(t *testing.T) {
	ctx := context.Background()
	clients := e2e.GetClients()
	nsObj := e2e.CreateNamespace(t)
	defer nsObj.Delete(t)

	at := createDrainFixture(ctx, t, clients, nsObj)

	_, _ = clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: drainAtespace}},
	})

	actorA := "drain-a-" + nsObj.Name
	actorB := "drain-b-" + nsObj.Name
	for _, name := range []string{actorA, actorB} {
		createActor(ctx, t, clients, nsObj, at, name)
	}

	// The port-forward tunnels pin to the router pod serving the Service right
	// now — the pod this test is about to delete. That is the point: they keep
	// working while the pod drains and break only when it exits.
	router, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("creating router client: %v", err)
	}
	defer router.Close()
	statusz, err := e2e.NewStatuszClient(ctx)
	if err != nil {
		t.Fatalf("creating statusz client: %v", err)
	}
	defer statusz.Close()

	oldPod := runningRouterPod(ctx, t, clients)
	t.Logf("router pod under test: %s", oldPod)

	// Occupy the only worker with actor A, so a request for B parks.
	resumeActor(ctx, t, clients, actorA)
	waitForActorStatus(ctx, t, clients, actorA, ateapipb.Actor_STATUS_RUNNING)

	type result struct {
		resp    *http.Response
		body    string
		err     error
		elapsed time.Duration
	}
	resCh := make(chan result, 1)
	start := time.Now()
	go func() {
		resp, err := router.Get(ctx, resources.ActorRef{Atespace: drainAtespace, Name: actorB}, "/")
		var body string
		if err == nil {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			body = string(b)
		}
		resCh <- result{resp, body, err, time.Since(start)}
	}()

	// Delete the router pod only once the request is observably parked on it.
	waitForParkedCount(ctx, t, statusz, func(active int) bool { return active >= 1 })
	deleteStart := time.Now()
	if err := clients.K8s.CoreV1().Pods(routerNamespace).Delete(ctx, oldPod, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting router pod %s: %v", oldPod, err)
	}
	waitForDeletionTimestamp(ctx, t, clients, oldPod)
	t.Logf("router pod %s is Terminating with a request parked on it", oldPod)

	// Free the worker: the parked request must now resume actor B and be
	// served by the terminating pod as if nothing were happening.
	suspendActor(ctx, t, clients, actorA)

	res := <-resCh
	if res.err != nil {
		t.Fatalf("parked request failed transport-level during the drain: %v", res.err)
	}
	if res.resp.StatusCode != http.StatusOK {
		t.Fatalf("parked request through the terminating pod: status = %d (body %q), want 200", res.resp.StatusCode, res.body)
	}
	if !strings.Contains(res.body, "hello from") {
		t.Errorf("parked request body = %q, want the counter greeting", res.body)
	}
	t.Logf("parked request served by the terminating pod after %v", res.elapsed)

	// The pod must exit via the drain sequence: after the drain-delay at the
	// earliest (an immediate exit would mean the sequence was skipped), and
	// well before the grace period (an exit near it means the drain wedged and
	// the kubelet SIGKILLed — the failure mode this work removed).
	gone := waitForPodGone(ctx, t, clients, oldPod, routerGracePeriod+15*time.Second)
	terminating := gone.Sub(deleteStart)
	t.Logf("router pod terminated %v after deletion", terminating)
	if terminating < routerDrainDelay-3*time.Second {
		t.Errorf("pod terminated after %v, before the %v drain-delay could have run — the drain sequence was skipped", terminating, routerDrainDelay)
	}
	if terminating > routerGracePeriod-5*time.Second {
		t.Errorf("pod terminated after %v, at the %v grace period — SIGKILL path, the drain-complete handshake did not release Envoy", terminating, routerGracePeriod)
	}

	// The Deployment must be whole again, and the Service must route through
	// the replacement pod: same request, fresh tunnel, fast-path 200.
	waitForRouterReady(ctx, t, clients, oldPod)
	router2, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("creating router client against the replacement pod: %v", err)
	}
	defer router2.Close()
	resp, err := router2.Get(ctx, resources.ActorRef{Atespace: drainAtespace, Name: actorB}, "/")
	if err != nil {
		t.Fatalf("request through the replacement pod: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("request through the replacement pod: status = %d (body %q), want 200", resp.StatusCode, string(body))
	}
	t.Log("replacement router pod serving")
}

// runningRouterPod returns the name of the single Ready, non-terminating
// atenet-router pod.
func runningRouterPod(ctx context.Context, t *testing.T, clients *e2e.Clients) string {
	t.Helper()
	pods, err := clients.K8s.CoreV1().Pods(routerNamespace).List(ctx, metav1.ListOptions{LabelSelector: routerAppLabel})
	if err != nil {
		t.Fatalf("listing router pods: %v", err)
	}
	var running []string
	for _, p := range pods.Items {
		if p.DeletionTimestamp == nil && p.Status.Phase == corev1.PodRunning {
			running = append(running, p.Name)
		}
	}
	if len(running) != 1 {
		t.Fatalf("expected exactly 1 running router pod, got %v", running)
	}
	return running[0]
}

// waitForDeletionTimestamp waits until the pod is marked Terminating, i.e.
// the kubelet has begun the shutdown (SIGTERM to atenet, preStop on Envoy).
func waitForDeletionTimestamp(ctx context.Context, t *testing.T, clients *e2e.Clients, pod string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		p, err := clients.K8s.CoreV1().Pods(routerNamespace).Get(ctx, pod, metav1.GetOptions{})
		if apierrors.IsNotFound(err) || (err == nil && p.DeletionTimestamp != nil) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for pod %s to be marked Terminating", pod)
}

// waitForPodGone waits for the pod object to disappear and returns when it did.
func waitForPodGone(ctx context.Context, t *testing.T, clients *e2e.Clients, pod string, timeout time.Duration) time.Time {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, err := clients.K8s.CoreV1().Pods(routerNamespace).Get(ctx, pod, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return time.Now()
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("router pod %s still present after %v — SIGKILL at the grace period should have removed it", pod, timeout)
	return time.Time{}
}

// waitForRouterReady waits until a Ready router pod other than oldPod backs
// the Service, leaving the cluster healthy for whatever runs next.
func waitForRouterReady(ctx context.Context, t *testing.T, clients *e2e.Clients, oldPod string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		pods, err := clients.K8s.CoreV1().Pods(routerNamespace).List(ctx, metav1.ListOptions{LabelSelector: routerAppLabel})
		if err == nil {
			for _, p := range pods.Items {
				if p.Name == oldPod || p.DeletionTimestamp != nil {
					continue
				}
				for _, c := range p.Status.Conditions {
					if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
						return
					}
				}
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("timed out waiting for a Ready replacement router pod")
}

// --- fixture helpers, following the parking suite ---

// createDrainFixture provisions a 1-worker pool and an ActorTemplate in the
// test namespace, copying the resolved runtime from the installed counter
// demo; the unique pool label keeps this pool's worker invisible to other
// namespaces' actors.
func createDrainFixture(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace) *v1alpha1.ActorTemplate {
	t.Helper()
	env, err := e2e.CheckEnv("BUCKET_NAME")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}

	srcNS, srcName := "ate-demo-counter", "counter"
	existingWp, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(srcNS).Get(ctx, srcName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get source WorkerPool %s/%s: %v", srcNS, srcName, err)
	}
	existingAt, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(srcNS).Get(ctx, srcName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get source ActorTemplate %s/%s: %v", srcNS, srcName, err)
	}

	wp := &v1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "drain",
			Namespace: nsObj.Name,
			Labels:    map[string]string{"demo": nsObj.Name},
		},
		Spec: v1alpha1.WorkerPoolSpec{
			Replicas:          1, // deliberately undersized: 2 actors contend for it
			AteomImage:        existingWp.Spec.AteomImage,
			SandboxClass:      existingWp.Spec.SandboxClass,
			SandboxConfigName: existingWp.Spec.SandboxConfigName,
		},
	}
	if _, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(nsObj.Name).Create(ctx, wp, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create WorkerPool: %v", err)
	}

	at := &v1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "drain",
			Namespace: nsObj.Name,
		},
		Spec: v1alpha1.ActorTemplateSpec{
			WorkerSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"demo": nsObj.Name},
			},
			SandboxClass: existingAt.Spec.SandboxClass,
			PauseImage:   existingAt.Spec.PauseImage,
			Containers:   existingAt.Spec.Containers,
			SnapshotsConfig: v1alpha1.SnapshotsConfig{
				Location: "gs://" + env["BUCKET_NAME"] + "/e2e-drain-" + nsObj.Name,
			},
			Volumes: existingAt.Spec.Volumes,
		},
	}
	if _, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(nsObj.Name).Create(ctx, at, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create ActorTemplate: %v", err)
	}

	t.Logf("Waiting for ActorTemplate %s to be Ready...", at.Name)
	tmplCtx, tmplCancel := context.WithTimeout(ctx, 90*time.Second)
	defer tmplCancel()
	var lastPhase v1alpha1.PhaseType
	for {
		curAt, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(nsObj.Name).Get(tmplCtx, at.Name, metav1.GetOptions{})
		if err == nil {
			lastPhase = curAt.Status.Phase
			if lastPhase == v1alpha1.PhaseReady {
				return at
			}
			if lastPhase == v1alpha1.PhaseFailed {
				t.Fatalf("ActorTemplate %s transitioned to PhaseFailed", at.Name)
			}
		}
		select {
		case <-tmplCtx.Done():
			t.Fatalf("timed out waiting for ActorTemplate %q to be Ready (last phase: %s, err: %v)", at.Name, lastPhase, err)
		case <-time.After(1 * time.Second):
		}
	}
}

func createActor(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, at *v1alpha1.ActorTemplate, name string) {
	t.Helper()
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: drainAtespace, Name: name},
		ActorTemplateNamespace: nsObj.Name,
		ActorTemplateName:      at.Name,
	}}); err != nil {
		t.Fatalf("failed to create actor %q: %v", name, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_, _ = clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapipb.SuspendActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: drainAtespace, Name: name},
		})
		_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapipb.DeleteActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: drainAtespace, Name: name},
		})
	})
}

func resumeActor(ctx context.Context, t *testing.T, clients *e2e.Clients, name string) {
	t.Helper()
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: drainAtespace, Name: name},
	}); err != nil {
		t.Fatalf("failed to resume actor %q: %v", name, err)
	}
}

func suspendActor(ctx context.Context, t *testing.T, clients *e2e.Clients, name string) {
	t.Helper()
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: drainAtespace, Name: name},
	}); err != nil {
		t.Fatalf("failed to suspend actor %q: %v", name, err)
	}
}

func waitForActorStatus(ctx context.Context, t *testing.T, clients *e2e.Clients, name string, want ateapipb.Actor_Status) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: drainAtespace, Name: name},
		})
		if err == nil && resp.GetStatus() == want {
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("timed out waiting for actor %q to reach %v", name, want)
}

// waitForParkedCount polls the router's statusz parking gauge until cond holds.
func waitForParkedCount(ctx context.Context, t *testing.T, statusz *e2e.StatuszClient, cond func(active int) bool) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	var last int
	for time.Now().Before(deadline) {
		p, err := statusz.Parking(ctx)
		if err == nil {
			last = p.Active
			if cond(p.Active) {
				return
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for the parking gauge to satisfy the condition (last active=%d)", last)
}
