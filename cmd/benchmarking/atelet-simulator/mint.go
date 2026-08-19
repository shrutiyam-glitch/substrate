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

package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"flag"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/ateapiauth"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// The mint callback replays the credential-minting step a real boot performs:
// atunnel starts inside the restored sandbox and immediately calls
// ateapi.MintCert (via atelet). The simulator fires the same call
// synchronously from Run/Restore, so every fake resume exercises the
// authorization race between the assignment commit and the worker-cache
// update. ateapi's "authorized via store read-through" log line counts the
// races the cache lost.
//
// The simulator must run under the atelet ServiceAccount with a podidentity
// certificate: MintCert authenticates the caller as atelet by SPIFFE ID, and
// authorizes only workers whose NodeName equals this pod's node — seed
// workers with --seed-worker-node-name set to this pod's node.
//
// Worker identity travels in the pod UID: the seeder writes
// WorkerPodUid = "sim://<namespace>/<pod>", which arrives here as
// TargetAteomUid — the only worker-identifying field on Run/Restore — and is
// parsed back into the MintCert request. Echoing the stored UID also
// satisfies the worker-pod-UID match.
var (
	mintTarget = flag.String("mint-target", "",
		"ateapi gRPC target for MintCert callbacks after each Run/Restore (e.g. dns:///api.ate-system.svc:443); empty disables minting.")
	mintCAFile = flag.String("mint-ca-file", "/run/servicedns.podcert.ate.dev/trust-bundle.pem",
		"CA bundle used to verify the ateapi serving certificate.")
	mintServerName = flag.String("mint-server-name", "api.ate-system.svc",
		"DNS name expected on the ateapi certificate.")
	mintCredBundle = flag.String("mint-client-cred-bundle", "/run/podidentity.podcert.ate.dev/credential-bundle.pem",
		"Client certificate bundle presented to ateapi; must carry the atelet ServiceAccount identity.")
	mintTimeout = flag.Duration("mint-timeout", 5*time.Second, "Per-mint deadline.")
)

// simUIDPrefix marks seeded worker pod UIDs carrying worker identity.
const simUIDPrefix = "sim://"

type minter struct {
	client                    ateapipb.ActorIdentityClient
	ok, denied, failed, skips atomic.Int64
}

func newMinter() (*minter, error) {
	dialOpts, err := ateapiauth.DialOptions(ateapiauth.ClientConfig{
		CAFile:           *mintCAFile,
		ServerName:       *mintServerName,
		ClientCredBundle: *mintCredBundle,
	})
	if err != nil {
		return nil, fmt.Errorf("building ateapi client credentials: %w", err)
	}
	conn, err := grpc.NewClient(*mintTarget, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating ateapi client: %w", err)
	}
	return &minter{client: ateapipb.NewActorIdentityClient(conn)}, nil
}

// mint performs one MintCert call for the worker/actor named by a Run or
// Restore request. Failures are counted and logged, never propagated: the
// data-plane stub must keep succeeding regardless, exactly like a real
// sandbox whose atunnel is retrying.
func (m *minter) mint(ctx context.Context, targetAteomUID, actorUID string) {
	rest, ok := strings.CutPrefix(targetAteomUID, simUIDPrefix)
	if !ok {
		m.skips.Add(1) // non-synthetic worker (real pod UID); identity unknown
		return
	}
	namespace, pod, ok := strings.Cut(rest, "/")
	if !ok {
		m.skips.Add(1)
		return
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		m.failed.Add(1)
		slog.Error("mint: generating key", "err", err)
		return
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: "atunnel-simulated"},
	}, key)
	if err != nil {
		m.failed.Add(1)
		slog.Error("mint: creating CSR", "err", err)
		return
	}

	mintCtx, cancel := context.WithTimeout(ctx, *mintTimeout)
	defer cancel()
	start := time.Now()
	_, err = m.client.MintCert(mintCtx, &ateapipb.MintCertRequest{
		WorkerNamespace:           namespace,
		WorkerPod:                 pod,
		WorkerPodUid:              targetAteomUID,
		CertificateSigningRequest: csr,
		ExpectedActorUid:          actorUID,
		Purpose:                   ateapipb.ActorCertificatePurpose_ACTOR_CERTIFICATE_PURPOSE_ATUNNEL,
	})
	elapsed := time.Since(start)
	switch status.Code(err) {
	case codes.OK:
		m.ok.Add(1)
		slog.Debug("mint ok", "worker", namespace+"/"+pod, "elapsed", elapsed)
	case codes.PermissionDenied, codes.FailedPrecondition:
		m.denied.Add(1)
		slog.Warn("mint denied", "worker", namespace+"/"+pod, "code", status.Code(err), "err", err, "elapsed", elapsed)
	default:
		m.failed.Add(1)
		slog.Warn("mint failed", "worker", namespace+"/"+pod, "err", err, "elapsed", elapsed)
	}
}

// reportLoop logs cumulative mint counters every interval.
func (m *minter) reportLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			slog.Info("mint totals",
				"ok", m.ok.Load(), "denied", m.denied.Load(),
				"failed", m.failed.Load(), "skipped", m.skips.Load())
		}
	}
}
