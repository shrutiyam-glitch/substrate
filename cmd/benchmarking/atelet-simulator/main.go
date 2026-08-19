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

// atelet-simulator is a fake atelet for control-plane benchmarking: it
// implements the AteomHerder service (Run / Checkpoint / Restore — the only
// RPCs ateapi's workflows call) and returns success after a configurable
// delay, without touching containers, snapshots, or storage.
//
// Point ateapi at it with --atelet-simulator-address (see
// docs/dev/fake-lifecycle-testing.md). This lets resume/suspend/pause flows
// run at scale with zero worker pods: the control plane — gRPC, auth,
// leases, CAS, store, watch — is fully real; only the data plane is faked.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

var (
	listenAddr = flag.String("listen-addr", ":9090", "gRPC listen address.")
	delay      = flag.Duration("response-delay", 1*time.Millisecond,
		"Sleep before every response, simulating minimal atelet work.")
)

type simulator struct {
	ateletpb.UnimplementedAteomHerderServer
	// minter, when non-nil, fires a MintCert callback at ateapi after every
	// Run/Restore — replaying the credential mint a real sandbox boot
	// performs (see mint.go).
	minter *minter
}

func (s *simulator) pause(ctx context.Context) error {
	select {
	case <-time.After(*delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *simulator) Run(ctx context.Context, req *ateletpb.RunRequest) (*ateletpb.RunResponse, error) {
	if err := s.pause(ctx); err != nil {
		return nil, err
	}
	if s.minter != nil {
		s.minter.mint(ctx, req.GetTargetAteomUid(), req.GetActorUid())
	}
	return &ateletpb.RunResponse{}, nil
}

func (s *simulator) Checkpoint(ctx context.Context, req *ateletpb.CheckpointRequest) (*ateletpb.CheckpointResponse, error) {
	if err := s.pause(ctx); err != nil {
		return nil, err
	}
	return &ateletpb.CheckpointResponse{}, nil
}

func (s *simulator) Restore(ctx context.Context, req *ateletpb.RestoreRequest) (*ateletpb.RestoreResponse, error) {
	if err := s.pause(ctx); err != nil {
		return nil, err
	}
	if s.minter != nil {
		s.minter.mint(ctx, req.GetTargetAteomUid(), req.GetActorUid())
	}
	return &ateletpb.RestoreResponse{}, nil
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		slog.Error("listen failed", "addr", *listenAddr, "err", err)
		os.Exit(1)
	}
	sim := &simulator{}
	if *mintTarget != "" {
		m, err := newMinter()
		if err != nil {
			slog.Error("mint setup failed", "err", err)
			os.Exit(1)
		}
		sim.minter = m
		go m.reportLoop(context.Background(), 10*time.Second)
		slog.Info("MintCert callbacks enabled", "target", *mintTarget)
	}
	srv := grpc.NewServer()
	ateletpb.RegisterAteomHerderServer(srv, sim)
	slog.Info("atelet-simulator serving", "addr", *listenAddr, "response-delay", delay.String())
	if err := srv.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
