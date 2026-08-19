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

package atepg

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TestPgoutputBinaryModeWithProtoV1 pins a disputed protocol fact: pgoutput's
// `binary` option is independent of `proto_version` (it was reviewed as
// "silently ignored below proto_version 2"). If that were true, the proto
// column would arrive as TupleDataTypeText (\x-hex); this test asserts it
// arrives as TupleDataTypeBinary under proto_version '1'.
func TestPgoutputBinaryModeWithProtoV1(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := s.dialReplication(ctx)
	if err != nil {
		t.Fatalf("dialReplication failed: %v", err)
	}
	defer conn.Close(context.Background()) //nolint:errcheck

	slot, err := pglogrepl.CreateReplicationSlot(ctx, conn, "pin_binmode_v1", "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	if err != nil {
		t.Fatalf("CreateReplicationSlot failed: %v", err)
	}
	startLSN, err := pglogrepl.ParseLSN(slot.ConsistentPoint)
	if err != nil {
		t.Fatalf("ParseLSN(%q) failed: %v", slot.ConsistentPoint, err)
	}
	if err := pglogrepl.StartReplication(ctx, conn, "pin_binmode_v1", startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", "publication_names '" + workerPublication + "'", "binary 'true'"},
	}); err != nil {
		t.Fatalf("StartReplication failed: %v", err)
	}

	worker := &ateapipb.Worker{WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: "binmode-pod"}
	if err := s.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	var rel *pglogrepl.RelationMessage
	for {
		rawMsg, err := conn.ReceiveMessage(ctx)
		if err != nil {
			t.Fatalf("ReceiveMessage failed before the insert arrived: %v", err)
		}
		cd, ok := rawMsg.(*pgproto3.CopyData)
		if !ok || cd.Data[0] != pglogrepl.XLogDataByteID {
			continue
		}
		xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
		if err != nil {
			t.Fatalf("ParseXLogData failed: %v", err)
		}
		logical, err := pglogrepl.Parse(xld.WALData)
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		switch m := logical.(type) {
		case *pglogrepl.RelationMessage:
			rel = m
		case *pglogrepl.InsertMessage:
			if rel == nil {
				t.Fatal("InsertMessage arrived before RelationMessage")
			}
			for i, col := range rel.Columns {
				if col.Name != "proto" {
					continue
				}
				got := m.Tuple.Columns[i].DataType
				if got != pglogrepl.TupleDataTypeBinary {
					t.Fatalf("proto column arrived as %q under proto_version '1' + binary 'true'; want %q (binary)",
						got, pglogrepl.TupleDataTypeBinary)
				}
				return // proven: binary mode active under proto_version 1
			}
			t.Fatal("relation message has no proto column")
		}
	}
}
