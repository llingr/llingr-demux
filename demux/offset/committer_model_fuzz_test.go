// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package offset

import (
	"math/rand"
	"testing"
)

// FuzzCommitterModel drives the committer model harness (committer_model_test.go)
// from fuzzer-chosen bytes instead of a seeded random walk: byte 0 picks the
// baseline mode, byte 1 seeds the stream/shuffle randomness, and each
// subsequent 3-byte group decodes to one operation (opcode, partition,
// magnitude). Every input ends with quiescence, so the named invariants
// (commit monotonicity, commit ownership, eventual completeness, quiescent
// drain, work item accounting, baseline agreement) are enforced for every
// generated interleaving. The decoder is total: any byte string is a valid
// operation sequence.
//
// Run the corpus as part of `go test`; explore with
// `go test -fuzz FuzzCommitterModel -fuzztime 30s ./demux/offset/`.
func FuzzCommitterModel(f *testing.F) {
	// Seeds shaped like the known historical failures, so the corpus always
	// exercises their families:
	// deliver -> complete -> tick at a gap boundary -> deliver -> complete
	f.Add([]byte{0, 7, 0, 0, 5, 2, 0, 3, 5, 0, 0, 0, 0, 4, 2, 0, 4, 5, 0, 0})
	// churn: revoke/assign cycles with orphans replaying across epochs
	f.Add([]byte{1, 3, 0, 1, 6, 2, 1, 4, 3, 1, 0, 6, 1, 2, 3, 1, 0, 0, 5, 1, 2, 6, 0, 1, 3, 0, 2})
	// other-owner progress while away, then re-assign and orphan replay
	f.Add([]byte{0, 9, 0, 0, 4, 2, 0, 2, 3, 0, 0, 7, 0, 1, 6, 0, 3, 3, 0, 1})
	// broker failure under load
	f.Add([]byte{1, 5, 0, 0, 8, 2, 0, 6, 8, 1, 0, 8, 0, 0})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		mode := len(data) > 0 && data[0]&1 == 1
		var seed int64 = 1
		if len(data) > 1 {
			seed = int64(data[1]) + 1
		}
		random := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic model, not crypto

		model := newCommitterModel(t, random, 2, 60, mode, false)

		operations := (len(data) - 2) / 3
		if operations > 2000 {
			operations = 2000
		}
		for i := 0; i < operations && !model.isFailed(); i++ {
			opcode := data[2+i*3]
			partition := int32(data[2+i*3+1]) % 2
			magnitude := 1 + int(data[2+i*3+2])%8
			switch opcode % 8 {
			case 0:
				model.opDeliver(partition, magnitude)
			case 1:
				model.opComplete(random, partition, magnitude)
			case 2:
				model.opTick()
			case 3:
				model.opRevoke(random, partition)
			case 4:
				model.opAssign(partition)
			case 5:
				model.opReplayOrphans(random, partition, magnitude)
			case 6:
				model.opOtherOwnerAdvance(random, partition)
			case 7:
				model.opBrokerFailTick()
			}
		}
		if !model.isFailed() {
			model.quiesce(random)
		}
	})
}
