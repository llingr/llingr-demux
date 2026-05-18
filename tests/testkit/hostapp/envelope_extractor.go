// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package hostapp

import (
	"context"

	"github.com/llingr/llingr-demux/tests/testkit/scenario"
	"github.com/llingr/llingr-nexus/nexus"
)

// SimpleEnvelopeExtractor extracts envelope from scenario.TestMessage.
func SimpleEnvelopeExtractor(payload scenario.TestMessage) nexus.Envelope {
	return nexus.Envelope{
		Partition: payload.Partition,
		Offset:    payload.Offset,
		Key:       payload.Key,
		Ctx:       context.Background(),
	}
}
