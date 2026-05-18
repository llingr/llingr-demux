// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

// Package scenario provides test message generation utilities.
// [GenerateMessages] creates sequences of test messages distributed across partitions.
package scenario

import (
	"fmt"
	"math/rand"

	"github.com/llingr/llingr-nexus/nexus"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(62)] //nolint:gosec,mnd // G404: test data; charset length
	}
	return string(b)
}

// GenerateMessages creates n test messages distributed across partitions.
// Messages are sequential per partition with ascending offsets.
func GenerateMessages(count int, numPartitions int32) []*nexus.Message[TestMessage] {
	messages := make([]*nexus.Message[TestMessage], count)
	partitionOffsets := make(map[int32]int64)

	for i := 0; i < count; i++ {
		partition := int32(i % int(numPartitions)) //nolint:gosec // G115: numPartitions bounded by Kafka limits
		offset := partitionOffsets[partition]
		partitionOffsets[partition]++

		payload := &TestMessage{
			ID:        i,
			Partition: partition,
			Offset:    offset,
			Key:       fmt.Sprintf("key-%d-%s", i, randomString(30)),   //nolint:mnd // test key length
			Data:      fmt.Sprintf("data-%d-%s", i, randomString(500)), //nolint:mnd // test payload size
		}

		messages[i] = &nexus.Message[TestMessage]{
			Partition: partition,
			Offset:    offset,
			Key:       payload.Key,
			Payload:   payload,
		}
	}

	return messages
}
