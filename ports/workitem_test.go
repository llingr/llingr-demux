// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package ports_test

import (
	"context"
	"testing"

	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

func TestWorkItem_PartitionOffset(t *testing.T) {
	t.Run("returns correct partition and offset", func(t *testing.T) {
		message := &nexus.Message[string]{
			Partition: 42,
			Offset:    1234567890,
			Key:       "test-key",
			Payload:   new(string),
		}

		workItem := &ports.WorkItem[string]{
			Message: message,
			Metrics: &nexus.Metrics{},
			Ctx:     context.Background(),
		}

		partition, offset := workItem.PartitionOffset()

		if partition != 42 {
			t.Errorf("Expected partition 42, got %d", partition)
		}
		if offset != 1234567890 {
			t.Errorf("Expected offset 1234567890, got %d", offset)
		}
	})

	t.Run("works with pointer types", func(t *testing.T) {
		type CustomStruct struct {
			Data string
		}

		c := &CustomStruct{Data: "test"}
		message := &nexus.Message[*CustomStruct]{
			Partition: 5,
			Offset:    999,
			Key:       "pointer-key",
			Payload:   &c,
		}

		workItem := &ports.WorkItem[*CustomStruct]{
			Message: message,
			Metrics: &nexus.Metrics{},
			Ctx:     context.Background(),
		}

		partition, offset := workItem.PartitionOffset()

		if partition != 5 {
			t.Errorf("Expected partition 5, got %d", partition)
		}
		if offset != 999 {
			t.Errorf("Expected offset 999, got %d", offset)
		}
	})

	t.Run("works with value types", func(t *testing.T) {
		type CustomValue struct {
			ID   int
			Name string
		}

		customValue := CustomValue{ID: 123, Name: "test"}

		message := &nexus.Message[CustomValue]{
			Partition: 15,
			Offset:    5555,
			Key:       "value-key",
			Payload:   &customValue,
		}

		workItem := &ports.WorkItem[CustomValue]{
			Message: message,
			Metrics: &nexus.Metrics{},
			Ctx:     context.Background(),
		}

		partition, offset := workItem.PartitionOffset()

		if partition != 15 {
			t.Errorf("Expected partition 15, got %d", partition)
		}
		if offset != 5555 {
			t.Errorf("Expected offset 5555, got %d", offset)
		}
	})

	t.Run("works with built-in types", func(t *testing.T) {
		messageInt := &nexus.Message[int]{
			Partition: 100,
			Offset:    2000,
			Payload:   new(int),
		}

		workItemInt := &ports.WorkItem[int]{
			Message: messageInt,
			Metrics: &nexus.Metrics{},
			Ctx:     context.Background(),
		}

		partition, offset := workItemInt.PartitionOffset()
		if partition != 100 || offset != 2000 {
			t.Errorf("Expected partition 100, offset 2000, got %d, %d", partition, offset)
		}

		// Test with []byte
		messageBytes := &nexus.Message[[]byte]{
			Partition: 200,
			Offset:    3000,
			Payload:   &[]byte{1, 2, 3},
		}

		workItemBytes := &ports.WorkItem[[]byte]{
			Message: messageBytes,
			Metrics: &nexus.Metrics{},
			Ctx:     context.Background(),
		}

		partition, offset = workItemBytes.PartitionOffset()
		if partition != 200 || offset != 3000 {
			t.Errorf("Expected partition 200, offset 3000, got %d, %d", partition, offset)
		}
	})

	t.Run("handles edge case values", func(t *testing.T) {
		message := &nexus.Message[string]{
			Partition: 0, // minimum partition
			Offset:    0, // minimum offset
			Payload:   new(string),
		}

		workItem := &ports.WorkItem[string]{
			Message: message,
			Ctx:     context.Background(),
		}

		partition, offset := workItem.PartitionOffset()

		if partition != 0 {
			t.Errorf("expected partition 0, got %d", partition)
		}
		if offset != 0 {
			t.Errorf("expected offset 0, got %d", offset)
		}

		message.Partition = 2147483647       // max int32
		message.Offset = 9223372036854775807 // max int64

		partition, offset = workItem.PartitionOffset()

		if partition != 2147483647 {
			t.Errorf("expected partition 2147483647, got %d", partition)
		}
		if offset != 9223372036854775807 {
			t.Errorf("expected offset 9223372036854775807, got %d", offset)
		}
	})
}

func TestWorkItem_PartitionOffsetPrevious(t *testing.T) {
	t.Run("returns correct partition, offset, and previous offset", func(t *testing.T) {
		message := &nexus.Message[string]{
			Partition: 42,
			Offset:    1234567890,
			Key:       "test-key",
			Payload:   new(string),
		}

		workItem := &ports.WorkItem[string]{
			Message:        message,
			Metrics:        &nexus.Metrics{},
			Ctx:            context.Background(),
			PreviousOffset: 1234567889,
		}

		partition, offset, prev := workItem.PartitionOffsetPrevious()

		if partition != 42 {
			t.Errorf("expected partition 42, got %d", partition)
		}
		if offset != 1234567890 {
			t.Errorf("expected offset 1234567890, got %d", offset)
		}
		if prev != 1234567889 {
			t.Errorf("expected previous offset 1234567889, got %d", prev)
		}
	})

	t.Run("handles gap in offsets", func(t *testing.T) {
		message := &nexus.Message[string]{
			Partition: 5,
			Offset:    100,
			Payload:   new(string),
		}

		workItem := &ports.WorkItem[string]{
			Message:        message,
			Ctx:            context.Background(),
			PreviousOffset: 50, // gap of 49 offsets (log compaction, control records)
		}

		partition, offset, prev := workItem.PartitionOffsetPrevious()

		if partition != 5 || offset != 100 || prev != 50 {
			t.Errorf("expected 5, 100, 50, got %d, %d, %d", partition, offset, prev)
		}
	})

	t.Run("handles zero previous offset", func(t *testing.T) {
		message := &nexus.Message[string]{
			Partition: 0,
			Offset:    1,
			Payload:   new(string),
		}

		workItem := &ports.WorkItem[string]{
			Message:        message,
			Ctx:            context.Background(),
			PreviousOffset: 0,
		}

		partition, offset, prev := workItem.PartitionOffsetPrevious()

		if partition != 0 || offset != 1 || prev != 0 {
			t.Errorf("expected 0, 1, 0, got %d, %d, %d", partition, offset, prev)
		}
	})
}
