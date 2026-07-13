// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// Package deadletter routes failed messages to the application's dead-letter sink.
//
// When ProcessMessage returns an error or panics, the [DeadLetter] writer invokes the
// application-provided WriteDeadLetter callback. This allows failed messages to be
// persisted for later investigation and potential replay, preventing silent data loss
// without blocking the pipeline.
//
// Dead-letter writes must be reliable. If WriteDeadLetter fails or panics, the circuit
// breaker triggers emergency shutdown because the llingr-demux framework refuses to
// silently drop messages.
package deadletter

import (
	"fmt"

	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

// DeadLetter writer routes messages that fail to process
// to the dead-letter sink registered on startup.
type DeadLetter[T any] struct {
	logger          nexus.Logger
	writeDeadLetter nexus.WriteDeadLetter[T]
}

// New dead-letter writer to route messages to the sink function
// registered on startup. Invoked when call to ProcessMessage fails.
func New[T any](writeDeadLetter nexus.WriteDeadLetter[T], logger nexus.Logger) *DeadLetter[T] {
	return &DeadLetter[T]{
		writeDeadLetter: writeDeadLetter,
		logger:          logger,
	}
}

const (
	prefix         = "dead-letter - partition: %d, offset: %d - "
	panicRecovered = prefix + "panic recovered - %v"
	writeFailed    = prefix + "write failed - %w"
	writing        = prefix + "caused by error: %v"
)

// Write sends message to the registered dead-letter sink.
//
// MUST BE RELIABLE: to avoid message loss, any failure writing
// a dead-letter triggers the emergency shutdown circuit-breaker.
func (dl *DeadLetter[T]) Write(workItem *ports.WorkItem[T], errReason error) (err error) {
	partition, offset := workItem.PartitionOffset()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf(panicRecovered, partition, offset, r)
			dl.logger.Error(workItem.Ctx, err.Error())
		} else if err != nil {
			err = fmt.Errorf(writeFailed, partition, offset, err)
			dl.logger.Error(workItem.Ctx, err.Error())
		}
	}()

	dl.logger.Warn(workItem.Ctx, fmt.Sprintf(writing, partition, offset, errReason))
	err = dl.writeDeadLetter(workItem.Ctx, workItem.Message, errReason)
	return err
}
