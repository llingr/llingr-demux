// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package subscription

import (
	"fmt"
	"time"
)

const (
	paused    = "polling loop paused on topic: %s"
	resuming  = "polling loop resuming on topic: %s"
	cancelled = "polling loop cancelled on topic: %s"
	stopped   = "polling loop stopped on topic: %s"
	pollError = "error polling topic: %s - %v"
)

// PollAndForward orchestrates message ingestion from Kafka into the demux pipeline.
// Messages enter pipeline before rebalance handling to prevent stragglers.
func (s *Subscription[T]) PollAndForward(pollTimeout time.Duration) {
	defer func() {
		s.logger.Info(s.ctx, "subscription: stopping polling loop")
	}()

	// lift from interface once - keeps fn pointer on stack frame
	processMessage := s.processor.Process

	var (
		now   = time.Now()
		delta time.Duration
	)

	for {
		select {
		case <-s.pausePolling:
			s.logger.Debug(s.ctx, fmt.Sprintf(paused, s.topicName))

			<-s.resumePolling
			s.logger.Debug(s.ctx, fmt.Sprintf(resuming, s.topicName))

		default:
			select {
			case <-s.mainCtxDone:
				s.logger.Debug(s.ctx, fmt.Sprintf(cancelled, s.topicName))
				return
			case <-s.stopPolling:
				s.logger.Debug(s.ctx, fmt.Sprintf(stopped, s.topicName))
				return

			default:
				// ok indicates if T is present, type-agnostic for any T (pointer, struct, interface...)
				if message, ok, err := s.poll(pollTimeout); ok {

					delta = time.Since(now)
					if delta > time.Second {
						now = time.Now()
						delta = 0
					}
					// blocks until there is an available Worker
					if err = processMessage(message, now.Add(delta)); err != nil {
						s.circuitBreaker.TriggerEmergencyShutdown(err)
						return
					}

				} else if err != nil {
					s.logger.Error(s.ctx, fmt.Sprintf(pollError, s.topicName, err))
				}
			}
		}
	}
}
