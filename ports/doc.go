// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// Package ports - defines internal interfaces for dependency injection and testability.
//
// These interfaces follow the Ports and Adapters (Hexagonal) architecture pattern,
// abstracting concrete struct dependencies to enable unit testing with mock implementations.
//
// Each port is a minimal interface containing only the methods required by consumers,
// keeping the contract focused and easy to implement for testing.
//
// Available ports:
//
//   - ProcessorPort[T]: abstracts pipeline.Processor[T] for message processing
//   - CommitterPort[T]: abstracts offset.Committer[T] for offset tracking and commit
//   - MetricsPort[T]: abstracts metrics.Collector[T] for observability
//   - DrainCoordinatorPort: abstracts drain.Coordinator[T] for rebalance/shutdown draining
//   - CircuitBreakerPort: abstracts circuitbreaker.CircuitBreaker for emergency shutdown
//
// Performance note: For hot-path methods like ProcessorPort.Process, CommitterPort.CollectAndCommit,
// and MetricsPort.Collect, consumers should lift the method to a local variable at construction
// time to keep the function pointer on the stack frame and avoid interface dispatch overhead
// in tight loops. See NewDemux and NewCommitter for examples.
package ports
