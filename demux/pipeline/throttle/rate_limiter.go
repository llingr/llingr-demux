// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package throttle

import "github.com/llingr/llingr-nexus/nexus"

// RateLimiter controls message processing rate.
//
// Rate-limiting is NOT normally recommended as it
// is generally better to throttle using concurrency,
// see: config.ConcurrentKeys
type RateLimiter[T any] interface {
	// Await blocks until the next message is permitted.
	// Called once per message before dispatch to a worker.
	Await(*nexus.Message[T])

	// Stop releases resources. Once this has been called,
	// the RateLimiter should no longer be used.
	Stop()
}
