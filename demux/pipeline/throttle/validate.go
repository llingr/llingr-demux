// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package throttle

import "fmt"

const (
	minRate = 1
	maxRate = 5000
)

func validateRate(ratePerSec int) {
	if ratePerSec < minRate || ratePerSec > maxRate {
		panic(fmt.Sprintf("ratePerSec must be between %d and %d", minRate, maxRate))
	}
}
