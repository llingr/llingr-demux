// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0
//
// Implementation of FNV-1a hash algorithm (the algorithm itself is in the public domain).

// Package fnv provides a zero-allocation FNV-1a hash for partition key routing.
//
// [HashIndex] hashes a partition key and applies a bitmask to select a worker shard.
package fnv

const (
	offsetBasis = uint32(2166136261)
	primeFactor = uint32(16777619)
)

// HashIndex zero-allocation implementation of FNV-1a algorithm
// avoids string to []byte copy behaviour in stdlib hash/fnv.
func HashIndex(partitionKey string, bitMask uint32) uint32 {
	h := offsetBasis
	for i := 0; i < len(partitionKey); i++ {
		h ^= uint32(partitionKey[i])
		h *= primeFactor
	}
	return h & bitMask
}
