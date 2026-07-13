// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package fnv

import (
	"fmt"
	fnvstdlib "hash/fnv"
	"testing"
)

func TestHashIndex(t *testing.T) {
	// bit masks
	bitMasks := []uint32{
		0,  // 1 bucket
		1,  // 2 buckets
		3,  // 4
		7,  // 8
		15, // 16...
		31,
		63,
		127,
	}

	partitionKeys := generateTestKeys()

	for _, bitMask := range bitMasks {
		maxIndex := bitMask
		bucketCounts := make(map[uint32]int)

		for _, key := range partitionKeys {
			index := HashIndex(key, bitMask)

			// index should be within expected range
			if index > maxIndex {
				t.Errorf("HashIndex(%q, %d) = %d, want <= %d", key, bitMask, index, maxIndex)
			}

			bucketCounts[index]++
		}

		// basic bucket distribution check
		expectedBuckets := maxIndex + 1
		if uint32(len(bucketCounts)) != expectedBuckets { //nolint:gosec // G115: len bounded by bitMask
			const bucketDistributionIssue = "bitMask %d, got %d unique indices, want %d"
			t.Errorf(bucketDistributionIssue, bitMask, len(bucketCounts), expectedBuckets)
		}
	}
}

func TestHashIndexConsistency(t *testing.T) {
	key := "test-partition-key"
	bitMask := uint32(15)

	// same input should always produce same output
	first := HashIndex(key, bitMask)
	for i := 0; i < 100; i++ {
		result := HashIndex(key, bitMask)
		if result != first {
			t.Errorf("HashIndex not consistent: first=%d, iteration %d=%d", first, i, result)
		}
	}
}

func TestHashIndexDistribution(t *testing.T) {
	bitMask := uint32(31) // 32 buckets
	numKeys := 1000
	keys := generateRandomKeys(numKeys, 10)

	bucketCounts := make(map[uint32]int)
	for _, key := range keys {
		index := HashIndex(key, bitMask)
		bucketCounts[index]++
	}

	// check distribution isn't too skewed
	// with good hash function, each bucket should get roughly numKeys/32 items
	expectedPerBucket := float64(numKeys) / float64(bitMask+1)
	tolerance := expectedPerBucket * 0.5 // Allow 50% variance

	for bucket, count := range bucketCounts {
		if float64(count) < expectedPerBucket-tolerance || float64(count) > expectedPerBucket+tolerance {
			t.Logf("Bucket %d has %d items (expected ~%.1f ± %.1f)",
				bucket, count, expectedPerBucket, tolerance)
		}
	}
}

func TestHashIndexEdgeCases(t *testing.T) {
	tests := []struct {
		key     string
		bitMask uint32
	}{
		{"", 7},
		{"a", 0},
		{"x", 255},
		{string(make([]byte, 1000)), 15},
	}

	for _, tt := range tests {
		index := HashIndex(tt.key, tt.bitMask)
		if index > tt.bitMask {
			t.Errorf("HashIndex(%q, %d) = %d, want <= %d",
				tt.key, tt.bitMask, index, tt.bitMask)
		}
	}
}

// generateTestKeys creates partition keys of length 1-50 with various patterns
func generateTestKeys() []string {
	var keys []string

	// sequential patterns
	for length := 1; length <= 50; length++ {
		// numbers
		key := ""
		for i := 0; i < length; i++ {
			key += string(rune('0' + (i % 10)))
		}
		keys = append(keys, key)

		// letters
		key = ""
		for i := 0; i < length; i++ {
			key += string(rune('a' + (i % 26)))
		}
		keys = append(keys, key)
	}

	return append(keys, generateRandomKeys(10_000, 25)...)
}

// generateRandomKeys creates n deterministic strings using simple LCG
func generateRandomKeys(n, avgLen int) []string {
	var keys []string

	chars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"

	// Simple Linear Congruential Generator for deterministic "randomness"
	seed := uint64(12345)
	lcg := func() uint64 {
		seed = (seed*1103515245 + 12345) & 0x7fffffff
		return seed
	}

	for i := 0; i < n; i++ {
		length := int(lcg()%uint64(avgLen*2)) + 1 //nolint:gosec // G115: result bounded by avgLen*2
		key := make([]byte, length)
		for j := range key {
			key[j] = chars[lcg()%uint64(len(chars))]
		}
		keys = append(keys, string(key))
	}

	return keys
}

func TestHashIndexVsStdlib(t *testing.T) {
	bitMasks := []uint32{0, 1, 3, 7, 15, 31, 63, 127}
	keys := generateStdlibComparisonKeys(5000)

	for _, bitMask := range bitMasks {
		t.Run(formatBitMask(bitMask), func(t *testing.T) {
			for i, key := range keys {
				customResult := HashIndex(key, bitMask)
				stdlibResult := stdlibFnvHash(key, bitMask)

				// results should be identical
				if customResult != stdlibResult {
					t.Errorf("key %d: HashIndex(%q, %d) = %d, stdlib = %d",
						i, key, bitMask, customResult, stdlibResult)
				}
			}
		})
	}
}

// stdlibFnvHash implements FNV-1a using stdlib hash/fnv
func stdlibFnvHash(key string, bitMask uint32) uint32 {
	h := fnvstdlib.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32() & bitMask
}

// generateStdlibComparisonKeys creates test keys with more variety for stdlib comparison
func generateStdlibComparisonKeys(n int) []string {
	var keys []string

	// deterministic LCG for reproducible tests
	seed := uint64(54321) // different from main tests
	lcg := func() uint64 {
		seed = (seed*1103515245 + 12345) & 0x7fffffff
		return seed
	}

	chars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_./:"

	for i := 0; i < n; i++ {
		// Variable lengths from 1 to 50
		length := int(lcg()%50) + 1 //nolint:gosec // G115: result bounded by 50
		key := make([]byte, length)
		for j := range key {
			key[j] = chars[lcg()%uint64(len(chars))]
		}
		keys = append(keys, string(key))
	}

	// patterns that might reveal differences
	keys = append(keys,
		"",
		"a", "ab", "abc", "abcd",
		"00000000-0000-0000-0000-000000000000",
		"ffffffff-ffff-ffff-ffff-ffffffffffff",
		"12345678-1234-1234-1234-123456789012",
		string(make([]byte, 100)), // 100 nil bytes
	)
	b := make([]byte, 100) // irregular nil/value
	b[40] = byte(78)
	b[80] = byte(10)
	b[65] = byte(10)
	keys = append(keys, string(b))
	return keys
}

// formatBitMask creates test name from bit mask
func formatBitMask(bitMask uint32) string {
	buckets := bitMask + 1
	if buckets == 1 {
		return "1_bucket"
	}
	return fmt.Sprintf("%d_buckets", buckets)
}

// Benchmark custom implementation vs stdlib
func BenchmarkHashIndexCustom(b *testing.B) {
	uuid := "12345678-1234-1234-1234-123456789012"
	bitMask := uint32(127)

	for i := 0; i < b.N; i++ {
		_ = HashIndex(uuid, bitMask)
	}
}

func BenchmarkHashIndexStdlib(b *testing.B) {
	uuid := "12345678-1234-1234-1234-123456789012"
	bitMask := uint32(127)

	for i := 0; i < b.N; i++ {
		_ = stdlibFnvHash(uuid, bitMask)
	}
}

// BenchmarkHashIndexCustomMixed mixed workloads
func BenchmarkHashIndexCustomMixed(b *testing.B) {
	keys := []string{
		"12345678-1234-1234-1234-123456789012",
		"key-123456",
		"partition-key-medium-length",
		"a",
		"very-long-partition-key-with-lots-of-characters-for-performance-testing",
	}
	bitMask := uint32(15)

	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			_ = HashIndex(key, bitMask)
		}
	}
}

func BenchmarkHashIndexStdlibMixed(b *testing.B) {
	keys := []string{
		"12345678-1234-1234-1234-123456789012",
		"user-12345",
		"partition-key-medium-length",
		"a",
		"very-long-partition-key-with-lots-of-characters-for-performance-testing",
	}
	bitMask := uint32(15)

	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			_ = stdlibFnvHash(key, bitMask)
		}
	}
}
