package main

import (
	"fmt"
	"math/rand"
	"testing"

	bitcask "github.com/201901407/bitcask/store"
)

// Benchmark sequential writes
func BenchmarkSequentialWrites(b *testing.B) {
	kvStore := &bitcask.BitcaskKVStore{}
	kvStore.Init() // or however you initialize

	b.ResetTimer() // Start timing after setup
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		kvStore.Set(key, value)
	}
}

// Benchmark random writes
func BenchmarkRandomWrites(b *testing.B) {
	kvStore := &bitcask.BitcaskKVStore{}
	kvStore.Init()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", rand.Intn(1000000))
		value := fmt.Sprintf("value_%d", i)
		kvStore.Set(key, value)
	}
}

// Benchmark sequential reads
func BenchmarkSequentialReads(b *testing.B) {
	kvStore := &bitcask.BitcaskKVStore{}
	kvStore.Init()

	// Pre-populate with data
	numKeys := 100000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		kvStore.Set(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i%numKeys)
		
		b.StartTimer()
		val, err := kvStore.Get(key)
		b.StopTimer()

		if err != nil {
			b.Errorf("Get error for %s: %v", key, err)
			continue
		}

		var idx int
		if _, err := fmt.Sscanf(key, "key_%d", &idx); err != nil {
			b.Errorf("failed to parse key %s: %v", key, err)
			continue
		}
		expected := fmt.Sprintf("value_%d", idx)
		if val != expected {
			b.Errorf("unexpected value for %s: got %q want %q", key, val, expected)
		}
	}
}

// Benchmark random reads
func BenchmarkRandomReads(b *testing.B) {
	kvStore := &bitcask.BitcaskKVStore{}
	kvStore.Init()

	// Pre-populate
	numKeys := 100000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		kvStore.Set(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", rand.Intn(numKeys))

		b.StartTimer()
		val, err := kvStore.Get(key)
		b.StopTimer()

		if err != nil {
			b.Errorf("Get error for %s: %v", key, err)
			continue
		}

		var idx int
		if _, err := fmt.Sscanf(key, "key_%d", &idx); err != nil {
			b.Errorf("failed to parse key %s: %v", key, err)
			continue
		}
		expected := fmt.Sprintf("value_%d", idx)
		if val != expected {
			b.Errorf("unexpected value for %s: got %q want %q", key, val, expected)
		}
	}
}

// Benchmark mixed workload (70% reads, 30% writes)
func BenchmarkMixedWorkload(b *testing.B) {
	kvStore := &bitcask.BitcaskKVStore{}
	kvStore.Init()

	// Pre-populate
	numKeys := 100000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		kvStore.Set(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if rand.Float32() < 0.7 {
			// Read
			key := fmt.Sprintf("key_%d", rand.Intn(numKeys))
			kvStore.Get(key)
		} else {
			// Write
			key := fmt.Sprintf("key_%d", rand.Intn(numKeys))
			value := fmt.Sprintf("value_%d", i)
			kvStore.Set(key, value)
		}
	}
}

// Benchmark with different value sizes
func BenchmarkLargeValues(b *testing.B) {
	kvStore := &bitcask.BitcaskKVStore{}
	kvStore.Init()

	// 1KB value
	largeValue := make([]byte, 1024)
	for i := range largeValue {
		largeValue[i] = byte('a')
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
		kvStore.Set(key, string(largeValue))
	}
}

// Benchmark compaction
func BenchmarkCompaction(b *testing.B) {
	kvStore := &bitcask.BitcaskKVStore{}
	kvStore.Init()

	// Create many segments by writing lots of data
	for i := 0; i < 50000; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		kvStore.Set(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kvStore.Compact()
	}
}
