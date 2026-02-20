package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	bitcask "github.com/201901407/bitcask/store"
)

const testDataDir = "./data"

// cleanDataDir removes all segment files written during a benchmark run.
func cleanDataDir() {
	os.RemoveAll(testDataDir)
}

// newStore initializes a BitcaskKVStore and immediately stops the background
// compaction goroutine so it cannot interfere with benchmark measurements.
func newStore(b *testing.B) *bitcask.BitcaskKVStore {
	b.Helper()
	kvStore := &bitcask.BitcaskKVStore{}
	if err := kvStore.Init(); err != nil {
		b.Fatalf("Init: %v", err)
	}
	kvStore.StopCompactionBackground()
	return kvStore
}

// writeSegmentFile writes a bitcask-format segment file at path containing
// the provided key-value pairs. The format matches what extractKeysFromSegmentFile
// expects: a 10-byte header (4B timestamp, 2B keySize, 4B valueSize) followed by
// the raw key and value bytes.
func writeSegmentFile(path string, pairs []struct{ key, value string }) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	ts := uint32(time.Now().Unix())
	for _, p := range pairs {
		keyBytes := []byte(p.key)
		valBytes := []byte(p.value)

		header := make([]byte, 10)
		binary.LittleEndian.PutUint32(header[0:4], ts)
		binary.LittleEndian.PutUint16(header[4:6], uint16(len(keyBytes)))
		binary.LittleEndian.PutUint32(header[6:10], uint32(len(valBytes)))

		record := append(append(header, keyBytes...), valBytes...)
		if _, err := f.Write(record); err != nil {
			return err
		}
	}
	return nil
}

// prepareSegments creates numSegments bitcask segment files under testDataDir,
// each holding keysPerSeg unique key-value pairs.
//
// Files are given synthetic timestamps well in the past (seconds since epoch,
// not milliseconds) so they sort before the active segment that Init() creates
// at time.Now().UnixMilli(). This guarantees Init() loads them all into
// SegmentTracker, giving Compact() real closed segments to process.
func prepareSegments(b *testing.B, numSegments, keysPerSeg int) {
	b.Helper()
	if err := os.MkdirAll(testDataDir, 0755); err != nil {
		b.Fatalf("os.MkdirAll: %v", err)
	}

	keyIdx := 0
	for s := range numSegments {
		// 1_000_000_000 seconds ≈ year 2001; safely below any real UnixMilli timestamp.
		ts := int64(1_000_000_000) + int64(s)
		path := filepath.Join(testDataDir, fmt.Sprintf("bitcask_segment_%d", ts))

		pairs := make([]struct{ key, value string }, keysPerSeg)
		for k := range keysPerSeg {
			pairs[k] = struct{ key, value string }{
				key:   fmt.Sprintf("key_%d", keyIdx),
				value: fmt.Sprintf("value_%d", keyIdx),
			}
			keyIdx++
		}

		if err := writeSegmentFile(path, pairs); err != nil {
			b.Fatalf("writeSegmentFile[%d]: %v", s, err)
		}
	}
}

// BenchmarkSequentialWrites measures the throughput of appending keys in order.
func BenchmarkSequentialWrites(b *testing.B) {
	cleanDataDir()
	defer cleanDataDir()

	kvStore := newStore(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kvStore.Set(fmt.Sprintf("key_%d", i), fmt.Sprintf("value_%d", i))
	}
}

// BenchmarkRandomWrites measures the throughput of writes with random keys.
func BenchmarkRandomWrites(b *testing.B) {
	cleanDataDir()
	defer cleanDataDir()

	kvStore := newStore(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kvStore.Set(fmt.Sprintf("key_%d", rand.Intn(1000000)), fmt.Sprintf("value_%d", i))
	}
}

// BenchmarkSequentialReads measures Get latency with a sequential access pattern.
func BenchmarkSequentialReads(b *testing.B) {
	cleanDataDir()
	defer cleanDataDir()

	kvStore := newStore(b)
	const numKeys = 100000
	for i := range numKeys {
		kvStore.Set(fmt.Sprintf("key_%d", i), fmt.Sprintf("value_%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := kvStore.Get(fmt.Sprintf("key_%d", i%numKeys)); err != nil {
			b.Fatalf("Get: %v", err)
		}
	}
}

// BenchmarkRandomReads measures Get latency with a uniformly random access pattern.
func BenchmarkRandomReads(b *testing.B) {
	cleanDataDir()
	defer cleanDataDir()

	kvStore := newStore(b)
	const numKeys = 100000
	for i := range numKeys {
		kvStore.Set(fmt.Sprintf("key_%d", i), fmt.Sprintf("value_%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := kvStore.Get(fmt.Sprintf("key_%d", rand.Intn(numKeys))); err != nil {
			b.Fatalf("Get: %v", err)
		}
	}
}

// BenchmarkMixedWorkload measures throughput under a 70 % read / 30 % write mix.
func BenchmarkMixedWorkload(b *testing.B) {
	cleanDataDir()
	defer cleanDataDir()

	kvStore := newStore(b)
	const numKeys = 100000
	for i := range numKeys {
		kvStore.Set(fmt.Sprintf("key_%d", i), fmt.Sprintf("value_%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if rand.Float32() < 0.7 {
			kvStore.Get(fmt.Sprintf("key_%d", rand.Intn(numKeys)))
		} else {
			kvStore.Set(fmt.Sprintf("key_%d", rand.Intn(numKeys)), fmt.Sprintf("value_%d", i))
		}
	}
}

// BenchmarkLargeValues measures write throughput for 1 KB values.
func BenchmarkLargeValues(b *testing.B) {
	cleanDataDir()
	defer cleanDataDir()

	kvStore := newStore(b)
	largeValue := string(make([]byte, 1024))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kvStore.Set(fmt.Sprintf("key_%d", i), largeValue)
	}
}

// BenchmarkCompaction measures a full compaction pass over 6 real closed segments
// (above COMPACTION_THRESHOLD = 5). Each iteration starts from a freshly written set
// of segment files so every Compact() call does real read-merge-write I/O instead
// of returning early because SegmentTracker is empty or keysToCompact is zero.
func BenchmarkCompaction(b *testing.B) {
	const (
		numSegments    = 6    // > COMPACTION_THRESHOLD (5)
		keysPerSegment = 5000 // 30 000 total keys across all segments
	)

	b.Cleanup(cleanDataDir)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cleanDataDir()
		prepareSegments(b, numSegments, keysPerSegment)
		// newStore calls Init() which loads all prepared files into SegmentTracker,
		// then stops background compaction so only our explicit Compact() call runs.
		kvStore := newStore(b)
		b.StartTimer()

		if err := kvStore.Compact(); err != nil {
			b.Fatalf("Compact: %v", err)
		}
	}
}
