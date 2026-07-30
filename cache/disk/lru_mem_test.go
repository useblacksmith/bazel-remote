package disk

// Measurement of resident LRU metadata cost per entry: the heap bytes
// retained by one cache entry independent of blob size (key string, entry
// struct, list.Element, map slot). This is the constant behind the
// entry-count cap in the embedder's memory envelope; run with:
//
//	go test ./cache/disk/ -run TestLRUEntryMetadataCost -v
//
// It asserts only a loose sanity bound so drift in Go runtime internals
// does not break the build, and logs the measured value for use in sizing.

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"runtime"
	"testing"
)

func heapInUse() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

func TestLRUEntryMetadataCost(t *testing.T) {
	const numEntries = 1_000_000

	// Realistic lookup keys: "cas/<64 hex>" plus an lruItem with the
	// random filename component populated, matching what the startup scan
	// and Put paths store.
	keys := make([]string, numEntries)
	var buf [32]byte
	for i := range keys {
		if _, err := rand.Read(buf[:]); err != nil {
			t.Fatal(err)
		}
		keys[i] = "cas/" + hex.EncodeToString(buf[:])
	}

	lru := NewSizedLRU(int64(numEntries)*BlockSize, nil, 0)

	// Baseline after key generation so only LRU-internal allocations are
	// attributed to the per-entry cost... except the keys themselves are
	// retained by the LRU, so charge them too by measuring before keys
	// are considered reachable only from the LRU. Simplest correct
	// accounting: measure baseline before insertion, keep the keys slice
	// alive, and add the average key size explicitly afterwards.
	baseline := heapInUse()

	for i, k := range keys {
		if !lru.Add(k, lruItem{size: 1, sizeOnDisk: 1, random: fmt.Sprintf("%08d", i)}) {
			t.Fatalf("Add rejected entry %d", i)
		}
	}

	after := heapInUse()

	// Keep both the LRU and the keys reachable across the measurement, or
	// the GC inside heapInUse collects them first.
	if lru.Len() != numEntries {
		t.Fatalf("expected %d entries, got %d", numEntries, lru.Len())
	}
	runtime.KeepAlive(keys)

	perEntry := float64(after-baseline) / numEntries

	// The key bytes (~68 B) and its string header were allocated during
	// key generation and are retained by the LRU afterwards; count them
	// in the reported total.
	const keyBytes = 4 + 64
	total := perEntry + keyBytes

	t.Logf("LRU metadata per entry: %.0f B internal + %d B key = %.0f B total (%d entries, heap %d -> %d)",
		perEntry, keyBytes, total, numEntries, baseline, after)

	// Sanity bounds only; the log line is the deliverable.
	if total < 100 || total > 1000 {
		t.Fatalf("implausible per-entry metadata cost: %.0f B", total)
	}
}
