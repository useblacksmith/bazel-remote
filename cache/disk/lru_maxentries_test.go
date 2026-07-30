package disk

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	testutils "github.com/buchgr/bazel-remote/v2/utils"
)

// Tests for the entry-count bound (SizedLRU.maxEntries / WithMaxEntries),
// which caps resident index metadata independently of the byte budget.

func TestMaxEntriesEvictsFromTail(t *testing.T) {
	var evicted []string
	onEvict := func(key string, value lruItem) {
		evicted = append(evicted, key)
	}

	lru := NewSizedLRU(math.MaxInt64, onEvict, 0)
	lru.maxEntries = 2

	for i := 0; i < 4; i++ {
		if !lru.Add(fmt.Sprintf("key-%d", i), lruItem{size: 1, sizeOnDisk: 1}) {
			t.Fatalf("Add of key-%d rejected", i)
		}
	}

	if lru.Len() != 2 {
		t.Fatalf("expected 2 entries, got %d", lru.Len())
	}
	for _, key := range []string{"key-2", "key-3"} {
		if _, ok := lru.Peek(key); !ok {
			t.Fatalf("expected %s to be resident", key)
		}
	}

	// Evicted entries reach onEvict through the queued-eviction channel,
	// which has no background consumer in LRU-only tests; drain it once
	// (all queued entries merge into a single slice).
	lru.performQueuedEvictions()
	if len(evicted) != 2 || evicted[0] != "key-0" || evicted[1] != "key-1" {
		t.Fatalf("expected [key-0 key-1] evicted in LRU order, got %v", evicted)
	}

	// The byte accounting must reflect the evictions: 2 resident single-
	// block entries.
	if lru.TotalSize() != 2*BlockSize {
		t.Fatalf("expected currentSize %d, got %d", 2*BlockSize, lru.TotalSize())
	}
}

func TestMaxEntriesBoundsZeroByteEntries(t *testing.T) {
	// Zero-byte entries charge nothing against the byte budget, so only
	// the entry-count bound limits them.
	lru := NewSizedLRU(math.MaxInt64, func(string, lruItem) {}, 0)
	lru.maxEntries = 10

	for i := 0; i < 100; i++ {
		if !lru.Add(fmt.Sprintf("zero-%d", i), lruItem{}) {
			t.Fatalf("Add of zero-%d rejected", i)
		}
	}

	if lru.Len() != 10 {
		t.Fatalf("expected 10 entries, got %d", lru.Len())
	}
	if lru.TotalSize() != 0 {
		t.Fatalf("expected zero currentSize, got %d", lru.TotalSize())
	}
}

func TestMaxEntriesOverwriteDoesNotEvict(t *testing.T) {
	var evicted []string
	onEvict := func(key string, value lruItem) {
		evicted = append(evicted, key)
	}

	lru := NewSizedLRU(math.MaxInt64, onEvict, 0)
	lru.maxEntries = 2

	if !lru.Add("a", lruItem{size: 1, sizeOnDisk: 1}) {
		t.Fatal("Add of a rejected")
	}
	if !lru.Add("b", lruItem{size: 1, sizeOnDisk: 1}) {
		t.Fatal("Add of b rejected")
	}

	// Overwriting a resident key at the cap must not change the count or
	// evict the other entry. (The overwrite itself queues the replaced
	// file for removal, which is not a count eviction but does reach
	// onEvict.)
	if !lru.Add("a", lruItem{size: 2, sizeOnDisk: 2}) {
		t.Fatal("overwrite of a rejected")
	}

	if lru.Len() != 2 {
		t.Fatalf("expected 2 entries, got %d", lru.Len())
	}
	if _, ok := lru.Peek("b"); !ok {
		t.Fatal("expected b to remain resident after overwrite of a")
	}

	lru.performQueuedEvictions()
	if len(evicted) != 1 || evicted[0] != "a" {
		t.Fatalf("expected only the replaced version of a in the eviction queue, got %v", evicted)
	}
}

func TestMaxEntriesDisabledByDefault(t *testing.T) {
	lru := NewSizedLRU(math.MaxInt64, func(string, lruItem) {}, 0)

	for i := 0; i < 1000; i++ {
		if !lru.Add(fmt.Sprintf("zero-%d", i), lruItem{}) {
			t.Fatalf("Add of zero-%d rejected", i)
		}
	}

	if lru.Len() != 1000 {
		t.Fatalf("expected 1000 entries with no count bound, got %d", lru.Len())
	}
}

func TestDiskCacheWithMaxEntries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheDir := tempDir(t)
	defer func() { _ = os.RemoveAll(cacheDir) }()

	const maxEntries = 4
	const numBlobs = 10
	const itemSize = int64(64)

	testCacheI, err := New(cacheDir, math.MaxInt64,
		WithAccessLogger(testutils.NewSilentLogger()),
		WithMaxEntries(maxEntries))
	if err != nil {
		t.Fatal(err)
	}
	testCache := testCacheI.(*diskCache)

	var hashes []string
	for i := 0; i < numBlobs; i++ {
		data, hash := testutils.RandomDataAndHash(itemSize)
		hashes = append(hashes, hash)
		err = testCache.Put(ctx, cache.CAS, hash, itemSize,
			io.NopCloser(bytes.NewReader(data)))
		if err != nil {
			t.Fatal(err)
		}
	}

	if testCache.lru.Len() != maxEntries {
		t.Fatalf("expected %d resident entries, got %d",
			maxEntries, testCache.lru.Len())
	}

	// The most recently put blobs are resident, the oldest are not.
	for _, hash := range hashes[numBlobs-maxEntries:] {
		if ok, _ := testCache.Contains(ctx, cache.CAS, hash, itemSize); !ok {
			t.Fatalf("expected recent blob %s to be resident", hash)
		}
	}
	for _, hash := range hashes[:numBlobs-maxEntries] {
		if ok, _ := testCache.Contains(ctx, cache.CAS, hash, itemSize); ok {
			t.Fatalf("expected old blob %s to have been evicted", hash)
		}
	}
}

func TestDiskCacheMaxEntriesTrimsOnLoad(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheDir := tempDir(t)
	defer func() { _ = os.RemoveAll(cacheDir) }()

	const maxEntries = 3
	const numBlobs = 8
	const itemSize = int64(64)

	// Fill a cache directory with no entry-count bound.
	unboundedI, err := New(cacheDir, math.MaxInt64,
		WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	unbounded := unboundedI.(*diskCache)
	for i := 0; i < numBlobs; i++ {
		data, hash := testutils.RandomDataAndHash(itemSize)
		err = unbounded.Put(ctx, cache.CAS, hash, itemSize,
			io.NopCloser(bytes.NewReader(data)))
		if err != nil {
			t.Fatal(err)
		}
	}
	if unbounded.lru.Len() != numBlobs {
		t.Fatalf("expected %d entries before reload, got %d",
			numBlobs, unbounded.lru.Len())
	}

	// Reloading the same directory with a bound trims it during load,
	// like a restart with a smaller maxSize. New only returns after the
	// eviction backlog has been removed from disk.
	boundedI, err := New(cacheDir, math.MaxInt64,
		WithAccessLogger(testutils.NewSilentLogger()),
		WithMaxEntries(maxEntries))
	if err != nil {
		t.Fatal(err)
	}
	bounded := boundedI.(*diskCache)

	if bounded.lru.Len() != maxEntries {
		t.Fatalf("expected %d entries after bounded reload, got %d",
			maxEntries, bounded.lru.Len())
	}
}
