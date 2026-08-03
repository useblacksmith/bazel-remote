package disk

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	testutils "github.com/buchgr/bazel-remote/v2/utils"
)

func TestOpenOwedBlobReturnsRawOnDiskBytes(t *testing.T) {
	ctx := context.Background()

	cacheDir := tempDir(t)
	defer func() { _ = os.RemoveAll(cacheDir) }()

	testCacheI, err := New(cacheDir, 1024*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	testCache := testCacheI.(*diskCache)

	data, hash := testutils.RandomDataAndHash(256)
	if err := testCache.Put(ctx, cache.CAS, hash, 256, io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatal(err)
	}

	rc, sizeOnDisk, err := testCache.OpenOwedBlob(ctx, cache.CAS, hash)
	if err != nil {
		t.Fatalf("OpenOwedBlob on a committed blob: %v", err)
	}
	raw, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(raw)) != sizeOnDisk {
		t.Fatalf("read %d raw bytes, want reported sizeOnDisk %d", len(raw), sizeOnDisk)
	}

	// The raw representation must be exactly the committed file's content —
	// the same bytes disk.Put streams to the proxy.
	key := cache.LookupKey(cache.CAS, hash)
	testCache.mu.Lock()
	item, el := testCache.lru.Get(key)
	testCache.mu.Unlock()
	if el == nil {
		t.Fatal("blob missing from LRU")
	}
	onDisk, err := os.ReadFile(cacheDir + "/" + testCache.FileLocation(cache.CAS, item.legacy, hash, item.size, item.random))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, onDisk) {
		t.Fatal("OpenOwedBlob bytes differ from the committed on-disk file")
	}

	// Unknown hash behaves like an evicted blob.
	_, unknownHash := testutils.RandomDataAndHash(64)
	if _, _, err := testCache.OpenOwedBlob(ctx, cache.CAS, unknownHash); err == nil {
		t.Fatal("OpenOwedBlob on an absent blob returned nil error")
	}
}

func TestOpenOwedBlobIsStoragePrefixScoped(t *testing.T) {
	cacheDir := tempDir(t)
	defer func() { _ = os.RemoveAll(cacheDir) }()

	testCacheI, err := New(cacheDir, 1024*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	testCache := testCacheI.(*diskCache)

	prefixCtx := cache.WithStoragePrefix(context.Background(), "tenant-a/prod")
	data, hash := testutils.RandomDataAndHash(128)
	if err := testCache.Put(prefixCtx, cache.CAS, hash, 128, io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatal(err)
	}

	// The prefix travels in the context, exactly as the sweeper rebuilds it.
	rc, _, err := testCache.OpenOwedBlob(prefixCtx, cache.CAS, hash)
	if err != nil {
		t.Fatalf("OpenOwedBlob with matching prefix ctx: %v", err)
	}
	_ = rc.Close()

	// Without the prefix the lookup key differs: no cross-tenant leakage.
	if _, _, err := testCache.OpenOwedBlob(context.Background(), cache.CAS, hash); err == nil {
		t.Fatal("OpenOwedBlob without the storage prefix found a prefix-scoped blob")
	}
}
