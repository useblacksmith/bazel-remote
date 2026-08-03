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

// TestDiskCacheLoadsWithOwedLedgerDirPresent pins the wiring contract: the
// proxy config creates <cache dir>/s3-owed BEFORE disk.New scans the cache
// root (setProxy runs during config load), and the startup scan must skip it
// like lost+found instead of failing the boot with "unexpected dir".
func TestDiskCacheLoadsWithOwedLedgerDirPresent(t *testing.T) {
	cacheDir := tempDir(t)
	defer func() { _ = os.RemoveAll(cacheDir) }()

	if err := os.MkdirAll(cacheDir+"/"+cache.OwedLedgerDirName, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cacheDir+"/"+cache.OwedLedgerDirName+"/owed-uploads-test-deadbeef.json", []byte("[]"), 0o644); err != nil {
		t.Fatal(err)
	}

	testCacheI, err := New(cacheDir, 1024*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatalf("disk.New with %s present: %v", cache.OwedLedgerDirName, err)
	}

	// And again with content in the cache, exercising the populated-scan path.
	data, hash := testutils.RandomDataAndHash(64)
	if err := testCacheI.Put(context.Background(), cache.CAS, hash, 64, io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatal(err)
	}
	if _, err := New(cacheDir, 1024*1024, WithAccessLogger(testutils.NewSilentLogger())); err != nil {
		t.Fatalf("disk.New rescan with %s present: %v", cache.OwedLedgerDirName, err)
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
