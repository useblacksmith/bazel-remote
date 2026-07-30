package disk

// Regression tests for bounded zstd encoder admission at the disk layer:
// a saturated (or canceled) encoder acquisition must fail only the current
// request. It must never evict the LRU entry, which would turn transient
// overload into cache churn and re-uploads.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/cache/disk/zstdimpl"
	testutils "github.com/buchgr/bazel-remote/v2/utils"

	"github.com/klauspost/compress/zstd"
)

func TestZstdEncoderSaturationDoesNotEvict(t *testing.T) {
	cacheDir := testutils.TempDir(t)

	testCacheI, err := New(cacheDir, 100*1024*1024,
		WithStorageMode("uncompressed"),
		WithZstdLimits(zstdimpl.ZstdLimits{
			MaxActiveEncoders:       1,
			EncoderAdmissionTimeout: 20 * time.Millisecond,
		}),
		WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	testCache := testCacheI.(*diskCache)

	ctx := context.Background()

	const itemSize = 256 * 1024
	data, hash := testutils.RandomDataAndHash(itemSize)
	err = testCache.Put(ctx, cache.CAS, hash, itemSize, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	// The first zstd read holds the only encoder slot until closed.
	held, foundSize, err := testCache.GetZstd(ctx, hash, itemSize, 0)
	if err != nil {
		t.Fatal(err)
	}
	if held == nil || foundSize != itemSize {
		t.Fatalf("expected hit of size %d, got rc=%v size=%d", itemSize, held, foundSize)
	}

	// A second zstd read must fail with saturation...
	rc, _, err := testCache.GetZstd(ctx, hash, itemSize, 0)
	if rc != nil {
		_ = rc.Close()
	}
	if !errors.Is(err, zstdimpl.ErrEncoderSaturated) {
		t.Fatalf("expected ErrEncoderSaturated, got %v", err)
	}

	// ...without evicting the entry.
	found, sz := testCache.Contains(ctx, cache.CAS, hash, itemSize)
	if !found || sz != itemSize {
		t.Fatalf("saturation must not evict the entry: found=%v size=%d", found, sz)
	}

	// An identity read must be unaffected by encoder saturation.
	idrc, _, err := testCache.Get(ctx, cache.CAS, hash, itemSize, 0)
	if err != nil || idrc == nil {
		t.Fatalf("identity read failed under encoder saturation: %v", err)
	}
	idData, err := io.ReadAll(idrc)
	_ = idrc.Close()
	if err != nil || !bytes.Equal(idData, data) {
		t.Fatalf("identity read returned wrong data under saturation: %v", err)
	}

	// Releasing the held encoder makes zstd reads work again, end to end.
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}

	rc2, _, err := testCache.GetZstd(ctx, hash, itemSize, 0)
	if err != nil {
		t.Fatalf("expected zstd read to succeed after release: %v", err)
	}
	compressed, err := io.ReadAll(rc2)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc2.Close(); err != nil {
		t.Fatal(err)
	}

	dec, err := zstd.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	decompressed, err := io.ReadAll(dec)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decompressed, data) {
		t.Fatal("zstd read after release returned wrong data")
	}
}

func TestZstdEncoderCancellationDoesNotEvict(t *testing.T) {
	cacheDir := testutils.TempDir(t)

	testCacheI, err := New(cacheDir, 100*1024*1024,
		WithStorageMode("uncompressed"),
		WithZstdLimits(zstdimpl.ZstdLimits{MaxActiveEncoders: 1}),
		WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	testCache := testCacheI.(*diskCache)

	ctx := context.Background()

	const itemSize = 64 * 1024
	data, hash := testutils.RandomDataAndHash(itemSize)
	err = testCache.Put(ctx, cache.CAS, hash, itemSize, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}

	held, _, err := testCache.GetZstd(ctx, hash, itemSize, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	rc, _, err := testCache.GetZstd(canceledCtx, hash, itemSize, 0)
	if rc != nil {
		_ = rc.Close()
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	found, _ := testCache.Contains(ctx, cache.CAS, hash, itemSize)
	if !found {
		t.Fatal("cancellation must not evict the entry")
	}
}
