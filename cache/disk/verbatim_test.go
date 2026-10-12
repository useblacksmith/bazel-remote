package disk

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/cache/disk/casblob"
	"github.com/buchgr/bazel-remote/v2/cache/disk/zstdimpl"
	testutils "github.com/buchgr/bazel-remote/v2/utils"
)

func newVerbatimTestCache(t *testing.T, workers int) *diskCache {
	c, err := New(tempDir(t), 1024*1024*1024, WithStorageMode("zstd"), WithVerbatimZstdUploads(workers))
	if err != nil {
		t.Fatal(err)
	}
	dc, ok := c.(*diskCache)
	if !ok {
		t.Fatalf("unexpected cache type %T", c)
	}
	return dc
}

func waitVerified(t *testing.T, c *diskCache) {
	deadline := time.Now().Add(10 * time.Second)
	for c.verifier.pending.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("verifier did not drain")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPutZstdStoresAsReceivedAndVerifies(t *testing.T) {
	c := newVerbatimTestCache(t, 1)
	ctx := context.Background()
	zstd, _ := zstdimpl.Get("go")

	const size = 2*1024*1024 + 77
	data, hash := testutils.RandomDataAndHash(size)
	frame := zstd.EncodeAll(data)

	if err := c.PutZstd(ctx, hash, size, bytes.NewReader(frame)); err != nil {
		t.Fatal(err)
	}
	waitVerified(t, c)

	if ok, _ := c.Contains(ctx, cache.CAS, hash, size); !ok {
		t.Fatal("verified blob should still be present")
	}
	rc, _, err := c.Get(ctx, cache.CAS, hash, size, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read back mismatch: %v", err)
	}
	rc, _, err = c.GetZstd(ctx, hash, size, 0)
	if err != nil {
		t.Fatal(err)
	}
	compressed, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if got, err = zstd.DecodeAll(compressed); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("compressed read back mismatch: %v", err)
	}
}

func TestPutZstdEvictsOnDigestMismatch(t *testing.T) {
	c := newVerbatimTestCache(t, 1)
	ctx := context.Background()
	zstd, _ := zstdimpl.Get("go")

	const size = 4096
	data, _ := testutils.RandomDataAndHash(size)
	_, wrongHash := testutils.RandomDataAndHash(size)
	frame := zstd.EncodeAll(data)

	if err := c.PutZstd(ctx, wrongHash, size, bytes.NewReader(frame)); err != nil {
		t.Fatal(err)
	}
	waitVerified(t, c)
	if ok, _ := c.Contains(ctx, cache.CAS, wrongHash, size); ok {
		t.Fatal("blob with wrong digest should have been evicted")
	}

	// Declared size far below the body: rejected at write time, since no
	// valid encoding of that many bytes can be that long.
	_, hash := testutils.RandomDataAndHash(size)
	if err := c.PutZstd(ctx, hash, size/2, bytes.NewReader(frame)); err == nil {
		t.Fatal("expected oversized body to be rejected")
	}
	if ok, _ := c.Contains(ctx, cache.CAS, hash, size/2); ok {
		t.Fatal("rejected blob should not be present")
	}

	// Declared size slightly below the decoded size: passes the write
	// bound, caught by the verifier.
	if err := c.PutZstd(ctx, hash, size-1, bytes.NewReader(frame)); err != nil {
		t.Fatal(err)
	}
	waitVerified(t, c)
	if ok, _ := c.Contains(ctx, cache.CAS, hash, size-1); ok {
		t.Fatal("blob with wrong size should have been evicted")
	}

	// Garbage that is not zstd at all.
	if err := c.PutZstd(ctx, hash, size, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	waitVerified(t, c)
	if ok, _ := c.Contains(ctx, cache.CAS, hash, size); ok {
		t.Fatal("undecodable blob should have been evicted")
	}
}

type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	c.n++
	return c.r.Read(p)
}

func TestPutZstdUnavailableDoesNotConsumeInput(t *testing.T) {
	ctx := context.Background()
	_, hash := testutils.RandomDataAndHash(10)

	disabled := newVerbatimTestCache(t, 0)
	cr := &countingReader{r: bytes.NewReader([]byte("x"))}
	if err := disabled.PutZstd(ctx, hash, 10, cr); err != ErrVerbatimUnavailable {
		t.Fatalf("expected ErrVerbatimUnavailable, got %v", err)
	}

	enabled := newVerbatimTestCache(t, 1)
	if err := enabled.PutZstd(ctx, hash, casblob.MaxVerbatimZstdSize+1, cr); err != ErrVerbatimUnavailable {
		t.Fatalf("expected ErrVerbatimUnavailable for oversized blob, got %v", err)
	}
	if cr.n != 0 {
		t.Fatalf("reader was read %d times", cr.n)
	}
}
