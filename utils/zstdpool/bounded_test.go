package zstdpool_test

import (
	"bytes"
	"io"
	"math/rand"
	"testing"

	"github.com/buchgr/bazel-remote/v2/utils/zstdpool"

	"github.com/klauspost/compress/zstd"
)

func compressForTest(t *testing.T, input []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := zstd.NewWriter(&buf,
		zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := enc.Write(input); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestBoundedDecoderPoolRoundTrip(t *testing.T) {
	pool := zstdpool.NewBoundedDecoderPool(1)

	input := make([]byte, 1<<20)
	rand.New(rand.NewSource(3)).Read(input)
	compressed := compressForTest(t, input)

	// Multiple sequential uses exercise both the create and reuse paths.
	for i := 0; i < 3; i++ {
		rc, err := pool.GetDecoder(bytes.NewReader(compressed))
		if err != nil {
			t.Fatal(err)
		}
		decompressed, err := io.ReadAll(rc)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(decompressed, input) {
			t.Fatalf("iteration %d: roundtrip mismatch", i)
		}
		if err := rc.Close(); err != nil {
			t.Fatal(err)
		}
		// Close must be idempotent: the write path's deferred cleanup can
		// race a completed put's explicit close.
		if err := rc.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBoundedDecoderPoolConcurrentUseAndRetention(t *testing.T) {
	const retained = 2
	pool := zstdpool.NewBoundedDecoderPool(retained)

	input := make([]byte, 256<<10)
	rand.New(rand.NewSource(4)).Read(input)
	compressed := compressForTest(t, input)

	// Hold more concurrent decoders than the retention limit. Creation is
	// unbounded by design (the embedder bounds concurrency upstream), so
	// all of these must succeed.
	const concurrent = 5
	readers := make([]io.ReadCloser, 0, concurrent)
	for i := 0; i < concurrent; i++ {
		rc, err := pool.GetDecoder(bytes.NewReader(compressed))
		if err != nil {
			t.Fatal(err)
		}
		readers = append(readers, rc)
	}

	for _, rc := range readers {
		decompressed, err := io.ReadAll(rc)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(decompressed, input) {
			t.Fatal("roundtrip mismatch")
		}
		if err := rc.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// After the burst, later gets must still work (mix of retained and
	// fresh decoders).
	rc, err := pool.GetDecoder(bytes.NewReader(compressed))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(rc); err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
}
