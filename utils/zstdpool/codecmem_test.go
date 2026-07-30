package zstdpool_test

// Measures the live heap retained by active zstd codecs, replicating the
// production paths that create them:
//
//   - Read path: casblob.GetLegacyZstdReadCloser pulls an encoder from
//     zstdpool.GetEncoderPool() and streams an uncompressed CAS file
//     through it (one encoder per concurrent zstd read of an
//     identity-stored blob).
//   - Write path: server/grpc_bytestream.go pulls a decoder from
//     zstdpool.GetDecoderPool() for each compressed ByteStream write.
//
// The numbers inform the codec-admission budget for the bounded-transcoding
// design. Run with:
//
//	go test -v -run TestCodecMemory ./utils/zstdpool/ -count=1 -timeout 900s

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/buchgr/bazel-remote/v2/utils/zstdpool"

	"github.com/klauspost/compress/zstd"
	syncpool "github.com/mostynb/zstdpool-syncpool"
)

const chunkSize = 256 * 1024 // matches FA's readChunkSizeBytes

// sourceBlock is 1 MiB of ~2:1 compressible data (random bytes with every
// other 4 KiB page zeroed). Allocation behavior is size-driven, but keep the
// content realistic so encoder output paths are exercised.
var sourceBlock = func() []byte {
	b := make([]byte, 1<<20)
	rng := rand.New(rand.NewSource(42))
	rng.Read(b)
	for page := 0; page < len(b); page += 8192 {
		for i := page; i < page+4096 && i < len(b); i++ {
			b[i] = 0
		}
	}
	return b
}()

// liveHeap forces collection and returns the live heap. Two GC cycles also
// fully drain sync.Pool (primary -> victim -> dropped), so each scenario
// starts from a codec-free baseline.
func liveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// writeStream writes total bytes to w in chunkSize writes, mirroring how
// enc.ReadFrom(f) consumes an uncompressed CAS file.
func writeStream(w io.Writer, total int64) error {
	var written int64
	for written < total {
		n := int64(chunkSize)
		if total-written < n {
			n = total - written
		}
		off := written % int64(len(sourceBlock))
		end := off + n
		if end > int64(len(sourceBlock)) {
			end = int64(len(sourceBlock))
		}
		m, err := w.Write(sourceBlock[off:end])
		if err != nil {
			return err
		}
		written += int64(m)
	}
	return nil
}

// runConcurrent starts n copies of scenario, waits until all have streamed
// their data and are holding live codec state, then measures live heap and
// goroutine growth over the baseline.
//
// Each scenario receives a hold function to call between streaming and
// closing: it signals readiness and blocks until every goroutine's
// measurement is complete.
func runConcurrent(t *testing.T, label string, n int, scenario func(hold func()) error) {
	t.Helper()

	baselineHeap := liveHeap()
	baselineGoroutines := runtime.NumGoroutine()

	release := make(chan struct{})
	var ready sync.WaitGroup
	var done sync.WaitGroup
	errs := make(chan error, n)

	for i := 0; i < n; i++ {
		ready.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			readyOnce := sync.OnceFunc(ready.Done)
			hold := func() {
				readyOnce()
				<-release
			}
			// If the scenario errors before holding, still unblock ready.Wait.
			defer readyOnce()
			if err := scenario(hold); err != nil {
				errs <- err
			}
		}()
	}

	ready.Wait()
	activeHeap := liveHeap()
	activeGoroutines := runtime.NumGoroutine()

	close(release)
	done.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("%s: scenario error: %v", label, err)
	}

	// After all codecs are closed and returned to their pools, one GC keeps
	// pooled objects alive (victim cache); this is the retained cost of a
	// burst after it subsides.
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	pooledHeap := m.HeapAlloc

	total := float64(int64(activeHeap)-int64(baselineHeap)) / (1 << 20)
	perCodec := total / float64(n)
	pooled := float64(int64(pooledHeap)-int64(baselineHeap)) / (1 << 20)
	t.Logf("%-40s n=%-3d perCodec=%7.2f MiB  totalLive=%8.2f MiB  pooledAfter=%8.2f MiB  goroutines +%d",
		label, n, perCodec, total, pooled, activeGoroutines-baselineGoroutines)
}

// poolEncoderScenario mirrors casblob.GetLegacyZstdReadCloser +
// zstdimpl/gozstd.go exactly: Get from the production pool, Reset, stream,
// hold, Close, Put.
func poolEncoderScenario(streamBytes int64) func(hold func()) error {
	pool := zstdpool.GetEncoderPool()
	return func(hold func()) error {
		enc, ok := pool.Get().(*syncpool.EncoderWrapper)
		if !ok {
			return fmt.Errorf("encoder pool returned unexpected type")
		}
		enc.Reset(io.Discard)
		if err := writeStream(enc, streamBytes); err != nil {
			return err
		}
		hold()
		if err := enc.Close(); err != nil {
			return err
		}
		pool.Put(enc)
		return nil
	}
}

// poolDecoderScenario mirrors the grpc_bytestream.go write path: Get a
// decoder from the production pool, Reset onto a compressed stream, consume
// roughly half (so the decoder is mid-stream when measured), hold, Close.
func poolDecoderScenario(compressed []byte, uncompressedBytes int64) func(hold func()) error {
	pool := zstdpool.GetDecoderPool()
	return func(hold func()) error {
		dec, ok := pool.Get().(*syncpool.DecoderWrapper)
		if !ok {
			return fmt.Errorf("decoder pool returned unexpected type")
		}
		if err := dec.Reset(bytes.NewReader(compressed)); err != nil {
			return err
		}
		rc := dec.IOReadCloser()
		if _, err := io.CopyN(io.Discard, rc, uncompressedBytes/2); err != nil {
			return err
		}
		hold()
		if _, err := io.Copy(io.Discard, rc); err != nil {
			return err
		}
		return rc.Close()
	}
}

// rawEncoderScenario measures alternative encoder configurations to inform
// per-codec footprint reduction options.
func rawEncoderScenario(streamBytes int64, opts ...zstd.EOption) func(hold func()) error {
	return func(hold func()) error {
		enc, err := zstd.NewWriter(io.Discard, opts...)
		if err != nil {
			return err
		}
		if err := writeStream(enc, streamBytes); err != nil {
			return err
		}
		hold()
		return enc.Close()
	}
}

func compressBlob(t *testing.T, uncompressedBytes int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := zstd.NewWriter(&buf,
		zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		t.Fatal(err)
	}
	if err := writeStream(enc, uncompressedBytes); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestCodecMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("memory measurement is not a short test")
	}

	const mib = int64(1 << 20)

	// Production encoder pool across blob sizes: shows how the retained
	// window grows with the streamed size.
	for _, streamMiB := range []int64{1, 8, 16, 64} {
		label := fmt.Sprintf("pool-encoder stream=%dMiB", streamMiB)
		runConcurrent(t, label, 16, poolEncoderScenario(streamMiB*mib))
	}

	// Incident emulation: 54 concurrent reads averaging ~9 MiB declared
	// data (499 MiB across 54 active ByteStream reads).
	runConcurrent(t, "pool-encoder INCIDENT stream=9MiB", 54, poolEncoderScenario(9*mib))

	// Alternative encoder configurations, same 16 MiB stream.
	base := []zstd.EOption{
		zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest),
	}
	runConcurrent(t, "raw-encoder default (pool config)", 16,
		rawEncoderScenario(16*mib, base...))
	runConcurrent(t, "raw-encoder window=1MiB", 16,
		rawEncoderScenario(16*mib, append(append([]zstd.EOption{}, base...), zstd.WithWindowSize(1<<20))...))
	runConcurrent(t, "raw-encoder window=256KiB", 16,
		rawEncoderScenario(16*mib, append(append([]zstd.EOption{}, base...), zstd.WithWindowSize(256<<10))...))
	runConcurrent(t, "raw-encoder lowerEncoderMem", 16,
		rawEncoderScenario(16*mib, append(append([]zstd.EOption{}, base...), zstd.WithLowerEncoderMem(true))...))

	// Production decoder pool (write path), mid-stream on a 16 MiB blob.
	compressed := compressBlob(t, 16*mib)
	runConcurrent(t, "pool-decoder stream=16MiB", 16,
		poolDecoderScenario(compressed, 16*mib))
}
