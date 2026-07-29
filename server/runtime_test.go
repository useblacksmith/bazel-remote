package server

import (
	"context"
	"testing"
	"time"
)

func TestReadLimiterBoundsAggregateBuffers(t *testing.T) {
	limiter := newReadLimiter(0, 10)
	if limited, err := limiter.acquireBuffer(context.Background(), 6); !limited || err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if limited, err := limiter.acquireBuffer(ctx, 5); !limited || err != context.DeadlineExceeded {
		t.Fatalf("expected deadline while budget was exhausted, got %v", err)
	}

	limiter.releaseBuffer(6)
	if limited, err := limiter.acquireBuffer(context.Background(), 5); !limited || err != nil {
		t.Fatalf("expected reservation after release: %v", err)
	}
	limiter.releaseBuffer(5)
}

func TestReadLimiterBoundsActiveReads(t *testing.T) {
	limiter := newReadLimiter(1, 0)
	if limited, err := limiter.acquireRead(context.Background()); !limited || err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if limited, err := limiter.acquireRead(ctx); !limited || err != context.DeadlineExceeded {
		t.Fatalf("expected deadline while active-read slot was occupied, got %v", err)
	}

	limiter.releaseRead()
	if limited, err := limiter.acquireRead(context.Background()); !limited || err != nil {
		t.Fatalf("expected admission after release: %v", err)
	}
	limiter.releaseRead()
}

func TestReadLimiterReportsDisabledLimits(t *testing.T) {
	limiter := newReadLimiter(0, 0)

	if limited, err := limiter.acquireRead(context.Background()); limited || err != nil {
		t.Fatalf("disabled active-read limit returned limited=%t, err=%v", limited, err)
	}
	if limited, err := limiter.acquireBuffer(context.Background(), 1); limited || err != nil {
		t.Fatalf("disabled buffer limit returned limited=%t, err=%v", limited, err)
	}
}

func TestReadLimitsZeroValuesClearEarlierLimits(t *testing.T) {
	s := &grpcServer{}
	if err := WithReadLimits(1, 10)(s); err != nil {
		t.Fatal(err)
	}
	if s.readLimiter == nil {
		t.Fatal("expected limiter to be installed")
	}

	if err := WithReadLimits(0, 0)(s); err != nil {
		t.Fatal(err)
	}
	if s.readLimiter != nil {
		t.Fatal("expected zero values to clear the previously installed limiter")
	}
}

func TestReadLimitOptionsRejectNegativeValues(t *testing.T) {
	s := &grpcServer{}
	if err := WithReadLimits(-1, 0)(s); err == nil {
		t.Fatal("expected negative active-read limit to fail")
	}
	if err := WithReadLimits(0, -1)(s); err == nil {
		t.Fatal("expected negative buffer limit to fail")
	}
}

func TestReadChunkSizeOption(t *testing.T) {
	s := &grpcServer{readChunkSizeBytes: maxChunkSize}
	if err := WithReadChunkSizeBytes(256 * 1024)(s); err != nil {
		t.Fatal(err)
	}
	if got, want := s.readChunkSizeBytes, int64(256*1024); got != want {
		t.Fatalf("read chunk size = %d, want %d", got, want)
	}

	if err := WithReadChunkSizeBytes(0)(s); err != nil {
		t.Fatal(err)
	}
	if got, want := s.readChunkSizeBytes, int64(256*1024); got != want {
		t.Fatalf("zero changed read chunk size to %d, want %d", got, want)
	}

	if err := WithReadChunkSizeBytes(-1)(s); err == nil {
		t.Fatal("expected negative read chunk size to fail")
	}
	if err := WithReadChunkSizeBytes(maxChunkSize + 1)(s); err == nil {
		t.Fatal("expected oversized read chunk size to fail")
	}
}

func TestMaxBatchTotalSizeOption(t *testing.T) {
	s := &grpcServer{}
	if err := WithMaxBatchTotalSizeBytes(4 * 1024 * 1024)(s); err != nil {
		t.Fatal(err)
	}
	if got, want := s.maxBatchTotalSizeBytes, int64(4*1024*1024); got != want {
		t.Fatalf("max batch total size = %d, want %d", got, want)
	}

	if err := WithMaxBatchTotalSizeBytes(0)(s); err != nil {
		t.Fatal(err)
	}
	if got := s.maxBatchTotalSizeBytes; got != 0 {
		t.Fatalf("zero did not disable the batch total size limit: %d", got)
	}

	if err := WithMaxBatchTotalSizeBytes(-1)(s); err == nil {
		t.Fatal("expected negative max batch total size to fail")
	}
}

func TestSourceBufferPoolRecyclesBuffers(t *testing.T) {
	pool := newSourceBufferPool(2, 16)

	first := pool.get(8)
	if len(first) != 8 || cap(first) != 16 {
		t.Fatalf("got len=%d cap=%d, want len=8 cap=16", len(first), cap(first))
	}
	first[0] = 0x42
	pool.put(first)

	second := pool.get(4)
	if len(second) != 4 {
		t.Fatalf("got len=%d, want 4", len(second))
	}
	if second[0] != 0x42 {
		t.Fatal("expected the pooled backing array to be recycled")
	}
}

func TestSourceBufferPoolDropsForeignAndExcessBuffers(t *testing.T) {
	pool := newSourceBufferPool(1, 16)

	// Arrays that don't match the pool capacity must not be admitted.
	pool.put(make([]byte, 8))
	if len(pool.buffers) != 0 {
		t.Fatal("foreign buffer was admitted to the pool")
	}

	// Puts beyond the pool size must not block; extras go to the GC.
	pool.put(make([]byte, 0, 16))
	pool.put(make([]byte, 0, 16))
	if len(pool.buffers) != 1 {
		t.Fatalf("pool holds %d buffers, want 1", len(pool.buffers))
	}
}

func TestSourceBufferPoolNilAndOversizedFallBackToAllocation(t *testing.T) {
	var pool *sourceBufferPool
	buf := pool.get(8)
	if len(buf) != 8 {
		t.Fatalf("nil pool returned len=%d, want 8", len(buf))
	}
	pool.put(buf)

	pool = newSourceBufferPool(1, 4)
	oversized := pool.get(8)
	if len(oversized) != 8 {
		t.Fatalf("oversized get returned len=%d, want 8", len(oversized))
	}
	pool.put(oversized)
	if len(pool.buffers) != 0 {
		t.Fatal("oversized buffer must not be admitted to the pool")
	}
}

func TestSourceBufferPoolDisabledWithoutPositiveBounds(t *testing.T) {
	if pool := newSourceBufferPool(0, 16); pool != nil {
		t.Fatal("expected nil pool for zero count")
	}
	if pool := newSourceBufferPool(4, 0); pool != nil {
		t.Fatal("expected nil pool for zero capacity")
	}
}
