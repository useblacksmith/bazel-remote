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

func TestMaxBatchReadSizeOption(t *testing.T) {
	s := &grpcServer{}
	if err := WithMaxBatchReadSizeBytes(4 * 1024 * 1024)(s); err != nil {
		t.Fatal(err)
	}
	if got, want := s.maxBatchReadSizeBytes, int64(4*1024*1024); got != want {
		t.Fatalf("max batch read size = %d, want %d", got, want)
	}

	if err := WithMaxBatchReadSizeBytes(0)(s); err != nil {
		t.Fatal(err)
	}
	if got := s.maxBatchReadSizeBytes; got != 0 {
		t.Fatalf("zero did not disable the batch read size limit: %d", got)
	}

	if err := WithMaxBatchReadSizeBytes(-1)(s); err == nil {
		t.Fatal("expected negative max batch read size to fail")
	}
}
