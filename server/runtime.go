package server

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/semaphore"
)

// RuntimeMetrics receives low-cardinality process-level measurements for
// memory-bearing read paths. Implementations must be safe for concurrent use.
type RuntimeMetrics interface {
	ByteStreamReadStarted(ctx context.Context, declaredBytes int64)
	ByteStreamReadFinished(ctx context.Context, declaredBytes int64)
	ByteStreamReadBufferReserved(ctx context.Context, reservedBytes int64)
	ByteStreamReadBufferReleased(ctx context.Context, reservedBytes int64)
	ByteStreamReadAdmissionWait(ctx context.Context, stage string, duration time.Duration)
	BatchReadStarted(ctx context.Context, declaredBytes int64)
	BatchReadFinished(ctx context.Context, declaredBytes int64)
	BatchReadBufferReserved(ctx context.Context, reservedBytes int64)
	BatchReadBufferReleased(ctx context.Context, reservedBytes int64)
	BatchReadAdmissionWait(ctx context.Context, duration time.Duration)
}

// GRPCServerOption configures optional server behavior for embedders without
// changing the standalone bazel-remote defaults.
type GRPCServerOption func(*grpcServer) error

// WithRuntimeMetrics installs an optional read-path metrics sink.
func WithRuntimeMetrics(metrics RuntimeMetrics) GRPCServerOption {
	return func(s *grpcServer) error {
		s.runtimeMetrics = metrics
		return nil
	}
}

// WithReadLimits bounds active ByteStream handlers and aggregate response
// buffer reservations. A zero value leaves the corresponding limit disabled.
func WithReadLimits(maxActiveReads, maxBufferBytes int64) GRPCServerOption {
	return func(s *grpcServer) error {
		if maxActiveReads < 0 {
			return fmt.Errorf("max active reads must not be negative: %d", maxActiveReads)
		}
		if maxBufferBytes < 0 {
			return fmt.Errorf("max read buffer bytes must not be negative: %d", maxBufferBytes)
		}
		if maxActiveReads == 0 && maxBufferBytes == 0 {
			return nil
		}
		s.readLimiter = newReadLimiter(maxActiveReads, maxBufferBytes)
		return nil
	}
}

// WithReadChunkSizeBytes sets the maximum ByteStream response payload. Zero
// preserves bazel-remote's default.
func WithReadChunkSizeBytes(chunkSizeBytes int64) GRPCServerOption {
	return func(s *grpcServer) error {
		if chunkSizeBytes < 0 {
			return fmt.Errorf("read chunk size must not be negative: %d", chunkSizeBytes)
		}
		if chunkSizeBytes > maxChunkSize {
			return fmt.Errorf("read chunk size %d exceeds the default maximum %d", chunkSizeBytes, maxChunkSize)
		}
		if chunkSizeBytes > 0 {
			s.readChunkSizeBytes = chunkSizeBytes
		}
		return nil
	}
}

// WithMaxBatchTotalSizeBytes advertises and enforces a BatchReadBlobs total
// payload limit. Zero preserves the REAPI "no limit" value.
func WithMaxBatchTotalSizeBytes(maxBytes int64) GRPCServerOption {
	return func(s *grpcServer) error {
		if maxBytes < 0 {
			return fmt.Errorf("max batch total size must not be negative: %d", maxBytes)
		}
		s.maxBatchTotalSizeBytes = maxBytes
		return nil
	}
}

type readLimiter struct {
	activeReads    *semaphore.Weighted
	bufferBytes    *semaphore.Weighted
	maxBufferBytes int64
}

func newReadLimiter(maxActiveReads, maxBufferBytes int64) *readLimiter {
	l := &readLimiter{maxBufferBytes: maxBufferBytes}
	if maxActiveReads > 0 {
		l.activeReads = semaphore.NewWeighted(maxActiveReads)
	}
	if maxBufferBytes > 0 {
		l.bufferBytes = semaphore.NewWeighted(maxBufferBytes)
	}
	return l
}

func (l *readLimiter) acquireRead(ctx context.Context) error {
	if l == nil || l.activeReads == nil {
		return nil
	}
	return l.activeReads.Acquire(ctx, 1)
}

func (l *readLimiter) releaseRead() {
	if l != nil && l.activeReads != nil {
		l.activeReads.Release(1)
	}
}

func (l *readLimiter) acquireBuffer(ctx context.Context, size int64) error {
	if l == nil || l.bufferBytes == nil || size == 0 {
		return nil
	}
	if size > l.maxBufferBytes {
		return fmt.Errorf("read buffer reservation %d exceeds configured budget %d", size, l.maxBufferBytes)
	}
	return l.bufferBytes.Acquire(ctx, size)
}

func (l *readLimiter) releaseBuffer(size int64) {
	if l != nil && l.bufferBytes != nil && size > 0 && size <= l.maxBufferBytes {
		l.bufferBytes.Release(size)
	}
}
