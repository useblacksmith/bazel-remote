package server

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/semaphore"
)

const (
	// ByteStreamReadAdmissionStageActiveReads identifies waits for an active
	// ByteStream handler slot.
	ByteStreamReadAdmissionStageActiveReads = "active_reads"
	// ByteStreamReadAdmissionStageBufferBytes identifies waits for a
	// bazel-remote response-buffer reservation.
	ByteStreamReadAdmissionStageBufferBytes = "buffer_bytes"
)

// RuntimeMetrics receives low-cardinality process-level measurements for
// memory-bearing read paths. Implementations must be safe for concurrent use.
type RuntimeMetrics interface {
	ByteStreamReadStarted(ctx context.Context, declaredBytes int64)
	ByteStreamReadFinished(ctx context.Context, declaredBytes int64)
	ByteStreamReadBufferReserved(ctx context.Context, reservedBytes int64)
	ByteStreamReadBufferReleased(ctx context.Context, reservedBytes int64)
	ByteStreamReadAdmissionWait(ctx context.Context, stage string, duration time.Duration)
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

// WithReadLimits bounds active ByteStream handlers and aggregate ByteStream
// response-buffer reservations. A zero value leaves the corresponding limit
// disabled.
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

// WithMaxBatchTotalSizeBytes limits the total declared blob bytes in one
// batch CAS request and advertises that limit through
// CacheCapabilities.MaxBatchTotalSizeBytes. Per REAPI that capability bounds
// both BatchReadBlobs and BatchUpdateBlobs, so the limit is enforced on both.
// Zero preserves bazel-remote's existing no-limit behavior.
func WithMaxBatchTotalSizeBytes(maxBatchTotalSizeBytes int64) GRPCServerOption {
	return func(s *grpcServer) error {
		if maxBatchTotalSizeBytes < 0 {
			return fmt.Errorf("max batch total size must not be negative: %d", maxBatchTotalSizeBytes)
		}
		s.maxBatchTotalSizeBytes = maxBatchTotalSizeBytes
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

func (l *readLimiter) acquireRead(ctx context.Context) (bool, error) {
	if l == nil || l.activeReads == nil {
		return false, nil
	}
	return true, l.activeReads.Acquire(ctx, 1)
}

func (l *readLimiter) releaseRead() {
	if l != nil && l.activeReads != nil {
		l.activeReads.Release(1)
	}
}

func (l *readLimiter) acquireBuffer(ctx context.Context, size int64) (bool, error) {
	if l == nil || l.bufferBytes == nil || size == 0 {
		return false, nil
	}
	if size > l.maxBufferBytes {
		return true, fmt.Errorf("read buffer reservation %d exceeds configured budget %d", size, l.maxBufferBytes)
	}
	return true, l.bufferBytes.Acquire(ctx, size)
}

func (l *readLimiter) releaseBuffer(size int64) {
	if l != nil && l.bufferBytes != nil && size > 0 && size <= l.maxBufferBytes {
		l.bufferBytes.Release(size)
	}
}
