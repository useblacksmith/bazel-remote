package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/buchgr/bazel-remote/v2/utils/zstdpool"

	"golang.org/x/sync/semaphore"
	"google.golang.org/genproto/googleapis/bytestream"
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
			// Zero values disable limits, including any installed by an
			// earlier WithReadLimits call.
			s.readLimiter = nil
			return nil
		}
		s.readLimiter = newReadLimiter(maxActiveReads, maxBufferBytes)
		return nil
	}
}

// WithZstdDecoderRetention bounds the number of idle zstd decoders retained
// between compressed ByteStream writes. Upstream behavior uses a sync.Pool,
// which retains a write burst's peak decoder memory (~5.2 MiB per decoder)
// until later GC cycles drain it. With this option, decoders beyond the
// retention limit are destroyed on release instead. Concurrent decoder
// creation is not bounded here; embedders bound it upstream via
// write-stream admission. A zero value preserves the sync.Pool behavior.
func WithZstdDecoderRetention(retainedDecoders int64) GRPCServerOption {
	return func(s *grpcServer) error {
		if retainedDecoders < 0 {
			return fmt.Errorf("retained zstd decoders must not be negative: %d", retainedDecoders)
		}
		if retainedDecoders == 0 {
			s.zstdDecoders = syncPoolDecoders{}
			return nil
		}
		s.zstdDecoders = zstdpool.NewBoundedDecoderPool(retainedDecoders)
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

// WithWritePayloadConsumed registers a callback the ByteStream Write handler
// invokes exactly once per received message, after that message's Data has
// been fully consumed (the pipe write to the cache reader returned). It is
// the explicit ownership-transfer point for embedders that decode
// WriteRequest payloads into reusable buffers: once the callback fires, the
// server holds no reference to req.Data and the embedder may recycle its
// backing array.
//
// The callback runs on the Write receive goroutine, so implementations must
// be fast and must not block. Messages whose payload is never consumed
// (errors, skipped writes because the blob already exists, stream teardown)
// do not trigger the callback; embedders must treat those payloads as
// possibly still referenced.
//
// MAINTAINERS: if the Write receive loop is ever restructured (pipelining,
// buffering payloads across iterations), this callback must move with the
// point where the payload is truly consumed. Firing it early turns embedder
// buffer reuse into silent cross-stream cache corruption. See
// TestWritePayloadConsumedFiresAfterConsumption.
func WithWritePayloadConsumed(fn func(*bytestream.WriteRequest)) GRPCServerOption {
	return func(s *grpcServer) error {
		s.writePayloadConsumed = fn
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
	maxActiveReads int64
	bufferBytes    *semaphore.Weighted
	maxBufferBytes int64
}

func newReadLimiter(maxActiveReads, maxBufferBytes int64) *readLimiter {
	l := &readLimiter{maxActiveReads: maxActiveReads, maxBufferBytes: maxBufferBytes}
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

// sourceBufferPool recycles ByteStream read source buffers so a burst of read
// RPCs does not allocate one fresh chunk-sized array per request. The
// active-read admission limit is what bounds live memory (at most
// maxActiveReads buffers in flight); the pool only reduces allocation churn.
//
// Retention is delegated to sync.Pool, i.e. to the garbage collector: an
// array unused for two consecutive GC cycles is reclaimed, so quiet stretches
// return the memory to the runtime at the cost of re-allocating the working
// set after such gaps. There is deliberately no fixed retained floor.
//
// Reuse across RPCs is safe for the same reason the read loop reuses one
// buffer across chunks within an RPC: ServerStream.SendMsg marshals the
// response synchronously, so once Send returns (and therefore once the
// handler returns) the transport holds no reference to the source array.
type sourceBufferPool struct {
	capacity int64
	pool     sync.Pool
}

func newSourceBufferPool(capacity int64) *sourceBufferPool {
	if capacity <= 0 {
		return nil
	}
	return &sourceBufferPool{capacity: capacity}
}

// get returns a length-size buffer, recycling a pooled array when one is
// available. A nil pool (limits disabled) or an oversized request falls back
// to upstream bazel-remote behavior: one fresh allocation per call.
func (p *sourceBufferPool) get(size int64) []byte {
	if p == nil || size > p.capacity {
		return make([]byte, size)
	}
	if v := p.pool.Get(); v != nil {
		return (*v.(*[]byte))[:size]
	}
	// Allocate at full capacity so this array is admissible on put.
	return make([]byte, size, p.capacity)
}

// put returns a buffer to the pool. Arrays that do not match the pool
// capacity (fallback allocations for oversized requests) are dropped for the
// GC so get can keep aliasing pooled arrays at full chunk capacity.
func (p *sourceBufferPool) put(b []byte) {
	if p == nil || int64(cap(b)) != p.capacity {
		return
	}
	b = b[:0]
	// Pointer indirection is the shape staticcheck SA6002 asks for. One
	// small slice-header allocation still escapes per put (get discards the
	// pointer, so the header is not itself recycled); that cost is noise
	// next to the chunk-sized array being reused.
	p.pool.Put(&b)
}
