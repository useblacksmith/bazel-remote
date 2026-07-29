package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	stdlog "log"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
)

// consumptionTrackingCache fakes disk.Cache for Write: Put drains the reader
// (as the real disk cache does) while recording how many payload bytes have
// been consumed so far.
type consumptionTrackingCache struct {
	consumed atomic.Int64
}

// consumptionReadBufferBytes is deliberately small relative to the test's
// chunk size: io.Pipe unblocks the writer the moment the final copy completes
// inside Read, before our counter update after Read returns, so the counter
// can legitimately lag by up to one read buffer. A contract violation
// (pipelined Recv) would lag by at least one full chunk.
const consumptionReadBufferBytes = 1024

func (c *consumptionTrackingCache) Put(ctx context.Context, kind cache.EntryKind, hash string, size int64, r io.Reader) error {
	buf := make([]byte, consumptionReadBufferBytes)
	for {
		n, err := r.Read(buf)
		c.consumed.Add(int64(n))
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (c *consumptionTrackingCache) Contains(ctx context.Context, kind cache.EntryKind, hash string, size int64) (bool, int64) {
	return false, -1
}

func (c *consumptionTrackingCache) Get(ctx context.Context, kind cache.EntryKind, hash string, size int64, offset int64) (io.ReadCloser, int64, error) {
	panic("not used by Write")
}

func (c *consumptionTrackingCache) GetValidatedActionResult(ctx context.Context, hash string) (*pb.ActionResult, []byte, error) {
	panic("not used by Write")
}

func (c *consumptionTrackingCache) GetZstd(ctx context.Context, hash string, size int64, offset int64) (io.ReadCloser, int64, error) {
	panic("not used by Write")
}

func (c *consumptionTrackingCache) FindMissingCasBlobs(ctx context.Context, blobs []*pb.Digest) ([]*pb.Digest, error) {
	panic("not used by Write")
}

func (c *consumptionTrackingCache) MaxSize() int64 { return 1 << 40 }

func (c *consumptionTrackingCache) Stats() (int64, int64, int, int64) { return 0, 0, 0, 0 }

func (c *consumptionTrackingCache) RegisterMetrics() {}

// contractWriteServer fakes bytestream.ByteStream_WriteServer, asserting on
// every Recv that all previously returned payload bytes were already consumed
// by the cache's Put reader.
type contractWriteServer struct {
	grpc.ServerStream
	t        *testing.T
	cache    *consumptionTrackingCache
	requests []*bytestream.WriteRequest
	next     int
	returned int64
	response *bytestream.WriteResponse
}

func (s *contractWriteServer) Context() context.Context { return context.Background() }

func (s *contractWriteServer) Recv() (*bytestream.WriteRequest, error) {
	if consumed := s.cache.consumed.Load(); consumed < s.returned-consumptionReadBufferBytes {
		s.t.Errorf(
			"Recv %d called before the previous payload was consumed: returned %d bytes, consumed %d",
			s.next, s.returned, consumed,
		)
	}
	if s.next >= len(s.requests) {
		return nil, io.EOF
	}
	req := s.requests[s.next]
	s.next++
	s.returned += int64(len(req.Data))
	return req, nil
}

func (s *contractWriteServer) SendAndClose(resp *bytestream.WriteResponse) error {
	// The response must only be sent after the recv loop terminated, i.e.
	// after every returned payload byte was consumed. No race tolerance here:
	// cache.Put has fully returned (through putResult) before the handler
	// responds, so the counter is final. Embedders recycle the last payload
	// buffer on this signal; an early-response refactor must fail this test.
	if consumed := s.cache.consumed.Load(); consumed != s.returned {
		s.t.Errorf(
			"response sent before the final payload was consumed: returned %d bytes, consumed %d",
			s.returned, consumed,
		)
	}
	s.response = resp
	return nil
}

// buildContractWriteRequests returns a 4-chunk upload of deterministic
// content, plus the full content for committed-size assertions.
func buildContractWriteRequests() ([]*bytestream.WriteRequest, []byte) {
	const chunkSize = 256 * 1024
	const chunks = 4

	content := make([]byte, chunkSize*chunks)
	for i := range content {
		content[i] = byte(i % 251)
	}
	digest := sha256.Sum256(content)
	resource := fmt.Sprintf("uploads/uuid/blobs/%s/%d", hex.EncodeToString(digest[:]), len(content))

	var requests []*bytestream.WriteRequest
	for offset := 0; offset < len(content); offset += chunkSize {
		req := &bytestream.WriteRequest{
			WriteOffset: int64(offset),
			Data:        content[offset : offset+chunkSize],
			FinishWrite: offset+chunkSize == len(content),
		}
		if offset == 0 {
			req.ResourceName = resource
		}
		requests = append(requests, req)
	}
	return requests, content
}

// TestWriteRecvLoopConsumesPayloadBeforeNextRecv pins the consumption
// contract embedders rely on to recycle decoded WriteRequest payload buffers:
// by the time the Write receive loop asks for message N+1, message N's Data
// has been fully consumed (pw.Write returned after the pipe reader copied the
// bytes out). If this test starts failing after a rebase, payload-buffer
// reuse in embedders becomes cache corruption - do not weaken it.
func TestWriteRecvLoopConsumesPayloadBeforeNextRecv(t *testing.T) {
	requests, content := buildContractWriteRequests()

	trackingCache := &consumptionTrackingCache{}
	discard := stdlog.New(io.Discard, "", 0)
	s := &grpcServer{
		cache:               trackingCache,
		accessLogger:        discard,
		errorLogger:         discard,
		maxCasBlobSizeBytes: 1 << 40,
		readChunkSizeBytes:  maxChunkSize,
	}

	srv := &contractWriteServer{t: t, cache: trackingCache, requests: requests}
	if err := s.Write(srv); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if srv.response == nil {
		t.Fatal("Write did not send a response")
	}
	if got := srv.response.GetCommittedSize(); got != int64(len(content)) {
		t.Fatalf("committed %d bytes, want %d", got, len(content))
	}
	if consumed := trackingCache.consumed.Load(); consumed != int64(len(content)) {
		t.Fatalf("consumed %d bytes, want %d", consumed, len(content))
	}
}

// TestWritePayloadConsumedFiresAfterConsumption pins the
// WithWritePayloadConsumed contract: the callback fires exactly once per
// received message, in order, with that message's request, and only after
// the message's payload bytes were consumed by the cache reader. Embedders
// recycle payload backing arrays on this signal; firing it early becomes
// silent cross-stream cache corruption - do not weaken this test.
func TestWritePayloadConsumedFiresAfterConsumption(t *testing.T) {
	requests, content := buildContractWriteRequests()

	trackingCache := &consumptionTrackingCache{}
	type hookObservation struct {
		req      *bytestream.WriteRequest
		consumed int64
	}
	var hookCalls []hookObservation
	discard := stdlog.New(io.Discard, "", 0)
	s := &grpcServer{
		cache:               trackingCache,
		accessLogger:        discard,
		errorLogger:         discard,
		maxCasBlobSizeBytes: 1 << 40,
		readChunkSizeBytes:  maxChunkSize,
		// Runs on the receive goroutine, strictly between pw.Write(msg N)
		// and Recv(msg N+1), so no synchronization is needed here.
		writePayloadConsumed: func(req *bytestream.WriteRequest) {
			hookCalls = append(hookCalls, hookObservation{
				req:      req,
				consumed: trackingCache.consumed.Load(),
			})
		},
	}

	srv := &contractWriteServer{t: t, cache: trackingCache, requests: requests}
	if err := s.Write(srv); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if srv.response == nil {
		t.Fatal("Write did not send a response")
	}

	if len(hookCalls) != len(requests) {
		t.Fatalf("hook fired %d times, want once per message (%d)", len(hookCalls), len(requests))
	}
	var cumulative int64
	for i, call := range hookCalls {
		cumulative += int64(len(requests[i].Data))
		if call.req != requests[i] {
			t.Fatalf("hook call %d received the wrong request", i)
		}
		// Same one-read-buffer tolerance as the Recv-ordering assertion: the
		// pipe unblocks the writer inside the reader's final Read, before the
		// consumption counter updates.
		if call.consumed < cumulative-consumptionReadBufferBytes {
			t.Fatalf("hook call %d fired before its payload was consumed: consumed %d, want >= %d",
				i, call.consumed, cumulative)
		}
	}
	if got := srv.response.GetCommittedSize(); got != int64(len(content)) {
		t.Fatalf("committed %d bytes, want %d", got, len(content))
	}
}

// malformedZstdWriteServer streams an endless sequence of chunks that are not
// valid zstd frames under a compressed-blobs resource name.
type malformedZstdWriteServer struct {
	grpc.ServerStream
	sentFirst bool
}

func (s *malformedZstdWriteServer) Context() context.Context { return context.Background() }

func (s *malformedZstdWriteServer) Recv() (*bytestream.WriteRequest, error) {
	req := &bytestream.WriteRequest{
		Data: bytes.Repeat([]byte{0x5A}, 64*1024),
	}
	if !s.sentFirst {
		s.sentFirst = true
		req.ResourceName = "uploads/uuid/compressed-blobs/zstd/" +
			strings.Repeat("a", 64) + "/1048576"
	}
	return req, nil
}

func (s *malformedZstdWriteServer) SendAndClose(*bytestream.WriteResponse) error { return nil }

// TestWriteMalformedZstdDoesNotLeakRecvGoroutine: when cache.Put fails early
// on undecodable zstd input, the pooled decoder's Close only resets the
// decoder - it does not close the underlying pipe reader - so without the
// handler's deferred pr.Close() the receive goroutine stays blocked in
// pw.Write forever, pinning its payload chunk.
func TestWriteMalformedZstdDoesNotLeakRecvGoroutine(t *testing.T) {
	trackingCache := &consumptionTrackingCache{}
	discard := stdlog.New(io.Discard, "", 0)
	s := &grpcServer{
		cache:               trackingCache,
		accessLogger:        discard,
		errorLogger:         discard,
		maxCasBlobSizeBytes: 1 << 40,
		readChunkSizeBytes:  maxChunkSize,
	}

	// Warm the zstd decoder pool first: pooled decoders keep background
	// goroutines alive, which would otherwise skew the baseline below.
	if err := s.Write(&malformedZstdWriteServer{}); err == nil {
		t.Fatal("expected malformed zstd write to fail")
	}

	baseline := runtime.NumGoroutine()
	if err := s.Write(&malformedZstdWriteServer{}); err == nil {
		t.Fatal("expected malformed zstd write to fail")
	}

	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > baseline {
		if time.Now().After(deadline) {
			t.Fatalf("receive goroutine leaked: %d goroutines, baseline %d",
				runtime.NumGoroutine(), baseline)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
