package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	stdlog "log"
	"sync/atomic"
	"testing"

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
	s.response = resp
	return nil
}

// TestWriteRecvLoopConsumesPayloadBeforeNextRecv pins the consumption
// contract embedders rely on to recycle decoded WriteRequest payload buffers:
// by the time the Write receive loop asks for message N+1, message N's Data
// has been fully consumed (pw.Write returned after the pipe reader copied the
// bytes out). If this test starts failing after a rebase, payload-buffer
// reuse in embedders becomes cache corruption - do not weaken it.
func TestWriteRecvLoopConsumesPayloadBeforeNextRecv(t *testing.T) {
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
