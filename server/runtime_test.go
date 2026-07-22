package server

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	testutils "github.com/buchgr/bazel-remote/v2/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReadLimiterBoundsAggregateBuffers(t *testing.T) {
	limiter := newReadLimiter(0, 10)
	if err := limiter.acquireBuffer(context.Background(), 6); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := limiter.acquireBuffer(ctx, 5); err != context.DeadlineExceeded {
		t.Fatalf("expected deadline while budget was exhausted, got %v", err)
	}

	limiter.releaseBuffer(6)
	if err := limiter.acquireBuffer(context.Background(), 5); err != nil {
		t.Fatalf("expected reservation after release: %v", err)
	}
	limiter.releaseBuffer(5)
}

func TestBatchReadLimitIsAdvertisedAndEnforced(t *testing.T) {
	logger := testutils.NewSilentLogger()
	s := &grpcServer{
		cache:        &StubCache{},
		accessLogger: logger,
		errorLogger:  logger,
	}
	if err := WithMaxBatchTotalSizeBytes(10)(s); err != nil {
		t.Fatal(err)
	}

	capabilities, err := s.GetCapabilities(context.Background(), &pb.GetCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got := capabilities.CacheCapabilities.MaxBatchTotalSizeBytes; got != 10 {
		t.Fatalf("advertised max batch size = %d, want 10", got)
	}

	_, err = s.BatchReadBlobs(context.Background(), &pb.BatchReadBlobsRequest{
		Digests: []*pb.Digest{{Hash: strings.Repeat("a", hashKeyLength), SizeBytes: 11}},
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("BatchReadBlobs error = %v, want ResourceExhausted", err)
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
	if err := WithMaxBatchTotalSizeBytes(-1)(s); err == nil {
		t.Fatal("expected negative batch limit to fail")
	}
}
