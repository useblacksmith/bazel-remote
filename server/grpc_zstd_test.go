package server

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/cache/disk"
	"github.com/buchgr/bazel-remote/v2/cache/disk/zstdimpl"
	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	testutils "github.com/buchgr/bazel-remote/v2/utils"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/codes"
)

func TestGRPCErrCodeZstdAdmissionMapping(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want codes.Code
	}{
		{fmt.Errorf("wrapped: %w", zstdimpl.ErrEncoderSaturated), codes.ResourceExhausted},
		{context.Canceled, codes.Canceled},
		{context.DeadlineExceeded, codes.DeadlineExceeded},
		{fmt.Errorf("some other error"), codes.NotFound},
	} {
		if got := gRPCErrCode(tc.err, codes.NotFound); got != tc.want {
			t.Errorf("gRPCErrCode(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

// BatchReadBlobs is a unary handler, so it must never park in the encoder
// admission queue (only ByteStream reads, which are bounded upstream by the
// read limiter, may wait). Under encoder saturation it must degrade to
// identity encoding - always acceptable per REAPI - and keep serving.
func TestBatchReadBlobsFallsBackToIdentityUnderEncoderSaturation(t *testing.T) {
	t.Parallel()

	const admissionTimeout = 10 * time.Second

	dir := testutils.TempDir(t)
	diskCache, err := disk.New(dir, 100*1024*1024,
		disk.WithStorageMode("uncompressed"),
		disk.WithZstdLimits(zstdimpl.ZstdLimits{
			MaxActiveEncoders: 1,
			// Deliberately long: the batch path must not wait for it.
			EncoderAdmissionTimeout: admissionTimeout,
		}),
		disk.WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	testCtx := context.Background()
	blob, hash := testutils.RandomDataAndHash(64 * 1024)
	size := int64(len(blob))
	if err := diskCache.Put(testCtx, cache.CAS, hash, size, bytes.NewReader(blob)); err != nil {
		t.Fatal(err)
	}

	s := &grpcServer{
		cache:              diskCache,
		accessLogger:       testutils.NewSilentLogger(),
		errorLogger:        testutils.NewSilentLogger(),
		readChunkSizeBytes: maxChunkSize,
	}
	request := &pb.BatchReadBlobsRequest{
		Digests:               []*pb.Digest{{Hash: hash, SizeBytes: size}},
		AcceptableCompressors: []pb.Compressor_Value{pb.Compressor_ZSTD},
	}

	// Occupy the only encoder slot with a streaming zstd read.
	held, _, err := diskCache.GetZstd(testCtx, hash, size, 0)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	resp, err := s.BatchReadBlobs(testCtx, request)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed > admissionTimeout/2 {
		t.Fatalf("saturated batch read took %v; it must not queue for encoder admission", elapsed)
	}
	if len(resp.Responses) != 1 {
		t.Fatalf("expected 1 response, got %d", len(resp.Responses))
	}
	r := resp.Responses[0]
	if r.Status.GetCode() != int32(codes.OK) {
		t.Fatalf("expected OK under saturation via identity fallback, got %v", r.Status)
	}
	if r.Compressor != pb.Compressor_IDENTITY {
		t.Fatalf("expected identity fallback under saturation, got %v", r.Compressor)
	}
	if !bytes.Equal(r.Data, blob) {
		t.Fatal("identity fallback returned wrong data")
	}

	// With the slot free again, the batch path serves zstd.
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	resp, err = s.BatchReadBlobs(testCtx, request)
	if err != nil {
		t.Fatal(err)
	}
	r = resp.Responses[0]
	if r.Compressor != pb.Compressor_ZSTD {
		t.Fatalf("expected zstd once capacity is free, got %v", r.Compressor)
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	decompressed, err := dec.DecodeAll(r.Data, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decompressed, blob) {
		t.Fatal("zstd batch read returned wrong data")
	}
}
