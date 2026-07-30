package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache/disk/zstdimpl"

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

func TestSyncPoolDecodersRoundTrip(t *testing.T) {
	input := []byte("the default write-path decoder source must keep working")

	var compressed bytes.Buffer
	enc, err := zstd.NewWriter(&compressed,
		zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := enc.Write(input); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	rc, err := syncPoolDecoders{}.GetDecoder(bytes.NewReader(compressed.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	decompressed, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decompressed, input) {
		t.Fatal("roundtrip mismatch")
	}
}

func TestWithZstdDecoderRetentionValidation(t *testing.T) {
	s := &grpcServer{}
	if err := WithZstdDecoderRetention(-1)(s); err == nil {
		t.Error("expected error for negative retention")
	}
	if err := WithZstdDecoderRetention(0)(s); err != nil {
		t.Errorf("zero retention must be valid (sync.Pool passthrough): %v", err)
	}
	if _, ok := s.zstdDecoders.(syncPoolDecoders); !ok {
		t.Error("zero retention must select the sync.Pool-backed source")
	}
	if err := WithZstdDecoderRetention(8)(s); err != nil {
		t.Errorf("positive retention must be valid: %v", err)
	}
}
