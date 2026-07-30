package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache/disk/zstdimpl"

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
