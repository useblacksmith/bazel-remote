package server

import (
	"context"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	backendA = "http://minio-a.example.com:9000"
	backendB = "https://minio-b.example.com:9000"

	bucketA       = "bazel-cache-a"
	bucketALegacy = "bazel-cache-a-pre-rename"
	bucketB       = "bazel-cache-b"
)

// allowedBackends mirrors a two-entry backends map: backend A allows its
// default bucket plus a pre-rename legacy bucket, backend B allows only its
// default. The asymmetry lets tests prove the PAIR is validated, not the
// halves independently.
func allowedBackends() map[string]map[string]bool {
	return map[string]map[string]bool{
		backendA: {bucketA: true, bucketALegacy: true},
		backendB: {bucketB: true},
	}
}

// pairMD builds incoming metadata carrying a complete (endpoint, bucket)
// pair, the well-formed shape the trusted upstream forwards.
func pairMD(endpoint, bucket string) metadata.MD {
	return metadata.Pairs(
		cache.S3BackendGRPCMetadataKey, endpoint,
		cache.S3BucketGRPCMetadataKey, bucket,
	)
}

// requireTrustRejection asserts that err is an InvalidArgument rejection
// carrying the typed ErrorInfo marker our trust interceptors mint — the
// wire contract the upstream grpcproxy uses to degrade config-race
// rejections to metered misses instead of failing builds.
func requireTrustRejection(t *testing.T, err error, reason, cause string) {
	t.Helper()
	s, ok := status.FromError(err)
	if !ok || s.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument status, got %v", err)
	}
	for _, detail := range s.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok {
			continue
		}
		if info.GetDomain() != cache.TrustRejectionErrorDomain {
			t.Fatalf("ErrorInfo domain = %q, want %q", info.GetDomain(), cache.TrustRejectionErrorDomain)
		}
		if info.GetReason() != reason {
			t.Fatalf("ErrorInfo reason = %q, want %q", info.GetReason(), reason)
		}
		if got := info.GetMetadata()["cause"]; got != cause {
			t.Fatalf("ErrorInfo cause = %q, want %q", got, cause)
		}
		return
	}
	t.Fatalf("rejection %v carries no ErrorInfo trust marker", err)
}

func TestS3BackendFromIncomingContext(t *testing.T) {
	t.Run("no metadata is rejected (fail-closed)", func(t *testing.T) {
		_, err := s3BackendFromIncomingContext(context.Background(), allowedBackends())
		requireTrustRejection(t, err, cache.RejectionReasonS3BackendSelector, "missing")
	})

	t.Run("missing selector key is rejected", func(t *testing.T) {
		md := metadata.Pairs("some-other-key", "value")
		_, err := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), allowedBackends())
		requireTrustRejection(t, err, cache.RejectionReasonS3BackendSelector, "missing")
	})

	t.Run("duplicate selector values are rejected", func(t *testing.T) {
		md := metadata.Pairs(
			cache.S3BackendGRPCMetadataKey, backendA,
			cache.S3BackendGRPCMetadataKey, backendB,
			cache.S3BucketGRPCMetadataKey, bucketA,
		)
		_, err := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), allowedBackends())
		requireTrustRejection(t, err, cache.RejectionReasonS3BackendSelector, "duplicate")
	})

	t.Run("unknown selector is rejected", func(t *testing.T) {
		md := pairMD("http://rogue.example.com:9000", bucketA)
		_, err := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), allowedBackends())
		requireTrustRejection(t, err, cache.RejectionReasonS3BackendSelector, "unknown")
	})

	t.Run("no URL normalization: near-miss selectors are rejected", func(t *testing.T) {
		// The contract is exact opaque string match against the configured
		// map keys; anything that would need normalizing to match must fail.
		for _, nearMiss := range []string{
			"http://minio-a.example.com:9000/", // trailing slash
			"minio-a.example.com:9000",         // missing scheme
			"HTTP://minio-a.example.com:9000",  // case difference
		} {
			md := pairMD(nearMiss, bucketA)
			_, err := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), allowedBackends())
			requireTrustRejection(t, err, cache.RejectionReasonS3BackendSelector, "unknown")
		}
	})

	t.Run("missing bucket is rejected (fail-closed)", func(t *testing.T) {
		md := metadata.Pairs(cache.S3BackendGRPCMetadataKey, backendA)
		_, err := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), allowedBackends())
		requireTrustRejection(t, err, cache.RejectionReasonS3BackendSelector, "bucket_missing")
	})

	t.Run("duplicate bucket values are rejected", func(t *testing.T) {
		md := metadata.Pairs(
			cache.S3BackendGRPCMetadataKey, backendA,
			cache.S3BucketGRPCMetadataKey, bucketA,
			cache.S3BucketGRPCMetadataKey, bucketALegacy,
		)
		_, err := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), allowedBackends())
		requireTrustRejection(t, err, cache.RejectionReasonS3BackendSelector, "bucket_duplicate")
	})

	t.Run("unknown bucket is rejected", func(t *testing.T) {
		md := pairMD(backendA, "rogue-bucket")
		_, err := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), allowedBackends())
		requireTrustRejection(t, err, cache.RejectionReasonS3BackendSelector, "bucket_unknown")
	})

	t.Run("the pair is validated: right endpoint, other endpoint's bucket is rejected", func(t *testing.T) {
		// bucketB is allowlisted — but only for backend B. Accepting it on
		// backend A would let a stale pair read another shard's bucket.
		md := pairMD(backendA, bucketB)
		_, err := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), allowedBackends())
		requireTrustRejection(t, err, cache.RejectionReasonS3BackendSelector, "bucket_unknown")
	})

	t.Run("byte-verbatim bucket match: near-miss buckets are rejected", func(t *testing.T) {
		for _, nearMiss := range []string{
			"BAZEL-CACHE-A",  // case difference
			" bazel-cache-a", // stray whitespace
			"bazel-cache-a/", // trailing slash
		} {
			md := pairMD(backendA, nearMiss)
			_, err := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), allowedBackends())
			requireTrustRejection(t, err, cache.RejectionReasonS3BackendSelector, "bucket_unknown")
		}
	})

	t.Run("endpoint violations are reported before bucket violations", func(t *testing.T) {
		// An unknown endpoint with a missing bucket must surface the
		// endpoint cause: the endpoint is the routing key, and during a
		// rollout the endpoint-level causes are the primary skew signal.
		md := metadata.Pairs(cache.S3BackendGRPCMetadataKey, "http://rogue.example.com:9000")
		_, err := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), allowedBackends())
		requireTrustRejection(t, err, cache.RejectionReasonS3BackendSelector, "unknown")
	})

	t.Run("allowlisted pair is lifted onto context", func(t *testing.T) {
		md := pairMD(backendB, bucketB)
		ctx, err := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), allowedBackends())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		selection, ok := cache.S3BackendFromContext(ctx)
		if !ok || selection.Endpoint != backendB || selection.Bucket != bucketB {
			t.Fatalf("unexpected selection %+v ok=%v", selection, ok)
		}
	})

	t.Run("extra (pre-rename) bucket of the matched entry is accepted", func(t *testing.T) {
		md := pairMD(backendA, bucketALegacy)
		ctx, err := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), allowedBackends())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		selection, ok := cache.S3BackendFromContext(ctx)
		if !ok || selection.Endpoint != backendA || selection.Bucket != bucketALegacy {
			t.Fatalf("unexpected selection %+v ok=%v", selection, ok)
		}
	})
}

func TestS3BackendInterceptorExemptsHealthAndCapabilities(t *testing.T) {
	interceptor := GRPCS3BackendUnaryServerInterceptor(allowedBackends())

	for _, method := range []string{
		"/grpc.health.v1.Health/Check",
		"/build.bazel.remote.execution.v2.Capabilities/GetCapabilities",
	} {
		handled := false
		_, err := interceptor(context.Background(), nil,
			&grpc.UnaryServerInfo{FullMethod: method},
			func(ctx context.Context, req interface{}) (interface{}, error) {
				handled = true
				return nil, nil
			})
		if err != nil || !handled {
			t.Fatalf("expected %s to bypass selector enforcement, err=%v handled=%v", method, err, handled)
		}
	}

	// A cache RPC without the selector is rejected before the handler runs.
	handled := false
	_, err := interceptor(context.Background(), nil,
		&grpc.UnaryServerInfo{FullMethod: "/build.bazel.remote.execution.v2.ActionCache/GetActionResult"},
		func(ctx context.Context, req interface{}) (interface{}, error) {
			handled = true
			return nil, nil
		})
	if handled {
		t.Fatal("handler ran despite missing selector")
	}
	requireTrustRejection(t, err, cache.RejectionReasonS3BackendSelector, "missing")
}

// fakeServerStream is the minimal grpc.ServerStream for interceptor tests:
// only Context() is exercised by the interceptor and the test handlers.
type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeServerStream) Context() context.Context {
	return f.ctx
}

func TestS3BackendStreamInterceptor(t *testing.T) {
	interceptor := GRPCS3BackendStreamServerInterceptor(allowedBackends())
	byteStreamRead := &grpc.StreamServerInfo{FullMethod: "/google.bytestream.ByteStream/Read"}

	t.Run("missing selector is rejected fail-closed before the handler", func(t *testing.T) {
		handled := false
		err := interceptor(nil,
			&fakeServerStream{ctx: context.Background()},
			byteStreamRead,
			func(srv interface{}, ss grpc.ServerStream) error {
				handled = true
				return nil
			})
		if handled {
			t.Fatal("handler ran despite missing selector")
		}
		requireTrustRejection(t, err, cache.RejectionReasonS3BackendSelector, "missing")
	})

	t.Run("unknown selector is rejected fail-closed before the handler", func(t *testing.T) {
		md := pairMD("http://rogue.example.com:9000", bucketA)
		handled := false
		err := interceptor(nil,
			&fakeServerStream{ctx: metadata.NewIncomingContext(context.Background(), md)},
			byteStreamRead,
			func(srv interface{}, ss grpc.ServerStream) error {
				handled = true
				return nil
			})
		if handled {
			t.Fatal("handler ran despite unknown selector")
		}
		requireTrustRejection(t, err, cache.RejectionReasonS3BackendSelector, "unknown")
	})

	t.Run("wrong bucket is rejected fail-closed before the handler", func(t *testing.T) {
		md := pairMD(backendA, bucketB)
		handled := false
		err := interceptor(nil,
			&fakeServerStream{ctx: metadata.NewIncomingContext(context.Background(), md)},
			byteStreamRead,
			func(srv interface{}, ss grpc.ServerStream) error {
				handled = true
				return nil
			})
		if handled {
			t.Fatal("handler ran despite wrong bucket")
		}
		requireTrustRejection(t, err, cache.RejectionReasonS3BackendSelector, "bucket_unknown")
	})

	t.Run("accepted stream's Context carries the pair", func(t *testing.T) {
		md := pairMD(backendA, bucketA)
		var handlerStream grpc.ServerStream
		err := interceptor(nil,
			&fakeServerStream{ctx: metadata.NewIncomingContext(context.Background(), md)},
			byteStreamRead,
			func(srv interface{}, ss grpc.ServerStream) error {
				handlerStream = ss
				return nil
			})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		selection, ok := cache.S3BackendFromContext(handlerStream.Context())
		if !ok || selection.Endpoint != backendA || selection.Bucket != bucketA {
			t.Fatalf("wrapped stream Context selection = %+v ok=%v, want (%s, %s)", selection, ok, backendA, bucketA)
		}
	})

	t.Run("exempt methods bypass enforcement", func(t *testing.T) {
		handled := false
		err := interceptor(nil,
			&fakeServerStream{ctx: context.Background()},
			&grpc.StreamServerInfo{FullMethod: "/grpc.health.v1.Health/Watch"},
			func(srv interface{}, ss grpc.ServerStream) error {
				handled = true
				return nil
			})
		if err != nil || !handled {
			t.Fatalf("expected health stream to bypass enforcement, err=%v handled=%v", err, handled)
		}
	})
}
