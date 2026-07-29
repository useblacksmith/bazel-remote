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
)

func allowedBackends() map[string]bool {
	return map[string]bool{backendA: true, backendB: true}
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
		)
		_, err := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), allowedBackends())
		requireTrustRejection(t, err, cache.RejectionReasonS3BackendSelector, "duplicate")
	})

	t.Run("unknown selector is rejected", func(t *testing.T) {
		md := metadata.Pairs(cache.S3BackendGRPCMetadataKey, "http://rogue.example.com:9000")
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
			md := metadata.Pairs(cache.S3BackendGRPCMetadataKey, nearMiss)
			_, err := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), allowedBackends())
			requireTrustRejection(t, err, cache.RejectionReasonS3BackendSelector, "unknown")
		}
	})

	t.Run("allowlisted selector is lifted onto context", func(t *testing.T) {
		md := metadata.Pairs(cache.S3BackendGRPCMetadataKey, backendB)
		ctx, err := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), allowedBackends())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		selector, ok := cache.S3BackendFromContext(ctx)
		if !ok || selector != backendB {
			t.Fatalf("unexpected selector %q ok=%v", selector, ok)
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
		md := metadata.Pairs(cache.S3BackendGRPCMetadataKey, "http://rogue.example.com:9000")
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

	t.Run("accepted stream's Context carries the selector", func(t *testing.T) {
		md := metadata.Pairs(cache.S3BackendGRPCMetadataKey, backendA)
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
		selector, ok := cache.S3BackendFromContext(handlerStream.Context())
		if !ok || selector != backendA {
			t.Fatalf("wrapped stream Context selector = %q ok=%v, want %q", selector, ok, backendA)
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
