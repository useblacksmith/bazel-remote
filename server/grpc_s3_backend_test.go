package server

import (
	"context"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"

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

func TestS3BackendFromIncomingContext(t *testing.T) {
	t.Run("no metadata is rejected (fail-closed)", func(t *testing.T) {
		_, err := s3BackendFromIncomingContext(context.Background(), allowedBackends())
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected InvalidArgument for missing metadata, got %v", err)
		}
	})

	t.Run("missing selector key is rejected", func(t *testing.T) {
		md := metadata.Pairs("some-other-key", "value")
		_, err := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), allowedBackends())
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected InvalidArgument for missing selector, got %v", err)
		}
	})

	t.Run("duplicate selector values are rejected", func(t *testing.T) {
		md := metadata.Pairs(
			cache.S3BackendGRPCMetadataKey, backendA,
			cache.S3BackendGRPCMetadataKey, backendB,
		)
		_, err := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), allowedBackends())
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected InvalidArgument for duplicate selectors, got %v", err)
		}
	})

	t.Run("unknown selector is rejected", func(t *testing.T) {
		md := metadata.Pairs(cache.S3BackendGRPCMetadataKey, "http://rogue.example.com:9000")
		_, err := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), allowedBackends())
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected InvalidArgument for unknown selector, got %v", err)
		}
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
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument for %q, got %v", nearMiss, err)
			}
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
	if status.Code(err) != codes.InvalidArgument || handled {
		t.Fatalf("expected InvalidArgument before handler, err=%v handled=%v", err, handled)
	}
}
