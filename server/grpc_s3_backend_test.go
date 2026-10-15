package server

import (
	"context"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	backendA = "http://minio-a.example.com:9000"
	backendB = "https://minio-b.example.com:9000"

	bucketA       = "bazel-cache-a"
	bucketALegacy = "bazel-cache-a-pre-rename"
	bucketB       = "bazel-cache-b"
)

// testRouting mirrors a two-entry backends map with backend A as the default:
// A allows its default bucket plus a pre-rename legacy bucket, B allows only
// its default. The asymmetry lets tests prove the PAIR is resolved together,
// not the halves independently.
func testRouting() S3BackendRouting {
	return S3BackendRouting{
		DefaultKey: backendA,
		Backends: map[string]S3BackendRoutingEntry{
			backendA: {DefaultBucket: bucketA, Buckets: map[string]bool{bucketA: true, bucketALegacy: true}},
			backendB: {DefaultBucket: bucketB, Buckets: map[string]bool{bucketB: true}},
		},
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

// requireSelection asserts the resolved (endpoint, bucket) selection lifted
// onto the context. Resolution never fails — the contract under test is
// WHICH backend and bucket an input routes to, not whether it is served.
func requireSelection(t *testing.T, ctx context.Context, endpoint, bucket string) {
	t.Helper()
	selection, ok := cache.S3BackendFromContext(ctx)
	if !ok {
		t.Fatal("no S3 backend selection on context")
	}
	if selection.Endpoint != endpoint || selection.Bucket != bucket {
		t.Fatalf("selection = %+v, want (%s, %s)", selection, endpoint, bucket)
	}
}

func TestS3BackendFromIncomingContext(t *testing.T) {
	t.Run("no metadata routes to the default backend and bucket", func(t *testing.T) {
		ctx := s3BackendFromIncomingContext(context.Background(), testRouting())
		requireSelection(t, ctx, backendA, bucketA)
	})

	t.Run("missing selector key routes to the default backend", func(t *testing.T) {
		md := metadata.Pairs("some-other-key", "value")
		ctx := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), testRouting())
		requireSelection(t, ctx, backendA, bucketA)
	})

	t.Run("duplicate selector values route to the default backend", func(t *testing.T) {
		// Two different forwarded values is drift, not a choosable input:
		// picking either would guess. The default is the deterministic
		// resolution.
		md := metadata.Pairs(
			cache.S3BackendGRPCMetadataKey, backendA,
			cache.S3BackendGRPCMetadataKey, backendB,
			cache.S3BucketGRPCMetadataKey, bucketA,
		)
		ctx := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), testRouting())
		requireSelection(t, ctx, backendA, bucketA)
	})

	t.Run("unknown selector routes to the default backend", func(t *testing.T) {
		// The 2026-09-03 shape: a host or pin still naming a torn-down
		// cluster. The L1 owns which backends exist; the request is served
		// from the default instead of being rejected.
		md := pairMD("http://minio.torn-down.example:9000", bucketA)
		ctx := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), testRouting())
		requireSelection(t, ctx, backendA, bucketA)
	})

	t.Run("no URL normalization: near-miss selectors route to the default", func(t *testing.T) {
		// Resolution is exact opaque string match against the configured
		// map keys; anything that would need normalizing to match is
		// treated as unknown and defaults. Near-misses of backend B prove
		// they do NOT resolve to B.
		for _, nearMiss := range []string{
			"https://minio-b.example.com:9000/", // trailing slash
			"minio-b.example.com:9000",          // missing scheme
			"HTTPS://minio-b.example.com:9000",  // case difference
		} {
			md := pairMD(nearMiss, bucketB)
			ctx := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), testRouting())
			// bucketB is not allowed on default backend A, so the bucket
			// defaults too.
			requireSelection(t, ctx, backendA, bucketA)
		}
	})

	t.Run("missing bucket uses the entry's default bucket", func(t *testing.T) {
		md := metadata.Pairs(cache.S3BackendGRPCMetadataKey, backendB)
		ctx := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), testRouting())
		requireSelection(t, ctx, backendB, bucketB)
	})

	t.Run("duplicate bucket values use the entry's default bucket", func(t *testing.T) {
		md := metadata.Pairs(
			cache.S3BackendGRPCMetadataKey, backendA,
			cache.S3BucketGRPCMetadataKey, bucketA,
			cache.S3BucketGRPCMetadataKey, bucketALegacy,
		)
		ctx := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), testRouting())
		requireSelection(t, ctx, backendA, bucketA)
	})

	t.Run("unknown bucket uses the entry's default bucket", func(t *testing.T) {
		md := pairMD(backendB, "rogue-bucket")
		ctx := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), testRouting())
		requireSelection(t, ctx, backendB, bucketB)
	})

	t.Run("the pair resolves together: another entry's bucket defaults on this entry", func(t *testing.T) {
		// bucketB exists in the map — but only for backend B. On backend A
		// it is unknown and A's default bucket is used; a stale pair must
		// not read another shard's bucket.
		md := pairMD(backendA, bucketB)
		ctx := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), testRouting())
		requireSelection(t, ctx, backendA, bucketA)
	})

	t.Run("byte-verbatim bucket match: near-miss buckets use the default", func(t *testing.T) {
		for _, nearMiss := range []string{
			"BAZEL-CACHE-A",  // case difference
			" bazel-cache-a", // stray whitespace
			"bazel-cache-a/", // trailing slash
		} {
			md := pairMD(backendA, nearMiss)
			ctx := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), testRouting())
			requireSelection(t, ctx, backendA, bucketA)
		}
	})

	t.Run("unknown selector with a bucket valid on the default entry keeps the bucket", func(t *testing.T) {
		// The halves default independently: the selector defaults to entry
		// A, and the forwarded bucket is then resolved against A's allowed
		// set — the legacy bucket is in it, so it is honored.
		md := pairMD("http://minio.torn-down.example:9000", bucketALegacy)
		ctx := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), testRouting())
		requireSelection(t, ctx, backendA, bucketALegacy)
	})

	t.Run("allowlisted pair is lifted onto context", func(t *testing.T) {
		md := pairMD(backendB, bucketB)
		ctx := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), testRouting())
		requireSelection(t, ctx, backendB, bucketB)
	})

	t.Run("extra (pre-rename) bucket of the matched entry is accepted", func(t *testing.T) {
		md := pairMD(backendA, bucketALegacy)
		ctx := s3BackendFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), testRouting())
		requireSelection(t, ctx, backendA, bucketALegacy)
	})
}

func TestS3BackendInterceptorExemptsHealthAndCapabilities(t *testing.T) {
	interceptor := GRPCS3BackendUnaryServerInterceptor(testRouting())

	for _, method := range []string{
		"/grpc.health.v1.Health/Check",
		"/build.bazel.remote.execution.v2.Capabilities/GetCapabilities",
	} {
		var handlerCtx context.Context
		_, err := interceptor(context.Background(), nil,
			&grpc.UnaryServerInfo{FullMethod: method},
			func(ctx context.Context, req interface{}) (interface{}, error) {
				handlerCtx = ctx
				return nil, nil
			})
		if err != nil || handlerCtx == nil {
			t.Fatalf("expected %s to reach the handler, err=%v", method, err)
		}
		// Exempt methods carry no tenant data; no selection is resolved.
		if _, ok := cache.S3BackendFromContext(handlerCtx); ok {
			t.Fatalf("exempt method %s carried an S3 backend selection", method)
		}
	}

	// A cache RPC without any selector reaches the handler with the default
	// backend resolved.
	var handlerCtx context.Context
	_, err := interceptor(context.Background(), nil,
		&grpc.UnaryServerInfo{FullMethod: "/build.bazel.remote.execution.v2.ActionCache/GetActionResult"},
		func(ctx context.Context, req interface{}) (interface{}, error) {
			handlerCtx = ctx
			return nil, nil
		})
	if err != nil || handlerCtx == nil {
		t.Fatalf("expected selector-less cache RPC to be served, err=%v", err)
	}
	requireSelection(t, handlerCtx, backendA, bucketA)
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
	interceptor := GRPCS3BackendStreamServerInterceptor(testRouting())
	byteStreamRead := &grpc.StreamServerInfo{FullMethod: "/google.bytestream.ByteStream/Read"}

	// streamSelection runs the interceptor with the given incoming metadata
	// and returns the handler stream's context.
	streamSelection := func(t *testing.T, ctx context.Context) context.Context {
		t.Helper()
		var handlerStream grpc.ServerStream
		err := interceptor(nil,
			&fakeServerStream{ctx: ctx},
			byteStreamRead,
			func(srv interface{}, ss grpc.ServerStream) error {
				handlerStream = ss
				return nil
			})
		if err != nil || handlerStream == nil {
			t.Fatalf("expected stream to reach the handler, err=%v", err)
		}
		return handlerStream.Context()
	}

	t.Run("missing selector routes to the default backend", func(t *testing.T) {
		ctx := streamSelection(t, context.Background())
		requireSelection(t, ctx, backendA, bucketA)
	})

	t.Run("unknown selector routes to the default backend", func(t *testing.T) {
		md := pairMD("http://minio.torn-down.example:9000", bucketA)
		ctx := streamSelection(t, metadata.NewIncomingContext(context.Background(), md))
		requireSelection(t, ctx, backendA, bucketA)
	})

	t.Run("another entry's bucket uses the resolved entry's default", func(t *testing.T) {
		md := pairMD(backendA, bucketB)
		ctx := streamSelection(t, metadata.NewIncomingContext(context.Background(), md))
		requireSelection(t, ctx, backendA, bucketA)
	})

	t.Run("accepted stream's Context carries the pair", func(t *testing.T) {
		md := pairMD(backendB, bucketB)
		ctx := streamSelection(t, metadata.NewIncomingContext(context.Background(), md))
		requireSelection(t, ctx, backendB, bucketB)
	})

	t.Run("exempt methods bypass resolution", func(t *testing.T) {
		handled := false
		err := interceptor(nil,
			&fakeServerStream{ctx: context.Background()},
			&grpc.StreamServerInfo{FullMethod: "/grpc.health.v1.Health/Watch"},
			func(srv interface{}, ss grpc.ServerStream) error {
				handled = true
				return nil
			})
		if err != nil || !handled {
			t.Fatalf("expected health stream to bypass resolution, err=%v handled=%v", err, handled)
		}
	})
}
