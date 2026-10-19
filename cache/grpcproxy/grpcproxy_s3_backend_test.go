package grpcproxy

import (
	"context"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"

	"google.golang.org/grpc/metadata"
)

var testBackendSelection = cache.S3BackendSelection{
	Endpoint: "http://minio-b.example.com:9000",
	Bucket:   "bazel-cache-b",
}

func TestWithOutgoingS3BackendIsIdempotent(t *testing.T) {
	ctx := cache.WithS3Backend(context.Background(), testBackendSelection)
	ctx = withOutgoingS3Backend(ctx)
	ctx = withOutgoingS3Backend(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	// Exactly one copy of each key: the downstream rejects duplicates
	// fail-closed (duplicate / bucket_duplicate), so a call path that layers
	// the attachment twice must not produce two copies of either header.
	if got := md.Get(cache.S3BackendGRPCMetadataKey); len(got) != 1 || got[0] != testBackendSelection.Endpoint {
		t.Fatalf("outgoing %s = %v, want exactly [%s]", cache.S3BackendGRPCMetadataKey, got, testBackendSelection.Endpoint)
	}
	if got := md.Get(cache.S3BucketGRPCMetadataKey); len(got) != 1 || got[0] != testBackendSelection.Bucket {
		t.Fatalf("outgoing %s = %v, want exactly [%s]", cache.S3BucketGRPCMetadataKey, got, testBackendSelection.Bucket)
	}
}

func TestWithOutgoingS3BackendEndpointOnlyIsIdempotent(t *testing.T) {
	ctx := cache.WithS3Backend(context.Background(),
		cache.S3BackendSelection{Endpoint: testBackendSelection.Endpoint})
	ctx = withOutgoingS3Backend(ctx)
	ctx = withOutgoingS3Backend(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	if got := md.Get(cache.S3BackendGRPCMetadataKey); len(got) != 1 || got[0] != testBackendSelection.Endpoint {
		t.Fatalf("outgoing %s = %v, want exactly [%s]", cache.S3BackendGRPCMetadataKey, got, testBackendSelection.Endpoint)
	}
	if got := md.Get(cache.S3BucketGRPCMetadataKey); len(got) != 0 {
		t.Fatalf("outgoing %s = %v, want none", cache.S3BucketGRPCMetadataKey, got)
	}
}
