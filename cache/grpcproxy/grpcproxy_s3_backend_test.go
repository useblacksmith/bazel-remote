package grpcproxy

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/utils/backendproxy"

	"google.golang.org/grpc/metadata"
)

var testBackendSelection = cache.S3BackendSelection{
	Endpoint: "http://minio-b.example.com:9000",
	Bucket:   "bazel-cache-b",
}

func TestPutCapturesS3BackendForAsyncUpload(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	uploadQueue := make(chan backendproxy.UploadReq, 1)
	r := &remoteGrpcProxyCache{uploadQueue: uploadQueue}

	ctx := cache.WithS3Backend(
		cache.WithStoragePrefix(context.Background(), "minio-prefix/bazel/staging/42/9876/v0"),
		testBackendSelection)
	r.Put(ctx, cache.CAS, hash, 4, 4, io.NopCloser(strings.NewReader("blob")))

	item := <-uploadQueue
	defer func() { _ = item.Rc.Close() }()
	// The pair must survive enqueue together: an upload replayed with the
	// endpoint but not the bucket would be rejected downstream (fail-closed).
	if item.S3Backend != testBackendSelection {
		t.Fatalf("queued upload S3Backend = %+v, want %+v", item.S3Backend, testBackendSelection)
	}
	if item.StoragePrefix == "" {
		t.Fatal("queued upload lost its storage prefix")
	}
}

func TestPutWithoutS3BackendLeavesUploadUnscoped(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	uploadQueue := make(chan backendproxy.UploadReq, 1)
	r := &remoteGrpcProxyCache{uploadQueue: uploadQueue}

	r.Put(context.Background(), cache.CAS, hash, 4, 4, io.NopCloser(strings.NewReader("blob")))

	item := <-uploadQueue
	defer func() { _ = item.Rc.Close() }()
	if item.S3Backend != (cache.S3BackendSelection{}) {
		t.Fatalf("queued upload S3Backend = %+v, want zero value", item.S3Backend)
	}
}

func TestUploadContextAttachesS3BackendPair(t *testing.T) {
	ctx, cancel := uploadContext(backendproxy.UploadReq{
		StoragePrefix:              "minio-prefix/bazel/staging/42/9876/v0",
		RequestScopedStoragePrefix: true,
		S3Backend:                  testBackendSelection,
	})
	defer cancel()

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	if got := md.Get(cache.S3BackendGRPCMetadataKey); len(got) != 1 || got[0] != testBackendSelection.Endpoint {
		t.Fatalf("outgoing %s = %v, want [%s]", cache.S3BackendGRPCMetadataKey, got, testBackendSelection.Endpoint)
	}
	if got := md.Get(cache.S3BucketGRPCMetadataKey); len(got) != 1 || got[0] != testBackendSelection.Bucket {
		t.Fatalf("outgoing %s = %v, want [%s]", cache.S3BucketGRPCMetadataKey, got, testBackendSelection.Bucket)
	}
	if got := md.Get(cache.StoragePrefixGRPCMetadataKey); len(got) != 1 {
		t.Fatalf("outgoing %s = %v, want one value", cache.StoragePrefixGRPCMetadataKey, got)
	}
}

func TestUploadContextWithEndpointOnlySelectionAttachesNoBucket(t *testing.T) {
	// An item captured by an upstream predating the bucket contract carries
	// only the endpoint; the bucket key must not be sent empty.
	ctx, cancel := uploadContext(backendproxy.UploadReq{
		S3Backend: cache.S3BackendSelection{Endpoint: testBackendSelection.Endpoint},
	})
	defer cancel()

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	if got := md.Get(cache.S3BackendGRPCMetadataKey); len(got) != 1 || got[0] != testBackendSelection.Endpoint {
		t.Fatalf("outgoing %s = %v, want [%s]", cache.S3BackendGRPCMetadataKey, got, testBackendSelection.Endpoint)
	}
	if got := md.Get(cache.S3BucketGRPCMetadataKey); len(got) != 0 {
		t.Fatalf("outgoing %s = %v, want none", cache.S3BucketGRPCMetadataKey, got)
	}
}

func TestUploadContextWithoutS3BackendAttachesNoSelector(t *testing.T) {
	ctx, cancel := uploadContext(backendproxy.UploadReq{})
	defer cancel()

	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		if got := md.Get(cache.S3BackendGRPCMetadataKey); len(got) != 0 {
			t.Fatalf("outgoing %s = %v, want none", cache.S3BackendGRPCMetadataKey, got)
		}
		if got := md.Get(cache.S3BucketGRPCMetadataKey); len(got) != 0 {
			t.Fatalf("outgoing %s = %v, want none", cache.S3BucketGRPCMetadataKey, got)
		}
	}
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
