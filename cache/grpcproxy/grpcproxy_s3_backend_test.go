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

const testBackendSelector = "http://minio-b.example.com:9000"

func TestPutCapturesS3BackendForAsyncUpload(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	uploadQueue := make(chan backendproxy.UploadReq, 1)
	r := &remoteGrpcProxyCache{uploadQueue: uploadQueue}

	ctx := cache.WithS3Backend(
		cache.WithStoragePrefix(context.Background(), "minio-prefix/bazel/staging/42/9876/v0"),
		testBackendSelector)
	r.Put(ctx, cache.CAS, hash, 4, 4, io.NopCloser(strings.NewReader("blob")))

	item := <-uploadQueue
	defer func() { _ = item.Rc.Close() }()
	if item.S3Backend != testBackendSelector {
		t.Fatalf("queued upload S3Backend = %q, want %q", item.S3Backend, testBackendSelector)
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
	if item.S3Backend != "" {
		t.Fatalf("queued upload S3Backend = %q, want empty", item.S3Backend)
	}
}

func TestUploadContextAttachesS3Backend(t *testing.T) {
	ctx, cancel := uploadContext(backendproxy.UploadReq{
		StoragePrefix:              "minio-prefix/bazel/staging/42/9876/v0",
		RequestScopedStoragePrefix: true,
		S3Backend:                  testBackendSelector,
	})
	defer cancel()

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	if got := md.Get(cache.S3BackendGRPCMetadataKey); len(got) != 1 || got[0] != testBackendSelector {
		t.Fatalf("outgoing %s = %v, want [%s]", cache.S3BackendGRPCMetadataKey, got, testBackendSelector)
	}
	if got := md.Get(cache.StoragePrefixGRPCMetadataKey); len(got) != 1 {
		t.Fatalf("outgoing %s = %v, want one value", cache.StoragePrefixGRPCMetadataKey, got)
	}
}

func TestUploadContextWithoutS3BackendAttachesNoSelector(t *testing.T) {
	ctx, cancel := uploadContext(backendproxy.UploadReq{})
	defer cancel()

	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		if got := md.Get(cache.S3BackendGRPCMetadataKey); len(got) != 0 {
			t.Fatalf("outgoing %s = %v, want none", cache.S3BackendGRPCMetadataKey, got)
		}
	}
}

func TestWithOutgoingS3BackendIsIdempotent(t *testing.T) {
	ctx := cache.WithS3Backend(context.Background(), testBackendSelector)
	ctx = withOutgoingS3Backend(ctx)
	ctx = withOutgoingS3Backend(ctx)

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	if got := md.Get(cache.S3BackendGRPCMetadataKey); len(got) != 1 || got[0] != testBackendSelector {
		t.Fatalf("outgoing %s = %v, want exactly [%s]", cache.S3BackendGRPCMetadataKey, got, testBackendSelector)
	}
}
