package cache

import (
	"context"
	"testing"
)

func TestS3BackendContextRoundTrip(t *testing.T) {
	if _, ok := S3BackendFromContext(context.Background()); ok {
		t.Fatal("expected no selection on a bare context")
	}

	if _, ok := S3BackendFromContext(nil); ok { //nolint:staticcheck // deliberate nil-context robustness check
		t.Fatal("expected no selection on a nil context")
	}

	selection := S3BackendSelection{
		Endpoint: "http://minio-a.example.com:9000",
		Bucket:   "bazel-cache-a",
	}
	ctx := WithS3Backend(context.Background(), selection)
	got, ok := S3BackendFromContext(ctx)
	if !ok || got != selection {
		t.Fatalf("S3BackendFromContext = (%+v, %v), want (%+v, true)", got, ok, selection)
	}

	// The pair travels as one value: an endpoint-only selection (upstream
	// predating the bucket contract) reads back with an empty bucket.
	endpointOnly := S3BackendSelection{Endpoint: "http://minio-a.example.com:9000"}
	got, ok = S3BackendFromContext(WithS3Backend(context.Background(), endpointOnly))
	if !ok || got != endpointOnly {
		t.Fatalf("S3BackendFromContext = (%+v, %v), want (%+v, true)", got, ok, endpointOnly)
	}

	// A selection without an endpoint is meaningless (the endpoint is the
	// routing key) and reads back as absent, bucket or not.
	if _, ok := S3BackendFromContext(WithS3Backend(context.Background(), S3BackendSelection{})); ok {
		t.Fatal("expected an empty selection to read back as absent")
	}
	if _, ok := S3BackendFromContext(WithS3Backend(context.Background(), S3BackendSelection{Bucket: "orphan-bucket"})); ok {
		t.Fatal("expected a bucket-only selection to read back as absent")
	}
}
