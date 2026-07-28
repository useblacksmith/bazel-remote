package cache

import (
	"context"
	"testing"
)

func TestS3BackendContextRoundTrip(t *testing.T) {
	if _, ok := S3BackendFromContext(context.Background()); ok {
		t.Fatal("expected no selector on a bare context")
	}

	if _, ok := S3BackendFromContext(nil); ok { //nolint:staticcheck // deliberate nil-context robustness check
		t.Fatal("expected no selector on a nil context")
	}

	selector := "http://minio-a.example.com:9000"
	ctx := WithS3Backend(context.Background(), selector)
	got, ok := S3BackendFromContext(ctx)
	if !ok || got != selector {
		t.Fatalf("S3BackendFromContext = (%q, %v), want (%q, true)", got, ok, selector)
	}

	if _, ok := S3BackendFromContext(WithS3Backend(context.Background(), "")); ok {
		t.Fatal("expected an empty selector to read back as absent")
	}
}
