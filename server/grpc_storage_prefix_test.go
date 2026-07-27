package server

import (
	"context"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestStoragePrefixFromIncomingContext(t *testing.T) {
	t.Run("no metadata passes through", func(t *testing.T) {
		ctx, err := storagePrefixFromIncomingContext(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := cache.StoragePrefixFromContext(ctx); ok {
			t.Fatal("expected no storage prefix on context")
		}
	})

	t.Run("valid prefix is lifted onto context", func(t *testing.T) {
		md := metadata.Pairs(cache.StoragePrefixGRPCMetadataKey, "bazel/staging/us-west/42/9876/v0/bazel/")
		ctx, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		prefix, ok := cache.StoragePrefixFromContext(ctx)
		if !ok || prefix != "bazel/staging/us-west/42/9876/v0/bazel/" {
			t.Fatalf("unexpected prefix %q ok=%v", prefix, ok)
		}
		if !cache.StoragePrefixRequiredFromContext(ctx) {
			t.Fatal("expected storage prefix to be marked required")
		}
	})

	for _, invalid := range []string{"", "/abs/path/", "a/../b/", "a//b/", "a\\b/", "..", "."} {
		t.Run("rejects "+invalid, func(t *testing.T) {
			md := metadata.Pairs(cache.StoragePrefixGRPCMetadataKey, invalid)
			_, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md))
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument for %q, got %v", invalid, err)
			}
		})
	}
}
