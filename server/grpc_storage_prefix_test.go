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
	t.Run("no metadata is rejected (fail-closed)", func(t *testing.T) {
		_, err := storagePrefixFromIncomingContext(context.Background(), "")
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected InvalidArgument for missing metadata, got %v", err)
		}
	})

	t.Run("missing prefix key is rejected", func(t *testing.T) {
		md := metadata.Pairs("some-other-key", "value")
		_, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), "")
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected InvalidArgument for missing prefix, got %v", err)
		}
	})

	t.Run("duplicate prefix values are rejected", func(t *testing.T) {
		md := metadata.Pairs(
			cache.StoragePrefixGRPCMetadataKey, "bazel/staging/us-west/42/9876/v0/bazel/",
			cache.StoragePrefixGRPCMetadataKey, "bazel/staging/us-west/43/1234/v0/bazel/",
		)
		_, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), "")
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected InvalidArgument for duplicate prefixes, got %v", err)
		}
	})

	t.Run("valid prefix is lifted onto context", func(t *testing.T) {
		md := metadata.Pairs(cache.StoragePrefixGRPCMetadataKey, "bazel/staging/us-west/42/9876/v0/bazel/")
		ctx, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), "")
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
			_, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), "")
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument for %q, got %v", invalid, err)
			}
		})
	}
}

func TestStoragePrefixAuthSecret(t *testing.T) {
	validPrefix := metadata.Pairs(cache.StoragePrefixGRPCMetadataKey, "bazel/staging/us-west/42/9876/v0/bazel/")

	t.Run("missing secret is rejected when configured", func(t *testing.T) {
		_, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), validPrefix), "hunter2")
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("expected Unauthenticated, got %v", err)
		}
	})

	t.Run("wrong secret is rejected", func(t *testing.T) {
		md := metadata.Join(validPrefix, metadata.Pairs(cache.AuthSecretGRPCMetadataKey, "wrong"))
		_, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), "hunter2")
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("expected Unauthenticated, got %v", err)
		}
	})

	t.Run("duplicate secrets are rejected", func(t *testing.T) {
		md := metadata.Join(validPrefix, metadata.Pairs(
			cache.AuthSecretGRPCMetadataKey, "hunter2",
			cache.AuthSecretGRPCMetadataKey, "hunter2",
		))
		_, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), "hunter2")
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("expected Unauthenticated for duplicate secrets, got %v", err)
		}
	})

	t.Run("correct secret passes", func(t *testing.T) {
		md := metadata.Join(validPrefix, metadata.Pairs(cache.AuthSecretGRPCMetadataKey, "hunter2"))
		ctx, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), "hunter2")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := cache.StoragePrefixFromContext(ctx); !ok {
			t.Fatal("expected storage prefix on context")
		}
	})

	t.Run("secret not required when unconfigured", func(t *testing.T) {
		_, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), validPrefix), "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestExemptFromStoragePrefix(t *testing.T) {
	for method, want := range map[string]bool{
		"/grpc.health.v1.Health/Check":                                  true,
		"/grpc.health.v1.Health/Watch":                                  true,
		"/build.bazel.remote.execution.v2.Capabilities/GetCapabilities": true,
		"/build.bazel.remote.execution.v2.ActionCache/GetActionResult":  false,
		"/google.bytestream.ByteStream/Read":                            false,
	} {
		if got := exemptFromStoragePrefix(method); got != want {
			t.Errorf("exemptFromStoragePrefix(%q) = %v, want %v", method, got, want)
		}
	}
}
