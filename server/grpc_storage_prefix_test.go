package server

import (
	"context"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

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

func TestStoragePrefixFromIncomingContext(t *testing.T) {
	t.Run("no metadata is rejected (fail-closed)", func(t *testing.T) {
		_, err := storagePrefixFromIncomingContext(context.Background(), "")
		requireTrustRejection(t, err, cache.RejectionReasonStoragePrefix, "missing")
	})

	t.Run("missing prefix key is rejected", func(t *testing.T) {
		md := metadata.Pairs("some-other-key", "value")
		_, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), "")
		requireTrustRejection(t, err, cache.RejectionReasonStoragePrefix, "missing")
	})

	t.Run("duplicate prefix values are rejected", func(t *testing.T) {
		md := metadata.Pairs(
			cache.StoragePrefixGRPCMetadataKey, "bazel/staging/us-west/42/9876/v0/bazel/",
			cache.StoragePrefixGRPCMetadataKey, "bazel/staging/us-west/43/1234/v0/bazel/",
		)
		_, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), "")
		requireTrustRejection(t, err, cache.RejectionReasonStoragePrefix, "duplicate")
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
			requireTrustRejection(t, err, cache.RejectionReasonStoragePrefix, "invalid")
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

func TestTrustRejectionCounters(t *testing.T) {
	// The rate-limited journald line points operators at the *_rejected_total
	// counters; this pins that every rejection cause actually increments one
	// (drill #2 finding: prefix and secret rejections were counter-less).
	validPrefix := metadata.Pairs(cache.StoragePrefixGRPCMetadataKey, "bazel/staging/us-west/42/9876/v0/bazel/")

	prefixBefore := testutil.ToFloat64(storagePrefixRejected.WithLabelValues("missing")) +
		testutil.ToFloat64(storagePrefixRejected.WithLabelValues("invalid"))
	secretBefore := testutil.ToFloat64(authSecretRejected.WithLabelValues("missing")) +
		testutil.ToFloat64(authSecretRejected.WithLabelValues("mismatch"))

	_, _ = storagePrefixFromIncomingContext(context.Background(), "")
	md := metadata.Pairs(cache.StoragePrefixGRPCMetadataKey, "a//b/")
	_, _ = storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), "")

	_, _ = storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), validPrefix), "hunter2")
	md = metadata.Join(validPrefix, metadata.Pairs(cache.AuthSecretGRPCMetadataKey, "wrong"))
	_, _ = storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), "hunter2")

	prefixAfter := testutil.ToFloat64(storagePrefixRejected.WithLabelValues("missing")) +
		testutil.ToFloat64(storagePrefixRejected.WithLabelValues("invalid"))
	secretAfter := testutil.ToFloat64(authSecretRejected.WithLabelValues("missing")) +
		testutil.ToFloat64(authSecretRejected.WithLabelValues("mismatch"))

	if prefixAfter != prefixBefore+2 {
		t.Errorf("storage_prefix_rejected_total: got %v, want %v", prefixAfter, prefixBefore+2)
	}
	if secretAfter != secretBefore+2 {
		t.Errorf("auth_secret_rejected_total: got %v, want %v", secretAfter, secretBefore+2)
	}
}

func TestExemptFromTenantMetadata(t *testing.T) {
	for method, want := range map[string]bool{
		"/grpc.health.v1.Health/Check":                                  true,
		"/grpc.health.v1.Health/Watch":                                  true,
		"/build.bazel.remote.execution.v2.Capabilities/GetCapabilities": true,
		"/build.bazel.remote.execution.v2.ActionCache/GetActionResult":  false,
		"/google.bytestream.ByteStream/Read":                            false,
	} {
		if got := exemptFromTenantMetadata(method); got != want {
			t.Errorf("exemptFromTenantMetadata(%q) = %v, want %v", method, got, want)
		}
	}
}
