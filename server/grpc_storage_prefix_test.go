package server

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestStoragePrefixFromIncomingContext(t *testing.T) {
	t.Run("no metadata is rejected (fail-closed)", func(t *testing.T) {
		_, err := storagePrefixFromIncomingContext(context.Background(), nil)
		requireTrustRejection(t, err, cache.RejectionReasonStoragePrefix, "missing")
	})

	t.Run("missing prefix key is rejected", func(t *testing.T) {
		md := metadata.Pairs("some-other-key", "value")
		_, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), nil)
		requireTrustRejection(t, err, cache.RejectionReasonStoragePrefix, "missing")
	})

	t.Run("duplicate prefix values are rejected", func(t *testing.T) {
		md := metadata.Pairs(
			cache.StoragePrefixGRPCMetadataKey, "bazel/staging/us-west/42/9876/v0/bazel/",
			cache.StoragePrefixGRPCMetadataKey, "bazel/staging/us-west/43/1234/v0/bazel/",
		)
		_, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), nil)
		requireTrustRejection(t, err, cache.RejectionReasonStoragePrefix, "duplicate")
	})

	t.Run("valid prefix is lifted onto context", func(t *testing.T) {
		md := metadata.Pairs(cache.StoragePrefixGRPCMetadataKey, "bazel/staging/us-west/42/9876/v0/bazel/")
		ctx, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), nil)
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
			_, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), nil)
			requireTrustRejection(t, err, cache.RejectionReasonStoragePrefix, "invalid")
		})
	}
}

func TestStoragePrefixAuthSecret(t *testing.T) {
	validPrefix := metadata.Pairs(cache.StoragePrefixGRPCMetadataKey, "bazel/staging/us-west/42/9876/v0/bazel/")

	t.Run("missing secret is rejected when configured", func(t *testing.T) {
		_, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), validPrefix), parseAuthSecrets("hunter2"))
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("expected Unauthenticated, got %v", err)
		}
	})

	t.Run("wrong secret is rejected", func(t *testing.T) {
		md := metadata.Join(validPrefix, metadata.Pairs(cache.AuthSecretGRPCMetadataKey, "wrong"))
		_, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), parseAuthSecrets("hunter2"))
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("expected Unauthenticated, got %v", err)
		}
	})

	t.Run("duplicate secrets are rejected", func(t *testing.T) {
		md := metadata.Join(validPrefix, metadata.Pairs(
			cache.AuthSecretGRPCMetadataKey, "hunter2",
			cache.AuthSecretGRPCMetadataKey, "hunter2",
		))
		_, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), parseAuthSecrets("hunter2"))
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("expected Unauthenticated for duplicate secrets, got %v", err)
		}
	})

	t.Run("correct secret passes", func(t *testing.T) {
		md := metadata.Join(validPrefix, metadata.Pairs(cache.AuthSecretGRPCMetadataKey, "hunter2"))
		ctx, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), parseAuthSecrets("hunter2"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := cache.StoragePrefixFromContext(ctx); !ok {
			t.Fatal("expected storage prefix on context")
		}
	})

	t.Run("secret not required when unconfigured", func(t *testing.T) {
		_, err := storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), validPrefix), nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestStoragePrefixAuthRotation(t *testing.T) {
	t.Parallel()
	const prefix = "bazel/staging/us-west/42/9876/v0/bazel/"
	for _, tc := range []struct {
		name       string
		configured string
		presented  []string
		want       codes.Code
	}{
		{"single key", "old", []string{"old"}, codes.OK},
		{"new key during overlap", "new,old", []string{"new"}, codes.OK},
		{"old key during overlap", "new,old", []string{"old"}, codes.OK},
		{"third key", "new,old,older", []string{"older"}, codes.OK},
		{"whitespace and empty entries", " , new, , old , ", []string{"old"}, codes.OK},
		{"repeated configured key", "new,new", []string{"new"}, codes.OK},
		{"new key after rotation", "new", []string{"new"}, codes.OK},
		{"retired key", "new", []string{"old"}, codes.Unauthenticated},
		{"unknown key", "new,old", []string{"unknown"}, codes.Unauthenticated},
		{"missing key", "new,old", nil, codes.Unauthenticated},
		{"empty key", "new,,old", []string{""}, codes.Unauthenticated},
		{"request list", "new,old", []string{"new,old"}, codes.Unauthenticated},
		{"multiple request values", "new,old", []string{"new", "old"}, codes.Unauthenticated},
		{"duplicate request values", "new,old", []string{"new", "new"}, codes.Unauthenticated},
		{"untrimmed request key", "new,old", []string{" old "}, codes.Unauthenticated},
		{"empty list", " , , ", []string{""}, codes.Unauthenticated},
		{"empty list with unknown key", " , , ", []string{"unknown"}, codes.Unauthenticated},
		{"whitespace configuration", " ", []string{" "}, codes.Unauthenticated},
		{"unconfigured dev mode", "", nil, codes.OK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			md := metadata.Pairs(cache.StoragePrefixGRPCMetadataKey, prefix)
			if tc.presented != nil {
				md.Set(cache.AuthSecretGRPCMetadataKey, tc.presented...)
			}
			ctx := metadata.NewIncomingContext(context.Background(), md)
			checkPrefix := func(t *testing.T, ctx context.Context) {
				t.Helper()
				got, ok := cache.StoragePrefixFromContext(ctx)
				if !ok || got != prefix || !cache.StoragePrefixRequiredFromContext(ctx) {
					t.Fatalf("authorized request lost required storage prefix: %q, present=%v", got, ok)
				}
			}
			t.Run("unary", func(t *testing.T) {
				called := false
				_, err := GRPCStoragePrefixUnaryServerInterceptor(tc.configured)(
					ctx, nil,
					&grpc.UnaryServerInfo{FullMethod: "/build.bazel.remote.execution.v2.ActionCache/GetActionResult"},
					func(ctx context.Context, _ interface{}) (interface{}, error) {
						called = true
						checkPrefix(t, ctx)
						return nil, nil
					})
				if status.Code(err) != tc.want || called != (tc.want == codes.OK) {
					t.Fatalf("code=%v, handler called=%v; want %v", status.Code(err), called, tc.want)
				}
			})
			t.Run("stream", func(t *testing.T) {
				called := false
				err := GRPCStoragePrefixStreamServerInterceptor(tc.configured)(
					nil, &tenantMetadataServerStream{ctx: ctx},
					&grpc.StreamServerInfo{FullMethod: "/google.bytestream.ByteStream/Read"},
					func(_ interface{}, stream grpc.ServerStream) error {
						called = true
						checkPrefix(t, stream.Context())
						return nil
					})
				if status.Code(err) != tc.want || called != (tc.want == codes.OK) {
					t.Fatalf("code=%v, handler called=%v; want %v", status.Code(err), called, tc.want)
				}
			})
		})
	}
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

	_, _ = storagePrefixFromIncomingContext(context.Background(), nil)
	md := metadata.Pairs(cache.StoragePrefixGRPCMetadataKey, "a//b/")
	_, _ = storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), nil)

	_, _ = storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), validPrefix), parseAuthSecrets("hunter2"))
	md = metadata.Join(validPrefix, metadata.Pairs(cache.AuthSecretGRPCMetadataKey, "wrong"))
	_, _ = storagePrefixFromIncomingContext(metadata.NewIncomingContext(context.Background(), md), parseAuthSecrets("hunter2"))

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

func BenchmarkAuthSecretLookup(b *testing.B) {
	for _, count := range []int{1, 2, 16, 128} {
		keys := make([]string, count)
		for i := range keys {
			keys[i] = fmt.Sprintf("%064x", i+1)
		}
		accepted := parseAuthSecrets(strings.Join(keys, ","))
		for _, hit := range []bool{true, false} {
			b.Run(fmt.Sprintf("keys=%d/hit=%t", count, hit), func(b *testing.B) {
				presented := strings.Repeat("f", 64)
				if hit {
					presented = strings.Clone(keys[len(keys)-1])
				}
				b.ReportAllocs()
				for b.Loop() {
					if matchesAuthSecret(presented, accepted) != hit {
						b.Fatal("unexpected lookup result")
					}
				}
			})
		}
	}
}
