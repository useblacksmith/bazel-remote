package server

import (
	"context"
	"strings"

	"github.com/buchgr/bazel-remote/v2/cache"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Storage-prefix trust interceptors: when bazel-remote runs as a shared L1
// node in front of an object-storage backend, upstream embedded bazel-remote
// instances (on FA hosts) forward each request's tenant storage prefix as
// gRPC metadata (cache.StoragePrefixGRPCMetadataKey). These interceptors lift
// that header onto the request context, which both partitions the local disk
// cache keyspace per tenant and scopes backend object keys.
//
// Trust-on is fail-closed: every cache RPC must carry exactly one valid
// prefix (and the shared secret, when configured) or it is rejected at the
// boundary. There is no accept-unscoped mode — a request that would land in
// a shared keyspace is an isolation bug, and the server rejection is the
// mechanism that surfaces it loudly. Health and capabilities RPCs are exempt
// so probes and CheckCapabilities work without tenant context.
//
// Prefix rejections carry the cache.TrustRejectionErrorDomain ErrorInfo
// marker (the prefix is minted by the trusted upstream itself, so a
// rejection means upstream drift, never customer input); auth rejections
// stay unmarked Unauthenticated, which the upstream already degrades via its
// code-based auth check.

// storagePrefixRejected and authSecretRejected meter the fail-closed
// rejections below, by cause — symmetric with the selector interceptor's
// bazel_remote_s3_backend_selector_rejected_total, and the counters the
// rate-limited trust-rejection log line points operators at. The prefix and
// secret are minted by the trusted upstream, never by customers, so any
// nonzero series means upstream drift: FA/L1 version skew, a Doppler secret
// mismatch, or a forwarding bug.
var storagePrefixRejected = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bazel_remote_storage_prefix_rejected_total",
	Help: "Cache RPCs rejected by the storage-prefix trust interceptor, by cause (missing/duplicate/invalid). Nonzero means upstream drift (FA/L1 version skew or a forwarding bug), never customer input.",
}, []string{"reason"})

var authSecretRejected = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bazel_remote_auth_secret_rejected_total",
	Help: "Cache RPCs rejected by the shared-secret auth check, by cause (missing/duplicate/mismatch). Nonzero means the upstream's BAZELRE_CACHE_L1_SECRET does not match any of this node's configured secrets.",
}, []string{"reason"})

func parseAuthSecrets(raw string) map[string]struct{} {
	if raw == "" {
		return nil
	}
	secrets := make(map[string]struct{})
	for _, secret := range strings.Split(raw, ",") {
		if secret = strings.TrimSpace(secret); secret != "" {
			secrets[secret] = struct{}{}
		}
	}
	// A non-nil empty set keeps authentication required when the
	// configuration contains no usable keys.
	return secrets
}

func matchesAuthSecret(got string, secrets map[string]struct{}) bool {
	_, ok := secrets[got]
	return ok
}

func storagePrefixFromIncomingContext(ctx context.Context, authSecrets map[string]struct{}) (context.Context, error) {
	md, _ := metadata.FromIncomingContext(ctx)

	if authSecrets != nil {
		secrets := md.Get(cache.AuthSecretGRPCMetadataKey)
		cause := ""
		switch {
		case len(secrets) == 0:
			cause = "missing"
		case len(secrets) > 1:
			cause = "duplicate"
		case !matchesAuthSecret(secrets[0], authSecrets):
			cause = "mismatch"
		}
		if cause != "" {
			authSecretRejected.WithLabelValues(cause).Inc()
			// Deliberately NOT a trustRejection: auth failures stay
			// unmarked Unauthenticated (the upstream degrades them via its
			// code-based auth check, see the package comment), but they
			// share the rate-limited journald line so a live debugging
			// session sees the incident on the node at all.
			logTrustRejection("AUTH_SECRET", cause,
				cause+" "+cache.AuthSecretGRPCMetadataKey+" metadata")
			return ctx, status.Errorf(codes.Unauthenticated,
				"missing or invalid %s metadata", cache.AuthSecretGRPCMetadataKey)
		}
	}

	prefix, cause := singleMetadataValue(md, cache.StoragePrefixGRPCMetadataKey)
	if cause != "" {
		storagePrefixRejected.WithLabelValues(cause).Inc()
		return ctx, trustRejection(cache.RejectionReasonStoragePrefix, cause,
			"%s %s metadata", cause, cache.StoragePrefixGRPCMetadataKey)
	}
	if !cache.ValidStoragePrefix(prefix) {
		storagePrefixRejected.WithLabelValues("invalid").Inc()
		return ctx, trustRejection(cache.RejectionReasonStoragePrefix, "invalid",
			"invalid %s metadata value", cache.StoragePrefixGRPCMetadataKey)
	}
	ctx = cache.WithRequiredStoragePrefix(cache.WithStoragePrefix(ctx, prefix))

	if values := md.Get(cache.FallbackPrefixesGRPCMetadataKey); len(values) > 0 {
		if len(values) > 1 {
			storagePrefixRejected.WithLabelValues("duplicate_fallback").Inc()
			return ctx, trustRejection(cache.RejectionReasonStoragePrefix, "duplicate_fallback",
				"duplicate %s metadata", cache.FallbackPrefixesGRPCMetadataKey)
		}
		fallbacks, ok := cache.ParseFallbackPrefixes(values[0])
		if !ok {
			storagePrefixRejected.WithLabelValues("invalid_fallback").Inc()
			return ctx, trustRejection(cache.RejectionReasonStoragePrefix, "invalid_fallback",
				"invalid %s metadata value", cache.FallbackPrefixesGRPCMetadataKey)
		}
		ctx = cache.WithFallbackStoragePrefixes(ctx, fallbacks)
	}

	if values := md.Get(cache.ACImmutableGRPCMetadataKey); len(values) > 0 {
		if len(values) != 1 || values[0] != "1" {
			storagePrefixRejected.WithLabelValues("invalid_ac_immutable").Inc()
			return ctx, trustRejection(cache.RejectionReasonStoragePrefix, "invalid_ac_immutable",
				"invalid %s metadata value", cache.ACImmutableGRPCMetadataKey)
		}
		ctx = cache.WithACImmutable(ctx)
	}
	return ctx, nil
}

// GRPCStoragePrefixUnaryServerInterceptor returns a unary interceptor that
// enforces the fail-closed trust contract and lifts the forwarded storage
// prefix onto the context. authSecret is a comma-separated list of accepted
// secrets; whitespace around each entry is ignored. When non-empty, exactly
// one cache.AuthSecretGRPCMetadataKey value must match a non-empty entry on
// every non-exempt RPC. An empty authSecret disables the secret check.
func GRPCStoragePrefixUnaryServerInterceptor(authSecret string) grpc.UnaryServerInterceptor {
	authSecrets := parseAuthSecrets(authSecret)
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if exemptFromTenantMetadata(info.FullMethod) {
			return handler(ctx, req)
		}
		ctx, err := storagePrefixFromIncomingContext(ctx, authSecrets)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// GRPCStoragePrefixStreamServerInterceptor returns a stream interceptor that
// enforces the fail-closed trust contract and lifts the forwarded storage
// prefix onto the context. authSecret has the same comma-separated format
// as GRPCStoragePrefixUnaryServerInterceptor.
func GRPCStoragePrefixStreamServerInterceptor(authSecret string) grpc.StreamServerInterceptor {
	authSecrets := parseAuthSecrets(authSecret)
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if exemptFromTenantMetadata(info.FullMethod) {
			return handler(srv, ss)
		}
		ctx, err := storagePrefixFromIncomingContext(ss.Context(), authSecrets)
		if err != nil {
			return err
		}
		return handler(srv, &tenantMetadataServerStream{ServerStream: ss, ctx: ctx})
	}
}
