package server

import (
	"context"
	"crypto/subtle"

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
	Help: "Cache RPCs rejected by the shared-secret auth check, by cause (missing/duplicate/mismatch). Nonzero means the upstream's BAZELRE_CACHE_L1_SECRET disagrees with this node's configured secret.",
}, []string{"reason"})

func storagePrefixFromIncomingContext(ctx context.Context, authSecret string) (context.Context, error) {
	md, _ := metadata.FromIncomingContext(ctx)

	if authSecret != "" {
		secrets := md.Get(cache.AuthSecretGRPCMetadataKey)
		// The value count is not secret-dependent, so distinguishing
		// missing/duplicate from mismatch leaks nothing; only the value
		// comparison itself must be constant-time.
		cause := ""
		switch {
		case len(secrets) == 0:
			cause = "missing"
		case len(secrets) > 1:
			cause = "duplicate"
		case subtle.ConstantTimeCompare([]byte(secrets[0]), []byte(authSecret)) != 1:
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
	return cache.WithRequiredStoragePrefix(cache.WithStoragePrefix(ctx, prefix)), nil
}

// GRPCStoragePrefixUnaryServerInterceptor returns a unary interceptor that
// enforces the fail-closed trust contract and lifts the forwarded storage
// prefix onto the context. authSecret, when non-empty, must match the
// cache.AuthSecretGRPCMetadataKey metadata on every non-exempt RPC.
func GRPCStoragePrefixUnaryServerInterceptor(authSecret string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if exemptFromTenantMetadata(info.FullMethod) {
			return handler(ctx, req)
		}
		ctx, err := storagePrefixFromIncomingContext(ctx, authSecret)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// GRPCStoragePrefixStreamServerInterceptor returns a stream interceptor that
// enforces the fail-closed trust contract and lifts the forwarded storage
// prefix onto the context.
func GRPCStoragePrefixStreamServerInterceptor(authSecret string) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if exemptFromTenantMetadata(info.FullMethod) {
			return handler(srv, ss)
		}
		ctx, err := storagePrefixFromIncomingContext(ss.Context(), authSecret)
		if err != nil {
			return err
		}
		return handler(srv, &tenantMetadataServerStream{ServerStream: ss, ctx: ctx})
	}
}
