package server

import (
	"context"

	"github.com/buchgr/bazel-remote/v2/cache"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// S3-backend trust interceptors: when an L1 bazel-remote node is configured
// with a map of allowlisted S3 backends (multi-shard MinIO), the trusted
// upstream (FA host) forwards each tenant's pinned backing-store endpoint as
// gRPC metadata (cache.S3BackendGRPCMetadataKey). These interceptors validate
// the forwarded endpoint against the allowlist and lift it onto the request
// context, from which the s3proxy routes reads and write-through to the
// matching MinIO cluster.
//
// The trust model mirrors the storage-prefix interceptors (fail-closed):
// when a backends map is configured, every cache RPC must carry exactly one
// allowlisted endpoint or it is rejected at the boundary — a request routed
// to a guessed backend would read or write another shard's keyspace. Health
// and capabilities RPCs are exempt, exactly like the storage-prefix contract.
// These interceptors are only installed in multi-backend mode; single-backend
// deployments ignore the metadata entirely (backward compatible).
//
// Rejections carry the cache.TrustRejectionErrorDomain ErrorInfo marker so
// the upstream degrades them to metered misses instead of failed builds (see
// trustRejection).

// s3BackendSelectorRejected meters the fail-closed rejections above, by
// cause. Nonzero missing/unknown while rolling out a backends-map change is
// the config-race signal: FA forwarding selectors an L1's allowlist does not
// (yet) contain, or not forwarding them at all (version skew).
var s3BackendSelectorRejected = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bazel_remote_s3_backend_selector_rejected_total",
	Help: "Cache RPCs rejected by the S3 backend-selector trust interceptor, by cause (missing/duplicate/unknown). Nonzero during a backends-map rollout indicates FA/L1 config-version skew.",
}, []string{"reason"})

func s3BackendFromIncomingContext(ctx context.Context, allowed map[string]bool) (context.Context, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	selector, cause := singleMetadataValue(md, cache.S3BackendGRPCMetadataKey)
	if cause != "" {
		s3BackendSelectorRejected.WithLabelValues(cause).Inc()
		return ctx, trustRejection(cache.RejectionReasonS3BackendSelector, cause,
			"%s %s metadata", cause, cache.S3BackendGRPCMetadataKey)
	}
	// Exact opaque string match against the configured backends map keys —
	// no URL normalization. The upstream must forward the adoption payload's
	// bazelre_cache_endpoint verbatim, and the map must be keyed by the same
	// strings.
	if !allowed[selector] {
		s3BackendSelectorRejected.WithLabelValues("unknown").Inc()
		return ctx, trustRejection(cache.RejectionReasonS3BackendSelector, "unknown",
			"unknown %s metadata value %q", cache.S3BackendGRPCMetadataKey, selector)
	}
	return cache.WithS3Backend(ctx, selector), nil
}

// GRPCS3BackendUnaryServerInterceptor returns a unary interceptor that
// enforces the fail-closed backend-selection contract and lifts the forwarded
// backend selector onto the context. allowed holds the configured backends
// map keys (tenant-facing endpoint URLs).
func GRPCS3BackendUnaryServerInterceptor(allowed map[string]bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if exemptFromTenantMetadata(info.FullMethod) {
			return handler(ctx, req)
		}
		ctx, err := s3BackendFromIncomingContext(ctx, allowed)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// GRPCS3BackendStreamServerInterceptor returns a stream interceptor that
// enforces the fail-closed backend-selection contract and lifts the forwarded
// backend selector onto the context.
func GRPCS3BackendStreamServerInterceptor(allowed map[string]bool) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if exemptFromTenantMetadata(info.FullMethod) {
			return handler(srv, ss)
		}
		ctx, err := s3BackendFromIncomingContext(ss.Context(), allowed)
		if err != nil {
			return err
		}
		return handler(srv, &tenantMetadataServerStream{ServerStream: ss, ctx: ctx})
	}
}
