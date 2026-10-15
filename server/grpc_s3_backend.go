package server

import (
	"context"
	"fmt"

	"github.com/buchgr/bazel-remote/v2/cache"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// S3-backend routing interceptors: when an L1 bazel-remote node is configured
// with a map of allowlisted S3 backends (multi-shard MinIO), the trusted
// upstream (FA host) forwards each tenant's pinned backing-store endpoint and
// bucket as gRPC metadata (cache.S3BackendGRPCMetadataKey and
// cache.S3BucketGRPCMetadataKey). These interceptors resolve the forwarded
// pair against the map and lift the resolved selection onto the request
// context, from which the s3proxy routes reads and write-through to the
// matching MinIO cluster and bucket.
//
// Resolution never rejects: a request whose selector is missing or not in
// the map routes to the map's designated DEFAULT backend, and a request
// whose bucket is missing or not in the resolved entry's allowed set uses
// that entry's default bucket — mirroring the actions cache's shard router,
// where placement values are backend-authored hints and anything unresolvable
// falls back to the default client. The L1 owns which MinIO backends exist
// (its config is the map); the upstream does not need any endpoint knowledge
// of its own to be served. Every defaulted resolution is metered by cause
// (s3BackendSelectorDefaulted) and rate-limit logged: a nonzero rate during
// a backends-map rollout is the config-drift signal (FA forwarding values an
// L1's map does not — or does not yet — contain), and sustained defaulting
// means a namespace's pin and the map have diverged, which quietly serves
// that tenant from the default shard (cold reads) until reconciled.
//
// History: these interceptors originally enforced the pair fail-closed
// (InvalidArgument trust rejections). That turned every config race into a
// build-visible event, and — because the FA boot probe presented the host's
// Doppler MINIO_ENDPOINT as its identity — let one stale host-level env
// value knock whole hosts off the L1 ring into direct-S3 fallback (dialing
// that same stale value). See the 2026-09-03 us-west legacy-cluster
// teardown: 134 hosts in s3_fallback with no working cache path.
// Tenant isolation does not depend on this check — the storage-prefix
// interceptor (still fail-closed) scopes every request's keyspace; the
// selector only picks which shard serves it.
//
// Health and capabilities RPCs are exempt, exactly like the storage-prefix
// contract. These interceptors are only installed in multi-backend mode;
// single-backend deployments ignore both metadata keys entirely (backward
// compatible).

// S3BackendRouting is the routing table the interceptors resolve against:
// every configured selector key with its default bucket and allowed bucket
// set, plus the designated default key for requests that carry no usable
// selector (built from config.S3CloudStorageConfig.RoutingBackends).
type S3BackendRouting struct {
	DefaultKey string
	Backends   map[string]S3BackendRoutingEntry
}

// S3BackendRoutingEntry mirrors config.S3BackendRoutingEntry (the server
// package deliberately does not import config).
type S3BackendRoutingEntry struct {
	DefaultBucket string
	Buckets       map[string]bool
}

// s3BackendSelectorDefaulted meters resolutions that fell back to a default,
// by cause. missing/duplicate/unknown mean the selector itself was unusable
// (routed to the default backend); bucket_missing/bucket_duplicate/
// bucket_unknown mean the bucket was unusable for the resolved entry (its
// default bucket was used). Nonzero during a backends-map rollout indicates
// FA/L1 config drift; sustained nonzero means a pinned namespace is being
// served from a default it was not allocated on — reconcile the map or the
// pin.
var s3BackendSelectorDefaulted = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bazel_remote_s3_backend_selector_defaulted_total",
	Help: "Cache RPCs whose forwarded S3 backend selector or bucket could not be resolved and fell back to a default, by cause (missing/duplicate/unknown route to the default backend; bucket_* use the resolved entry's default bucket). Nonzero indicates FA/L1 backends-map drift.",
}, []string{"reason"})

// s3BackendFromIncomingContext resolves the forwarded (endpoint, bucket)
// pair against the routing table and lifts the resolved selection onto the
// context. Resolution never fails — see the package comment for the
// defaulting contract.
func s3BackendFromIncomingContext(ctx context.Context, routing S3BackendRouting) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)

	key, cause := singleMetadataValue(md, cache.S3BackendGRPCMetadataKey)
	if cause == "" {
		// Exact opaque string match against the configured backends map
		// keys — no URL normalization. Web pins namespaces by these exact
		// strings; anything else is drift and routes to the default.
		if _, ok := routing.Backends[key]; !ok {
			cause = "unknown"
		}
	}
	if cause != "" {
		s3BackendSelectorDefaulted.WithLabelValues(cause).Inc()
		logRateLimited("s3_backend_defaulted/"+cause,
			"S3 backend selector unresolvable (%s %s metadata%s); routing to the default backend %q",
			cause, cache.S3BackendGRPCMetadataKey, selectorDetail(cause, key), routing.DefaultKey)
		key = routing.DefaultKey
	}
	entry := routing.Backends[key]

	bucket, bucketCause := singleMetadataValue(md, cache.S3BucketGRPCMetadataKey)
	if bucketCause == "" && !entry.Buckets[bucket] {
		// Byte-verbatim match against the resolved entry's allowed set
		// (default bucket plus extra_buckets) — the pair is resolved
		// together: a bucket allowed on another endpoint defaults here.
		bucketCause = "unknown"
	}
	if bucketCause != "" {
		cause := "bucket_" + bucketCause
		s3BackendSelectorDefaulted.WithLabelValues(cause).Inc()
		logRateLimited("s3_backend_defaulted/"+cause,
			"S3 bucket unresolvable for backend %q (%s %s metadata%s); using the entry's default bucket %q",
			key, bucketCause, cache.S3BucketGRPCMetadataKey, selectorDetail(bucketCause, bucket), entry.DefaultBucket)
		bucket = entry.DefaultBucket
	}

	return cache.WithS3Backend(ctx, cache.S3BackendSelection{Endpoint: key, Bucket: bucket})
}

// selectorDetail renders the offending value for the rate-limited log line —
// only for the "unknown" cause, where a concrete value exists to show.
func selectorDetail(cause, value string) string {
	if cause != "unknown" {
		return ""
	}
	return fmt.Sprintf(" value %q", value)
}

// GRPCS3BackendUnaryServerInterceptor returns a unary interceptor that
// resolves the forwarded (endpoint, bucket) pair per the defaulting contract
// and lifts the resolved selection onto the context.
func GRPCS3BackendUnaryServerInterceptor(routing S3BackendRouting) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if exemptFromTenantMetadata(info.FullMethod) {
			return handler(ctx, req)
		}
		return handler(s3BackendFromIncomingContext(ctx, routing), req)
	}
}

// GRPCS3BackendStreamServerInterceptor returns a stream interceptor that
// resolves the forwarded (endpoint, bucket) pair per the defaulting contract
// and lifts the resolved selection onto the context.
func GRPCS3BackendStreamServerInterceptor(routing S3BackendRouting) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if exemptFromTenantMetadata(info.FullMethod) {
			return handler(srv, ss)
		}
		ctx := s3BackendFromIncomingContext(ss.Context(), routing)
		return handler(srv, &tenantMetadataServerStream{ServerStream: ss, ctx: ctx})
	}
}
