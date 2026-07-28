package cache

import "context"

// S3BackendGRPCMetadataKey is the gRPC metadata key a trusted upstream
// bazel-remote (the FA host's embedded instance) uses to forward the tenant's
// pinned backing-store endpoint to a downstream L1 node. The value is the
// namespace's `bazelre_cache_endpoint` from the adoption payload (web's
// CacheShardRegistry endpointUrl — a fully qualified URL such as
// "http://staging-minio.uswest.blacksmith.sh:9000"), forwarded verbatim.
//
// The L1 treats the value as an opaque selector: it must match a key of the
// configured s3_proxy backends map exactly (no URL normalization), and when a
// backends map is configured every cache RPC must carry exactly one allowed
// value or it is rejected fail-closed (see server.GRPCS3Backend*Interceptor).
// Deployments without a backends map ignore this metadata entirely, which is
// today's behavior (single --s3.* backend) and keeps mixed versions safe: FA
// may start forwarding endpoints before any L1 is switched to a backend map.
const S3BackendGRPCMetadataKey = "x-blacksmith-s3-endpoint"

type s3BackendContextKey struct{}

// WithS3Backend returns a context whose cache backend operations should be
// routed to the S3 backend registered under the given selector (the tenant's
// pinned backing-store endpoint URL).
func WithS3Backend(ctx context.Context, selector string) context.Context {
	return context.WithValue(ctx, s3BackendContextKey{}, selector)
}

// S3BackendFromContext returns the request-scoped S3 backend selector when
// one was attached to ctx.
func S3BackendFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	selector, ok := ctx.Value(s3BackendContextKey{}).(string)
	if !ok || selector == "" {
		return "", false
	}
	return selector, true
}
