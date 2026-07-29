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

// S3BucketGRPCMetadataKey is the gRPC metadata key carrying the tenant's
// pinned bucket alongside the endpoint above: the namespace's
// `bazelre_cache_bucket` from the adoption payload, forwarded byte-verbatim.
// The bucket travels separately from the endpoint because web snapshots it
// per namespace at allocation time — two namespaces pinned to the same
// endpoint can legitimately live in different buckets (pre-rename buckets
// from the cache-sharding migration), so the endpoint alone does not
// determine where a tenant's objects are.
//
// The L1 validates the (endpoint, bucket) PAIR against the configured
// backends map: the bucket must be in the matched entry's allowlisted bucket
// set (its default bucket plus extra_buckets). When a backends map is
// configured every cache RPC must carry exactly one allowed bucket or it is
// rejected fail-closed with cause bucket_missing/bucket_duplicate/
// bucket_unknown. Deployments without a backends map ignore this metadata
// entirely, same mixed-version guarantee as the endpoint key.
const S3BucketGRPCMetadataKey = "x-blacksmith-s3-bucket"

// S3BackendSelection is the validated (endpoint, bucket) routing pair carried
// on the request context: which allowlisted backend entry serves the request
// (Endpoint, the opaque selector — a configured backends-map key) and which
// bucket within that backend the tenant's objects live in (Bucket). The pair
// travels together because neither half alone identifies the tenant's
// storage: connections are per endpoint, but the bucket is per request.
//
// Bucket may be empty on contexts built by an upstream that predates the
// bucket contract; the s3proxy then falls back to the matched entry's default
// bucket. Contexts minted by the L1 trust interceptor always carry both.
type S3BackendSelection struct {
	Endpoint string
	Bucket   string
}

type s3BackendContextKey struct{}

// WithS3Backend returns a context whose cache backend operations should be
// routed to the S3 backend registered under selection.Endpoint, targeting
// selection.Bucket.
func WithS3Backend(ctx context.Context, selection S3BackendSelection) context.Context {
	return context.WithValue(ctx, s3BackendContextKey{}, selection)
}

// S3BackendFromContext returns the request-scoped S3 backend selection when
// one was attached to ctx. A selection without an endpoint is treated as
// absent: the endpoint is the routing key, so a bucket alone is meaningless.
func S3BackendFromContext(ctx context.Context) (S3BackendSelection, bool) {
	if ctx == nil {
		return S3BackendSelection{}, false
	}
	selection, ok := ctx.Value(s3BackendContextKey{}).(S3BackendSelection)
	if !ok || selection.Endpoint == "" {
		return S3BackendSelection{}, false
	}
	return selection, true
}
