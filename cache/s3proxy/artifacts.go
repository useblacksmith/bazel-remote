package s3proxy

import (
	"bytes"
	"context"

	"github.com/minio/minio-go/v7"
)

// LRU observation artifacts must land in the same (endpoint, bucket, prefix)
// keyspace as the cache entries they describe: the web-side retention sweep
// follows the namespace row's shard pin and reads `<storagePrefix>lru/`, so
// an artifact written anywhere else is invisible to it. The cache.Proxy
// interface has no vocabulary for non-cache-entry objects, hence this narrow
// side surface implemented by both the single- and multi-backend proxies.
// The interface it satisfies is owned by the sole consumer, lruflush.Sink:
// routing (which backend, which bucket) comes from the request-scoped
// selection on ctx, exactly like cache operations; the caller owns key
// composition and retries.

// artifactObjectTagKey/Value tag every artifact so a single bucket-wide MinIO
// lifecycle (ILM) expiration rule can target LRU artifacts by tag. The `lru/`
// segment is nested inside the per-tenant storage prefix, so a key-prefix
// lifecycle filter cannot match it; a tag filter can. The TTL is a backstop
// to the sweep's own artifact GC. The values are a wire contract with the
// deployed ILM rules — do not change them.
const (
	artifactObjectTagKey   = "lru"
	artifactObjectTagValue = "true"
)

// PutArtifact uploads one artifact through this backend's minio core. The
// per-backend breaker is consulted READ-ONLY for the sick-shard fail-fast
// (when the shard is struggling, failing the flush fast beats stacking
// artifact PUTs on top of it), but the call runs outside the breaker and
// never records an outcome. Advisory traffic is contractually barred from
// influencing cache behavior in either direction: an artifact-only failure
// (say, a bucket tagging-permission gap) must not open the breaker against
// customer reads, and a small artifact success must not reset the failure
// streak of a real read brownout or consume the half-open probe slot that a
// cache read needs to close the breaker.
func (c *s3Cache) PutArtifact(ctx context.Context, key string, body []byte) error {
	bucket := c.bucketForContext(ctx)
	if !c.breaker.isClosed() {
		logResponse(c.accessLogger, "LRU_ARTIFACT", bucket, key, errBreakerOpen)
		return errBreakerOpen
	}
	opts := minio.PutObjectOptions{
		ContentType: "application/x-ndjson",
		UserTags:    map[string]string{artifactObjectTagKey: artifactObjectTagValue},
	}
	_, err := c.mcore.PutObject(ctx, bucket, key,
		bytes.NewReader(body), int64(len(body)), "", "", opts)
	logResponse(c.accessLogger, "LRU_ARTIFACT", bucket, key, err)
	return err
}

// PutArtifact routes to the backend selected on ctx, mirroring the cache
// operations' dispatch: missing selector uses the default backend (metered by
// backendFor), unknown selector refuses rather than guessing a shard.
func (m *multiS3Cache) PutArtifact(ctx context.Context, key string, body []byte) error {
	backend := m.backendFor(ctx, "LRU_ARTIFACT")
	if backend == nil {
		return errUnknownBackend
	}
	return backend.PutArtifact(ctx, key, body)
}
