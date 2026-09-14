package s3proxy

// v1 CAS fallback: hydrate zstd-mode (v2) backends from the uncompressed
// v1 object keyspace.
//
// storage_mode selects both the local blob format and the backend contract:
// v2mode reads and writes casblob objects under <prefix>/cas.v2/, so the
// pre-existing raw objects under <prefix>/cas/ are invisible to it. Without
// a fallback, every node flipped from uncompressed to zstd starts with a
// cold CAS backend layer — ring moves, evictions, and node rebuilds during
// the format transition all miss to MinIO for content it actually holds.
//
// The fallback runs only on a v2 CAS miss: fetch the v1 object, transcode
// it to a casblob through the same WriteAndClose path client uploads use
// (content hash and size verified), serve the casblob to the caller, and
// promote it to the v2 key via the ordinary write-through queue. First
// access migrates a key permanently; once cas.v2 is warm the fallback goes
// quiet (watch bazel_remote_s3_v1_fallback_total). Transcodes buffer
// through a temp file because the casblob container header (chunk index)
// cannot be produced in one streaming pass.
//
// The fallback is enabled automatically in v2mode. Kill switch without a
// binary roll: BAZEL_REMOTE_DISABLE_S3_V1_FALLBACK=1.

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/cache/disk/casblob"

	"github.com/minio/minio-go/v7"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const disableV1FallbackEnv = "BAZEL_REMOTE_DISABLE_S3_V1_FALLBACK"

var v1FallbackEvents = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bazel_remote_s3_v1_fallback_total",
	Help: "Outcomes of v1 CAS keyspace fallback lookups from a v2 (zstd) backend.",
}, []string{"backend", "event"})

const (
	v1FallbackGetHit      = "get_hit"
	v1FallbackGetMiss     = "get_miss"
	v1FallbackContainsHit = "contains_hit"
	v1FallbackError       = "error"
)

// WithV1FallbackTempDir sets the directory used to buffer v1 objects while
// they are transcoded to casblobs. It should live on a filesystem large
// enough for the biggest expected CAS blob (the cache volume, not tmpfs).
func WithV1FallbackTempDir(dir string) Option {
	return func(c *s3Cache) {
		c.v1FallbackTmpDir = dir
	}
}

// getV1Fallback is called on a v2 CAS read miss. It returns a casblob
// stream and the blob's logical size, or (nil, -1, nil) for a miss —
// mirroring Get's contract. Every problem on this path degrades to a
// miss, never an error: before the fallback existed a v2 NoSuchKey was a
// terminal miss, and hydration must not turn that into a client-visible
// failure. expectedSize (when >= 0) gates the transcode: the fallback
// buffers the whole object, so a size mismatch the disk layer would
// reject anyway must be caught before any bytes move.
func (c *s3Cache) getV1Fallback(ctx context.Context, prefix string, bucket string, hash string, expectedSize int64) (io.ReadCloser, int64, error) {
	objectKey := objectKeyV1(prefix, hash, cache.CAS)

	rctx, cancel := context.WithTimeout(ctx, c.effectiveReadDeadline())

	if !c.breaker.allow() {
		cancel()
		cacheMisses.WithLabelValues(c.key).Inc()
		logResponse(c.accessLogger, "DOWNLOAD", bucket, objectKey, errBreakerOpen)
		return nil, -1, nil
	}

	rc, info, _, getErr := c.mcore.GetObject(rctx, bucket, objectKey, minio.GetObjectOptions{})
	if getErr != nil {
		c.breaker.record(breakerReadOutcome(ctx, getErr))
		cancel()
		cacheMisses.WithLabelValues(c.key).Inc()
		if minio.ToErrorResponse(getErr).Code == "NoSuchKey" {
			v1FallbackEvents.WithLabelValues(c.key, v1FallbackGetMiss).Inc()
			logResponse(c.accessLogger, "DOWNLOAD", bucket, objectKey, errNotFound)
		} else {
			v1FallbackEvents.WithLabelValues(c.key, v1FallbackError).Inc()
			logResponse(c.accessLogger, "DOWNLOAD", bucket, objectKey, getErr)
		}
		return nil, -1, nil
	}

	body := &bodyOutcomeReadCloser{
		ReadCloser: rc,
		cancel:     cancel,
		breaker:    c.breaker,
		parent:     ctx,
		rctx:       rctx,
	}

	if expectedSize >= 0 && info.Size != expectedSize {
		_ = body.Close()
		v1FallbackEvents.WithLabelValues(c.key, v1FallbackError).Inc()
		cacheMisses.WithLabelValues(c.key).Inc()
		c.errorLogger.Printf("S3 v1 fallback for %s: object size %d does not match expected %d, treating as miss",
			objectKey, info.Size, expectedSize)
		return nil, -1, nil
	}

	fillRC, logicalSize, err := c.transcodeV1(ctx, hash, info.Size, body)
	if err != nil {
		// The v1 object exists but could not be transcoded (stream died,
		// hash mismatch, local temp I/O). Degrade to a miss: the client
		// rebuilds, and the rebuild's write-through repopulates cas.v2.
		v1FallbackEvents.WithLabelValues(c.key, v1FallbackError).Inc()
		cacheMisses.WithLabelValues(c.key).Inc()
		c.errorLogger.Printf("S3 v1 fallback transcode for %s failed, treating as miss: %v", objectKey, err)
		return nil, -1, nil
	}

	v1FallbackEvents.WithLabelValues(c.key, v1FallbackGetHit).Inc()
	cacheHits.WithLabelValues(c.key).Inc()
	logResponse(c.accessLogger, "DOWNLOAD", bucket, objectKey, nil)
	return fillRC, logicalSize, nil
}

// transcodeV1 buffers a raw v1 CAS object into a zstd casblob via a temp
// file, verifying content hash and size, then returns a reader of the
// casblob for the disk fill and enqueues a second reader as a write-through
// promotion to the v2 key. body is always closed.
func (c *s3Cache) transcodeV1(ctx context.Context, hash string, logicalSize int64, body io.ReadCloser) (io.ReadCloser, int64, error) {
	tmpDir := c.v1FallbackTmpDir
	if tmpDir == "" {
		tmpDir = os.TempDir()
	}
	tf, err := os.CreateTemp(tmpDir, "s3-v1-fallback-*")
	if err != nil {
		_ = body.Close()
		return nil, -1, err
	}
	tmpPath := tf.Name()

	// WriteAndClose verifies the content hash and logical size and closes tf.
	sizeOnDisk, err := casblob.WriteAndClose(c.zstd, body, tf, casblob.Zstandard, hash, logicalSize)
	_ = body.Close()
	if err != nil {
		_ = os.Remove(tmpPath)
		return nil, -1, err
	}

	fillFile, err := os.Open(tmpPath)
	if err != nil {
		_ = os.Remove(tmpPath)
		return nil, -1, err
	}
	promoteFile, err := os.Open(tmpPath)
	// The unlinked path keeps its bytes until both descriptors close.
	_ = os.Remove(tmpPath)
	if err != nil {
		// Promotion is best-effort: serve the fill anyway. The key stays
		// v1-only and migrates on a later access.
		c.errorLogger.Printf("S3 v1 fallback promote open for %s failed: %v", hash, err)
		return fillFile, logicalSize, nil
	}

	// Promote through the ordinary write-through queue: create-if-absent
	// PutObject to the v2 key, standard upload metrics and breaker rules.
	c.Put(ctx, cache.CAS, hash, logicalSize, sizeOnDisk, promoteFile)

	return fillFile, logicalSize, nil
}

// containsV1Fallback is called on a v2 CAS Contains miss. Size is reported
// as -1, matching the v2 CAS convention (logical size is not knowable from
// a stat of either representation).
func (c *s3Cache) containsV1Fallback(ctx context.Context, prefix string, bucket string, hash string) bool {
	objectKey := objectKeyV1(prefix, hash, cache.CAS)

	sctx, cancel := context.WithTimeout(ctx, c.effectiveReadDeadline())
	defer cancel()

	var statErr error
	berr := c.breaker.Execute(func() breakerOutcome {
		_, statErr = c.mcore.StatObject(sctx, bucket, objectKey, minio.StatObjectOptions{})
		return breakerReadOutcome(ctx, statErr)
	})
	if berr != nil || statErr != nil {
		return false
	}

	v1FallbackEvents.WithLabelValues(c.key, v1FallbackContainsHit).Inc()
	logResponse(c.accessLogger, "CONTAINS", bucket, objectKey, nil)
	return true
}

// initV1Fallback wires the fallback into a freshly constructed v2mode
// backend, creating the transcode temp dir. Any failure disables the
// fallback rather than the backend.
func (c *s3Cache) initV1Fallback() {
	if !c.v2mode || os.Getenv(disableV1FallbackEnv) != "" {
		return
	}
	if c.v1FallbackTmpDir != "" {
		if err := os.MkdirAll(c.v1FallbackTmpDir, 0o755); err != nil {
			c.errorLogger.Printf("S3 v1 fallback disabled: cannot create temp dir %s: %v",
				c.v1FallbackTmpDir, err)
			return
		}
		// Reboot hygiene: transcodes interrupted by a crash leave orphans.
		if entries, err := os.ReadDir(c.v1FallbackTmpDir); err == nil {
			for _, e := range entries {
				_ = os.Remove(filepath.Join(c.v1FallbackTmpDir, e.Name()))
			}
		}
	}
	c.v1Fallback = true
}
