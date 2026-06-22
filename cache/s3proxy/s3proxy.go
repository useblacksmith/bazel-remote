package s3proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"path"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/cache/disk/casblob"
	"github.com/buchgr/bazel-remote/v2/utils/backendproxy"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics is an optional sink for s3proxy reliability signals. It lets an
// embedder (e.g. the Blacksmith FA agent) record events without this module
// importing the embedder. Implementations must be safe to call from background
// upload-worker goroutines and tolerate a nil receiver being skipped by callers.
type Metrics interface {
	// IncPrefixMissing is invoked when a request that required a request-scoped
	// storage prefix did not carry one, so the configured fallback prefix was
	// used instead (a potential cross-namespace read/write). operation is one of
	// "UPLOAD", "DOWNLOAD", "CONTAINS".
	IncPrefixMissing(operation string)
}

type s3Cache struct {
	mcore            *minio.Core
	prefix           string
	bucket           string
	uploadQueue      chan<- backendproxy.UploadReq
	accessLogger     cache.Logger
	errorLogger      cache.Logger
	metrics          Metrics
	v2mode           bool
	updateTimestamps bool
	objectKey        func(prefix string, hash string, kind cache.EntryKind) string
	observer         cache.OperationObserver
}

type Option func(*s3Cache)

func WithOperationObserver(observer cache.OperationObserver) Option {
	return func(c *s3Cache) {
		c.observer = observer
	}
}

var (
	cacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bazel_remote_s3_cache_hits",
		Help: "The total number of s3 backend cache hits",
	})
	cacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bazel_remote_s3_cache_misses",
		Help: "The total number of s3 backend cache misses",
	})
)

// Used in place of minio's verbose "NoSuchKey" error.
var errNotFound = errors.New("NOT FOUND")

// New returns a new instance of the S3-API based cache
func New(
	// S3CloudStorageConfig struct fields:
	Endpoint string,
	Bucket string,
	BucketLookupType minio.BucketLookupType,
	Prefix string,
	Credentials *credentials.Credentials,
	DisableSSL bool,
	UpdateTimestamps bool,
	Region string,

	storageMode string, accessLogger cache.Logger,
	errorLogger cache.Logger, numUploaders, maxQueuedUploads int,
	metrics Metrics, options ...Option) cache.Proxy {

	fmt.Println("Using S3 backend.")

	var minioCore *minio.Core
	var err error

	if Credentials == nil {
		log.Fatalf("Failed to determine s3proxy credentials")
	}

	// Initialize minio client with credentials
	minioOpts := &minio.Options{
		Creds:        Credentials,
		BucketLookup: BucketLookupType,

		Region: Region,
		Secure: !DisableSSL,
	}
	minioCore, err = minio.NewCore(Endpoint, minioOpts)
	if err != nil {
		log.Fatalln(err)
	}

	if storageMode != "zstd" && storageMode != "uncompressed" {
		log.Fatalf("Unsupported storage mode for the s3proxy backend: %q, must be one of \"zstd\" or \"uncompressed\"",
			storageMode)
	}

	c := &s3Cache{
		mcore:            minioCore,
		prefix:           Prefix,
		bucket:           Bucket,
		accessLogger:     accessLogger,
		errorLogger:      errorLogger,
		metrics:          metrics,
		v2mode:           storageMode == "zstd",
		updateTimestamps: UpdateTimestamps,
	}
	for _, opt := range options {
		opt(c)
	}

	if c.v2mode {
		c.objectKey = objectKeyV2
	} else {
		c.objectKey = objectKeyV1
	}

	c.uploadQueue = backendproxy.StartUploaders(c, numUploaders, maxQueuedUploads)

	return c
}

func objectKeyV2(prefix string, hash string, kind cache.EntryKind) string {
	var baseKey string
	if kind == cache.CAS {
		// Use "cas.v2" to distinguish new from old format blobs.
		baseKey = path.Join("cas.v2", hash[:2], hash)
	} else {
		baseKey = path.Join(kind.String(), hash[:2], hash)
	}

	if prefix == "" {
		return baseKey
	}

	return path.Join(prefix, baseKey)
}

func objectKeyV1(prefix string, hash string, kind cache.EntryKind) string {
	if prefix == "" {
		return path.Join(kind.String(), hash[:2], hash)
	}

	return path.Join(prefix, kind.String(), hash[:2], hash)
}

func (c *s3Cache) prefixForContext(ctx context.Context, kind cache.EntryKind) (string, bool, bool) {
	if kind != cache.RAW {
		if prefix, ok := cache.StoragePrefixFromContext(ctx); ok {
			return prefix, true, cache.StoragePrefixRequiredFromContext(ctx)
		}
		return c.prefix, false, cache.StoragePrefixRequiredFromContext(ctx)
	}
	return c.prefix, false, false
}

func (c *s3Cache) objectKeyForPrefix(prefix string, hash string, kind cache.EntryKind) string {
	return c.objectKey(prefix, hash, kind)
}

func (c *s3Cache) objectKeyForContext(ctx context.Context, hash string, kind cache.EntryKind) string {
	prefix, _, _ := c.prefixForContext(ctx, kind)
	return c.objectKeyForPrefix(prefix, hash, kind)
}

func (c *s3Cache) logMissingRequiredStoragePrefix(operation string, kind cache.EntryKind, hash string) {
	if c.metrics != nil {
		c.metrics.IncPrefixMissing(operation)
	}
	if c.errorLogger == nil {
		return
	}
	c.errorLogger.Printf(
		"S3 %s missing request-scoped storage prefix for %s %s; using configured prefix %q",
		operation,
		kind.String(),
		hash,
		c.prefix,
	)
}

// Helper function for logging responses
func logResponse(log cache.Logger, method, bucket, key string, err error) {
	status := "OK"
	if err != nil {
		status = err.Error()
	}

	log.Printf("S3 %s %s %s %s", method, bucket, key, status)
}

func (c *s3Cache) UploadFile(item backendproxy.UploadReq) {
	prefix := item.StoragePrefix
	requestScopedPrefix := item.RequestScopedStoragePrefix
	requirePrefix := item.RequireStoragePrefix
	if item.Kind == cache.RAW {
		prefix = c.prefix
		requestScopedPrefix = false
		requirePrefix = false
	}
	if prefix == "" {
		prefix = c.prefix
	}
	if requirePrefix && !requestScopedPrefix {
		c.logMissingRequiredStoragePrefix("UPLOAD", item.Kind, item.Hash)
	}
	objectKey := c.objectKeyForPrefix(prefix, item.Hash, item.Kind)
	_, err := c.mcore.PutObject(
		context.Background(),
		c.bucket,        // bucketName
		objectKey,       // objectName
		item.Rc,         // reader
		item.SizeOnDisk, // objectSize
		"",              // md5base64
		"",              // sha256
		minio.PutObjectOptions{
			UserMetadata: map[string]string{
				"Content-Type": "application/octet-stream",
			},
		}, // metadata
	)

	logResponse(c.accessLogger, "UPLOAD", c.bucket, objectKey, err)
	if err != nil {
		c.observeUpload(context.Background(), item, "error", "s3_put_failed")
	}

	item.Rc.Close()
}

func (c *s3Cache) Put(ctx context.Context, kind cache.EntryKind, hash string, logicalSize int64, sizeOnDisk int64, rc io.ReadCloser) {
	if c.uploadQueue == nil {
		rc.Close()
		return
	}
	prefix, requestScopedPrefix, requirePrefix := c.prefixForContext(ctx, kind)
	labels, _ := cache.MetricsLabelsFromContext(ctx)

	select {
	case c.uploadQueue <- backendproxy.UploadReq{
		Hash:                       hash,
		LogicalSize:                logicalSize,
		SizeOnDisk:                 sizeOnDisk,
		Kind:                       kind,
		Rc:                         rc,
		StoragePrefix:              prefix,
		RequestScopedStoragePrefix: requestScopedPrefix,
		RequireStoragePrefix:       requirePrefix,
		MetricsLabels:              labels,
	}:
	default:
		c.errorLogger.Printf("too many uploads queued\n")
		cache.ObserveOperation(ctx, c.observer, cache.OperationOutcome{
			Method: "backend_upload",
			Status: "dropped",
			Reason: "upload_queue_full",
			Ops:    1,
			Bytes:  nonNegativeUint64(logicalSize),
		})
		rc.Close()
	}
}

func (c *s3Cache) UpdateModificationTimestamp(ctx context.Context, bucket string, object string) {
	src := minio.CopySrcOptions{
		Bucket: bucket,
		Object: object,
	}

	dst := minio.CopyDestOptions{
		Bucket:          bucket,
		Object:          object,
		ReplaceMetadata: true,
	}

	_, err := c.mcore.ComposeObject(context.Background(), dst, src)

	logResponse(c.accessLogger, "COMPOSE", bucket, object, err)
}

func (c *s3Cache) Get(ctx context.Context, kind cache.EntryKind, hash string, _ int64) (io.ReadCloser, int64, error) {
	prefix, requestScopedPrefix, requirePrefix := c.prefixForContext(ctx, kind)
	if requirePrefix && !requestScopedPrefix {
		c.logMissingRequiredStoragePrefix("DOWNLOAD", kind, hash)
	}
	objectKey := c.objectKeyForPrefix(prefix, hash, kind)

	rc, info, _, err := c.mcore.GetObject(
		ctx,
		c.bucket,                 // bucketName
		objectKey,                // objectName
		minio.GetObjectOptions{}, // opts
	)
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			cacheMisses.Inc()
			logResponse(c.accessLogger, "DOWNLOAD", c.bucket, objectKey, errNotFound)
			return nil, -1, nil
		}
		cacheMisses.Inc()
		logResponse(c.accessLogger, "DOWNLOAD", c.bucket, objectKey, err)
		return nil, -1, err
	}
	cacheHits.Inc()

	if c.updateTimestamps {
		c.UpdateModificationTimestamp(ctx, c.bucket, objectKey)
	}

	logResponse(c.accessLogger, "DOWNLOAD", c.bucket, objectKey, nil)

	if kind == cache.CAS && c.v2mode {
		return casblob.ExtractLogicalSize(rc)
	}

	return rc, info.Size, nil
}

func (c *s3Cache) Contains(ctx context.Context, kind cache.EntryKind, hash string, _ int64) (bool, int64) {
	size := int64(-1)
	exists := false
	prefix, requestScopedPrefix, requirePrefix := c.prefixForContext(ctx, kind)
	if requirePrefix && !requestScopedPrefix {
		c.logMissingRequiredStoragePrefix("CONTAINS", kind, hash)
	}
	objectKey := c.objectKeyForPrefix(prefix, hash, kind)

	s, err := c.mcore.StatObject(
		ctx,
		c.bucket,                  // bucketName
		objectKey,                 // objectName
		minio.StatObjectOptions{}, // opts
	)

	exists = (err == nil)
	if err != nil {
		err = errNotFound
	} else if kind != cache.CAS || !c.v2mode {
		size = s.Size
	}

	logResponse(c.accessLogger, "CONTAINS", c.bucket, objectKey, err)

	return exists, size
}

func (c *s3Cache) observeUpload(ctx context.Context, item backendproxy.UploadReq, status string, reason string) {
	cache.ObserveOperation(cache.WithMetricsLabels(ctx, item.MetricsLabels), c.observer, cache.OperationOutcome{
		Method: "backend_upload",
		Status: status,
		Reason: reason,
		Ops:    1,
		Bytes:  nonNegativeUint64(item.LogicalSize),
	})
}

func nonNegativeUint64(value int64) uint64 {
	if value < 0 {
		return 0
	}
	return uint64(value)
}
