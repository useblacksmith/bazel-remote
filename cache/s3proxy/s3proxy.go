package s3proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"path"
	"time"

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
	// key identifies this backend in a multi-backend deployment: the
	// tenant-facing selector from the backends map (also the "backend"
	// metrics label). Single-backend deployments use the endpoint.
	key    string
	mcore  *minio.Core
	prefix string
	// bucket is this backend's default bucket: the target for requests that
	// carry no request-scoped bucket (selector-less HTTP side-door traffic,
	// single-backend deployments). Requests whose context carries a
	// validated (endpoint, bucket) selection use that bucket instead — the
	// minio client and upload queue are per endpoint, but the bucket is per
	// request (see cache.S3BucketGRPCMetadataKey).
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
	cacheHits = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bazel_remote_s3_cache_hits",
		Help: "The total number of s3 backend cache hits",
	}, []string{"backend"})
	cacheMisses = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bazel_remote_s3_cache_misses",
		Help: "The total number of s3 backend cache misses",
	}, []string{"backend"})
	uploadQueueDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bazel_remote_s3_upload_queue_dropped_total",
		Help: "Backend uploads dropped because the S3 upload queue was full.",
	}, []string{"backend"})
	uploadQueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bazel_remote_s3_upload_queue_depth",
		Help: "Queued backend uploads awaiting an S3 upload worker.",
	}, []string{"backend"})
	prefixMissing = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bazel_remote_s3_prefix_missing_total",
		Help: "Requests that required a request-scoped storage prefix but carried none (configured fallback prefix used).",
	}, []string{"operation"})
	// uploadOutcomes is the native terminal-outcome counter for write-through
	// uploads, independent of any installed OperationObserver: standalone
	// (L1-node) deployments install no observer, and without this series a
	// sustained run of failed PUTs drains the queue while every queue-drop
	// alert stays green. Labels are bounded: status/reason come from
	// classifyUploadOutcome (created | already_exists/precondition_failed |
	// error/s3_put_failed).
	uploadOutcomes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bazel_remote_s3_upload_outcomes_total",
		Help: "Terminal S3 write-through upload outcomes per backend (created, already_exists, error).",
	}, []string{"backend", "status", "reason"})
)

// PrometheusMetrics returns a Metrics implementation backed by this package's
// Prometheus counters. Standalone deployments (e.g. an L1 node) use it so the
// prefix-safety signal exports without an embedder-provided sink.
func PrometheusMetrics() Metrics {
	return promMetricsSink{}
}

type promMetricsSink struct{}

func (promMetricsSink) IncPrefixMissing(operation string) {
	prefixMissing.WithLabelValues(operation).Inc()
}

// Used in place of minio's verbose "NoSuchKey" error.
var errNotFound = errors.New("NOT FOUND")

// Connection hygiene defaults. MinIO clusters have no load balancer: the
// endpoint is a DNS name round-robinning bare node IPs, so load spread
// depends on connections being (re-)dialed often enough to sample fresh DNS
// answers. Go's http.Transport resolves DNS only at dial time and reuses a
// pooled connection indefinitely while it keeps getting picked up within
// IdleConnTimeout, so a long-lived consolidating proxy (an L1 node absorbing
// thousands of host pools) can pin its traffic to whichever nodes it happened
// to dial first. Two measures restore deliberate spread:
//
//   - a modest idle-conn cap (32 per host key, 64 total per backend — the
//     execution plan's 16–64 band, and double minio-go's own 16 to absorb
//     request bursts without churn; previously the unset flag clobbered the
//     transport to Go's per-host fallback of 2),
//   - periodic CloseIdleConnections() (config default: every 5 minutes,
//     resolved at config load; non-positive disables), which closes whatever
//     is momentarily idle so subsequent requests re-dial and re-resolve,
//     rotating across MinIO nodes within a few intervals. Dials are cheap
//     here (plaintext, one LAN RTT), so the recycle interval errs toward
//     responsiveness; the per-MinIO-node balance dashboard is the acceptance
//     gauge.
const (
	defaultMaxIdleConns        = 64
	defaultMaxIdleConnsPerHost = 32
)

// uploadTimeout bounds a single backend PutObject, mirroring grpcproxy's
// uploadTimeout: a hung MinIO connection must not pin one of this backend's
// upload workers forever. The bound is generous (large CAS blobs can be slow
// on a busy link); it exists to reclaim workers, not to police latency. A
// var only so tests can shrink it.
var uploadTimeout = 10 * time.Minute

// readDeadline bounds a single read-path MinIO call (the miss fall-through
// GetObject and the Contains StatObject): a sick MinIO node must answer a
// read in seconds or the caller gets a miss-shaped failure instead of a
// pinned connection. The deadline context also governs the streamed GET
// body (canceled when the caller closes it), so a transfer stalled
// mid-stream is reclaimed too. A var only so tests can shrink it.
var readDeadline = 5 * time.Second

// maxRetries caps minio-go's internal per-call retries (library default:
// MaxRetry = 10 attempts with exponential backoff, ~a minute of retention
// per failing call). Three attempts ride out a blip; anything worse must
// surface as a failure in seconds.
const maxRetries = 3

// cancelReadCloser releases the readDeadline timer when the caller finishes
// with a streamed GET body.
type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelReadCloser) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

func newBackend(spec BackendSpec, updateTimestamps bool, connRecycleInterval time.Duration,
	storageMode string, accessLogger cache.Logger, errorLogger cache.Logger,
	numUploaders, maxQueuedUploads int, metrics Metrics, options ...Option) (*s3Cache, error) {

	if spec.Credentials == nil {
		return nil, fmt.Errorf("failed to determine s3proxy credentials for backend %q", spec.Key)
	}

	secure := !spec.DisableSSL
	tr, err := minio.DefaultTransport(secure)
	if err != nil {
		return nil, fmt.Errorf("failed to create default minio transport: %w", err)
	}

	if spec.MaxIdleConns > 0 {
		tr.MaxIdleConns = spec.MaxIdleConns
		tr.MaxIdleConnsPerHost = spec.MaxIdleConns
	} else {
		tr.MaxIdleConns = defaultMaxIdleConns
		tr.MaxIdleConnsPerHost = defaultMaxIdleConnsPerHost
	}

	// connRecycleInterval arrives fully resolved from config load (the
	// default is applied there); non-positive disables recycling.
	if connRecycleInterval > 0 {
		go func() {
			for range time.Tick(connRecycleInterval) {
				tr.CloseIdleConnections()
			}
		}()
	}

	minioOpts := &minio.Options{
		Creds:        spec.Credentials,
		BucketLookup: spec.BucketLookupType,
		MaxRetries:   maxRetries,

		Region:    spec.Region,
		Secure:    secure,
		Transport: tr,
	}
	minioCore, err := minio.NewCore(spec.Endpoint, minioOpts)
	if err != nil {
		return nil, err
	}

	if storageMode != "zstd" && storageMode != "uncompressed" {
		return nil, fmt.Errorf("unsupported storage mode for the s3proxy backend: %q, must be one of \"zstd\" or \"uncompressed\"",
			storageMode)
	}

	key := spec.Key
	if key == "" {
		key = spec.Endpoint
	}

	c := &s3Cache{
		key:              key,
		mcore:            minioCore,
		prefix:           spec.Prefix,
		bucket:           spec.Bucket,
		accessLogger:     accessLogger,
		errorLogger:      errorLogger,
		metrics:          metrics,
		v2mode:           storageMode == "zstd",
		updateTimestamps: updateTimestamps,
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

	return c, nil
}

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
	MaxIdleConns int,
	ConnRecycleInterval time.Duration,

	storageMode string, accessLogger cache.Logger,
	errorLogger cache.Logger, numUploaders, maxQueuedUploads int,
	metrics Metrics, options ...Option) cache.Proxy {

	fmt.Println("Using S3 backend.")

	c, err := newBackend(BackendSpec{
		Endpoint:         Endpoint,
		Bucket:           Bucket,
		BucketLookupType: BucketLookupType,
		Prefix:           Prefix,
		Credentials:      Credentials,
		DisableSSL:       DisableSSL,
		Region:           Region,
		MaxIdleConns:     MaxIdleConns,
	}, UpdateTimestamps, ConnRecycleInterval, storageMode, accessLogger, errorLogger,
		numUploaders, maxQueuedUploads, metrics, options...)
	if err != nil {
		log.Fatalln(err)
	}

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

// bucketForContext resolves the bucket for a request: the validated
// request-scoped bucket when the trust interceptor attached one, otherwise
// this backend's configured default bucket (selector-less traffic, and
// upstreams that predate the bucket half of the wire contract).
func (c *s3Cache) bucketForContext(ctx context.Context) string {
	if selection, ok := cache.S3BackendFromContext(ctx); ok && selection.Bucket != "" {
		return selection.Bucket
	}
	return c.bucket
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

	// The bucket captured at enqueue time, like the storage prefix above:
	// uploads are asynchronous, so the routing decision must travel with the
	// item. An empty capture (selector-less traffic) means the default bucket.
	bucket := item.S3Backend.Bucket
	if bucket == "" {
		bucket = c.bucket
	}

	opts := minio.PutObjectOptions{
		UserMetadata: map[string]string{
			"Content-Type": "application/octet-stream",
		},
	}
	// Create-if-absent: a backend upload only counts toward the storage footprint
	// when it stores a net-new object. If the object already exists, MinIO rejects
	// the conditional PUT with a precondition failure and we classify it as
	// already_exists instead of created.
	opts.SetMatchETagExcept("*")

	// Uploads run on background workers with no request deadline; the
	// timeout reclaims a worker pinned by a hung MinIO connection.
	ctx, cancel := context.WithTimeout(context.Background(), uploadTimeout)
	defer cancel()

	_, err := c.mcore.PutObject(
		ctx,
		bucket,          // bucketName
		objectKey,       // objectName
		item.Rc,         // reader
		item.SizeOnDisk, // objectSize
		"",              // md5base64
		"",              // sha256
		opts,            // metadata
	)

	logResponse(c.accessLogger, "UPLOAD", bucket, objectKey, err)

	status, reason := classifyUploadOutcome(err)
	uploadOutcomes.WithLabelValues(c.key, status, reason).Inc()
	c.observeUpload(context.Background(), item, status, reason)
	uploadQueueDepth.WithLabelValues(c.key).Set(float64(len(c.uploadQueue)))

	_ = item.Rc.Close()
}

// classifyUploadOutcome maps a create-if-absent backend PutObject result to a
// terminal storage-accounting status. Only a net-new object (no error) is
// "created" and counts toward the footprint. A precondition failure means the
// object already existed: RFC-compliant servers return 412 PreconditionFailed,
// while some older MinIO releases return 304 NotModified, so both map to
// already_exists. Anything else is a genuine failure.
//
// The failure status is "error" (not "failed") to match the shared build-cache
// status taxonomy: FA emits "error" (buildCacheStatusError) and the web reader
// counts status IN ('error', 'rejected', 'dropped') as failures
// (BuildCacheMetricsService::FAILURE_STATUSES). A divergent "failed" would be
// silently dropped by those consumers.
func classifyUploadOutcome(err error) (status string, reason string) {
	if err == nil {
		return "created", ""
	}
	switch minio.ToErrorResponse(err).StatusCode {
	case http.StatusPreconditionFailed, http.StatusNotModified:
		return "already_exists", "precondition_failed"
	default:
		return "error", "s3_put_failed"
	}
}

func (c *s3Cache) Put(ctx context.Context, kind cache.EntryKind, hash string, logicalSize int64, sizeOnDisk int64, rc io.ReadCloser) {
	if c.uploadQueue == nil {
		_ = rc.Close()
		return
	}
	prefix, requestScopedPrefix, requirePrefix := c.prefixForContext(ctx, kind)
	labels, _ := cache.MetricsLabelsFromContext(ctx)
	// Capture the (endpoint, bucket) selection at enqueue time for the same
	// reason as the storage prefix: the request context is gone when upload
	// workers run, and the bucket is per request. (The endpoint half is
	// already settled — this backend's queue IS the endpoint routing
	// decision — but the pair travels together.)
	selection, _ := cache.S3BackendFromContext(ctx)

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
		S3Backend:                  selection,
		MetricsLabels:              labels,
	}:
		uploadQueueDepth.WithLabelValues(c.key).Set(float64(len(c.uploadQueue)))
	default:
		c.errorLogger.Printf("too many uploads queued for S3 backend %s\n", c.key)
		uploadQueueDropped.WithLabelValues(c.key).Inc()
		cache.ObserveOperation(ctx, c.observer, cache.OperationOutcome{
			Method: "backend_upload",
			Kind:   kind.String(),
			Status: "dropped",
			Reason: "upload_queue_full",
			Ops:    1,
			Bytes:  nonNegativeUint64(sizeOnDisk),
		})
		_ = rc.Close()
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
	bucket := c.bucketForContext(ctx)

	rctx, cancel := context.WithTimeout(ctx, readDeadline)

	rc, info, _, err := c.mcore.GetObject(
		rctx,
		bucket,                   // bucketName
		objectKey,                // objectName
		minio.GetObjectOptions{}, // opts
	)
	if err != nil {
		cancel()
		cacheMisses.WithLabelValues(c.key).Inc()
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			logResponse(c.accessLogger, "DOWNLOAD", bucket, objectKey, errNotFound)
			return nil, -1, nil
		}
		logResponse(c.accessLogger, "DOWNLOAD", bucket, objectKey, err)
		return nil, -1, err
	}
	cacheHits.WithLabelValues(c.key).Inc()

	// The deadline context governs the streamed body; releasing the timer
	// belongs to whoever closes the body.
	rc = &cancelReadCloser{ReadCloser: rc, cancel: cancel}

	if c.updateTimestamps {
		c.UpdateModificationTimestamp(ctx, bucket, objectKey)
	}

	logResponse(c.accessLogger, "DOWNLOAD", bucket, objectKey, nil)

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
	bucket := c.bucketForContext(ctx)

	sctx, cancel := context.WithTimeout(ctx, readDeadline)
	defer cancel()

	s, err := c.mcore.StatObject(
		sctx,
		bucket,                    // bucketName
		objectKey,                 // objectName
		minio.StatObjectOptions{}, // opts
	)

	exists = (err == nil)
	if err != nil {
		err = errNotFound
	} else if kind != cache.CAS || !c.v2mode {
		size = s.Size
	}

	if exists {
		// Surface the stored object size for LRU closure capture without
		// changing the value returned to the validator. StatObject already
		// computed s.Size, so this is free, and for CAS/v2 it is the only
		// place the on-disk size is available (the returned size stays -1).
		// A nil sink (the common case) makes this a no-op.
		if sink, ok := cache.LeafSizeSinkFromContext(ctx); ok && s.Size >= 0 {
			sink.RecordLeafSize(hash, s.Size, true)
		}
	}

	logResponse(c.accessLogger, "CONTAINS", bucket, objectKey, err)

	return exists, size
}

func (c *s3Cache) observeUpload(ctx context.Context, item backendproxy.UploadReq, status string, reason string) {
	// SizeOnDisk is the compressed/stored byte count, matching what MinIO actually
	// persists; the footprint accumulator and the MinIO drift scan share this unit.
	cache.ObserveOperation(cache.WithMetricsLabels(ctx, item.MetricsLabels), c.observer, cache.OperationOutcome{
		Method: "backend_upload",
		Kind:   item.Kind.String(),
		Status: status,
		Reason: reason,
		Ops:    1,
		Bytes:  nonNegativeUint64(item.SizeOnDisk),
	})
}

func nonNegativeUint64(value int64) uint64 {
	if value < 0 {
		return 0
	}
	return uint64(value)
}
