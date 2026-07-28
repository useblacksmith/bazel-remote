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
	// tenant-facing selector from the backends map (also the metrics label).
	// Single-backend deployments use the endpoint.
	key              string
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

	// Per-backend metric instances, bound to the "backend" label value.
	hits         prometheus.Counter
	misses       prometheus.Counter
	queueDropped prometheus.Counter
	queueDepth   prometheus.Gauge
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
	backendUnknown = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bazel_remote_s3_backend_unknown_total",
		Help: "Requests carrying a backend selector not present in the configured backends map (refused; indicates an interceptor/config mismatch).",
	}, []string{"operation"})
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

// errUnknownBackend is returned when a request carries a backend selector
// that is not in the configured backends map. The gRPC interceptor validates
// selectors fail-closed at the boundary, so hitting this means an
// interceptor/config mismatch bug; refusing the operation is safer than
// touching the wrong shard.
var errUnknownBackend = errors.New("unknown S3 backend selector")

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
//   - periodic CloseIdleConnections() (default every 5 minutes), which closes
//     whatever is momentarily idle so subsequent requests re-dial and
//     re-resolve, rotating across MinIO nodes within a few intervals. Dials
//     are cheap here (plaintext, one LAN RTT), so the recycle interval errs
//     toward responsiveness; the per-MinIO-node balance dashboard is the
//     acceptance gauge.
const (
	DefaultConnRecycleInterval = 5 * time.Minute

	defaultMaxIdleConns        = 64
	defaultMaxIdleConnsPerHost = 32
)

// BackendSpec describes one S3 backend of a multi-backend proxy.
type BackendSpec struct {
	// Key is the tenant-facing selector this backend is registered under
	// (the allowlisted endpoint URL forwarded as gRPC metadata). It is also
	// the "backend" metrics label.
	Key              string
	Endpoint         string
	Bucket           string
	BucketLookupType minio.BucketLookupType
	Prefix           string
	Credentials      *credentials.Credentials
	DisableSSL       bool
	Region           string
	MaxIdleConns     int
	Default          bool
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

	if connRecycleInterval == 0 {
		connRecycleInterval = DefaultConnRecycleInterval
	}
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
		hits:             cacheHits.WithLabelValues(key),
		misses:           cacheMisses.WithLabelValues(key),
		queueDropped:     uploadQueueDropped.WithLabelValues(key),
		queueDepth:       uploadQueueDepth.WithLabelValues(key),
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

// NewMulti returns a cache.Proxy that dispatches over a map of allowlisted S3
// backends. Each backend gets its own minio client, transport, and upload
// queue (numUploaders/maxQueuedUploads apply per backend). Requests carrying
// a backend selector on the context (lifted from validated gRPC metadata by
// the server interceptor) route to the matching backend; requests without a
// selector route to the designated default backend.
func NewMulti(specs []BackendSpec, updateTimestamps bool, connRecycleInterval time.Duration,
	storageMode string, accessLogger cache.Logger, errorLogger cache.Logger,
	numUploaders, maxQueuedUploads int, metrics Metrics, options ...Option) (cache.Proxy, error) {

	if len(specs) == 0 {
		return nil, errors.New("s3proxy.NewMulti requires at least one backend")
	}

	m := &multiS3Cache{
		backends:    make(map[string]*s3Cache, len(specs)),
		errorLogger: errorLogger,
	}
	for _, spec := range specs {
		if spec.Key == "" {
			return nil, errors.New("s3proxy.NewMulti backends require a non-empty Key")
		}
		if _, exists := m.backends[spec.Key]; exists {
			return nil, fmt.Errorf("duplicate S3 backend key %q", spec.Key)
		}
		backend, err := newBackend(spec, updateTimestamps, connRecycleInterval, storageMode,
			accessLogger, errorLogger, numUploaders, maxQueuedUploads, metrics, options...)
		if err != nil {
			return nil, err
		}
		m.backends[spec.Key] = backend
		if spec.Default {
			if m.def != nil {
				return nil, errors.New("multiple S3 backends marked as default")
			}
			m.def = backend
		}
	}
	if m.def == nil {
		return nil, errors.New("no S3 backend marked as default")
	}

	fmt.Printf("Using S3 backend map with %d backends (default: %s).\n", len(m.backends), m.def.key)

	return m, nil
}

// multiS3Cache routes cache.Proxy operations to one of several s3Cache
// backends based on the request-scoped backend selector.
type multiS3Cache struct {
	backends    map[string]*s3Cache
	def         *s3Cache
	errorLogger cache.Logger
}

// backendFor resolves the backend for a request. A missing selector routes
// to the default backend (single-backend compatibility, HTTP API paths, RAW
// entries); an unknown selector refuses the operation — the gRPC interceptor
// already rejects those fail-closed, so this is a belt-and-braces guard
// against interceptor/config drift, and must never fall back to a guessed
// backend.
func (m *multiS3Cache) backendFor(ctx context.Context, operation string) *s3Cache {
	selector, ok := cache.S3BackendFromContext(ctx)
	if !ok {
		return m.def
	}
	backend, ok := m.backends[selector]
	if !ok {
		backendUnknown.WithLabelValues(operation).Inc()
		if m.errorLogger != nil {
			m.errorLogger.Printf("S3 %s unknown backend selector %q; refusing operation", operation, selector)
		}
		return nil
	}
	return backend
}

func (m *multiS3Cache) Put(ctx context.Context, kind cache.EntryKind, hash string, logicalSize int64, sizeOnDisk int64, rc io.ReadCloser) {
	backend := m.backendFor(ctx, "UPLOAD")
	if backend == nil {
		_ = rc.Close()
		return
	}
	backend.Put(ctx, kind, hash, logicalSize, sizeOnDisk, rc)
}

func (m *multiS3Cache) Get(ctx context.Context, kind cache.EntryKind, hash string, size int64) (io.ReadCloser, int64, error) {
	backend := m.backendFor(ctx, "DOWNLOAD")
	if backend == nil {
		return nil, -1, errUnknownBackend
	}
	return backend.Get(ctx, kind, hash, size)
}

func (m *multiS3Cache) Contains(ctx context.Context, kind cache.EntryKind, hash string, size int64) (bool, int64) {
	backend := m.backendFor(ctx, "CONTAINS")
	if backend == nil {
		return false, -1
	}
	return backend.Contains(ctx, kind, hash, size)
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

// Metric helpers, nil-safe so tests can construct s3Cache directly without
// binding Prometheus instances.
func (c *s3Cache) incHits() {
	if c.hits != nil {
		c.hits.Inc()
	}
}

func (c *s3Cache) incMisses() {
	if c.misses != nil {
		c.misses.Inc()
	}
}

func (c *s3Cache) incQueueDropped() {
	if c.queueDropped != nil {
		c.queueDropped.Inc()
	}
}

func (c *s3Cache) setQueueDepth() {
	if c.queueDepth != nil {
		c.queueDepth.Set(float64(len(c.uploadQueue)))
	}
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

	_, err := c.mcore.PutObject(
		context.Background(),
		c.bucket,        // bucketName
		objectKey,       // objectName
		item.Rc,         // reader
		item.SizeOnDisk, // objectSize
		"",              // md5base64
		"",              // sha256
		opts,            // metadata
	)

	logResponse(c.accessLogger, "UPLOAD", c.bucket, objectKey, err)

	status, reason := classifyUploadOutcome(err)
	c.observeUpload(context.Background(), item, status, reason)
	c.setQueueDepth()

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
		c.setQueueDepth()
	default:
		c.errorLogger.Printf("too many uploads queued\n")
		c.incQueueDropped()
		cache.ObserveOperation(ctx, c.observer, cache.OperationOutcome{
			Method: "backend_upload",
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

	rc, info, _, err := c.mcore.GetObject(
		ctx,
		c.bucket,                 // bucketName
		objectKey,                // objectName
		minio.GetObjectOptions{}, // opts
	)
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			c.incMisses()
			logResponse(c.accessLogger, "DOWNLOAD", c.bucket, objectKey, errNotFound)
			return nil, -1, nil
		}
		c.incMisses()
		logResponse(c.accessLogger, "DOWNLOAD", c.bucket, objectKey, err)
		return nil, -1, err
	}
	c.incHits()

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

	logResponse(c.accessLogger, "CONTAINS", c.bucket, objectKey, err)

	return exists, size
}

func (c *s3Cache) observeUpload(ctx context.Context, item backendproxy.UploadReq, status string, reason string) {
	// SizeOnDisk is the compressed/stored byte count, matching what MinIO actually
	// persists; the footprint accumulator and the MinIO drift scan share this unit.
	cache.ObserveOperation(cache.WithMetricsLabels(ctx, item.MetricsLabels), c.observer, cache.OperationOutcome{
		Method: "backend_upload",
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
