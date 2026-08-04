package s3proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"path"
	"sync"
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
	bucket      string
	uploadQueue chan<- backendproxy.UploadReq
	// breaker guards every MinIO call this backend makes (read fall-through,
	// existence checks, write-through PUTs). Per backend by construction: in
	// multi-backend map mode a sick shard trips only its own breaker and
	// must not blind the healthy shards.
	breaker          *breaker
	accessLogger     cache.Logger
	errorLogger      cache.Logger
	metrics          Metrics
	v2mode           bool
	updateTimestamps bool
	// readDeadline is the overall bound on one read-path call including the
	// streamed body; defaults to the package-level readDeadline, overridable
	// via WithReadDeadline. Connection failure is bounded separately and
	// much tighter by connectTimeout on the transport.
	readDeadline time.Duration
	objectKey    func(prefix string, hash string, kind cache.EntryKind) string
	observer     cache.OperationObserver
	// inflight coalesces duplicate enqueues; owed records shed/failed
	// uploads for the background sweeper; blobSource reopens local blobs
	// for deferred uploads. See owed.go.
	inflight       *inflightSet
	owed           *owedLedger
	owedDir        string
	blobSource     BlobSource
	blobSourceOnce sync.Once
}

type Option func(*s3Cache)

func WithOperationObserver(observer cache.OperationObserver) Option {
	return func(c *s3Cache) {
		c.observer = observer
	}
}

// WithOwedLedgerDir enables the owed-upload ledger (see owed.go), persisting
// per-backend snapshots under dir. Without this option, shed uploads are
// dropped exactly as before — embedders that don't own durable storage (the
// host-side fallback proxy) keep the old best-effort semantics.
func WithOwedLedgerDir(dir string) Option {
	return func(c *s3Cache) {
		c.owedDir = dir
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

// connectTimeout bounds the connection-establishment legs of every MinIO
// call at the transport layer: TCP dial, TLS handshake, and time to response
// headers. This — not the overall read deadline — is what makes a dead or
// black-holed backend fail in seconds: a sick node must ANSWER fast, but a
// healthy node streaming a large blob may legitimately take much longer than
// this to finish. 10s is forgiving to a browning-out-but-alive backend while
// staying well inside a Bazel client's own 60s remote_timeout. Deliberately
// a constant: raising it toward that client timeout re-introduces the
// 60-220s miss stalls the catastrophe drills measured pre-deadline.
const connectTimeout = 10 * time.Second

// readDeadline is the default overall bound on a single read-path MinIO
// call (the miss fall-through GetObject and the Contains StatObject),
// INCLUDING the streamed GET body (the deadline context governs it and is
// canceled when the caller closes the body), so a transfer stalled
// mid-stream is reclaimed too. Generous by design, mirroring uploadTimeout's
// reasoning: multi-GB CAS blobs are minutes on a busy link, and guillotining
// a healthy fill means that blob can never be served through the L1 at all.
// Connection failure is connectTimeout's job; this only has to reclaim
// mid-stream stalls. Configurable per deployment via WithReadDeadline
// (s3_proxy.read_timeout in standalone config). A var so tests can shrink
// it.
var readDeadline = 5 * time.Minute

// WithReadDeadline overrides the overall read-path deadline for this
// backend. Non-positive values keep the default.
func WithReadDeadline(d time.Duration) Option {
	return func(c *s3Cache) {
		if d > 0 {
			c.readDeadline = d
		}
	}
}

// effectiveReadDeadline returns the configured per-backend deadline, falling
// back to the package default for s3Cache values built without newBackend
// (test fixtures) — a zero deadline would expire every read instantly.
func (c *s3Cache) effectiveReadDeadline() time.Duration {
	if c.readDeadline > 0 {
		return c.readDeadline
	}
	return readDeadline
}

// maxRetries caps minio-go's internal per-call retries (library default:
// MaxRetry = 10 attempts with exponential backoff, ~a minute of retention
// per failing call). Three attempts ride out a blip; anything worse must
// surface to the circuit breaker in seconds, which owns backoff across
// calls.
const maxRetries = 3

// isNotFound reports whether err is MinIO answering "no such object": a
// miss is a healthy answer and must count as breaker success, or a burst of
// legitimate misses would trip the breaker on a perfectly healthy backend.
func isNotFound(err error) bool {
	resp := minio.ToErrorResponse(err)
	return resp.Code == "NoSuchKey" || resp.StatusCode == http.StatusNotFound
}

// breakerReadOutcome classifies a read-path minio result for the breaker.
// Not-found is success (see isNotFound). A parent-context cancellation (the
// client went away mid-request) is not a backend-health signal and is
// counted neither way; only genuine backend errors — including our own
// readDeadline expiring — count as failures.
func breakerReadOutcome(parent context.Context, err error) breakerOutcome {
	if err == nil || isNotFound(err) {
		return outcomeSuccess
	}
	if parent.Err() != nil {
		return outcomeIgnore
	}
	return outcomeFailure
}

// breakerUploadOutcome classifies a write-path minio result for the
// breaker. A precondition failure means the object already exists — MinIO
// answered, so the backend is healthy (mirrors classifyUploadOutcome's
// already_exists mapping).
func breakerUploadOutcome(err error) breakerOutcome {
	if err == nil {
		return outcomeSuccess
	}
	switch minio.ToErrorResponse(err).StatusCode {
	case http.StatusPreconditionFailed, http.StatusNotModified:
		return outcomeSuccess
	}
	return outcomeFailure
}

// bodyOutcomeReadCloser releases the readDeadline timer when the caller
// finishes with a streamed GET body, and reports the body's TERMINAL state
// to the breaker — the streamed read's ONLY breaker outcome. Headers alone
// prove nothing in the dominant brownout mode (connections open fine, bytes
// don't flow): if time-to-first-byte recorded a success, every brownout
// read would reset the very failure streak its body failure then increments
// and the breaker would never trip, with the fleet paying a full deadline
// per read instead of flipping to fast-miss. Exactly one outcome is
// reported per body: clean EOF is a success, a mid-stream error with the
// caller still interested is a failure (including our own deadline
// expiring), and a caller-side abandonment (parent context dead, or Close
// before EOF with the deadline unexpired) says nothing about backend
// health. A late report is a breaker straggler, which record already
// handles. Callers must Close the body on every path: while half-open, the
// probe slot is held until this report lands.
type bodyOutcomeReadCloser struct {
	io.ReadCloser
	cancel  context.CancelFunc
	breaker *breaker
	// parent is the request context, rctx the deadline-bearing child that
	// governs the body.
	parent context.Context
	rctx   context.Context
	once   sync.Once
}

func (c *bodyOutcomeReadCloser) report(outcome breakerOutcome) {
	c.once.Do(func() { c.breaker.record(outcome) })
}

func (c *bodyOutcomeReadCloser) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	if err == io.EOF {
		c.report(outcomeSuccess)
	} else if err != nil {
		if c.parent.Err() != nil {
			c.report(outcomeIgnore)
		} else {
			c.report(outcomeFailure)
		}
	}
	return n, err
}

func (c *bodyOutcomeReadCloser) Close() error {
	err := c.ReadCloser.Close()
	// Close before any terminal Read: if our own deadline expired while
	// the caller was still interested, this body stalled out — a failure.
	// Any other early Close is the caller abandoning a healthy stream.
	if c.rctx.Err() != nil && c.parent.Err() == nil {
		c.report(outcomeFailure)
	} else {
		c.report(outcomeIgnore)
	}
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

	// Fail connection establishment fast, independent of the (generous)
	// overall read deadline: a black-holed backend must be answered in
	// seconds by the dialer / TLS / response-header legs, while an
	// established stream keeps the full readDeadline to move its body.
	tr.DialContext = (&net.Dialer{
		Timeout:   connectTimeout,
		KeepAlive: 30 * time.Second,
	}).DialContext
	tr.TLSHandshakeTimeout = connectTimeout
	tr.ResponseHeaderTimeout = connectTimeout

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
		key:    key,
		mcore:  minioCore,
		prefix: spec.Prefix,
		bucket: spec.Bucket,
		// Named by the backend key so the breaker series joins the other
		// backend-labeled metrics (upload outcomes, hits/misses, queue) on
		// dashboards. Bucket is deliberately absent: since the multi-backend
		// piece it is per-request state, not backend identity.
		breaker:          newBreaker(key, errorLogger),
		accessLogger:     accessLogger,
		errorLogger:      errorLogger,
		metrics:          metrics,
		v2mode:           storageMode == "zstd",
		updateTimestamps: updateTimestamps,
		readDeadline:     readDeadline,
	}
	for _, opt := range options {
		opt(c)
	}

	if c.v2mode {
		c.objectKey = objectKeyV2
	} else {
		c.objectKey = objectKeyV1
	}

	c.inflight = newInflightSet()
	if c.owedDir != "" {
		c.owed = newOwedLedger(c.owedDir, key, errorLogger)
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
	// timeout reclaims a worker pinned by a hung MinIO connection. No
	// tighter deadline here: large CAS blobs may legitimately take long,
	// and the breaker covers the sick-MinIO case.
	ctx, cancel := context.WithTimeout(context.Background(), uploadTimeout)
	defer cancel()

	// NoProbe: a PutObject bounded only by uploadTimeout must not become
	// the half-open recovery probe (see ExecuteNoProbe).
	var putErr error
	berr := c.breaker.ExecuteNoProbe(func() breakerOutcome {
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
		putErr = err
		return breakerUploadOutcome(err)
	})

	var status, reason string
	if berr != nil {
		// The breaker refused the call without dialing MinIO: fail the item
		// fast with its own terminal outcome so dashboards can tell a sick
		// backend (breaker_open) from failing PUTs (s3_put_failed).
		logResponse(c.accessLogger, "UPLOAD", bucket, objectKey, berr)
		status, reason = "error", "breaker_open"
	} else {
		logResponse(c.accessLogger, "UPLOAD", bucket, objectKey, putErr)
		status, reason = classifyUploadOutcome(putErr)
	}
	uploadOutcomes.WithLabelValues(c.key, status, reason).Inc()
	c.observeUpload(context.Background(), item, status, reason)

	// Settle or record the owed-ledger debt for this upload identity. Any
	// success (created, or already_exists — someone else stored it) clears
	// the debt; any terminal failure (failed PUT, breaker refusal) records
	// it so the sweeper retries once the backend is healthy again.
	identity := c.resolveUploadIdentity(item)
	if status == "error" {
		c.owed.add(c.owedEntryForItem(identity, item))
	} else {
		c.owed.settle(identity)
	}
	c.inflight.remove(identity)

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

	item := backendproxy.UploadReq{
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
	}

	// Coalesce duplicate uploads: matrix fan-out delivers the same missing
	// blob from many hosts at once, MinIO's create-if-absent would 412 all
	// but one anyway, and every queued duplicate pins an open FD. The
	// coalesced-away upload still records its debt: usually the in-flight
	// copy's success settles it moments later, but the in-flight claim can
	// also be the SWEEPER holding a blob it just found evicted — about to
	// settle the debt as void while this Put proves the blob is back. The
	// seq bump from this add makes that void-settle a no-op (settleVoid),
	// and the next sweep pass repays it for real.
	key := c.resolveUploadIdentity(item)
	if !c.inflight.tryAdd(key) {
		uploadQueueCoalesced.WithLabelValues(c.key).Inc()
		c.owed.add(c.owedEntryForItem(key, item))
		_ = rc.Close()
		return
	}

	select {
	case c.uploadQueue <- item:
		uploadQueueDepth.WithLabelValues(c.key).Set(float64(len(c.uploadQueue)))
	default:
		c.inflight.remove(key)
		c.errorLogger.Printf("too many uploads queued for S3 backend %s\n", c.key)
		uploadQueueDropped.WithLabelValues(c.key).Inc()
		// Shedding is a deferral, not a loss: FindMissingBlobs answers
		// local-first, so nothing would ever re-upload this blob. Record
		// the debt; the sweeper repays it when the queue has headroom.
		c.owed.add(c.owedEntryForItem(key, item))
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

	rctx, cancel := context.WithTimeout(ctx, c.effectiveReadDeadline())

	// The streamed GET defers its breaker outcome to the body's terminal
	// state, so it uses allow/record directly rather than Execute: healthy
	// response headers prove nothing in the dominant brownout mode
	// (connections open fine, bytes don't flow), and recording a success at
	// time-to-first-byte would reset the failure streak that the body
	// failures are trying to build. The half-open probe slot is likewise
	// held until the body terminates — a probe whose headers arrived but
	// whose body is still streaming has not yet proven the backend healthy.
	if !c.breaker.allow() {
		// Breaker open: report a clean miss without dialing MinIO. A miss is
		// the honest answer the disk cache layer can act on (serve from
		// elsewhere or rebuild); an error would surface to the client.
		cancel()
		cacheMisses.WithLabelValues(c.key).Inc()
		logResponse(c.accessLogger, "DOWNLOAD", bucket, objectKey, errBreakerOpen)
		return nil, -1, nil
	}
	rc, info, _, getErr := c.mcore.GetObject(
		rctx,
		bucket,                   // bucketName
		objectKey,                // objectName
		minio.GetObjectOptions{}, // opts
	)
	if getErr != nil {
		// Terminal at headers: not-found is a healthy answer (success),
		// everything else classifies as usual.
		c.breaker.record(breakerReadOutcome(ctx, getErr))
		cancel()
		cacheMisses.WithLabelValues(c.key).Inc()
		if minio.ToErrorResponse(getErr).Code == "NoSuchKey" {
			logResponse(c.accessLogger, "DOWNLOAD", bucket, objectKey, errNotFound)
			return nil, -1, nil
		}
		logResponse(c.accessLogger, "DOWNLOAD", bucket, objectKey, getErr)
		return nil, -1, getErr
	}
	cacheHits.WithLabelValues(c.key).Inc()

	// The deadline context governs the streamed body; releasing the timer
	// belongs to whoever closes the body, and the body's terminal state is
	// what actually proves this backend healthy (see bodyOutcomeReadCloser).
	rc = &bodyOutcomeReadCloser{
		ReadCloser: rc,
		cancel:     cancel,
		breaker:    c.breaker,
		parent:     ctx,
		rctx:       rctx,
	}

	if c.updateTimestamps {
		c.UpdateModificationTimestamp(ctx, bucket, objectKey)
	}

	logResponse(c.accessLogger, "DOWNLOAD", bucket, objectKey, nil)

	if kind == cache.CAS && c.v2mode {
		lrc, logicalSize, err := casblob.ExtractLogicalSize(rc)
		if err != nil {
			// A malformed casblob header says the stored OBJECT is bad, not
			// the backend: Close both releases the deadline timer and files
			// the body's breaker outcome (an un-expired early close counts
			// neither way), so a half-open probe slot is never wedged by a
			// corrupt blob.
			_ = rc.Close()
			return nil, -1, err
		}
		return lrc, logicalSize, nil
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

	sctx, cancel := context.WithTimeout(ctx, c.effectiveReadDeadline())
	defer cancel()

	var s minio.ObjectInfo
	var err error
	berr := c.breaker.Execute(func() breakerOutcome {
		s, err = c.mcore.StatObject(
			sctx,
			bucket,                    // bucketName
			objectKey,                 // objectName
			minio.StatObjectOptions{}, // opts
		)
		return breakerReadOutcome(ctx, err)
	})
	if berr != nil {
		// Breaker open: report "not present" without dialing MinIO — the
		// existing miss convention of this method.
		logResponse(c.accessLogger, "CONTAINS", bucket, objectKey, berr)
		return false, -1
	}

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
