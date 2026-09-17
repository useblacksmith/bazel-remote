package s3proxy

import (
	"bytes"
	"context"
	"io"
	stdlog "log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/utils/backendproxy"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type recordingObserver struct {
	outcomes []cache.OperationOutcome
}

func (r *recordingObserver) RecordOutcome(_ context.Context, outcome cache.OperationOutcome) {
	r.outcomes = append(r.outcomes, outcome)
}

func TestObjectKey(t *testing.T) {
	testCases := []struct {
		prefix     string
		key        string
		kind       cache.EntryKind
		expectedV1 string
		expectedV2 string
	}{
		{"", "1234", cache.CAS, "cas/12/1234", "cas.v2/12/1234"},
		{"test", "1234", cache.CAS, "test/cas/12/1234", "test/cas.v2/12/1234"},
		{"foo/bar/grok", "1234", cache.CAS, "foo/bar/grok/cas/12/1234", "foo/bar/grok/cas.v2/12/1234"},
		{"", "1234", cache.AC, "ac/12/1234", "ac/12/1234"},
		{"", "1234", cache.RAW, "raw/12/1234", "raw/12/1234"},
		{"foo/bar", "1234", cache.AC, "foo/bar/ac/12/1234", "foo/bar/ac/12/1234"},
	}

	for _, tc := range testCases {
		result := objectKeyV2(tc.prefix, tc.key, tc.kind)
		if result != tc.expectedV2 {
			t.Errorf("objectKeyV2 did not match. (result: '%s' expected: '%s'",
				result, tc.expectedV2)
		}

		result = objectKeyV1(tc.prefix, tc.key, tc.kind)
		if result != tc.expectedV1 {
			t.Errorf("objectKeyV1 did not match. (result: '%s' expected: '%s'",
				result, tc.expectedV1)
		}
	}
}

func TestObjectKeyForContextDefaultsToConfiguredPrefix(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	configuredPrefix := "minio-prefix/buck2/production/us-east-1"
	c := &s3Cache{
		prefix:    configuredPrefix,
		objectKey: objectKeyV2,
	}

	result := c.objectKeyForContext(context.Background(), hash, cache.CAS)
	expected := configuredPrefix + "/cas.v2/ab/" + hash
	if result != expected {
		t.Errorf("objectKeyForContext did not use configured prefix. (result: '%s' expected: '%s')",
			result, expected)
	}
}

func TestObjectKeyForContextUsesRequestScopedPrefixForACAndCAS(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	repoAPrefix := "minio-prefix/bazel/production/us-east-1/42/987654/v0"
	repoBPrefix := "minio-prefix/bazel/production/us-east-1/42/111111/v0"
	configuredPrefix := "minio-prefix/buck2/production/us-east-1"
	c := &s3Cache{
		prefix:    configuredPrefix,
		objectKey: objectKeyV2,
	}

	repoAContext := cache.WithStoragePrefix(context.Background(), repoAPrefix)
	repoBContext := cache.WithStoragePrefix(context.Background(), repoBPrefix)

	testCases := []struct {
		name     string
		ctx      context.Context
		kind     cache.EntryKind
		expected string
	}{
		{
			name:     "repo a cas",
			ctx:      repoAContext,
			kind:     cache.CAS,
			expected: repoAPrefix + "/cas.v2/ab/" + hash,
		},
		{
			name:     "repo b cas",
			ctx:      repoBContext,
			kind:     cache.CAS,
			expected: repoBPrefix + "/cas.v2/ab/" + hash,
		},
		{
			name:     "repo a action cache",
			ctx:      repoAContext,
			kind:     cache.AC,
			expected: repoAPrefix + "/ac/ab/" + hash,
		},
		{
			name:     "repo b action cache",
			ctx:      repoBContext,
			kind:     cache.AC,
			expected: repoBPrefix + "/ac/ab/" + hash,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := c.objectKeyForContext(tc.ctx, hash, tc.kind)
			if result != tc.expected {
				t.Errorf("objectKeyForContext did not use request-scoped prefix. (result: '%s' expected: '%s')",
					result, tc.expected)
			}
		})
	}

	repoACASKey := c.objectKeyForContext(repoAContext, hash, cache.CAS)
	repoBCASKey := c.objectKeyForContext(repoBContext, hash, cache.CAS)
	if repoACASKey == repoBCASKey {
		t.Fatalf("same CAS digest produced identical object keys for different request-scoped prefixes: %s", repoACASKey)
	}

	repoAACKey := c.objectKeyForContext(repoAContext, hash, cache.AC)
	repoBACKey := c.objectKeyForContext(repoBContext, hash, cache.AC)
	if repoAACKey == repoBACKey {
		t.Fatalf("same AC digest produced identical object keys for different request-scoped prefixes: %s", repoAACKey)
	}
}

func TestPutCapturesRequestScopedPrefixForAsyncUpload(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	requestPrefix := "minio-prefix/bazel/production/us-east-1/42/987654/v0"
	uploadQueue := make(chan backendproxy.UploadReq, 1)
	c := &s3Cache{
		prefix:      "minio-prefix/buck2/production/us-east-1",
		uploadQueue: uploadQueue,
	}

	rc := io.NopCloser(strings.NewReader("blob"))
	c.Put(cache.WithStoragePrefix(context.Background(), requestPrefix), cache.CAS, hash, 4, 4, rc)

	item := <-uploadQueue
	defer item.Rc.Close()
	if item.StoragePrefix != requestPrefix {
		t.Fatalf("queued upload StoragePrefix = %q, want %q", item.StoragePrefix, requestPrefix)
	}
	if !item.RequestScopedStoragePrefix {
		t.Fatal("queued upload RequestScopedStoragePrefix = false, want true")
	}
	if item.RequireStoragePrefix {
		t.Fatal("queued upload RequireStoragePrefix = true, want false")
	}
}

func TestPutCapturesRequestScopedPrefixForActionCacheAsyncUpload(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	requestPrefix := "minio-prefix/bazel/production/us-east-1/42/987654/v0"
	uploadQueue := make(chan backendproxy.UploadReq, 1)
	c := &s3Cache{
		prefix:      "minio-prefix/buck2/production/us-east-1",
		uploadQueue: uploadQueue,
	}

	ctx := cache.WithRequiredStoragePrefix(cache.WithStoragePrefix(context.Background(), requestPrefix))
	rc := io.NopCloser(strings.NewReader("blob"))
	c.Put(ctx, cache.AC, hash, 4, 4, rc)

	item := <-uploadQueue
	defer item.Rc.Close()
	if item.StoragePrefix != requestPrefix {
		t.Fatalf("queued upload StoragePrefix = %q, want %q", item.StoragePrefix, requestPrefix)
	}
	if !item.RequestScopedStoragePrefix {
		t.Fatal("queued upload RequestScopedStoragePrefix = false, want true")
	}
	if !item.RequireStoragePrefix {
		t.Fatal("queued upload RequireStoragePrefix = false, want true")
	}
}

func TestPutCapturesMissingRequiredRequestScopedPrefixForAsyncUpload(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	configuredPrefix := "minio-prefix/buck2/production/us-east-1"
	uploadQueue := make(chan backendproxy.UploadReq, 1)
	c := &s3Cache{
		prefix:      configuredPrefix,
		uploadQueue: uploadQueue,
	}

	rc := io.NopCloser(strings.NewReader("blob"))
	c.Put(cache.WithRequiredStoragePrefix(context.Background()), cache.CAS, hash, 4, 4, rc)

	item := <-uploadQueue
	defer item.Rc.Close()
	if item.StoragePrefix != configuredPrefix {
		t.Fatalf("queued upload StoragePrefix = %q, want %q", item.StoragePrefix, configuredPrefix)
	}
	if item.RequestScopedStoragePrefix {
		t.Fatal("queued upload RequestScopedStoragePrefix = true, want false")
	}
	if !item.RequireStoragePrefix {
		t.Fatal("queued upload RequireStoragePrefix = false, want true")
	}
}

func TestPutRecordsUploadQueueDrop(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	uploadQueue := make(chan backendproxy.UploadReq, 1)
	uploadQueue <- backendproxy.UploadReq{Hash: "queued", Rc: io.NopCloser(strings.NewReader("queued"))}
	observer := &recordingObserver{}
	var errBuf bytes.Buffer
	c := &s3Cache{
		key:         backendKeyA,
		prefix:      "minio-prefix/staging/10/717982840/v0/bazel",
		uploadQueue: uploadQueue,
		errorLogger: stdlog.New(&errBuf, "", 0),
		observer:    observer,
	}

	ctx := cache.WithMetricsLabels(context.Background(), cache.MetricsLabels{
		InstallationID: "10",
		RepositoryID:   "717982840",
		Generation:     "v0",
		BuildToolID:    "bazel",
		VMID:           "vm-123",
		JobID:          "job-456",
	})
	// Put is called with logicalSize=4, sizeOnDisk=4; the dropped outcome now
	// reports SizeOnDisk bytes.
	c.Put(ctx, cache.CAS, hash, 4, 4, io.NopCloser(strings.NewReader("blob")))

	if len(observer.outcomes) != 1 {
		t.Fatalf("observer outcomes len = %d, want 1", len(observer.outcomes))
	}
	outcome := observer.outcomes[0]
	if outcome.Method != "backend_upload" || outcome.Status != "dropped" || outcome.Reason != "upload_queue_full" {
		t.Fatalf("unexpected outcome: %+v", outcome)
	}
	if outcome.Bytes != 4 {
		t.Fatalf("dropped outcome bytes = %d, want 4 (SizeOnDisk)", outcome.Bytes)
	}
	if outcome.Labels.RepositoryID != "717982840" || outcome.Labels.JobID != "job-456" {
		t.Fatalf("unexpected labels: %+v", outcome.Labels)
	}
	// The overflow log names the backend so a multi-backend L1's logs
	// identify which shard's queue is saturated.
	if !strings.Contains(errBuf.String(), backendKeyA) {
		t.Fatalf("queue-full log %q does not name the backend key", errBuf.String())
	}
}

func TestObserveUploadReportsSizeOnDisk(t *testing.T) {
	observer := &recordingObserver{}
	c := &s3Cache{observer: observer}
	c.observeUpload(context.Background(), backendproxy.UploadReq{
		// LogicalSize must be ignored; only SizeOnDisk (stored bytes) is reported.
		LogicalSize: 99,
		SizeOnDisk:  12,
		Kind:        cache.CAS,
		MetricsLabels: cache.MetricsLabels{
			RepositoryID: "717982840",
			JobID:        "job-456",
		},
	}, "error", "s3_put_failed")

	if len(observer.outcomes) != 1 {
		t.Fatalf("observer outcomes len = %d, want 1", len(observer.outcomes))
	}
	outcome := observer.outcomes[0]
	if outcome.Method != "backend_upload" || outcome.Status != "error" || outcome.Reason != "s3_put_failed" {
		t.Fatalf("unexpected outcome: %+v", outcome)
	}
	if outcome.Bytes != 12 {
		t.Fatalf("outcome bytes = %d, want 12 (SizeOnDisk, not LogicalSize)", outcome.Bytes)
	}
	if outcome.Labels.RepositoryID != "717982840" || outcome.Labels.JobID != "job-456" {
		t.Fatalf("unexpected labels: %+v", outcome.Labels)
	}
}

func TestClassifyUploadOutcome(t *testing.T) {
	testCases := []struct {
		name           string
		err            error
		expectedStatus string
		expectedReason string
	}{
		{"net-new object", nil, "created", ""},
		{"precondition failed 412", minio.ErrorResponse{StatusCode: http.StatusPreconditionFailed}, "already_exists", "precondition_failed"},
		{"not modified 304 (older minio)", minio.ErrorResponse{StatusCode: http.StatusNotModified}, "already_exists", "precondition_failed"},
		{"server error", minio.ErrorResponse{StatusCode: http.StatusInternalServerError}, "error", "s3_put_failed"},
		{"non-minio error", errNotFound, "error", "s3_put_failed"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			status, reason := classifyUploadOutcome(tc.err)
			if status != tc.expectedStatus || reason != tc.expectedReason {
				t.Fatalf("classifyUploadOutcome(%v) = (%q, %q), want (%q, %q)",
					tc.err, status, reason, tc.expectedStatus, tc.expectedReason)
			}
		})
	}
}

const (
	backendKeyA = "http://minio-a.example.com:9000"
	backendKeyB = "https://minio-b.example.com:9000"
)

// twoBackendMulti builds a multiS3Cache over two hand-constructed backends
// with observable upload queues, avoiding any real minio client.
func twoBackendMulti(t *testing.T) (*multiS3Cache, chan backendproxy.UploadReq, chan backendproxy.UploadReq) {
	t.Helper()
	queueA := make(chan backendproxy.UploadReq, 1)
	queueB := make(chan backendproxy.UploadReq, 1)
	backendA := &s3Cache{key: backendKeyA, prefix: "prefix-a", uploadQueue: queueA}
	backendB := &s3Cache{key: backendKeyB, prefix: "prefix-b", uploadQueue: queueB}
	m := &multiS3Cache{
		backends: map[string]*s3Cache{
			backendKeyA: backendA,
			backendKeyB: backendB,
		},
		def: backendA,
	}
	return m, queueA, queueB
}

func TestMultiBackendPutRoutesToSelectedBackend(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	m, queueA, queueB := twoBackendMulti(t)

	// Selector B routes the async upload to backend B's queue.
	ctxB := cache.WithS3Backend(context.Background(), cache.S3BackendSelection{Endpoint: backendKeyB})
	m.Put(ctxB, cache.CAS, hash, 4, 4, io.NopCloser(strings.NewReader("blob")))
	select {
	case item := <-queueB:
		_ = item.Rc.Close()
	default:
		t.Fatal("expected upload in backend B's queue")
	}
	select {
	case <-queueA:
		t.Fatal("upload leaked into backend A's queue")
	default:
	}

	// Selector A routes to backend A's queue.
	ctxA := cache.WithS3Backend(context.Background(), cache.S3BackendSelection{Endpoint: backendKeyA})
	m.Put(ctxA, cache.AC, hash, 4, 4, io.NopCloser(strings.NewReader("blob")))
	select {
	case item := <-queueA:
		_ = item.Rc.Close()
	default:
		t.Fatal("expected upload in backend A's queue")
	}
}

func TestMultiBackendMissingSelectorRoutesToDefault(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	m, queueA, queueB := twoBackendMulti(t)

	// The fallback is metered: it is the HTTP-side-door / lost-selector
	// signal on a multi-backend node.
	before := testutil.ToFloat64(defaultBackendFallback.WithLabelValues("UPLOAD"))
	m.Put(context.Background(), cache.CAS, hash, 4, 4, io.NopCloser(strings.NewReader("blob")))
	select {
	case item := <-queueA:
		_ = item.Rc.Close()
	default:
		t.Fatal("expected upload in the default backend's queue")
	}
	select {
	case <-queueB:
		t.Fatal("upload leaked into the non-default backend's queue")
	default:
	}
	if got := testutil.ToFloat64(defaultBackendFallback.WithLabelValues("UPLOAD")) - before; got != 1 {
		t.Fatalf("defaultBackendFallback{UPLOAD} delta = %v, want 1", got)
	}

	// A selector-carrying request does not touch the fallback meter.
	before = testutil.ToFloat64(defaultBackendFallback.WithLabelValues("UPLOAD"))
	m.Put(cache.WithS3Backend(context.Background(), cache.S3BackendSelection{Endpoint: backendKeyB}), cache.CAS, hash, 4, 4, io.NopCloser(strings.NewReader("blob")))
	if item := <-queueB; item.Rc != nil {
		_ = item.Rc.Close()
	}
	if got := testutil.ToFloat64(defaultBackendFallback.WithLabelValues("UPLOAD")) - before; got != 0 {
		t.Fatalf("defaultBackendFallback{UPLOAD} delta = %v, want 0", got)
	}
}

// TestUploadFileDeadlineReclaimsWorkerFromHungBackend pins the upload
// deadline seam: a PutObject against a backend that never responds must
// return once uploadTimeout elapses (reporting an error outcome) instead of
// pinning the upload worker forever.
func TestUploadFileDeadlineReclaimsWorkerFromHungBackend(t *testing.T) {
	hung := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-hung // Hold every request open until the test finishes.
	}))
	defer ts.Close()
	defer close(hung)

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	core, err := minio.NewCore(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4("KEY", "SECRET", ""),
		Secure:       false,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		t.Fatal(err)
	}

	oldTimeout := uploadTimeout
	uploadTimeout = 100 * time.Millisecond
	defer func() { uploadTimeout = oldTimeout }()

	observer := &recordingObserver{}
	c := &s3Cache{
		key:          backendKeyA,
		mcore:        core,
		bucket:       "test-bucket",
		breaker:      newBreaker("test-upload-deadline", nil),
		objectKey:    objectKeyV2,
		accessLogger: stdlog.New(&bytes.Buffer{}, "", 0),
		observer:     observer,
	}

	done := make(chan struct{})
	go func() {
		c.UploadFile(backendproxy.UploadReq{
			Hash:       "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
			Kind:       cache.CAS,
			SizeOnDisk: 4,
			Rc:         io.NopCloser(strings.NewReader("blob")),
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("UploadFile did not return: the upload deadline is not applied")
	}
	if len(observer.outcomes) != 1 || observer.outcomes[0].Status != "error" {
		t.Fatalf("expected one error outcome, got %+v", observer.outcomes)
	}
}

// closeRecorder observes that the router closed the payload reader when it
// refused the operation.
type closeRecorder struct {
	io.Reader
	closed bool
}

func (c *closeRecorder) Close() error {
	c.closed = true
	return nil
}

func TestMultiBackendUnknownSelectorRefusesOperations(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	var errBuf bytes.Buffer
	m, queueA, queueB := twoBackendMulti(t)
	m.errorLogger = stdlog.New(&errBuf, "", 0)

	ctx := cache.WithS3Backend(context.Background(), cache.S3BackendSelection{Endpoint: "http://rogue.example.com:9000"})

	rc := &closeRecorder{Reader: strings.NewReader("blob")}
	m.Put(ctx, cache.CAS, hash, 4, 4, rc)
	if !rc.closed {
		t.Fatal("expected refused Put to close the reader")
	}
	select {
	case <-queueA:
		t.Fatal("refused Put reached backend A")
	case <-queueB:
		t.Fatal("refused Put reached backend B")
	default:
	}

	if _, _, err := m.Get(ctx, cache.CAS, hash, 4); err != errUnknownBackend {
		t.Fatalf("Get err = %v, want errUnknownBackend", err)
	}

	exists, size := m.Contains(ctx, cache.CAS, hash, 4)
	if exists || size != -1 {
		t.Fatalf("Contains = (%v, %d), want (false, -1)", exists, size)
	}

	if !strings.Contains(errBuf.String(), "unknown backend selector") {
		t.Fatalf("expected refusal to be logged, got %q", errBuf.String())
	}
}

// NewMulti validates only what config validation cannot express: exactly
// one default backend. Structural spec checks (non-empty keys, endpoints,
// duplicate map keys) are config validation's job.
func TestNewMultiValidation(t *testing.T) {
	creds := credentials.NewStaticV4("ak", "sk", "")
	spec := func(key string, def bool) BackendSpec {
		return BackendSpec{
			Key:         key,
			Endpoint:    "minio.example.com:9000",
			Bucket:      "bucket",
			Credentials: creds,
			DisableSSL:  true,
			Default:     def,
		}
	}

	if _, err := NewMulti(nil, false, -1, "uncompressed", nil, nil, 0, 0, nil); err == nil ||
		!strings.Contains(err.Error(), "no S3 backend marked as default") {
		t.Fatal("expected no-default error for empty backend list")
	}

	if _, err := NewMulti([]BackendSpec{spec(backendKeyA, false)},
		false, -1, "uncompressed", nil, nil, 0, 0, nil); err == nil ||
		!strings.Contains(err.Error(), "no S3 backend marked as default") {
		t.Fatal("expected error when no backend is default")
	}

	if _, err := NewMulti([]BackendSpec{spec(backendKeyA, true), spec(backendKeyB, true)},
		false, -1, "uncompressed", nil, nil, 0, 0, nil); err == nil ||
		!strings.Contains(err.Error(), "multiple S3 backends marked as default") {
		t.Fatal("expected error for multiple default backends")
	}

	proxy, err := NewMulti([]BackendSpec{spec(backendKeyA, true), spec(backendKeyB, false)},
		false, -1, "uncompressed", nil, nil, 0, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m, ok := proxy.(*multiS3Cache)
	if !ok {
		t.Fatalf("unexpected proxy type %T", proxy)
	}
	if m.def == nil || m.def.key != backendKeyA {
		t.Fatalf("unexpected default backend %+v", m.def)
	}
	if len(m.backends) != 2 {
		t.Fatalf("expected 2 backends, got %d", len(m.backends))
	}
}

func TestBucketForContext(t *testing.T) {
	c := &s3Cache{bucket: "default-bucket"}

	// Selector-less traffic (HTTP side door, single-backend deployments)
	// targets the configured default bucket.
	if got := c.bucketForContext(context.Background()); got != "default-bucket" {
		t.Fatalf("bucketForContext(bare) = %q, want default-bucket", got)
	}

	// A validated selection's bucket wins: the bucket is per request even
	// though the minio client is per endpoint.
	ctx := cache.WithS3Backend(context.Background(),
		cache.S3BackendSelection{Endpoint: backendKeyA, Bucket: "tenant-bucket"})
	if got := c.bucketForContext(ctx); got != "tenant-bucket" {
		t.Fatalf("bucketForContext(selection) = %q, want tenant-bucket", got)
	}

	// An endpoint-only selection (upstream predating the bucket contract)
	// falls back to the default bucket.
	ctx = cache.WithS3Backend(context.Background(), cache.S3BackendSelection{Endpoint: backendKeyA})
	if got := c.bucketForContext(ctx); got != "default-bucket" {
		t.Fatalf("bucketForContext(endpoint-only) = %q, want default-bucket", got)
	}
}

// fakeS3Backend builds an s3Cache over an in-memory S3 server (the same
// gofakes3 used by the system test's fakes3 binary) hosting the given
// buckets behind ONE endpoint — the seam for proving that the bucket is a
// per-request routing input while the client and endpoint stay shared.
func fakeS3Backend(t *testing.T, buckets ...string) *s3Cache {
	t.Helper()
	backend := s3mem.New()
	for _, bucket := range buckets {
		if err := backend.CreateBucket(bucket); err != nil {
			t.Fatal(err)
		}
	}
	ts := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(ts.Close)

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	core, err := minio.NewCore(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4("KEY", "SECRET", ""),
		Secure:       false,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		t.Fatal(err)
	}

	return &s3Cache{
		key:          backendKeyA,
		mcore:        core,
		bucket:       "default-bucket",
		breaker:      newBreaker("test-fake-s3", nil),
		objectKey:    objectKeyV1,
		accessLogger: stdlog.New(&bytes.Buffer{}, "", 0),
	}
}

func seedS3Object(t *testing.T, c *s3Cache, prefix string, kind cache.EntryKind, hash, body string) {
	t.Helper()
	key := c.objectKeyForPrefix(prefix, hash, kind)
	_, err := c.mcore.PutObject(context.Background(), c.bucket, key,
		strings.NewReader(body), int64(len(body)), "", "", minio.PutObjectOptions{})
	if err != nil {
		t.Fatalf("seed PutObject %s: %v", key, err)
	}
}

// TestPerRequestBucketRouting pins the v2 routing semantics: one endpoint,
// one minio client — but the bucket comes from the request. Uploads captured
// with different buckets on the same backend must land in their own buckets,
// reads must only see their own bucket's objects, and selector-less traffic
// must use the default bucket.
func TestPerRequestBucketRouting(t *testing.T) {
	hashTenant := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	hashDefault := "1234560123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	c := fakeS3Backend(t, "default-bucket", "tenant-bucket-1", "tenant-bucket-2")

	ctxBucket1 := cache.WithS3Backend(context.Background(),
		cache.S3BackendSelection{Endpoint: backendKeyA, Bucket: "tenant-bucket-1"})
	ctxBucket2 := cache.WithS3Backend(context.Background(),
		cache.S3BackendSelection{Endpoint: backendKeyA, Bucket: "tenant-bucket-2"})

	// An upload captured with bucket 1 lands in bucket 1 and only there:
	// visible via a bucket-1 selection, a miss via bucket 2 and the default.
	c.UploadFile(backendproxy.UploadReq{
		Hash:       hashTenant,
		Kind:       cache.CAS,
		SizeOnDisk: 4,
		Rc:         io.NopCloser(strings.NewReader("blob")),
		S3Backend:  cache.S3BackendSelection{Endpoint: backendKeyA, Bucket: "tenant-bucket-1"},
	})
	rc, _, err := c.Get(ctxBucket1, cache.CAS, hashTenant, -1)
	if err != nil || rc == nil {
		t.Fatalf("Get from bucket 1 = (%v, %v), want hit", rc, err)
	}
	data, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || string(data) != "blob" {
		t.Fatalf("Get from bucket 1 read (%q, %v), want \"blob\"", data, err)
	}
	if rc, _, err := c.Get(ctxBucket2, cache.CAS, hashTenant, -1); err != nil || rc != nil {
		t.Fatalf("Get from bucket 2 = (%v, %v), want plain miss: same endpoint, different bucket", rc, err)
	}
	if rc, _, err := c.Get(context.Background(), cache.CAS, hashTenant, -1); err != nil || rc != nil {
		t.Fatalf("selector-less Get = (%v, %v), want plain miss (default bucket)", rc, err)
	}

	// Contains follows the same per-request resolution.
	if exists, _ := c.Contains(ctxBucket1, cache.CAS, hashTenant, -1); !exists {
		t.Fatal("Contains via bucket 1 = false, want true")
	}
	if exists, _ := c.Contains(ctxBucket2, cache.CAS, hashTenant, -1); exists {
		t.Fatal("Contains via bucket 2 = true, want false: same endpoint, different bucket")
	}

	// A selector-less capture (no request-scoped bucket) lands in the
	// default bucket, invisible to the tenant buckets.
	c.UploadFile(backendproxy.UploadReq{
		Hash:       hashDefault,
		Kind:       cache.CAS,
		SizeOnDisk: 4,
		Rc:         io.NopCloser(strings.NewReader("dflt")),
	})
	if exists, _ := c.Contains(context.Background(), cache.CAS, hashDefault, -1); !exists {
		t.Fatal("selector-less Contains for default-bucket object = false, want true")
	}
	if exists, _ := c.Contains(ctxBucket1, cache.CAS, hashDefault, -1); exists {
		t.Fatal("default-bucket object visible via tenant bucket 1")
	}
}

func TestPutCapturesS3BackendSelectionForAsyncUpload(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	uploadQueue := make(chan backendproxy.UploadReq, 1)
	c := &s3Cache{
		bucket:      "default-bucket",
		uploadQueue: uploadQueue,
	}

	selection := cache.S3BackendSelection{Endpoint: backendKeyA, Bucket: "tenant-bucket-1"}
	c.Put(cache.WithS3Backend(context.Background(), selection), cache.CAS, hash, 4, 4,
		io.NopCloser(strings.NewReader("blob")))

	item := <-uploadQueue
	defer func() { _ = item.Rc.Close() }()
	if item.S3Backend != selection {
		t.Fatalf("queued upload S3Backend = %+v, want %+v", item.S3Backend, selection)
	}
}

func TestLogMissingRequiredStoragePrefix(t *testing.T) {
	var buf bytes.Buffer
	c := &s3Cache{
		prefix:      "minio-prefix/buck2/production/us-east-1",
		errorLogger: stdlog.New(&buf, "", 0),
	}

	c.logMissingRequiredStoragePrefix("UPLOAD", cache.CAS, "hash")

	result := buf.String()
	for _, expected := range []string{
		"S3 UPLOAD missing request-scoped storage prefix",
		"cas hash",
		`using configured prefix "minio-prefix/buck2/production/us-east-1"`,
	} {
		if !strings.Contains(result, expected) {
			t.Fatalf("log line %q does not contain %q", result, expected)
		}
	}
}

func TestSkipGoActionCacheBackendLookup(t *testing.T) {
	goPrefix := cache.WithStoragePrefix(context.Background(), "prd/10/123/v0/go")
	goPrefixSlash := cache.WithStoragePrefix(context.Background(), "staging/42/987654321/go/")
	bazelPrefix := cache.WithStoragePrefix(context.Background(), "prd/10/123/v0/bazel")
	buck2Prefix := cache.WithStoragePrefix(context.Background(), "prd/10/123/v0/buck2")
	goTool := cache.WithMetricsLabels(context.Background(), cache.MetricsLabels{BuildToolID: "go"})

	cases := []struct {
		name string
		ctx  context.Context
		kind cache.EntryKind
		skip bool
	}{
		{name: "go prefix AC", ctx: goPrefix, kind: cache.AC, skip: true},
		{name: "go prefix trailing slash AC", ctx: goPrefixSlash, kind: cache.AC, skip: true},
		{name: "go BuildToolID AC without prefix", ctx: goTool, kind: cache.AC, skip: true},
		{name: "go prefix CAS", ctx: goPrefix, kind: cache.CAS, skip: false},
		{name: "go BuildToolID CAS", ctx: goTool, kind: cache.CAS, skip: false},
		{name: "bazel prefix AC", ctx: bazelPrefix, kind: cache.AC, skip: false},
		{name: "buck2 prefix AC", ctx: buck2Prefix, kind: cache.AC, skip: false},
		{name: "unscoped AC", ctx: context.Background(), kind: cache.AC, skip: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := skipGoActionCacheBackendLookup(tc.ctx, tc.kind); got != tc.skip {
				t.Fatalf("skipGoActionCacheBackendLookup = %v, want %v", got, tc.skip)
			}
		})
	}
}

func TestGoActionCacheSkipsExistingMinioObject(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	c := fakeS3Backend(t, "default-bucket")
	c.key = "go-ac-skip-existing"

	goPrefix := "prd/10/123/v0/go"
	bazelPrefix := "prd/10/123/v0/bazel"
	ctxGo := cache.WithStoragePrefix(context.Background(), goPrefix)
	ctxBazel := cache.WithStoragePrefix(context.Background(), bazelPrefix)
	ctxGoTool := cache.WithMetricsLabels(context.Background(), cache.MetricsLabels{BuildToolID: "go"})

	seedS3Object(t, c, goPrefix, cache.AC, hash, "goac")
	seedS3Object(t, c, goPrefix, cache.CAS, hash, "gocas")
	seedS3Object(t, c, bazelPrefix, cache.AC, hash, "bzlac")
	seedS3Object(t, c, "", cache.AC, hash, "unsc")

	missesBefore := testutil.ToFloat64(cacheMisses.WithLabelValues(c.key))
	skipsBefore := testutil.ToFloat64(backendLookupsSkipped.WithLabelValues(c.key, skipReasonGoAC))

	rc, size, err := c.Get(ctxGo, cache.AC, hash, -1)
	if rc != nil || size != -1 || err != nil {
		if rc != nil {
			_ = rc.Close()
		}
		t.Fatalf("Go prefix AC Get = (%v, %d, %v), want skip miss", rc, size, err)
	}
	if exists, size := c.Contains(ctxGo, cache.AC, hash, -1); exists || size != -1 {
		t.Fatalf("Go prefix AC Contains = (%v, %d), want skip miss", exists, size)
	}

	rc, _, err = c.Get(ctxGo, cache.CAS, hash, -1)
	if err != nil || rc == nil {
		t.Fatalf("Go prefix CAS Get = (%v, %v), want hit", rc, err)
	}
	data, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || string(data) != "gocas" {
		t.Fatalf("Go prefix CAS Get body = (%q, %v), want %q", data, err, "gocas")
	}

	rc, _, err = c.Get(ctxBazel, cache.AC, hash, -1)
	if err != nil || rc == nil {
		t.Fatalf("Bazel prefix AC Get = (%v, %v), want hit", rc, err)
	}
	data, err = io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || string(data) != "bzlac" {
		t.Fatalf("Bazel prefix AC Get body = (%q, %v), want %q", data, err, "bzlac")
	}

	rc, size, err = c.Get(ctxGoTool, cache.AC, hash, -1)
	if rc != nil || size != -1 || err != nil {
		if rc != nil {
			_ = rc.Close()
		}
		t.Fatalf("BuildToolID go AC Get = (%v, %d, %v), want skip miss", rc, size, err)
	}

	if got := testutil.ToFloat64(cacheMisses.WithLabelValues(c.key)) - missesBefore; got != 0 {
		t.Fatalf("cacheMisses delta = %v, want 0", got)
	}
	if got := testutil.ToFloat64(backendLookupsSkipped.WithLabelValues(c.key, skipReasonGoAC)) - skipsBefore; got != 3 {
		t.Fatalf("backendLookupsSkipped{reason=go_ac} delta = %v, want 3", got)
	}
}

func TestGoActionCacheSkipDoesNotDialHungBackend(t *testing.T) {
	hung := make(chan struct{})
	var requests atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		<-hung
	}))
	defer ts.Close()
	defer close(hung)

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	core, err := minio.NewCore(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4("KEY", "SECRET", ""),
		Secure:       false,
		BucketLookup: minio.BucketLookupPath,
		MaxRetries:   1,
		Region:       "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	const hungDeadline = 200 * time.Millisecond
	c := &s3Cache{
		key:          "hung-go-ac-skip",
		mcore:        core,
		bucket:       "test-bucket",
		breaker:      newBreaker("test-go-ac-hung", nil),
		objectKey:    objectKeyV1,
		readDeadline: hungDeadline,
		accessLogger: stdlog.New(&bytes.Buffer{}, "", 0),
	}

	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	ctxGo := cache.WithStoragePrefix(context.Background(), "prd/10/123/v0/go")
	ctxBazel := cache.WithStoragePrefix(context.Background(), "prd/10/123/v0/bazel")

	start := time.Now()
	rc, size, err := c.Get(ctxGo, cache.AC, hash, -1)
	elapsed := time.Since(start)
	if rc != nil || size != -1 || err != nil {
		if rc != nil {
			_ = rc.Close()
		}
		t.Fatalf("Go AC Get = (%v, %d, %v), want immediate miss", rc, size, err)
	}
	if elapsed >= 80*time.Millisecond {
		t.Fatalf("Go AC Get took %v, want immediate skip (not readDeadline)", elapsed)
	}

	start = time.Now()
	exists, size := c.Contains(ctxGo, cache.AC, hash, -1)
	elapsed = time.Since(start)
	if exists || size != -1 {
		t.Fatalf("Go AC Contains = (%v, %d), want skip miss", exists, size)
	}
	if elapsed >= 80*time.Millisecond {
		t.Fatalf("Go AC Contains took %v, want immediate skip (not readDeadline)", elapsed)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("hung backend requests after Go AC skip = %d, want 0", got)
	}

	start = time.Now()
	rc, _, err = c.Get(ctxBazel, cache.AC, hash, -1)
	elapsed = time.Since(start)
	if rc != nil {
		_ = rc.Close()
	}
	if err == nil {
		t.Fatal("Bazel AC Get skipped MinIO or returned a healthy miss; want hung-backend error")
	}
	if elapsed < 150*time.Millisecond {
		t.Fatalf("Bazel AC Get took %v, want ~readDeadline", elapsed)
	}

	start = time.Now()
	rc, _, err = c.Get(ctxGo, cache.CAS, hash, -1)
	elapsed = time.Since(start)
	if rc != nil {
		_ = rc.Close()
	}
	if err == nil {
		t.Fatal("Go CAS Get skipped MinIO or returned a healthy miss; want hung-backend error")
	}
	if elapsed < 150*time.Millisecond {
		t.Fatalf("Go CAS Get took %v, want ~readDeadline", elapsed)
	}

	start = time.Now()
	rc, _, err = c.Get(context.Background(), cache.AC, hash, -1)
	elapsed = time.Since(start)
	if rc != nil {
		_ = rc.Close()
	}
	if err == nil {
		t.Fatal("unscoped AC Get skipped MinIO; Buck2 / unscoped traffic must still probe")
	}
	if elapsed < 150*time.Millisecond {
		t.Fatalf("unscoped AC Get took %v, want ~readDeadline", elapsed)
	}
	if got := requests.Load(); got == 0 {
		t.Fatal("expected hung backend to be dialed for Bazel AC / Go CAS / unscoped AC")
	}
}

func TestSkipBackendLookupReason(t *testing.T) {
	goPrefix := cache.WithStoragePrefix(context.Background(), "prd/10/123/v0/go")
	goTool := cache.WithMetricsLabels(context.Background(), cache.MetricsLabels{BuildToolID: "go"})
	bazelPrefix := cache.WithStoragePrefix(context.Background(), "prd/10/123/v0/bazel")

	cases := []struct {
		name       string
		disconnect bool
		ctx        context.Context
		kind       cache.EntryKind
		reason     string
		skip       bool
	}{
		{name: "off go prefix AC", disconnect: false, ctx: goPrefix, kind: cache.AC, reason: skipReasonGoAC, skip: true},
		{name: "off go prefix CAS", disconnect: false, ctx: goPrefix, kind: cache.CAS, skip: false},
		{name: "off go label CAS", disconnect: false, ctx: goTool, kind: cache.CAS, skip: false},
		{name: "off bazel prefix AC", disconnect: false, ctx: bazelPrefix, kind: cache.AC, skip: false},
		{name: "on go prefix AC", disconnect: true, ctx: goPrefix, kind: cache.AC, reason: skipReasonGoDisconnect, skip: true},
		{name: "on go prefix CAS", disconnect: true, ctx: goPrefix, kind: cache.CAS, reason: skipReasonGoDisconnect, skip: true},
		{name: "on go label AC", disconnect: true, ctx: goTool, kind: cache.AC, reason: skipReasonGoDisconnect, skip: true},
		{name: "on go label CAS", disconnect: true, ctx: goTool, kind: cache.CAS, reason: skipReasonGoDisconnect, skip: true},
		{name: "on bazel prefix AC", disconnect: true, ctx: bazelPrefix, kind: cache.AC, skip: false},
		{name: "on bazel prefix CAS", disconnect: true, ctx: bazelPrefix, kind: cache.CAS, skip: false},
		{name: "on unscoped AC", disconnect: true, ctx: context.Background(), kind: cache.AC, skip: false},
		{name: "on unscoped CAS", disconnect: true, ctx: context.Background(), kind: cache.CAS, skip: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &s3Cache{goBackendDisconnect: tc.disconnect}
			reason, skip := c.skipBackendLookupReason(tc.ctx, tc.kind)
			if skip != tc.skip || reason != tc.reason {
				t.Fatalf("skipBackendLookupReason = (%q, %v), want (%q, %v)", reason, skip, tc.reason, tc.skip)
			}
		})
	}
}

// countingFakeS3Backend is fakeS3Backend with a request counter in front of
// the endpoint — the seam for proving an operation never dialed the backend.
func countingFakeS3Backend(t *testing.T, requests *atomic.Int64, buckets ...string) *s3Cache {
	t.Helper()
	backend := s3mem.New()
	for _, bucket := range buckets {
		if err := backend.CreateBucket(bucket); err != nil {
			t.Fatal(err)
		}
	}
	inner := gofakes3.New(backend).Server()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	core, err := minio.NewCore(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4("KEY", "SECRET", ""),
		Secure:       false,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		t.Fatal(err)
	}

	return &s3Cache{
		key:          backendKeyA,
		mcore:        core,
		bucket:       "default-bucket",
		breaker:      newBreaker("test-counting-fake-s3", nil),
		objectKey:    objectKeyV1,
		accessLogger: stdlog.New(&bytes.Buffer{}, "", 0),
	}
}

// TestGoBackendDisconnectSkipsAllKinds pins the canary toggle's read side:
// with the disconnect on, Go traffic (either detection signal) answers clean
// misses for EVERY entry kind without dialing the backend — even for objects
// the backend demonstrably holds — while non-Go traffic is untouched.
func TestGoBackendDisconnectSkipsAllKinds(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	var requests atomic.Int64
	c := countingFakeS3Backend(t, &requests, "default-bucket")
	c.key = "go-disconnect-lookups"
	c.goBackendDisconnect = true

	goPrefix := "prd/10/123/v0/go"
	bazelPrefix := "prd/10/123/v0/bazel"
	ctxGo := cache.WithStoragePrefix(context.Background(), goPrefix)
	ctxGoTool := cache.WithMetricsLabels(context.Background(), cache.MetricsLabels{BuildToolID: "go"})
	ctxBazel := cache.WithStoragePrefix(context.Background(), bazelPrefix)

	seedS3Object(t, c, goPrefix, cache.AC, hash, "goac")
	seedS3Object(t, c, goPrefix, cache.CAS, hash, "gocas")
	seedS3Object(t, c, bazelPrefix, cache.AC, hash, "bzlac")
	seedS3Object(t, c, bazelPrefix, cache.CAS, hash, "bzlcas")

	seeded := requests.Load()
	disconnectSkipsBefore := testutil.ToFloat64(backendLookupsSkipped.WithLabelValues(c.key, skipReasonGoDisconnect))
	goACSkipsBefore := testutil.ToFloat64(backendLookupsSkipped.WithLabelValues(c.key, skipReasonGoAC))

	for _, signal := range []struct {
		name string
		ctx  context.Context
	}{
		{"go prefix", ctxGo},
		{"go build tool label", ctxGoTool},
	} {
		for _, kind := range []cache.EntryKind{cache.AC, cache.CAS} {
			rc, size, err := c.Get(signal.ctx, kind, hash, -1)
			if rc != nil || size != -1 || err != nil {
				if rc != nil {
					_ = rc.Close()
				}
				t.Fatalf("%s %s Get = (%v, %d, %v), want skip miss", signal.name, kind, rc, size, err)
			}
			if exists, size := c.Contains(signal.ctx, kind, hash, -1); exists || size != -1 {
				t.Fatalf("%s %s Contains = (%v, %d), want skip miss", signal.name, kind, exists, size)
			}
		}
	}

	if got := requests.Load() - seeded; got != 0 {
		t.Fatalf("backend dialed %d times for disconnected Go traffic, want 0", got)
	}
	if got := testutil.ToFloat64(backendLookupsSkipped.WithLabelValues(c.key, skipReasonGoDisconnect)) - disconnectSkipsBefore; got != 8 {
		t.Fatalf("backendLookupsSkipped{reason=go_disconnect} delta = %v, want 8", got)
	}
	// With the toggle on, Go AC skips count under go_disconnect, not go_ac,
	// so dashboards can tell the canary apart from the steady-state skip.
	if got := testutil.ToFloat64(backendLookupsSkipped.WithLabelValues(c.key, skipReasonGoAC)) - goACSkipsBefore; got != 0 {
		t.Fatalf("backendLookupsSkipped{reason=go_ac} delta = %v, want 0", got)
	}

	for _, kind := range []cache.EntryKind{cache.AC, cache.CAS} {
		rc, _, err := c.Get(ctxBazel, kind, hash, -1)
		if err != nil || rc == nil {
			t.Fatalf("bazel %s Get = (%v, %v), want hit", kind, rc, err)
		}
		_ = rc.Close()
		if exists, _ := c.Contains(ctxBazel, kind, hash, -1); !exists {
			t.Fatalf("bazel %s Contains = false, want true", kind)
		}
	}
	if got := requests.Load() - seeded; got == 0 {
		t.Fatal("expected non-Go traffic to dial the backend with the disconnect on")
	}
}

// TestGoBackendDisconnectPutSkipsEnqueue pins the canary toggle's write
// side: with the disconnect on, a Go Put closes the payload and returns
// without enqueueing, counted on backendUploadsSkipped and — critically —
// with NO operation outcome (dropped/error/rejected statuses are consumed
// by the web-side accounting as failures).
func TestGoBackendDisconnectPutSkipsEnqueue(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	uploadQueue := make(chan backendproxy.UploadReq, 2)
	observer := &recordingObserver{}
	c := &s3Cache{
		key:                 "go-disconnect-put",
		uploadQueue:         uploadQueue,
		observer:            observer,
		goBackendDisconnect: true,
	}

	skipsBefore := testutil.ToFloat64(backendUploadsSkipped.WithLabelValues(c.key, skipReasonGoDisconnect))

	ctxGoPrefix := cache.WithStoragePrefix(context.Background(), "prd/10/123/v0/go")
	ctxGoTool := cache.WithMetricsLabels(context.Background(), cache.MetricsLabels{BuildToolID: "go"})

	for _, tc := range []struct {
		name string
		ctx  context.Context
		kind cache.EntryKind
	}{
		{"go prefix CAS", ctxGoPrefix, cache.CAS},
		{"go prefix AC", ctxGoPrefix, cache.AC},
		{"go build tool label CAS", ctxGoTool, cache.CAS},
		{"go build tool label AC", ctxGoTool, cache.AC},
	} {
		rc := &closeRecorder{Reader: strings.NewReader("blob")}
		c.Put(tc.ctx, tc.kind, hash, 4, 4, rc)
		if !rc.closed {
			t.Fatalf("%s: expected skipped Put to close the reader", tc.name)
		}
		select {
		case <-uploadQueue:
			t.Fatalf("%s: skipped Put reached the upload queue", tc.name)
		default:
		}
	}

	if got := testutil.ToFloat64(backendUploadsSkipped.WithLabelValues(c.key, skipReasonGoDisconnect)) - skipsBefore; got != 4 {
		t.Fatalf("backendUploadsSkipped{reason=go_disconnect} delta = %v, want 4", got)
	}
	if len(observer.outcomes) != 0 {
		t.Fatalf("observer outcomes = %+v, want none for policy skips", observer.outcomes)
	}

	// Non-Go traffic still write-throughs with the disconnect on.
	c.Put(cache.WithStoragePrefix(context.Background(), "prd/10/123/v0/bazel"), cache.CAS, hash, 4, 4,
		io.NopCloser(strings.NewReader("blob")))
	select {
	case item := <-uploadQueue:
		_ = item.Rc.Close()
	default:
		t.Fatal("expected non-Go Put to enqueue with the disconnect on")
	}
}

// TestGoBackendDisconnectOffGoPutStillEnqueues pins the toggle-off contract:
// without the disconnect, Go traffic write-throughs exactly as before, for
// both entry kinds.
func TestGoBackendDisconnectOffGoPutStillEnqueues(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	uploadQueue := make(chan backendproxy.UploadReq, 1)
	c := &s3Cache{uploadQueue: uploadQueue}

	ctxGo := cache.WithStoragePrefix(context.Background(), "prd/10/123/v0/go")
	ctxGoTool := cache.WithMetricsLabels(context.Background(), cache.MetricsLabels{BuildToolID: "go"})

	for _, tc := range []struct {
		name string
		ctx  context.Context
		kind cache.EntryKind
	}{
		{"go prefix CAS", ctxGo, cache.CAS},
		{"go prefix AC", ctxGo, cache.AC},
		{"go build tool label CAS", ctxGoTool, cache.CAS},
	} {
		c.Put(tc.ctx, tc.kind, hash, 4, 4, io.NopCloser(strings.NewReader("blob")))
		select {
		case item := <-uploadQueue:
			_ = item.Rc.Close()
		default:
			t.Fatalf("%s: toggle-off Go Put did not enqueue", tc.name)
		}
	}
}

// Go-tenant LRU artifacts are part of the disconnect's "zero S3 ops"
// contract: skipped as a successful no-op (an error would make the flusher
// log every pass), counted on the skip counter. Key-based, because artifact
// flushes run on timers whose contexts carry no tenant identity.
func TestGoBackendDisconnectSkipsGoTenantArtifacts(t *testing.T) {
	c := &s3Cache{key: "go-disconnect-artifacts", goBackendDisconnect: true}

	skipsBefore := testutil.ToFloat64(backendUploadsSkipped.WithLabelValues(c.key, skipReasonGoDisconnect))
	if err := c.PutArtifact(context.Background(), "prd/10/123/go/lru/00000001-x.jsonl", []byte("{}")); err != nil {
		t.Fatalf("skipped go artifact must be a successful no-op, got %v", err)
	}
	if got := testutil.ToFloat64(backendUploadsSkipped.WithLabelValues(c.key, skipReasonGoDisconnect)) - skipsBefore; got != 1 {
		t.Fatalf("expected 1 skipped artifact upload, got %v", got)
	}
}

func TestGoTenantArtifactKey(t *testing.T) {
	cases := map[string]bool{
		"prd/10/123/go/lru/00000001-x.jsonl":    true,
		"staging/42/9/v0/go/lru/x.jsonl":        true,
		"go/lru/x.jsonl":                        true,
		"prd/10/123/bazel/lru/00000001-x.jsonl": false,
		"prd/10/123/go/cas.v2/ab/abcd":          false,
		"lru/x.jsonl":                           false,
		"prd/10/go":                             false,
	}
	for key, want := range cases {
		if got := goTenantArtifactKey(key); got != want {
			t.Errorf("goTenantArtifactKey(%q) = %v, want %v", key, got, want)
		}
	}
}
