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
		objectKey:    objectKeyV1,
		accessLogger: stdlog.New(&bytes.Buffer{}, "", 0),
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
