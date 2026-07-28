package s3proxy

import (
	"bytes"
	"context"
	"io"
	stdlog "log"
	"net/http"
	"strings"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/utils/backendproxy"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
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
	ctxB := cache.WithS3Backend(context.Background(), backendKeyB)
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
	ctxA := cache.WithS3Backend(context.Background(), backendKeyA)
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

	ctx := cache.WithS3Backend(context.Background(), "http://rogue.example.com:9000")

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

	if _, err := NewMulti(nil, false, -1, "uncompressed", nil, nil, 0, 0, nil); err == nil {
		t.Fatal("expected error for empty backend list")
	}

	if _, err := NewMulti([]BackendSpec{spec(backendKeyA, false)},
		false, -1, "uncompressed", nil, nil, 0, 0, nil); err == nil ||
		!strings.Contains(err.Error(), "no S3 backend marked as default") {
		t.Fatal("expected error when no backend is default")
	}

	if _, err := NewMulti([]BackendSpec{spec(backendKeyA, true), spec(backendKeyA, false)},
		false, -1, "uncompressed", nil, nil, 0, 0, nil); err == nil ||
		!strings.Contains(err.Error(), "duplicate S3 backend key") {
		t.Fatal("expected error for duplicate backend keys")
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
