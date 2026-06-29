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
