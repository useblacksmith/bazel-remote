package s3proxy

import (
	"bytes"
	"context"
	stdlog "log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const testHash = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

// failingServer serves 500 to every request and counts them, standing in
// for a sick MinIO node that still answers (as opposed to one that hangs).
func failingServer(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var requests atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(ts.Close)
	return ts, &requests
}

// coreFor builds a minio core against a test server, with the retry count
// under the test's control.
func coreFor(t *testing.T, ts *httptest.Server, maxRetries int) *minio.Core {
	t.Helper()
	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	core, err := minio.NewCore(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4("KEY", "SECRET", ""),
		Secure:       false,
		BucketLookup: minio.BucketLookupPath,
		MaxRetries:   maxRetries,
		// A fixed region skips minio-go's bucket-location lookup, keeping
		// request counts deterministic against the fake backends.
		Region: "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	return core
}

// TestReadDeadlineBoundsHungGet pins the read deadline: a backend that
// accepts the connection and never responds must fail the Get once
// readDeadline elapses, instead of pinning the caller for the transport's
// 60s+.
func TestReadDeadlineBoundsHungGet(t *testing.T) {
	oldDeadline := readDeadline
	readDeadline = 100 * time.Millisecond
	defer func() { readDeadline = oldDeadline }()

	hung := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-hung // Hold every request open until the test finishes.
	}))
	t.Cleanup(ts.Close)
	t.Cleanup(func() { close(hung) })

	c := &s3Cache{
		key:          "deadline-test",
		mcore:        coreFor(t, ts, 1),
		bucket:       "test-bucket",
		objectKey:    objectKeyV1,
		accessLogger: stdlog.New(&bytes.Buffer{}, "", 0),
	}

	start := time.Now()
	rc, _, err := c.Get(context.Background(), cache.CAS, testHash, -1)
	elapsed := time.Since(start)
	if err == nil || rc != nil {
		t.Fatalf("hung-backend Get = (%v, %v), want deadline error", rc, err)
	}
	if elapsed >= 2*time.Second {
		t.Fatalf("hung-backend Get took %v, want ~readDeadline (100ms)", elapsed)
	}
}

// TestNewBackendCapsMinioRetries pins the retry cap on the constructed
// client: one failing Get must dial the backend exactly maxRetries times
// (minio-go's library default of 10 attempts is what turned each failing
// call into ~a minute of retention).
func TestNewBackendCapsMinioRetries(t *testing.T) {
	ts, requests := failingServer(t)
	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}

	c, err := newBackend(BackendSpec{
		Key:              "retry-cap-test",
		Endpoint:         u.Host,
		Bucket:           "test-bucket",
		BucketLookupType: minio.BucketLookupPath,
		Credentials:      credentials.NewStaticV4("KEY", "SECRET", ""),
		DisableSSL:       true,
		// A fixed region skips minio-go's bucket-location lookup, so every
		// counted request is a real GET attempt.
		Region: "us-east-1",
	}, false, -1, "uncompressed", stdlog.New(&bytes.Buffer{}, "", 0), nil, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := c.Get(context.Background(), cache.CAS, testHash, -1); err == nil {
		t.Fatal("Get against failing backend returned nil error")
	}
	if got := requests.Load(); got != maxRetries {
		t.Fatalf("failing Get dialed the backend %d times, want %d (retry cap)", got, maxRetries)
	}
}
