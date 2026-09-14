package s3proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	stdlog "log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/cache/disk/casblob"
	"github.com/buchgr/bazel-remote/v2/cache/disk/zstdimpl"
	"github.com/buchgr/bazel-remote/v2/utils/backendproxy"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

const fallbackBackendKey = "v1-fallback-test-backend"

// fakeV2S3Backend builds a v2mode (zstd) s3Cache with the v1 fallback
// enabled over an in-memory S3 server, with live upload workers so
// promotions actually run.
func fakeV2S3Backend(t *testing.T) *s3Cache {
	t.Helper()
	c := fakeS3Backend(t, "default-bucket")
	c.key = fallbackBackendKey
	c.v2mode = true
	c.objectKey = objectKeyV2
	c.v1FallbackTmpDir = filepath.Join(t.TempDir(), "s3-v1-fallback-tmp")
	c.errorLogger = stdlog.New(&bytes.Buffer{}, "", 0)

	zi, err := zstdimpl.Get("go")
	if err != nil {
		t.Fatal(err)
	}
	c.zstd = zi
	c.initV1Fallback()
	if !c.v1Fallback {
		t.Fatal("v1 fallback did not enable")
	}
	c.uploadQueue = backendproxy.StartUploaders(c, 1, 16)
	return c
}

func seedV1CASObject(t *testing.T, c *s3Cache, prefix string, body string) string {
	t.Helper()
	sum := sha256.Sum256([]byte(body))
	hash := hex.EncodeToString(sum[:])
	key := objectKeyV1(prefix, hash, cache.CAS)
	_, err := c.mcore.PutObject(context.Background(), c.bucket, key,
		strings.NewReader(body), int64(len(body)), "", "", minio.PutObjectOptions{})
	if err != nil {
		t.Fatalf("seed v1 PutObject %s: %v", key, err)
	}
	return hash
}

// readCasblobStream verifies a stream is a well-formed zstd casblob holding
// exactly want, by writing it to a file and reading it back decompressed.
func readCasblobStream(t *testing.T, zi zstdimpl.ZstdImpl, rc io.ReadCloser, want string) {
	t.Helper()
	raw, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("reading returned stream: %v", err)
	}
	f, err := os.CreateTemp(t.TempDir(), "casblob-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(raw); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	urc, err := casblob.GetUncompressedReadCloser(zi, f, int64(len(want)), 0)
	if err != nil {
		t.Fatalf("stream is not a valid casblob: %v", err)
	}
	got, err := io.ReadAll(urc)
	_ = urc.Close()
	if err != nil {
		t.Fatalf("decompressing casblob: %v", err)
	}
	if string(got) != want {
		t.Fatalf("casblob content mismatch: got %d bytes, want %d", len(got), len(want))
	}
}

func counterValue(t *testing.T, event string) float64 {
	t.Helper()
	return testutil.ToFloat64(v1FallbackEvents.WithLabelValues(fallbackBackendKey, event))
}

func TestV1FallbackGetHydratesAndPromotes(t *testing.T) {
	c := fakeV2S3Backend(t)
	zi := c.zstd
	body := strings.Repeat("v1 fallback test content. ", 100)
	hash := seedV1CASObject(t, c, "", body)
	ctx := context.Background()

	hitsBefore := counterValue(t, v1FallbackGetHit)

	rc, logicalSize, err := c.Get(ctx, cache.CAS, hash, int64(len(body)))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if rc == nil {
		t.Fatal("Get returned a miss for a v1-present object")
	}
	if logicalSize != int64(len(body)) {
		t.Fatalf("logical size: got %d, want %d", logicalSize, len(body))
	}
	readCasblobStream(t, zi, rc, body)

	if got := counterValue(t, v1FallbackGetHit); got != hitsBefore+1 {
		t.Fatalf("get_hit counter: got %v, want %v", got, hitsBefore+1)
	}

	// The promotion runs on an upload worker; poll for the v2 object.
	v2Key := objectKeyV2("", hash, cache.CAS)
	deadline := time.Now().Add(5 * time.Second)
	var statErr error
	for time.Now().Before(deadline) {
		_, statErr = c.mcore.StatObject(ctx, c.bucket, v2Key, minio.StatObjectOptions{})
		if statErr == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if statErr != nil {
		t.Fatalf("promoted v2 object never appeared at %s: %v", v2Key, statErr)
	}

	// The promoted object must itself be a valid casblob, and a subsequent
	// Get must be served from cas.v2 without touching the fallback.
	rc2, logicalSize2, err := c.Get(ctx, cache.CAS, hash, int64(len(body)))
	if err != nil || rc2 == nil {
		t.Fatalf("post-promotion Get: rc=%v err=%v", rc2, err)
	}
	if logicalSize2 != int64(len(body)) {
		t.Fatalf("post-promotion logical size: got %d, want %d", logicalSize2, len(body))
	}
	readCasblobStream(t, zi, rc2, body)
	if got := counterValue(t, v1FallbackGetHit); got != hitsBefore+1 {
		t.Fatalf("post-promotion Get used the fallback again: counter %v", got)
	}
}

func TestV1FallbackGetMissWhenNeitherKeyspaceHasIt(t *testing.T) {
	c := fakeV2S3Backend(t)
	missesBefore := counterValue(t, v1FallbackGetMiss)

	hash := strings.Repeat("ab", 32)
	rc, _, err := c.Get(context.Background(), cache.CAS, hash, 100)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if rc != nil {
		t.Fatal("Get returned a body for an absent object")
	}
	if got := counterValue(t, v1FallbackGetMiss); got != missesBefore+1 {
		t.Fatalf("get_miss counter: got %v, want %v", got, missesBefore+1)
	}
}

func TestV1FallbackTranscodeRejectsCorruptObject(t *testing.T) {
	c := fakeV2S3Backend(t)
	body := "the real content"
	sum := sha256.Sum256([]byte(body))
	hash := hex.EncodeToString(sum[:])
	// Seed DIFFERENT bytes under that hash's v1 key: hash verification in
	// the transcode must reject it and degrade to a miss.
	corrupt := "not the real content"
	key := objectKeyV1("", hash, cache.CAS)
	_, err := c.mcore.PutObject(context.Background(), c.bucket, key,
		strings.NewReader(corrupt), int64(len(corrupt)), "", "", minio.PutObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	errsBefore := counterValue(t, v1FallbackError)
	rc, _, err := c.Get(context.Background(), cache.CAS, hash, int64(len(corrupt)))
	if err != nil {
		t.Fatalf("Get must degrade to a miss, got error: %v", err)
	}
	if rc != nil {
		t.Fatal("Get served a corrupt v1 object")
	}
	if got := counterValue(t, v1FallbackError); got != errsBefore+1 {
		t.Fatalf("error counter: got %v, want %v", got, errsBefore+1)
	}
}

func TestV1FallbackContains(t *testing.T) {
	c := fakeV2S3Backend(t)
	body := "contains fallback body"
	hash := seedV1CASObject(t, c, "", body)

	containsBefore := counterValue(t, v1FallbackContainsHit)
	exists, size := c.Contains(context.Background(), cache.CAS, hash, int64(len(body)))
	if !exists {
		t.Fatal("Contains missed a v1-present object")
	}
	if size != -1 {
		t.Fatalf("Contains size: got %d, want -1 (CAS/v2 convention)", size)
	}
	if got := counterValue(t, v1FallbackContainsHit); got != containsBefore+1 {
		t.Fatalf("contains_hit counter: got %v, want %v", got, containsBefore+1)
	}

	absent := strings.Repeat("cd", 32)
	exists, _ = c.Contains(context.Background(), cache.CAS, absent, 10)
	if exists {
		t.Fatal("Contains reported an absent object as present")
	}
}

func TestV1FallbackDisabledOutsideV2Mode(t *testing.T) {
	c := fakeS3Backend(t, "default-bucket")
	c.errorLogger = stdlog.New(&bytes.Buffer{}, "", 0)
	c.initV1Fallback()
	if c.v1Fallback {
		t.Fatal("v1 fallback enabled on a non-v2 backend")
	}
}

func TestV1FallbackKillSwitch(t *testing.T) {
	t.Setenv(disableV1FallbackEnv, "1")
	c := fakeS3Backend(t, "default-bucket")
	c.v2mode = true
	c.errorLogger = stdlog.New(&bytes.Buffer{}, "", 0)
	c.initV1Fallback()
	if c.v1Fallback {
		t.Fatal("v1 fallback enabled despite kill switch")
	}
}

func TestV1FallbackSizeMismatchDegradesToMiss(t *testing.T) {
	c := fakeV2S3Backend(t)
	body := "sized content"
	hash := seedV1CASObject(t, c, "", body)

	errsBefore := counterValue(t, v1FallbackError)
	// Expected size disagrees with the v1 object: the fallback must refuse
	// before transcoding and report a miss, not an error.
	rc, _, err := c.Get(context.Background(), cache.CAS, hash, int64(len(body))+7)
	if err != nil {
		t.Fatalf("Get must degrade to a miss, got error: %v", err)
	}
	if rc != nil {
		t.Fatal("Get served an object whose size does not match the request")
	}
	if got := counterValue(t, v1FallbackError); got != errsBefore+1 {
		t.Fatalf("error counter: got %v, want %v", got, errsBefore+1)
	}

	// The unknown-size convention (-1) must still hydrate.
	rc, logicalSize, err := c.Get(context.Background(), cache.CAS, hash, -1)
	if err != nil || rc == nil {
		t.Fatalf("Get with unknown size: rc=%v err=%v", rc, err)
	}
	defer func() { _ = rc.Close() }()
	if logicalSize != int64(len(body)) {
		t.Fatalf("logical size: got %d, want %d", logicalSize, len(body))
	}
}

// TestV1FallbackBackendErrorDegradesToMiss pins the no-new-failure-mode
// property: a v2 NoSuchKey used to be a terminal miss, so a backend that
// then fails the v1 attempt (here: 500s) must still produce a miss, never
// a client-visible error.
func TestV1FallbackBackendErrorDegradesToMiss(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("location") {
			// minio-go probes the bucket location before the first request.
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint></LocationConstraint>`))
			return
		}
		if strings.Contains(r.URL.Path, "cas.v2") {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>absent</Message></Error>`))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
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
	zi, err := zstdimpl.Get("go")
	if err != nil {
		t.Fatal(err)
	}
	c := &s3Cache{
		key:              fallbackBackendKey,
		mcore:            core,
		bucket:           "default-bucket",
		breaker:          newBreaker("test-v1-fallback-error", nil),
		objectKey:        objectKeyV2,
		v2mode:           true,
		zstd:             zi,
		v1Fallback:       true,
		v1FallbackTmpDir: t.TempDir(),
		accessLogger:     stdlog.New(&bytes.Buffer{}, "", 0),
		errorLogger:      stdlog.New(&bytes.Buffer{}, "", 0),
	}

	errsBefore := counterValue(t, v1FallbackError)
	hash := strings.Repeat("ef", 32)
	rc, _, err := c.Get(context.Background(), cache.CAS, hash, 10)
	if err != nil {
		t.Fatalf("Get must degrade to a miss when the v1 attempt fails, got error: %v", err)
	}
	if rc != nil {
		t.Fatal("Get returned a body from a failing backend")
	}
	if got := counterValue(t, v1FallbackError); got != errsBefore+1 {
		t.Fatalf("error counter: got %v, want %v", got, errsBefore+1)
	}
}
