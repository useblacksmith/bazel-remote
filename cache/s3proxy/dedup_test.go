package s3proxy

import (
	"bytes"
	"context"
	"io"
	stdlog "log"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/utils/backendproxy"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func dedupTestCache(t *testing.T, name string, queueCap int) (*s3Cache, chan backendproxy.UploadReq) {
	t.Helper()
	queue := make(chan backendproxy.UploadReq, queueCap)
	logger := stdlog.New(&bytes.Buffer{}, "", 0)
	c := &s3Cache{
		key:          name,
		bucket:       "test-bucket",
		breaker:      newBreaker(name, nil),
		objectKey:    objectKeyV1,
		readDeadline: readDeadline,
		accessLogger: logger,
		errorLogger:  logger,
		inflight:     newInflightSet(),
		uploadQueue:  queue,
	}
	return c, queue
}

func TestDuplicateEnqueueCoalesced(t *testing.T) {
	c, queue := dedupTestCache(t, "dedup-coalesce-test", 4)

	before := testutil.ToFloat64(uploadQueueCoalesced.WithLabelValues(c.key))
	c.Put(context.Background(), cache.CAS, testHash, 4, 4, io.NopCloser(strings.NewReader("blob")))
	c.Put(context.Background(), cache.CAS, testHash, 4, 4, io.NopCloser(strings.NewReader("blob")))

	if got := len(queue); got != 1 {
		t.Fatalf("queue depth after duplicate Put = %d, want 1", got)
	}
	if got := testutil.ToFloat64(uploadQueueCoalesced.WithLabelValues(c.key)) - before; got != 1 {
		t.Fatalf("coalesced counter delta = %v, want 1", got)
	}

	// A different hash is not coalesced.
	otherHash := strings.Repeat("ab", 32)
	c.Put(context.Background(), cache.CAS, otherHash, 4, 4, io.NopCloser(strings.NewReader("blob")))
	if got := len(queue); got != 2 {
		t.Fatalf("queue depth after distinct Put = %d, want 2", got)
	}
}

// TestQueueFullShedReleasesIdentity pins the shed path's claim hygiene: a
// Put dropped for queue-full must release its in-flight claim, so a later
// Put of the same object is admitted instead of coalescing against a ghost.
func TestQueueFullShedReleasesIdentity(t *testing.T) {
	c, queue := dedupTestCache(t, "dedup-shed-test", 1)

	fillerHash := strings.Repeat("cd", 32)
	c.Put(context.Background(), cache.CAS, fillerHash, 4, 4, io.NopCloser(strings.NewReader("fill")))
	// Queue (cap 1) is full: this one sheds.
	c.Put(context.Background(), cache.CAS, testHash, 4, 4, io.NopCloser(strings.NewReader("blob")))
	if got := len(queue); got != 1 {
		t.Fatalf("queue depth after shed = %d, want 1", got)
	}

	// Drain the filler; the shed identity must be admittable again.
	<-queue
	before := testutil.ToFloat64(uploadQueueCoalesced.WithLabelValues(c.key))
	c.Put(context.Background(), cache.CAS, testHash, 4, 4, io.NopCloser(strings.NewReader("blob")))
	if got := len(queue); got != 1 {
		t.Fatalf("queue depth after re-Put of shed identity = %d, want 1", got)
	}
	if got := testutil.ToFloat64(uploadQueueCoalesced.WithLabelValues(c.key)) - before; got != 0 {
		t.Fatalf("re-Put of shed identity was coalesced (delta %v), want admitted", got)
	}
}

// TestUploadReleasesIdentity pins the worker side: a terminal upload —
// success here — must release the identity so the next Put of the same
// object is admitted.
func TestUploadReleasesIdentity(t *testing.T) {
	backend := s3mem.New()
	if err := backend.CreateBucket("test-bucket"); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(ts.Close)

	c, queue := dedupTestCache(t, "dedup-release-test", 4)
	c.mcore = coreFor(t, ts, 1)

	content := []byte("dedup-blob-content")
	c.Put(context.Background(), cache.CAS, testHash, int64(len(content)), int64(len(content)),
		io.NopCloser(bytes.NewReader(content)))
	c.UploadFile(<-queue)

	c.Put(context.Background(), cache.CAS, testHash, int64(len(content)), int64(len(content)),
		io.NopCloser(bytes.NewReader(content)))
	if got := len(queue); got != 1 {
		t.Fatalf("queue depth after post-upload re-Put = %d, want 1 (identity not released)", got)
	}
}
