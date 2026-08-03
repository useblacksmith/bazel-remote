package s3proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	stdlog "log"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/utils/backendproxy"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeBlobSource serves owed blobs from a map; missing keys error like an
// evicted blob.
type fakeBlobSource struct {
	blobs map[string][]byte
	calls int
}

func (f *fakeBlobSource) OpenOwedBlob(_ context.Context, _ cache.EntryKind, hash string) (io.ReadCloser, int64, error) {
	f.calls++
	data, ok := f.blobs[hash]
	if !ok {
		return nil, -1, errors.New("evicted")
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
}

func owedTestCache(t *testing.T, name string, queueCap int) (*s3Cache, chan backendproxy.UploadReq) {
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
		owed:         newOwedLedger(t.TempDir(), name, logger),
		uploadQueue:  queue,
	}
	return c, queue
}

func TestDuplicateEnqueueCoalesced(t *testing.T) {
	c, queue := owedTestCache(t, "owed-coalesce-test", 4)

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

func TestQueueFullRecordsOwedAndSweeperRepays(t *testing.T) {
	backend := s3mem.New()
	if err := backend.CreateBucket("test-bucket"); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(ts.Close)

	c, _ := owedTestCache(t, "owed-repay-test", 1)
	c.mcore = coreFor(t, ts, 1)

	content := []byte("owed-blob-content")

	// Fill the queue, then overflow it with the blob we care about.
	fillerHash := strings.Repeat("cd", 32)
	c.Put(context.Background(), cache.CAS, fillerHash, 4, 4, io.NopCloser(strings.NewReader("fill")))
	c.Put(context.Background(), cache.CAS, testHash, int64(len(content)), int64(len(content)),
		io.NopCloser(bytes.NewReader(content)))

	if got := testutil.ToFloat64(owedBacklog.WithLabelValues(c.key)); got != 1 {
		t.Fatalf("owed backlog after shed = %v, want 1", got)
	}

	// Sweep into a fresh, roomy queue (the shed queue stays full so the
	// sweeper's headroom check would rightly refuse it).
	sweepQueue := make(chan backendproxy.UploadReq, 8)
	c.uploadQueue = sweepQueue
	c.blobSource = &fakeBlobSource{blobs: map[string][]byte{testHash: content}}
	c.sweepOwedOnce()

	if got := len(sweepQueue); got != 1 {
		t.Fatalf("sweep queue depth = %d, want 1 requeued upload", got)
	}
	req := <-sweepQueue
	if req.Hash != testHash || req.SizeOnDisk != int64(len(content)) {
		t.Fatalf("requeued req = %+v, want hash %s size %d", req, testHash, len(content))
	}

	// Run the upload; success must settle the debt and store the object.
	c.UploadFile(req)
	if got := testutil.ToFloat64(owedBacklog.WithLabelValues(c.key)); got != 0 {
		t.Fatalf("owed backlog after successful repay = %v, want 0", got)
	}
	rc, _, err := c.Get(context.Background(), cache.CAS, testHash, -1)
	if err != nil || rc == nil {
		t.Fatalf("Get after repay = (%v, %v), want hit", rc, err)
	}
	stored, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || !bytes.Equal(stored, content) {
		t.Fatalf("stored bytes = %q (err %v), want %q", stored, err, content)
	}
}

func TestUploadFailureRecordsOwedAndBreakerBlocksSweep(t *testing.T) {
	c, _ := owedTestCache(t, "owed-failure-test", 4)

	// A breaker-refused upload is a terminal failure: the debt must be
	// recorded and the reader closed.
	tripBreaker(t, c.breaker)
	c.UploadFile(backendproxy.UploadReq{
		Hash:       testHash,
		Kind:       cache.CAS,
		SizeOnDisk: 4,
		Rc:         io.NopCloser(strings.NewReader("blob")),
	})
	if got := testutil.ToFloat64(owedBacklog.WithLabelValues(c.key)); got != 1 {
		t.Fatalf("owed backlog after breaker-refused upload = %v, want 1", got)
	}

	// While the breaker is open the sweeper must not even consult the blob
	// source — MinIO is sick; retrying now is the old thundering herd.
	src := &fakeBlobSource{blobs: map[string][]byte{}}
	c.blobSource = src
	c.sweepOwedOnce()
	if src.calls != 0 {
		t.Fatalf("sweep consulted blob source %d times with breaker open, want 0", src.calls)
	}
}

func TestSweepEvictedBlobSettlesDebt(t *testing.T) {
	c, queue := owedTestCache(t, "owed-evicted-test", 4)
	c.owed.add(owedEntry{Key: uploadKey{Kind: cache.CAS, Hash: testHash, Bucket: "test-bucket"}})

	c.blobSource = &fakeBlobSource{blobs: map[string][]byte{}} // nothing local
	c.sweepOwedOnce()

	if got := testutil.ToFloat64(owedBacklog.WithLabelValues(c.key)); got != 0 {
		t.Fatalf("owed backlog after evicted-blob sweep = %v, want 0 (debt void)", got)
	}
	if got := len(queue); got != 0 {
		t.Fatalf("queue depth after evicted-blob sweep = %d, want 0", got)
	}
}

func TestSweepYieldsToBusyQueue(t *testing.T) {
	c, queue := owedTestCache(t, "owed-yield-test", 4)
	c.owed.add(owedEntry{Key: uploadKey{Kind: cache.CAS, Hash: testHash, Bucket: "test-bucket"}})

	// 2 of 4 slots used = at the 50% headroom threshold: sweep must yield.
	queue <- backendproxy.UploadReq{}
	queue <- backendproxy.UploadReq{}

	src := &fakeBlobSource{blobs: map[string][]byte{testHash: []byte("blob")}}
	c.blobSource = src
	c.sweepOwedOnce()

	if src.calls != 0 {
		t.Fatalf("sweep consulted blob source %d times with a busy queue, want 0", src.calls)
	}
	if got := testutil.ToFloat64(owedBacklog.WithLabelValues(c.key)); got != 1 {
		t.Fatalf("owed backlog after yielded sweep = %v, want 1 (still owed)", got)
	}
}

func TestOwedLedgerSnapshotRoundtrip(t *testing.T) {
	dir := t.TempDir()
	logger := stdlog.New(&bytes.Buffer{}, "", 0)

	l := newOwedLedger(dir, "snap-test", logger)
	entry := owedEntry{
		Key:                        uploadKey{Kind: cache.CAS, Hash: testHash, Prefix: "tenant-a", Bucket: "b"},
		LogicalSize:                42,
		RequestScopedStoragePrefix: true,
		RequireStoragePrefix:       true,
	}
	l.add(entry)
	l.snapshotIfDirty(logger)

	reloaded := newOwedLedger(dir, "snap-test", logger)
	got := reloaded.batch(10)
	if len(got) != 1 || got[0] != entry {
		t.Fatalf("reloaded ledger = %+v, want [%+v]", got, entry)
	}

	// A corrupt snapshot must start empty without failing.
	if err := os.WriteFile(filepath.Join(dir, "owed-uploads-snap-test.json"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	corrupt := newOwedLedger(dir, "snap-test", logger)
	if got := corrupt.batch(10); len(got) != 0 {
		t.Fatalf("ledger from corrupt snapshot = %+v, want empty", got)
	}
}

func TestOwedLedgerCapacityRejectsNewDebt(t *testing.T) {
	logger := stdlog.New(&bytes.Buffer{}, "", 0)
	l := newOwedLedger(t.TempDir(), "cap-test", logger)

	before := testutil.ToFloat64(owedRejected.WithLabelValues("cap-test"))
	for i := 0; i < maxOwedEntries; i++ {
		l.entries[uploadKey{Hash: strconv.Itoa(i)}] = owedEntry{}
	}

	newKey := uploadKey{Kind: cache.CAS, Hash: testHash}
	l.add(owedEntry{Key: newKey})
	if _, ok := l.entries[newKey]; ok {
		t.Fatal("full ledger accepted new debt, want rejection")
	}
	if got := testutil.ToFloat64(owedRejected.WithLabelValues("cap-test")) - before; got != 1 {
		t.Fatalf("owed rejected counter delta = %v, want 1", got)
	}

	// Updating an EXISTING key must still succeed at capacity.
	var existing uploadKey
	for k := range l.entries {
		existing = k
		break
	}
	l.add(owedEntry{Key: existing, LogicalSize: 99})
	if l.entries[existing].LogicalSize != 99 {
		t.Fatal("full ledger refused update of existing debt, want acceptance")
	}
}
