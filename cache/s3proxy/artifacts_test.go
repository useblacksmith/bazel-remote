package s3proxy

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/cache/lruflush"

	"github.com/minio/minio-go/v7"
)

// Both proxies must satisfy the flusher's sink contract; a signature drift
// should fail compilation here, not at the main.go type assertion.
var (
	_ lruflush.Sink = (*s3Cache)(nil)
	_ lruflush.Sink = (*multiS3Cache)(nil)
)

func readObject(t *testing.T, c *s3Cache, bucket, key string) []byte {
	t.Helper()
	rc, _, _, err := c.mcore.GetObject(context.Background(), bucket, key, minio.GetObjectOptions{})
	if err != nil {
		t.Fatalf("GetObject(%s, %s): %v", bucket, key, err)
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// TestPutArtifactRoutesByRequestBucket pins the artifact write path to the
// same per-request bucket semantics as cache uploads: the selection's bucket
// when one is attached, the backend default otherwise, key used verbatim.
func TestPutArtifactRoutesByRequestBucket(t *testing.T) {
	c := fakeS3Backend(t, "default-bucket", "tenant-bucket-1")

	ctxTenant := cache.WithS3Backend(context.Background(),
		cache.S3BackendSelection{Endpoint: backendKeyA, Bucket: "tenant-bucket-1"})
	tenantBody := []byte(`{"schema_version":1}` + "\n")
	if err := c.PutArtifact(ctxTenant, "tenant/v1/lru/00000000000000001000-p-1.jsonl", tenantBody); err != nil {
		t.Fatalf("PutArtifact(tenant bucket): %v", err)
	}
	got := readObject(t, c, "tenant-bucket-1", "tenant/v1/lru/00000000000000001000-p-1.jsonl")
	if !bytes.Equal(got, tenantBody) {
		t.Fatalf("artifact body mismatch: %q", got)
	}

	if err := c.PutArtifact(context.Background(), "tenant/v1/lru/00000000000000002000-p-2.jsonl", tenantBody); err != nil {
		t.Fatalf("PutArtifact(default bucket): %v", err)
	}
	readObject(t, c, "default-bucket", "tenant/v1/lru/00000000000000002000-p-2.jsonl")
}

// TestMultiPutArtifactDispatch pins multi-backend routing: the selection's
// endpoint picks the backend, an unknown selector refuses rather than
// guessing a shard, and no selector uses the default backend.
func TestMultiPutArtifactDispatch(t *testing.T) {
	a := fakeS3Backend(t, "default-bucket")
	b := fakeS3Backend(t, "default-bucket")
	m := &multiS3Cache{
		backends: map[string]*s3Cache{"endpoint-a": a, "endpoint-b": b},
		def:      a,
	}
	body := []byte("x\n")

	ctxB := cache.WithS3Backend(context.Background(), cache.S3BackendSelection{Endpoint: "endpoint-b"})
	if err := m.PutArtifact(ctxB, "t/lru/1-b.jsonl", body); err != nil {
		t.Fatalf("PutArtifact via endpoint-b: %v", err)
	}
	readObject(t, b, "default-bucket", "t/lru/1-b.jsonl")
	if _, _, _, err := a.mcore.GetObject(context.Background(), "default-bucket", "t/lru/1-b.jsonl", minio.GetObjectOptions{}); err == nil {
		t.Fatal("artifact leaked to the non-selected backend")
	}

	ctxUnknown := cache.WithS3Backend(context.Background(), cache.S3BackendSelection{Endpoint: "endpoint-nope"})
	if err := m.PutArtifact(ctxUnknown, "t/lru/2-x.jsonl", body); err == nil {
		t.Fatal("PutArtifact with unknown selector must refuse")
	}

	if err := m.PutArtifact(context.Background(), "t/lru/3-d.jsonl", body); err != nil {
		t.Fatalf("PutArtifact selector-less: %v", err)
	}
	readObject(t, a, "default-bucket", "t/lru/3-d.jsonl")
}

// TestPutArtifactRefusesWhenBreakerOpen pins the fail-fast contract: a sick
// shard's artifact flushes must not stack PUTs onto it.
func TestPutArtifactRefusesWhenBreakerOpen(t *testing.T) {
	c := fakeS3Backend(t, "default-bucket")
	tripBreaker(t, c.breaker)
	if err := c.PutArtifact(context.Background(), "t/lru/1-a.jsonl", []byte("x\n")); err == nil {
		t.Fatal("PutArtifact with open breaker must fail fast")
	}
}

// TestArtifactFailuresCannotTripBreaker pins one direction of the
// failure-isolation contract: artifact-only failures (here, a bucket that
// does not exist — the shape of a tagging/policy/IAM gap) must never open
// the data-plane breaker against customer traffic, no matter how many occur.
func TestArtifactFailuresCannotTripBreaker(t *testing.T) {
	c := fakeS3Backend(t, "default-bucket")
	ctxBad := cache.WithS3Backend(context.Background(),
		cache.S3BackendSelection{Endpoint: backendKeyA, Bucket: "no-such-bucket"})
	for i := 0; i < 2*breakerConsecutiveFailures; i++ {
		if err := c.PutArtifact(ctxBad, "t/lru/fail.jsonl", []byte("x\n")); err == nil {
			t.Fatal("PutArtifact to a missing bucket must fail")
		}
	}
	if got := c.breaker.State(); got != breakerClosed {
		t.Fatalf("breaker state after %d artifact failures = %v, want closed", 2*breakerConsecutiveFailures, got)
	}
	// Customer traffic is unaffected.
	if err := c.PutArtifact(context.Background(), "t/lru/ok.jsonl", []byte("x\n")); err != nil {
		t.Fatalf("data plane degraded by artifact failures: %v", err)
	}
}

// TestArtifactSuccessCannotHealFailureStreak pins the other direction: a
// small artifact success must not reset a real data-plane failure streak.
// Four data-plane failures, one successful artifact PUT, then the fifth
// failure — the breaker must open, proving the artifact success recorded
// nothing.
func TestArtifactSuccessCannotHealFailureStreak(t *testing.T) {
	c := fakeS3Backend(t, "default-bucket")
	for i := 0; i < breakerConsecutiveFailures-1; i++ {
		_ = c.breaker.Execute(func() breakerOutcome { return outcomeFailure })
	}
	if err := c.PutArtifact(context.Background(), "t/lru/mid-streak.jsonl", []byte("x\n")); err != nil {
		t.Fatalf("artifact PUT during a not-yet-open streak should succeed: %v", err)
	}
	_ = c.breaker.Execute(func() breakerOutcome { return outcomeFailure })
	if got := c.breaker.State(); got != breakerOpen {
		t.Fatalf("breaker state after 4 failures + artifact success + 1 failure = %v, want open (artifact success must not reset the streak)", got)
	}
}
