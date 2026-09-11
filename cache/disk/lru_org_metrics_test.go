package disk

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
	"unsafe"

	"github.com/buchgr/bazel-remote/v2/cache"
	testutils "github.com/buchgr/bazel-remote/v2/utils"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func orgBytesValue(t *testing.T, lru *SizedLRU, org string) int64 {
	t.Helper()
	// Read the internal map rather than the GaugeVec: WithLabelValues
	// would re-create series that were deleted when an org hit zero.
	return lru.orgBytes[org]
}

func checkOrgGauge(t *testing.T, lru *SizedLRU, org string, expected int64) {
	t.Helper()
	if got := orgBytesValue(t, lru, org); got != expected {
		t.Fatalf("orgBytes[%q]: expected %d, got %d", org, expected, got)
	}
	if expected != 0 {
		if got := testutil.ToFloat64(lru.gaugeOrgBytes.WithLabelValues(org)); got != float64(expected) {
			t.Fatalf("gaugeOrgBytes{org=%q}: expected %d, got %f", org, expected, got)
		}
	}
}

func checkResidenceBucket(t *testing.T, lru *SizedLRU, le string, expected int64) {
	t.Helper()
	// All bucket series are pre-created in NewSizedLRU, so WithLabelValues
	// never creates a series here.
	got := testutil.ToFloat64(lru.counterEvictedBytesByResidenceAge.WithLabelValues(le))
	if got != float64(expected) {
		t.Fatalf("evicted_bytes_by_residence_age{le=%q}: expected %d, got %f", le, expected, got)
	}
}

func TestOrgBytesAccounting(t *testing.T) {
	lru := NewSizedLRU(2*BlockSize, nil, 0)

	// Insert for two orgs.
	if !lru.Add("k1", lruItem{size: 100, sizeOnDisk: 100, org: "org-a"}) {
		t.Fatal("Add k1 failed")
	}
	if !lru.Add("k2", lruItem{size: 200, sizeOnDisk: 200, org: "org-b"}) {
		t.Fatal("Add k2 failed")
	}
	checkOrgGauge(t, &lru, "org-a", 100)
	checkOrgGauge(t, &lru, "org-b", 200)

	// Overwrite k2 with a different org: org-b returns to zero (series
	// deleted), org-c picks up the new bytes.
	if !lru.Add("k2", lruItem{size: 300, sizeOnDisk: 300, org: "org-c"}) {
		t.Fatal("overwrite k2 failed")
	}
	checkOrgGauge(t, &lru, "org-b", 0)
	checkOrgGauge(t, &lru, "org-c", 300)
	if n := testutil.CollectAndCount(lru.gaugeOrgBytes); n != 2 {
		t.Fatalf("expected 2 org series (org-a, org-c), got %d", n)
	}

	// LRU-pressure eviction: k1 is at the back (k2 was moved to the front
	// by the overwrite), and a one-block insert pushes the cache over its
	// two-block budget.
	if !lru.Add("k3", lruItem{size: BlockSize, sizeOnDisk: BlockSize, org: "org-c"}) {
		t.Fatal("Add k3 failed")
	}
	checkOrgGauge(t, &lru, "org-a", 0)
	checkOrgGauge(t, &lru, "org-c", 300+BlockSize)

	// An item without an org is attributed to "unknown".
	lru.RemoveKey("k2")
	if !lru.Add("k4", lruItem{size: 50, sizeOnDisk: 50}) {
		t.Fatal("Add k4 failed")
	}
	checkOrgGauge(t, &lru, orgUnknown, 50)

	// Draining the cache deletes every org series.
	lru.RemoveKey("k3")
	lru.RemoveKey("k4")
	if len(lru.orgBytes) != 0 {
		t.Fatalf("expected empty orgBytes, got %v", lru.orgBytes)
	}
	if n := testutil.CollectAndCount(lru.gaugeOrgBytes); n != 0 {
		t.Fatalf("expected 0 org series after drain, got %d", n)
	}
}

func TestOrgStringInterning(t *testing.T) {
	lru := NewSizedLRU(10*BlockSize, nil, 0)

	// Two equal org strings with distinct backing arrays.
	raw := []byte("acme-corp")
	org1 := string(raw)
	org2 := string(raw)

	if !lru.Add("k1", lruItem{size: 1, sizeOnDisk: 1, org: org1}) {
		t.Fatal("Add k1 failed")
	}
	if !lru.Add("k2", lruItem{size: 1, sizeOnDisk: 1, org: org2}) {
		t.Fatal("Add k2 failed")
	}

	if len(lru.orgIntern) != 1 {
		t.Fatalf("expected 1 interned org, got %d", len(lru.orgIntern))
	}

	i1, ok := lru.Peek("k1")
	if !ok {
		t.Fatal("Get k1 failed")
	}
	i2, ok := lru.Peek("k2")
	if !ok {
		t.Fatal("Get k2 failed")
	}
	if unsafe.StringData(i1.org) != unsafe.StringData(i2.org) {
		t.Fatal("expected both items to share one interned org string allocation")
	}
}

func TestEvictedResidenceAgeBuckets(t *testing.T) {
	lru := NewSizedLRU(10*BlockSize, nil, 0)
	now := time.Now().Unix()

	// Resident for ~2h: counted in every bucket except le="3600".
	if !lru.Add("twoHours", lruItem{size: 5000, sizeOnDisk: 5000, org: "a", addedAt: now - 7200}) {
		t.Fatal("Add failed")
	}
	lru.RemoveKey("twoHours") // Removal shares removeElement with LRU-pressure eviction.

	checkResidenceBucket(t, &lru, "3600", 0)
	checkResidenceBucket(t, &lru, "21600", 5000)
	checkResidenceBucket(t, &lru, "86400", 5000)
	checkResidenceBucket(t, &lru, "259200", 5000)
	checkResidenceBucket(t, &lru, "+Inf", 5000)

	// Resident for ~100000s (between 24h and 72h).
	if !lru.Add("old", lruItem{size: 5000, sizeOnDisk: 5000, org: "a", addedAt: now - 100000}) {
		t.Fatal("Add failed")
	}
	lru.RemoveKey("old")

	checkResidenceBucket(t, &lru, "3600", 0)
	checkResidenceBucket(t, &lru, "21600", 5000)
	checkResidenceBucket(t, &lru, "86400", 5000)
	checkResidenceBucket(t, &lru, "259200", 10000)
	checkResidenceBucket(t, &lru, "+Inf", 10000)

	// A fresh entry evicted by genuine LRU pressure lands in every bucket.
	if !lru.Add("fresh", lruItem{size: 3000, sizeOnDisk: 3000, org: "a", addedAt: now}) {
		t.Fatal("Add failed")
	}
	if !lru.Add("bulk", lruItem{size: 10 * BlockSize, sizeOnDisk: 10 * BlockSize, org: "a", addedAt: now}) {
		t.Fatal("Add failed")
	}

	checkResidenceBucket(t, &lru, "3600", 3000)
	checkResidenceBucket(t, &lru, "21600", 8000)
	checkResidenceBucket(t, &lru, "86400", 8000)
	checkResidenceBucket(t, &lru, "259200", 13000)
	checkResidenceBucket(t, &lru, "+Inf", 13000)
}

func TestPutPathOrgAttribution(t *testing.T) {
	cacheDir := testutils.TempDir(t)
	defer os.RemoveAll(cacheDir)

	testCacheI, err := New(cacheDir, 100*BlockSize, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	testCache := testCacheI.(*diskCache)

	ctx := context.Background()

	// InstallationID takes precedence.
	ctxInstall := cache.WithMetricsLabels(ctx, cache.MetricsLabels{InstallationID: "org-123"})
	dataA, hashA := testutils.RandomDataAndHash(1024)
	if err := testCache.Put(ctxInstall, cache.RAW, hashA, 1024, bytes.NewReader(dataA)); err != nil {
		t.Fatal(err)
	}
	checkOrgGauge(t, &testCache.lru, "org-123", 1024)

	// Without labels, the first path segment of the storage prefix is used.
	ctxPrefix := cache.WithStoragePrefix(ctx, "installation-77/production/us-east-1/42/repo/v0")
	dataB, hashB := testutils.RandomDataAndHash(2048)
	if err := testCache.Put(ctxPrefix, cache.AC, hashB, 2048, bytes.NewReader(dataB)); err != nil {
		t.Fatal(err)
	}
	checkOrgGauge(t, &testCache.lru, "installation-77", 2048)

	// With neither, bytes are attributed to "unknown".
	dataC, hashC := testutils.RandomDataAndHash(512)
	if err := testCache.Put(ctx, cache.RAW, hashC, 512, bytes.NewReader(dataC)); err != nil {
		t.Fatal(err)
	}
	checkOrgGauge(t, &testCache.lru, orgUnknown, 512)
}

func TestLoadPathOrgUnknownAndMtimeAge(t *testing.T) {
	cacheDir := testutils.TempDir(t)
	defer os.RemoveAll(cacheDir)

	testCacheI, err := New(cacheDir, 100*BlockSize, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	testCache := testCacheI.(*diskCache)

	ctxInstall := cache.WithMetricsLabels(context.Background(), cache.MetricsLabels{InstallationID: "org-123"})
	data, hash := testutils.RandomDataAndHash(1024)
	if err := testCache.Put(ctxInstall, cache.RAW, hash, 1024, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	checkOrgGauge(t, &testCache.lru, "org-123", 1024)

	// Backdate the file's mtime by two hours; the reloaded entry must
	// derive addedAt from it.
	matches, err := filepath.Glob(filepath.Join(cacheDir, "raw.v2", hash[:2], hash+"-*"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("expected exactly one blob file, got %v (err: %v)", matches, err)
	}
	backdated := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(matches[0], backdated, backdated); err != nil {
		t.Fatal(err)
	}

	restartedCacheI, err := New(cacheDir, 100*BlockSize, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	restartedCache := restartedCacheI.(*diskCache)

	// Attribution is lost across restarts: everything lands in "unknown".
	checkOrgGauge(t, &restartedCache.lru, "org-123", 0)
	checkOrgGauge(t, &restartedCache.lru, orgUnknown, 1024)

	key := "raw/" + hash
	item, ok := restartedCache.lru.Peek(key)
	if !ok {
		t.Fatal("expected reloaded item to be present")
	}
	if item.addedAt != backdated.Unix() {
		t.Fatalf("addedAt: expected mtime %d, got %d", backdated.Unix(), item.addedAt)
	}

	// Evicting the reloaded entry uses the mtime-derived age: two hours
	// old, so every bucket except le="3600" counts it.
	restartedCache.mu.Lock()
	restartedCache.lru.RemoveKey(key)
	restartedCache.mu.Unlock()

	checkResidenceBucket(t, &restartedCache.lru, "3600", 0)
	checkResidenceBucket(t, &restartedCache.lru, "21600", 1024)
	checkResidenceBucket(t, &restartedCache.lru, "+Inf", 1024)
}
