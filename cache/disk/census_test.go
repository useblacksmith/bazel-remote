package disk

import (
	"context"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
)

func TestCensusBucketIdx(t *testing.T) {
	cases := []struct {
		age  uint32
		want string
	}{
		{0, "1h"},
		{3600, "1h"},
		{3601, "6h"},
		{12 * 3600, "12h"},
		{24*3600 + 1, "48h"},
		{72 * 3600, "72h"},
		{169 * 3600, "+Inf"},
		{^uint32(0), "+Inf"},
	}
	for _, tc := range cases {
		got := censusBucketLabels[censusBucketIdx(tc.age)]
		if got != tc.want {
			t.Errorf("censusBucketIdx(%d) = %s, want %s", tc.age, got, tc.want)
		}
	}
}

// newTestLRUWithCensus builds a SizedLRU wired to a censusRecorder with a
// controllable clock.
func newTestLRUWithCensus(t *testing.T, maxSize int64, clock *uint32) (*SizedLRU, *censusRecorder) {
	t.Helper()
	nowFn := func() uint32 { return *clock }

	rec := newCensusRecorder()
	rec.nowFn = nowFn

	lru := NewSizedLRU(maxSize, func(key string, value lruItem) {}, 0)
	lru.observer = rec
	lru.nowFn = nowFn

	// Drain the eviction queue so appendEvictionToQueue never blocks.
	go lru.performQueuedEvictionsContinuously()

	return &lru, rec
}

const testHashA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const testHashB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func TestCensusAccounting(t *testing.T) {
	clock := uint32(1000)
	lru, rec := newTestLRUWithCensus(t, 100*BlockSize, &clock)

	prefix := "bazelre/staging/42/12345/go/"
	prefixID := cache.StoragePrefixID(prefix)
	keyA := cache.LookupKeyForStoragePrefixID(prefixID, cache.CAS, testHashA)
	keyB := cache.LookupKeyForStoragePrefixID(prefixID, cache.CAS, testHashB)

	// Learn the tenant mapping, as the request paths would.
	ctx := cache.WithStoragePrefix(context.Background(), prefix)
	rec.RecordTenant(ctx, keyA)

	// Put A (1 block) at t=1000.
	if !lru.Add(keyA, lruItem{size: 100, sizeOnDisk: 100}) {
		t.Fatal("Add A failed")
	}

	// Read A two hours later: inter-access age = 2h -> "6h" bucket.
	clock = 1000 + 2*3600
	if item, ele := lru.Get(keyA); ele == nil || item.lastAccess != clock {
		t.Fatalf("Get A: ele=%v lastAccess=%d want %d", ele, item.lastAccess, clock)
	}

	// Put B at t+2h.
	if !lru.Add(keyB, lruItem{size: 200, sizeOnDisk: 200}) {
		t.Fatal("Add B failed")
	}

	// Evict A 13 hours after its last read (15h after creation): B is
	// never read. Shrink via maxEntries: cap at 1 entry by adding a
	// third... simpler: evict explicitly.
	clock = 1000 + 15*3600
	lru.RemoveKey(keyA)

	header, rows := rec.snapshot("test-host")
	if header.TenantCount != 1 {
		t.Fatalf("TenantCount = %d, want 1", header.TenantCount)
	}
	row := rows[0]

	if row.PrefixID != prefixID {
		t.Errorf("PrefixID = %q, want %q", row.PrefixID, prefixID)
	}
	if row.RawPrefix != prefix {
		t.Errorf("RawPrefix = %q, want %q", row.RawPrefix, prefix)
	}
	if row.PutEntries != 2 || row.PutBytes != 300 {
		t.Errorf("Put = %d entries / %d bytes, want 2 / 300", row.PutEntries, row.PutBytes)
	}
	if row.HitCount != 1 || row.HitBytes != 100 {
		t.Errorf("Hit = %d / %d bytes, want 1 / 100", row.HitCount, row.HitBytes)
	}
	// Only B (200 bytes) remains resident.
	if row.ResidentEntries != 1 || row.ResidentBytes != 200 {
		t.Errorf("Resident = %d entries / %d bytes, want 1 / 200", row.ResidentEntries, row.ResidentBytes)
	}
	if row.EvictedEntries != 1 || row.EvictedBytes != 100 {
		t.Errorf("Evicted = %d entries / %d bytes, want 1 / 100", row.EvictedEntries, row.EvictedBytes)
	}
	// A was read (not never-read), and its last read was 13h before
	// eviction: outside the 12h live threshold.
	if row.EvictedNeverReadBytes != 0 {
		t.Errorf("EvictedNeverReadBytes = %d, want 0", row.EvictedNeverReadBytes)
	}
	if row.EvictedLiveBytes != 0 {
		t.Errorf("EvictedLiveBytes = %d, want 0", row.EvictedLiveBytes)
	}
	// last-read age 13h -> "24h" bucket (index 3); created age 15h -> "24h".
	if got := row.EvictedByLastReadAge[3]; got != 100 {
		t.Errorf("EvictedByLastReadAge[24h] = %d, want 100", got)
	}
	if got := row.EvictedByCreatedAge[3]; got != 100 {
		t.Errorf("EvictedByCreatedAge[24h] = %d, want 100", got)
	}
	// inter-access age 2h -> "6h" bucket (index 1).
	if got := row.AccessIntervalBytes[1]; got != 100 {
		t.Errorf("AccessIntervalBytes[6h] = %d, want 100", got)
	}

	// Second snapshot: deltas reset, resident state persists.
	_, rows2 := rec.snapshot("test-host")
	if len(rows2) != 1 {
		t.Fatalf("second snapshot rows = %d, want 1", len(rows2))
	}
	if rows2[0].PutEntries != 0 || rows2[0].EvictedBytes != 0 || rows2[0].HitCount != 0 {
		t.Errorf("second snapshot deltas not reset: %+v", rows2[0])
	}
	if rows2[0].ResidentBytes != 200 {
		t.Errorf("second snapshot ResidentBytes = %d, want 200", rows2[0].ResidentBytes)
	}

	// Evict B: never read since creation.
	clock += 3600
	lru.RemoveKey(keyB)
	_, rows3 := rec.snapshot("test-host")
	if len(rows3) != 1 {
		t.Fatalf("third snapshot rows = %d, want 1", len(rows3))
	}
	if rows3[0].EvictedNeverReadBytes != 200 {
		t.Errorf("EvictedNeverReadBytes = %d, want 200", rows3[0].EvictedNeverReadBytes)
	}
	// B's last read is its creation stamp, 14h before eviction: not live.
	if rows3[0].EvictedLiveBytes != 0 {
		t.Errorf("EvictedLiveBytes = %d, want 0", rows3[0].EvictedLiveBytes)
	}
	if rows3[0].ResidentBytes != 0 || rows3[0].ResidentEntries != 0 {
		t.Errorf("Resident after evicting all = %d bytes / %d entries, want 0/0",
			rows3[0].ResidentBytes, rows3[0].ResidentEntries)
	}

	// Fourth snapshot: the empty tenant was dropped.
	header4, rows4 := rec.snapshot("test-host")
	if header4.TenantCount != 0 || len(rows4) != 0 {
		t.Errorf("fourth snapshot = %d tenants, want 0", header4.TenantCount)
	}
}

func TestCensusEvictedLiveBytes(t *testing.T) {
	clock := uint32(5000)
	lru, rec := newTestLRUWithCensus(t, 100*BlockSize, &clock)

	key := cache.LookupKey(cache.CAS, testHashA)
	if !lru.Add(key, lruItem{size: 50, sizeOnDisk: 50}) {
		t.Fatal("Add failed")
	}

	// Read one hour after creation, evict one hour after the read:
	// still-live eviction.
	clock += 3600
	lru.Get(key)
	clock += 3600
	lru.RemoveKey(key)

	_, rows := rec.snapshot("h")
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if rows[0].EvictedLiveBytes != 50 {
		t.Errorf("EvictedLiveBytes = %d, want 50", rows[0].EvictedLiveBytes)
	}
	if rows[0].EvictedNeverReadBytes != 0 {
		t.Errorf("EvictedNeverReadBytes = %d, want 0", rows[0].EvictedNeverReadBytes)
	}
	// Unprefixed key accumulates under the empty prefix ID.
	if rows[0].PrefixID != "" {
		t.Errorf("PrefixID = %q, want empty", rows[0].PrefixID)
	}
}

func TestCensusOverwrite(t *testing.T) {
	clock := uint32(9000)
	lru, rec := newTestLRUWithCensus(t, 100*BlockSize, &clock)

	key := cache.LookupKey(cache.CAS, testHashA)
	if !lru.Add(key, lruItem{size: 100, sizeOnDisk: 100, random: "1"}) {
		t.Fatal("first Add failed")
	}
	if !lru.Add(key, lruItem{size: 300, sizeOnDisk: 300, random: "2"}) {
		t.Fatal("overwrite Add failed")
	}

	_, rows := rec.snapshot("h")
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if rows[0].ResidentBytes != 300 || rows[0].ResidentEntries != 1 {
		t.Errorf("Resident = %d bytes / %d entries, want 300 / 1",
			rows[0].ResidentBytes, rows[0].ResidentEntries)
	}
	if rows[0].PutBytes != 400 || rows[0].PutEntries != 2 {
		t.Errorf("Put = %d bytes / %d entries, want 400 / 2",
			rows[0].PutBytes, rows[0].PutEntries)
	}
	// Overwrites are not evictions.
	if rows[0].EvictedBytes != 0 {
		t.Errorf("EvictedBytes = %d, want 0", rows[0].EvictedBytes)
	}
}

func TestToolForPrefix(t *testing.T) {
	cases := []struct{ prefix, want string }{
		{"", "unknown"},
		{"bazelre/prod/42/999/go/", "go"},
		{"bazelre/prod/42/999/bazel", "bazel"},
		{"/", "unknown"},
	}
	for _, tc := range cases {
		if got := toolForPrefix(tc.prefix); got != tc.want {
			t.Errorf("toolForPrefix(%q) = %q, want %q", tc.prefix, got, tc.want)
		}
	}
}
