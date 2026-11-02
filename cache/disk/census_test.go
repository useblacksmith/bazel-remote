package disk

import (
	"context"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	// Entry-count twins: B is one never-read entry; A was one previously-read
	// entry in the 24h last_read band (evicted 14h after its last read).
	if got := testutil.ToFloat64(rec.evictedNeverReadEntries.WithLabelValues("go")); got != 1 {
		t.Errorf("evictedNeverReadEntries = %v, want 1", got)
	}
	if got := testutil.ToFloat64(rec.evictedEntriesByAge.WithLabelValues("last_read", "24h", "go")); got != 1 {
		t.Errorf("evictedEntriesByAge last_read 24h = %v, want 1", got)
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

func TestCensusClassIdx(t *testing.T) {
	cases := []struct {
		kind cache.EntryKind
		size int64
		want string
	}{
		{cache.AC, 0, "ac/4k"},
		{cache.AC, 4 << 10, "ac/4k"},
		{cache.RAW, 4<<10 + 1, "ac/64k"},
		{cache.CAS, 100, "cas/4k"},
		{cache.CAS, 1 << 20, "cas/1m"},
		{cache.CAS, 1<<20 + 1, "cas/16m"},
		{cache.CAS, 200 << 20, "cas/256m"},
		{cache.CAS, 1 << 30, "cas/+Inf"},
	}
	for _, tc := range cases {
		got := censusClassLabels[censusClassIdx(tc.kind, tc.size)]
		if got != tc.want {
			t.Errorf("censusClassIdx(%v, %d) = %s, want %s", tc.kind, tc.size, got, tc.want)
		}
	}
}

func TestCensusWallBucketIdx(t *testing.T) {
	cases := []struct {
		ms   int64
		want string
	}{
		{1, "100ms"},
		{100, "100ms"},
		{101, "1s"},
		{5000, "10s"},
		{60000, "60s"},
		{60001, "+Inf"},
	}
	for _, tc := range cases {
		if got := censusWallLabels[censusWallBucketIdx(tc.ms)]; got != tc.want {
			t.Errorf("censusWallBucketIdx(%d) = %s, want %s", tc.ms, got, tc.want)
		}
	}
}

func TestCensusClassAccounting(t *testing.T) {
	clock := uint32(1000)
	lru, rec := newTestLRUWithCensus(t, 1000*BlockSize, &clock)

	casKey := cache.LookupKey(cache.CAS, testHashA)
	acKey := cache.LookupKey(cache.AC, testHashB)
	casClass := censusClassIdx(cache.CAS, 100)
	acClass := censusClassIdx(cache.AC, 200<<10)

	if !lru.Add(casKey, lruItem{size: 100, sizeOnDisk: 100}) {
		t.Fatal("Add CAS failed")
	}
	if !lru.Add(acKey, lruItem{size: 200 << 10, sizeOnDisk: 200 << 10}) {
		t.Fatal("Add AC failed")
	}

	// Both resident and never read.
	rec.mu.Lock()
	acc := rec.tenant("")
	if acc.residentBytesByClass[casClass] != 100 || acc.residentNeverReadBytesByClass[casClass] != 100 {
		t.Errorf("cas class resident/never-read = %d/%d, want 100/100",
			acc.residentBytesByClass[casClass], acc.residentNeverReadBytesByClass[casClass])
	}
	if acc.residentEntriesByClass[acClass] != 1 || acc.residentNeverReadBytesByClass[acClass] != 200<<10 {
		t.Errorf("ac class entries/never-read = %d/%d, want 1/%d",
			acc.residentEntriesByClass[acClass], acc.residentNeverReadBytesByClass[acClass], 200<<10)
	}
	rec.mu.Unlock()

	// Read the CAS blob twice in the same second: moves out of never-read exactly once.
	lru.Get(casKey)
	lru.Get(casKey)

	rec.mu.Lock()
	if acc.residentNeverReadBytesByClass[casClass] != 0 || acc.residentBytesByClass[casClass] != 100 {
		t.Errorf("after reads: cas never-read/resident = %d/%d, want 0/100",
			acc.residentNeverReadBytesByClass[casClass], acc.residentBytesByClass[casClass])
	}
	if acc.hitBytesByClass[casClass] != 200 {
		t.Errorf("hitBytesByClass = %d, want 200", acc.hitBytesByClass[casClass])
	}
	rec.mu.Unlock()

	// Evict both: only the never-read AC counts as evicted-never-read.
	lru.RemoveKey(casKey)
	lru.RemoveKey(acKey)

	_, rows := rec.snapshot("h")
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	row := rows[0]
	if row.EvictedNeverReadBytesByClass[casClass] != 0 || row.EvictedNeverReadBytesByClass[acClass] != 200<<10 {
		t.Errorf("EvictedNeverReadBytesByClass cas/ac = %d/%d, want 0/%d",
			row.EvictedNeverReadBytesByClass[casClass], row.EvictedNeverReadBytesByClass[acClass], 200<<10)
	}
	for i, v := range row.ResidentBytesByClass {
		if v != 0 || row.ResidentEntriesByClass[i] != 0 || row.ResidentNeverReadBytesByClass[i] != 0 {
			t.Errorf("class %s still resident after evictions: %d bytes / %d entries / %d never-read",
				censusClassLabels[i], v, row.ResidentEntriesByClass[i], row.ResidentNeverReadBytesByClass[i])
		}
	}
	if row.PutBytesByClass[casClass] != 100 || row.PutBytesByClass[acClass] != 200<<10 {
		t.Errorf("PutBytesByClass cas/ac = %d/%d", row.PutBytesByClass[casClass], row.PutBytesByClass[acClass])
	}
}

func TestCensusBootScannedIsUnknownNotNeverRead(t *testing.T) {
	clock := uint32(5000)
	lru, rec := newTestLRUWithCensus(t, 100*BlockSize, &clock)

	key := cache.LookupKey(cache.CAS, testHashA)
	class := censusClassIdx(cache.CAS, 50)
	// As the boot scan seeds entries: timestamps from file metadata, state unknown.
	if !lru.Add(key, lruItem{size: 50, sizeOnDisk: 50, addedAt: 4000, lastAccess: 4000, accessState: itemStateBootScanned}) {
		t.Fatal("Add failed")
	}

	rec.mu.Lock()
	acc := rec.tenant("")
	if acc.residentUnknownBytesByClass[class] != 50 || acc.residentNeverReadBytesByClass[class] != 0 {
		t.Errorf("unknown/never-read = %d/%d, want 50/0",
			acc.residentUnknownBytesByClass[class], acc.residentNeverReadBytesByClass[class])
	}
	rec.mu.Unlock()

	lru.Get(key)

	rec.mu.Lock()
	if acc.residentUnknownBytesByClass[class] != 0 {
		t.Errorf("unknown after read = %d, want 0", acc.residentUnknownBytesByClass[class])
	}
	rec.mu.Unlock()

	// Add another boot-scanned entry and evict it untouched: it is not a
	// never-read eviction in the class accounting (its history is unknown).
	keyB := cache.LookupKey(cache.CAS, testHashB)
	if !lru.Add(keyB, lruItem{size: 50, sizeOnDisk: 50, addedAt: 4000, lastAccess: 4000, accessState: itemStateBootScanned}) {
		t.Fatal("Add B failed")
	}
	lru.RemoveKey(keyB)

	_, rows := rec.snapshot("h")
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if rows[0].EvictedNeverReadBytesByClass[class] != 0 {
		t.Errorf("EvictedNeverReadBytesByClass = %d, want 0", rows[0].EvictedNeverReadBytesByClass[class])
	}
	// The scalar and its counters follow the same access state: a boot-scanned
	// entry has addedAt == lastAccess, which must not read as never-read.
	if rows[0].EvictedNeverReadBytes != 0 || rows[0].EvictedBytes != 50 {
		t.Errorf("EvictedNeverReadBytes/EvictedBytes = %d/%d, want 0/50", rows[0].EvictedNeverReadBytes, rows[0].EvictedBytes)
	}
	if got := testutil.ToFloat64(rec.evictedNeverReadEntries.WithLabelValues("unknown")); got != 0 {
		t.Errorf("evictedNeverReadEntries = %v, want 0", got)
	}
	// Nor as read: its lastAccess is file atime, not a hit.
	if rows[0].EvictedLiveBytes != 0 {
		t.Errorf("EvictedLiveBytes = %d, want 0", rows[0].EvictedLiveBytes)
	}
	for i, v := range rows[0].EvictedByLastReadAge {
		if v != 0 {
			t.Errorf("EvictedByLastReadAge[%d] = %d, want 0", i, v)
		}
	}
	if rows[0].ResidentUnknownBytesByClass[class] != 0 || rows[0].ResidentBytesByClass[class] != 50 {
		t.Errorf("resident unknown/total = %d/%d, want 0/50",
			rows[0].ResidentUnknownBytesByClass[class], rows[0].ResidentBytesByClass[class])
	}
}

func TestCensusActionWallTime(t *testing.T) {
	rec := newCensusRecorder()

	prefix := "bazelre/prod/42/12345/bazel/"
	prefixID := cache.StoragePrefixID(prefix)
	key := cache.LookupKeyForStoragePrefixID(prefixID, cache.AC, testHashA)

	withTimes := func(ms int64) *pb.ActionResult {
		start := time.Unix(1_700_000_000, 0)
		return &pb.ActionResult{ExecutionMetadata: &pb.ExecutedActionMetadata{
			ExecutionStartTimestamp:     timestamppb.New(start),
			ExecutionCompletedTimestamp: timestamppb.New(start.Add(time.Duration(ms) * time.Millisecond)),
		}}
	}

	rec.RecordACHit(key, withTimes(50))       // 100ms bucket
	rec.RecordACHit(key, withTimes(2500))     // 10s bucket
	rec.RecordACHit(key, &pb.ActionResult{})  // no metadata
	rec.RecordACWrite(key, withTimes(90_000)) // +Inf bucket
	rec.RecordACWrite(key, &pb.ActionResult{ExecutionMetadata: &pb.ExecutedActionMetadata{
		ExecutionStartTimestamp: timestamppb.New(time.Unix(1, 0)),
	}}) // half-stamped: no metadata
	rec.RecordACWrite(key, nil) // capture skipped: no metadata

	_, rows := rec.snapshot("h")
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	row := rows[0]
	if row.PrefixID != prefixID {
		t.Errorf("PrefixID = %q, want %q", row.PrefixID, prefixID)
	}
	wantHitMs := []int64{50, 0, 2500, 0, 0}
	wantHitN := []int64{1, 0, 1, 0, 0}
	wantWriteMs := []int64{0, 0, 0, 0, 90_000}
	wantWriteN := []int64{0, 0, 0, 0, 1}
	for i := range wantHitMs {
		if row.ACHitWallMsByBucket[i] != wantHitMs[i] || row.ACHitCountByBucket[i] != wantHitN[i] {
			t.Errorf("hit bucket %s = %d ms / %d, want %d / %d", censusWallLabels[i],
				row.ACHitWallMsByBucket[i], row.ACHitCountByBucket[i], wantHitMs[i], wantHitN[i])
		}
		if row.ACWriteWallMsByBucket[i] != wantWriteMs[i] || row.ACWriteCountByBucket[i] != wantWriteN[i] {
			t.Errorf("write bucket %s = %d ms / %d, want %d / %d", censusWallLabels[i],
				row.ACWriteWallMsByBucket[i], row.ACWriteCountByBucket[i], wantWriteMs[i], wantWriteN[i])
		}
	}
	if row.ACHitNoMetadata != 1 || row.ACWriteNoMetadata != 2 {
		t.Errorf("NoMetadata hit/write = %d/%d, want 1/2", row.ACHitNoMetadata, row.ACWriteNoMetadata)
	}

	// Window deltas reset; the tenant had nothing resident so it is dropped.
	_, rows = rec.snapshot("h")
	if len(rows) != 0 {
		t.Errorf("second snapshot rows = %d, want 0", len(rows))
	}
}
