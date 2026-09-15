package disk

// Tenant census and byte-weighted age accounting for the disk cache.
//
// The censusRecorder is an IndexObserver: it receives synchronous, O(1)
// callbacks from SizedLRU under the diskCache mutex and maintains
//
//   1. Byte-weighted age metrics exported to Prometheus, labelled by build
//      tool and age bucket only (never by tenant - cardinality):
//      evicted bytes by age-at-eviction (both since-last-read and
//      since-created), never-read evicted bytes, and inter-access ages
//      observed on Get. These are plain CounterVecs with an age_bucket
//      label rather than native histograms, which are observation-weighted:
//      a million evicted 2 KB stamp files must not drown out one evicted
//      5 GB working set.
//
//   2. Per-tenant accumulators (resident bytes/entries, put/hit/evicted
//      deltas, eviction age distributions) snapshotted periodically as a
//      JSONL artifact through the proxy backend's artifact sink. O(tenants)
//      to produce, never O(entries): no index walks, no disk scans. The
//      accumulators are re-baselined at every restart by the boot scan,
//      which already visits every file, so drift never survives a roll.
//
// Tenant identity: LRU keys carry sha256(storage prefix) ("prefix ID"), not
// the raw prefix. The recorder learns prefixID -> raw prefix mappings from
// request contexts (RecordTenant) and resolves them at snapshot/metric time.
// Entries baselined by the boot scan may stay unresolved until the tenant's
// first request after the restart; their metrics use tool="unknown" and
// census rows carry an empty prefix alongside the prefix ID.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/prometheus/client_golang/prometheus"
)

// CensusArtifactSchemaVersion is stamped into every census artifact header.
// Bump it on any breaking change to the JSONL format.
const CensusArtifactSchemaVersion = 1

// CensusArtifactKeyPrefix is the S3 key prefix for census snapshots. It
// lives at the bucket root, outside every tenant storage prefix, so
// tenant-scoped retention sweeps never touch it.
const CensusArtifactKeyPrefix = "census/"

// ArtifactSink uploads small metadata artifacts to the backing store. The
// s3proxy backend satisfies this (same shape as lruflush.Sink).
type ArtifactSink interface {
	PutArtifact(ctx context.Context, key string, body []byte) error
}

// Age bucket edges in seconds. The 72h edge is the Friday-push-Monday-build
// boundary. Label values are aligned with censusBucketLabels.
var censusBucketEdges = [...]uint32{
	3600,       // 1h
	6 * 3600,   // 6h
	12 * 3600,  // 12h
	24 * 3600,  // 24h
	48 * 3600,  // 48h
	72 * 3600,  // 72h
	96 * 3600,  // 96h
	168 * 3600, // 168h
}

var censusBucketLabels = [...]string{
	"1h", "6h", "12h", "24h", "48h", "72h", "96h", "168h", "+Inf",
}

const censusNumBuckets = len(censusBucketEdges) + 1

func censusBucketIdx(ageSeconds uint32) int {
	for i, edge := range censusBucketEdges {
		if ageSeconds <= edge {
			return i
		}
	}
	return censusNumBuckets - 1
}

// tenantAcc accumulates per-tenant counters. Resident values track current
// state; the remaining fields are deltas since the previous snapshot.
type tenantAcc struct {
	rawPrefix string // "" until learned from a request context

	residentBytes   int64
	residentEntries int64

	putBytes   int64
	putEntries int64

	hitBytes int64
	hitCount int64

	evictedBytes         int64
	evictedEntries       int64
	evictedLiveBytes     int64 // last read < censusLiveThreshold before eviction
	evictedNeverReadByes int64 // never read since creation

	evictedByLastReadAge [censusNumBuckets]int64 // bytes
	evictedByCreatedAge  [censusNumBuckets]int64 // bytes
	accessIntervalBytes  [censusNumBuckets]int64 // bytes
}

// censusLiveThreshold is the "still live" boundary used for the per-tenant
// evicted-live-bytes column: bytes evicted less than this long after their
// last read were plausibly still part of someone's working set.
const censusLiveThreshold = 12 * 3600 // seconds

type censusRecorder struct {
	mu sync.Mutex

	nowFn func() uint32

	// prefixID -> accumulator. The zero prefix ID (unprefixed keys, e.g.
	// RAW entries) accumulates under "".
	tenants map[string]*tenantAcc

	windowStart time.Time

	// Prometheus collectors. Labels are {age_kind, age_bucket, tool} /
	// {age_bucket, tool} / {tool}: bounded cardinality, no tenant labels.
	evictedBytesByAge     *prometheus.CounterVec
	accessIntervalByAge   *prometheus.CounterVec
	evictedNeverReadBytes *prometheus.CounterVec
	gaugeRetentionWindow  prometheus.Gauge
}

func newCensusRecorder() *censusRecorder {
	return &censusRecorder{
		nowFn:       unixNow,
		tenants:     make(map[string]*tenantAcc),
		windowStart: time.Now(),

		evictedBytesByAge: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bazel_remote_disk_cache_evicted_bytes_by_age",
			Help: "Bytes evicted from the disk cache, bucketed by age at eviction. age_kind is last_read (time since the entry was last read: evicted still-live bytes are the harm signal) or created (time since the entry was written: write-once bytes dying young are the garbage signal).",
		}, []string{"age_kind", "age_bucket", "tool"}),

		accessIntervalByAge: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bazel_remote_disk_cache_access_interval_bytes",
			Help: "Bytes re-read from the disk cache, bucketed by time since the previous read: each workload's re-read cadence, byte-weighted.",
		}, []string{"age_bucket", "tool"}),

		evictedNeverReadBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bazel_remote_disk_cache_evicted_never_read_bytes",
			Help: "Bytes evicted without ever having been read since creation. High never-read share plus low hit ratio indicates a cache-hostile workload rather than genuine churn.",
		}, []string{"tool"}),

		gaugeRetentionWindow: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "bazel_remote_disk_cache_retention_window_seconds",
			Help: "Age (now - last access) of the LRU tail: the current evicted-after horizon. Computed from in-memory access stamps, so it is meaningful on noatime mounts, but resets at restart.",
		}),
	}
}

func (r *censusRecorder) registerMetrics() {
	prometheus.MustRegister(r.evictedBytesByAge)
	prometheus.MustRegister(r.accessIntervalByAge)
	prometheus.MustRegister(r.evictedNeverReadBytes)
	prometheus.MustRegister(r.gaugeRetentionWindow)
}

// RecordTenant learns the prefixID -> raw prefix mapping from a request
// context. Called on Put/Get request paths (not under the diskCache mutex).
func (r *censusRecorder) RecordTenant(ctx context.Context, key string) {
	prefix, ok := cache.StoragePrefixFromContext(ctx)
	if !ok || prefix == "" {
		return
	}
	_, _, prefixID := lookupKeyParts(key)
	if prefixID == "" {
		return
	}

	r.mu.Lock()
	acc := r.tenant(prefixID)
	if acc.rawPrefix == "" {
		acc.rawPrefix = prefix
	}
	r.mu.Unlock()
}

// tenant returns the accumulator for a prefix ID, creating it if needed.
// r.mu must be held.
func (r *censusRecorder) tenant(prefixID string) *tenantAcc {
	acc, ok := r.tenants[prefixID]
	if !ok {
		acc = &tenantAcc{}
		r.tenants[prefixID] = acc
	}
	return acc
}

// toolForPrefix extracts the build tool from a raw storage prefix. Prefixes
// look like "<env>/<installation>/<repo>/<tool>/" (trailing slash optional).
func toolForPrefix(rawPrefix string) string {
	if rawPrefix == "" {
		return "unknown"
	}
	segs := strings.Split(strings.Trim(rawPrefix, "/"), "/")
	tool := segs[len(segs)-1]
	if tool == "" {
		return "unknown"
	}
	return tool
}

// IndexObserver implementation. All four callbacks fire under the diskCache
// mutex and take only r.mu (a leaf lock): O(1), memory-only.

func (r *censusRecorder) OnAdd(key string, value lruItem) {
	_, _, prefixID := lookupKeyParts(key)

	r.mu.Lock()
	acc := r.tenant(prefixID)
	acc.residentBytes += value.sizeOnDisk
	acc.residentEntries++
	acc.putBytes += value.sizeOnDisk
	acc.putEntries++
	r.mu.Unlock()
}

func (r *censusRecorder) OnOverwrite(key string, oldValue lruItem, newValue lruItem) {
	_, _, prefixID := lookupKeyParts(key)

	r.mu.Lock()
	acc := r.tenant(prefixID)
	acc.residentBytes += newValue.sizeOnDisk - oldValue.sizeOnDisk
	acc.putBytes += newValue.sizeOnDisk
	acc.putEntries++
	r.mu.Unlock()
}

func (r *censusRecorder) OnEvict(key string, value lruItem) {
	_, _, prefixID := lookupKeyParts(key)
	now := r.nowFn()

	lastReadAge := uint32(0)
	if now > value.lastAccess {
		lastReadAge = now - value.lastAccess
	}
	createdAge := uint32(0)
	if now > value.addedAt {
		createdAge = now - value.addedAt
	}
	neverRead := value.lastAccess == value.addedAt
	lastReadIdx := censusBucketIdx(lastReadAge)
	createdIdx := censusBucketIdx(createdAge)

	r.mu.Lock()
	acc := r.tenant(prefixID)
	tool := toolForPrefix(acc.rawPrefix)
	acc.residentBytes -= value.sizeOnDisk
	acc.residentEntries--
	acc.evictedBytes += value.sizeOnDisk
	acc.evictedEntries++
	if lastReadAge < censusLiveThreshold && !neverRead {
		acc.evictedLiveBytes += value.sizeOnDisk
	}
	if neverRead {
		acc.evictedNeverReadByes += value.sizeOnDisk
	}
	acc.evictedByLastReadAge[lastReadIdx] += value.sizeOnDisk
	acc.evictedByCreatedAge[createdIdx] += value.sizeOnDisk
	r.mu.Unlock()

	sz := float64(value.sizeOnDisk)
	r.evictedBytesByAge.WithLabelValues("last_read", censusBucketLabels[lastReadIdx], tool).Add(sz)
	r.evictedBytesByAge.WithLabelValues("created", censusBucketLabels[createdIdx], tool).Add(sz)
	if neverRead {
		r.evictedNeverReadBytes.WithLabelValues(tool).Add(sz)
	}
}

func (r *censusRecorder) OnAccess(key string, value lruItem, prevAccess uint32) {
	_, _, prefixID := lookupKeyParts(key)
	now := value.lastAccess

	interval := uint32(0)
	if now > prevAccess {
		interval = now - prevAccess
	}
	idx := censusBucketIdx(interval)

	r.mu.Lock()
	acc := r.tenant(prefixID)
	tool := toolForPrefix(acc.rawPrefix)
	acc.hitBytes += value.sizeOnDisk
	acc.hitCount++
	acc.accessIntervalBytes[idx] += value.sizeOnDisk
	r.mu.Unlock()

	r.accessIntervalByAge.WithLabelValues(censusBucketLabels[idx], tool).Add(float64(value.sizeOnDisk))
}

// CensusHeader is the first JSONL line of a census artifact.
type CensusHeader struct {
	SchemaVersion int    `json:"schema_version"`
	Host          string `json:"host"`
	WindowStartMs int64  `json:"window_start_ms"`
	WindowEndMs   int64  `json:"window_end_ms"`
	TenantCount   int    `json:"tenant_count"`
}

// CensusRow is one tenant's counters for one snapshot window. Resident
// values are point-in-time; the rest are deltas over the window. Bucketed
// arrays are indexed in censusBucketLabels order.
type CensusRow struct {
	PrefixID  string `json:"prefix_id"`
	RawPrefix string `json:"prefix,omitempty"`

	ResidentBytes   int64 `json:"resident_bytes"`
	ResidentEntries int64 `json:"resident_entries"`

	PutBytes   int64 `json:"put_bytes"`
	PutEntries int64 `json:"put_entries"`

	// Hits count LRU recency bumps: data reads and contains checks
	// (FindMissingBlobs) alike, matching exactly what extends an entry's
	// life in the eviction order.
	HitBytes int64 `json:"hit_bytes"`
	HitCount int64 `json:"hit_count"`

	EvictedBytes          int64 `json:"evicted_bytes"`
	EvictedEntries        int64 `json:"evicted_entries"`
	EvictedLiveBytes      int64 `json:"evicted_live_bytes"`
	EvictedNeverReadBytes int64 `json:"evicted_never_read_bytes"`

	EvictedByLastReadAge []int64 `json:"evicted_by_last_read_age,omitempty"`
	EvictedByCreatedAge  []int64 `json:"evicted_by_created_age,omitempty"`
	AccessIntervalBytes  []int64 `json:"access_interval_bytes,omitempty"`
}

// resetWindowDeltas clears the per-window counters on every tenant while
// keeping resident state. Called once after the boot scan, whose Add loop
// must not count as Put traffic.
func (r *censusRecorder) resetWindowDeltas() {
	r.mu.Lock()
	for _, acc := range r.tenants {
		acc.putBytes, acc.putEntries = 0, 0
		acc.hitBytes, acc.hitCount = 0, 0
		acc.evictedBytes, acc.evictedEntries = 0, 0
		acc.evictedLiveBytes, acc.evictedNeverReadByes = 0, 0
		acc.evictedByLastReadAge = [censusNumBuckets]int64{}
		acc.evictedByCreatedAge = [censusNumBuckets]int64{}
		acc.accessIntervalBytes = [censusNumBuckets]int64{}
	}
	r.windowStart = time.Now()
	r.mu.Unlock()
}

// snapshot copies the current accumulators, resets the per-window deltas,
// and drops empty tenants. It holds r.mu only for the copy (O(tenants)).
func (r *censusRecorder) snapshot(host string) (CensusHeader, []CensusRow) {
	endMs := time.Now().UnixMilli()

	r.mu.Lock()
	startMs := r.windowStart.UnixMilli()
	rows := make([]CensusRow, 0, len(r.tenants))
	for prefixID, acc := range r.tenants {
		row := CensusRow{
			PrefixID:              prefixID,
			RawPrefix:             acc.rawPrefix,
			ResidentBytes:         acc.residentBytes,
			ResidentEntries:       acc.residentEntries,
			PutBytes:              acc.putBytes,
			PutEntries:            acc.putEntries,
			HitBytes:              acc.hitBytes,
			HitCount:              acc.hitCount,
			EvictedBytes:          acc.evictedBytes,
			EvictedEntries:        acc.evictedEntries,
			EvictedLiveBytes:      acc.evictedLiveBytes,
			EvictedNeverReadBytes: acc.evictedNeverReadByes,
		}
		if acc.evictedBytes > 0 {
			row.EvictedByLastReadAge = append([]int64(nil), acc.evictedByLastReadAge[:]...)
			row.EvictedByCreatedAge = append([]int64(nil), acc.evictedByCreatedAge[:]...)
		}
		if acc.hitBytes > 0 {
			row.AccessIntervalBytes = append([]int64(nil), acc.accessIntervalBytes[:]...)
		}
		rows = append(rows, row)

		// Reset window deltas; resident state persists.
		acc.putBytes, acc.putEntries = 0, 0
		acc.hitBytes, acc.hitCount = 0, 0
		acc.evictedBytes, acc.evictedEntries = 0, 0
		acc.evictedLiveBytes, acc.evictedNeverReadByes = 0, 0
		acc.evictedByLastReadAge = [censusNumBuckets]int64{}
		acc.evictedByCreatedAge = [censusNumBuckets]int64{}
		acc.accessIntervalBytes = [censusNumBuckets]int64{}

		// Drop tenants with nothing resident so the map stays bounded.
		if acc.residentEntries <= 0 && acc.residentBytes <= 0 {
			delete(r.tenants, prefixID)
		}
	}
	r.windowStart = time.Now()
	r.mu.Unlock()

	header := CensusHeader{
		SchemaVersion: CensusArtifactSchemaVersion,
		Host:          host,
		WindowStartMs: startMs,
		WindowEndMs:   endMs,
		TenantCount:   len(rows),
	}
	return header, rows
}

// CensusArtifactKey returns the object key for a census snapshot. The
// zero-padded window-end epoch-ms makes lexical (S3) listings chronological.
func CensusArtifactKey(windowEndMs int64, host string) string {
	return fmt.Sprintf("%s%020d-%s.jsonl", CensusArtifactKeyPrefix, windowEndMs, host)
}

// writeCensusArtifact serializes a census snapshot as JSONL: the header on
// the first line, then one CensusRow per line.
func writeCensusArtifact(w io.Writer, header CensusHeader, rows []CensusRow) error {
	enc := json.NewEncoder(w)
	if err := enc.Encode(header); err != nil {
		return err
	}
	for i := range rows {
		if err := enc.Encode(rows[i]); err != nil {
			return err
		}
	}
	return nil
}

// ReadCensusArtifact parses a JSONL census artifact. Provided for Go
// consumers and tests.
func ReadCensusArtifact(r io.Reader) (CensusHeader, []CensusRow, error) {
	var header CensusHeader

	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)

	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			return header, nil, err
		}
		return header, nil, fmt.Errorf("census artifact: empty input")
	}
	if err := json.Unmarshal(sc.Bytes(), &header); err != nil {
		return header, nil, fmt.Errorf("census artifact: bad header: %w", err)
	}

	var rows []CensusRow
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var row CensusRow
		if err := json.Unmarshal(line, &row); err != nil {
			return header, nil, fmt.Errorf("census artifact: bad row: %w", err)
		}
		rows = append(rows, row)
	}
	if err := sc.Err(); err != nil {
		return header, nil, err
	}
	return header, rows, nil
}

// StartCensusSnapshots periodically snapshots the tenant census and uploads
// it as a JSONL artifact through sink. Runs until the process exits. Upload
// failures are logged and the window's deltas are dropped (best-effort:
// census gates dashboards and investigations, not serving).
func (c *diskCache) StartCensusSnapshots(sink ArtifactSink, interval time.Duration, host string) {
	if c.census == nil || sink == nil || interval <= 0 {
		return
	}
	log.Printf("Starting tenant census snapshots every %s", interval)
	go func() {
		ticker := time.NewTicker(interval)
		for range ticker.C {
			header, rows := c.census.snapshot(host)

			var buf strings.Builder
			if err := writeCensusArtifact(&buf, header, rows); err != nil {
				log.Printf("ERROR: failed to serialize census artifact: %v", err)
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			key := CensusArtifactKey(header.WindowEndMs, host)
			if err := sink.PutArtifact(ctx, key, []byte(buf.String())); err != nil {
				log.Printf("ERROR: failed to upload census artifact %s: %v", key, err)
			}
			cancel()
		}
	}()
}
