// Package lruflush buffers AC-access observations (cache.LRUObserver) in
// memory and periodically flushes them as JSONL artifacts to the S3 backend
// serving the observed tenant, using the artifact schema and key layout
// defined in the cache package (the same shape the web-side retention sweep
// already consumes — no consumer changes).
//
// The design is deliberately minimal: one aggregation map, one flush
// goroutine, direct PUTs. Everything here is advisory — the artifact stream
// informs retention decisions days out, so the correct response to any
// pressure (full buffer, slow backend, failed upload) is to drop the
// observations, count the drop, and move on. The next access to the same
// entries re-establishes the recency signal. There are no queues, workers,
// or retries to tune, and by construction nothing here can stall or fail a
// cache request. Loss bound: a crash or drop loses at most one flush
// interval of advisory observations — bounded minutes against retention
// horizons of days.
package lruflush

import (
	"bytes"
	"context"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	// defaultFlushInterval bounds observation staleness. On the L1 the
	// ticker is the primary flush trigger (no teardown signal), so it also
	// bounds crash loss.
	defaultFlushInterval = 5 * time.Minute
	// uploadTimeout caps a single artifact PUT. Flushing runs on its own
	// goroutine, so this bounds how long one slow tenant backend can delay
	// the other tenants' artifacts within a window, not a request.
	uploadTimeout = 30 * time.Second

	// capObjects is the process-wide bound on buffered object REFERENCES
	// (AC entries + CAS leaf references; a leaf under two ACs counts
	// twice, because both buffered memory and serialized artifact size are
	// proportional to references). Observations past the cap are dropped
	// whole and counted; the periodic flush clears the buffers, so this is
	// pure OOM defense, expected to fire ~never (per-tenant touch-sets are
	// kilobytes). Because the flush loop detaches buffers before
	// uploading, peak live memory is one detached window plus one filling
	// window: ~2x this cap's worth of references, ~85 B each.
	capObjects = 250_000
	// capClosureLeaves drops a single closure whose leaf list alone would
	// dominate an artifact: the consumer reads one closure per JSONL line
	// with a 16 MiB line limit (~190k leaves at ~85 B/ref), so 50k keeps a
	// ~3.5x margin. Dropped WHOLE, never truncated — truncation would break
	// complete-or-drop (the sweep would keep the AC but evict untracked
	// leaves, serving broken action results). Advisory recency loss only.
	capClosureLeaves = 50_000
)

// Flush triggers (label values for bazel_remote_lru_artifact_flush_total).
const (
	triggerPeriodic = "periodic"
	triggerShutdown = "shutdown"
)

// Flush results (label values for bazel_remote_lru_artifact_flush_total).
const (
	resultSuccess = "success"
	resultFailure = "failure"
)

// Drop reasons (label values for
// bazel_remote_lru_flush_observations_dropped_total).
const (
	dropReasonMissingPrefix   = "missing_prefix"
	dropReasonBufferFull      = "buffer_full"
	dropReasonClosureTooLarge = "closure_too_large"
)

var (
	flushTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bazel_remote_lru_artifact_flush_total",
		Help: "LRU observation artifact flushes by trigger and result (one artifact per tenant prefix per window; failures drop the window's observations).",
	}, []string{"trigger", "result"})
	// Distinct from the capture stage's
	// bazel_remote_lru_observations_dropped_total (cache/disk): that counts
	// closures never emitted (incomplete capture); this counts emitted
	// closures the flusher refused to buffer.
	droppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bazel_remote_lru_flush_observations_dropped_total",
		Help: "Emitted AC-access observations dropped before buffering (advisory recency loss, never a cache error).",
	}, []string{"reason"})
	bufferedObjects = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bazel_remote_lru_flush_buffered_objects",
		Help: "Object references (AC entries + CAS leaves) currently buffered awaiting the next flush.",
	})
	flushBytes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "bazel_remote_lru_artifact_flush_bytes",
		Help:    "Serialized size of successfully flushed LRU artifacts.",
		Buckets: prometheus.ExponentialBuckets(1024, 4, 10),
	})
	flushEntries = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "bazel_remote_lru_artifact_flush_entries",
		Help:    "AC closure count of successfully flushed LRU artifacts.",
		Buckets: prometheus.ExponentialBuckets(1, 4, 10),
	})
)

// Sink writes one serialized artifact under a fully-composed object key,
// routing by the request-scoped backend selection on ctx (both s3proxy
// proxies implement this). This is the single owner of the artifact-write
// contract. Implementations must not let artifact traffic influence cache
// request behavior (see s3Cache.PutArtifact for the breaker stance).
type Sink interface {
	PutArtifact(ctx context.Context, key string, body []byte) error
}

// closureBuf accumulates one AC entry within a window. Leaves are a
// hash→sizeOnDisk map so a closure's shared leaves dedup within the entry
// (cross-closure dedup is the sweep's job).
type closureBuf struct {
	acSizeOnDisk int64
	lastAccessMs int64
	leaves       map[string]int64
}

// prefixBuffer accumulates AC closures for one storage prefix (which already
// encodes install/repo/generation) since that prefix's last flush.
type prefixBuffer struct {
	// selection is the tenant's backend routing pair, captured from the
	// request context and refreshed on every observation: artifacts must
	// land next to the objects they describe, or the sweep (which follows
	// the namespace row's pin) never sees them.
	selection     cache.S3BackendSelection
	windowStartMs int64
	// order preserves first-seen order of AC hashes so artifact lines stay
	// in access order without a sort.
	order   []string
	entries map[string]*closureBuf
}

// Flusher implements cache.LRUObserver: it buffers AC-access observations per
// storage prefix, dedups/unions them by AC hash, and serially flushes one
// JSONL artifact per prefix through the Sink on a periodic ticker and at
// shutdown.
type Flusher struct {
	sink      Sink
	interval  time.Duration
	host      string
	processID string
	uniqSeq   atomic.Uint64

	mu      sync.Mutex
	buffers map[string]*prefixBuffer
	objects int // buffered object references across all buffers, vs capObjects

	stop     chan struct{}
	stopOnce sync.Once
	loopDone chan struct{}
}

// Option adjusts test-relevant knobs; production uses the defaults.
type Option func(*Flusher)

// WithFlushInterval overrides the periodic flush interval.
func WithFlushInterval(d time.Duration) Option {
	return func(f *Flusher) { f.interval = d }
}

// New returns a Flusher writing through sink. Call Start to begin the
// periodic flush loop and Drain exactly once at shutdown.
func New(sink Sink, options ...Option) *Flusher {
	host, err := os.Hostname()
	if err != nil {
		host = ""
	}
	f := &Flusher{
		sink:      sink,
		interval:  defaultFlushInterval,
		host:      host,
		processID: uuid.NewString(),
		buffers:   map[string]*prefixBuffer{},
		stop:      make(chan struct{}),
		loopDone:  make(chan struct{}),
	}
	for _, opt := range options {
		opt(f)
	}
	return f
}

// RecordACAccess buffers one AC-access observation. It performs only bounded,
// round-trip-free in-memory work; serialization and the upload happen on the
// flush goroutine. Only trusted-mode requests carry a storage prefix, so a
// missing prefix (HTTP side door, RAW entries) is dropped — those paths have
// no tenant keyspace for an artifact to describe.
func (f *Flusher) RecordACAccess(ctx context.Context, closure cache.ACClosure) {
	if f == nil {
		return
	}
	prefix, ok := cache.StoragePrefixFromContext(ctx)
	if !ok {
		droppedTotal.WithLabelValues(dropReasonMissingPrefix).Inc()
		return
	}
	// Whole-closure guard: one closure past this would dominate an artifact
	// and can exceed the consumer's per-line read limit. Dropped WHOLE,
	// never truncated (see capClosureLeaves).
	if len(closure.Leaves) > capClosureLeaves {
		droppedTotal.WithLabelValues(dropReasonClosureTooLarge).Inc()
		return
	}
	selection, _ := cache.S3BackendFromContext(ctx)

	f.mu.Lock()
	defer f.mu.Unlock()
	// Full-buffer policy is drop, not flush: an emergency flush under
	// pressure is exactly when the backend is least likely to absorb it,
	// and the recency signal re-establishes itself on the next access.
	// The cap is checked against the worst case for this observation (one
	// AC reference plus all its leaves) so the accounting never runs ahead
	// of the cap mid-merge.
	if f.objects+1+len(closure.Leaves) > capObjects {
		droppedTotal.WithLabelValues(dropReasonBufferFull).Inc()
		return
	}
	buf := f.buffers[prefix]
	if buf == nil {
		buf = &prefixBuffer{
			windowStartMs: closure.TSMillis,
			entries:       map[string]*closureBuf{},
		}
		f.buffers[prefix] = buf
	}
	// Refresh the routing pair on every observation so a re-pinned tenant's
	// later observations steer the whole window to the current backend.
	if selection.Endpoint != "" {
		buf.selection = selection
	}
	entry := buf.entries[closure.AC.Hash]
	if entry == nil {
		entry = &closureBuf{leaves: map[string]int64{}}
		buf.entries[closure.AC.Hash] = entry
		buf.order = append(buf.order, closure.AC.Hash)
		f.objects++ // the AC reference itself
	}
	entry.acSizeOnDisk = closure.AC.SizeOnDisk
	if closure.TSMillis > entry.lastAccessMs {
		entry.lastAccessMs = closure.TSMillis
	}
	if closure.TSMillis != 0 && (buf.windowStartMs == 0 || closure.TSMillis < buf.windowStartMs) {
		buf.windowStartMs = closure.TSMillis
	}
	for _, leaf := range closure.Leaves {
		if _, exists := entry.leaves[leaf.Hash]; !exists {
			f.objects++ // newly inserted leaf reference
		}
		entry.leaves[leaf.Hash] = leaf.SizeOnDisk
	}
	bufferedObjects.Set(float64(f.objects))
}

// Start launches the flush loop. It must be called at most once.
func (f *Flusher) Start() {
	go func() {
		defer close(f.loopDone)
		ticker := time.NewTicker(f.interval)
		defer ticker.Stop()
		for {
			select {
			case <-f.stop:
				return
			case <-ticker.C:
				f.flush(triggerPeriodic)
			}
		}
	}()
}

// Drain stops the flush loop and synchronously flushes remaining buffers.
// Call once, after request serving has stopped (no new observations arrive
// past that point).
func (f *Flusher) Drain() {
	if f == nil {
		return
	}
	f.stopOnce.Do(func() { close(f.stop) })
	<-f.loopDone
	f.flush(triggerShutdown)
}

// flush detaches all buffers in O(1) under the lock — RecordACAccess sits on
// the request path, so no serialization or upload work ever happens while
// holding f.mu — then serially uploads one artifact per prefix. Serial and
// direct on purpose: this is the degenerate, sufficient form of a bounded
// upload pipeline at one small artifact per tenant per window. A slow
// backend delays only later artifacts in the same pass; buffered memory
// stays bounded because new observations accumulate against a fresh map
// under the same global cap.
func (f *Flusher) flush(trigger string) {
	windowEndMs := nowMs()
	f.mu.Lock()
	detached := f.buffers
	f.buffers = map[string]*prefixBuffer{}
	f.objects = 0
	bufferedObjects.Set(0)
	f.mu.Unlock()

	for prefix, buf := range detached {
		if len(buf.entries) == 0 {
			continue
		}
		f.writeArtifact(prefix, buf, windowEndMs, trigger)
	}
}

// writeArtifact serializes one prefix's window and uploads it, once. A failed
// upload drops the window's observations (advisory; counted).
func (f *Flusher) writeArtifact(prefix string, buf *prefixBuffer, windowEndMs int64, trigger string) {
	closures := make([]cache.ACClosure, 0, len(buf.entries))
	for _, acHash := range buf.order {
		entry := buf.entries[acHash]
		if entry == nil {
			continue
		}
		leaves := make([]cache.LRUObject, 0, len(entry.leaves))
		for leafHash, leafSize := range entry.leaves {
			leaves = append(leaves, cache.LRUObject{Hash: leafHash, SizeOnDisk: leafSize})
		}
		closures = append(closures, cache.ACClosure{
			AC:       cache.LRUObject{Hash: acHash, SizeOnDisk: entry.acSizeOnDisk},
			Leaves:   leaves,
			TSMillis: entry.lastAccessMs,
		})
	}

	// Generation and InstanceName stay empty on the L1: the header carries
	// provenance only, and the object key (via the storage prefix) is what
	// encodes the generation for the sweep. Reconstructing them here would
	// bake the host's prefix layout into the fork for no consumer benefit.
	header := cache.LRUArtifactHeader{
		SchemaVersion: cache.LRUArtifactSchemaVersion,
		Host:          f.host,
		ProcessID:     f.processID,
		WindowStartMs: buf.windowStartMs,
		WindowEndMs:   windowEndMs,
		EntryCount:    len(closures),
	}

	var body bytes.Buffer
	if err := cache.WriteLRUArtifact(&body, header, closures); err != nil {
		flushTotal.WithLabelValues(trigger, resultFailure).Inc()
		return
	}
	key := cache.LRUArtifactKey(normalizePrefix(prefix), windowEndMs, f.nextUniq())
	ctx := context.Background()
	if buf.selection.Endpoint != "" {
		ctx = cache.WithS3Backend(ctx, buf.selection)
	}
	putCtx, cancel := context.WithTimeout(ctx, uploadTimeout)
	defer cancel()
	if err := f.sink.PutArtifact(putCtx, key, body.Bytes()); err != nil {
		flushTotal.WithLabelValues(trigger, resultFailure).Inc()
		return
	}
	flushTotal.WithLabelValues(trigger, resultSuccess).Inc()
	flushBytes.Observe(float64(body.Len()))
	flushEntries.Observe(float64(len(closures)))
}

// nextUniq returns a process-scoped discriminator so two flushes of the same
// window for the same prefix never collide on the object key.
func (f *Flusher) nextUniq() string {
	short := strings.ReplaceAll(f.processID, "-", "")
	if len(short) > 8 {
		short = short[:8]
	}
	return short + "-" + strconv.FormatUint(f.uniqSeq.Add(1), 10)
}

// normalizePrefix ensures exactly one trailing slash: LRUArtifactKey
// concatenates the prefix verbatim, and the trust boundary accepts a prefix
// with or without one.
func normalizePrefix(prefix string) string {
	return strings.TrimSuffix(prefix, "/") + "/"
}

func nowMs() int64 {
	return time.Now().UTC().UnixMilli()
}
