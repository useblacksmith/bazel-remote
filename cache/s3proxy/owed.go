package s3proxy

// Owed-upload machinery: restores the "L1 contents ⊆ MinIO contents"
// invariant that lossy write-through shedding silently violates.
//
// Why this exists: the disk cache answers FindMissingBlobs local-first, so a
// blob that landed on the L1 but whose backend upload was shed is reported
// "present" to clients — nothing ever re-uploads it, and MinIO permanently
// lacks an object the footprint accounting may already have counted. Shedding
// therefore must be a DEFERRAL, not a loss:
//
//   - Put() overflow and terminal upload failures record the item in a
//     per-backend owed ledger (bounded, snapshotted to disk so restarts
//     don't forget the debt).
//   - A background sweeper re-enqueues owed items through the normal upload
//     queue — the same worker pool, breaker (ExecuteNoProbe), and outcome
//     accounting govern the retry. The sweeper yields to live traffic: it
//     only runs while the breaker is closed and the queue is under half
//     full, so convergence never competes with builds.
//   - Upload success (created or already_exists) settles the debt.
//
// The inflight set doubles as upload coalescing: cross-host matrix fan-out
// makes the same missing blob arrive from every host at once, and MinIO
// would reject the duplicates with 412 anyway (create-if-absent), so the
// duplicate enqueue is dropped at the door and its reader (an open FD — the
// queue's real cost) is released immediately.

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/utils/backendproxy"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	// maxOwedEntries bounds one backend's ledger. At ~200 bytes/entry this
	// is ~50 MiB of memory and snapshot. When the ledger is full, new debts
	// are REJECTED (metered) rather than evicting older ones: older entries
	// are closer to LRU eviction, which self-resolves them, while a
	// saturated ledger is an alertable capacity signal either way.
	maxOwedEntries = 1 << 18

	// sweepInterval paces the background sweeper. Convergence within
	// minutes-to-hours is the goal; this is deliberately unhurried.
	sweepInterval = 15 * time.Second

	// sweepBatch caps how many owed items one pass re-enqueues.
	sweepBatch = 256

	// sweepQueueHeadroom: the sweeper only injects work while the live
	// queue is under this fraction of capacity, so deferred uploads never
	// compete with current build traffic for queue slots or workers.
	sweepQueueHeadroom = 0.5
)

var (
	uploadQueueCoalesced = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bazel_remote_s3_upload_queue_coalesced_total",
		Help: "Backend uploads dropped at enqueue because an identical upload (kind+hash+prefix+bucket) was already queued or in flight.",
	}, []string{"backend"})
	owedBacklog = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bazel_remote_s3_owed_uploads",
		Help: "Blobs present locally but owed to the S3 backend (shed or failed write-throughs awaiting the sweeper).",
	}, []string{"backend"})
	owedSweep = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bazel_remote_s3_owed_sweep_total",
		Help: "Owed-upload sweeper dispositions (requeued, blob_evicted).",
	}, []string{"backend", "result"})
	owedRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bazel_remote_s3_owed_rejected_total",
		Help: "Owed-upload records rejected because the ledger is at capacity (permanent MinIO gap until the blob is evicted and re-uploaded).",
	}, []string{"backend"})
)

// BlobSource reopens a committed blob's raw on-disk representation (exactly
// the bytes the original write-through would have streamed) for a deferred
// upload. ctx carries the entry's storage prefix when it was request-scoped,
// because the disk layout and lookup key are prefix-scoped. An evicted or
// missing blob returns an error; callers settle the debt (eviction makes the
// blob honestly absent everywhere, so FindMissingBlobs heals it the normal
// way).
type BlobSource interface {
	OpenOwedBlob(ctx context.Context, kind cache.EntryKind, hash string) (io.ReadCloser, int64, error)
}

// BlobSourceSetter is implemented by the proxies returned from New/NewMulti.
// The disk cache is constructed AFTER the proxy (it takes the proxy as an
// argument), so the back-reference is injected post-construction.
type BlobSourceSetter interface {
	SetBlobSource(src BlobSource)
}

// uploadKey identifies one logical backend upload. Two uploads with the same
// key write the same object to the same place; queueing both is pure waste.
type uploadKey struct {
	Kind   cache.EntryKind `json:"kind"`
	Hash   string          `json:"hash"`
	Prefix string          `json:"prefix"`
	Bucket string          `json:"bucket"`
}

// owedEntry is everything needed to reconstruct an UploadReq later. Sizes are
// re-derived from disk at sweep time (the authoritative copy), but
// LogicalSize is kept for observer accounting parity.
type owedEntry struct {
	Key                        uploadKey `json:"key"`
	LogicalSize                int64     `json:"logical_size"`
	RequestScopedStoragePrefix bool      `json:"request_scoped_prefix"`
	RequireStoragePrefix       bool      `json:"require_prefix"`
}

// inflightSet tracks uploads that are queued or being uploaded right now.
// Methods are safe on a nil receiver (hand-built caches in tests): a nil set
// admits everything and coalesces nothing.
type inflightSet struct {
	mu sync.Mutex
	m  map[uploadKey]struct{}
}

func newInflightSet() *inflightSet {
	return &inflightSet{m: make(map[uploadKey]struct{})}
}

// tryAdd returns false when the key is already inflight (coalesce case).
func (s *inflightSet) tryAdd(k uploadKey) bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.m[k]; dup {
		return false
	}
	s.m[k] = struct{}{}
	return true
}

func (s *inflightSet) remove(k uploadKey) {
	if s == nil {
		return
	}
	s.mu.Lock()
	delete(s.m, k)
	s.mu.Unlock()
}

// owedLedger is the bounded, snapshotted record of uploads the backend is
// still owed. All methods are safe on a nil receiver (feature disabled).
type owedLedger struct {
	backendKey string
	path       string

	mu      sync.Mutex
	entries map[uploadKey]owedEntry
	dirty   bool
}

// newOwedLedger loads any snapshot left by a previous process. A corrupt or
// unreadable snapshot starts empty and logs: the ledger is a convergence
// accelerator, never a correctness gate worth failing startup for.
func newOwedLedger(dir, backendKey string, errorLogger cache.Logger) *owedLedger {
	l := &owedLedger{
		backendKey: backendKey,
		path:       filepath.Join(dir, "owed-uploads-"+sanitizeForFilename(backendKey)+".json"),
		entries:    make(map[uploadKey]owedEntry),
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		errorLogger.Printf("owed ledger: cannot create %s (%v); continuing without restart persistence", dir, err)
		l.path = ""
		return l
	}
	data, err := os.ReadFile(l.path)
	if err == nil {
		var snapshot []owedEntry
		if jerr := json.Unmarshal(data, &snapshot); jerr != nil {
			errorLogger.Printf("owed ledger: corrupt snapshot %s (%v); starting empty", l.path, jerr)
		} else {
			for _, e := range snapshot {
				if len(l.entries) >= maxOwedEntries {
					break
				}
				l.entries[e.Key] = e
			}
		}
	}
	owedBacklog.WithLabelValues(backendKey).Set(float64(len(l.entries)))
	return l
}

func (l *owedLedger) add(e owedEntry) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.entries[e.Key]; !exists && len(l.entries) >= maxOwedEntries {
		owedRejected.WithLabelValues(l.backendKey).Inc()
		return
	}
	l.entries[e.Key] = e
	l.dirty = true
	owedBacklog.WithLabelValues(l.backendKey).Set(float64(len(l.entries)))
}

func (l *owedLedger) settle(k uploadKey) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.entries[k]; !ok {
		return
	}
	delete(l.entries, k)
	l.dirty = true
	owedBacklog.WithLabelValues(l.backendKey).Set(float64(len(l.entries)))
}

// batch returns up to n entries for a sweep pass, in map order (unordered —
// fairness across passes comes from settled entries leaving the map).
func (l *owedLedger) batch(n int) []owedEntry {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]owedEntry, 0, n)
	for _, e := range l.entries {
		out = append(out, e)
		if len(out) == n {
			break
		}
	}
	return out
}

// snapshotIfDirty atomically rewrites the on-disk snapshot. Called from the
// sweeper loop; a crash loses at most one sweepInterval of ledger changes,
// which the next drop or eviction re-discovers (documented, acceptable).
func (l *owedLedger) snapshotIfDirty(errorLogger cache.Logger) {
	if l == nil || l.path == "" {
		return
	}
	l.mu.Lock()
	if !l.dirty {
		l.mu.Unlock()
		return
	}
	snapshot := make([]owedEntry, 0, len(l.entries))
	for _, e := range l.entries {
		snapshot = append(snapshot, e)
	}
	l.dirty = false
	l.mu.Unlock()

	data, err := json.Marshal(snapshot)
	if err != nil {
		errorLogger.Printf("owed ledger: marshal failed: %v", err)
		return
	}
	tmp := l.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		errorLogger.Printf("owed ledger: snapshot write failed: %v", err)
		return
	}
	if err := os.Rename(tmp, l.path); err != nil {
		errorLogger.Printf("owed ledger: snapshot rename failed: %v", err)
	}
}

func sanitizeForFilename(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '.':
			return r
		default:
			return '_'
		}
	}, s)
}

// resolveUploadIdentity mirrors UploadFile's prefix/bucket resolution so the
// identity computed at enqueue time matches the one settled at outcome time.
func (c *s3Cache) resolveUploadIdentity(item backendproxy.UploadReq) uploadKey {
	prefix := item.StoragePrefix
	if item.Kind == cache.RAW || prefix == "" {
		prefix = c.prefix
	}
	bucket := item.S3Backend.Bucket
	if bucket == "" {
		bucket = c.bucket
	}
	return uploadKey{Kind: item.Kind, Hash: item.Hash, Prefix: prefix, Bucket: bucket}
}

func (c *s3Cache) owedEntryForItem(key uploadKey, item backendproxy.UploadReq) owedEntry {
	requestScoped := item.RequestScopedStoragePrefix
	if item.Kind == cache.RAW {
		requestScoped = false
	}
	return owedEntry{
		Key:                        key,
		LogicalSize:                item.LogicalSize,
		RequestScopedStoragePrefix: requestScoped,
		RequireStoragePrefix:       item.RequireStoragePrefix && requestScoped,
	}
}

// SetBlobSource injects the disk cache back-reference and starts the sweeper.
// Called once, after the disk cache is constructed. Without a blob source the
// ledger still records and persists debts (visible in metrics and settled by
// live re-uploads), but nothing can proactively drain it.
func (c *s3Cache) SetBlobSource(src BlobSource) {
	if c.owed == nil || src == nil {
		return
	}
	c.blobSourceOnce.Do(func() {
		c.blobSource = src
		go c.sweepOwedLoop()
	})
}

func (m *multiS3Cache) SetBlobSource(src BlobSource) {
	for _, backend := range m.backends {
		backend.SetBlobSource(src)
	}
}

func (c *s3Cache) sweepOwedLoop() {
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()
	for range ticker.C {
		c.sweepOwedOnce()
		c.owed.snapshotIfDirty(c.errorLogger)
	}
}

// sweepOwedOnce re-enqueues one batch of owed uploads if the backend is
// healthy and the queue has headroom. Every re-enqueued item flows through
// the normal worker/breaker/outcome path; settlement happens there.
func (c *s3Cache) sweepOwedOnce() {
	if c.owed == nil || c.blobSource == nil || c.uploadQueue == nil {
		return
	}
	if c.breaker != nil && c.breaker.State() != breakerClosed {
		return
	}
	if len(c.uploadQueue) >= int(sweepQueueHeadroom*float64(cap(c.uploadQueue))) {
		return
	}

	for _, entry := range c.owed.batch(sweepBatch) {
		if len(c.uploadQueue) >= int(sweepQueueHeadroom*float64(cap(c.uploadQueue))) {
			return
		}
		if !c.inflight.tryAdd(entry.Key) {
			continue // already queued or uploading; its outcome will settle it
		}

		ctx := context.Background()
		if entry.RequestScopedStoragePrefix {
			ctx = cache.WithStoragePrefix(ctx, entry.Key.Prefix)
		}
		rc, sizeOnDisk, err := c.blobSource.OpenOwedBlob(ctx, entry.Key.Kind, entry.Key.Hash)
		if err != nil {
			// Evicted (or unreadable): the blob is honestly absent
			// everywhere now, so FindMissingBlobs will report it missing
			// and a future client upload recreates both copies. Debt void.
			c.inflight.remove(entry.Key)
			c.owed.settle(entry.Key)
			owedSweep.WithLabelValues(c.key, "blob_evicted").Inc()
			continue
		}

		req := backendproxy.UploadReq{
			Hash:                       entry.Key.Hash,
			LogicalSize:                entry.LogicalSize,
			SizeOnDisk:                 sizeOnDisk,
			Kind:                       entry.Key.Kind,
			Rc:                         rc,
			StoragePrefix:              entry.Key.Prefix,
			RequestScopedStoragePrefix: entry.RequestScopedStoragePrefix,
			RequireStoragePrefix:       entry.RequireStoragePrefix,
			S3Backend:                  cache.S3BackendSelection{Bucket: entry.Key.Bucket},
		}
		select {
		case c.uploadQueue <- req:
			owedSweep.WithLabelValues(c.key, "requeued").Inc()
			uploadQueueDepth.WithLabelValues(c.key).Set(float64(len(c.uploadQueue)))
		default:
			// Lost the headroom race; stay owed, try next pass.
			c.inflight.remove(entry.Key)
			_ = rc.Close()
			return
		}
	}
}
