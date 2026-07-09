package disk

import (
	"context"
	"sync"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	leafSizeSourceLocal = "local"
	leafSizeSourceProxy = "proxy"
	// leafSizeSourceMissing is recorded when a closure leaf's sizeOnDisk could
	// not be resolved from either the local index or a proxy stat during
	// validation. It is the field validator for the zero-extra-round-trip size
	// assumption (D11): a non-trivial rate means sizes are not always free and
	// the design must be revisited. It always coincides with a complete-or-drop
	// (incomplete_closure) drop of the enclosing AC entry.
	leafSizeSourceMissing = "missing"

	dropReasonIncompleteClosure  = "incomplete_closure"
	dropReasonWriteHasDirs       = "write_has_directories"
	dropReasonWriteInlinedLeaf   = "write_inlined_leaf"
	dropReasonUnmarshalActionRes = "ac_unmarshal_failed"
	dropReasonACTooLarge         = "ac_too_large"
	dropReasonCaptureBudget      = "capture_budget"
)

const (
	// lruMaxACCaptureBytes bounds the in-memory buffering of ONE AC write for
	// closure capture. Without an observer the write streams to a tempfile
	// with constant memory; capture must parse the whole ActionResult, so it
	// buffers the blob - and the declared size is client-controlled. 4 MiB
	// is the gRPC max-message default: every AC arriving via UpdateActionResult
	// fits under it by construction (the AR rides inside the message with
	// envelope overhead as margin), so the bound only bites the HTTP path,
	// where no transport limit exists. Oversized ACs are cached normally via
	// the streaming path and simply not observed (capture is advisory and
	// must never gate the write path) - dropped as ac_too_large.
	lruMaxACCaptureBytes = 4 << 20

	// lruCaptureBudgetBytes bounds the AGGREGATE capture memory across all
	// in-flight AC writes: the per-request cap alone leaves a worst case of
	// (max concurrent AC writes x 4 MiB), which scales with VM count and
	// stream counts. The budget makes the ceiling an explicit constant.
	// Requests that cannot acquire budget skip observation (capture_budget
	// drop) - under memory pressure, not observing is exactly the right
	// degradation for an advisory feature.
	lruCaptureBudgetBytes = 64 << 20
)

var (
	// lruLeafSizeSource validates the zero-extra-round-trip size assumption in
	// the field: every captured leaf size should come from the local index or
	// a proxy stat that validation already performed.
	lruLeafSizeSource = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bazel_remote_lru_leaf_size_source_total",
		Help: "Source of sizeOnDisk values recorded for LRU closure leaves.",
	}, []string{"source"})

	// lruObservationsDropped counts AC closures that were not emitted because
	// they could not be completed (complete-or-drop). Advisory loss only.
	lruObservationsDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bazel_remote_lru_observations_dropped_total",
		Help: "AC closures dropped before emission, by reason.",
	}, []string{"reason"})
)

// tryAcquireCaptureBudget reserves size bytes of the process-wide AC capture
// budget, non-blocking: capture is advisory, so under budget pressure we skip
// the observation rather than delay the write. Callers must pair a successful
// acquire with releaseCaptureBudget once the buffered blob is dead.
func (c *diskCache) tryAcquireCaptureBudget(size int64) bool {
	if c.lruCaptureSem == nil {
		// Zero-value diskCache (tests construct these directly): no budget
		// configured means no bound, matching the pre-budget behavior.
		return true
	}
	return c.lruCaptureSem.TryAcquire(size)
}

func (c *diskCache) releaseCaptureBudget(size int64) {
	if c.lruCaptureSem != nil {
		c.lruCaptureSem.Release(size)
	}
}

// leafSizeCollector implements cache.LeafSizeSink for a single AC-closure
// validation. It records sizeOnDisk per CAS leaf hash discovered during
// existence checks (local index or proxy stat), with zero extra round trips.
type leafSizeCollector struct {
	mu    sync.Mutex
	sizes map[string]int64
}

func newLeafSizeCollector() *leafSizeCollector {
	return &leafSizeCollector{sizes: make(map[string]int64)}
}

func (c *leafSizeCollector) RecordLeafSize(hash string, sizeOnDisk int64, fromProxy bool) {
	source := leafSizeSourceLocal
	if fromProxy {
		source = leafSizeSourceProxy
	}
	lruLeafSizeSource.WithLabelValues(source).Inc()

	c.mu.Lock()
	c.sizes[hash] = sizeOnDisk
	c.mu.Unlock()
}

func (c *leafSizeCollector) size(hash string) (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.sizes[hash]
	return s, ok
}

// lookupSizeOnDisk returns the on-disk size of a locally-stored entry, used by
// the AC write path to resolve just-written leaves from the index. It never
// reaches the proxy, and it uses a recency-neutral Peek so observation-only
// lookups do not promote entries and change eviction order.
func (c *diskCache) lookupSizeOnDisk(ctx context.Context, kind cache.EntryKind, hash string) (int64, bool) {
	if kind == cache.CAS && hash == emptySha256 {
		return 0, true
	}
	key := cache.LookupKeyForContext(ctx, kind, hash)
	c.mu.Lock()
	item, ok := c.lru.Peek(key)
	c.mu.Unlock()
	if !ok {
		return 0, false
	}
	return item.sizeOnDisk, true
}

// emitACClosureFromHit assembles and emits the closure for a validated AC hit.
// leafHashes is the snapshot of CAS leaf hashes (output files, expanded Tree
// files, the Tree blobs themselves, stdout/stderr) taken before validation
// nil-ed the digest slice; collector holds their sizes. Complete-or-drop: if
// any leaf size is unresolved the whole closure is dropped (§4.4).
func (c *diskCache) emitACClosureFromHit(ctx context.Context, acHash string, acSizeOnDisk int64, leafHashes []string, collector *leafSizeCollector) {
	leaves := make([]cache.LRUObject, 0, len(leafHashes))
	seen := make(map[string]struct{}, len(leafHashes))

	for _, h := range leafHashes {
		if _, dup := seen[h]; dup {
			continue
		}
		seen[h] = struct{}{}

		if h == emptySha256 {
			leaves = append(leaves, cache.LRUObject{Hash: h, SizeOnDisk: 0})
			continue
		}

		sz, ok := collector.size(h)
		if !ok {
			// Record the missing leaf so the zero-round-trip size assumption is
			// observable in the field, then drop the whole closure (D11/§4.4).
			lruLeafSizeSource.WithLabelValues(leafSizeSourceMissing).Inc()
			lruObservationsDropped.WithLabelValues(dropReasonIncompleteClosure).Inc()
			return
		}
		leaves = append(leaves, cache.LRUObject{Hash: h, SizeOnDisk: sz})
	}

	cache.ObserveACAccess(ctx, c.lruObserver, cache.ACClosure{
		AC:       cache.LRUObject{Hash: acHash, SizeOnDisk: acSizeOnDisk},
		Leaves:   leaves,
		TSMillis: time.Now().UnixMilli(),
	})
}

// emitACClosureFromWrite assembles and emits the closure for a just-written AC.
// Per D10/§4.2 it does no Tree reads: an AC with output directories, or with
// inlined leaves not yet in the CAS index, is left for the read path to record
// completely, so no partial closure is ever emitted.
func (c *diskCache) emitACClosureFromWrite(ctx context.Context, acHash string, acSizeOnDisk int64, ar *pb.ActionResult) {
	if len(ar.OutputDirectories) > 0 {
		lruObservationsDropped.WithLabelValues(dropReasonWriteHasDirs).Inc()
		return
	}

	leaves := make([]cache.LRUObject, 0, len(ar.OutputFiles)+2)
	seen := make(map[string]struct{}, len(ar.OutputFiles)+2)

	addLeaf := func(d *pb.Digest) bool {
		if d == nil {
			return true
		}
		if _, dup := seen[d.Hash]; dup {
			return true
		}
		seen[d.Hash] = struct{}{}
		sz, ok := c.lookupSizeOnDisk(ctx, cache.CAS, d.Hash)
		if !ok {
			return false
		}
		leaves = append(leaves, cache.LRUObject{Hash: d.Hash, SizeOnDisk: sz})
		return true
	}

	for _, f := range ar.OutputFiles {
		if f == nil {
			continue
		}
		if len(f.Contents) > 0 {
			// Inlined content is stored to the CAS after this AC write, so its
			// blob is not in the index yet; defer the whole closure to the read.
			lruObservationsDropped.WithLabelValues(dropReasonWriteInlinedLeaf).Inc()
			return
		}
		if !addLeaf(f.Digest) {
			lruObservationsDropped.WithLabelValues(dropReasonIncompleteClosure).Inc()
			return
		}
	}

	if len(ar.StdoutRaw) > 0 || len(ar.StderrRaw) > 0 {
		lruObservationsDropped.WithLabelValues(dropReasonWriteInlinedLeaf).Inc()
		return
	}
	if !addLeaf(ar.StdoutDigest) || !addLeaf(ar.StderrDigest) {
		lruObservationsDropped.WithLabelValues(dropReasonIncompleteClosure).Inc()
		return
	}

	cache.ObserveACAccess(ctx, c.lruObserver, cache.ACClosure{
		AC:       cache.LRUObject{Hash: acHash, SizeOnDisk: acSizeOnDisk},
		Leaves:   leaves,
		TSMillis: time.Now().UnixMilli(),
	})
}
