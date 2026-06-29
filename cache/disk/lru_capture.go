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

	dropReasonIncompleteClosure  = "incomplete_closure"
	dropReasonWriteHasDirs       = "write_has_directories"
	dropReasonWriteInlinedLeaf   = "write_inlined_leaf"
	dropReasonUnmarshalActionRes = "ac_unmarshal_failed"
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
// reaches the proxy.
func (c *diskCache) lookupSizeOnDisk(ctx context.Context, kind cache.EntryKind, hash string) (int64, bool) {
	if kind == cache.CAS && hash == emptySha256 {
		return 0, true
	}
	key := cache.LookupKeyForContext(ctx, kind, hash)
	c.mu.Lock()
	item, ok := c.lru.Get(key)
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
