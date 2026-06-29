package cache

import "context"

// LeafSizeSink collects the on-disk (stored/compressed) byte size of CAS
// leaves as they are discovered during AC-closure validation, so the LRU
// closure can be assembled with zero extra storage round trips. It is an
// internal capture mechanism (not the artifact contract); implementations
// must be safe for concurrent use because proxy existence checks run in
// parallel goroutines.
type LeafSizeSink interface {
	// RecordLeafSize records the sizeOnDisk for a CAS leaf. fromProxy
	// distinguishes a size resolved from the local LRU index (false) from
	// one resolved from a proxy StatObject (true), for diagnostics.
	RecordLeafSize(hash string, sizeOnDisk int64, fromProxy bool)
}

type leafSizeSinkKey struct{}

// WithLeafSizeSink attaches a LeafSizeSink to ctx. Only AC-closure validation
// inside GetValidatedActionResult attaches one; the public FindMissingBlobs
// RPC does not, so it keeps its current behavior at zero extra cost.
func WithLeafSizeSink(ctx context.Context, sink LeafSizeSink) context.Context {
	return context.WithValue(ctx, leafSizeSinkKey{}, sink)
}

// LeafSizeSinkFromContext returns the LeafSizeSink attached to ctx, if any.
func LeafSizeSinkFromContext(ctx context.Context) (LeafSizeSink, bool) {
	sink, ok := ctx.Value(leafSizeSinkKey{}).(LeafSizeSink)
	return sink, ok && sink != nil
}
