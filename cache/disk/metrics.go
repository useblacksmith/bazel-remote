package disk

import (
	"context"
	"io"

	"github.com/buchgr/bazel-remote/v2/cache"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"

	"github.com/prometheus/client_golang/prometheus"
)

type metricsDecorator struct {
	counter  *prometheus.CounterVec
	observer cache.OperationObserver
	*diskCache
}

const (
	hitStatus   = "hit"
	missStatus  = "miss"
	errorStatus = "error"

	containsMethod = "contains"
	getMethod      = "get"
	actionCacheGet = "action_cache_get"
	casLookup      = "cas_lookup"
	//putMethod      = "put"

	acKind  = "ac" // This must be lowercase to match cache.EntryKind.String()
	casKind = "cas"
	rawKind = "raw"
)

func (m *metricsDecorator) RegisterMetrics() {
	if m.counter != nil {
		prometheus.MustRegister(m.counter)
	}
	m.diskCache.RegisterMetrics()
}

func (m *metricsDecorator) Get(ctx context.Context, kind cache.EntryKind, hash string, size int64, offset int64) (io.ReadCloser, int64, error) {
	rc, size, err := m.diskCache.Get(ctx, kind, hash, size, offset)
	if err != nil {
		m.recordLookup(ctx, kind, getMethod, errorStatus, "get_failed", 1, 0)
		return rc, size, err
	}

	status := missStatus
	if rc != nil {
		status = hitStatus
	}
	m.incCounter(getMethod, kind.String(), status, 1)
	m.recordLookup(ctx, kind, getMethod, status, "", 1, nonNegativeUint64(size))

	return rc, size, nil
}

func (m *metricsDecorator) GetValidatedActionResult(ctx context.Context, hash string) (*pb.ActionResult, []byte, error) {
	ar, data, err := m.diskCache.GetValidatedActionResult(ctx, hash)
	if err != nil {
		m.record(ctx, actionCacheGet, errorStatus, "get_action_result_failed", 1, 0)
		return ar, data, err
	}

	status := missStatus
	if ar != nil {
		status = hitStatus
	}
	m.incCounter(getMethod, acKind, status, 1)
	m.record(ctx, actionCacheGet, status, "", 1, uint64(len(data)))

	return ar, data, err
}

func (m *metricsDecorator) GetZstd(ctx context.Context, hash string, size int64, offset int64) (io.ReadCloser, int64, error) {
	rc, size, err := m.diskCache.GetZstd(ctx, hash, size, offset)
	if err != nil {
		m.record(ctx, casLookup, errorStatus, "get_zstd_failed", 1, 0)
		return rc, size, err
	}

	status := missStatus
	if rc != nil {
		status = hitStatus
	}
	m.incCounter(getMethod, casKind, status, 1)
	m.record(ctx, casLookup, status, "", 1, nonNegativeUint64(size))

	return rc, size, nil
}

func (m *metricsDecorator) Contains(ctx context.Context, kind cache.EntryKind, hash string, size int64) (bool, int64) {
	ok, size := m.diskCache.Contains(ctx, kind, hash, size)

	status := missStatus
	if ok {
		status = hitStatus
	}
	m.incCounter(containsMethod, kind.String(), status, 1)
	m.recordLookup(ctx, kind, containsMethod, status, "", 1, nonNegativeUint64(size))

	return ok, size
}

func (m *metricsDecorator) FindMissingCasBlobs(ctx context.Context, blobs []*pb.Digest) ([]*pb.Digest, error) {
	numLooking := len(blobs)
	digests, err := m.diskCache.FindMissingCasBlobs(ctx, blobs)
	if err != nil {
		m.record(ctx, casLookup, errorStatus, "find_missing_cas_blobs_failed", uint64(numLooking), 0)
		return digests, err
	}

	numMissing := len(digests)

	numFound := numLooking - numMissing

	m.incCounter(containsMethod, casKind, hitStatus, float64(numFound))
	m.incCounter(containsMethod, casKind, missStatus, float64(numMissing))
	if numFound > 0 {
		m.record(ctx, casLookup, hitStatus, "", uint64(numFound), 0)
	}
	if numMissing > 0 {
		m.record(ctx, casLookup, missStatus, "", uint64(numMissing), 0)
	}

	return digests, nil
}

func (m *metricsDecorator) incCounter(method, kind, status string, value float64) {
	if m.counter == nil || value == 0 {
		return
	}
	m.counter.With(prometheus.Labels{"method": method, "kind": kind, "status": status}).Add(value)
}

func (m *metricsDecorator) recordLookup(ctx context.Context, kind cache.EntryKind, method, status, reason string, ops uint64, bytes uint64) {
	switch kind {
	case cache.AC:
		m.record(ctx, actionCacheGet, status, reason, ops, bytes)
	case cache.CAS:
		m.record(ctx, casLookup, status, reason, ops, bytes)
	default:
		m.record(ctx, method, status, reason, ops, bytes)
	}
}

func (m *metricsDecorator) record(ctx context.Context, operation, status, reason string, ops uint64, bytes uint64) {
	cache.ObserveOperation(ctx, m.observer, cache.OperationOutcome{
		Method: operation,
		Status: status,
		Reason: reason,
		Ops:    ops,
		Bytes:  bytes,
	})
}

func nonNegativeUint64(value int64) uint64 {
	if value < 0 {
		return 0
	}
	return uint64(value)
}
