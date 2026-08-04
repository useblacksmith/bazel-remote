package s3proxy

// Duplicate-upload coalescing for the write-through queue. Matrix fan-out
// delivers the same missing blob from many hosts at nearly the same moment;
// MinIO's create-if-absent would 412 all but one of the resulting uploads
// anyway, and every queued duplicate pins an open FD — the queue's true
// scarce resource. An in-flight identity set at the enqueue door drops the
// duplicates before they cost anything.
//
// This is deliberately dedup-only: a coalesced or shed upload is dropped,
// not deferred (no owed ledger — simplicity first; revisit if production
// drop counters show real pressure).

import (
	"sync"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/utils/backendproxy"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var uploadQueueCoalesced = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bazel_remote_s3_upload_queue_coalesced_total",
	Help: "Backend uploads dropped at enqueue because an identical upload (kind+hash+prefix+bucket) was already queued or in flight.",
}, []string{"backend"})

// uploadKey identifies one logical backend upload. Two uploads with the same
// key write the same object to the same place; queueing both is pure waste.
type uploadKey struct {
	Kind   cache.EntryKind
	Hash   string
	Prefix string
	Bucket string
}

// resolveUploadIdentity resolves the queue-time request fields to the
// upload's storage identity, applying the same prefix/bucket fallbacks the
// upload worker itself will apply, so two requests that would write the same
// object always map to the same key.
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
