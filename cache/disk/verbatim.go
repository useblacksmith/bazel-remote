package disk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"os"
	"sync/atomic"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/cache/disk/casblob"
	"github.com/buchgr/bazel-remote/v2/utils/tempfile"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ErrVerbatimUnavailable is returned by PutZstd, before any bytes are read,
// when the blob must take the decoding Put path: the feature is disabled,
// the blob cannot be stored as a single chunk, or the verifier is backlogged.
var ErrVerbatimUnavailable = errors.New("verbatim zstd upload path unavailable")

// verbatimVerifyBacklog bounds the number of committed-but-unverified blobs.
// Beyond it PutZstd falls back to inline verification rather than letting
// unverified data accumulate without bound during an upload burst.
const verbatimVerifyBacklog = 1 << 16

var (
	verbatimPutsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bazel_remote_verbatim_zstd_puts_total",
		Help: "Client-compressed CAS uploads stored as received, with deferred digest verification.",
	})
	verbatimFallbackTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bazel_remote_verbatim_zstd_fallback_total",
		Help: "Client-compressed CAS uploads that took the decoding Put path instead.",
	}, []string{"reason"})
	verbatimVerifyTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bazel_remote_verbatim_zstd_verify_total",
		Help: "Outcomes of background digest verification of blobs stored as received.",
	}, []string{"result"})
	verbatimVerifyPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bazel_remote_verbatim_zstd_verify_pending",
		Help: "Blobs stored as received that are awaiting digest verification.",
	})
)

type verifyJob struct {
	// ctx carries the request's storage prefix and backend selection for
	// the deferred proxy write; it is detached from the request's lifetime.
	ctx    context.Context
	key    string
	hash   string
	random string
	size   int64

	// Set by put once the job is on the queue, which then owns the
	// pending-slot reservation.
	enqueued bool
}

type verbatimVerifier struct {
	queue   chan verifyJob
	pending atomic.Int64
}

func (c *diskCache) startVerbatimVerifier(workers int) {
	v := &verbatimVerifier{queue: make(chan verifyJob, verbatimVerifyBacklog)}
	c.verifier = v
	for i := 0; i < workers; i++ {
		go func() {
			for job := range v.queue {
				c.verifyVerbatim(job)
				v.release()
			}
		}()
	}
}

func (v *verbatimVerifier) tryReserve() bool {
	if v.pending.Add(1) > verbatimVerifyBacklog {
		v.pending.Add(-1)
		return false
	}
	verbatimVerifyPending.Set(float64(v.pending.Load()))
	return true
}

func (v *verbatimVerifier) release() {
	verbatimVerifyPending.Set(float64(v.pending.Add(-1)))
}

func (v *verbatimVerifier) enqueue(job verifyJob) {
	// Never blocks: tryReserve bounds the number of outstanding jobs to the
	// channel capacity.
	v.queue <- job
}

// PutZstd stores a client-compressed CAS blob as received and verifies its
// digest in the background, evicting it on mismatch. The blob is a valid
// casblob from the moment it is committed, so reads served before
// verification completes return whatever the client sent; a client whose
// upload does not match its digest only corrupts entries under its own
// storage prefix, and only until the verifier catches up.
//
// Until then the file keeps the setgid "in progress" bit tempfile.Create
// set, so a blob whose verification is lost to a restart is dropped by the
// next boot scan rather than loaded as trusted.
func (c *diskCache) PutZstd(ctx context.Context, hash string, size int64, r io.Reader) error {
	if c.verifier == nil || c.storageMode != casblob.Zstandard {
		verbatimFallbackTotal.WithLabelValues("disabled").Inc()
		return ErrVerbatimUnavailable
	}
	if size <= 0 || size > casblob.MaxVerbatimZstdSize {
		verbatimFallbackTotal.WithLabelValues("size").Inc()
		return ErrVerbatimUnavailable
	}
	if !c.verifier.tryReserve() {
		verbatimFallbackTotal.WithLabelValues("backlog").Inc()
		return ErrVerbatimUnavailable
	}

	job := &verifyJob{
		ctx:  context.WithoutCancel(ctx),
		hash: hash,
		size: size,
	}
	err := c.put(ctx, cache.CAS, hash, size, r, job)
	if !job.enqueued {
		c.verifier.release()
	}
	if err == nil {
		verbatimPutsTotal.Inc()
	}
	return err
}

func (c *diskCache) verifyVerbatim(job verifyJob) {
	c.mu.Lock()
	item, ok := c.lru.Peek(job.key)
	c.mu.Unlock()
	if !ok || item.random != job.random {
		verbatimVerifyTotal.WithLabelValues("gone").Inc()
		return
	}
	blobPath := c.getElementPath(job.key, item)

	result := c.checkVerbatimDigest(blobPath, job)
	verbatimVerifyTotal.WithLabelValues(result).Inc()

	if result != "ok" {
		if result != "gone" {
			log.Printf("Evicting %s: stored-as-received blob failed verification (%s)", job.key, result)
		}
		c.mu.Lock()
		if cur, ok := c.lru.Peek(job.key); ok && cur.random == job.random {
			c.lru.RemoveKey(job.key)
		}
		c.mu.Unlock()
		return
	}

	if err := os.Chmod(blobPath, tempfile.FinalMode); err != nil {
		log.Println("failed to mark", blobPath, "as complete:", err)
	}

	if c.proxy != nil {
		f, err := os.Open(blobPath)
		if err != nil {
			log.Println("Failed to proxy Put:", err)
			return
		}
		c.proxy.Put(job.ctx, cache.CAS, job.hash, job.size, item.sizeOnDisk, f)
	}
}

func (c *diskCache) checkVerbatimDigest(blobPath string, job verifyJob) string {
	f, err := os.Open(blobPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "gone"
		}
		return "read_error"
	}

	rc, err := casblob.GetUncheckedReadCloser(c.zstd, f)
	if err != nil {
		return "decode_error"
	}
	defer func() { _ = rc.Close() }()

	hasher := sha256.New()
	// Stop one byte past the declared size: a frame that expands further
	// is wrong regardless of what follows.
	n, err := io.Copy(hasher, io.LimitReader(rc, job.size+1))
	if err != nil {
		return "decode_error"
	}
	if n != job.size {
		return "size_mismatch"
	}
	if hex.EncodeToString(hasher.Sum(nil)) != job.hash {
		return "hash_mismatch"
	}
	return "ok"
}
