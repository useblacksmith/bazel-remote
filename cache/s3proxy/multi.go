package s3proxy

import (
	"context"
	"errors"
	"io"
	"log"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Multi-backend dispatch: an L1 node fronting several MinIO clusters routes
// each operation to the backend matching the request-scoped selector (the
// tenant's pinned backing-store endpoint, lifted from validated gRPC
// metadata by the server interceptor). Each backend is a full s3Cache — own
// minio client, transport, and upload queue — so dispatch happens once, at
// the top, and write-through queue membership is the captured routing
// decision.

var (
	// backendUnknown is belt-and-braces coverage for interceptor/config
	// drift and stays flat in practice: the gRPC interceptor rejects
	// unknown selectors at the boundary, and the HTTP listener carries no
	// selector at all. Do NOT alert on it — the interceptor rejection
	// counters (bazel_remote_s3_backend_selector_rejected_total{cause})
	// are the signal.
	backendUnknown = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bazel_remote_s3_backend_unknown_total",
		Help: "Requests carrying a backend selector not present in the configured backends map (refused; indicates an interceptor/config mismatch).",
	}, []string{"operation"})
	defaultBackendFallback = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bazel_remote_s3_default_backend_fallback_total",
		Help: "Operations routed to the default backend because the request carried no backend selector: HTTP-listener side-door traffic (expected to be firewall-gated) or a lost-selector bug on a gRPC path.",
	}, []string{"operation"})
)

// errUnknownBackend is returned when a request carries a backend selector
// that is not in the configured backends map. The gRPC interceptor validates
// selectors fail-closed at the boundary, so hitting this means an
// interceptor/config mismatch bug; refusing the operation is safer than
// touching the wrong shard.
var errUnknownBackend = errors.New("unknown S3 backend selector")

// uploadQueueSlotBytes approximates the resident cost of one preallocated
// upload-queue slot (a backendproxy.UploadReq: several strings, an
// interface, sizes), for the startup aggregate-footprint log.
const uploadQueueSlotBytes = 200

// BackendSpec describes one S3 backend of a multi-backend proxy.
type BackendSpec struct {
	// Key is the tenant-facing selector this backend is registered under
	// (the allowlisted endpoint URL forwarded as gRPC metadata). It is also
	// the "backend" metrics label.
	Key              string
	Endpoint         string
	Bucket           string
	BucketLookupType minio.BucketLookupType
	Prefix           string
	Credentials      *credentials.Credentials
	DisableSSL       bool
	Region           string
	MaxIdleConns     int
	Default          bool
}

// NewMulti returns a cache.Proxy that dispatches over a map of allowlisted S3
// backends. Each backend gets its own minio client, transport, and upload
// queue (numUploaders/maxQueuedUploads apply per backend). Requests carrying
// a backend selector on the context (lifted from validated gRPC metadata by
// the server interceptor) route to the matching backend; requests without a
// selector route to the designated default backend.
//
// Config validation owns structural checks on the specs (non-empty keys,
// resolvable endpoints; map-sourced specs cannot repeat keys); NewMulti only
// enforces what config cannot express: exactly one default backend.
func NewMulti(specs []BackendSpec, updateTimestamps bool, connRecycleInterval time.Duration,
	storageMode string, accessLogger cache.Logger, errorLogger cache.Logger,
	numUploaders, maxQueuedUploads int, metrics Metrics, options ...Option) (cache.Proxy, error) {

	m := &multiS3Cache{
		backends:    make(map[string]*s3Cache, len(specs)),
		errorLogger: errorLogger,
	}
	for _, spec := range specs {
		backend, err := newBackend(spec, updateTimestamps, connRecycleInterval, storageMode,
			accessLogger, errorLogger, numUploaders, maxQueuedUploads, metrics, options...)
		if err != nil {
			return nil, err
		}
		m.backends[spec.Key] = backend
		if spec.Default {
			if m.def != nil {
				return nil, errors.New("multiple S3 backends marked as default")
			}
			m.def = backend
		}
	}
	if m.def == nil {
		return nil, errors.New("no S3 backend marked as default")
	}

	// Upload queues preallocate and uploaders are goroutine pools, both per
	// backend; surface the aggregate so an oversized per-backend setting in
	// a many-backend map is visible at startup instead of as an OOM.
	backends := len(m.backends)
	totalSlots := backends * maxQueuedUploads
	log.Printf("Using S3 backend map with %d backends (default: %s): per backend %d queued-upload slots / %d uploaders; aggregate %d slots (~%d MiB) and %d uploader goroutines.",
		backends, m.def.key, maxQueuedUploads, numUploaders,
		totalSlots, totalSlots*uploadQueueSlotBytes/(1<<20), backends*numUploaders)

	return m, nil
}

// multiS3Cache routes cache.Proxy operations to one of several s3Cache
// backends based on the request-scoped backend selector.
type multiS3Cache struct {
	backends    map[string]*s3Cache
	def         *s3Cache
	errorLogger cache.Logger
}

// backendFor resolves the backend for a request. A missing selector routes
// to the default backend, metered: legitimate for HTTP API paths and RAW
// entries (the HTTP listener is a firewall-gated side door by deployment
// contract), a bug if it ever fires on a gRPC cache path, where the
// interceptor guarantees a selector. An unknown selector refuses the
// operation — the gRPC interceptor already rejects those fail-closed, so
// this is a belt-and-braces guard against interceptor/config drift, and
// must never fall back to a guessed backend.
func (m *multiS3Cache) backendFor(ctx context.Context, operation string) *s3Cache {
	selector, ok := cache.S3BackendFromContext(ctx)
	if !ok {
		defaultBackendFallback.WithLabelValues(operation).Inc()
		return m.def
	}
	backend, ok := m.backends[selector]
	if !ok {
		backendUnknown.WithLabelValues(operation).Inc()
		if m.errorLogger != nil {
			m.errorLogger.Printf("S3 %s unknown backend selector %q; refusing operation", operation, selector)
		}
		return nil
	}
	return backend
}

func (m *multiS3Cache) Put(ctx context.Context, kind cache.EntryKind, hash string, logicalSize int64, sizeOnDisk int64, rc io.ReadCloser) {
	backend := m.backendFor(ctx, "UPLOAD")
	if backend == nil {
		_ = rc.Close()
		return
	}
	backend.Put(ctx, kind, hash, logicalSize, sizeOnDisk, rc)
}

func (m *multiS3Cache) Get(ctx context.Context, kind cache.EntryKind, hash string, size int64) (io.ReadCloser, int64, error) {
	backend := m.backendFor(ctx, "DOWNLOAD")
	if backend == nil {
		return nil, -1, errUnknownBackend
	}
	return backend.Get(ctx, kind, hash, size)
}

func (m *multiS3Cache) Contains(ctx context.Context, kind cache.EntryKind, hash string, size int64) (bool, int64) {
	backend := m.backendFor(ctx, "CONTAINS")
	if backend == nil {
		return false, -1
	}
	return backend.Contains(ctx, kind, hash, size)
}
