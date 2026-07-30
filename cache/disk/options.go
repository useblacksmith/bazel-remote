package disk

import (
	"fmt"
	"log"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/cache/disk/casblob"
	"github.com/buchgr/bazel-remote/v2/cache/disk/zstdimpl"

	"github.com/prometheus/client_golang/prometheus"
)

type Option func(*CacheConfig) error

type CacheConfig struct {
	diskCache        *diskCache        // Assumed to be non-nil.
	metrics          *metricsDecorator // May be nil.
	maxSizeHardLimit int64
	maxEntries       int64
}

func WithStorageMode(mode string) Option {
	return func(c *CacheConfig) error {
		switch mode {
		case "zstd":
			c.diskCache.storageMode = casblob.Zstandard
			return nil
		case "uncompressed":
			c.diskCache.storageMode = casblob.Identity
			return nil
		default:
			return fmt.Errorf("unsupported storage mode: %s", mode)
		}
	}
}

func WithZstdImplementation(impl string) Option {
	return func(c *CacheConfig) error {
		var err error
		c.diskCache.zstd, err = zstdimpl.Get(impl)
		return err
	}
}

// WithZstdLimits replaces the cache's zstd implementation with a bounded
// pure-Go implementation whose streaming encoders (used for on-demand
// compression of identity-stored CAS blobs) are counted against a fixed
// admission budget. See zstdimpl.ZstdLimits for the semantics of each
// field. This option is mutually exclusive with selecting the "cgo"
// implementation via WithZstdImplementation; the last applied option wins.
func WithZstdLimits(limits zstdimpl.ZstdLimits) Option {
	return func(c *CacheConfig) error {
		impl, err := zstdimpl.NewBoundedGoZstd(limits)
		if err != nil {
			return err
		}
		c.diskCache.zstd = impl
		return nil
	}
}

// WithMaxEntries bounds the number of entries resident in the cache index,
// evicting least-recently-used entries when the bound is exceeded - the same
// semantics as the byte budget, but counting entries. Each resident entry
// costs a fixed ~270 bytes of process memory (key string, entry struct, list
// node, map slot) regardless of blob size, while charging at most one 4 KiB
// block against the byte budget (zero-byte blobs charge nothing), so without
// a count bound the index metadata of a byte-full cache is effectively
// unbounded. n <= 0 (the default) disables the bound.
func WithMaxEntries(n int64) Option {
	return func(c *CacheConfig) error {
		c.maxEntries = n
		return nil
	}
}

func WithMaxBlobSize(size int64) Option {
	return func(c *CacheConfig) error {
		if size <= 0 {
			return fmt.Errorf("invalid MaxBlobSize: %d", size)
		}

		c.diskCache.maxBlobSize = size
		return nil
	}
}

func WithProxyBackend(proxy cache.Proxy) Option {
	return func(c *CacheConfig) error {
		if c.diskCache.proxy != nil && proxy != nil {
			return fmt.Errorf("proxy backends may be set only once")
		}

		if proxy != nil {
			c.diskCache.proxy = proxy
			c.diskCache.initContainsCheckLimiter()
		}

		return nil
	}
}

func WithProxyMaxBlobSize(maxProxyBlobSize int64) Option {
	return func(c *CacheConfig) error {
		if maxProxyBlobSize <= 0 {
			return fmt.Errorf("invalid MaxProxyBlobSize: %d", maxProxyBlobSize)
		}

		c.diskCache.maxProxyBlobSize = maxProxyBlobSize
		return nil
	}
}

func WithAccessLogger(logger *log.Logger) Option {
	return func(c *CacheConfig) error {
		c.diskCache.accessLogger = logger
		return nil
	}
}

func WithEndpointMetrics() Option {
	return func(c *CacheConfig) error {
		if c.metrics != nil && c.metrics.counter != nil {
			return fmt.Errorf("WithEndpointMetrics specified multiple times")
		}

		if c.metrics == nil {
			c.metrics = &metricsDecorator{}
		}
		c.metrics.counter = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bazel_remote_incoming_requests_total",
			Help: "The number of incoming cache requests",
		},
			[]string{"method", "kind", "status"})

		c.metrics.counter.WithLabelValues("get", "cas", "hit").Add(0)
		c.metrics.counter.WithLabelValues("get", "cas", "miss").Add(0)
		c.metrics.counter.WithLabelValues("contains", "cas", "hit").Add(0)
		c.metrics.counter.WithLabelValues("contains", "cas", "miss").Add(0)
		c.metrics.counter.WithLabelValues("get", "ac", "hit").Add(0)
		c.metrics.counter.WithLabelValues("get", "ac", "miss").Add(0)

		return nil
	}
}

func WithMaxSizeHardLimit(maxSizeHardLimit int64) Option {
	return func(cc *CacheConfig) error {
		cc.maxSizeHardLimit = maxSizeHardLimit
		return nil
	}
}

func WithOperationObserver(observer cache.OperationObserver) Option {
	return func(c *CacheConfig) error {
		if observer == nil {
			return nil
		}
		if c.metrics == nil {
			c.metrics = &metricsDecorator{}
		}
		c.metrics.observer = observer
		return nil
	}
}

// WithLRUObserver sets the (optional) sink for AC-access closures used to build
// LRU retention artifacts. Capture happens inside the diskCache (D15); a nil
// observer disables capture entirely with no behavior change.
func WithLRUObserver(observer cache.LRUObserver) Option {
	return func(c *CacheConfig) error {
		c.diskCache.lruObserver = observer
		return nil
	}
}
