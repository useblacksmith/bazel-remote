package disk

import (
	"context"
	"time"

	"golang.org/x/sync/semaphore"
)

// RuntimeMetrics receives low-cardinality process-level measurements for
// proxied cache reads. Implementations must be safe for concurrent use.
type RuntimeMetrics interface {
	ProxyGetWaitingChanged(ctx context.Context, delta int64)
	ProxyGetActiveChanged(ctx context.Context, delta int64)
	ProxyGetAdmissionWait(ctx context.Context, duration time.Duration)
}

func (c *diskCache) acquireProxyGet(ctx context.Context) error {
	waitStarted := time.Now()
	if c.runtimeMetrics != nil {
		c.runtimeMetrics.ProxyGetWaitingChanged(ctx, 1)
		defer c.runtimeMetrics.ProxyGetWaitingChanged(ctx, -1)
		defer func() {
			c.runtimeMetrics.ProxyGetAdmissionWait(ctx, time.Since(waitStarted))
		}()
	}

	if c.proxyGetSem != nil {
		if err := c.proxyGetSem.Acquire(ctx, 1); err != nil {
			return err
		}
	}

	if err := c.diskWaitSem.Acquire(ctx, 1); err != nil {
		if c.proxyGetSem != nil {
			c.proxyGetSem.Release(1)
		}
		return err
	}

	if c.runtimeMetrics != nil {
		c.runtimeMetrics.ProxyGetActiveChanged(ctx, 1)
	}
	return nil
}

func (c *diskCache) releaseProxyGet(ctx context.Context) {
	if c.runtimeMetrics != nil {
		c.runtimeMetrics.ProxyGetActiveChanged(ctx, -1)
	}
	c.diskWaitSem.Release(1)
	if c.proxyGetSem != nil {
		c.proxyGetSem.Release(1)
	}
}

func newProxyGetLimiter(limit int64) *semaphore.Weighted {
	return semaphore.NewWeighted(limit)
}
