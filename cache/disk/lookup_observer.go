package disk

import (
	"context"

	"github.com/buchgr/bazel-remote/v2/cache"
)

func (c *diskCache) observeLookup(ctx context.Context, kind cache.EntryKind, access cache.LookupAccess, source cache.LookupSource, result cache.LookupResult, ops uint64) {
	cache.ObserveLookupAttempt(ctx, c.lookupObserver, cache.LookupAttempt{
		Kind:   kind,
		Access: access,
		Source: source,
		Result: result,
		Ops:    ops,
	})
}
