package disk

import (
	"context"
	"errors"
	"io"
	"os"
	"path"

	"github.com/buchgr/bazel-remote/v2/cache"
)

var errOwedBlobUnavailable = errors.New("owed blob not present in local cache")

// OpenOwedBlob implements s3proxy.BlobSource: it reopens a committed blob's
// raw on-disk representation (header + compression included — the same bytes
// Put streams to the proxy) for a deferred backend upload, returning the
// reader and the on-disk size. ctx must carry the entry's request-scoped
// storage prefix when it had one, because both the lookup key and the file
// location are prefix-scoped.
//
// An evicted or unreadable blob returns an error. Callers treat that as
// debt-settled: once the blob is gone locally, FindMissingBlobs reports it
// honestly missing and the normal client re-upload path restores both tiers.
func (c *diskCache) OpenOwedBlob(ctx context.Context, kind cache.EntryKind, hash string) (io.ReadCloser, int64, error) {
	key := cache.LookupKeyForContext(ctx, kind, hash)

	c.mu.Lock()
	item, listElem := c.lru.Get(key)
	if listElem == nil {
		c.mu.Unlock()
		return nil, -1, errOwedBlobUnavailable
	}
	blobPath := path.Join(c.dir, c.FileLocationForContext(ctx, kind, item.legacy, hash, item.size, item.random))
	f, err := os.Open(blobPath)
	c.mu.Unlock()

	if err != nil {
		return nil, -1, errOwedBlobUnavailable
	}
	return f, item.sizeOnDisk, nil
}
