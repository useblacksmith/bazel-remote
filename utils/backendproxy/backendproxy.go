package backendproxy

import (
	"io"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
)

type UploadReq struct {
	Hash        string
	LogicalSize int64
	SizeOnDisk  int64
	Kind        cache.EntryKind
	Rc          io.ReadCloser
	// EnqueuedAt is stamped by asynchronous backends immediately before the
	// queue send. Workers use it to observe active-work drain delay without
	// exporting fleet-wide queue depth.
	EnqueuedAt time.Time
	// StoragePrefix captures the request-scoped physical object-key prefix at
	// enqueue time. Uploads are asynchronous, so backends cannot rely on the
	// original request context still being available when workers process this.
	StoragePrefix              string
	RequestScopedStoragePrefix bool
	RequireStoragePrefix       bool
	MetricsLabels              cache.MetricsLabels
}

type Uploader interface {
	UploadFile(item UploadReq)
}

func StartUploaders(u Uploader, numUploaders int, maxQueuedUploads int) chan UploadReq {
	if maxQueuedUploads <= 0 || numUploaders <= 0 {
		return nil
	}

	uploadQueue := make(chan UploadReq, maxQueuedUploads)

	for i := 0; i < numUploaders; i++ {
		go func() {
			for item := range uploadQueue {
				u.UploadFile(item)
			}
		}()
	}

	return uploadQueue
}
