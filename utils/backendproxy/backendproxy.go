package backendproxy

import (
	"io"

	"github.com/buchgr/bazel-remote/v2/cache"
)

type UploadReq struct {
	Hash        string
	LogicalSize int64
	SizeOnDisk  int64
	Kind        cache.EntryKind
	Rc          io.ReadCloser
	// StoragePrefix captures the request-scoped physical object-key prefix at
	// enqueue time. Uploads are asynchronous, so backends cannot rely on the
	// original request context still being available when workers process this.
	StoragePrefix              string
	RequestScopedStoragePrefix bool
	RequireStoragePrefix       bool
	// S3Backend captures the request-scoped (endpoint, bucket) selection at
	// enqueue time, for the same reason as StoragePrefix above: grpcproxy
	// upload workers re-attach both halves as outgoing metadata so the
	// downstream L1 routes the write-through to the right backend and
	// bucket. The s3proxy's multi-backend router settles the endpoint half
	// at enqueue time (dispatch to the selected backend's own queue), but
	// its upload workers still need the bucket half — the bucket is per
	// request, not per backend.
	S3Backend     cache.S3BackendSelection
	MetricsLabels cache.MetricsLabels
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
