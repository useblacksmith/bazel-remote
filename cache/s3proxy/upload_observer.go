package s3proxy

import (
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
)

// UploadStage identifies a transition in the asynchronous S3 upload lifecycle.
type UploadStage string

const (
	UploadStageEnqueued UploadStage = "enqueued"
	UploadStageDropped  UploadStage = "dropped"
	UploadStageStarted  UploadStage = "started"
	UploadStageFinished UploadStage = "finished"
)

// UploadEvent describes one asynchronous S3 upload lifecycle transition.
// QueueDepth and ActiveUploads are concurrent snapshots; lifecycle event counts
// are exact, but consumers must not treat snapshots as a totally ordered trace.
type UploadEvent struct {
	Stage           UploadStage
	Status          string
	Reason          string
	Kind            cache.EntryKind
	Labels          cache.MetricsLabels
	SizeOnDisk      int64
	QueueDepth      int
	QueueCapacity   int
	ActiveUploads   int64
	QueueWait       time.Duration
	BackendDuration time.Duration
}

// UploadObserver receives best-effort S3 upload lifecycle events. Implementations
// must be safe for concurrent calls and must not affect cache behavior.
type UploadObserver interface {
	RecordUploadEvent(event UploadEvent)
}

func observeUploadEvent(observer UploadObserver, event UploadEvent) {
	if observer == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	observer.RecordUploadEvent(event)
}
