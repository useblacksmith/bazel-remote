package s3proxy

import (
	"bytes"
	"context"
	"io"
	stdlog "log"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/utils/backendproxy"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type recordingUploadObserver struct {
	mu     sync.Mutex
	events []UploadEvent
}

func (r *recordingUploadObserver) RecordUploadEvent(event UploadEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *recordingUploadObserver) snapshot() []UploadEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]UploadEvent(nil), r.events...)
}

type panickingUploadObserver struct{}

func (panickingUploadObserver) RecordUploadEvent(UploadEvent) {
	panic("observer failure")
}

func TestPutRecordsEnqueuedAndDroppedUploadEvents(t *testing.T) {
	uploadQueue := make(chan backendproxy.UploadReq, 1)
	observer := &recordingUploadObserver{}
	c := &s3Cache{
		uploadQueue:    uploadQueue,
		errorLogger:    stdlog.New(io.Discard, "", 0),
		uploadObserver: observer,
	}

	hash := strings.Repeat("a", 64)
	c.Put(context.Background(), cache.CAS, hash, 4, 4, io.NopCloser(strings.NewReader("first")))
	c.Put(context.Background(), cache.AC, hash, 6, 6, io.NopCloser(strings.NewReader("second")))

	item := <-uploadQueue
	defer item.Rc.Close()
	if item.EnqueuedAt.IsZero() {
		t.Fatal("accepted upload did not capture enqueue time")
	}

	events := observer.snapshot()
	if len(events) != 2 {
		t.Fatalf("upload events len = %d, want 2", len(events))
	}
	if events[0].Stage != UploadStageEnqueued || events[0].Kind != cache.CAS {
		t.Fatalf("unexpected accepted event: %+v", events[0])
	}
	if events[0].QueueDepth != 1 || events[0].QueueCapacity != 1 {
		t.Fatalf("accepted queue snapshot = %d/%d, want 1/1", events[0].QueueDepth, events[0].QueueCapacity)
	}
	if events[1].Stage != UploadStageDropped || events[1].Status != "dropped" || events[1].Reason != "upload_queue_full" {
		t.Fatalf("unexpected dropped event: %+v", events[1])
	}
}

func TestUploadFileRecordsStartedAndFinishedEvents(t *testing.T) {
	backend := s3mem.New()
	if err := backend.CreateBucket("test-bucket"); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(gofakes3.New(backend).Server())
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	core, err := minio.NewCore(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4("KEY", "SECRET", ""),
		Secure:       false,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		t.Fatal(err)
	}

	observer := &recordingUploadObserver{}
	c := &s3Cache{
		mcore:          core,
		bucket:         "test-bucket",
		uploadQueue:    make(chan backendproxy.UploadReq, 2),
		objectKey:      objectKeyV1,
		accessLogger:   stdlog.New(io.Discard, "", 0),
		errorLogger:    stdlog.New(io.Discard, "", 0),
		uploadObserver: observer,
	}

	content := []byte("blob")
	c.UploadFile(backendproxy.UploadReq{
		Hash:        strings.Repeat("b", 64),
		LogicalSize: int64(len(content)),
		SizeOnDisk:  int64(len(content)),
		Kind:        cache.CAS,
		Rc:          io.NopCloser(bytes.NewReader(content)),
		EnqueuedAt:  time.Now().Add(-time.Millisecond),
	})

	events := observer.snapshot()
	if len(events) != 2 {
		t.Fatalf("upload events len = %d, want 2", len(events))
	}
	if events[0].Stage != UploadStageStarted || events[0].ActiveUploads != 1 || events[0].QueueWait <= 0 {
		t.Fatalf("unexpected started event: %+v", events[0])
	}
	if events[1].Stage != UploadStageFinished || events[1].Status != "created" || events[1].ActiveUploads != 0 {
		t.Fatalf("unexpected finished event: %+v", events[1])
	}
	if events[1].BackendDuration <= 0 {
		t.Fatalf("finished event backend duration = %s, want positive", events[1].BackendDuration)
	}
}

func TestUploadObserverPanicDoesNotAffectPut(t *testing.T) {
	uploadQueue := make(chan backendproxy.UploadReq, 1)
	c := &s3Cache{
		uploadQueue:    uploadQueue,
		uploadObserver: panickingUploadObserver{},
	}

	c.Put(context.Background(), cache.CAS, strings.Repeat("c", 64), 4, 4, io.NopCloser(strings.NewReader("blob")))
	item := <-uploadQueue
	defer item.Rc.Close()
}
