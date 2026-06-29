package s3proxy

import (
	"bytes"
	"context"
	stdlog "log"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type captureSink struct {
	sizes     map[string]int64
	fromProxy map[string]bool
}

func (s *captureSink) RecordLeafSize(hash string, sizeOnDisk int64, fromProxy bool) {
	s.sizes[hash] = sizeOnDisk
	s.fromProxy[hash] = fromProxy
}

// TestContainsRecordsStoredSizeIntoSink verifies that a proxy existence check
// surfaces the stored object size (StatObject.Size) into the leaf-size sink
// for CAS/v2, while still returning -1 as the logical size to the validator.
func TestContainsRecordsStoredSizeIntoSink(t *testing.T) {
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

	c := &s3Cache{
		mcore:        core,
		bucket:       "test-bucket",
		v2mode:       true,
		objectKey:    objectKeyV2,
		accessLogger: stdlog.New(&bytes.Buffer{}, "", 0),
	}

	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	content := []byte("hello stored bytes")
	key := objectKeyV2("", hash, cache.CAS)
	if _, err := core.PutObject(context.Background(), "test-bucket", key,
		bytes.NewReader(content), int64(len(content)), "", "", minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}

	sink := &captureSink{sizes: map[string]int64{}, fromProxy: map[string]bool{}}
	ctx := cache.WithLeafSizeSink(context.Background(), sink)

	exists, size := c.Contains(ctx, cache.CAS, hash, -1)
	if !exists {
		t.Fatal("expected object to exist in proxy")
	}
	// CAS/v2 still returns -1 logical size to the validator (unchanged).
	if size != -1 {
		t.Fatalf("Contains returned size %d, want -1 for CAS/v2", size)
	}
	// ...but the stored size is surfaced to the sink for LRU capture.
	if got := sink.sizes[hash]; got != int64(len(content)) {
		t.Fatalf("sink recorded size %d, want %d", got, len(content))
	}
	if !sink.fromProxy[hash] {
		t.Fatalf("sink should mark the size as proxy-sourced")
	}
}

func TestContainsWithoutSinkIsNoop(t *testing.T) {
	backend := s3mem.New()
	if err := backend.CreateBucket("test-bucket"); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(gofakes3.New(backend).Server())
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	core, err := minio.NewCore(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4("KEY", "SECRET", ""),
		Secure:       false,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		t.Fatal(err)
	}

	c := &s3Cache{
		mcore:        core,
		bucket:       "test-bucket",
		v2mode:       true,
		objectKey:    objectKeyV2,
		accessLogger: stdlog.New(&bytes.Buffer{}, "", 0),
	}

	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	// No object stored, no sink in context: must simply report missing.
	exists, size := c.Contains(context.Background(), cache.CAS, hash, -1)
	if exists {
		t.Fatal("expected miss for absent object")
	}
	if size != -1 {
		t.Fatalf("Contains returned size %d, want -1", size)
	}
}
