package disk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	testutils "github.com/buchgr/bazel-remote/v2/utils"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/proto"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
)

func TestFilterNonNIl(t *testing.T) {
	t.Parallel()

	blob1 := pb.Digest{
		Hash:      "7f715e87ab77cfa3084ce8f7bb8f51e4059d02147b2139635673b7751004a170",
		SizeBytes: 152,
	}
	blob2 := pb.Digest{
		Hash:      "3db63cc7c4972b451c075f1ee198f4c02d8e5ec065f04b5d7b6cb2ba3aeb8ca6",
		SizeBytes: 136,
	}
	blob3 := pb.Digest{
		Hash:      "9205adc12a2c8b65e7cd77918ff8e6e20f39bdd0b7fc4b984abfd690c79d80c1",
		SizeBytes: 217,
	}

	tcs := []struct {
		input    []*pb.Digest
		expected map[*pb.Digest]struct{}
	}{
		{
			[]*pb.Digest{},
			map[*pb.Digest]struct{}{},
		},
		{
			[]*pb.Digest{nil},
			map[*pb.Digest]struct{}{},
		},
		{
			[]*pb.Digest{nil, nil, nil},
			map[*pb.Digest]struct{}{},
		},
		{
			[]*pb.Digest{nil, &blob1, &blob2, nil, &blob3},
			map[*pb.Digest]struct{}{
				&blob1: {},
				&blob2: {},
				&blob3: {},
			},
		},
	}

	for _, tc := range tcs {
		output := filterNonNil(tc.input)

		if len(output) != len(tc.expected) {
			t.Errorf("Expected %d items, found %d",
				len(tc.expected), len(output))
		}

		for _, ptr := range output {
			if ptr == nil {
				t.Errorf("Found nil pointer in output")
			}
		}

		for _, ptr := range tc.input {
			if ptr == nil {
				continue
			}

			_, exists := tc.expected[ptr]
			if !exists {
				t.Errorf("Expected to find %q in output", ptr)
			}
		}
	}
}

type testCWProxy struct {
	blob string
}

func (p *testCWProxy) Put(ctx context.Context, kind cache.EntryKind, hash string, logicalSize int64, sizeOnDisk int64, rc io.ReadCloser) {
}

func (p *testCWProxy) Get(ctx context.Context, kind cache.EntryKind, hash string, _ int64) (io.ReadCloser, int64, error) {
	return nil, -1, nil
}

func (p *testCWProxy) Contains(ctx context.Context, kind cache.EntryKind, hash string, _ int64) (bool, int64) {
	if kind == cache.CAS && hash == p.blob {
		return true, 42
	}
	return false, -1
}

func TestProcessContainsCheck(t *testing.T) {
	t.Parallel()

	tp := testCWProxy{blob: "9205adc12a2c8b65e7cd77918ff8e6e20f39bdd0b7fc4b984abfd690c79d80c1"}

	c := diskCache{
		accessLogger: testutils.NewSilentLogger(),
		proxy:        &tp,
	}

	digests := []*pb.Digest{
		// Expect this to be found in the proxy, and replaced with nil.
		{Hash: tp.blob, SizeBytes: 42},

		// Expect this not to be found in the proxy, and left unchanged.
		{Hash: "423789fae66b9539c5622134c580700a154a15e355af4e3311a4e12ee0c9d243", SizeBytes: 43},

		// Expect this to be left unchanged: its context is already
		// cancelled, so the proxy must not be consulted at all.
		{Hash: tp.blob, SizeBytes: 42},
	}

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	var wg sync.WaitGroup
	wg.Add(3)
	c.processContainsCheck(proxyCheck{wg: &wg, digest: &digests[0]})
	c.processContainsCheck(proxyCheck{wg: &wg, digest: &digests[1]})
	c.processContainsCheck(proxyCheck{wg: &wg, digest: &digests[2], ctx: cancelledCtx})
	wg.Wait()

	if digests[0] != nil {
		t.Error("Expected digests[0] to be found in the proxy and replaced by nil")
	}

	if digests[1] == nil {
		t.Error("Expected digests[1] to not be found in the proxy and left as-is")
	}

	if digests[2] == nil {
		t.Error("Expected digests[2] to be skipped due to cancelled context and left as-is")
	}
}

type proxyAdapter struct {
	cache Cache
}

func NewProxyAdapter(cache Cache) (*proxyAdapter, error) {
	if cache == nil {
		return nil, fmt.Errorf("cache cannot be nil")
	}
	return &proxyAdapter{
		cache: cache,
	}, nil
}

func (p *proxyAdapter) Put(ctx context.Context, kind cache.EntryKind, hash string, logicalSize int64, sizeOnDisk int64, rc io.ReadCloser) {
	err := p.cache.Put(ctx, kind, hash, logicalSize, rc)
	if err != nil {
		panic(err)
	}
}

func (p *proxyAdapter) Get(ctx context.Context, kind cache.EntryKind, hash string, _ int64) (rc io.ReadCloser, size int64, err error) {
	return p.cache.Get(ctx, kind, hash, size, 0)
}

func (p *proxyAdapter) Contains(ctx context.Context, kind cache.EntryKind, hash string, _ int64) (bool, int64) {
	return p.cache.Contains(ctx, kind, hash, -1)
}

func TestFindMissingCasBlobsWithProxy(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheDir := tempDir(t)
	defer os.RemoveAll(cacheDir)
	proxyCacheDir := tempDir(t)
	defer os.RemoveAll(proxyCacheDir)

	cacheForProxy, err := New(proxyCacheDir, 10*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	proxy, err := NewProxyAdapter(cacheForProxy)
	if err != nil {
		t.Fatal(err)
	}

	testCache, err := New(cacheDir, 10*1024, WithProxyBackend(proxy), WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	data1, digest1 := testutils.RandomDataAndDigest(100)
	_, digest2 := testutils.RandomDataAndDigest(200)
	data3, digest3 := testutils.RandomDataAndDigest(300)
	_, digest4 := testutils.RandomDataAndDigest(400)

	proxy.Put(ctx, cache.CAS, digest1.Hash, digest1.SizeBytes, digest1.SizeBytes, io.NopCloser(bytes.NewReader(data1)))
	proxy.Put(ctx, cache.CAS, digest3.Hash, digest3.SizeBytes, digest3.SizeBytes, io.NopCloser(bytes.NewReader(data3)))

	missing, err := testCache.FindMissingCasBlobs(ctx, []*pb.Digest{
		&digest1,
		&digest2,
		&digest3,
		&digest4,
	})

	if err != nil {
		t.Fatal(err)
	}

	if len(missing) != 2 {
		t.Fatalf("Expected missing array to have exactly two entries, got %d", len(missing))
	}

	if !proto.Equal(missing[0], &digest2) {
		t.Fatalf("Expected missing[0] == digest2, got: %+v", missing[0])
	}

	if !proto.Equal(missing[1], &digest4) {
		t.Fatalf("Expected missing[1] == digest4, got: %+v", missing[1])
	}
}

func TestFindMissingCasBlobsWithProxyFailFast(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheDir := tempDir(t)
	defer os.RemoveAll(cacheDir)
	proxyCacheDir := tempDir(t)
	defer os.RemoveAll(proxyCacheDir)

	cacheForProxy, err := New(proxyCacheDir, 10*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	proxy, err := NewProxyAdapter(cacheForProxy)
	if err != nil {
		t.Fatal(err)
	}

	// Explicitly avoid using WithProxyBackEnd, as we want to control the concurrency limit.
	testCacheI, err := New(cacheDir, 10*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	actualDiskCache := testCacheI.(*diskCache)
	actualDiskCache.proxy = proxy
	// Allow only a single Contains check in flight at a time.
	actualDiskCache.containsSem = semaphore.NewWeighted(1)

	data1, digest1 := testutils.RandomDataAndDigest(100)
	_, digest2 := testutils.RandomDataAndDigest(200)
	data3, digest3 := testutils.RandomDataAndDigest(300)
	_, digest4 := testutils.RandomDataAndDigest(400)

	proxy.Put(ctx, cache.CAS, digest1.Hash, digest1.SizeBytes, digest1.SizeBytes, io.NopCloser(bytes.NewReader(data1)))
	proxy.Put(ctx, cache.CAS, digest3.Hash, digest3.SizeBytes, digest3.SizeBytes, io.NopCloser(bytes.NewReader(data3)))

	blobs := []*pb.Digest{
		&digest1,
		&digest2,
		&digest3,
		&digest4,
	}
	err = actualDiskCache.findMissingCasBlobsInternal(ctx, blobs, true)

	if !errors.Is(err, errMissingBlob) {
		t.Fatalf("Expected err to be errMissingBlob, got: %s", err)
	}

	if proto.Equal(blobs[0], &digest1) {
		t.Fatalf("Expected blobs[0] to equal digest1, got: %+v", blobs[0])
	}
}

func TestFindMissingCasBlobsWithProxyFailFastNoneMissing(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheDir := tempDir(t)
	defer os.RemoveAll(cacheDir)
	proxyCacheDir := tempDir(t)
	defer os.RemoveAll(proxyCacheDir)

	cacheForProxy, err := New(proxyCacheDir, 40*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	proxy, err := NewProxyAdapter(cacheForProxy)
	if err != nil {
		t.Fatal(err)
	}

	// Explicitly avoid using WithProxyBackEnd, as we want to control the concurrency limit.
	testCacheI, err := New(cacheDir, 40*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	actualDiskCache := testCacheI.(*diskCache)
	actualDiskCache.proxy = proxy
	// Allow only a single Contains check in flight at a time.
	actualDiskCache.containsSem = semaphore.NewWeighted(1)

	data1, digest1 := testutils.RandomDataAndDigest(100)
	data2, digest2 := testutils.RandomDataAndDigest(200)
	data3, digest3 := testutils.RandomDataAndDigest(300)
	data4, digest4 := testutils.RandomDataAndDigest(400)

	proxy.Put(ctx, cache.CAS, digest1.Hash, digest1.SizeBytes, digest1.SizeBytes, io.NopCloser(bytes.NewReader(data1)))
	proxy.Put(ctx, cache.CAS, digest2.Hash, digest2.SizeBytes, digest2.SizeBytes, io.NopCloser(bytes.NewReader(data2)))
	proxy.Put(ctx, cache.CAS, digest3.Hash, digest3.SizeBytes, digest3.SizeBytes, io.NopCloser(bytes.NewReader(data3)))
	proxy.Put(ctx, cache.CAS, digest4.Hash, digest4.SizeBytes, digest4.SizeBytes, io.NopCloser(bytes.NewReader(data4)))

	blobs := []*pb.Digest{
		&digest1,
		&digest2,
		&digest3,
		&digest4,
	}

	err = actualDiskCache.findMissingCasBlobsInternal(ctx, blobs, true)

	if err != nil {
		t.Fatal(err)
	}

	if blobs[0] != nil {
		t.Fatalf("Expected blobs[0] to be nil, got: %+v", blobs[0])
	}

	if blobs[1] != nil {
		t.Fatalf("Expected blobs[1] to be nil, got: %+v", blobs[1])
	}

	if blobs[2] != nil {
		t.Fatalf("Expected blobs[3] to be nil, got: %+v", blobs[2])
	}

	if blobs[3] != nil {
		t.Fatalf("Expected blobs[3] to be nil, got: %+v", blobs[3])
	}
}

func TestFindMissingCasBlobsWithProxyFailFastMaxProxyBlobSize(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheDir := tempDir(t)
	defer os.RemoveAll(cacheDir)
	proxyCacheDir := tempDir(t)
	defer os.RemoveAll(proxyCacheDir)

	cacheForProxy, err := New(proxyCacheDir, 10*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	proxy, err := NewProxyAdapter(cacheForProxy)
	if err != nil {
		t.Fatal(err)
	}

	// Explicitly avoid using WithProxyBackEnd, as we want to control the concurrency limit.
	testCacheI, err := New(cacheDir, 10*1024, WithAccessLogger(testutils.NewSilentLogger()), WithProxyMaxBlobSize(300))
	if err != nil {
		t.Fatal(err)
	}
	actualDiskCache := testCacheI.(*diskCache)
	actualDiskCache.proxy = proxy
	// Allow only a single Contains check in flight at a time.
	actualDiskCache.containsSem = semaphore.NewWeighted(1)

	data1, digest1 := testutils.RandomDataAndDigest(100)
	data2, digest2 := testutils.RandomDataAndDigest(200)
	data3, digest3 := testutils.RandomDataAndDigest(300) // We expect this blob to not be found.

	// Put blobs directly into proxy backend, where it will not be filtered out.
	proxy.Put(ctx, cache.CAS, digest1.Hash, digest1.SizeBytes, digest1.SizeBytes, io.NopCloser(bytes.NewReader(data1)))
	proxy.Put(ctx, cache.CAS, digest2.Hash, digest2.SizeBytes, digest2.SizeBytes, io.NopCloser(bytes.NewReader(data2)))
	proxy.Put(ctx, cache.CAS, digest3.Hash, digest3.SizeBytes, digest3.SizeBytes, io.NopCloser(bytes.NewReader(data3)))

	blobs := []*pb.Digest{
		&digest1,
		&digest2,
		&digest3,
	}
	err = actualDiskCache.findMissingCasBlobsInternal(ctx, blobs, true)

	if !errors.Is(err, errMissingBlob) {
		t.Fatalf("Expected err to be errMissingBlob, got: %s", err)
	}

	if blobs[0] == nil {
		t.Fatalf("Expected blobs[0] to be nil, got: %+v", blobs[0])
	}

	if blobs[1] == nil {
		t.Fatalf("Expected blobs[1] to be nil, got: %+v", blobs[1])
	}

	if !proto.Equal(blobs[2], &digest3) {
		t.Fatalf("Expected blobs[2] == digest3, got %+v", blobs[2])
	}
}

func TestFindMissingCasBlobsWithProxyMaxProxyBlobSize(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheDir := tempDir(t)
	defer os.RemoveAll(cacheDir)
	proxyCacheDir := tempDir(t)
	defer os.RemoveAll(proxyCacheDir)

	cacheForProxy, err := New(proxyCacheDir, 10*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	proxy, err := NewProxyAdapter(cacheForProxy)
	if err != nil {
		t.Fatal(err)
	}

	testCache, err := New(cacheDir, 10*1024, WithProxyBackend(proxy), WithAccessLogger(testutils.NewSilentLogger()), WithProxyMaxBlobSize(500))
	if err != nil {
		t.Fatal(err)
	}

	data1, digest1 := testutils.RandomDataAndDigest(100)
	data2, digest2 := testutils.RandomDataAndDigest(600)

	proxy.Put(ctx, cache.CAS, digest1.Hash, digest1.SizeBytes, digest1.SizeBytes, io.NopCloser(bytes.NewReader(data1)))
	proxy.Put(ctx, cache.CAS, digest2.Hash, digest2.SizeBytes, digest2.SizeBytes, io.NopCloser(bytes.NewReader(data2)))

	missing, err := testCache.FindMissingCasBlobs(ctx, []*pb.Digest{
		&digest1,
		&digest2,
	})

	if err != nil {
		t.Fatal(err)
	}

	if len(missing) != 1 {
		t.Fatalf("Expected missing array to have exactly one entry, got %d", len(missing))
	}

	if !proto.Equal(missing[0], &digest2) {
		t.Fatalf("Expected missing[0] == digest2, got %+v", missing[0])
	}
}

// gatedProxy reports every blob as missing, blocking each Contains call until
// a token is sent on gate. It tracks the high-water mark of concurrent calls.
type gatedProxy struct {
	gate     chan struct{}
	inflight atomic.Int32
	high     atomic.Int32
}

func (p *gatedProxy) Put(ctx context.Context, kind cache.EntryKind, hash string, logicalSize int64, sizeOnDisk int64, rc io.ReadCloser) {
}

func (p *gatedProxy) Get(ctx context.Context, kind cache.EntryKind, hash string, _ int64) (io.ReadCloser, int64, error) {
	return nil, -1, nil
}

func (p *gatedProxy) Contains(ctx context.Context, kind cache.EntryKind, hash string, _ int64) (bool, int64) {
	n := p.inflight.Add(1)
	defer p.inflight.Add(-1)
	for {
		h := p.high.Load()
		if n <= h || p.high.CompareAndSwap(h, n) {
			break
		}
	}
	<-p.gate
	return false, -1
}

func waitForInflight(t *testing.T, p *gatedProxy, want int32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for p.inflight.Load() != want {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d in-flight Contains calls, have %d",
				want, p.inflight.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestFindMissingCasBlobsConcurrencyLimit(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheDir := tempDir(t)
	defer os.RemoveAll(cacheDir)

	testCacheI, err := New(cacheDir, 10*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	c := testCacheI.(*diskCache)

	const limit = 4
	const numBlobs = 32

	proxy := &gatedProxy{gate: make(chan struct{})}
	c.proxy = proxy
	c.containsSem = semaphore.NewWeighted(limit)

	blobs := make([]*pb.Digest, numBlobs)
	for i := range blobs {
		_, digest := testutils.RandomDataAndDigest(int64(100 + i))
		blobs[i] = &digest
	}

	done := make(chan error, 1)
	go func() {
		done <- c.findMissingCasBlobsInternal(ctx, blobs, false)
	}()

	// With every Contains call gated, the producer must saturate the
	// semaphore and park.
	waitForInflight(t, proxy, limit)

	for i := 0; i < numBlobs; i++ {
		proxy.gate <- struct{}{}
	}

	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if got := proxy.high.Load(); got != limit {
		t.Errorf("Expected high-water mark of concurrent Contains calls to be %d, got %d", limit, got)
	}

	if got := len(filterNonNil(blobs)); got != numBlobs {
		t.Errorf("Expected all %d blobs to be reported missing, got %d", numBlobs, got)
	}
}

// slowMissProxy reports every blob as missing after a fixed delay.
type slowMissProxy struct {
	delay time.Duration
}

func (p *slowMissProxy) Put(ctx context.Context, kind cache.EntryKind, hash string, logicalSize int64, sizeOnDisk int64, rc io.ReadCloser) {
}

func (p *slowMissProxy) Get(ctx context.Context, kind cache.EntryKind, hash string, _ int64) (io.ReadCloser, int64, error) {
	return nil, -1, nil
}

func (p *slowMissProxy) Contains(ctx context.Context, kind cache.EntryKind, hash string, _ int64) (bool, int64) {
	time.Sleep(p.delay)
	return false, -1
}

func TestFindMissingCasBlobsFailFastWakesBlockedProducer(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheDir := tempDir(t)
	defer os.RemoveAll(cacheDir)

	testCacheI, err := New(cacheDir, 10*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	c := testCacheI.(*diskCache)
	c.proxy = &slowMissProxy{delay: 50 * time.Millisecond}
	c.containsSem = semaphore.NewWeighted(1)

	_, digest1 := testutils.RandomDataAndDigest(100)
	_, digest2 := testutils.RandomDataAndDigest(200)
	_, digest3 := testutils.RandomDataAndDigest(300)

	// With a concurrency limit of 1, the first check occupies the
	// semaphore for the proxy delay while the producer parks in Acquire
	// for the second. The first check's miss triggers the fail-fast
	// cancellation, which must wake the parked producer.
	start := time.Now()
	err = c.findMissingCasBlobsInternal(ctx, []*pb.Digest{&digest1, &digest2, &digest3}, true)
	elapsed := time.Since(start)

	if !errors.Is(err, errMissingBlob) {
		t.Fatalf("Expected err to be errMissingBlob, got: %s", err)
	}

	if elapsed > 5*time.Second {
		t.Fatalf("Expected fail-fast to return promptly, took %s", elapsed)
	}
}

func TestFindMissingCasBlobsCancelWakesBlockedProducer(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())

	cacheDir := tempDir(t)
	defer os.RemoveAll(cacheDir)

	testCacheI, err := New(cacheDir, 10*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	c := testCacheI.(*diskCache)

	proxy := &gatedProxy{gate: make(chan struct{})}
	c.proxy = proxy
	c.containsSem = semaphore.NewWeighted(1)
	// Unblock the in-flight Contains call when the test finishes.
	defer close(proxy.gate)

	_, digest1 := testutils.RandomDataAndDigest(100)
	_, digest2 := testutils.RandomDataAndDigest(200)

	done := make(chan error, 1)
	go func() {
		done <- c.findMissingCasBlobsInternal(ctx, []*pb.Digest{&digest1, &digest2}, false)
	}()

	// Wait until the first check is in flight, so the producer is parked
	// in Acquire for the second, then cancel.
	waitForInflight(t, proxy, 1)
	cancel()

	if err := <-done; !errors.Is(err, errRequestCancelled) {
		t.Fatalf("Expected err to be errRequestCancelled, got: %s", err)
	}
}

func TestFindMissingCasBlobsConcurrentCallers(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheDir := tempDir(t)
	defer os.RemoveAll(cacheDir)

	testCacheI, err := New(cacheDir, 10*1024, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	c := testCacheI.(*diskCache)
	c.proxy = &testCWProxy{blob: "no-such-blob"}
	c.containsSem = semaphore.NewWeighted(2)

	const numCallers = 16
	const blobsPerCaller = 200

	var wg sync.WaitGroup
	errs := make(chan error, numCallers)
	for i := 0; i < numCallers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			blobs := make([]*pb.Digest, blobsPerCaller)
			for j := range blobs {
				_, digest := testutils.RandomDataAndDigest(int64(100 + j))
				blobs[j] = &digest
			}
			missing, err := c.FindMissingCasBlobs(ctx, blobs)
			if err != nil {
				errs <- err
				return
			}
			if len(missing) != blobsPerCaller {
				errs <- fmt.Errorf("expected %d missing blobs, got %d", blobsPerCaller, len(missing))
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
