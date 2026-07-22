package disk

import (
	"context"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	testutils "github.com/buchgr/bazel-remote/v2/utils"
)

type runtimeMetricsRecorder struct {
	waiting atomic.Int64
	active  atomic.Int64
}

func (m *runtimeMetricsRecorder) ProxyGetWaitingChanged(_ context.Context, delta int64) {
	m.waiting.Add(delta)
}

func (m *runtimeMetricsRecorder) ProxyGetActiveChanged(_ context.Context, delta int64) {
	m.active.Add(delta)
}

func (m *runtimeMetricsRecorder) ProxyGetAdmissionWait(context.Context, time.Duration) {}

type blockingProxy struct {
	started chan struct{}
	release chan struct{}
}

func (p *blockingProxy) Put(context.Context, cache.EntryKind, string, int64, int64, io.ReadCloser) {}

func (p *blockingProxy) Get(ctx context.Context, _ cache.EntryKind, _ string, size int64) (io.ReadCloser, int64, error) {
	p.started <- struct{}{}
	select {
	case <-p.release:
		return io.NopCloser(&zeroReader{remaining: size}), size, nil
	case <-ctx.Done():
		return nil, -1, ctx.Err()
	}
}

func (p *blockingProxy) Contains(context.Context, cache.EntryKind, string, int64) (bool, int64) {
	return true, BlockSize
}

type zeroReader struct {
	remaining int64
}

func (r *zeroReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := int64(len(p))
	if n > r.remaining {
		n = r.remaining
	}
	for i := int64(0); i < n; i++ {
		p[i] = 0
	}
	r.remaining -= n
	return int(n), nil
}

func TestProxyGetLimitIsContextAwareAndObservable(t *testing.T) {
	cacheDir := tempDir(t)
	defer func() { _ = os.RemoveAll(cacheDir) }()

	proxy := &blockingProxy{
		started: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	metrics := &runtimeMetricsRecorder{}
	testCache, err := New(
		cacheDir,
		10*BlockSize,
		WithProxyBackend(proxy),
		WithMaxConcurrentProxyGets(1),
		WithRuntimeMetrics(metrics),
		WithStorageMode("uncompressed"),
		WithAccessLogger(testutils.NewSilentLogger()),
	)
	if err != nil {
		t.Fatal(err)
	}

	var results sync.WaitGroup
	results.Add(1)
	go func() {
		defer results.Done()
		r, _, _ := testCache.Get(context.Background(), cache.CAS, stringsOf("a", sha256HashStrSize), BlockSize, 0)
		if r != nil {
			_ = r.Close()
		}
	}()

	select {
	case <-proxy.started:
	case <-time.After(time.Second):
		t.Fatal("first proxy Get did not start")
	}
	if got := metrics.active.Load(); got != 1 {
		t.Fatalf("active proxy Gets = %d, want 1", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		r, _, err := testCache.Get(ctx, cache.CAS, stringsOf("b", sha256HashStrSize), BlockSize, 0)
		if r != nil {
			_ = r.Close()
		}
		secondDone <- err
	}()

	deadline := time.Now().Add(time.Second)
	for metrics.waiting.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := metrics.waiting.Load(); got != 1 {
		t.Fatalf("waiting proxy Gets = %d, want 1", got)
	}
	cancel()
	select {
	case err := <-secondDone:
		if err == nil {
			t.Fatal("expected canceled waiting Get to fail")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled waiting Get did not return")
	}

	close(proxy.release)
	results.Wait()
	if got := metrics.active.Load(); got != 0 {
		t.Fatalf("active proxy Gets after completion = %d, want 0", got)
	}
}

func stringsOf(char string, count int) string {
	b := make([]byte, count)
	for i := range b {
		b[i] = char[0]
	}
	return string(b)
}
