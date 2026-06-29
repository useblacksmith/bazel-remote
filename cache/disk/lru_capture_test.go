package disk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	testutils "github.com/buchgr/bazel-remote/v2/utils"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/protobuf/proto"
)

type recordingLRUObserver struct {
	mu       sync.Mutex
	closures []cache.ACClosure
}

func (r *recordingLRUObserver) RecordACAccess(_ context.Context, c cache.ACClosure) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closures = append(r.closures, c)
}

func (r *recordingLRUObserver) snapshot() []cache.ACClosure {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]cache.ACClosure(nil), r.closures...)
}

// sinkRecordingProxy mimics s3proxy for CAS/v2: it reports existence, records
// the stored size into the leaf-size sink, and returns -1 as the logical size.
type sinkRecordingProxy struct {
	mu            sync.Mutex
	objects       map[string]int64 // hash -> sizeOnDisk to report
	containsCalls int
}

func (p *sinkRecordingProxy) Put(context.Context, cache.EntryKind, string, int64, int64, io.ReadCloser) {
}

func (p *sinkRecordingProxy) Get(context.Context, cache.EntryKind, string, int64) (io.ReadCloser, int64, error) {
	return nil, -1, nil
}

func (p *sinkRecordingProxy) Contains(ctx context.Context, kind cache.EntryKind, hash string, _ int64) (bool, int64) {
	p.mu.Lock()
	p.containsCalls++
	sz, ok := p.objects[hash]
	p.mu.Unlock()
	if !ok {
		return false, -1
	}
	if sink, ok := cache.LeafSizeSinkFromContext(ctx); ok {
		sink.RecordLeafSize(hash, sz, true)
	}
	return true, -1
}

func (p *sinkRecordingProxy) calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.containsCalls
}

func putCAS(t *testing.T, c *diskCache, data []byte) *pb.Digest {
	t.Helper()
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])
	if err := c.Put(context.Background(), cache.CAS, hash, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatalf("put CAS %s: %v", hash, err)
	}
	return &pb.Digest{Hash: hash, SizeBytes: int64(len(data))}
}

func putAC(t *testing.T, c *diskCache, ar *pb.ActionResult) string {
	t.Helper()
	data, err := proto.Marshal(ar)
	if err != nil {
		t.Fatalf("marshal AC: %v", err)
	}
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])
	if err := c.Put(context.Background(), cache.AC, hash, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatalf("put AC %s: %v", hash, err)
	}
	return hash
}

func leafByHash(closure cache.ACClosure, hash string) (cache.LRUObject, bool) {
	for _, l := range closure.Leaves {
		if l.Hash == hash {
			return l, true
		}
	}
	return cache.LRUObject{}, false
}

func TestGetValidatedActionResultEmitsClosureOnHit(t *testing.T) {
	ctx := context.Background()
	cacheDir := testutils.TempDir(t)
	defer os.RemoveAll(cacheDir)

	obs := &recordingLRUObserver{}
	ci, err := New(cacheDir, 1024*32,
		WithAccessLogger(testutils.NewSilentLogger()),
		WithLRUObserver(obs))
	if err != nil {
		t.Fatal(err)
	}
	c := ci.(*diskCache)

	fooData := []byte("foo test data")
	grokData := []byte("grok test data")
	foo := putCAS(t, c, fooData)
	grok := putCAS(t, c, grokData)

	barDir := pb.Directory{Files: []*pb.FileNode{
		{Name: "foo.txt", Digest: foo},
		{Name: "grok.txt", Digest: grok},
	}}
	barData, _ := proto.Marshal(&barDir)
	tree := pb.Tree{Root: &pb.Directory{}, Children: []*pb.Directory{&barDir}}
	treeData, _ := proto.Marshal(&tree)
	treeDigest := putCAS(t, c, treeData)
	_ = barData

	acHash := putAC(t, c, &pb.ActionResult{
		OutputFiles: []*pb.OutputFile{{Path: "foo.txt", Digest: foo}},
		OutputDirectories: []*pb.OutputDirectory{
			{Path: "", TreeDigest: treeDigest},
		},
	})

	if _, _, err := c.GetValidatedActionResult(ctx, acHash); err != nil {
		t.Fatal(err)
	}

	closures := obs.snapshot()
	if len(closures) != 1 {
		t.Fatalf("expected 1 closure, got %d", len(closures))
	}
	cl := closures[0]
	if cl.AC.Hash != acHash {
		t.Fatalf("closure AC hash = %s, want %s", cl.AC.Hash, acHash)
	}
	if cl.AC.SizeOnDisk <= 0 {
		t.Fatalf("closure AC sizeOnDisk = %d, want > 0", cl.AC.SizeOnDisk)
	}
	if cl.TSMillis <= 0 {
		t.Fatalf("closure ts_ms = %d, want > 0", cl.TSMillis)
	}

	// Expect foo, grok (output file + tree leaf, deduped) and the Tree blob.
	for _, want := range []*pb.Digest{foo, grok, treeDigest} {
		leaf, ok := leafByHash(cl, want.Hash)
		if !ok {
			t.Fatalf("closure missing leaf %s; leaves=%+v", want.Hash, cl.Leaves)
		}
		expected, found := c.lookupSizeOnDisk(ctx, cache.CAS, want.Hash)
		if !found || leaf.SizeOnDisk != expected {
			t.Fatalf("leaf %s sizeOnDisk = %d, want %d (index)", want.Hash, leaf.SizeOnDisk, expected)
		}
	}
	if len(cl.Leaves) != 3 {
		t.Fatalf("expected 3 deduped leaves, got %d: %+v", len(cl.Leaves), cl.Leaves)
	}
}

func TestGetValidatedActionResultLeafSizeSource(t *testing.T) {
	ctx := context.Background()
	cacheDir := testutils.TempDir(t)
	defer os.RemoveAll(cacheDir)

	const proxyLeafSize = int64(4242)
	bazData, bazDigest := testutils.RandomDataAndDigest(900)
	_ = bazData
	proxy := &sinkRecordingProxy{objects: map[string]int64{bazDigest.Hash: proxyLeafSize}}

	obs := &recordingLRUObserver{}
	ci, err := New(cacheDir, 1024*64,
		WithAccessLogger(testutils.NewSilentLogger()),
		WithProxyBackend(proxy),
		WithLRUObserver(obs))
	if err != nil {
		t.Fatal(err)
	}
	c := ci.(*diskCache)

	// foo is local; baz lives only in the proxy.
	foo := putCAS(t, c, []byte("local foo data"))

	beforeLocal := testutil.ToFloat64(lruLeafSizeSource.WithLabelValues(leafSizeSourceLocal))
	beforeProxy := testutil.ToFloat64(lruLeafSizeSource.WithLabelValues(leafSizeSourceProxy))

	acHash := putAC(t, c, &pb.ActionResult{
		OutputFiles: []*pb.OutputFile{
			{Path: "foo.txt", Digest: foo},
			{Path: "baz.txt", Digest: &bazDigest},
		},
	})
	if _, _, err := c.GetValidatedActionResult(ctx, acHash); err != nil {
		t.Fatal(err)
	}

	closures := obs.snapshot()
	if len(closures) != 1 {
		t.Fatalf("expected 1 closure, got %d", len(closures))
	}
	cl := closures[0]

	localLeaf, ok := leafByHash(cl, foo.Hash)
	if !ok {
		t.Fatalf("missing local leaf %s", foo.Hash)
	}
	expectedLocal, _ := c.lookupSizeOnDisk(ctx, cache.CAS, foo.Hash)
	if localLeaf.SizeOnDisk != expectedLocal {
		t.Fatalf("local leaf size = %d, want %d", localLeaf.SizeOnDisk, expectedLocal)
	}

	proxyLeaf, ok := leafByHash(cl, bazDigest.Hash)
	if !ok {
		t.Fatalf("missing proxy leaf %s", bazDigest.Hash)
	}
	if proxyLeaf.SizeOnDisk != proxyLeafSize {
		t.Fatalf("proxy leaf size = %d, want %d (StatObject.Size)", proxyLeaf.SizeOnDisk, proxyLeafSize)
	}

	if got := testutil.ToFloat64(lruLeafSizeSource.WithLabelValues(leafSizeSourceLocal)); got <= beforeLocal {
		t.Fatalf("local source counter did not increment: before %v, after %v", beforeLocal, got)
	}
	if got := testutil.ToFloat64(lruLeafSizeSource.WithLabelValues(leafSizeSourceProxy)); got <= beforeProxy {
		t.Fatalf("proxy source counter did not increment: before %v, after %v", beforeProxy, got)
	}
}

func TestGetValidatedActionResultNoExtraProxyStat(t *testing.T) {
	ctx := context.Background()

	build := func(obs cache.LRUObserver) (*diskCache, *sinkRecordingProxy, *pb.Digest) {
		cacheDir := testutils.TempDir(t)
		t.Cleanup(func() { os.RemoveAll(cacheDir) })

		bazData, bazDigest := testutils.RandomDataAndDigest(900)
		_ = bazData
		proxy := &sinkRecordingProxy{objects: map[string]int64{bazDigest.Hash: 100}}

		opts := []Option{WithAccessLogger(testutils.NewSilentLogger()), WithProxyBackend(proxy)}
		if obs != nil {
			opts = append(opts, WithLRUObserver(obs))
		}
		ci, err := New(cacheDir, 1024*64, opts...)
		if err != nil {
			t.Fatal(err)
		}
		return ci.(*diskCache), proxy, &bazDigest
	}

	run := func(c *diskCache, baz *pb.Digest) {
		foo := putCAS(t, c, []byte("local foo data"))
		acHash := putAC(t, c, &pb.ActionResult{OutputFiles: []*pb.OutputFile{
			{Path: "foo.txt", Digest: foo},
			{Path: "baz.txt", Digest: baz},
		}})
		if _, _, err := c.GetValidatedActionResult(ctx, acHash); err != nil {
			t.Fatal(err)
		}
	}

	withObs := &recordingLRUObserver{}
	c1, p1, baz1 := build(withObs)
	run(c1, baz1)

	c2, p2, baz2 := build(nil)
	run(c2, baz2)

	if p1.calls() != p2.calls() {
		t.Fatalf("capture changed proxy Contains count: with observer %d, without %d", p1.calls(), p2.calls())
	}
	if len(withObs.snapshot()) != 1 {
		t.Fatalf("expected observer to capture 1 closure, got %d", len(withObs.snapshot()))
	}
}

func TestPutActionResultEmitsClosureWithoutDirectories(t *testing.T) {
	ctx := context.Background()
	cacheDir := testutils.TempDir(t)
	defer os.RemoveAll(cacheDir)

	obs := &recordingLRUObserver{}
	ci, err := New(cacheDir, 1024*32,
		WithAccessLogger(testutils.NewSilentLogger()),
		WithLRUObserver(obs))
	if err != nil {
		t.Fatal(err)
	}
	c := ci.(*diskCache)

	foo := putCAS(t, c, []byte("foo write data"))
	acHash := putAC(t, c, &pb.ActionResult{
		OutputFiles: []*pb.OutputFile{{Path: "foo.txt", Digest: foo}},
	})

	closures := obs.snapshot()
	if len(closures) != 1 {
		t.Fatalf("expected 1 write closure, got %d", len(closures))
	}
	if closures[0].AC.Hash != acHash {
		t.Fatalf("write closure AC hash = %s, want %s", closures[0].AC.Hash, acHash)
	}
	leaf, ok := leafByHash(closures[0], foo.Hash)
	if !ok {
		t.Fatalf("write closure missing leaf %s", foo.Hash)
	}
	expected, _ := c.lookupSizeOnDisk(ctx, cache.CAS, foo.Hash)
	if leaf.SizeOnDisk != expected {
		t.Fatalf("write leaf size = %d, want %d", leaf.SizeOnDisk, expected)
	}
}

func TestPutActionResultWithDirectoriesEmitsNothing(t *testing.T) {
	cacheDir := testutils.TempDir(t)
	defer os.RemoveAll(cacheDir)

	obs := &recordingLRUObserver{}
	ci, err := New(cacheDir, 1024*32,
		WithAccessLogger(testutils.NewSilentLogger()),
		WithLRUObserver(obs))
	if err != nil {
		t.Fatal(err)
	}
	c := ci.(*diskCache)

	foo := putCAS(t, c, []byte("foo dir data"))
	tree := pb.Tree{Root: &pb.Directory{}}
	treeData, _ := proto.Marshal(&tree)
	treeDigest := putCAS(t, c, treeData)

	putAC(t, c, &pb.ActionResult{
		OutputFiles: []*pb.OutputFile{{Path: "foo.txt", Digest: foo}},
		OutputDirectories: []*pb.OutputDirectory{
			{Path: "out", TreeDigest: treeDigest},
		},
	})

	if got := len(obs.snapshot()); got != 0 {
		t.Fatalf("expected no write closure when output directories present, got %d", got)
	}
}

func TestPutActionResultCompleteOrDropOnMissingLeaf(t *testing.T) {
	cacheDir := testutils.TempDir(t)
	defer os.RemoveAll(cacheDir)

	obs := &recordingLRUObserver{}
	ci, err := New(cacheDir, 1024*32,
		WithAccessLogger(testutils.NewSilentLogger()),
		WithLRUObserver(obs))
	if err != nil {
		t.Fatal(err)
	}
	c := ci.(*diskCache)

	// Reference an output file that was never written to the CAS.
	_, missing := testutils.RandomDataAndDigest(500)
	putAC(t, c, &pb.ActionResult{
		OutputFiles: []*pb.OutputFile{{Path: "missing.txt", Digest: &missing}},
	})

	if got := len(obs.snapshot()); got != 0 {
		t.Fatalf("expected drop (no closure) for unresolved leaf, got %d", got)
	}
}

func TestNilObserverDoesNotCapture(t *testing.T) {
	ctx := context.Background()
	cacheDir := testutils.TempDir(t)
	defer os.RemoveAll(cacheDir)

	ci, err := New(cacheDir, 1024*32, WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}
	c := ci.(*diskCache)

	foo := putCAS(t, c, []byte("foo nil-observer"))
	acHash := putAC(t, c, &pb.ActionResult{
		OutputFiles: []*pb.OutputFile{{Path: "foo.txt", Digest: foo}},
	})
	// Must not panic and must behave as before with no observer wired.
	ar, _, err := c.GetValidatedActionResult(ctx, acHash)
	if err != nil {
		t.Fatal(err)
	}
	if ar == nil {
		t.Fatal("expected a validated action result")
	}
}
