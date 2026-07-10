package disk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"sync"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	testutils "github.com/buchgr/bazel-remote/v2/utils"
)

type recordingLookupObserver struct {
	mu       sync.Mutex
	attempts []cache.LookupAttempt
}

func (r *recordingLookupObserver) RecordLookupAttempt(_ context.Context, attempt cache.LookupAttempt) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts = append(r.attempts, attempt)
}

func (r *recordingLookupObserver) has(kind cache.EntryKind, access cache.LookupAccess, source cache.LookupSource, result cache.LookupResult) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, attempt := range r.attempts {
		if attempt.Kind == kind && attempt.Access == access && attempt.Source == source && attempt.Result == result && attempt.Ops > 0 {
			return true
		}
	}
	return false
}

type lookupProxy struct {
	dataByHash map[string][]byte
}

func (*lookupProxy) Put(context.Context, cache.EntryKind, string, int64, int64, io.ReadCloser) {}

func (p *lookupProxy) Get(_ context.Context, _ cache.EntryKind, hash string, _ int64) (io.ReadCloser, int64, error) {
	data, ok := p.dataByHash[hash]
	if !ok {
		return nil, -1, nil
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
}

func (p *lookupProxy) Contains(_ context.Context, _ cache.EntryKind, hash string, _ int64) (bool, int64) {
	data, ok := p.dataByHash[hash]
	if !ok {
		return false, -1
	}
	return true, int64(len(data))
}

func TestLookupObserverDistinguishesLocalAndBackendAttempts(t *testing.T) {
	localData := []byte("local")
	localSum := sha256.Sum256(localData)
	localHash := hex.EncodeToString(localSum[:])
	backendData := []byte("backend")
	backendSum := sha256.Sum256(backendData)
	backendHash := hex.EncodeToString(backendSum[:])
	missingHash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	observer := &recordingLookupObserver{}
	cacheI, err := New(t.TempDir(), 1024*1024,
		WithAccessLogger(testutils.NewSilentLogger()),
		WithStorageMode("uncompressed"),
		WithProxyBackend(&lookupProxy{dataByHash: map[string][]byte{backendHash: backendData}}),
		WithLookupAttemptObserver(observer),
	)
	if err != nil {
		t.Fatal(err)
	}
	diskCache := cacheI.(*diskCache)
	if err := diskCache.Put(context.Background(), cache.CAS, localHash, int64(len(localData)), bytes.NewReader(localData)); err != nil {
		t.Fatal(err)
	}

	rc, _, err := diskCache.Get(context.Background(), cache.CAS, localHash, int64(len(localData)), 0)
	if err != nil {
		t.Fatal(err)
	}
	if rc != nil {
		_ = rc.Close()
	}
	rc, _, err = diskCache.Get(context.Background(), cache.CAS, backendHash, int64(len(backendData)), 0)
	if err != nil {
		t.Fatal(err)
	}
	if rc != nil {
		_ = rc.Close()
	}
	_, _, _ = diskCache.Get(context.Background(), cache.CAS, missingHash, 7, 0)

	if !observer.has(cache.CAS, cache.LookupAccessGet, cache.LookupSourceLocal, cache.LookupResultHit) {
		t.Fatal("missing local get hit")
	}
	if !observer.has(cache.CAS, cache.LookupAccessGet, cache.LookupSourceLocal, cache.LookupResultMiss) {
		t.Fatal("missing local get miss")
	}
	if !observer.has(cache.CAS, cache.LookupAccessGet, cache.LookupSourceBackend, cache.LookupResultHit) {
		t.Fatal("missing backend get hit")
	}
	if !observer.has(cache.CAS, cache.LookupAccessGet, cache.LookupSourceBackend, cache.LookupResultMiss) {
		t.Fatal("missing backend get miss")
	}
}

func TestLookupObserverRecordsClosureHitAndDependencyMissing(t *testing.T) {
	observer := &recordingLookupObserver{}
	cacheI, err := New(t.TempDir(), 1024*1024,
		WithAccessLogger(testutils.NewSilentLogger()),
		WithLookupAttemptObserver(observer),
	)
	if err != nil {
		t.Fatal(err)
	}
	diskCache := cacheI.(*diskCache)

	present := putCAS(t, diskCache, []byte("present"))
	hitHash := putAC(t, diskCache, &pb.ActionResult{
		OutputFiles: []*pb.OutputFile{{Path: "present.txt", Digest: present}},
	})
	if result, _, err := diskCache.GetValidatedActionResult(context.Background(), hitHash); err != nil || result == nil {
		t.Fatalf("validated hit = (%v, %v), want non-nil result", result, err)
	}

	_, missing := testutils.RandomDataAndDigest(17)
	missingHash := putAC(t, diskCache, &pb.ActionResult{
		OutputFiles: []*pb.OutputFile{{Path: "missing.txt", Digest: &missing}},
	})
	if result, _, err := diskCache.GetValidatedActionResult(context.Background(), missingHash); err != nil || result != nil {
		t.Fatalf("incomplete closure = (%v, %v), want cache miss", result, err)
	}

	if !observer.has(cache.AC, cache.LookupAccessValidatedAction, cache.LookupSourceValidation, cache.LookupResultHit) {
		t.Fatal("missing validated-action hit")
	}
	if !observer.has(cache.AC, cache.LookupAccessValidatedAction, cache.LookupSourceValidation, cache.LookupResultDependencyMissing) {
		t.Fatal("missing dependency_missing validation outcome")
	}
}

func TestLookupObserverRecordsMissingTreeAsDependencyMissing(t *testing.T) {
	observer := &recordingLookupObserver{}
	cacheI, err := New(t.TempDir(), 1024*1024,
		WithAccessLogger(testutils.NewSilentLogger()),
		WithLookupAttemptObserver(observer),
	)
	if err != nil {
		t.Fatal(err)
	}
	diskCache := cacheI.(*diskCache)

	_, treeDigest := testutils.RandomDataAndDigest(23)
	actionHash := putAC(t, diskCache, &pb.ActionResult{
		OutputDirectories: []*pb.OutputDirectory{{Path: "out", TreeDigest: &treeDigest}},
	})
	result, _, err := diskCache.GetValidatedActionResult(context.Background(), actionHash)
	if err != nil || result != nil {
		t.Fatalf("missing Tree closure = (%v, %v), want cache miss", result, err)
	}
	if !observer.has(cache.AC, cache.LookupAccessValidatedAction, cache.LookupSourceValidation, cache.LookupResultDependencyMissing) {
		t.Fatal("missing Tree blob did not emit dependency_missing")
	}
}
