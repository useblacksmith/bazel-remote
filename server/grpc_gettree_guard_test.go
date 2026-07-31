package server

// Tests for the GetTree guards (WithGetTreeLimits): the fail-fast concurrency
// slot and the running response byte cap. GetTree materializes the whole
// directory tree into one in-memory response, so these guards are its only
// memory bound.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"sync"
	"testing"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/cache/disk"
	testutils "github.com/buchgr/bazel-remote/v2/utils"
)

// recordingGetTreeMetrics counts guard trips by reason.
type recordingGetTreeMetrics struct {
	mu      sync.Mutex
	reasons []string
}

func (m *recordingGetTreeMetrics) GetTreeDenied(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reasons = append(m.reasons, reason)
}

func (m *recordingGetTreeMetrics) denied() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.reasons...)
}

// fakeGetTreeStream satisfies pb.ContentAddressableStorage_GetTreeServer for
// direct handler invocation. Only Context and Send are exercised.
type fakeGetTreeStream struct {
	grpc.ServerStream
	ctx       context.Context
	responses []*pb.GetTreeResponse
}

func (s *fakeGetTreeStream) Context() context.Context { return s.ctx }

func (s *fakeGetTreeStream) Send(r *pb.GetTreeResponse) error {
	s.responses = append(s.responses, r)
	return nil
}

// getTreeGuardHarness holds a CAS with a two-level directory tree and a
// directly-constructed grpcServer, so guard behavior can be tested without
// goroutines or wire plumbing.
type getTreeGuardHarness struct {
	server     *grpcServer
	metrics    *recordingGetTreeMetrics
	rootDigest *pb.Digest
	// Serialized bytes of the root directory blob and of all directory
	// blobs together, for sizing byte caps precisely.
	rootBytes  int64
	totalBytes int64
	numDirs    int
}

func newGetTreeGuardHarness(t *testing.T) *getTreeGuardHarness {
	t.Helper()

	ctx := context.Background()
	cacheDir, err := os.MkdirTemp("", "bazel-remote-gettree-guard-"+t.Name())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(cacheDir) })

	diskCache, err := disk.New(cacheDir, 1024*1024,
		disk.WithAccessLogger(testutils.NewSilentLogger()))
	if err != nil {
		t.Fatal(err)
	}

	putDir := func(dir *pb.Directory) *pb.Digest {
		data, err := proto.Marshal(dir)
		if err != nil {
			t.Fatal(err)
		}
		hash := sha256.Sum256(data)
		hashStr := hex.EncodeToString(hash[:])
		err = diskCache.Put(ctx, cache.CAS, hashStr, int64(len(data)),
			bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		return &pb.Digest{Hash: hashStr, SizeBytes: int64(len(data))}
	}

	fileBlob := []byte("gettree guard test file contents")
	fileHash := sha256.Sum256(fileBlob)
	fileHashStr := hex.EncodeToString(fileHash[:])
	err = diskCache.Put(ctx, cache.CAS, fileHashStr, int64(len(fileBlob)),
		bytes.NewReader(fileBlob))
	if err != nil {
		t.Fatal(err)
	}
	fileNode := &pb.FileNode{
		Name:   "file.txt",
		Digest: &pb.Digest{Hash: fileHashStr, SizeBytes: int64(len(fileBlob))},
	}

	subDigest := putDir(&pb.Directory{Files: []*pb.FileNode{fileNode}})
	rootDigest := putDir(&pb.Directory{
		Files: []*pb.FileNode{fileNode},
		Directories: []*pb.DirectoryNode{
			{Name: "subdir", Digest: subDigest},
		},
	})

	metrics := &recordingGetTreeMetrics{}
	return &getTreeGuardHarness{
		server: &grpcServer{
			cache:          diskCache,
			accessLogger:   testutils.NewSilentLogger(),
			errorLogger:    testutils.NewSilentLogger(),
			getTreeMetrics: metrics,
		},
		metrics:    metrics,
		rootDigest: rootDigest,
		rootBytes:  rootDigest.SizeBytes,
		totalBytes: rootDigest.SizeBytes + subDigest.SizeBytes,
		numDirs:    2,
	}
}

func (h *getTreeGuardHarness) getTree(t *testing.T) ([]*pb.GetTreeResponse, error) {
	t.Helper()
	stream := &fakeGetTreeStream{ctx: context.Background()}
	err := h.server.GetTree(&pb.GetTreeRequest{RootDigest: h.rootDigest}, stream)
	return stream.responses, err
}

func requireResourceExhausted(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected ResourceExhausted, got success")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v", err)
	}
}

func TestGetTreeConcurrencyGuardFailsFast(t *testing.T) {
	h := newGetTreeGuardHarness(t)
	h.server.getTreeSem = semaphore.NewWeighted(1)

	// With the only slot held, GetTree is denied immediately.
	if !h.server.getTreeSem.TryAcquire(1) {
		t.Fatal("failed to occupy the GetTree slot")
	}
	_, err := h.getTree(t)
	requireResourceExhausted(t, err)
	if got := h.metrics.denied(); len(got) != 1 || got[0] != GetTreeDeniedSaturated {
		t.Fatalf("expected one %q denial, got %v", GetTreeDeniedSaturated, got)
	}

	// Releasing the slot makes GetTree succeed, and the handler releases
	// the slot on completion so a subsequent call also succeeds.
	h.server.getTreeSem.Release(1)
	for i := 0; i < 2; i++ {
		responses, err := h.getTree(t)
		if err != nil {
			t.Fatalf("GetTree attempt %d failed after slot release: %v", i, err)
		}
		if len(responses) != 1 || len(responses[0].Directories) != h.numDirs {
			t.Fatalf("expected one response with %d directories, got %v", h.numDirs, responses)
		}
	}
	if got := h.metrics.denied(); len(got) != 1 {
		t.Fatalf("expected no further denials, got %v", got)
	}
}

func TestGetTreeResponseByteCap(t *testing.T) {
	h := newGetTreeGuardHarness(t)

	// A budget that covers the root but not the child directory aborts
	// mid-traversal.
	h.server.getTreeMaxResponseBytes = h.totalBytes - 1
	_, err := h.getTree(t)
	requireResourceExhausted(t, err)
	if got := h.metrics.denied(); len(got) != 1 || got[0] != GetTreeDeniedResponseBytes {
		t.Fatalf("expected one %q denial, got %v", GetTreeDeniedResponseBytes, got)
	}

	// A budget smaller than the root blob is denied before unmarshaling
	// anything.
	h.server.getTreeMaxResponseBytes = h.rootBytes - 1
	_, err = h.getTree(t)
	requireResourceExhausted(t, err)

	// An exact budget serves the full tree.
	h.server.getTreeMaxResponseBytes = h.totalBytes
	responses, err := h.getTree(t)
	if err != nil {
		t.Fatalf("GetTree failed with an exact byte budget: %v", err)
	}
	if len(responses) != 1 || len(responses[0].Directories) != h.numDirs {
		t.Fatalf("expected one response with %d directories, got %v", h.numDirs, responses)
	}
}

// TestGetTreeGuardsEndToEnd proves the option plumbing: a server configured
// through WithGetTreeLimits denies an over-budget tree over the wire.
func TestGetTreeGuardsEndToEnd(t *testing.T) {
	metrics := &recordingGetTreeMetrics{}
	fixture := grpcTestSetupInternal(t, false, WithGetTreeLimits(GetTreeLimits{
		MaxConcurrent:    1,
		MaxResponseBytes: 1,
		Metrics:          metrics,
	}))
	defer func() { _ = os.RemoveAll(fixture.tempdir) }()

	dir := pb.Directory{}
	dirData, err := proto.Marshal(&dir)
	if err != nil {
		t.Fatal(err)
	}
	// An empty Directory marshals to zero bytes, which would pass any
	// budget; add a file to make it non-empty.
	fileBlob := []byte("x")
	fileHash := sha256.Sum256(fileBlob)
	dir.Files = []*pb.FileNode{{
		Name:   "f",
		Digest: &pb.Digest{Hash: hex.EncodeToString(fileHash[:]), SizeBytes: 1},
	}}
	dirData, err = proto.Marshal(&dir)
	if err != nil {
		t.Fatal(err)
	}
	dirHash := sha256.Sum256(dirData)
	dirDigest := pb.Digest{
		Hash:      hex.EncodeToString(dirHash[:]),
		SizeBytes: int64(len(dirData)),
	}

	upReq := pb.BatchUpdateBlobsRequest{
		Requests: []*pb.BatchUpdateBlobsRequest_Request{
			{Digest: &dirDigest, Data: dirData},
		},
	}
	_, err = fixture.casClient.BatchUpdateBlobs(ctx, &upReq)
	if err != nil {
		t.Fatal(err)
	}

	stream, err := fixture.casClient.GetTree(ctx,
		&pb.GetTreeRequest{RootDigest: &dirDigest})
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	requireResourceExhausted(t, err)
	if got := metrics.denied(); len(got) != 1 || got[0] != GetTreeDeniedResponseBytes {
		t.Fatalf("expected one %q denial, got %v", GetTreeDeniedResponseBytes, got)
	}
}
