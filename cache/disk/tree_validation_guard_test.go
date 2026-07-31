package disk

// Tests for WithTreeValidationSizeLimit: GetValidatedActionResult reads every
// referenced output-directory Tree blob wholly into memory, so a cap on the
// total declared Tree bytes is its only per-request memory bound. Over-cap
// results must be reported as a miss (semantically safe: the client
// rebuilds), never as an error.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"

	"github.com/buchgr/bazel-remote/v2/cache"
	testutils "github.com/buchgr/bazel-remote/v2/utils"

	"google.golang.org/protobuf/proto"
)

// putTreeBackedActionResult stores a CAS directory tree and an ActionResult
// referencing it as an output directory, returning the AC hash and the
// declared Tree blob size.
func putTreeBackedActionResult(ctx context.Context, t *testing.T, c *diskCache) (string, int64) {
	t.Helper()

	fileData := []byte("tree validation guard file contents")
	fileHash := sha256.Sum256(fileData)
	fileHashStr := hex.EncodeToString(fileHash[:])
	if err := c.Put(ctx, cache.CAS, fileHashStr, int64(len(fileData)),
		bytes.NewReader(fileData)); err != nil {
		t.Fatal(err)
	}

	rootDir := pb.Directory{
		Files: []*pb.FileNode{
			{
				Name: "file.txt",
				Digest: &pb.Digest{
					Hash:      fileHashStr,
					SizeBytes: int64(len(fileData)),
				},
			},
		},
	}

	tree := pb.Tree{Root: &rootDir}
	treeData, err := proto.Marshal(&tree)
	if err != nil {
		t.Fatal(err)
	}
	treeHash := sha256.Sum256(treeData)
	treeHashStr := hex.EncodeToString(treeHash[:])
	if err := c.Put(ctx, cache.CAS, treeHashStr, int64(len(treeData)),
		bytes.NewReader(treeData)); err != nil {
		t.Fatal(err)
	}

	ar := pb.ActionResult{
		OutputFiles: []*pb.OutputFile{
			{
				Path: "file.txt",
				Digest: &pb.Digest{
					Hash:      fileHashStr,
					SizeBytes: int64(len(fileData)),
				},
			},
		},
		OutputDirectories: []*pb.OutputDirectory{
			{
				Path: "out",
				TreeDigest: &pb.Digest{
					Hash:      treeHashStr,
					SizeBytes: int64(len(treeData)),
				},
			},
		},
	}
	arData, err := proto.Marshal(&ar)
	if err != nil {
		t.Fatal(err)
	}
	arHash := sha256.Sum256([]byte("tree validation guard action"))
	arHashStr := hex.EncodeToString(arHash[:])
	if err := c.Put(ctx, cache.AC, arHashStr, int64(len(arData)),
		bytes.NewReader(arData)); err != nil {
		t.Fatal(err)
	}

	return arHashStr, int64(len(treeData))
}

func newTreeValidationCache(t *testing.T, opts ...Option) *diskCache {
	t.Helper()
	cacheDir := testutils.TempDir(t)
	t.Cleanup(func() { _ = os.RemoveAll(cacheDir) })

	opts = append([]Option{WithAccessLogger(testutils.NewSilentLogger())}, opts...)
	c, err := New(cacheDir, 1024*32, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return c.(*diskCache)
}

func TestTreeValidationSizeLimitAllowsUnderCap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var trips []int64
	// The limit is set after the blobs exist, via a second cache below;
	// here declare a generous cap up-front.
	c := newTreeValidationCache(t,
		WithTreeValidationSizeLimit(1024*1024, func(declared int64) {
			trips = append(trips, declared)
		}))
	arHash, _ := putTreeBackedActionResult(ctx, t, c)

	result, data, err := c.GetValidatedActionResult(ctx, arHash)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || data == nil {
		t.Fatal("expected a validated hit under the cap")
	}
	if len(trips) != 0 {
		t.Fatalf("expected no guard trips, got %v", trips)
	}
}

func TestTreeValidationSizeLimitReportsMissOverCap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Build the blobs first with no cap, then flip the cap below the
	// declared Tree size on the same cache to prove the guard alone flips
	// hit to miss.
	c := newTreeValidationCache(t)
	arHash, treeBytes := putTreeBackedActionResult(ctx, t, c)

	result, data, err := c.GetValidatedActionResult(ctx, arHash)
	if err != nil || result == nil || data == nil {
		t.Fatalf("expected a hit before the cap: result=%v data=%v err=%v", result, data, err)
	}

	var trips []int64
	c.maxTreeValidationBytes = treeBytes - 1
	c.treeValidationExceeded = func(declared int64) {
		trips = append(trips, declared)
	}

	result, data, err = c.GetValidatedActionResult(ctx, arHash)
	if err != nil {
		t.Fatalf("over-cap validation must be a miss, not an error: %v", err)
	}
	if result != nil || data != nil {
		t.Fatal("expected a miss over the cap")
	}
	if len(trips) != 1 || trips[0] != treeBytes {
		t.Fatalf("expected one trip with declared bytes %d, got %v", treeBytes, trips)
	}

	// An exact cap admits the result again.
	c.maxTreeValidationBytes = treeBytes
	result, data, err = c.GetValidatedActionResult(ctx, arHash)
	if err != nil || result == nil || data == nil {
		t.Fatalf("expected a hit at the exact cap: result=%v data=%v err=%v", result, data, err)
	}
	if len(trips) != 1 {
		t.Fatalf("expected no further trips, got %v", trips)
	}
}
