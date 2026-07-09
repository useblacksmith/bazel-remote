package disk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	testutils "github.com/buchgr/bazel-remote/v2/utils"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"

	"google.golang.org/protobuf/proto"
)

// noopLRUObserver satisfies cache.LRUObserver with zero work, so a benchmark
// measures the bazel-remote-side capture cost (closure assembly + size harvest)
// rather than any downstream buffering.
type noopLRUObserver struct{}

func (noopLRUObserver) RecordACAccess(context.Context, cache.ACClosure) {}

// benchNewCache builds a diskCache for benchmarking, with capture enabled
// (no-op observer) or disabled (nil observer). The cache is large enough that
// nothing evicts during the run.
func benchNewCache(tb testing.TB, captureOn bool) *diskCache {
	tb.Helper()
	// diskCache.New logs index-loading progress via the standard logger to
	// stdout, which corrupts the machine-readable benchmark output; discard it.
	log.SetOutput(io.Discard)
	opts := []Option{WithAccessLogger(testutils.NewSilentLogger())}
	if captureOn {
		opts = append(opts, WithLRUObserver(noopLRUObserver{}))
	}
	ci, err := New(tb.TempDir(), 1<<30, opts...)
	if err != nil {
		tb.Fatal(err)
	}
	return ci.(*diskCache)
}

// benchSetupHitAC stores an AC whose closure has numFiles output files (plus
// stdout, and optionally an output_directories Tree expanding to numFiles tree
// files) and returns its hash. Every leaf is stored locally so a hit is a pure
// local-index closure walk.
func benchSetupHitAC(tb testing.TB, c *diskCache, numFiles int, withTree bool) string {
	tb.Helper()
	ar := &pb.ActionResult{}
	for i := 0; i < numFiles; i++ {
		d := putCAS(tb, c, []byte(fmt.Sprintf("out-file-%d-data", i)))
		ar.OutputFiles = append(ar.OutputFiles, &pb.OutputFile{Path: fmt.Sprintf("f%d", i), Digest: d})
	}
	ar.StdoutDigest = putCAS(tb, c, []byte("stdout data"))

	if withTree {
		treeFiles := make([]*pb.FileNode, 0, numFiles)
		for i := 0; i < numFiles; i++ {
			d := putCAS(tb, c, []byte(fmt.Sprintf("tree-file-%d-data", i)))
			treeFiles = append(treeFiles, &pb.FileNode{Name: fmt.Sprintf("t%d", i), Digest: d})
		}
		tree := pb.Tree{Root: &pb.Directory{Files: treeFiles}}
		treeData, err := proto.Marshal(&tree)
		if err != nil {
			tb.Fatal(err)
		}
		treeDigest := putCAS(tb, c, treeData)
		ar.OutputDirectories = []*pb.OutputDirectory{{Path: "out", TreeDigest: treeDigest}}
	}

	return putAC(tb, c, ar)
}

// benchSetupWriteAC builds (but does not store) the AC blob a write benchmark
// repeatedly Puts. For the no-dirs case every referenced output file is stored
// locally so the write emits a complete closure; the with-dirs case drops
// early (D10) so the Tree digest need not exist.
func benchSetupWriteAC(tb testing.TB, c *diskCache, withDirs bool) (hash string, acBytes []byte) {
	tb.Helper()
	ar := &pb.ActionResult{}
	for i := 0; i < 3; i++ {
		d := putCAS(tb, c, []byte(fmt.Sprintf("write-file-%d-data", i)))
		ar.OutputFiles = append(ar.OutputFiles, &pb.OutputFile{Path: fmt.Sprintf("f%d", i), Digest: d})
	}
	if withDirs {
		_, treeDigest := testutils.RandomDataAndDigest(128)
		ar.OutputDirectories = []*pb.OutputDirectory{{Path: "out", TreeDigest: &treeDigest}}
	}
	acBytes, err := proto.Marshal(ar)
	if err != nil {
		tb.Fatal(err)
	}
	sum := sha256.Sum256(acBytes)
	return hex.EncodeToString(sum[:]), acBytes
}

// BenchmarkLRUCaptureHit measures GetValidatedActionResult on the AC hit path
// with capture on vs off. The delta is the closure-assembly + size-harvest cost
// that capture adds to a validated hit.
func BenchmarkLRUCaptureHit(b *testing.B) {
	cases := []struct {
		name     string
		numFiles int
		withTree bool
	}{
		{"small", 3, false},
		{"large", 200, true},
	}
	for _, tc := range cases {
		for _, on := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/observer=%v", tc.name, on), func(b *testing.B) {
				ctx := context.Background()
				c := benchNewCache(b, on)
				acHash := benchSetupHitAC(b, c, tc.numFiles, tc.withTree)
				if _, _, err := c.GetValidatedActionResult(ctx, acHash); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, _, err := c.GetValidatedActionResult(ctx, acHash); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkLRUCaptureWrite measures Put(AC) with capture on vs off. The delta is
// the io.ReadAll + proto.Unmarshal + emitACClosureFromWrite cost that capture
// adds to an AC write (no-dirs emits a closure; with-dirs drops early per D10).
func BenchmarkLRUCaptureWrite(b *testing.B) {
	cases := []struct {
		name     string
		withDirs bool
	}{
		{"no_dirs", false},
		{"with_dirs", true},
	}
	for _, tc := range cases {
		for _, on := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/observer=%v", tc.name, on), func(b *testing.B) {
				ctx := context.Background()
				c := benchNewCache(b, on)
				hash, acBytes := benchSetupWriteAC(b, c, tc.withDirs)
				size := int64(len(acBytes))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := c.Put(ctx, cache.AC, hash, size, bytes.NewReader(acBytes)); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
