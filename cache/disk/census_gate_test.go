package disk

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	testutils "github.com/buchgr/bazel-remote/v2/utils"

	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type discardCensusSink struct{}

func (discardCensusSink) PutCensus(context.Context, CensusHeader, []CensusRow) error { return nil }

// Without an LRU observer or census export, an AC write must stay on the
// streaming path: nothing is buffered or decoded, so the census records no
// action wall time. With export configured the same write is accounted.
func TestACWriteCaptureOnlyWhenCensusExported(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	ar := &pb.ActionResult{ExecutionMetadata: &pb.ExecutedActionMetadata{
		ExecutionStartTimestamp:     timestamppb.New(start),
		ExecutionCompletedTimestamp: timestamppb.New(start.Add(2 * time.Second)),
	}}
	arData, err := proto.Marshal(ar)
	if err != nil {
		t.Fatal(err)
	}
	const fakeActionHash = "8f279f9d8bc605b4d733d0ba9386de2376004ab628fee6b000144fdc7b30a6a1"

	writeCount := func(t *testing.T, opts ...Option) int64 {
		t.Helper()
		cacheDir := testutils.TempDir(t)
		opts = append(opts, WithAccessLogger(testutils.NewSilentLogger()))
		testCacheI, err := New(cacheDir, 1024*1024, opts...)
		if err != nil {
			t.Fatal(err)
		}
		testCache := testCacheI.(*diskCache)
		if err := testCache.Put(context.Background(), cache.AC, fakeActionHash, int64(len(arData)), bytes.NewReader(arData)); err != nil {
			t.Fatal(err)
		}
		_, rows := testCache.census.snapshot("h")
		var n int64
		for _, row := range rows {
			for _, c := range row.ACWriteCountByBucket {
				n += c
			}
			n += row.ACWriteNoMetadata
		}
		return n
	}

	if n := writeCount(t); n != 0 {
		t.Errorf("without census export: ac writes accounted = %d, want 0", n)
	}
	if n := writeCount(t, WithCensusSnapshots(discardCensusSink{}, time.Hour, "h")); n != 1 {
		t.Errorf("with census export: ac writes accounted = %d, want 1", n)
	}
}
