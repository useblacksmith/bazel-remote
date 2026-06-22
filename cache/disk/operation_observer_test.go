package disk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	stdlog "log"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	pb "github.com/buchgr/bazel-remote/v2/genproto/build/bazel/remote/execution/v2"
	"google.golang.org/protobuf/proto"
)

type recordingObserver struct {
	outcomes []cache.OperationOutcome
}

func (r *recordingObserver) RecordOutcome(_ context.Context, outcome cache.OperationOutcome) {
	r.outcomes = append(r.outcomes, outcome)
}

func TestOperationObserverReceivesActionCacheHitAndMiss(t *testing.T) {
	observer := &recordingObserver{}
	diskCache, err := New(t.TempDir(), 1024*1024, WithOperationObserver(observer), WithAccessLogger(stdlog.New(io.Discard, "", 0)))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	result := &pb.ActionResult{StdoutRaw: []byte("ok")}
	data, err := proto.Marshal(result)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])
	if err := diskCache.Put(context.Background(), cache.AC, hash, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if _, _, err := diskCache.GetValidatedActionResult(context.Background(), hash); err != nil {
		t.Fatalf("GetValidatedActionResult(hit) error = %v", err)
	}
	if _, _, err := diskCache.GetValidatedActionResult(context.Background(), "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"); err != nil {
		t.Fatalf("GetValidatedActionResult(miss) error = %v", err)
	}

	requireOutcome(t, observer.outcomes, actionCacheGet, hitStatus)
	requireOutcome(t, observer.outcomes, actionCacheGet, missStatus)
}

func TestOperationObserverReceivesFindMissingCasBlobsOutcomes(t *testing.T) {
	observer := &recordingObserver{}
	diskCache, err := New(t.TempDir(), 1024*1024, WithOperationObserver(observer), WithAccessLogger(stdlog.New(io.Discard, "", 0)))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	data := []byte("blob")
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])
	if err := diskCache.Put(context.Background(), cache.CAS, hash, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	_, err = diskCache.FindMissingCasBlobs(context.Background(), []*pb.Digest{
		{Hash: hash, SizeBytes: int64(len(data))},
		{Hash: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789", SizeBytes: 12},
	})
	if err != nil {
		t.Fatalf("FindMissingCasBlobs() error = %v", err)
	}

	requireOutcome(t, observer.outcomes, casLookup, hitStatus)
	requireOutcome(t, observer.outcomes, casLookup, missStatus)
}

func requireOutcome(t *testing.T, outcomes []cache.OperationOutcome, method string, status string) {
	t.Helper()
	for _, outcome := range outcomes {
		if outcome.Method == method && outcome.Status == status {
			return
		}
	}
	t.Fatalf("missing outcome method=%s status=%s in %+v", method, status, outcomes)
}
