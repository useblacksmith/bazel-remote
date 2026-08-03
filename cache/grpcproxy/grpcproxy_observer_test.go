package grpcproxy

import (
	"context"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/utils/backendproxy"
)

type capturingObserver struct {
	outcomes []cache.OperationOutcome
}

func (o *capturingObserver) RecordOutcome(_ context.Context, outcome cache.OperationOutcome) {
	o.outcomes = append(o.outcomes, outcome)
}

// Backend-upload outcomes must carry the entry kind so accounting observers
// can treat forwarded CAS writes (deduplicated before upload) differently
// from forwarded AC writes (unconditional same-digest rewrites).
func TestObserveUploadCarriesEntryKind(t *testing.T) {
	for _, tc := range []struct {
		kind     cache.EntryKind
		wantKind string
	}{
		{cache.CAS, "cas"},
		{cache.AC, "ac"},
		{cache.RAW, "raw"},
	} {
		observer := &capturingObserver{}
		r := &remoteGrpcProxyCache{observer: observer}
		item := backendproxy.UploadReq{
			Hash:        "0000000000000000000000000000000000000000000000000000000000000000",
			LogicalSize: 42,
			SizeOnDisk:  42,
			Kind:        tc.kind,
		}
		r.observeUpload(item, "forwarded", "")

		if len(observer.outcomes) != 1 {
			t.Fatalf("kind %v: expected 1 outcome, got %d", tc.kind, len(observer.outcomes))
		}
		outcome := observer.outcomes[0]
		if outcome.Method != "backend_upload" {
			t.Errorf("kind %v: expected method backend_upload, got %q", tc.kind, outcome.Method)
		}
		if outcome.Kind != tc.wantKind {
			t.Errorf("kind %v: expected outcome kind %q, got %q", tc.kind, tc.wantKind, outcome.Kind)
		}
		if outcome.Status != "forwarded" {
			t.Errorf("kind %v: expected status forwarded, got %q", tc.kind, outcome.Status)
		}
		if outcome.Bytes != 42 {
			t.Errorf("kind %v: expected 42 bytes, got %d", tc.kind, outcome.Bytes)
		}
		// Cross-repo tripwire (Piotr, fork 3/5 review): successful forwards
		// deliberately carry no reason — both emit sites pass "". The fa
		// consumer (agent/cache/bazelre/build_cache_metrics.go) maps
		// forwarded CAS uploads to created and unconditionally overwrites
		// Reason with l1_forwarded, so any reason the fork attaches to a
		// successful forward would be silently swallowed there. If this
		// assertion ever fires, reconcile with that mapping first.
		if outcome.Reason != "" {
			t.Errorf("kind %v: successful forwards must carry an empty reason, got %q", tc.kind, outcome.Reason)
		}
	}
}
