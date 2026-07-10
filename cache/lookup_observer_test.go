package cache

import (
	"context"
	"testing"
)

type lookupObserverFunc func(context.Context, LookupAttempt)

func (f lookupObserverFunc) RecordLookupAttempt(ctx context.Context, attempt LookupAttempt) {
	f(ctx, attempt)
}

func TestObserveLookupAttemptDefaultsOpsAndRecovers(t *testing.T) {
	var got LookupAttempt
	ObserveLookupAttempt(context.Background(), lookupObserverFunc(func(_ context.Context, attempt LookupAttempt) {
		got = attempt
	}), LookupAttempt{Kind: CAS, Access: LookupAccessGet, Source: LookupSourceLocal, Result: LookupResultHit})
	if got.Ops != 1 {
		t.Fatalf("default Ops = %d, want 1", got.Ops)
	}

	deferredReached := false
	func() {
		defer func() { deferredReached = true }()
		ObserveLookupAttempt(context.Background(), lookupObserverFunc(func(context.Context, LookupAttempt) {
			panic("observer failure")
		}), LookupAttempt{})
	}()
	if !deferredReached {
		t.Fatal("observer panic escaped into the cache path")
	}
}
