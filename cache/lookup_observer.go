package cache

import "context"

// LookupAccess identifies the semantic lookup being attempted.
type LookupAccess string

const (
	LookupAccessGet             LookupAccess = "get"
	LookupAccessContains        LookupAccess = "contains"
	LookupAccessValidatedAction LookupAccess = "validated_action"
)

// LookupSource identifies the cache layer or final validation boundary.
type LookupSource string

const (
	LookupSourceLocal      LookupSource = "local"
	LookupSourceBackend    LookupSource = "backend"
	LookupSourceValidation LookupSource = "validation"
)

// LookupResult is the bounded result vocabulary for lookup attempts.
type LookupResult string

const (
	LookupResultHit               LookupResult = "hit"
	LookupResultMiss              LookupResult = "miss"
	LookupResultError             LookupResult = "error"
	LookupResultDependencyMissing LookupResult = "dependency_missing"
)

// LookupAttempt describes one or more equivalent low-cardinality layer
// attempts. Ops permits batched closure checks to avoid one observer call per
// digest.
type LookupAttempt struct {
	Kind   EntryKind
	Access LookupAccess
	Source LookupSource
	Result LookupResult
	Ops    uint64
}

// LookupAttemptObserver receives best-effort lookup and closure-validation
// outcomes. Implementations must not affect cache request behavior.
type LookupAttemptObserver interface {
	RecordLookupAttempt(context.Context, LookupAttempt)
}

// ObserveLookupAttempt isolates cache behavior from observer failures.
func ObserveLookupAttempt(ctx context.Context, observer LookupAttemptObserver, attempt LookupAttempt) {
	if observer == nil {
		return
	}
	if attempt.Ops == 0 {
		attempt.Ops = 1
	}
	defer func() {
		_ = recover()
	}()
	observer.RecordLookupAttempt(ctx, attempt)
}
