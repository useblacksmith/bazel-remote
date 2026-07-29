package cache

import "context"

type metricsLabelsContextKey struct{}

// MetricsLabels contains opaque caller-supplied dimensions for cache
// operation observers. bazel-remote treats these values as labels only and
// does not interpret tenant or job identity.
type MetricsLabels struct {
	InstallationID string
	RepositoryID   string
	Generation     string
	BuildToolID    string
	VMID           string
	JobID          string
}

// WithMetricsLabels attaches caller-owned metrics labels to a request context.
func WithMetricsLabels(ctx context.Context, labels MetricsLabels) context.Context {
	return context.WithValue(ctx, metricsLabelsContextKey{}, labels)
}

// MetricsLabelsFromContext returns caller-owned metrics labels, if present.
func MetricsLabelsFromContext(ctx context.Context) (MetricsLabels, bool) {
	labels, ok := ctx.Value(metricsLabelsContextKey{}).(MetricsLabels)
	return labels, ok
}

// OperationObserver receives best-effort cache operation outcomes. Observer
// implementations must not affect cache request behavior.
type OperationObserver interface {
	RecordOutcome(ctx context.Context, outcome OperationOutcome)
}

// OperationOutcome describes an observed cache operation outcome.
type OperationOutcome struct {
	Labels MetricsLabels
	Method string
	// Kind is the cache entry kind ("ac", "cas", or "raw", per
	// EntryKind.String()) when the outcome concerns a specific entry kind;
	// empty otherwise. A string rather than EntryKind so the zero value
	// reads as "unset" instead of AC. Emitters that know the kind should
	// set it: observers must be able to account AC and CAS writes
	// differently (AC updates are unconditional same-digest rewrites, CAS
	// writes are content-addressed and deduplicated).
	Kind   string
	Status string
	Reason string
	Ops    uint64
	Bytes  uint64
}

// ObserveOperation records an outcome when an observer is configured. Panics
// from observer implementations are swallowed so metrics can never change
// cache request behavior.
func ObserveOperation(ctx context.Context, observer OperationObserver, outcome OperationOutcome) {
	if observer == nil {
		return
	}
	if labels, ok := MetricsLabelsFromContext(ctx); ok {
		outcome.Labels = labels
	}
	defer func() {
		_ = recover()
	}()
	observer.RecordOutcome(ctx, outcome)
}
