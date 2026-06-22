package cache

import (
	"context"
	"testing"
)

func TestMetricsLabelsRoundTrip(t *testing.T) {
	labels := MetricsLabels{
		InstallationID: "10",
		RepositoryID:   "717982840",
		Generation:     "v0",
		BuildToolID:    "bazel",
		VMID:           "vm-123",
		JobID:          "job-456",
	}

	got, ok := MetricsLabelsFromContext(WithMetricsLabels(context.Background(), labels))
	if !ok {
		t.Fatal("MetricsLabelsFromContext ok = false, want true")
	}
	if got != labels {
		t.Fatalf("MetricsLabelsFromContext = %+v, want %+v", got, labels)
	}
}

func TestObserveOperationWithoutObserverDoesNothing(t *testing.T) {
	ObserveOperation(context.Background(), nil, OperationOutcome{Method: "get", Status: "hit"})
}
