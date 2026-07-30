package server

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

// Trust rejections must be visible in the server log (journald), not only in
// the Prometheus counters — the 2026-07-30 staging validation found selector
// rejections incrementing counters with a completely silent journal. One
// line per (reason, cause) per interval: the counter carries the volume, the
// log line carries the existence of the incident.
func TestTrustRejectionLogsOncePerCause(t *testing.T) {
	var buf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(prev)

	for i := 0; i < 5; i++ {
		_ = trustRejection("TEST_REASON", "test_cause_a", "rejected value %q", "evil")
	}
	_ = trustRejection("TEST_REASON", "test_cause_b", "rejected value %q", "worse")

	out := buf.String()
	if got := strings.Count(out, "cause=test_cause_a"); got != 1 {
		t.Fatalf("cause_a logged %d times in the rate-limit window, want exactly 1:\n%s", got, out)
	}
	if got := strings.Count(out, "cause=test_cause_b"); got != 1 {
		t.Fatalf("cause_b logged %d times, want 1 (causes must be limited independently):\n%s", got, out)
	}
	if !strings.Contains(out, `rejected value "evil"`) {
		t.Fatalf("log line does not carry the rejection message:\n%s", out)
	}
}
