package disk

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func testCensusSnapshot() (CensusHeader, []CensusRow) {
	header := CensusHeader{
		SchemaVersion: CensusSchemaVersion,
		Host:          "bazel-l1-test-1",
		WindowStartMs: 1_000_000,
		WindowEndMs:   2_000_000,
		TenantCount:   2,
	}
	rows := []CensusRow{
		{
			PrefixID:             "abc",
			RawPrefix:            "bazelre/prod/1/2/go/",
			ResidentBytes:        123,
			ResidentEntries:      4,
			PutBytes:             10,
			EvictedBytes:         5,
			EvictedByLastReadAge: []int64{1, 0, 0, 4, 0, 0, 0, 0, 0},
		},
		{PrefixID: "def", ResidentBytes: 9},
	}
	return header, rows
}

// The wire contract with the l1_tenant_census table: JSONEachRow lines with
// the header denormalized onto every row, the deterministic dedup token,
// synchronous insert, and unknown-field tolerance for additive skew.
func TestClickHouseCensusSinkInsertShape(t *testing.T) {
	type seen struct {
		query      string
		dedupToken string
		asyncOff   bool
		skipUnk    bool
		user, key  string
		lines      []map[string]any
	}
	got := make(chan seen, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		var lines []map[string]any
		sc := bufio.NewScanner(r.Body)
		for sc.Scan() {
			if len(sc.Bytes()) == 0 {
				continue
			}
			var m map[string]any
			if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
				t.Errorf("bad JSONEachRow line %q: %v", sc.Text(), err)
			}
			lines = append(lines, m)
		}
		got <- seen{
			query:      q.Get("query"),
			dedupToken: q.Get("insert_deduplication_token"),
			asyncOff:   q.Get("async_insert") == "0" && q.Get("wait_for_async_insert") == "1",
			skipUnk:    q.Get("input_format_skip_unknown_fields") == "1",
			user:       r.Header.Get("X-ClickHouse-User"),
			key:        r.Header.Get("X-ClickHouse-Key"),
			lines:      lines,
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink, err := NewClickHouseCensusSink(srv.URL, "l1-census", "secret", "")
	if err != nil {
		t.Fatal(err)
	}
	header, rows := testCensusSnapshot()
	if err := sink.PutCensus(context.Background(), header, rows); err != nil {
		t.Fatal(err)
	}

	s := <-got
	if s.query != "INSERT INTO "+DefaultCensusTable+" FORMAT JSONEachRow" {
		t.Errorf("query = %q", s.query)
	}
	if s.dedupToken != "bazel-l1-test-1/2000000" {
		t.Errorf("dedup token = %q", s.dedupToken)
	}
	if !s.asyncOff {
		t.Error("async_insert must be explicitly disabled (dedup tokens are not honored async)")
	}
	if !s.skipUnk {
		t.Error("input_format_skip_unknown_fields must be set for additive version skew")
	}
	if s.user != "l1-census" || s.key != "secret" {
		t.Errorf("auth headers = %q/%q", s.user, s.key)
	}
	if len(s.lines) != 2 {
		t.Fatalf("lines = %d, want 2", len(s.lines))
	}
	first := s.lines[0]
	if first["host"] != "bazel-l1-test-1" || first["schema_version"] != float64(CensusSchemaVersion) {
		t.Errorf("header fields not denormalized: %+v", first)
	}
	if first["window_start"] != float64(1000) || first["window_end"] != float64(2000) {
		t.Errorf("window bounds not unix seconds: %+v", first)
	}
	if first["prefix"] != "bazelre/prod/1/2/go/" || first["resident_bytes"] != float64(123) {
		t.Errorf("row fields missing: %+v", first)
	}
	if _, ok := s.lines[1]["prefix"]; ok {
		t.Error("unresolved prefix must be omitted, not empty")
	}
}

// Three attempts, identical bodies and dedup token each time, then drop.
func TestClickHouseCensusSinkRetriesThenDrops(t *testing.T) {
	var calls atomic.Int64
	tokens := make(chan string, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		tokens <- r.URL.Query().Get("insert_deduplication_token")
		_, _ = io.Copy(io.Discard, r.Body)
		http.Error(w, "Code: 241. DB::Exception: memory limit", http.StatusInternalServerError)
	}))
	defer srv.Close()

	sink, err := NewClickHouseCensusSink(srv.URL, "u", "p", "")
	if err != nil {
		t.Fatal(err)
	}
	// Shorten the pauses via context: the retry pause is 2s, so allow them.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	header, rows := testCensusSnapshot()
	if err := sink.PutCensus(ctx, header, rows); err == nil {
		t.Fatal("expected an error after exhausted attempts")
	}
	if n := calls.Load(); n != censusInsertAttempts {
		t.Fatalf("attempts = %d, want %d", n, censusInsertAttempts)
	}
	first := <-tokens
	for i := 1; i < censusInsertAttempts; i++ {
		if tok := <-tokens; tok != first {
			t.Errorf("dedup token changed across retries: %q vs %q", first, tok)
		}
	}
}

// A transient failure recovers on a later attempt within the same window.
func TestClickHouseCensusSinkRecoversMidBudget(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if calls.Add(1) < 2 {
			http.Error(w, "transient", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink, err := NewClickHouseCensusSink(srv.URL, "u", "p", "custom.census")
	if err != nil {
		t.Fatal(err)
	}
	header, rows := testCensusSnapshot()
	if err := sink.PutCensus(context.Background(), header, rows); err != nil {
		t.Fatalf("expected recovery on attempt 2: %v", err)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("attempts = %d, want 2", n)
	}
}

// Zero tenant rows is a successful no-op: no request, no error.
func TestClickHouseCensusSinkEmptySnapshot(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("no request expected for an empty snapshot")
	}))
	defer srv.Close()

	sink, err := NewClickHouseCensusSink(srv.URL, "u", "p", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.PutCensus(context.Background(), CensusHeader{}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestNewClickHouseCensusSinkRejectsBadURL(t *testing.T) {
	for _, bad := range []string{"", "not-a-url", "host:8443"} {
		if _, err := NewClickHouseCensusSink(bad, "u", "p", ""); err == nil {
			t.Errorf("URL %q must be rejected", bad)
		}
	}
}
