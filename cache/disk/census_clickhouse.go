package disk

// The census export path to ClickHouse, deliberately driverless: one HTTPS
// POST of JSONEachRow per window through net/http. The L1s stay thin — no
// database client in the fork's dependency tree, no schema knowledge beyond
// the CensusRow JSON tags, which are the wire contract with the
// observability.l1_tenant_census table (owned by web migrations).
//
// Reliability posture, matching the rest of the census: advisory,
// best-effort, bounded. Three attempts with a short pause, then the window
// is dropped — the sample is tiny and affordable to lose. Retries are safe
// because every attempt carries the same insert_deduplication_token
// (host/window-end), so a retry after an ambiguous failure lands the rows
// exactly once. async_insert is explicitly disabled on the query: async
// inserts do not honor dedup tokens in production, which would defeat the
// retry safety.
//
// Version skew: input_format_skip_unknown_fields=1 means a fork that ships a
// new row field before the table migration lands degrades to dropping that
// field — never to failed inserts. Additive schema changes therefore need no
// deploy-order dance; only renames/removals do (and those bump
// CensusSchemaVersion).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultCensusTable is the fully-qualified insert target unless the
// deployment overrides it (BAZEL_REMOTE_CLICKHOUSE_CENSUS_TABLE).
const DefaultCensusTable = "observability.l1_tenant_census"

const (
	censusInsertAttempts       = 3
	censusInsertRetryPause     = 2 * time.Second
	censusInsertAttemptTimeout = 30 * time.Second
)

// ClickHouseCensusSink inserts census snapshots over the ClickHouse HTTP
// interface. Credentials ride headers (never the URL, which gets logged).
type ClickHouseCensusSink struct {
	baseURL  string
	user     string
	password string
	table    string
	client   *http.Client
}

// NewClickHouseCensusSink builds a sink for the HTTP(S) endpoint at baseURL
// (e.g. "https://x.clickhouse.cloud:8443"). An empty table selects
// DefaultCensusTable. The table name is embedded in a SQL statement; it must
// come from deployment config, never from request data.
func NewClickHouseCensusSink(baseURL, user, password, table string) (*ClickHouseCensusSink, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("clickhouse census sink: base URL %q is not an absolute http(s) URL", baseURL)
	}
	if table == "" {
		table = DefaultCensusTable
	}
	return &ClickHouseCensusSink{
		baseURL:  strings.TrimRight(baseURL, "/"),
		user:     user,
		password: password,
		table:    table,
		client:   &http.Client{},
	}, nil
}

// censusInsertRow is one JSONEachRow line: the header fields denormalized
// onto every tenant row (embedding flattens CensusRow's own JSON tags).
// Window bounds are unix seconds — ClickHouse DateTime columns accept
// integer epoch seconds natively in JSONEachRow.
type censusInsertRow struct {
	SchemaVersion int    `json:"schema_version"`
	Host          string `json:"host"`
	WindowStart   int64  `json:"window_start"`
	WindowEnd     int64  `json:"window_end"`
	CensusRow
}

// PutCensus inserts one snapshot, retrying up to censusInsertAttempts times
// and returning the last error when the budget is exhausted (the caller
// drops the window). A snapshot with zero tenant rows is a successful no-op.
func (s *ClickHouseCensusSink) PutCensus(ctx context.Context, header CensusHeader, rows []CensusRow) error {
	if len(rows) == 0 {
		return nil
	}

	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for i := range rows {
		if err := enc.Encode(censusInsertRow{
			SchemaVersion: header.SchemaVersion,
			Host:          header.Host,
			WindowStart:   header.WindowStartMs / 1000,
			WindowEnd:     header.WindowEndMs / 1000,
			CensusRow:     rows[i],
		}); err != nil {
			return fmt.Errorf("encode census row: %w", err)
		}
	}

	params := url.Values{}
	params.Set("query", fmt.Sprintf("INSERT INTO %s FORMAT JSONEachRow", s.table))
	// Deterministic per window: retries (including ambiguous timeouts whose
	// insert actually landed) are exactly-once.
	params.Set("insert_deduplication_token", fmt.Sprintf("%s/%d", header.Host, header.WindowEndMs))
	params.Set("async_insert", "0")
	params.Set("wait_for_async_insert", "1")
	params.Set("input_format_skip_unknown_fields", "1")
	insertURL := s.baseURL + "/?" + params.Encode()

	var lastErr error
	for attempt := 1; attempt <= censusInsertAttempts; attempt++ {
		if attempt > 1 {
			select {
			case <-ctx.Done():
				return lastErr
			case <-time.After(censusInsertRetryPause):
			}
		}
		lastErr = s.insertOnce(ctx, insertURL, body.Bytes())
		if lastErr == nil {
			return nil
		}
	}
	return fmt.Errorf("census insert failed after %d attempts: %w", censusInsertAttempts, lastErr)
}

func (s *ClickHouseCensusSink) insertOnce(ctx context.Context, insertURL string, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, censusInsertAttemptTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, insertURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("X-ClickHouse-User", s.user)
	req.Header.Set("X-ClickHouse-Key", s.password)

	res, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		// ClickHouse puts the error text in the body; keep a bounded snippet.
		snippet, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		return fmt.Errorf("clickhouse insert: status %d: %s", res.StatusCode, strings.TrimSpace(string(snippet)))
	}
	return nil
}
