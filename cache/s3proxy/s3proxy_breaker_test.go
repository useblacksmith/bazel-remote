package s3proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	stdlog "log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/utils/backendproxy"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

const testHash = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

// failingServer serves 500 to every request and counts them, standing in
// for a sick MinIO node that still answers (as opposed to one that hangs).
func failingServer(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var requests atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(ts.Close)
	return ts, &requests
}

// coreFor builds a minio core against a test server. maxRetries 1 keeps
// failure-simulation tests fast (the retry cap itself is covered by
// TestNewBackendCapsMinioRetries).
func coreFor(t *testing.T, ts *httptest.Server, maxRetries int) *minio.Core {
	t.Helper()
	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	core, err := minio.NewCore(u.Host, &minio.Options{
		Creds:        credentials.NewStaticV4("KEY", "SECRET", ""),
		Secure:       false,
		BucketLookup: minio.BucketLookupPath,
		MaxRetries:   maxRetries,
		// A fixed region skips minio-go's bucket-location lookup, keeping
		// request counts deterministic against the fake backends.
		Region: "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	return core
}

func breakerCache(t *testing.T, core *minio.Core, breakerName string, errorLogger cache.Logger) *s3Cache {
	t.Helper()
	return &s3Cache{
		key:          breakerName,
		mcore:        core,
		bucket:       "test-bucket",
		breaker:      newBreaker(breakerName, errorLogger),
		objectKey:    objectKeyV1,
		readDeadline: readDeadline,
		accessLogger: stdlog.New(&bytes.Buffer{}, "", 0),
		errorLogger:  errorLogger,
	}
}

// tripBreaker forces the breaker open with simulated consecutive failures.
func tripBreaker(t *testing.T, cb *breaker) {
	t.Helper()
	for i := 0; i < breakerConsecutiveFailures; i++ {
		_ = cb.Execute(func() breakerOutcome { return outcomeFailure })
	}
	if cb.State() != breakerOpen {
		t.Fatalf("breaker state after %d consecutive failures = %v, want open", breakerConsecutiveFailures, cb.State())
	}
}

// TestBreakerTripsAndReadsFailFastWhenOpen drives the breaker open through
// the real Get path (5 consecutive 500s) and pins the open-state contract:
// Get returns the miss convention (nil, -1, nil) and Contains returns
// (false, -1), both immediately and without dialing MinIO, and the state
// change exports the gauge, the transition counter, and a log line.
func TestBreakerTripsAndReadsFailFastWhenOpen(t *testing.T) {
	ts, requests := failingServer(t)
	var errBuf bytes.Buffer
	name := "breaker-read-test/test-bucket"
	c := breakerCache(t, coreFor(t, ts, 1), name, stdlog.New(&errBuf, "", 0))

	for i := 0; i < breakerConsecutiveFailures; i++ {
		if _, _, err := c.Get(context.Background(), cache.CAS, testHash, -1); err == nil {
			t.Fatalf("Get %d against failing backend returned nil error", i)
		}
	}
	if c.breaker.State() != breakerOpen {
		t.Fatalf("breaker state after %d failing Gets = %v, want open", breakerConsecutiveFailures, c.breaker.State())
	}

	// Open breaker: Get is a clean, immediate miss with no dial.
	dialed := requests.Load()
	start := time.Now()
	rc, size, err := c.Get(context.Background(), cache.CAS, testHash, -1)
	elapsed := time.Since(start)
	if rc != nil || size != -1 || err != nil {
		t.Fatalf("open-breaker Get = (%v, %d, %v), want (nil, -1, nil) miss convention", rc, size, err)
	}
	if elapsed >= 100*time.Millisecond {
		t.Fatalf("open-breaker Get took %v, want <100ms (must not dial)", elapsed)
	}
	if got := requests.Load(); got != dialed {
		t.Fatalf("open-breaker Get dialed the backend (%d -> %d requests)", dialed, got)
	}

	// Same for Contains: the existing "not present" convention.
	exists, size := c.Contains(context.Background(), cache.CAS, testHash, -1)
	if exists || size != -1 {
		t.Fatalf("open-breaker Contains = (%v, %d), want (false, -1)", exists, size)
	}
	if got := requests.Load(); got != dialed {
		t.Fatalf("open-breaker Contains dialed the backend (%d -> %d requests)", dialed, got)
	}

	// Observability: gauge at 2 (open), one transition to open, a log line.
	if got := testutil.ToFloat64(breakerState.WithLabelValues(name)); got != 2 {
		t.Fatalf("breaker state gauge = %v, want 2 (open)", got)
	}
	if got := testutil.ToFloat64(breakerTransitions.WithLabelValues(name, "open")); got != 1 {
		t.Fatalf("breaker transitions{to=open} = %v, want 1", got)
	}
	if !strings.Contains(errBuf.String(), "closed -> open") {
		t.Fatalf("state-change log missing, got %q", errBuf.String())
	}
}

// TestBreakerOpenClassifiesUploadBreakerOpen pins the write-path contract:
// with the breaker open, an upload item fails fast without dialing MinIO
// and lands in the terminal-outcome counter and the OperationObserver with
// the distinct breaker_open reason.
func TestBreakerOpenClassifiesUploadBreakerOpen(t *testing.T) {
	ts, requests := failingServer(t)
	observer := &recordingObserver{}
	name := "breaker-upload-test/test-bucket"
	c := breakerCache(t, coreFor(t, ts, 1), name, nil)
	c.observer = observer
	tripBreaker(t, c.breaker)

	before := testutil.ToFloat64(uploadOutcomes.WithLabelValues(name, "error", "breaker_open"))
	c.UploadFile(backendproxy.UploadReq{
		Hash:       testHash,
		Kind:       cache.CAS,
		SizeOnDisk: 4,
		Rc:         io.NopCloser(strings.NewReader("blob")),
	})

	if got := requests.Load(); got != 0 {
		t.Fatalf("open-breaker UploadFile dialed the backend (%d requests)", got)
	}
	if got := testutil.ToFloat64(uploadOutcomes.WithLabelValues(name, "error", "breaker_open")) - before; got != 1 {
		t.Fatalf("uploadOutcomes{error,breaker_open} delta = %v, want 1", got)
	}
	if len(observer.outcomes) != 1 {
		t.Fatalf("observer outcomes len = %d, want 1", len(observer.outcomes))
	}
	outcome := observer.outcomes[0]
	if outcome.Method != "backend_upload" || outcome.Status != "error" || outcome.Reason != "breaker_open" {
		t.Fatalf("unexpected outcome: %+v", outcome)
	}
}

// TestBreakerHalfOpenProbeClosesOnSuccess pins the recovery path: after the
// open timeout the breaker admits ONE probe, and its success closes the
// breaker again.
func TestBreakerHalfOpenProbeClosesOnSuccess(t *testing.T) {
	oldTimeout := breakerTimeout
	breakerTimeout = 50 * time.Millisecond
	defer func() { breakerTimeout = oldTimeout }()

	backend := s3mem.New()
	if err := backend.CreateBucket("test-bucket"); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(ts.Close)
	core := coreFor(t, ts, 1)
	c := breakerCache(t, core, "breaker-halfopen-test/test-bucket", nil)

	content := []byte("blob")
	key := objectKeyV1("", testHash, cache.CAS)
	if _, err := core.PutObject(context.Background(), "test-bucket", key,
		bytes.NewReader(content), int64(len(content)), "", "", minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}

	tripBreaker(t, c.breaker)
	if rc, size, err := c.Get(context.Background(), cache.CAS, testHash, -1); rc != nil || size != -1 || err != nil {
		t.Fatalf("open-breaker Get = (%v, %d, %v), want miss convention despite the object existing", rc, size, err)
	}

	time.Sleep(breakerTimeout + 20*time.Millisecond)

	// Half-open: the single probe goes through, and its success closes
	// the breaker.
	rc, _, err := c.Get(context.Background(), cache.CAS, testHash, -1)
	if err != nil || rc == nil {
		t.Fatalf("half-open probe Get = (%v, %v), want hit", rc, err)
	}
	data, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || string(data) != "blob" {
		t.Fatalf("half-open probe Get read (%q, %v), want \"blob\"", data, err)
	}
	if c.breaker.State() != breakerClosed {
		t.Fatalf("breaker state after successful probe = %v, want closed", c.breaker.State())
	}
}

// TestBreakerProbeFailureReopens pins the other half of the recovery path:
// a failed half-open probe re-opens the breaker for another fixed window
// (no exponential backoff), and a later successful probe still closes it.
func TestBreakerProbeFailureReopens(t *testing.T) {
	oldTimeout := breakerTimeout
	breakerTimeout = 50 * time.Millisecond
	defer func() { breakerTimeout = oldTimeout }()

	backend := s3mem.New()
	if err := backend.CreateBucket("test-bucket"); err != nil {
		t.Fatal(err)
	}
	fake := gofakes3.New(backend).Server()
	var failing atomic.Bool
	var requests atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if failing.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		fake.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	core := coreFor(t, ts, 1)
	c := breakerCache(t, core, "breaker-reprobe-test/test-bucket", nil)

	content := []byte("blob")
	key := objectKeyV1("", testHash, cache.CAS)
	if _, err := core.PutObject(context.Background(), "test-bucket", key,
		bytes.NewReader(content), int64(len(content)), "", "", minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}

	failing.Store(true)
	tripBreaker(t, c.breaker)
	time.Sleep(breakerTimeout + 20*time.Millisecond)

	// The half-open probe dials the (still sick) backend, fails, and
	// re-opens the breaker.
	if _, _, err := c.Get(context.Background(), cache.CAS, testHash, -1); err == nil {
		t.Fatal("failed probe Get returned nil error")
	}
	if c.breaker.State() != breakerOpen {
		t.Fatalf("breaker state after failed probe = %v, want re-opened", c.breaker.State())
	}

	// Re-opened: the next request is refused without a dial.
	dialed := requests.Load()
	if rc, size, err := c.Get(context.Background(), cache.CAS, testHash, -1); rc != nil || size != -1 || err != nil {
		t.Fatalf("re-opened Get = (%v, %d, %v), want miss convention", rc, size, err)
	}
	if got := requests.Load(); got != dialed {
		t.Fatalf("re-opened Get dialed the backend (%d -> %d requests)", dialed, got)
	}

	// After another fixed window a healthy probe closes the breaker.
	failing.Store(false)
	time.Sleep(breakerTimeout + 20*time.Millisecond)
	rc, _, err := c.Get(context.Background(), cache.CAS, testHash, -1)
	if err != nil || rc == nil {
		t.Fatalf("recovered probe Get = (%v, %v), want hit", rc, err)
	}
	_ = rc.Close()
	if c.breaker.State() != breakerClosed {
		t.Fatalf("breaker state after recovered probe = %v, want closed", c.breaker.State())
	}
}

// TestBreakerHalfOpenAdmitsSingleProbe pins the probe gate: while the one
// admitted half-open probe is still in flight, a concurrent request is
// refused without dialing the backend.
func TestBreakerHalfOpenAdmitsSingleProbe(t *testing.T) {
	oldTimeout := breakerTimeout
	breakerTimeout = 50 * time.Millisecond
	defer func() { breakerTimeout = oldTimeout }()

	backend := s3mem.New()
	if err := backend.CreateBucket("test-bucket"); err != nil {
		t.Fatal(err)
	}
	fake := gofakes3.New(backend).Server()
	var gateArmed atomic.Bool
	var requests atomic.Int64
	gate := make(chan struct{})
	probeArrived := make(chan struct{}, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if gateArmed.Load() {
			select {
			case probeArrived <- struct{}{}:
			default:
			}
			<-gate
		}
		fake.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	core := coreFor(t, ts, 1)
	c := breakerCache(t, core, "breaker-probegate-test/test-bucket", nil)

	content := []byte("blob")
	key := objectKeyV1("", testHash, cache.CAS)
	if _, err := core.PutObject(context.Background(), "test-bucket", key,
		bytes.NewReader(content), int64(len(content)), "", "", minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}

	tripBreaker(t, c.breaker)
	gateArmed.Store(true)
	time.Sleep(breakerTimeout + 20*time.Millisecond)

	// The probe is admitted and parks inside the backend, holding the
	// half-open slot.
	probeDone := make(chan error, 1)
	go func() {
		rc, _, err := c.Get(context.Background(), cache.CAS, testHash, -1)
		if rc != nil {
			_ = rc.Close()
		}
		probeDone <- err
	}()
	<-probeArrived
	dialed := requests.Load()

	// A second request during the in-flight probe is refused, miss-shaped,
	// without a dial.
	if rc, size, err := c.Get(context.Background(), cache.CAS, testHash, -1); rc != nil || size != -1 || err != nil {
		t.Fatalf("Get during in-flight probe = (%v, %d, %v), want miss convention", rc, size, err)
	}
	if got := requests.Load(); got != dialed {
		t.Fatalf("Get during in-flight probe dialed the backend (%d -> %d requests)", dialed, got)
	}
	if c.breaker.State() != breakerHalfOpen {
		t.Fatalf("breaker state during in-flight probe = %v, want half-open", c.breaker.State())
	}

	// Release the probe; its success closes the breaker.
	gateArmed.Store(false)
	close(gate)
	if err := <-probeDone; err != nil {
		t.Fatalf("released probe Get err = %v, want nil", err)
	}
	if c.breaker.State() != breakerClosed {
		t.Fatalf("breaker state after released probe = %v, want closed", c.breaker.State())
	}
}

// TestNotFoundDoesNotTripBreaker pins the miss-is-healthy rule: a burst of
// genuine not-found answers keeps the breaker closed with zero recorded
// failures, on both the Get and the Contains path.
func TestNotFoundDoesNotTripBreaker(t *testing.T) {
	backend := s3mem.New()
	if err := backend.CreateBucket("test-bucket"); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(ts.Close)
	c := breakerCache(t, coreFor(t, ts, 1), "breaker-notfound-test/test-bucket", nil)

	for i := 0; i < 3*breakerConsecutiveFailures; i++ {
		if rc, size, err := c.Get(context.Background(), cache.CAS, testHash, -1); rc != nil || size != -1 || err != nil {
			t.Fatalf("Get for absent object = (%v, %d, %v), want (nil, -1, nil)", rc, size, err)
		}
		if exists, _ := c.Contains(context.Background(), cache.CAS, testHash, -1); exists {
			t.Fatal("Contains for absent object = true, want false")
		}
	}
	if c.breaker.State() != breakerClosed {
		t.Fatalf("breaker state after misses = %v, want closed", c.breaker.State())
	}
	if got := c.breaker.consecutiveFailures; got != 0 {
		t.Fatalf("breaker recorded %d consecutive failures for genuine misses, want 0", got)
	}
}

// TestReadDeadlineBoundsHungGet pins the read deadline: a backend that
// accepts the connection and never responds must fail the Get once
// readDeadline elapses (counting as a breaker failure), instead of pinning
// the caller for the transport's 60s+.
func TestReadDeadlineBoundsHungGet(t *testing.T) {
	oldDeadline := readDeadline
	readDeadline = 100 * time.Millisecond
	defer func() { readDeadline = oldDeadline }()

	hung := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-hung // Hold every request open until the test finishes.
	}))
	t.Cleanup(ts.Close)
	t.Cleanup(func() { close(hung) })

	c := breakerCache(t, coreFor(t, ts, 1), "breaker-deadline-test/test-bucket", nil)

	start := time.Now()
	rc, _, err := c.Get(context.Background(), cache.CAS, testHash, -1)
	elapsed := time.Since(start)
	if err == nil || rc != nil {
		t.Fatalf("hung-backend Get = (%v, %v), want deadline error", rc, err)
	}
	if elapsed >= 2*time.Second {
		t.Fatalf("hung-backend Get took %v, want ~readDeadline (100ms)", elapsed)
	}
	if got := c.breaker.consecutiveFailures; got != 1 {
		t.Fatalf("breaker consecutive failures after deadline-exceeded Get = %d, want 1", got)
	}
}

// TestSlowBodySurvivesConnectTimeoutLegs pins the deadline split: connection
// establishment is bounded tight (connectTimeout on the transport), but a
// healthy backend that answered fast may take its time streaming the body —
// only the overall readDeadline governs that. A body that dribbles in over
// several write-flush-sleep rounds must arrive complete, not be guillotined
// by any handshake-scale timeout.
func TestSlowBodySurvivesConnectTimeoutLegs(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 64)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		for i := 0; i < len(payload); i += 16 {
			_, _ = w.Write(payload[i : i+16])
			flusher.Flush()
			time.Sleep(75 * time.Millisecond)
		}
	}))
	t.Cleanup(ts.Close)

	c := breakerCache(t, coreFor(t, ts, 1), "slow-body-test/test-bucket", nil)
	c.readDeadline = 5 * time.Second // ~300ms of body dribble, generous ceiling

	rc, size, err := c.Get(context.Background(), cache.CAS, testHash, -1)
	if err != nil || rc == nil {
		t.Fatalf("slow-body Get = (%v, %d, %v), want streamed success", rc, size, err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading slow body: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("slow body arrived corrupted: got %d bytes, want %d", len(got), len(payload))
	}
	if failures := c.breaker.consecutiveFailures; failures != 0 {
		t.Fatalf("healthy slow body counted %d breaker failures, want 0", failures)
	}
}

func TestWithReadDeadlineOption(t *testing.T) {
	c := &s3Cache{readDeadline: readDeadline}
	WithReadDeadline(30 * time.Second)(c)
	if c.readDeadline != 30*time.Second {
		t.Fatalf("WithReadDeadline(30s) left %v", c.readDeadline)
	}
	// Non-positive keeps the current value — config passes zero for "unset".
	WithReadDeadline(0)(c)
	if c.readDeadline != 30*time.Second {
		t.Fatalf("WithReadDeadline(0) must be a no-op, left %v", c.readDeadline)
	}
}

// TestNewBackendCapsMinioRetries pins the retry cap on the constructed
// client: one failing Get must dial the backend exactly maxRetries times
// (minio-go's library default of 10 attempts is what turned each failing
// call into ~a minute of retention).
func TestNewBackendCapsMinioRetries(t *testing.T) {
	ts, requests := failingServer(t)
	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}

	c, err := newBackend(BackendSpec{
		Key:              "retry-cap-test",
		Endpoint:         u.Host,
		Bucket:           "test-bucket",
		BucketLookupType: minio.BucketLookupPath,
		Credentials:      credentials.NewStaticV4("KEY", "SECRET", ""),
		DisableSSL:       true,
		// A fixed region skips minio-go's bucket-location lookup, so every
		// counted request is a real GET attempt.
		Region: "us-east-1",
	}, false, -1, "uncompressed", stdlog.New(&bytes.Buffer{}, "", 0), nil, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := c.Get(context.Background(), cache.CAS, testHash, -1); err == nil {
		t.Fatal("Get against failing backend returned nil error")
	}
	if got := requests.Load(); got != maxRetries {
		t.Fatalf("failing Get dialed the backend %d times, want %d (retry cap)", got, maxRetries)
	}
}
