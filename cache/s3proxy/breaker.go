package s3proxy

import (
	"errors"
	"sync"
	"time"

	"github.com/buchgr/bazel-remote/v2/cache"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// A hand-rolled circuit breaker for the MinIO edge, modeled on the FA
// agent's failureBreaker (agent/ingress/failure_breaker.go): a mutex, a
// counter and a timestamp — no goroutines, no timers, state advances
// lazily on the requests that consult it. Hand-rolled rather than a
// library because this module is embedded in the FA agent, and an extra
// module in the fork's go.mod ripples into the agent's module graph.
//
// Policy:
//
//   - Consecutive-failure trip only: breakerConsecutiveFailures failures
//     in a row open the breaker. There is deliberately no failure-ratio /
//     rolling-window trip: the read deadline already bounds what a
//     brownout can cost per request, and consecutive-only matches the
//     agent's breaker precedent.
//   - Open lasts breakerTimeout; then half-open admits exactly ONE probe.
//     Probe success closes the breaker and resets the streak; probe
//     failure re-opens it for another fixed breakerTimeout — no
//     exponential backoff, because a cache edge wants fast re-probe
//     (unlike job adoption, a wasted probe here costs one request, not a
//     job).
//   - Outcome classification is the caller's job (breakerReadOutcome,
//     breakerUploadOutcome): a not-found is a success (MinIO answered), a
//     parent-context cancellation is counted neither way.
//   - Streamed GETs defer their outcome to the body's terminal state via
//     allow/record and bodyOutcomeReadCloser (healthy headers prove nothing
//     in a brownout), holding a half-open probe slot until the body
//     finishes. Write-through uploads never probe at all (ExecuteNoProbe).

var (
	breakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bazel_remote_s3_breaker_state",
		Help: "Per-backend MinIO circuit breaker state: 0=closed, 1=half-open, 2=open.",
	}, []string{"backend"})
	breakerTransitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bazel_remote_s3_breaker_transitions_total",
		Help: "Per-backend MinIO circuit breaker state transitions, labelled by the state transitioned to.",
	}, []string{"backend", "to"})
)

// breakerConsecutiveFailures is the trip threshold: healthy backends
// virtually never fail several calls back-to-back, while a sick one fails
// every single call, so a small streak separates the two cleanly.
const breakerConsecutiveFailures = 5

// breakerTimeout is how long the breaker stays open before admitting a
// half-open probe. A var only so tests can shrink it.
var breakerTimeout = 15 * time.Second

// Breaker states. The numeric values are the wire contract of the
// bazel_remote_s3_breaker_state gauge.
const (
	breakerClosed int32 = iota // 0
	breakerHalfOpen            // 1
	breakerOpen                // 2
)

func breakerStateName(state int32) string {
	switch state {
	case breakerOpen:
		return "open"
	case breakerHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

// breakerOutcome is how a wrapped call reports back to the breaker.
type breakerOutcome int

const (
	outcomeSuccess breakerOutcome = iota
	outcomeFailure
	// outcomeIgnore is for results that say nothing about backend health
	// (e.g. the client canceled the request): counted neither way.
	outcomeIgnore
)

// errBreakerOpen is returned by Execute when the breaker refuses a call
// without dialing MinIO: it is open, or half-open with the probe already
// in flight.
var errBreakerOpen = errors.New("circuit breaker is open")

// breaker is one backend's circuit breaker. Safe for concurrent use by the
// upload workers and the request-path reads.
type breaker struct {
	// name is the backend's key (backends-map selector, endpoint fallback):
	// the "backend" label on the breaker metrics — matching every other
	// backend-labeled series so they join on dashboards — and the subject
	// of the transition log lines.
	name        string
	errorLogger cache.Logger

	mu                  sync.Mutex
	state               int32
	consecutiveFailures int
	// openedAt is when the breaker last opened; open -> half-open advances
	// lazily once breakerTimeout has elapsed past it.
	openedAt time.Time
	// probeInFlight gates half-open to a single admitted call.
	probeInFlight bool
}

func newBreaker(name string, errorLogger cache.Logger) *breaker {
	// Publish the closed state eagerly so the series exists before the
	// first transition.
	breakerState.WithLabelValues(name).Set(float64(breakerClosed))
	return &breaker{name: name, errorLogger: errorLogger}
}

// Execute runs call unless the breaker refuses it (errBreakerOpen), and
// feeds the reported outcome back into the breaker. The lock is never held
// across the call itself.
func (b *breaker) Execute(call func() breakerOutcome) error {
	if !b.allow() {
		return errBreakerOpen
	}
	b.record(call())
	return nil
}

// ExecuteNoProbe is Execute for calls too expensive to serve as the
// half-open recovery probe — write-through PutObjects bounded only by the
// 10m uploadTimeout. If such a call claimed the single probe slot, every
// read on the backend would keep failing fast as errBreakerOpen for however
// long the upload takes, so a recovered MinIO could serve artificial misses
// for minutes. Instead these calls run only while the breaker is closed and
// fail fast otherwise; probing is left to the read path, whose calls are
// bounded in seconds and arrive constantly (the host's FindMissingBlobs
// falls through to a backend Stat before every upload cycle).
func (b *breaker) ExecuteNoProbe(call func() breakerOutcome) error {
	b.mu.Lock()
	closed := b.state == breakerClosed
	b.mu.Unlock()
	if !closed {
		return errBreakerOpen
	}
	b.record(call())
	return nil
}

// allow decides whether a call may dial the backend, advancing
// open -> half-open lazily once breakerTimeout has elapsed.
func (b *breaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case breakerOpen:
		if time.Since(b.openedAt) < breakerTimeout {
			return false
		}
		b.transition(breakerHalfOpen)
		b.probeInFlight = true
		return true
	case breakerHalfOpen:
		// Only reachable when the previous probe reported outcomeIgnore
		// (probe failure re-opens, probe success closes): the next request
		// becomes the new probe.
		if b.probeInFlight {
			return false
		}
		b.probeInFlight = true
		return true
	default:
		return true
	}
}

// record feeds a call's outcome back into the breaker. Outcomes are not
// matched to the state that admitted the call: a straggler admitted while
// closed whose failure lands after the trip is simply ignored (the open
// window from the trip stands), and one landing during half-open counts
// like a probe result — a fresh failure signal during recovery is a valid
// reason to re-open.
func (b *breaker) record(outcome breakerOutcome) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == breakerHalfOpen {
		b.probeInFlight = false
	}
	switch outcome {
	case outcomeIgnore:
	case outcomeSuccess:
		switch b.state {
		case breakerClosed:
			b.consecutiveFailures = 0
		case breakerHalfOpen:
			b.consecutiveFailures = 0
			b.transition(breakerClosed)
		case breakerOpen:
			// Straggler; recovery goes through the probe cycle.
		}
	case outcomeFailure:
		switch b.state {
		case breakerClosed:
			b.consecutiveFailures++
			if b.consecutiveFailures >= breakerConsecutiveFailures {
				b.openedAt = time.Now()
				b.transition(breakerOpen)
			}
		case breakerHalfOpen:
			// Probe failed: re-open for another fixed window.
			b.openedAt = time.Now()
			b.transition(breakerOpen)
		case breakerOpen:
			// Straggler; the open window from the trip stands.
		}
	}
}

// transition moves the breaker to state to, exporting the gauge, the
// transition counter and a log line. Transitions are inherently rare, so
// the log is not rate limited. Callers must hold b.mu.
func (b *breaker) transition(to int32) {
	from := b.state
	b.state = to
	breakerState.WithLabelValues(b.name).Set(float64(to))
	breakerTransitions.WithLabelValues(b.name, breakerStateName(to)).Inc()
	if b.errorLogger != nil {
		b.errorLogger.Printf("S3 circuit breaker %s: %s -> %s",
			b.name, breakerStateName(from), breakerStateName(to))
	}
}

// State returns a snapshot of the current state (open -> half-open
// advancement is left to allow). Test seam.
func (b *breaker) State() int32 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}
