package zstdimpl

// Bounded zstd transcoding: a ZstdImpl whose streaming encoders are counted
// against a fixed admission budget, so on-demand compression of
// identity-stored CAS blobs cannot allocate encoder state (window buffers
// and goroutines) proportional to read concurrency.
//
// Measurements with the production encoder configuration (klauspost zstd,
// SpeedFastest, concurrency 1) show each live streaming encoder retains a
// constant ~8.7 MiB at the default 8 MiB window and ~2.7 MiB at a 1 MiB
// window, independent of blob size (see utils/zstdpool/codecmem_test.go).
// A count-based budget therefore bounds encoder memory exactly.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/semaphore"
)

// ErrEncoderSaturated is returned by bounded implementations when encoder
// capacity could not be acquired within the admission timeout. Callers
// should surface it as a retryable, resource-exhausted condition and must
// not treat it as data corruption.
var ErrEncoderSaturated = errors.New("zstd encoder capacity saturated")

// EncoderAdmissionOutcome labels the result of one encoder admission
// attempt for metrics purposes.
type EncoderAdmissionOutcome string

const (
	EncoderAdmitted          EncoderAdmissionOutcome = "admitted"
	EncoderAdmissionTimeout  EncoderAdmissionOutcome = "timeout"
	EncoderAdmissionCanceled EncoderAdmissionOutcome = "canceled"
)

// ZstdMetrics receives measurements from a bounded ZstdImpl.
// Implementations must be safe for concurrent use. All methods are called
// synchronously on request paths and must not block.
type ZstdMetrics interface {
	// EncoderAdmissionStarted is called when an admission attempt begins.
	// Waiter gauges can be derived from started minus completed attempts.
	EncoderAdmissionStarted()
	// EncoderAdmissionCompleted is called when an admission attempt ends,
	// with the time spent waiting and the outcome.
	EncoderAdmissionCompleted(outcome EncoderAdmissionOutcome, wait time.Duration)
	// EncoderReleased is called when an admitted encoder's capacity is
	// returned. Active-encoder gauges can be derived from admitted minus
	// released.
	EncoderReleased()
}

// ZstdLimits configures NewBoundedGoZstd.
type ZstdLimits struct {
	// MaxActiveEncoders is the number of streaming encoders that may be
	// live at once. Must be positive.
	MaxActiveEncoders int64

	// EncoderAdmissionTimeout bounds the wait for encoder capacity before
	// GetEncoder fails with ErrEncoderSaturated. Zero means wait until the
	// caller's context is done.
	EncoderAdmissionTimeout time.Duration

	// EncoderWindowSizeBytes overrides the encoder window size. Zero keeps
	// the library default (8 MiB, ~8.7 MiB retained per encoder); 1 MiB
	// reduces retention to ~2.7 MiB per encoder at a small compression
	// ratio cost. Affects only the transfer encoding, never stored bytes.
	EncoderWindowSizeBytes int

	// Metrics is an optional sink for admission measurements.
	Metrics ZstdMetrics
}

// boundedGoZstd delegates everything except streaming-encoder creation to
// the unbounded Go implementation. Decoders and the EncodeAll/DecodeAll
// paths keep their existing behavior: FA's identity storage mode never uses
// EncodeAll, and write-path decoders are bounded by the embedder's
// write-stream admission.
type boundedGoZstd struct {
	goZstd

	sem      *semaphore.Weighted
	timeout  time.Duration
	metrics  ZstdMetrics
	encoders chan *zstd.Encoder // free list; retention == MaxActiveEncoders
	options  []zstd.EOption
}

// NewBoundedGoZstd returns a ZstdImpl backed by the pure-Go zstd
// implementation whose streaming encoders are bounded by limits. Encoders
// are retained on a free list of the same size as the admission budget, so
// steady-state memory equals the worst-case bound and does not churn.
func NewBoundedGoZstd(limits ZstdLimits) (ZstdImpl, error) {
	if limits.MaxActiveEncoders <= 0 {
		return nil, fmt.Errorf("MaxActiveEncoders must be positive: %d", limits.MaxActiveEncoders)
	}
	if limits.EncoderAdmissionTimeout < 0 {
		return nil, fmt.Errorf("EncoderAdmissionTimeout must not be negative: %v", limits.EncoderAdmissionTimeout)
	}
	if limits.EncoderWindowSizeBytes < 0 {
		return nil, fmt.Errorf("EncoderWindowSizeBytes must not be negative: %d", limits.EncoderWindowSizeBytes)
	}

	options := []zstd.EOption{
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderLevel(zstd.SpeedFastest),
	}
	if limits.EncoderWindowSizeBytes > 0 {
		options = append(options, zstd.WithWindowSize(limits.EncoderWindowSizeBytes))
	}

	// Validate the options eagerly so a misconfigured window size fails at
	// startup instead of on the first read.
	probe, err := zstd.NewWriter(nil, options...)
	if err != nil {
		return nil, fmt.Errorf("invalid encoder options: %w", err)
	}
	_ = probe.Close()

	return &boundedGoZstd{
		sem:      semaphore.NewWeighted(limits.MaxActiveEncoders),
		timeout:  limits.EncoderAdmissionTimeout,
		metrics:  limits.Metrics,
		encoders: make(chan *zstd.Encoder, limits.MaxActiveEncoders),
		options:  options,
	}, nil
}

func (b *boundedGoZstd) GetEncoder(ctx context.Context, out io.WriteCloser) (zstdEncoder, error) {
	if b.metrics != nil {
		b.metrics.EncoderAdmissionStarted()
	}
	waitStarted := time.Now()

	acquireCtx := ctx
	var cancel context.CancelFunc
	if b.timeout > 0 {
		acquireCtx, cancel = context.WithTimeout(ctx, b.timeout)
		defer cancel()
	}

	err := b.sem.Acquire(acquireCtx, 1)
	if err != nil {
		outcome := EncoderAdmissionCanceled
		// Distinguish the admission timeout from the caller's own
		// cancellation or deadline.
		if ctx.Err() == nil && errors.Is(acquireCtx.Err(), context.DeadlineExceeded) {
			outcome = EncoderAdmissionTimeout
			err = fmt.Errorf("%w: no encoder capacity within %v", ErrEncoderSaturated, b.timeout)
		} else {
			err = ctx.Err()
		}
		if b.metrics != nil {
			b.metrics.EncoderAdmissionCompleted(outcome, time.Since(waitStarted))
		}
		return nil, err
	}
	if b.metrics != nil {
		b.metrics.EncoderAdmissionCompleted(EncoderAdmitted, time.Since(waitStarted))
	}

	var enc *zstd.Encoder
	select {
	case enc = <-b.encoders:
	default:
		// The semaphore guarantees at most MaxActiveEncoders live encoders,
		// so this allocation cannot exceed the budget.
		enc, err = zstd.NewWriter(nil, b.options...)
		if err != nil {
			b.release(nil)
			return nil, err
		}
	}

	enc.Reset(out)
	return &boundedEncoder{owner: b, enc: enc}, nil
}

// release returns capacity and, when the encoder is still healthy, retains
// it on the free list. A nil encoder (or a full free list, which cannot
// happen while the semaphore is held but is handled defensively) releases
// capacity only.
func (b *boundedGoZstd) release(enc *zstd.Encoder) {
	if enc != nil {
		select {
		case b.encoders <- enc:
		default:
		}
	}
	b.sem.Release(1)
	if b.metrics != nil {
		b.metrics.EncoderReleased()
	}
}

// boundedEncoder wraps a *zstd.Encoder and returns its admission slot on
// Close. Close is idempotent: casblob's error paths and deferred cleanup
// may both close the encoder.
type boundedEncoder struct {
	owner  *boundedGoZstd
	enc    *zstd.Encoder
	closed bool
}

func (e *boundedEncoder) Write(p []byte) (int, error) {
	return e.enc.Write(p)
}

func (e *boundedEncoder) ReadFrom(r io.Reader) (int64, error) {
	return e.enc.ReadFrom(r)
}

func (e *boundedEncoder) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true

	err := e.enc.Close()
	if err != nil {
		// A failed Close may leave internal state inconsistent; drop the
		// encoder rather than recycling it. Capacity is still released.
		e.owner.release(nil)
		return err
	}
	e.owner.release(e.enc)
	return nil
}
