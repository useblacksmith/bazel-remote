package zstdimpl

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }

type countingMetrics struct {
	started   atomic.Int64
	admitted  atomic.Int64
	timeouts  atomic.Int64
	canceled  atomic.Int64
	released  atomic.Int64
	waitTotal atomic.Int64
}

func (m *countingMetrics) EncoderAdmissionStarted() {
	m.started.Add(1)
}

func (m *countingMetrics) EncoderAdmissionCompleted(outcome EncoderAdmissionOutcome, wait time.Duration) {
	m.waitTotal.Add(int64(wait))
	switch outcome {
	case EncoderAdmitted:
		m.admitted.Add(1)
	case EncoderAdmissionTimeout:
		m.timeouts.Add(1)
	case EncoderAdmissionCanceled:
		m.canceled.Add(1)
	}
}

func (m *countingMetrics) EncoderReleased() {
	m.released.Add(1)
}

func newBounded(t *testing.T, limits ZstdLimits) ZstdImpl {
	t.Helper()
	impl, err := NewBoundedGoZstd(limits)
	if err != nil {
		t.Fatal(err)
	}
	return impl
}

func TestBoundedGoZstdValidation(t *testing.T) {
	for _, limits := range []ZstdLimits{
		{MaxActiveEncoders: 0},
		{MaxActiveEncoders: -1},
		{MaxActiveEncoders: 1, EncoderAdmissionTimeout: -time.Second},
		{MaxActiveEncoders: 1, EncoderWindowSizeBytes: -1},
	} {
		if _, err := NewBoundedGoZstd(limits); err == nil {
			t.Errorf("expected error for limits %+v", limits)
		}
	}
}

func TestBoundedGoZstdRoundTrip(t *testing.T) {
	// Exercise both the default window and an overridden window, and
	// verify output decodes back to the input in both cases.
	for _, windowBytes := range []int{0, 1 << 20} {
		impl := newBounded(t, ZstdLimits{
			MaxActiveEncoders:      2,
			EncoderWindowSizeBytes: windowBytes,
		})

		input := make([]byte, 4<<20)
		rng := rand.New(rand.NewSource(1))
		rng.Read(input)
		// Zero every other page so the data is compressible.
		for page := 0; page < len(input); page += 8192 {
			for i := page; i < page+4096 && i < len(input); i++ {
				input[i] = 0
			}
		}

		// Reuse the same slot several times to cover the free-list path.
		for i := 0; i < 3; i++ {
			var compressed bytes.Buffer
			enc, err := impl.GetEncoder(context.Background(), nopWriteCloser{&compressed})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := enc.ReadFrom(bytes.NewReader(input)); err != nil {
				t.Fatal(err)
			}
			if err := enc.Close(); err != nil {
				t.Fatal(err)
			}

			decompressed, err := impl.DecodeAll(compressed.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(decompressed, input) {
				t.Fatalf("window=%d iteration=%d: roundtrip mismatch", windowBytes, i)
			}
		}
	}
}

func TestBoundedGoZstdAdmissionTimeout(t *testing.T) {
	metrics := &countingMetrics{}
	impl := newBounded(t, ZstdLimits{
		MaxActiveEncoders:       1,
		EncoderAdmissionTimeout: 20 * time.Millisecond,
		Metrics:                 metrics,
	})

	held, err := impl.GetEncoder(context.Background(), nopWriteCloser{io.Discard})
	if err != nil {
		t.Fatal(err)
	}

	_, err = impl.GetEncoder(context.Background(), nopWriteCloser{io.Discard})
	if !errors.Is(err, ErrEncoderSaturated) {
		t.Fatalf("expected ErrEncoderSaturated, got %v", err)
	}

	if err := held.Close(); err != nil {
		t.Fatal(err)
	}

	// Capacity must be available again after release.
	enc, err := impl.GetEncoder(context.Background(), nopWriteCloser{io.Discard})
	if err != nil {
		t.Fatalf("expected admission after release, got %v", err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	if got := metrics.started.Load(); got != 3 {
		t.Errorf("expected 3 admission attempts, got %d", got)
	}
	if got := metrics.admitted.Load(); got != 2 {
		t.Errorf("expected 2 admissions, got %d", got)
	}
	if got := metrics.timeouts.Load(); got != 1 {
		t.Errorf("expected 1 timeout, got %d", got)
	}
	if got := metrics.released.Load(); got != 2 {
		t.Errorf("expected 2 releases, got %d", got)
	}
}

func TestBoundedGoZstdCancellationWhileWaiting(t *testing.T) {
	metrics := &countingMetrics{}
	impl := newBounded(t, ZstdLimits{
		MaxActiveEncoders: 1,
		// No admission timeout: only the caller's context can end the wait.
		Metrics: metrics,
	})

	held, err := impl.GetEncoder(context.Background(), nopWriteCloser{io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	waitErr := make(chan error, 1)
	go func() {
		_, err := impl.GetEncoder(ctx, nopWriteCloser{io.Discard})
		waitErr <- err
	}()

	// Give the waiter a moment to block, then cancel it.
	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-waitErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		if errors.Is(err, ErrEncoderSaturated) {
			t.Fatalf("caller cancellation must not be reported as saturation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled waiter did not return")
	}

	if got := metrics.canceled.Load(); got != 1 {
		t.Errorf("expected 1 canceled admission, got %d", got)
	}
}

func TestBoundedGoZstdCallerDeadlinePreempts(t *testing.T) {
	impl := newBounded(t, ZstdLimits{
		MaxActiveEncoders:       1,
		EncoderAdmissionTimeout: 10 * time.Second,
	})

	held, err := impl.GetEncoder(context.Background(), nopWriteCloser{io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err = impl.GetEncoder(ctx, nopWriteCloser{io.Discard})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected caller's DeadlineExceeded, got %v", err)
	}
	if errors.Is(err, ErrEncoderSaturated) {
		t.Fatalf("caller deadline must not be reported as saturation: %v", err)
	}
}

func TestBoundedGoZstdCloseIdempotent(t *testing.T) {
	metrics := &countingMetrics{}
	impl := newBounded(t, ZstdLimits{
		MaxActiveEncoders:       1,
		EncoderAdmissionTimeout: 20 * time.Millisecond,
		Metrics:                 metrics,
	})

	enc, err := impl.GetEncoder(context.Background(), nopWriteCloser{io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	if got := metrics.released.Load(); got != 1 {
		t.Fatalf("double Close must release capacity exactly once, got %d releases", got)
	}

	// The single slot must still be usable exactly once at a time: a
	// double release would have inflated capacity to two.
	first, err := impl.GetEncoder(context.Background(), nopWriteCloser{io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	_, err = impl.GetEncoder(context.Background(), nopWriteCloser{io.Discard})
	if !errors.Is(err, ErrEncoderSaturated) {
		t.Fatalf("expected saturation with one slot held, got %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBoundedGoZstdConcurrentStress(t *testing.T) {
	impl := newBounded(t, ZstdLimits{
		MaxActiveEncoders:       4,
		EncoderAdmissionTimeout: 5 * time.Second,
	})

	input := make([]byte, 64<<10)
	rand.New(rand.NewSource(2)).Read(input)

	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 8; i++ {
				var compressed bytes.Buffer
				enc, err := impl.GetEncoder(context.Background(), nopWriteCloser{&compressed})
				if err != nil {
					t.Errorf("GetEncoder: %v", err)
					return
				}
				if _, err := enc.Write(input); err != nil {
					t.Errorf("Write: %v", err)
					_ = enc.Close()
					return
				}
				if err := enc.Close(); err != nil {
					t.Errorf("Close: %v", err)
					return
				}
				decompressed, err := impl.DecodeAll(compressed.Bytes())
				if err != nil || !bytes.Equal(decompressed, input) {
					t.Errorf("roundtrip mismatch: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	// All capacity must be released: the full budget is acquirable.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var encoders []zstdEncoder
	for i := 0; i < 4; i++ {
		enc, err := impl.GetEncoder(ctx, nopWriteCloser{io.Discard})
		if err != nil {
			t.Fatalf("slot %d not released after stress: %v", i, err)
		}
		encoders = append(encoders, enc)
	}
	for _, enc := range encoders {
		_ = enc.Close()
	}
}
