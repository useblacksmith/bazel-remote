package zstdpool

import (
	"io"

	"github.com/klauspost/compress/zstd"
)

// BoundedDecoderPool hands out streaming zstd decoders and retains at most
// a fixed number of idle ones. Unlike a sync.Pool, a burst of concurrent
// decoders does not pin its peak memory until the next GC cycles: decoders
// beyond the retention limit are closed (freeing their buffers and
// goroutines) as soon as they are released.
//
// The pool does not bound concurrent decoder creation; embedders are
// expected to bound concurrency upstream (for example via write-stream
// admission), which then also bounds the transient decoder memory at
// concurrency x per-decoder cost (~5.2 MiB measured, see codecmem_test.go).
type BoundedDecoderPool struct {
	free chan *zstd.Decoder
}

// NewBoundedDecoderPool returns a pool retaining up to retained idle
// decoders. retained must be positive.
func NewBoundedDecoderPool(retained int64) *BoundedDecoderPool {
	return &BoundedDecoderPool{
		free: make(chan *zstd.Decoder, retained),
	}
}

// GetDecoder returns an io.ReadCloser that streams the decompressed form of
// r. Closing it quiesces the decoder and either retains it for reuse or
// destroys it if the retention limit is reached. The ReadCloser is not safe
// for concurrent use, and Close is idempotent.
func (p *BoundedDecoderPool) GetDecoder(r io.Reader) (io.ReadCloser, error) {
	var dec *zstd.Decoder
	select {
	case dec = <-p.free:
	default:
		var err error
		dec, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			return nil, err
		}
	}

	if err := dec.Reset(r); err != nil {
		// Reset only fails on a closed decoder, which cannot be in the
		// free list; handle it defensively anyway.
		dec.Close()
		return nil, err
	}

	return &pooledDecoderReadCloser{pool: p, dec: dec}, nil
}

type pooledDecoderReadCloser struct {
	pool   *BoundedDecoderPool
	dec    *zstd.Decoder
	closed bool
}

func (d *pooledDecoderReadCloser) Read(p []byte) (int, error) {
	return d.dec.Read(p)
}

func (d *pooledDecoderReadCloser) Close() error {
	if d.closed {
		return nil
	}
	d.closed = true

	if err := d.dec.Reset(nil); err != nil {
		d.dec.Close()
		return nil
	}
	select {
	case d.pool.free <- d.dec:
	default:
		// Retention limit reached: free the decoder's buffers and
		// goroutines instead of pinning them until the next GC.
		d.dec.Close()
	}
	return nil
}
