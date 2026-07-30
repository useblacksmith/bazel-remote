package server

import (
	"io"

	syncpool "github.com/mostynb/zstdpool-syncpool"
)

// zstdDecoderSource provides streaming decoders for compressed ByteStream
// writes. The returned ReadCloser must be closed exactly once by the write
// path; Close returns the decoder to its pool.
type zstdDecoderSource interface {
	GetDecoder(r io.Reader) (io.ReadCloser, error)
}

// zstdDecoderSource returns the configured source, defaulting to the
// process-wide sync.Pool. The nil check keeps directly-constructed
// grpcServer values (tests) working without ServeGRPC's initialization.
func (s *grpcServer) zstdDecoderSource() zstdDecoderSource {
	if s.zstdDecoders == nil {
		return syncPoolDecoders{}
	}
	return s.zstdDecoders
}

// syncPoolDecoders preserves upstream bazel-remote behavior: decoders come
// from the process-wide sync.Pool and return to it on Close. A burst of
// concurrent writes retains its peak decoder memory until the pool drains
// over subsequent GC cycles; use WithZstdDecoderRetention to bound that.
type syncPoolDecoders struct{}

func (syncPoolDecoders) GetDecoder(r io.Reader) (io.ReadCloser, error) {
	dec, ok := decoderPool.Get().(*syncpool.DecoderWrapper)
	if !ok {
		return nil, errDecoderPoolFail
	}
	if err := dec.Reset(r); err != nil {
		return nil, err
	}
	return dec.IOReadCloser(), nil
}
