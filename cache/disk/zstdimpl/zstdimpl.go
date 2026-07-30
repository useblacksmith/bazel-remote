package zstdimpl

import (
	"context"
	"fmt"
	"io"
	"slices"
)

var registry map[string]ZstdImpl

func register(implName string, impl ZstdImpl) {
	if registry == nil {
		registry = make(map[string]ZstdImpl)
	}
	registry[implName] = impl
}

func Get(implName string) (ZstdImpl, error) {
	impl, ok := registry[implName]
	if !ok {
		return nil, fmt.Errorf("unrecognized ZSTD implementation: %s, supported: %s", implName, GetImplementations())
	}
	return impl, nil
}

func GetImplementations() []string {
	result := make([]string, 0, len(registry))

	for name := range registry {
		result = append(result, name)
	}

	slices.Sort(result)

	return result
}

type ZstdImpl interface {
	GetDecoder(in io.ReadCloser) (io.ReadCloser, error)

	// GetEncoder returns a streaming encoder writing to out. The context
	// bounds any wait for encoder capacity: implementations with bounded
	// codec pools may block until an encoder is available, the context is
	// done, or an implementation-defined admission timeout elapses (in
	// which case ErrEncoderSaturated is returned).
	GetEncoder(ctx context.Context, out io.WriteCloser) (zstdEncoder, error)

	DecodeAll(in []byte) ([]byte, error)
	EncodeAll(in []byte) []byte
}

type zstdEncoder interface {
	io.WriteCloser
	io.ReaderFrom
}
