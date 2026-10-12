package casblob

import (
	"bytes"
	"io"
	"os"
	"path"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache/disk/zstdimpl"
	testutils "github.com/buchgr/bazel-remote/v2/utils"
)

func TestVerbatimZstdRoundTrip(t *testing.T) {
	zstd, err := zstdimpl.Get("go")
	if err != nil {
		t.Fatal(err)
	}

	const size = 3*1024*1024 + 123
	data, _ := testutils.RandomDataAndHash(int64(size))
	frame := zstd.EncodeAll(data)

	dir := testutils.TempDir(t)
	fn := path.Join(dir, "blob")
	f, err := os.Create(fn)
	if err != nil {
		t.Fatal(err)
	}
	onDisk, err := WriteZstdFrameAndClose(bytes.NewReader(frame), f, size)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(len(frame)) + chunkTableOffset + 2*8; onDisk != want {
		t.Fatalf("size on disk %d, want %d", onDisk, want)
	}

	for _, offset := range []int64{0, 1, 1024*1024 + 7, size - 1} {
		f, err := os.Open(fn)
		if err != nil {
			t.Fatal(err)
		}
		rc, err := GetUncompressedReadCloser(zstd, f, size, offset)
		if err != nil {
			t.Fatalf("offset %d: %v", offset, err)
		}
		got, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatalf("offset %d: %v", offset, err)
		}
		if !bytes.Equal(got, data[offset:]) {
			t.Fatalf("offset %d: uncompressed read mismatch (%d bytes)", offset, len(got))
		}

		f, err = os.Open(fn)
		if err != nil {
			t.Fatal(err)
		}
		rc, err = GetZstdReadCloser(zstd, f, size, offset)
		if err != nil {
			t.Fatalf("offset %d: %v", offset, err)
		}
		compressed, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatalf("offset %d: %v", offset, err)
		}
		got, err = zstd.DecodeAll(compressed)
		if err != nil {
			t.Fatalf("offset %d: %v", offset, err)
		}
		if !bytes.Equal(got, data[offset:]) {
			t.Fatalf("offset %d: zstd read mismatch (%d bytes)", offset, len(got))
		}
	}
}

func TestWriteZstdFrameAndCloseRejectsBadSizes(t *testing.T) {
	dir := testutils.TempDir(t)
	for _, size := range []int64{0, -1, MaxVerbatimZstdSize + 1} {
		f, err := os.Create(path.Join(dir, "blob"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := WriteZstdFrameAndClose(bytes.NewReader([]byte{1}), f, size); err == nil {
			t.Fatalf("size %d: expected error", size)
		}
	}
}
