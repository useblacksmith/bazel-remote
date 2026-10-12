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
	for _, size := range []int64{3*1024*1024 + 123, 4096 + 7, defaultChunkSize, defaultChunkSize - 1, defaultChunkSize + 1} {
		testVerbatimZstdRoundTrip(t, zstd, size)
	}
}

func testVerbatimZstdRoundTrip(t *testing.T, zstd zstdimpl.ZstdImpl, size int64) {
	data, _ := testutils.RandomDataAndHash(size)
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

	for _, offset := range []int64{0, 1, size / 2, size - 1} {
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

func TestWriteZstdFrameAndCloseRejectsOversizedBody(t *testing.T) {
	dir := testutils.TempDir(t)
	const size = 4096
	body := make([]byte, MaxCompressedSize(size)+1)
	f, err := os.Create(path.Join(dir, "blob"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteZstdFrameAndClose(bytes.NewReader(body), f, size); err == nil {
		t.Fatal("expected error for body longer than any valid encoding of size")
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

func TestVerbatimUncompressedReadCappedAtDeclaredSize(t *testing.T) {
	zstd, err := zstdimpl.Get("go")
	if err != nil {
		t.Fatal(err)
	}
	dir := testutils.TempDir(t)
	const actual = 64 * 1024
	const declared = actual - 300
	data, _ := testutils.RandomDataAndHash(actual)
	frame := zstd.EncodeAll(data)
	f, err := os.Create(path.Join(dir, "blob"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteZstdFrameAndClose(bytes.NewReader(frame), f, declared); err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int64{0, 17} {
		rf, err := os.Open(path.Join(dir, "blob"))
		if err != nil {
			t.Fatal(err)
		}
		rc, err := GetUncompressedReadCloser(zstd, rf, declared, offset)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(got)) != declared-offset {
			t.Fatalf("offset %d: read %d bytes, want %d", offset, len(got), declared-offset)
		}
		if !bytes.Equal(got, data[offset:declared]) {
			t.Fatalf("offset %d: content mismatch", offset)
		}
	}
}

func TestVerbatimReadWithUnknownExpectedSize(t *testing.T) {
	zstd, err := zstdimpl.Get("go")
	if err != nil {
		t.Fatal(err)
	}
	dir := testutils.TempDir(t)
	const size = 48 * 1024
	data, _ := testutils.RandomDataAndHash(size)
	frame := zstd.EncodeAll(data)
	f, err := os.Create(path.Join(dir, "blob"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteZstdFrameAndClose(bytes.NewReader(frame), f, size); err != nil {
		t.Fatal(err)
	}
	rf, err := os.Open(path.Join(dir, "blob"))
	if err != nil {
		t.Fatal(err)
	}
	rc, err := GetUncompressedReadCloser(zstd, rf, -1, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read %d bytes with unknown expected size, want %d", len(got), size)
	}

	rf, err = os.Open(path.Join(dir, "blob"))
	if err != nil {
		t.Fatal(err)
	}
	rc, err = GetUncheckedReadCloser(zstd, rf)
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("unchecked read mismatch")
	}
}
