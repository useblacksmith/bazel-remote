package casblob_test

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"testing"
	"unsafe"

	"github.com/buchgr/bazel-remote/v2/cache/disk/casblob"
	"github.com/buchgr/bazel-remote/v2/cache/disk/zstdimpl"
	testutils "github.com/buchgr/bazel-remote/v2/utils"
)

func TestLenSize(t *testing.T) {
	slice := []int{}
	if unsafe.Sizeof(len(slice)) > 8 {
		// If this fails, then we have a bunch of potential truncation
		// errors all over the place.
		t.Errorf("len() returns a value larger than 8 bytes")
	}
	if len(slice) != 0 {
		// We should never hit this case.
		t.Errorf("This should silence linters that think slice is never used")
	}
}

func TestWriteAndCloseStoresAlreadyCompressedBytesAsIdentity(t *testing.T) {
	zstd, err := zstdimpl.Get("go")
	if err != nil {
		t.Fatal(err)
	}
	plain := bytes.Repeat([]byte("cache-me-"), 4096)
	var gzipped bytes.Buffer
	zw := gzip.NewWriter(&gzipped)
	if _, err := zw.Write(plain); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	compressed := zstd.EncodeAll(plain)

	for _, tc := range []struct {
		name       string
		body       []byte
		wantStored byte
	}{
		{name: "plain", body: plain, wantStored: byte(casblob.Zstandard)},
		{name: "gzip", body: gzipped.Bytes(), wantStored: byte(casblob.Identity)},
		{name: "zstd", body: compressed, wantStored: byte(casblob.Identity)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sum := sha256.Sum256(tc.body)
			hash := hex.EncodeToString(sum[:])
			dir := testutils.TempDir(t)
			file, err := os.OpenFile(dir+"/"+hash, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o664)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := casblob.WriteAndClose(zstd, bytes.NewReader(tc.body), file, casblob.Zstandard, hash, int64(len(tc.body))); err != nil {
				t.Fatal(err)
			}
			stored, err := os.ReadFile(dir + "/" + hash)
			if err != nil {
				t.Fatal(err)
			}
			if len(stored) < 17 {
				t.Fatalf("blob is %d bytes, shorter than a casblob header", len(stored))
			}
			if stored[16] != tc.wantStored {
				t.Fatalf("compression byte = %d, want %d", stored[16], tc.wantStored)
			}
			in, err := os.Open(dir + "/" + hash)
			if err != nil {
				t.Fatal(err)
			}
			rc, err := casblob.GetUncompressedReadCloser(zstd, in, int64(len(tc.body)), 0)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, tc.body) {
				t.Fatalf("round trip = %d bytes, want %d", len(got), len(tc.body))
			}
		})
	}
}

func TestZstdFromLegacy(t *testing.T) {
	size := 1024
	zstd, err := zstdimpl.Get("go")
	if err != nil {
		t.Fatal(err)
	}

	data, hash := testutils.RandomDataAndHash(int64(size))
	dir := testutils.TempDir(t)
	filename := fmt.Sprintf("%s/%s", dir, hash)
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0664)
	if err != nil {
		t.Fatal(err)
	}
	n, err := file.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	if n != size {
		t.Fatalf("Unexpected short write %d, expected %d", n, size)
	}
	err = file.Close()
	if err != nil {
		t.Fatal(err)
	}

	file, err = os.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	zrc, err := casblob.GetLegacyZstdReadCloser(zstd, file)
	if err != nil {
		t.Fatal(err)
	}
	rc, err := zstd.GetDecoder(zrc)
	if err != nil {
		t.Fatal(err)
	}
	buf := bytes.NewBuffer(nil)
	_, err = io.Copy(buf, rc)
	if err != nil {
		t.Fatal(err)
	}

	if buf.Len() != size {
		t.Fatalf("Unexpected buf size %d, expected %d", buf.Len(), size)
	}

	h := sha256.Sum256(data)
	hs := hex.EncodeToString(h[:])
	if hs != hash {
		t.Fatalf("Unexpected content sha %s, expected %s", hs, hash)
	}
}
