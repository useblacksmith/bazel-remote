package config

import (
	"path/filepath"
	"strings"
	"testing"
)

// The disk cache's startup scan (cache/disk/load.go queueCacheDirs)
// hard-fails the whole server on any unknown directory inside the cache
// dir. The v1-fallback transcode buffer must therefore always be placed
// OUTSIDE the cache dir (but on the same filesystem).
func TestV1FallbackTmpDirIsNeverInsideCacheDir(t *testing.T) {
	for _, cacheDir := range []string{
		"/mnt/bazel-cache/data",
		"/mnt/bazel-cache/data/", // trailing slash must not change the answer
		"relative/data",
	} {
		got := v1FallbackTmpDir(cacheDir)
		norm := filepath.Clean(cacheDir)
		if got == norm || strings.HasPrefix(got, norm+string(filepath.Separator)) {
			t.Fatalf("v1FallbackTmpDir(%q) = %q is inside the scanned cache dir", cacheDir, got)
		}
		if filepath.Dir(got) != filepath.Dir(norm) {
			t.Fatalf("v1FallbackTmpDir(%q) = %q is not a sibling of the cache dir", cacheDir, got)
		}
	}
}
