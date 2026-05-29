package s3proxy

import (
	"bytes"
	"context"
	"io"
	stdlog "log"
	"strings"
	"testing"

	"github.com/buchgr/bazel-remote/v2/cache"
	"github.com/buchgr/bazel-remote/v2/utils/backendproxy"
)

func TestObjectKey(t *testing.T) {
	testCases := []struct {
		prefix     string
		key        string
		kind       cache.EntryKind
		expectedV1 string
		expectedV2 string
	}{
		{"", "1234", cache.CAS, "cas/12/1234", "cas.v2/12/1234"},
		{"test", "1234", cache.CAS, "test/cas/12/1234", "test/cas.v2/12/1234"},
		{"foo/bar/grok", "1234", cache.CAS, "foo/bar/grok/cas/12/1234", "foo/bar/grok/cas.v2/12/1234"},
		{"", "1234", cache.AC, "ac/12/1234", "ac/12/1234"},
		{"", "1234", cache.RAW, "raw/12/1234", "raw/12/1234"},
		{"foo/bar", "1234", cache.AC, "foo/bar/ac/12/1234", "foo/bar/ac/12/1234"},
	}

	for _, tc := range testCases {
		result := objectKeyV2(tc.prefix, tc.key, tc.kind)
		if result != tc.expectedV2 {
			t.Errorf("objectKeyV2 did not match. (result: '%s' expected: '%s'",
				result, tc.expectedV2)
		}

		result = objectKeyV1(tc.prefix, tc.key, tc.kind)
		if result != tc.expectedV1 {
			t.Errorf("objectKeyV1 did not match. (result: '%s' expected: '%s'",
				result, tc.expectedV1)
		}
	}
}

func TestObjectKeyForContextDefaultsToConfiguredPrefix(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	configuredPrefix := "minio-prefix/buck2/production/us-east-1"
	c := &s3Cache{
		prefix:    configuredPrefix,
		objectKey: objectKeyV2,
	}

	result := c.objectKeyForContext(context.Background(), hash, cache.CAS)
	expected := configuredPrefix + "/cas.v2/ab/" + hash
	if result != expected {
		t.Errorf("objectKeyForContext did not use configured prefix. (result: '%s' expected: '%s')",
			result, expected)
	}
}

func TestObjectKeyForContextUsesRequestScopedPrefixForCASOnly(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	repoAPrefix := "minio-prefix/bazel/production/us-east-1/42/987654/v0"
	repoBPrefix := "minio-prefix/bazel/production/us-east-1/42/111111/v0"
	configuredPrefix := "minio-prefix/buck2/production/us-east-1"
	c := &s3Cache{
		prefix:    configuredPrefix,
		objectKey: objectKeyV2,
	}

	repoAContext := cache.WithStoragePrefix(context.Background(), repoAPrefix)
	repoBContext := cache.WithStoragePrefix(context.Background(), repoBPrefix)

	testCases := []struct {
		name     string
		ctx      context.Context
		kind     cache.EntryKind
		expected string
	}{
		{
			name:     "repo a cas",
			ctx:      repoAContext,
			kind:     cache.CAS,
			expected: repoAPrefix + "/cas.v2/ab/" + hash,
		},
		{
			name:     "repo b cas",
			ctx:      repoBContext,
			kind:     cache.CAS,
			expected: repoBPrefix + "/cas.v2/ab/" + hash,
		},
		{
			name:     "repo a action cache",
			ctx:      repoAContext,
			kind:     cache.AC,
			expected: configuredPrefix + "/ac/ab/" + hash,
		},
		{
			name:     "repo b action cache",
			ctx:      repoBContext,
			kind:     cache.AC,
			expected: configuredPrefix + "/ac/ab/" + hash,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := c.objectKeyForContext(tc.ctx, hash, tc.kind)
			if result != tc.expected {
				t.Errorf("objectKeyForContext did not use request-scoped prefix. (result: '%s' expected: '%s')",
					result, tc.expected)
			}
		})
	}

	repoACASKey := c.objectKeyForContext(repoAContext, hash, cache.CAS)
	repoBCASKey := c.objectKeyForContext(repoBContext, hash, cache.CAS)
	if repoACASKey == repoBCASKey {
		t.Fatalf("same CAS digest produced identical object keys for different request-scoped prefixes: %s", repoACASKey)
	}

	repoAACKey := c.objectKeyForContext(repoAContext, hash, cache.AC)
	repoBACKey := c.objectKeyForContext(repoBContext, hash, cache.AC)
	if repoAACKey != repoBACKey {
		t.Fatalf("action cache object keys should ignore request-scoped prefix: %s != %s", repoAACKey, repoBACKey)
	}
}

func TestPutCapturesRequestScopedPrefixForAsyncUpload(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	requestPrefix := "minio-prefix/bazel/production/us-east-1/42/987654/v0"
	uploadQueue := make(chan backendproxy.UploadReq, 1)
	c := &s3Cache{
		prefix:      "minio-prefix/buck2/production/us-east-1",
		uploadQueue: uploadQueue,
	}

	rc := io.NopCloser(strings.NewReader("blob"))
	c.Put(cache.WithStoragePrefix(context.Background(), requestPrefix), cache.CAS, hash, 4, 4, rc)

	item := <-uploadQueue
	defer item.Rc.Close()
	if item.StoragePrefix != requestPrefix {
		t.Fatalf("queued upload StoragePrefix = %q, want %q", item.StoragePrefix, requestPrefix)
	}
	if !item.RequestScopedStoragePrefix {
		t.Fatal("queued upload RequestScopedStoragePrefix = false, want true")
	}
	if item.RequireStoragePrefix {
		t.Fatal("queued upload RequireStoragePrefix = true, want false")
	}
}

func TestPutIgnoresRequestScopedPrefixForActionCacheAsyncUpload(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	requestPrefix := "minio-prefix/bazel/production/us-east-1/42/987654/v0"
	configuredPrefix := "minio-prefix/buck2/production/us-east-1"
	uploadQueue := make(chan backendproxy.UploadReq, 1)
	c := &s3Cache{
		prefix:      configuredPrefix,
		uploadQueue: uploadQueue,
	}

	ctx := cache.WithRequiredStoragePrefix(cache.WithStoragePrefix(context.Background(), requestPrefix))
	rc := io.NopCloser(strings.NewReader("blob"))
	c.Put(ctx, cache.AC, hash, 4, 4, rc)

	item := <-uploadQueue
	defer item.Rc.Close()
	if item.StoragePrefix != configuredPrefix {
		t.Fatalf("queued upload StoragePrefix = %q, want %q", item.StoragePrefix, configuredPrefix)
	}
	if item.RequestScopedStoragePrefix {
		t.Fatal("queued upload RequestScopedStoragePrefix = true, want false")
	}
	if item.RequireStoragePrefix {
		t.Fatal("queued upload RequireStoragePrefix = true, want false")
	}
}

func TestPutCapturesMissingRequiredRequestScopedPrefixForAsyncUpload(t *testing.T) {
	hash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	configuredPrefix := "minio-prefix/buck2/production/us-east-1"
	uploadQueue := make(chan backendproxy.UploadReq, 1)
	c := &s3Cache{
		prefix:      configuredPrefix,
		uploadQueue: uploadQueue,
	}

	rc := io.NopCloser(strings.NewReader("blob"))
	c.Put(cache.WithRequiredStoragePrefix(context.Background()), cache.CAS, hash, 4, 4, rc)

	item := <-uploadQueue
	defer item.Rc.Close()
	if item.StoragePrefix != configuredPrefix {
		t.Fatalf("queued upload StoragePrefix = %q, want %q", item.StoragePrefix, configuredPrefix)
	}
	if item.RequestScopedStoragePrefix {
		t.Fatal("queued upload RequestScopedStoragePrefix = true, want false")
	}
	if !item.RequireStoragePrefix {
		t.Fatal("queued upload RequireStoragePrefix = false, want true")
	}
}

func TestLogMissingRequiredStoragePrefix(t *testing.T) {
	var buf bytes.Buffer
	c := &s3Cache{
		prefix:      "minio-prefix/buck2/production/us-east-1",
		errorLogger: stdlog.New(&buf, "", 0),
	}

	c.logMissingRequiredStoragePrefix("UPLOAD", cache.CAS, "hash")

	result := buf.String()
	for _, expected := range []string{
		"S3 UPLOAD missing request-scoped storage prefix",
		"cas hash",
		`using configured prefix "minio-prefix/buck2/production/us-east-1"`,
	} {
		if !strings.Contains(result, expected) {
			t.Fatalf("log line %q does not contain %q", result, expected)
		}
	}
}
