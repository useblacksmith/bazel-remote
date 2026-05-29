package cache

import (
	"context"
	"testing"
)

func TestLookupKeyForContextDefaultsToOriginalKey(t *testing.T) {
	result := LookupKeyForContext(context.Background(), CAS, "hash")
	expected := "cas/hash"
	if result != expected {
		t.Fatalf("LookupKeyForContext() = %q, want %q", result, expected)
	}
}

func TestLookupKeyForContextIncludesStoragePrefix(t *testing.T) {
	prefix := "bazel/production/us-east-1/42/987654/v0"
	ctx := WithStoragePrefix(context.Background(), prefix)

	result := LookupKeyForContext(ctx, CAS, "hash")
	expected := "cas/hash/storage_prefix/" + StoragePrefixID(prefix)
	if result != expected {
		t.Fatalf("LookupKeyForContext() = %q, want %q", result, expected)
	}
}

func TestLookupKeyForContextIgnoresStoragePrefixForActionCache(t *testing.T) {
	ctx := WithStoragePrefix(context.Background(), "bazel/production/us-east-1/42/987654/v0")

	result := LookupKeyForContext(ctx, AC, "hash")
	expected := "ac/hash"
	if result != expected {
		t.Fatalf("LookupKeyForContext() = %q, want %q", result, expected)
	}
}

func TestStoragePrefixRequiredFromContext(t *testing.T) {
	if StoragePrefixRequiredFromContext(context.Background()) {
		t.Fatal("StoragePrefixRequiredFromContext() = true, want false")
	}

	ctx := WithRequiredStoragePrefix(context.Background())
	if !StoragePrefixRequiredFromContext(ctx) {
		t.Fatal("StoragePrefixRequiredFromContext() = false, want true")
	}
}
