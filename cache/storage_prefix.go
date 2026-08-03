package cache

import (
	"context"
	"strings"
)

// StoragePrefixGRPCMetadataKey is the gRPC metadata key a trusted upstream
// bazel-remote (e.g. the FA host's embedded instance running a grpc proxy
// backend) uses to forward the request-scoped storage prefix to a downstream
// L1 bazel-remote node. The downstream node only honors it when explicitly
// configured to trust its callers (private network / authenticated peers).
const StoragePrefixGRPCMetadataKey = "x-blacksmith-storage-prefix"

// AuthSecretGRPCMetadataKey carries the shared secret that authenticates a
// trusted upstream (FA host) to an L1 bazel-remote node. It is a static
// host-level credential (ansible/1Password-distributed); VLAN exposure and
// iptables allowlists remain the outer defense layers.
const AuthSecretGRPCMetadataKey = "x-blacksmith-cache-auth"

// ValidStoragePrefix reports whether prefix is safe to use as a physical
// object-key prefix received from a remote caller. It rejects anything that
// could escape the intended keyspace (absolute paths, dot-dot segments,
// backslashes) or is unreasonably large.
func ValidStoragePrefix(prefix string) bool {
	if prefix == "" || len(prefix) > 512 {
		return false
	}
	if strings.HasPrefix(prefix, "/") || strings.Contains(prefix, "\\") {
		return false
	}
	for _, segment := range strings.Split(strings.TrimSuffix(prefix, "/"), "/") {
		if segment == "" || segment == "." || segment == ".." {
			return false
		}
	}
	return true
}

type storagePrefixContextKey struct{}
type requireStoragePrefixContextKey struct{}

// WithStoragePrefix returns a context whose cache backend operations should use
// prefix as the physical object-key prefix for this request.
func WithStoragePrefix(ctx context.Context, prefix string) context.Context {
	return context.WithValue(ctx, storagePrefixContextKey{}, prefix)
}

// StoragePrefixFromContext returns a request-scoped physical object-key prefix
// when one was attached to ctx.
func StoragePrefixFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	prefix, ok := ctx.Value(storagePrefixContextKey{}).(string)
	if !ok || prefix == "" {
		return "", false
	}
	return prefix, true
}

// WithRequiredStoragePrefix marks a request as expecting a request-scoped
// storage prefix. Backends can use this to log when they must fall back to the
// configured process-wide prefix.
func WithRequiredStoragePrefix(ctx context.Context) context.Context {
	return context.WithValue(ctx, requireStoragePrefixContextKey{}, true)
}

func StoragePrefixRequiredFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	required, ok := ctx.Value(requireStoragePrefixContextKey{}).(bool)
	return ok && required
}
