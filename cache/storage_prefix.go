package cache

import "context"

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
