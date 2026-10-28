package cache

import (
	"context"
	"strings"
)

// FallbackPrefixesGRPCMetadataKey carries the comma-separated, already
// validated storage prefixes a read may consult after the stamped
// StoragePrefixGRPCMetadataKey misses. Writes ignore it: an entry is only
// ever stored under the stamped prefix.
const FallbackPrefixesGRPCMetadataKey = "x-blacksmith-fallback-prefixes"

// ACImmutableGRPCMetadataKey, when set to "1", makes UpdateActionResult refuse
// to overwrite an existing action result (ALREADY_EXISTS).
const ACImmutableGRPCMetadataKey = "x-blacksmith-ac-immutable"

type fallbackPrefixesContextKey struct{}
type acImmutableContextKey struct{}

// ParseFallbackPrefixes splits a FallbackPrefixesGRPCMetadataKey value. It
// returns ok=false when any entry fails ValidStoragePrefix.
func ParseFallbackPrefixes(raw string) ([]string, bool) {
	if strings.TrimSpace(raw) == "" {
		return nil, true
	}
	parts := strings.Split(raw, ",")
	prefixes := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if !ValidStoragePrefix(p) {
			return nil, false
		}
		prefixes = append(prefixes, p)
	}
	return prefixes, true
}

// WithFallbackStoragePrefixes attaches the ordered read-fallback prefixes.
func WithFallbackStoragePrefixes(ctx context.Context, prefixes []string) context.Context {
	if len(prefixes) == 0 {
		return ctx
	}
	return context.WithValue(ctx, fallbackPrefixesContextKey{}, prefixes)
}

// FallbackStoragePrefixesFromContext returns the ordered read-fallback
// prefixes attached to ctx, or nil.
func FallbackStoragePrefixesFromContext(ctx context.Context) []string {
	if ctx == nil {
		return nil
	}
	prefixes, _ := ctx.Value(fallbackPrefixesContextKey{}).([]string)
	return prefixes
}

// ReadContexts returns the contexts a read should try in order: ctx itself
// (the stamped prefix) followed by one context per fallback prefix. The
// fallback contexts carry no fallback list of their own, so a backend that
// recurses through this function stops after one level.
func ReadContexts(ctx context.Context) []context.Context {
	fallbacks := FallbackStoragePrefixesFromContext(ctx)
	if len(fallbacks) == 0 {
		return []context.Context{ctx}
	}
	stamped, _ := StoragePrefixFromContext(ctx)
	contexts := make([]context.Context, 0, 1+len(fallbacks))
	contexts = append(contexts, ctx)
	base := context.WithValue(ctx, fallbackPrefixesContextKey{}, []string(nil))
	for _, prefix := range fallbacks {
		if prefix == stamped {
			continue
		}
		contexts = append(contexts, WithStoragePrefix(base, prefix))
	}
	return contexts
}

// WithACImmutable marks the request's action-cache writes as create-only.
func WithACImmutable(ctx context.Context) context.Context {
	return context.WithValue(ctx, acImmutableContextKey{}, true)
}

// ACImmutableFromContext reports whether action-cache writes on ctx must not
// overwrite an existing entry.
func ACImmutableFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	immutable, ok := ctx.Value(acImmutableContextKey{}).(bool)
	return ok && immutable
}
