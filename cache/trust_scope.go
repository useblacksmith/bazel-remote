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

// TrustScopeTrusted and TrustScopeUntrusted are the two trust pools a
// client-hashed namespace is split into: "<base>/trusted" holds what reviewed
// runs produced, "<base>/untrusted" what same-repo PRs and feature branches
// produced. The only read across pools the L1 honours is untrusted -> trusted.
const (
	TrustScopeTrusted   = "trusted"
	TrustScopeUntrusted = "untrusted"
)

// FallbackAllowed reports whether a request stamped with prefix stamped may
// read fallback: stamped must be the untrusted pool of a namespace and
// fallback the trusted pool of that same namespace. Anything else (a foreign
// base, trusted reading untrusted, an arbitrary sibling) is refused, so the
// one-way trust invariant is enforced here and not only by the grant issuer.
func FallbackAllowed(stamped, fallback string) bool {
	sb, ss, ok1 := splitScope(stamped)
	fb, fs, ok2 := splitScope(fallback)
	return ok1 && ok2 && sb == fb && ss == TrustScopeUntrusted && fs == TrustScopeTrusted
}

// SameNamespace reports whether stamped and fallback are scopes of one
// namespace ("<base>/<scope>/" with an identical base).
func SameNamespace(stamped, fallback string) bool {
	sb, _, ok1 := splitScope(stamped)
	fb, _, ok2 := splitScope(fallback)
	return ok1 && ok2 && sb == fb
}

func splitScope(prefix string) (base, scope string, ok bool) {
	trimmed := strings.TrimSuffix(prefix, "/")
	i := strings.LastIndex(trimmed, "/")
	if i <= 0 || i == len(trimmed)-1 {
		return "", "", false
	}
	return trimmed[:i], trimmed[i+1:], true
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

// WithoutFallbackStoragePrefixes returns ctx pinned to its stamped prefix
// only, for checks that must answer for exactly one scope (the create-only
// existence check: a key present only in a fallback must still be creatable).
func WithoutFallbackStoragePrefixes(ctx context.Context) context.Context {
	if len(FallbackStoragePrefixesFromContext(ctx)) == 0 {
		return ctx
	}
	return context.WithValue(ctx, fallbackPrefixesContextKey{}, []string(nil))
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
