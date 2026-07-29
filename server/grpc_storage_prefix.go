package server

import (
	"context"
	"crypto/subtle"

	"github.com/buchgr/bazel-remote/v2/cache"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Storage-prefix trust interceptors: when bazel-remote runs as a shared L1
// node in front of an object-storage backend, upstream embedded bazel-remote
// instances (on FA hosts) forward each request's tenant storage prefix as
// gRPC metadata (cache.StoragePrefixGRPCMetadataKey). These interceptors lift
// that header onto the request context, which both partitions the local disk
// cache keyspace per tenant and scopes backend object keys.
//
// Trust-on is fail-closed: every cache RPC must carry exactly one valid
// prefix (and the shared secret, when configured) or it is rejected at the
// boundary. There is no accept-unscoped mode — a request that would land in
// a shared keyspace is an isolation bug, and the server rejection is the
// mechanism that surfaces it loudly. Health and capabilities RPCs are exempt
// so probes and CheckCapabilities work without tenant context.
//
// Prefix rejections carry the cache.TrustRejectionErrorDomain ErrorInfo
// marker (the prefix is minted by the trusted upstream itself, so a
// rejection means upstream drift, never customer input); auth rejections
// stay unmarked Unauthenticated, which the upstream already degrades via its
// code-based auth check.

func storagePrefixFromIncomingContext(ctx context.Context, authSecret string) (context.Context, error) {
	md, _ := metadata.FromIncomingContext(ctx)

	if authSecret != "" {
		secrets := md.Get(cache.AuthSecretGRPCMetadataKey)
		if len(secrets) != 1 ||
			subtle.ConstantTimeCompare([]byte(secrets[0]), []byte(authSecret)) != 1 {
			return ctx, status.Errorf(codes.Unauthenticated,
				"missing or invalid %s metadata", cache.AuthSecretGRPCMetadataKey)
		}
	}

	prefix, cause := singleMetadataValue(md, cache.StoragePrefixGRPCMetadataKey)
	if cause != "" {
		return ctx, trustRejection(cache.RejectionReasonStoragePrefix, cause,
			"%s %s metadata", cause, cache.StoragePrefixGRPCMetadataKey)
	}
	if !cache.ValidStoragePrefix(prefix) {
		return ctx, trustRejection(cache.RejectionReasonStoragePrefix, "invalid",
			"invalid %s metadata value", cache.StoragePrefixGRPCMetadataKey)
	}
	return cache.WithRequiredStoragePrefix(cache.WithStoragePrefix(ctx, prefix)), nil
}

// GRPCStoragePrefixUnaryServerInterceptor returns a unary interceptor that
// enforces the fail-closed trust contract and lifts the forwarded storage
// prefix onto the context. authSecret, when non-empty, must match the
// cache.AuthSecretGRPCMetadataKey metadata on every non-exempt RPC.
func GRPCStoragePrefixUnaryServerInterceptor(authSecret string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if exemptFromTenantMetadata(info.FullMethod) {
			return handler(ctx, req)
		}
		ctx, err := storagePrefixFromIncomingContext(ctx, authSecret)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// GRPCStoragePrefixStreamServerInterceptor returns a stream interceptor that
// enforces the fail-closed trust contract and lifts the forwarded storage
// prefix onto the context.
func GRPCStoragePrefixStreamServerInterceptor(authSecret string) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if exemptFromTenantMetadata(info.FullMethod) {
			return handler(srv, ss)
		}
		ctx, err := storagePrefixFromIncomingContext(ss.Context(), authSecret)
		if err != nil {
			return err
		}
		return handler(srv, &tenantMetadataServerStream{ServerStream: ss, ctx: ctx})
	}
}
